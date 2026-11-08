#include <cmath>
#include <string>
#include <iostream>
#include <numa.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <boost/fiber/operations.hpp>

#include "client.h"

uint64_t successed_insert_cnt[3][MAX_THREAD_NUM][8];
uint64_t successed_search_cnt[3][MAX_THREAD_NUM][8];
uint64_t successed_others_cnt[3][MAX_THREAD_NUM][8];

uint64_t rdma_write_num[MAX_THREAD_NUM][8];
uint64_t rdma_write_size[MAX_THREAD_NUM][8];
uint64_t rdma_read_num[MAX_THREAD_NUM][8];
uint64_t rdma_read_size[MAX_THREAD_NUM][8];
uint64_t rdma_atomic_num[MAX_THREAD_NUM][8];
uint64_t rdma_atomic_size[MAX_THREAD_NUM][8];

uint64_t cache_hit[MAX_THREAD_NUM][8];
uint64_t cache_miss[MAX_THREAD_NUM][8];

uint64_t stash_stall_num[MAX_THREAD_NUM][8];

thread_local std::random_device rd;
thread_local std::mt19937_64 gen(rd());
thread_local std::uniform_int_distribution<uint64_t> dis;
thread_local uint64_t hit_depth[MAX_CORO_NUM][8];
thread_local uint64_t miss_depth[MAX_CORO_NUM][8];

thread_local Buffer* Client::buf = NULL;
thread_local uint64_t Client::t_conn_id = 0;
thread_local uint64_t Client::t_numa_id = 0;
thread_local ThreadConnection* Client::t_conn = NULL;
thread_local LocalAllocator* Client::local_allocator = NULL;

thread_local bool coroutine_done[MAX_CORO_NUM];
thread_local std::vector<CoroPush> Client::workers;
thread_local CoroQueue Client::busy_waiting_queue;

int map_thread_to_logical_cpu_rr(int tid) {
  assert(tid < MAX_THREAD_NUM);

  int total_cores = CPU_NUM*CORE_PER_CPU;
  int usable_cores = CPU_NUM*(CORE_PER_CPU - 1);
  int usable_logical_cores = usable_cores*HYPERTHREAD_PER_CORE;

  int ht_id, cpu_id, core_id;

  if (tid < usable_logical_cores) {
    ht_id = tid / usable_cores;
    cpu_id = (tid % usable_cores) % CPU_NUM;
    core_id = (tid % usable_cores) / CPU_NUM;
  } else {
    ht_id = (tid - usable_logical_cores) / CPU_NUM;
    cpu_id = (tid - usable_logical_cores) % CPU_NUM;
    core_id = CORE_PER_CPU - 1;
  } 

  return ht_id*total_cores + cpu_id*CORE_PER_CPU + core_id;
}

int map_thread_to_logical_cpu_numa(int tid, int numa_node) {
  assert(tid < CORE_PER_CPU*HYPERTHREAD_PER_CORE);

  if (tid < CORE_PER_CPU) {
    return numa_node*CORE_PER_CPU + tid;
  } else {
    return CPU_NUM*CORE_PER_CPU + tid % CORE_PER_CPU;
  }
}

Client::Client(const struct ClientConfig& conf, uint64_t _thread_num, uint16_t _coroutine_num) {
  cid = conf.cid;
  server_num = conf.server_num;
  client_num = conf.client_num;

  assert(TOTAL_TAB_NUM % conf.server_num == 0);
  table_per_mn = TOTAL_TAB_NUM / conf.server_num;
  range_size = (MAX + 1 - MIN) / TOTAL_TAB_NUM;

  cache_size = conf.cache_size;
  coroutine_num = _coroutine_num;

  for (int i = 0; i < server_num; ++i) {
    strncpy(server_ips[i], conf.server_ips[i], 16);
  }

  thread_num = _thread_num;
  t_conns.reserve(thread_num+1);
  for (int i = 0; i < thread_num; ++i) {
  t_conns[i] = new ThreadConnection(conf, map_thread_to_logical_cpu_rr(i));
  }

  // Last connection for signal passing
  t_conns[thread_num] = new ThreadConnection(conf, false);

  local_allocators.reserve(thread_num);
  for (int i = 0; i < thread_num; ++i) {
    local_allocators[i] = new LocalAllocator;
  }

  is_roce = conf.is_roce;
  udp_port = conf.udp_port;
  udp_sock = socket(AF_INET, SOCK_DGRAM, 0);

  for (int i = 0; i < thread_num+1; ++i) {
    for (int j = 0; j < server_num; ++j) {
      // Create QP for new server connection
      struct ibv_qp* ib_qp = t_conns[i]->create_qp();

      struct sockaddr_in udp_server_info;
      udp_server_info.sin_family = AF_INET;
      udp_server_info.sin_port = htons(udp_port);
      udp_server_info.sin_addr.s_addr = inet_addr(server_ips[j]);

      struct ServerInfo info;
      while (true) {
        send_udp_info(i, ib_qp, udp_server_info);

        if (timeout_udp_socket(3) && recv_udp_info(&info)) {
          t_conns[i]->make_connection(ib_qp, udp_server_info, &info);
          break;
        }
      }
    }
  }

  close(udp_sock);
  t_conns[thread_num]->init_message_passing();

  global_signal_lock.store(0, std::memory_order_release);
  for (int i = 0; i < CPU_NUM; ++i) {
    void* mem = numa_alloc_onnode(sizeof(std::atomic<uint64_t>), i);
    numa_signal_lock[i] = new (mem) std::atomic<uint64_t>(0);
  }
  for (int i = 0; i < TOTAL_TAB_NUM; ++i) {
    local_tab_locks[i] = 0;
  }

  index_cache = NULL;
#ifdef SPECULATIVE_READ
  key_cache = NULL;
#endif

  local_locks = new OnChipLocalLock*[server_num];
  for (int i = 0; i < server_num; ++i) {
    local_locks[i] = new OnChipLocalLock[ON_CHIP_LOCK_NUM];
    for (int j = 0; j < ON_CHIP_LOCK_NUM; ++j) {
      auto &n = local_locks[i][j];
      n.ticket_lock.store(0);
      n.hand_over = false;
      n.hand_time = 0;
    }
  }

  local_lock_table = new LocalLockTable;

  wait_signal(SignalType::START);
}

// TODO: Need to safely free memory region
Client::~Client(void) {
  index_cache->print_statistics();
#ifdef SPECULATIVE_READ
  key_cache->print_statistics();
#endif
  delete index_cache;
#ifdef SPECULATIVE_READ
  delete key_cache;
#endif
}

bool Client::timeout_udp_socket(uint64_t timeout_sec) {
  int ret;
  fd_set fds;

  // Set timeout
  struct timeval tv;
  tv.tv_sec = timeout_sec;
  tv.tv_usec = 0;

  // Non-blocking UDP socket
  FD_ZERO(&fds);
  FD_SET(udp_sock, &fds);

  if (fcntl(udp_sock, F_SETFL, O_NONBLOCK) == -1) {
    fprintf(stdout, "[ERROR] Non-blocking socket set failure (error: %s)\n", strerror(errno));
    exit(1);
  }

  ret = select(udp_sock+1, &fds, NULL, NULL, &tv) ;

  if (ret > 0) {
    return true;
  }

  return false;
}

void Client::send_udp_info(uint64_t tid, struct ibv_qp* ib_qp, struct sockaddr_in udp_server_info) {
  struct ThreadInfo info;

  info.is_server = false;

  info.id = htons(cid);
  info.tid = htonll(tid);
  info.lid = htons(t_conns[tid]->ib_port_attr.lid);
  info.qp_num = htonl(ib_qp->qp_num); 
  info.base_addr = htonll(t_conns[tid]->base_addr);
  info.thread_num = htonll(thread_num+1);

  if (is_roce) {
    memcpy(info.gid, (uint8_t*)&(t_conns[tid]->ib_gid), sizeof(uint8_t)*16);
    info.gid_idx = t_conns[tid]->ib_gid_idx;
  } else {
    memset(info.gid, 0, sizeof(uint8_t)*16);
    info.gid_idx = 0;  
  }
  
  int send_len = sendto(udp_sock, &info, sizeof(ThreadInfo), 0, (struct sockaddr*)&udp_server_info, sizeof(struct sockaddr_in));

  if (send_len < 0) {
    fprintf(stderr, "[ERROR] Failed sending UDP client thread information (error: %s)\n", strerror(errno));
  }
}

bool Client::recv_udp_info(struct ServerInfo* info) {
  uint8_t type;
  auto ret = recvfrom(udp_sock, &type, sizeof(uint8_t), MSG_PEEK, NULL, NULL);

  if (ret > 0 && (MsgType)type == MsgType::SERVER_INFO) {
    recvfrom(udp_sock, info, sizeof(ServerInfo), 0, NULL, NULL);
    return true;
  }

  return false;
}

bool Client::recv_signal(struct Signal& sig) {
  uint64_t wr_id;
  auto ret = rdma_poll_once_with_id(t_conns[thread_num]->ib_recv_cq, wr_id);

  if (ret) {
    sig = *(Signal *)t_conns[thread_num]->get_recv_buffer(wr_id);
    assert(sig.msg == MsgType::SIGNAL);   

    struct ibv_recv_wr* bad_wr;
    if (ibv_post_recv(t_conns[thread_num]->ib_qp_list[sig.id], &t_conns[thread_num]->recv_wr[wr_id], &bad_wr)) {
      fprintf(stderr, "[ERROR] Failed RDMA RECV (error: %s)\n", strerror(errno));
    }

    return true;
  }

  return false;
}

/* RDMA write to server. */
void Client::write(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t size, CoroPull* sink, bool signal) {
  if (sink == nullptr) {
    rdma_send_write(t_conn->ib_mr, t_conn->ib_qp_list[sid], t_conn->server_info_list[sid].mr_rkey[type],
                    (uint64_t)src, t_conn->server_info_list[sid].base_addr[type] + offset, size, 0, signal);

    if (signal && !rdma_poll_completion(t_conn->ib_cq)) {
      fprintf(stderr, "[ERROR] Write failure (Target server %u / Target component %u / Target size %lu)\n", sid, type, size);   
      exit(1);
    }
  } else {
    rdma_send_write(t_conn->ib_mr, t_conn->ib_qp_list[sid], t_conn->server_info_list[sid].mr_rkey[type],
                    (uint64_t)src, t_conn->server_info_list[sid].base_addr[type] + offset, size, sink->get(), true);  
    (*sink)();
  }

#ifdef STATS
  rdma_write_num[t_conn_id][0]++;
  rdma_write_size[t_conn_id][0] += size;
#endif
}

/* RDMA batched write to server. */
void Client::batched_write(Batch* batch, CoroPull* sink) {
  rdma_send_batched_write(t_conn->ib_mr, t_conn->ib_qp_list[batch->get_sid()], batch->get_batch_num(), batch->get_batch_list(), sink ? sink->get() : 0);

  if (sink != nullptr) {
    (*sink)();
  } else if (!rdma_poll_completion(t_conn->ib_cq)) {
    fprintf(stderr, "[ERROR] Batched write failure (Target server %u / Batch number %d)\n", batch->get_sid(), batch->get_batch_num()); 
    exit(1);
  }

#ifdef STATS
  rdma_write_num[t_conn_id][0]++;
  for (int i = 0; i < batch->get_batch_num(); ++i) {
    rdma_write_size[t_conn_id][0] += batch->get_size(i);
  }
#endif
}

/* RDMA read to server. */
void Client::read(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t size, CoroPull* sink) {
  rdma_send_read(t_conn->ib_mr, t_conn->ib_qp_list[sid], t_conn->server_info_list[sid].mr_rkey[type],
                  (uint64_t)src, t_conn->server_info_list[sid].base_addr[type] + offset, size, sink ? sink->get() : 0);

  if (sink != nullptr) {
    (*sink)();
  } else if (!rdma_poll_completion(t_conn->ib_cq)) {
    fprintf(stderr, "[ERROR] Read failure (Target server %u / Target component %u / Target size %lu)\n", sid, type, size);   
    exit(1);
  }

#ifdef STATS
  rdma_read_num[t_conn_id][0]++;
  rdma_read_size[t_conn_id][0] += size;
#endif
}

/* RDMA batched read to server. */
void Client::batched_read(Batch* batch, CoroPull* sink) {
  rdma_send_batched_read(t_conn->ib_mr, t_conn->ib_qp_list[batch->get_sid()], batch->get_batch_num(), batch->get_batch_list(), sink ? sink->get() : 0);

  if (sink != nullptr) {
    (*sink)();
  } else if (!rdma_poll_completion(t_conn->ib_cq)) {
    fprintf(stderr, "[ERROR] Batched read failure (Target server %u / Batch number %d)\n", batch->get_sid(), batch->get_batch_num());  
    exit(1);
  }

#ifdef STATS
  rdma_read_num[t_conn_id][0]++;
  for (int i = 0; i < batch->get_batch_num(); ++i) {
    rdma_read_size[t_conn_id][0] += batch->get_size(i);
  }
#endif
}

/* RDMA compare-and-swap to server. */
void Client::CAS(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t compare, uint64_t swap, CoroPull* sink) {
  // RDMA CAS destination address must be aligned
  rdma_send_compare_and_swap(t_conn->ib_mr, t_conn->ib_qp_list[sid], t_conn->server_info_list[sid].mr_rkey[type],
                             (uint64_t)src, t_conn->server_info_list[sid].base_addr[type] + offset, compare, swap, sink ? sink->get() : 0);

  if (sink != nullptr) {
    (*sink)();
  } else if (!rdma_poll_completion(t_conn->ib_cq)) {
    fprintf(stderr, "[ERROR] CAS failure (Target server %u / Target component %u)\n", sid, type);   
    exit(1);
  }

#ifdef STATS
  rdma_atomic_num[t_conn_id][0]++;
  rdma_atomic_size[t_conn_id][0] += sizeof(uint64_t);
#endif
}

/* RDMA compare-and-swap to server with mask. */
void Client::masked_CAS(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t compare, uint64_t swap, uint64_t mask, CoroPull* sink) {
  // RDMA CAS destination address must be aligned
  rdma_send_masked_compare_and_swap(t_conn->ib_mr, t_conn->ib_qp_list[sid], t_conn->server_info_list[sid].mr_rkey[type],
                                    (uint64_t)src, t_conn->server_info_list[sid].base_addr[type] + offset, compare, swap, mask, sink ? sink->get() : 0);

  if (sink != nullptr) {
    (*sink)();
  } else if (!rdma_poll_completion(t_conn->ib_cq)) {
    fprintf(stderr, "[ERROR] masked CAS failure (Target server %u / Target component %u)\n", sid, type);   
    exit(1);
  }

#ifdef STATS
  rdma_atomic_num[t_conn_id][0]++;
  rdma_atomic_size[t_conn_id][0] += sizeof(uint64_t);
#endif
}

/* RDMA fetch-and-add to server. */
void Client::FAA(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t add, CoroPull* sink) {
  rdma_send_fetch_and_add(t_conn->ib_mr, t_conn->ib_qp_list[sid], t_conn->server_info_list[sid].mr_rkey[type],
                          (uint64_t)src, t_conn->server_info_list[sid].base_addr[type] + offset, add, sink ? sink->get() : 0);

  if (sink != nullptr) {
    (*sink)();
  } else if (!rdma_poll_completion(t_conn->ib_cq)) {
    fprintf(stderr, "[ERROR] FAA failure (Target server %u / Target component %u)\n", sid, type);   
    exit(1);
  }
  
#ifdef STATS
  rdma_atomic_num[t_conn_id][0]++;
  rdma_atomic_size[t_conn_id][0] += sizeof(uint64_t);
#endif
}

/* RDMA fetch-and-add to server with mask. */
void Client::masked_FAA(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t add, uint64_t mask, CoroPull* sink) {
  rdma_send_masked_fetch_and_add(t_conn->ib_mr, t_conn->ib_qp_list[sid], t_conn->server_info_list[sid].mr_rkey[type],
                                 (uint64_t)src, t_conn->server_info_list[sid].base_addr[type] + offset, add, mask, sink ? sink->get() : 0);

  if (sink != nullptr) {
    (*sink)();
  } else if (!rdma_poll_completion(t_conn->ib_cq)) {
    fprintf(stderr, "[ERROR] masked FAA failure (Target server %u / Target component %u)\n", sid, type);   
    exit(1);
  }
  
#ifdef STATS
  rdma_atomic_num[t_conn_id][0]++;
  rdma_atomic_size[t_conn_id][0] += sizeof(uint64_t);
#endif
}

void Client::check_tab_update(uint16_t tab_id, CoroPull* sink) {
  uint64_t UNLOCKED = 0;
  uint64_t LOCKED = 1;

  if (numa_signal_lock[t_numa_id]->load(std::memory_order_relaxed) == 0 &&
      numa_signal_lock[t_numa_id]->compare_exchange_weak(UNLOCKED, 1, std::memory_order_acquire, std::memory_order_relaxed)) {
    if (global_signal_lock.load(std::memory_order_relaxed) == 0 && 
        global_signal_lock.compare_exchange_weak(UNLOCKED, 1, std::memory_order_acquire, std::memory_order_relaxed)) {
      struct Signal sig;
      while (recv_signal(sig)) {
        if ((SignalType)sig.signal == SignalType::TABLE_UPDATE) {
          local_tab_locks[sig.tab_id].store(1, std::memory_order_release);
        }
      }

      global_signal_lock = 0;
    }
    
    numa_signal_lock[t_numa_id]->store(0);
  }

  if (local_tab_locks[tab_id].exchange(0, std::memory_order_acquire) == LOCKED) {
    sync_tab(tab_id, sink);
  }
}

bool Client::sync_tab(uint16_t tab_id, CoroPull* sink) {
  uint64_t entered_addr = buf[sink ? sink->get() : 0].get_cur_addr();

  uint16_t sid = get_tab_sid(tab_id);
  uint64_t tab_start_offset = get_tab_start_offset(sid, tab_id);

  TableHeader* tab_hdr;
  void* tab_hdr_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(TableHeader));

retry:
  do {
    read(tab_hdr_buffer, sid, tab_start_offset, MRType::TABLE, sizeof(TableHeader), sink);
    tab_hdr = (TableHeader*)tab_hdr_buffer;
    assert(tab_hdr->get_capacity() > 0);
  } while (!tab_hdr->is_consistent());

  TableEntry* tab_entry;
  bool consistent = false;
  void* tab_buffer = nullptr;

  if (index_cache->promote_tab_to_cache(tab_id, true, sizeof(TableHeader) + tab_hdr->get_tab_size())) { 
    tab_buffer = buf[sink ? sink->get() : 0].alloc(tab_hdr->get_tab_size());

    while (!consistent) {
      read(tab_buffer, sid, tab_start_offset+TableHeader::get_tab_offset(0), MRType::TABLE, tab_hdr->get_tab_size(), sink);
      tab_entry = (TableEntry*)tab_buffer;

      for (uint64_t i = 0; i < tab_hdr->get_capacity(); ++i) {
        if (!tab_entry[i].is_node_consistent(tab_hdr->get_version())) { // Table merge occurred
          buf[sink ? sink->get() : 0].free(tab_hdr->get_tab_size());
          goto retry;
        } else if (!tab_entry[i].is_entry_consistent() ||
                   (i < tab_hdr->get_capacity()-1 && tab_entry[i] == tab_entry[i+1] && tab_entry[i].get_dir_version() != tab_entry[i+1].get_dir_version()) || 
                   (i < tab_hdr->get_capacity()-1 && tab_entry[i] != tab_entry[i+1] && tab_entry[i].get_next_dir_num() != tab_entry[i+1].get_dir_ptr().num)) {
          break;
        } else if (i == tab_hdr->get_capacity() - 1) {
          consistent = true;
        }
      }
    }
  }

  index_cache->update_tab_to_cache(tab_id, tab_hdr, tab_buffer);
  buf[sink ? sink->get() : 0].revert_to(entered_addr);

  return true;
}

#ifndef LOCK_FREE_STASH
/* Client acquires lock to stash. */
void Client::lock_stash(void* buffer, uint16_t sid, uint64_t offset, CoroPull* sink) {
  uint64_t retry = 0;

  while (true) {
    retry++;

    CAS(buffer, sid, offset, MRType::STASH, 0, 1, sink);
    uint64_t lock = *(uint64_t*)buffer;

    if (lock == 0) {
      break;
    }

    uint64_t W = 100*retry + dis(gen) % 1000;
    if (sink == nullptr) {
      std::this_thread::sleep_for(std::chrono::nanoseconds(W));
    } else {
      boost::this_fiber::sleep_for(std::chrono::nanoseconds(W));
    }
  }
}

/* Client releases lock to stash. */
void Client::unlock_stash(void* buffer, uint16_t sid, uint64_t offset, CoroPull* sink) {
  uint64_t* lock = new (buffer) uint64_t(0);
  write(buffer, sid, offset, MRType::STASH, sizeof(uint64_t), sink);
}
#endif

bool Client::alloc_stash(void* buffer, uint16_t sid, uint64_t& offset, CoroPull* sink) {
  masked_FAA(buffer, sid, offset, MRType::STASH, 1UL << 32, STASH_FAA_MASK, sink);
  StashHeader* new_stash_hdr = (StashHeader*)buffer;

  if (new_stash_hdr->full()) {
    stash_stall_num[t_conn_id][0]++;
    
    do {
      read(buffer, sid, offset, MRType::STASH, sizeof(uint64_t), sink);
      new_stash_hdr = (StashHeader*)buffer;
    } while(new_stash_hdr->full());

    return false;
  }

  offset += Stash::get_stash_offset(new_stash_hdr->transpose_idx(new_stash_hdr->entry_used));
  return true;
}

bool Client::acquire_local_lock(Address lock_addr, CoroPull* sink) {
  auto &node = local_locks[lock_addr.sid][lock_addr.num];

  uint64_t lock_val = node.ticket_lock.fetch_add(1);

  uint32_t ticket = lock_val << 32 >> 32;
  uint32_t current = lock_val >> 32;

  while (ticket != current) { // lock failed

    if (sink != nullptr) {
      yield_coroutine(sink);
    }

    current = node.ticket_lock.load(std::memory_order_relaxed) >> 32;
  }

  node.hand_time++;

  return node.hand_over;
}

bool Client::can_hand_over(Address lock_addr) {
  auto &node = local_locks[lock_addr.sid][lock_addr.num];
  uint64_t lock_val = node.ticket_lock.load(std::memory_order_relaxed);

  uint32_t ticket = lock_val << 32 >> 32;
  uint32_t current = lock_val >> 32;

  if (ticket <= current + 1) { // no pending locks
    node.hand_over = false;
  } else {
    node.hand_over = node.hand_time < MAX_HANDOVER_TIME;
  }

  if (!node.hand_over) {
    node.hand_time = 0;
  }

  return node.hand_over;
}

void Client::releases_local_lock(Address lock_addr) {
  auto &node = local_locks[lock_addr.sid][lock_addr.num];
  node.ticket_lock.fetch_add((1ull << 32));
}

void Client::lock_bucket(void* buffer, Address lock_addr, CoroPull* sink) {
  bool hand_over = acquire_local_lock(lock_addr, sink);
  if (hand_over) {
    return;
  }

  while (true) {
    CAS(buffer, lock_addr.sid, lock_addr.num*sizeof(uint64_t), MRType::ON_CHIP, 0, 1, sink);
    uint64_t lock = *(uint64_t*)buffer;

    if (lock == 0) {
      break;
    }
  }
}

void Client::unlock_bucket(void* buffer, Address lock_addr, CoroPull* sink) {
  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other) {
    releases_local_lock(lock_addr);
    return;
  }

  uint64_t* lock = new (buffer) uint64_t(0);
  write(buffer, lock_addr.sid, lock_addr.num*sizeof(uint64_t), MRType::ON_CHIP, sizeof(uint64_t), sink, false);

  releases_local_lock(lock_addr);
}

void Client::write_and_unlock_bucket(void* bucket_buffer, uint16_t bucket_sid, uint64_t bucket_offset, uint64_t bucket_size, void* lock_buffer, Address lock_addr, CoroPull* sink) {
  bool hand_over_other = can_hand_over(lock_addr);
  if (hand_over_other) {
    write(bucket_buffer, bucket_sid, bucket_offset, MRType::BUCKET, bucket_size, sink);
    releases_local_lock(lock_addr);
    return;
  }

  uint64_t* lock = new (lock_buffer) uint64_t(0);

  Batch buc_batch(bucket_sid, 2);
  buc_batch.add(create_batch(bucket_buffer, bucket_sid, bucket_offset, MRType::BUCKET, bucket_size));
  buc_batch.add(create_batch(lock_buffer, lock_addr.sid, lock_addr.num*sizeof(uint64_t), MRType::ON_CHIP, sizeof(uint64_t)));
  batched_write(&buc_batch, sink);

  releases_local_lock(lock_addr);  
}

#ifdef SPECULATIVE_READ
bool Client::speculative_read(Bucket* bucket, Address& dest, const Key_t& k, Value_t& v, int& speculative_idx, bool for_write, CoroPull* sink) {
  if (key_cache->search_idx_from_cache(dest, 0, BUCKET_CAPACITY, k, speculative_idx)) {
    do {
      read(&bucket->slot[speculative_idx], dest.sid, Bucket::get_bucket_offset(dest.num)+Bucket::get_pair_offset(speculative_idx), MRType::BUCKET, sizeof(Pair), sink); 
    } while (!for_write && !bucket->slot[speculative_idx].is_consistent());

    if (bucket->slot[speculative_idx].key == k) {
      return true;
    }
  }

  return false;
}
#endif

Bucket* Client::read_bucket(void* buffer, Address dest, bool locked, CoroPull* sink) {
  Bucket* bucket;
  do {
    read(buffer, dest.sid, Bucket::get_bucket_offset(dest.num), MRType::BUCKET, sizeof(Bucket), sink);
    bucket = (Bucket*)buffer;  
  } while (!locked && !bucket->is_consistent());

  return bucket;
}

/* Allocate a new bucket and return the address. */
Address Client::alloc_bucket(void* buffer, uint16_t sid, CoroPull* sink) {
retry:
  bool need_chunk = true;
  Address bucket_addr = local_allocator->malloc(need_chunk);

  if (need_chunk)  {
    if (local_allocator->lock_global_alloc()) {
      // Select target server on a round robin manner
      sid = (t_conn_id + cid) % server_num;

      // Allocate memory on the target server
      FAA(buffer, sid, 0, MRType::BUCKET, MEMORY_CHUNK_BUCKET_NUM, sink);

      Address head_addr(0, sid, *(uint64_t*)buffer);
      local_allocator->set_chunk(head_addr);

      local_allocator->unlock_global_alloc();

      // Retry
      bucket_addr = local_allocator->malloc(need_chunk);
    } else {
      yield_coroutine(sink);
      goto retry;
    }
  }

  return bucket_addr;
}

#ifdef VAR_VALUE_SIZE
// Allocate memory on the target server and return the offset
DataPtr Client::alloc_kv_data(void* buffer, CoroPull* sink) {
retry:
  bool need_chunk = true;
  Address kv_data_addr = local_allocator->malloc(need_chunk, false);

  if (need_chunk)  {
    if (local_allocator->lock_global_alloc(false)) {
      // Select target server on a round robin manner
      uint64_t sid = (t_conn_id + cid) % server_num;

      // Allocate memory on the target server
      FAA(buffer, sid, 0, MRType::KV_DATA, MEMORY_CHUNK_KV_DATA_NUM, sink);
      
      Address head_addr(0, sid, *(uint64_t*)buffer);
      local_allocator->set_chunk(head_addr, false);

      local_allocator->unlock_global_alloc(false);

      // Retry
      kv_data_addr = local_allocator->malloc(need_chunk, false);
    } else {
      yield_coroutine(sink);
      goto retry;
    }
  }

  // Pack address information
  DataPtr trans_kv_data_addr(kv_data_addr.sid, kv_data_addr.num);

  return trans_kv_data_addr;
}

void Client::write_kv_data(Value_t& value, CoroPull* sink) {
  void* u64_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(uint64_t));
  DataPtr kv_data_addr = alloc_kv_data(u64_buffer, sink);

  void* kv_data_buffer = buf[sink ? sink->get() : 0].alloc(KV_DATA_SIZE);
  KVData* kv_data = new (kv_data_buffer) KVData(value);

  write(kv_data_buffer, kv_data_addr.sid, KVData::get_kv_data_offset(kv_data_addr.num), MRType::KV_DATA, KV_DATA_SIZE, sink);
  value = (uint64_t)kv_data_addr;

  buf[sink ? sink->get() : 0].free(sizeof(uint64_t) + KV_DATA_SIZE);
}
#endif

bool Client::search_table(const Key_t& k, uint16_t tab_id, TableEntry& low_addr, TableEntry& hi_addr, CoroPull* sink) {
  uint64_t entered_addr = buf[sink ? sink->get() : 0].get_cur_addr();

  TableHeader tab_hdr = index_cache->get_tab_header(tab_id);
  uint64_t tab_idx = tab_hdr.get_target_block(k);

  uint16_t tab_sid = get_tab_sid(tab_id);
  uint64_t tab_start_offset = get_tab_start_offset(tab_sid, tab_id);

  TableEntry* tab_entry;
#ifndef TAB_PARTIAL_READ
  void* tab_buffer = buf[sink ? sink->get() : 0].alloc(tab_hdr.get_tab_size());

  bool consistent = false;
  while (!consistent) {
    read(tab_buffer, tab_sid, tab_start_offset+TableHeader::get_tab_offset(0), MRType::TABLE, tab_hdr.get_tab_size(), sink);
    tab_entry = (TableEntry*)tab_buffer;

    for (uint64_t i = 0; i < tab_hdr.get_capacity(); ++i) {
      if (!tab_entry[i].is_node_consistent(tab_hdr.get_version())) { // Table revamp occurred
        buf[sink ? sink->get() : 0].revert_to(entered_addr);
        return false;
      } else if (!tab_entry[i].is_entry_consistent() || // Table merge occurred
                (i < tab_hdr.get_capacity()-1 && tab_entry[i] == tab_entry[i+1] && tab_entry[i].get_dir_version() != tab_entry[i+1].get_dir_version()) || 
                (i < tab_hdr.get_capacity()-1 && tab_entry[i] != tab_entry[i+1] && tab_entry[i].get_next_dir_num() != tab_entry[i+1].get_dir_ptr().num)) {
        break;
      } else if (i == tab_hdr.get_capacity() - 1) {
        consistent = true;
      }
    }
  }

  if (tab_idx == tab_hdr.get_capacity()-1) {
    low_addr = tab_entry[tab_idx];
    hi_addr = tab_entry[tab_idx];
    hi_addr.dir_idx = DIR_CAPACITY - 1;
  } else {
    low_addr = tab_entry[tab_idx];
    hi_addr = tab_entry[tab_idx+1];
  }
#else
  void* tab_buffer = buf[sink ? sink->get() : 0].alloc(2*sizeof(TableEntry));

  if (tab_idx == tab_hdr.get_capacity() - 1) {
    do {
      read(tab_buffer, tab_sid, tab_start_offset+TableHeader::get_tab_offset(tab_idx), MRType::TABLE, sizeof(TableEntry), sink);
      tab_entry = (TableEntry*)tab_buffer;

      if (!tab_entry[0].is_node_consistent(tab_hdr.get_version())) { // Table revamp occurred
        buf[sink ? sink->get() : 0].revert_to(entered_addr);
        return false;
      }
    } while (false);

    low_addr = tab_entry[0];
    hi_addr = tab_entry[0];
    hi_addr.dir_idx = DIR_CAPACITY - 1;
  } else {
    do {
      read(tab_buffer, tab_sid, tab_start_offset+TableHeader::get_tab_offset(tab_idx), MRType::TABLE, 2*sizeof(TableEntry), sink);
      tab_entry = (TableEntry*)tab_buffer;

      if (!tab_entry[0].is_node_consistent(tab_hdr.get_version()) || 
          !tab_entry[1].is_node_consistent(tab_hdr.get_version())) { // Table revamp occurred
        buf[sink ? sink->get() : 0].revert_to(entered_addr);
        return false;
      }
    } while (!tab_entry[0].is_entry_consistent() || !tab_entry[1].is_entry_consistent() 
            || (tab_entry[0] == tab_entry[1] && tab_entry[0].get_dir_version() != tab_entry[1].get_dir_version())
            || (tab_entry[0] != tab_entry[1] && tab_entry[0].get_next_dir_num() != tab_entry[1].get_dir_ptr().num)); // Table merge occurred

    low_addr = tab_entry[0];
    hi_addr = tab_entry[1];
  }
#endif

  buf[sink ? sink->get() : 0].revert_to(entered_addr);
  return true;
}

/* Conduct range limited binary search on the directory.
   Find the entry with maximum value among entries with smaller values than the key. */
Address Client::search_directory(const Key_t& key, const CacheEntry* entry, TableEntry tab_entry, uint64_t low_idx, uint64_t hi_idx, int cache_flag, CoroPull* sink) {
  uint64_t entered_addr = buf[sink ? sink->get() : 0].get_cur_addr(); 
  void* u64_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(uint64_t)); 
  void* dir_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(Directory));

  Address bucket_addr = Address::null();

  if (cache_flag == 0) {
#ifndef DIR_PARTIAL_READ
    low_idx = 0;
    hi_idx = DIR_CAPACITY;
#endif

    uint64_t search_length = hi_idx - low_idx;
    DirectoryEntry* partial_dir = read_directory_partial(dir_buffer, tab_entry.get_dir_ptr(), low_idx, search_length, sink);

    // Table is stale
    if (tab_entry.get_dir_version() != partial_dir[0].get_version()) {
      return Address::null();
    }

    uint64_t low_idx = 0;
    uint64_t hi_idx = search_length;

    while (low_idx < hi_idx) {
      size_t mid = (low_idx + hi_idx) >> 1;

      // Since we do not know the directory capacity, skip searching entries with null value
      if (partial_dir[mid].ptr == Address::null() || key < partial_dir[mid].get_lowest()) {
        hi_idx = mid;
      } else {
        bucket_addr = partial_dir[mid].ptr;
        low_idx = mid + 1;
      }
    }
  } else {
    assert(cache_flag > 0); // Need to acquire lock first to update cache

    Directory* dir = read_directory_and_update_cache(dir_buffer, entry, tab_entry.get_dir_ptr(), entry == nullptr, sink);

    if (tab_entry.get_dir_version() != dir->get_version()) { // Table update is on progress
      low_idx = 0;
      hi_idx = dir->get_capacity();
    } else {
      hi_idx = hi_idx < dir->get_capacity() ? hi_idx : dir->get_capacity();
    }

    while (low_idx < hi_idx) {
      size_t mid = (low_idx + hi_idx) >> 1;

      if (key < dir->get_entry_lowest(mid)) {
        hi_idx = mid;
      } else {
        bucket_addr = dir->entry[mid].ptr;
        low_idx = mid + 1;
      }
    }
  }

  buf[sink ? sink->get() : 0].revert_to(entered_addr);
  return bucket_addr;
}

/* Conduct linear search on the stash directory.
   Find the entry with maximum value among entries with smaller values than the key. */
Address Client::search_stash(const Key_t& key, uint16_t sid, uint64_t offset, CoroPull* sink) {
#ifndef LOCK_FREE_STASH
  void* u64_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(uint64_t));
  lock_stash(u64_buffer, sid, offset, sink);
#endif

  void* stash_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(Stash));
  read(stash_buffer, sid, offset, MRType::STASH, sizeof(Stash), sink);
  Stash* stash = (Stash*)stash_buffer;

  uint64_t lower_bound = MIN;
  Address bucket_addr = Address::null();
  for (int i = 0; i < stash->get_entry_used(); ++i) {
    DirectoryEntry* e = stash->get_target_entry(i);
#ifndef LOCK_FREE_STASH
    if (false) {
#else
    if (!e->is_consistent(1)) {
#endif
      continue;
    } else if (e->get_lowest() <= key && e->get_lowest() > lower_bound) {
      bucket_addr = e->ptr;
      lower_bound = e->get_lowest();
    }
  }

#ifndef LOCK_FREE_STASH
  unlock_stash(u64_buffer, sid, offset, sink);
  buf[sink ? sink->get() : 0].free(sizeof(uint64_t));
#endif

  buf[sink ? sink->get() : 0].free(sizeof(Stash));
  return bucket_addr;
}

inline bool Client::is_consistent(DirectoryEntry *dir, uint64_t length) {
  uint64_t version = dir[0].get_version();

  for (uint64_t i = 1; i < length; ++i) {
    if (!dir[i].is_consistent(version)) {
      return false;
    }
  }

  return true;
}

/* Makes cache update decisions                           **
** -1 : Decided to update cache but entry lock failure    **
**  0 : Decided to dump                                   **
**  1 : Decided to update cache                           */
inline int Client::dump_or_update_cache(uint16_t tab_id, const CacheEntry* entry) {
  if (!index_cache->promote_dir_to_cache(entry->dir_addr, sizeof(CacheDirectory))) {
    return 0;
  }

  if (!entry || index_cache->lock_entry(entry)) {
    return 1;
  } else {
    return -1;
  }
}

inline Directory* Client::read_directory_and_update_cache(void* buffer, const CacheEntry* entry, Address dir_addr, bool new_entry, CoroPull* sink) {
  Directory* dir;

  do {
    read(buffer, dir_addr.sid, Directory::get_dir_hdr_offset(dir_addr.num), MRType::DIRECTORY, sizeof(Directory), sink);
    dir = (Directory*)buffer;
  } while (!dir->is_consistent());

  if (new_entry) {
    index_cache->add_to_cache(dir);
  } else {
    index_cache->update_and_unlock_to_cache(entry, dir);
  }

  return dir;
}

inline DirectoryEntry* Client::read_directory_partial(void* buffer, Address dir_addr, uint64_t s_idx, uint64_t length, CoroPull* sink) {
  DirectoryEntry* dir;

  do {
    read(buffer, dir_addr.sid, Directory::get_dir_offset(dir_addr.num, s_idx), MRType::DIRECTORY, length*sizeof(DirectoryEntry), sink);
    dir = (DirectoryEntry*)buffer;
  } while (!is_consistent(dir, length));

  return dir;
}

/* Find the target table of the given key. */
inline uint16_t Client::get_tab_id(const Key_t& key) {
  uint16_t tab_id = key / range_size;
  return tab_id < TOTAL_TAB_NUM ? tab_id : TOTAL_TAB_NUM - 1;
}

/* Find the server ID from the table ID. */
inline uint16_t Client::get_tab_sid(uint16_t tab_id) {
  return tab_id % server_num;
}

inline uint64_t Client::get_tab_start_offset(uint16_t sid, uint16_t tab_id) {
  return TableHeader::get_start_offset(table_per_mn, tab_id/server_num, t_conn->server_info_list[sid].base_size[MRType::TABLE]);
}

inline uint64_t Client::get_stash_start_offset(uint16_t sid, uint16_t tab_id) {
  return Stash::get_start_offset(table_per_mn, tab_id/server_num, t_conn->server_info_list[sid].base_size[MRType::STASH]);
}

inline RDMAInfo* Client::create_batch(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t size) {
  RDMAInfo* batch = new RDMAInfo;

  batch->rkey = t_conn->server_info_list[sid].mr_rkey[type];
  batch->src = (uint64_t)src;
  batch->dest = t_conn->server_info_list[sid].base_addr[type] + offset;
  batch->size = size;

  return batch;
}

inline void Client::yield_coroutine(CoroPull* sink) {
  if (sink != nullptr) {
    busy_waiting_queue.push(sink->get());
    (*sink)();
  }
}

void Client::init_bolt(void) {
  // Temporal thread register for index initialization
  register_thread(0);

  // Table is allocated to server in a round robin manner
  for (uint16_t tab_id = 0; tab_id < TOTAL_TAB_NUM; ++tab_id) {
    uint16_t sid = get_tab_sid(tab_id);
    uint64_t range_min = tab_id*range_size;
    uint64_t range_max = tab_id == TOTAL_TAB_NUM - 1 ? MAX : (tab_id + 1)*range_size - 1;

    // Allocate bucket
    void* u64_buffer = buf[0].alloc(sizeof(uint64_t));
    Address bucket_addr = alloc_bucket(u64_buffer, sid, nullptr);

    // Initialize bucket
    void* bucket_buffer = buf[0].alloc(sizeof(Bucket));
    struct Bucket* bucket = new (bucket_buffer) Bucket(range_min, range_max);
    write(bucket_buffer, bucket_addr.sid, Bucket::get_bucket_offset(bucket_addr.num), MRType::BUCKET, sizeof(Bucket), nullptr);

    // Allocate directory
    void* alloc_num_buffer = buf[0].alloc(sizeof(uint64_t));
    FAA(alloc_num_buffer, sid, 0, MRType::DIRECTORY, 1, nullptr);
    uint64_t* alloc_num = (uint64_t*)alloc_num_buffer;

    void* dir_addr_buffer = buf[0].alloc(sizeof(TableEntry));
    TableEntry* dir_addr = new (dir_addr_buffer) TableEntry(sid, *alloc_num);

    // Initialize directory
    void* dir_buffer = buf[0].alloc(sizeof(Directory));
    struct Directory* dir = new (dir_buffer) Directory(dir_addr->get_dir_ptr());
    
    dir->entry[0] = DirectoryEntry(bucket->get_lowest(), bucket_addr);
    dir->set_consistent(0);

    write(dir_buffer, dir_addr->get_dir_ptr().sid, Directory::get_dir_hdr_offset(dir_addr->get_dir_ptr().num), MRType::DIRECTORY, sizeof(Directory), nullptr);

    // Initialize table
    void* tab_hdr_buffer = buf[0].alloc(sizeof(TableHeader));
    struct TableHeader* tab_hdr = new (tab_hdr_buffer) TableHeader(tab_id, range_min, range_max);
    uint64_t tab_start_offset = get_tab_start_offset(sid, tab_id);
    write(tab_hdr_buffer, sid, tab_start_offset, MRType::TABLE, sizeof(TableHeader), nullptr);
    write(dir_addr_buffer, sid, tab_start_offset+TableHeader::get_tab_offset(0), MRType::TABLE, sizeof(TableEntry), nullptr);
    
    // Initialize stash
    void* stash_buffer = buf[0].alloc(sizeof(Stash));
    write(stash_buffer, sid, get_stash_start_offset(sid, tab_id), MRType::STASH, sizeof(Stash), nullptr);

    buf[0].clear();
  }

  send_signal(SignalType::INIT_DONE);
  reset_thread();
}

/* Initializes the cache. */
void Client::init_cache(void) {
  // Temporal thread register for cache initialization
  register_thread(0);
  wait_signal(SignalType::START);

  assert(cache_size > 4 * LOCAL_LOCK_NUM); // SMART insists 32-bit local locks
#ifdef SPECULATIVE_READ
  if (cache_size > 4 * LOCAL_LOCK_NUM + HOTSPOTBUFFER_SIZE + 20 * 1024 * 1024) {
    index_cache = new Cache(TOTAL_TAB_NUM, cache_size - 4 * LOCAL_LOCK_NUM - HOTSPOTBUFFER_SIZE, thread_num);
    key_cache = new IdxCache(HOTSPOTBUFFER_SIZE);
  } else {
    index_cache = new Cache(TOTAL_TAB_NUM, cache_size - 4 * LOCAL_LOCK_NUM, thread_num);
    key_cache = new IdxCache(0);
  }
#else
  index_cache = new Cache(TOTAL_TAB_NUM, cache_size - 4 * LOCAL_LOCK_NUM, thread_num);
#endif

  // Cache table
  for (uint16_t tab_id = 0; tab_id < TOTAL_TAB_NUM; ++tab_id) {
    uint16_t sid = get_tab_sid(tab_id);
    uint64_t tab_start_offset = get_tab_start_offset(sid, tab_id);

    void* tab_hdr_buffer = buf[0].alloc(sizeof(TableHeader));
    read(tab_hdr_buffer, sid, tab_start_offset, MRType::TABLE, sizeof(TableHeader), nullptr);
    TableHeader* tab_hdr = (TableHeader*)tab_hdr_buffer;
    assert(tab_hdr->get_capacity() > 0);

    void* tab_buffer = buf[0].alloc(tab_hdr->get_tab_size());
    read(tab_buffer, sid, tab_start_offset+TableHeader::get_tab_offset(0), MRType::TABLE, tab_hdr->get_tab_size(), nullptr);

    index_cache->update_tab_to_cache(tab_id, tab_hdr, tab_buffer);
  }

  buf[0].clear();

  fprintf(stdout, "[NOTICE] Read %lu table into cache\n", TOTAL_TAB_NUM);

  reset_thread();  
}

void Client::reset_cache(void) {
  delete index_cache;
  index_cache = new Cache(TOTAL_TAB_NUM, cache_size, thread_num);
  fprintf(stdout, "[NOTICE] Cleared cache\n");

  // Cache table
  for (uint16_t tab_id = 0; tab_id < TOTAL_TAB_NUM; ++tab_id) {
    uint16_t sid = get_tab_sid(tab_id);
    uint64_t tab_start_offset = get_tab_start_offset(sid, tab_id);

    void* tab_hdr_buffer = buf[0].alloc(sizeof(TableHeader));
    read(tab_hdr_buffer, sid, tab_start_offset, MRType::TABLE, sizeof(TableHeader), nullptr);
    TableHeader* tab_hdr = (TableHeader*)tab_hdr_buffer;
    assert(tab_hdr->get_capacity() > 0);

    void* tab_buffer = buf[0].alloc(tab_hdr->get_tab_size());
    read(tab_buffer, sid, tab_start_offset+TableHeader::get_tab_offset(0), MRType::TABLE, tab_hdr->get_tab_size(), nullptr);

    index_cache->update_tab_to_cache(tab_id, tab_hdr, tab_buffer);
  }

  buf[0].clear();

  fprintf(stdout, "[NOTICE] Read %lu table into cache\n", TOTAL_TAB_NUM);
}

void Client::run_coroutine(CoroFunc func, uint16_t coro_cnt, WorkFunc work_func, int phase, const std::vector<Request>& requests) {
  using namespace std::placeholders;

  for (int i = 0; i < MAX_CORO_NUM; ++i) {
    coroutine_done[i] = false;
  }
  workers.clear();

  assert(coro_cnt <= MAX_CORO_NUM);
  for (uint16_t i = 0; i < coro_cnt; ++i) {
    auto gen = func(i, coro_cnt, requests);
    workers.emplace_back([=](CoroPull& sink) {
      coro_worker(sink, gen, i, work_func, phase);
    });
  }

  for (int i = 0; i < coro_cnt; ++i) {
    workers[i](i);
  }

  while (true) {
    uint64_t next_coro_id;

    if (rdma_poll_once_with_id(t_conn->ib_cq, next_coro_id)) {
      workers[next_coro_id](next_coro_id);
    }

    if (!busy_waiting_queue.empty()) {
      auto next_coro_id = busy_waiting_queue.front();
      busy_waiting_queue.pop();
      workers[next_coro_id](next_coro_id);
    }

    for (uint16_t i = 0; i < coro_cnt; ++i) {
      if (!coroutine_done[i]) {
        break;
      } else if (i == coro_cnt-1) {
        return;
      }
    }
  }
}

void Client::run_router_coroutine(CoroRouterFunc func, uint16_t coro_cnt, WorkFunc work_func, int phase, Queue* gq) {
  using namespace std::placeholders;

  for (int i = 0; i < MAX_CORO_NUM; ++i) {
    coroutine_done[i] = false;
  }
  workers.clear();

  assert(coro_cnt <= MAX_CORO_NUM);
  for (uint16_t i = 0; i < coro_cnt; ++i) {
    auto gen = func(gq);
    workers.emplace_back([=](CoroPull& sink) {
      coro_router_worker(sink, gen, i, work_func, phase);
    });
  }

  for (int i = 0; i < coro_cnt; ++i) {
    workers[i](i);
  }

  while (true) {
    uint64_t next_coro_id;

    if (rdma_poll_once_with_id(t_conn->ib_cq, next_coro_id)) {
      workers[next_coro_id](next_coro_id);
    }

    if (!busy_waiting_queue.empty()) {
      auto next_coro_id = busy_waiting_queue.front();
      busy_waiting_queue.pop();
      workers[next_coro_id](next_coro_id);
    }

    for (uint16_t i = 0; i < coro_cnt; ++i) {
      if (!coroutine_done[i]) {
        break;
      } else if (i == coro_cnt-1) {
        return;
      }
    }
  }
}

void Client::coro_worker(CoroPull &sink, RequstGen *gen, uint16_t coro_id, WorkFunc work_func, int phase) {
  while (true) {
    auto r = gen->next();
    work_func(this, r.value(), phase, t_conn_id, &sink, coro_id);
    if (gen->is_finished()) {
      break;
    }

    busy_waiting_queue.push(sink.get());
    sink();
  }

  coroutine_done[coro_id] = true;
}

void Client::coro_router_worker(CoroPull &sink, RequstGen *gen, uint16_t coro_id, WorkFunc work_func, int phase) {
  while (true) {
    auto r = gen->next();
    if (r.has_value()) {
      work_func(this, r.value(), phase, t_conn_id, &sink, coro_id);
    } else if (gen->is_finished()) {
      break;
    }

    busy_waiting_queue.push(sink.get());
    sink();
  }

  coroutine_done[coro_id] = true;
}

void Client::insert(const Key_t& key, Value_t value, CoroPull* sink) {
  int retry_num = 0;
  RetryType retry_flag;

  bool write_handover = false;
  std::pair<bool, bool> lock_res = std::make_pair(false, false);
  lock_res = local_lock_table->acquire_local_write_lock(key, value, &busy_waiting_queue, sink);
  write_handover = (lock_res.first && !lock_res.second);
  if (write_handover) {
    local_lock_table->release_local_write_lock(key, lock_res);
    return;
  }

  uint16_t tab_id = get_tab_id(key);

RETRY_INSERT:
  if (retry_num > 1000000) {
    fprintf(stdout, "[CAUTION] Thread %lu fallen into infinite loop with flag %d \n", t_conn_id, retry_flag);
    assert(false);
  }

  hit_depth[sink ? sink->get() : 0][0] = 0;
  miss_depth[sink ? sink->get() : 0][0] = 0;

  buf[sink ? sink->get() : 0].clear();
  check_tab_update(tab_id, sink);

  void* u64_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(uint64_t));
  void* unlock_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(uint64_t));
  
  Address dir_addr;
  TableEntry low_addr, hi_addr;
  if (!index_cache->search_tab_from_cache(key, tab_id, low_addr, hi_addr, t_conn_id)) {
    miss_depth[sink ? sink->get() : 0][0]++;
    if (!search_table(key, tab_id, low_addr, hi_addr, sink)) {
      retry_flag = RetryType::TAB_META_STALE;
      goto RETRY_INSERT;
    }
  } else {
    hit_depth[sink ? sink->get() : 0][0]++;
  }

  // Search from cache
  bool uncertain = false;
  Address bucket_addr = Address::null();
  const CacheEntry* entry = index_cache->search_from_cache(key, low_addr, hi_addr, &bucket_addr, uncertain, t_conn_id);

  bool read_low = false;
  bool read_hi = false;
  if (!entry) { // low_addr, hi_addr both new to cache
    assert(uncertain);
    read_low = true;
    read_hi = true;
  } else {
    if (bucket_addr == Address::null()) { // Directory stale
      if (low_addr.get_dir_ptr() == entry->dir_addr) { // low_addr stale
        read_low = true;
      } else { // hi_addr stale
        read_hi = true;
      }

      index_cache->invalidate(entry);
    }

    if (uncertain) { // Need to check hi_addr 
      assert((low_addr.get_dir_ptr() != hi_addr.get_dir_ptr()) && (low_addr.get_dir_ptr() == entry->dir_addr));
      read_hi = true;
    }
  }
  
  if (read_low || read_hi) {
    if (read_low && !read_hi) { // low_addr certain but stale
      assert(entry && (low_addr.get_dir_ptr() == entry->dir_addr) && !uncertain);

      int cache_flag = dump_or_update_cache(tab_id, entry);
      if (cache_flag < 0) {
        retry_flag = RetryType::WAITING_DIR_UPDATE;
        yield_coroutine(sink);
        goto RETRY_INSERT;
      }

      uint64_t low_idx = low_addr.get_dir_idx();
      uint64_t hi_idx = low_addr == hi_addr ? hi_addr.get_dir_idx() + 1 : DIR_CAPACITY;
      bucket_addr = search_directory(key, entry, low_addr, low_idx, hi_idx, cache_flag, sink);
      dir_addr = low_addr.get_dir_ptr();
    } else if (!read_low && read_hi) { // Either 1) low_addr fresh but uncertain or 2) hi_addr certain but stale
      assert(entry);
      
      if (low_addr.get_dir_ptr() == entry->dir_addr) {
        assert(uncertain);

        Address hi_bucket_addr = search_directory(key, nullptr, hi_addr, 0, hi_addr.get_dir_idx() + 1, 1, sink);
        if (hi_bucket_addr == Address::null()) {
          dir_addr = low_addr.get_dir_ptr();
        } else {
          bucket_addr = hi_bucket_addr;
          dir_addr = hi_addr.get_dir_ptr();
        }
      } else {
        assert(hi_addr.get_dir_ptr() == entry->dir_addr && !uncertain);

        int cache_flag = dump_or_update_cache(tab_id, entry);
        if (cache_flag < 0) {
          retry_flag = RetryType::WAITING_DIR_UPDATE;
          yield_coroutine(sink);
          goto RETRY_INSERT;
        }

        bucket_addr = search_directory(key, entry, hi_addr, 0, hi_addr.get_dir_idx() + 1, cache_flag, sink);
        dir_addr = hi_addr.get_dir_ptr();
      }
    } else { // Either 1) both new to cache or 2) low_addr stale and uncertain
      assert(uncertain && (!entry || (low_addr.get_dir_ptr() == entry->dir_addr)));

      int cache_flag = 1;
      if (entry) {
        cache_flag = dump_or_update_cache(tab_id, entry);
        if (cache_flag < 0) {
          retry_flag = RetryType::WAITING_DIR_UPDATE;
          yield_coroutine(sink);
          goto RETRY_INSERT;
        }
      }

      uint64_t low_idx = low_addr.get_dir_idx();
      uint64_t hi_idx = low_addr == hi_addr ? hi_addr.get_dir_idx() + 1 : DIR_CAPACITY;
      Address low_bucket_addr = search_directory(key, entry, low_addr, low_idx, hi_idx, cache_flag, sink);

      Address hi_bucket_addr = Address::null();
      if (low_addr != hi_addr) {
        low_idx = 0;
        hi_idx = hi_addr.get_dir_idx() + 1;
        hi_bucket_addr = search_directory(key, nullptr, hi_addr, low_idx, hi_idx, 1, sink);
      }

      if (hi_bucket_addr == Address::null()) {
        bucket_addr = low_bucket_addr;
        dir_addr = low_addr.get_dir_ptr();
      } else {
        bucket_addr = hi_bucket_addr;
        dir_addr = hi_addr.get_dir_ptr();
      }
    }

    miss_depth[sink ? sink->get() : 0][0]++;
  } else {
    dir_addr = entry->dir_addr;
    hit_depth[sink ? sink->get() : 0][0]++;
  }

  if (bucket_addr == Address::null()) {
    retry_num++;
    retry_flag = RetryType::TAB_ENTRY_STALE;
    yield_coroutine(sink);
    goto RETRY_INSERT;
  }

  assert(get_tab_sid(tab_id) == dir_addr.sid);

  uint64_t lock_index = CityHash64((char *)&bucket_addr, sizeof(bucket_addr)) % ON_CHIP_LOCK_NUM;
  Address lock_addr(0, bucket_addr.sid, lock_index);
  lock_bucket(u64_buffer, lock_addr, sink);

  int loc;
  void* bucket_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(Bucket));
  Bucket* bucket = (Bucket*)bucket_buffer;

#ifdef SPECULATIVE_READ
  if (speculative_read(bucket, bucket_addr, key, value, loc, true, sink)) {
    local_lock_table->get_combining_value(key, value);

#ifdef VAR_VALUE_SIZE
    write_kv_data(value, sink);
#endif

    bucket->slot[loc].value = value;
    bucket->slot[loc].set_entry_version_consistent();
    goto END_INSERT;
  }
#endif
  
  read_bucket(bucket_buffer, bucket_addr, true, sink);

  if (key < bucket->get_lowest() || key > bucket->get_highest()) {
    unlock_bucket(unlock_buffer, lock_addr, sink);
    bucket_addr = search_stash(key, dir_addr.sid, get_stash_start_offset(dir_addr.sid, tab_id), sink);

    lock_index = CityHash64((char *)&bucket_addr, sizeof(bucket_addr)) % ON_CHIP_LOCK_NUM;
    lock_addr.sid = bucket_addr.sid;
    lock_addr.num = lock_index;

    if (bucket_addr != Address::null()) {
      lock_bucket(u64_buffer, lock_addr, sink);

#ifdef SPECULATIVE_READ
      if (speculative_read(bucket, bucket_addr, key, value, loc, true, sink)) {
        local_lock_table->get_combining_value(key, value);

#ifdef VAR_VALUE_SIZE
        write_kv_data(value, sink);
#endif

        bucket->slot[loc].value = value;
        bucket->slot[loc].set_entry_version_consistent();
        goto END_INSERT;
      }
#endif

      read_bucket(bucket_buffer, bucket_addr, true, sink);

      if (key < bucket->get_lowest() || key > bucket->get_highest()) {
        unlock_bucket(unlock_buffer, lock_addr, sink);

        if (!read_low && !read_hi) {
          index_cache->invalidate(entry);
        }

        retry_num++;
        retry_flag = RetryType::FOUND_WRONG_BUCKET;
        goto RETRY_INSERT;
      }
    } else {
      if (!read_low && !read_hi) {
        index_cache->invalidate(entry);
      }

      retry_num++;
      retry_flag = RetryType::STASH_SEARCH_FAILURE;
      goto RETRY_INSERT;
    }
  }

  local_lock_table->get_combining_value(key, value);

#ifdef VAR_VALUE_SIZE
  write_kv_data(value, sink);
#endif

  if (bucket->insert(key, value, loc)) {
END_INSERT:
#ifdef SPECULATIVE_READ
    key_cache->add_to_cache(bucket_addr, loc, key);
#endif
    write_and_unlock_bucket(&bucket->slot[loc], bucket_addr.sid, Bucket::get_bucket_offset(bucket_addr.num)+Bucket::get_pair_offset(loc),
                            sizeof(Pair), unlock_buffer, lock_addr, sink);
    local_lock_table->release_local_write_lock(key, lock_res);
    cache_hit[t_conn_id][0] += hit_depth[sink ? sink->get() : 0][0];
    cache_miss[t_conn_id][0] += miss_depth[sink ? sink->get() : 0][0];
    return;
  }

  // Split bucket
  void* new_bucket_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(Bucket));
  Bucket* new_bucket = bucket->split_and_insert(new_bucket_buffer, key, value);

  Address new_bucket_addr = alloc_bucket(u64_buffer, dir_addr.sid, sink);
  write(new_bucket_buffer, new_bucket_addr.sid, Bucket::get_bucket_offset(new_bucket_addr.num), MRType::BUCKET, sizeof(Bucket), sink);

  uint64_t stash_offset = get_stash_start_offset(dir_addr.sid, tab_id);
RETRY_SMO:
#ifndef LOCK_FREE_STASH
  lock_stash(u64_buffer, dir_addr.sid, stash_offset, sink);

  void* stash_hdr_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(StashHeader));
  read(stash_hdr_buffer, dir_addr.sid, stash_offset, MRType::STASH, sizeof(StashHeader), sink);

  StashHeader* stash_hdr = (StashHeader*)stash_hdr_buffer;

  // Wait until background merge is done
  if (stash_hdr->full()) {
    unlock_stash(u64_buffer, dir_addr.sid, stash_offset, sink);
    buf[sink ? sink->get() : 0].free(sizeof(StashHeader));
    stash_stall_num[t_conn_id][0]++;
    goto RETRY_SMO;
  }
#else
  if (!alloc_stash(u64_buffer, dir_addr.sid, stash_offset, sink)) {
    goto RETRY_SMO;
  }
#endif

  // Allocate space for new entry 
  void* entry_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(DirectoryEntry));
  DirectoryEntry* e = new (entry_buffer) DirectoryEntry(new_bucket->get_lowest(), new_bucket_addr);

#ifndef LOCK_FREE_STASH
  uint64_t entry_offset = Stash::get_stash_offset(stash_hdr->transpose_idx(stash_hdr->entry_used));

  stash_hdr->entry_used++;

  Batch stash_batch(dir_addr.sid, 2);
  stash_batch.add(create_batch(entry_buffer, dir_addr.sid, stash_offset+entry_offset,
                  MRType::STASH, sizeof(DirectoryEntry)));
  stash_batch.add(create_batch(stash_hdr_buffer, dir_addr.sid, stash_offset,
                  MRType::STASH, sizeof(StashHeader)));

  batched_write(&stash_batch, sink);
  write(bucket_buffer, bucket_addr.sid, Bucket::get_bucket_offset(bucket_addr.num), MRType::BUCKET, sizeof(Bucket), sink);
  
  unlock_stash(u64_buffer, dir_addr.sid, stash_offset, sink);
  unlock_bucket(unlock_buffer, lock_addr, sink);
#else
  e->set_consistent(1);

  // Append entry to stash
  write(entry_buffer, dir_addr.sid, stash_offset, MRType::STASH, sizeof(DirectoryEntry), sink);

  // Write back original bucket and unlock it
  write_and_unlock_bucket(bucket_buffer, bucket_addr.sid, Bucket::get_bucket_offset(bucket_addr.num),
                          sizeof(Bucket), unlock_buffer, lock_addr, sink);
#endif

  local_lock_table->release_local_write_lock(key, lock_res);
  cache_hit[t_conn_id][0] += hit_depth[sink ? sink->get() : 0][0];
  cache_miss[t_conn_id][0] += miss_depth[sink ? sink->get() : 0][0];
  return;
}

Value_t Client::search(const Key_t& key, CoroPull* sink) {
  int retry_num = 0;
  RetryType retry_flag;

  Value_t value;
  bool search_res = false;
  std::pair<bool, bool> lock_res = std::make_pair(false, false);
  bool read_handover = false;
  lock_res = local_lock_table->acquire_local_read_lock(key, &busy_waiting_queue, sink);
  read_handover = (lock_res.first && !lock_res.second);
  if (read_handover) {
    local_lock_table->release_local_read_lock(key, lock_res, search_res, value);
    return value;
  }

  value = INVALID_VAL;
  uint16_t tab_id = get_tab_id(key);

RETRY_SEARCH:
  if (retry_num > 1000000) {
    fprintf(stdout, "[CAUTION] Thread %lu fallen into infinite loop with flag %d \n", t_conn_id, retry_flag);
    assert(false);
  }

  hit_depth[sink ? sink->get() : 0][0] = 0;
  miss_depth[sink ? sink->get() : 0][0] = 0;

  buf[sink ? sink->get() : 0].clear();
  check_tab_update(tab_id, sink);

  void* u64_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(uint64_t));
  
  Address dir_addr;
  TableEntry low_addr, hi_addr;
  if (!index_cache->search_tab_from_cache(key, tab_id, low_addr, hi_addr, t_conn_id)) {
    miss_depth[sink ? sink->get() : 0][0]++;
    if (!search_table(key, tab_id, low_addr, hi_addr, sink)) {
      retry_flag = RetryType::TAB_META_STALE;
      goto RETRY_SEARCH;
    }
  } else {
    hit_depth[sink ? sink->get() : 0][0]++;
  }

  // Search from cache
  bool uncertain = false;
  Address bucket_addr = Address::null();
  const CacheEntry* entry = index_cache->search_from_cache(key, low_addr, hi_addr, &bucket_addr, uncertain, t_conn_id);

  bool read_low = false;
  bool read_hi = false;
  if (!entry) { // low_addr, hi_addr both new to cache
    assert(uncertain);
    read_low = true;
    read_hi = true;
  } else {
    if (bucket_addr == Address::null()) { // Directory stale
      if (low_addr.get_dir_ptr() == entry->dir_addr) { // low_addr stale
        read_low = true;
      } else { // hi_addr stale
        read_hi = true;
      }

      index_cache->invalidate(entry);
    }

    if (uncertain) { // Need to check hi_addr 
      assert((low_addr.get_dir_ptr() != hi_addr.get_dir_ptr()) && (low_addr.get_dir_ptr() == entry->dir_addr));
      read_hi = true;
    }
  }
  
  if (read_low || read_hi) {
    if (read_low && !read_hi) { // low_addr certain but stale
      assert(entry && (low_addr.get_dir_ptr() == entry->dir_addr) && !uncertain);

      int cache_flag = dump_or_update_cache(tab_id, entry);
      if (cache_flag < 0) {
        retry_flag = RetryType::WAITING_DIR_UPDATE;
        yield_coroutine(sink);
        goto RETRY_SEARCH;
      }

      uint64_t low_idx = low_addr.get_dir_idx();
      uint64_t hi_idx = low_addr == hi_addr ? hi_addr.get_dir_idx() + 1 : DIR_CAPACITY;
      bucket_addr = search_directory(key, entry, low_addr, low_idx, hi_idx, cache_flag, sink);
      dir_addr = low_addr.get_dir_ptr();
    } else if (!read_low && read_hi) { // Either 1) low_addr fresh but uncertain or 2) hi_addr certain but stale
      assert(entry);
      
      if (low_addr.get_dir_ptr() == entry->dir_addr) {
        assert(uncertain);

        Address hi_bucket_addr = search_directory(key, nullptr, hi_addr, 0, hi_addr.get_dir_idx() + 1, 1, sink);
        if (hi_bucket_addr == Address::null()) {
          dir_addr = low_addr.get_dir_ptr();
        } else {
          bucket_addr = hi_bucket_addr;
          dir_addr = hi_addr.get_dir_ptr();
        }
      } else {
        assert(hi_addr.get_dir_ptr() == entry->dir_addr && !uncertain);

        int cache_flag = dump_or_update_cache(tab_id, entry);
        if (cache_flag < 0) {
          retry_flag = RetryType::WAITING_DIR_UPDATE;
          yield_coroutine(sink);
          goto RETRY_SEARCH;
        }

        bucket_addr = search_directory(key, entry, hi_addr, 0, hi_addr.get_dir_idx() + 1, cache_flag, sink);
        dir_addr = hi_addr.get_dir_ptr();
      }
    } else { // Either 1) both new to cache or 2) low_addr stale and uncertain
      assert(uncertain && (!entry || (low_addr.get_dir_ptr() == entry->dir_addr)));

      int cache_flag = 1;
      if (entry) {
        cache_flag = dump_or_update_cache(tab_id, entry);
        if (cache_flag < 0) {
          retry_flag = RetryType::WAITING_DIR_UPDATE;
          yield_coroutine(sink);
          goto RETRY_SEARCH;
        }
      }

      uint64_t low_idx = low_addr.get_dir_idx();
      uint64_t hi_idx = low_addr == hi_addr ? hi_addr.get_dir_idx() + 1 : DIR_CAPACITY;
      Address low_bucket_addr = search_directory(key, entry, low_addr, low_idx, hi_idx, cache_flag, sink);

      Address hi_bucket_addr = Address::null();
      if (low_addr != hi_addr) {
        low_idx = 0;
        hi_idx = hi_addr.get_dir_idx() + 1;
        hi_bucket_addr = search_directory(key, nullptr, hi_addr, low_idx, hi_idx, 1, sink);
      }

      if (hi_bucket_addr == Address::null()) {
        bucket_addr = low_bucket_addr;
        dir_addr = low_addr.get_dir_ptr();
      } else {
        bucket_addr = hi_bucket_addr;
        dir_addr = hi_addr.get_dir_ptr();
      }
    }

    miss_depth[sink ? sink->get() : 0][0]++;
  } else {
    dir_addr = entry->dir_addr;
    hit_depth[sink ? sink->get() : 0][0]++;
  }

  if (bucket_addr == Address::null()) {
    retry_num++;
    retry_flag = RetryType::TAB_ENTRY_STALE;
    yield_coroutine(sink);
    goto RETRY_SEARCH;
  }

  assert(get_tab_sid(tab_id) == dir_addr.sid);

  int loc;
  void* bucket_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(Bucket));
  Bucket* bucket = (Bucket*)bucket_buffer;

#ifdef SPECULATIVE_READ
  if (speculative_read(bucket, bucket_addr, key, value, loc, false, sink)) {
    value = bucket->slot[loc].value;
    goto END_SEARCH;
  }
#endif

  read_bucket(bucket_buffer, bucket_addr, false, sink);

  if (key < bucket->get_lowest() || key > bucket->get_highest()) {
    bucket_addr = search_stash(key, dir_addr.sid, get_stash_start_offset(dir_addr.sid, tab_id), sink);
    if (bucket_addr != Address::null()) {
#ifdef SPECULATIVE_READ
      if (speculative_read(bucket, bucket_addr, key, value, loc, false, sink)) {
        value = bucket->slot[loc].value;
        goto END_SEARCH;
      }
#endif

      read_bucket(bucket_buffer, bucket_addr, false, sink);
      if (key < bucket->get_lowest() || key > bucket->get_highest()) {
        if (!read_low && !read_hi) {
          index_cache->invalidate(entry);
        }

        retry_num++;
        retry_flag = RetryType::FOUND_WRONG_BUCKET;
        goto RETRY_SEARCH;
      }     
    } else {
      if (!read_low && !read_hi) {
        index_cache->invalidate(entry);
      }

      retry_num++;
      retry_flag = RetryType::STASH_SEARCH_FAILURE;
      goto RETRY_SEARCH;
    }
  }

  value = bucket->search(key, loc);

END_SEARCH:
  search_res = (value != INVALID_VAL);
  if (search_res) {
#ifdef SPECULATIVE_READ
    key_cache->add_to_cache(bucket_addr, loc, key);
#endif

#ifdef VAR_VALUE_SIZE
    uint64_t kv_data_sid = ((DataPtr *)&value)->sid;
    uint64_t kv_data_num = ((DataPtr *)&value)->num;

    void* kv_data_buffer = buf[sink ? sink->get() : 0].alloc(KV_DATA_SIZE);
    read(kv_data_buffer, kv_data_sid, KVData::get_kv_data_offset(kv_data_num), MRType::KV_DATA, KV_DATA_SIZE, sink);

    value = ((KVData*)kv_data_buffer)->value;
    buf[sink ? sink->get() : 0].free(KV_DATA_SIZE);
#endif
  }

  local_lock_table->release_local_read_lock(key, lock_res, search_res, value);
  cache_hit[t_conn_id][0] += hit_depth[sink ? sink->get() : 0][0];
  cache_miss[t_conn_id][0] += miss_depth[sink ? sink->get() : 0][0];

  return value;
}

/* Assumes that 1) all directories are cached and 2) there exists no concurrent write/update operations. */
uint64_t Client::range_query(const Key_t &from, const Key_t &to, Value_t* res, CoroPull* sink) {
#ifdef VAR_VALUE_SIZE
  // Scan operation currently does not support variable key value size
  assert(false);
#endif

  int retry_num = 0;
  RetryType retry_flag;

  uint16_t from_tab_id = get_tab_id(from);
  uint16_t to_tab_id = get_tab_id(to);

RETRY_RANGE:
  if (retry_num > 1000000) {
    fprintf(stdout, "[CAUTION] Thread %lu fallen into infinite loop with flag %d \n", t_conn_id, retry_flag);
    assert(false);
  }

  buf[sink ? sink->get() : 0].clear();
  for (uint16_t i = from_tab_id; i < to_tab_id; ++i) {
    check_tab_update(i, sink);
  }

  void* u64_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(uint64_t));
  
  uint64_t found = 0;
  struct ScanAddress from_addr, to_addr;

  // 1. Get address from cache
  from_addr.tab_id = from_tab_id;
  if (!index_cache->search_tab_from_cache(from, from_addr.tab_id, from_addr.low, from_addr.hi, t_conn_id)) {
    if (!search_table(from, from_addr.tab_id, from_addr.low, from_addr.hi, sink)) {
      retry_flag = RetryType::TAB_META_STALE;
      goto RETRY_RANGE;
    }
  }

  to_addr.tab_id = to_tab_id;
  if (!index_cache->search_tab_from_cache(to, to_addr.tab_id, to_addr.low, to_addr.hi, t_conn_id)) {
    if (!search_table(to, to_addr.tab_id, to_addr.low, to_addr.hi, sink)) {
      retry_flag = RetryType::TAB_META_STALE;
      goto RETRY_RANGE;
    }
  }

  std::vector<Address> bucket_addr;

  // 2. Search directories from cache
  const std::vector<CacheEntry*> entry = index_cache->search_range_from_cache(from, to, from_addr, to_addr, bucket_addr, t_conn_id);
 
  void* bucket_buffer = buf[sink ? sink->get() : 0].alloc(sizeof(Bucket));

  for (Address addr : bucket_addr) {
    if (addr != Address::null()) {
      Bucket* bucket = read_bucket(bucket_buffer, addr, false, sink);
      bucket->range_query(from, to, found, res); 
    }
  }

  return found;
}

void Client::reset_thread(void) {
  delete buf;

  t_conn = NULL;
  buf = NULL;
  local_allocator = NULL;
}

void Client::register_thread(uint64_t tid) {
  t_conn_id = tid;
  t_numa_id = numa_node_of_cpu(map_thread_to_logical_cpu_rr(t_conn_id));
  t_conn = t_conns[t_conn_id];
  local_allocator = local_allocators[t_conn_id];

  // Divide the memory region for each coroutine
  uint64_t buf_size = t_conn->base_size / coroutine_num;

  buf = new Buffer[coroutine_num];
  for (int i = 0; i < coroutine_num; ++i) {
    buf[i].init_buffer(buf_size, t_conn->base_addr + i*buf_size);
  }
}

void Client::clear_statistics(void) {
  for (int i = 0; i < MAX_THREAD_NUM; ++i) {
    rdma_write_num[i][0] = 0;
    rdma_write_size[i][0] = 0;
    rdma_read_num[i][0] = 0;
    rdma_read_size[i][0] = 0;
    rdma_atomic_num[i][0] = 0;
    rdma_atomic_size[i][0] = 0;

    cache_hit[i][0] = 0;
    cache_miss[i][0] = 0;

    stash_stall_num[i][0] = 0;
  }
}

void Client::send_signal(SignalType sig_type) {
  uint64_t sig_buffer = t_conns[thread_num]->get_send_buffer(MASTER_SERVER_ID);

  struct Signal* sig = (Signal *)sig_buffer;
  sig->id = cid;
  sig->signal = (uint8_t)sig_type;
  
  while (true) {
    rdma_post_send(t_conns[thread_num]->ib_qp_list[MASTER_SERVER_ID], sig_buffer,
                   sizeof(Signal), t_conns[thread_num]->ib_mr->lkey, MASTER_SERVER_ID);

    if (rdma_poll_completion(t_conns[thread_num]->ib_cq)) {
      break;
    } else {
      fprintf(stderr, "[CAUTION] Retry sending signal to master server (error: %s)\n", strerror(errno));
    }
  }
}

void Client::broadcast_signal(SignalType sig_type) {
  fprintf(stdout, "[NOTICE] Broadcasting signal %d to %u servers\n", sig_type, server_num);

  // Broadcast to all servers
  for (uint16_t i = 0; i < server_num; ++i) {
    uint64_t sig_buffer = t_conns[thread_num]->get_send_buffer(i);

    struct Signal* sig = (Signal *)sig_buffer;
    sig->id = cid;
    sig->signal = (uint8_t)sig_type;

    while (true) {
      rdma_post_send(t_conns[thread_num]->ib_qp_list[i], sig_buffer,
                     sizeof(Signal), t_conns[thread_num]->ib_mr->lkey, i);

      if (rdma_poll_completion(t_conns[thread_num]->ib_cq)) {
        break;
      } else {
        fprintf(stderr, "[CAUTION] Retry broadcasting signal (error: %s)\n", strerror(errno));
      }
    }
  }
}

void Client::wait_signal(SignalType sig_type) {
  struct Signal sig;

  while (true) {
    if (recv_signal(sig)) {
      SignalType recv_sig_type = (SignalType)sig.signal;

      if (recv_sig_type == sig_type) {
        return;
      } else if (recv_sig_type == SignalType::TABLE_UPDATE) {
        uint64_t UNLOCKED = 0;
        local_tab_locks[sig.tab_id].compare_exchange_strong(UNLOCKED, 1,
                                                            std::memory_order_acquire,
                                                            std::memory_order_relaxed);
      }
    }
  }
}

void Client::send_stats(ClientStats* stats) {
  for (uint16_t i = 0; i < server_num; ++i) {
    uint64_t sig_buffer = t_conns[thread_num]->get_send_buffer(i);
    memcpy((ClientStats *)sig_buffer, stats, sizeof(ClientStats));

    while (true) {
      rdma_post_send(t_conns[thread_num]->ib_qp_list[i], sig_buffer,
                     sizeof(ClientStats), t_conns[thread_num]->ib_mr->lkey, i);

      if (rdma_poll_completion(t_conns[thread_num]->ib_cq)) {
        break;
      } else {
        fprintf(stderr, "[CAUTION] Retry sending statistics (error: %s)\n", strerror(errno));
      }
    }
  }
}