#include <set>
#include <chrono>
#include "memory_server.h"

thread_local TableHeader* MemoryServer::tab_hdr = NULL;
thread_local TableEntry* MemoryServer::tab = NULL;
thread_local Stash* MemoryServer::stash = NULL;

void* master_server_main(void* server_arg) {
  MemoryServer* server_instance = (MemoryServer*)server_arg;

  return server_instance->master_busy_wait();
}

void* memory_server_main(void* server_arg) {
  MemoryServer* server_instance = (MemoryServer*)server_arg;

  return server_instance->busy_wait();
}

MemoryServer::MemoryServer(const struct ServerConfig& conf) : Server(conf) {
  unlock_buffer = buf;
  buf = (void*)((uint64_t)buf + sizeof(int64_t));

  assert(TOTAL_TAB_NUM % conf.server_num == 0);
  table_per_mn = TOTAL_TAB_NUM / conf.server_num;
  uint64_t quotient = base_size[MRType::TABLE] / table_per_mn;
  uint64_t remainder = quotient % 8;

  if (remainder == 0) {
    max_tab_capacity = (quotient - sizeof(TableHeader)) / sizeof(TableEntry);
  } else {
    max_tab_capacity = (quotient + 8 - remainder - sizeof(TableHeader)) / sizeof(TableEntry);
  }

  merge_manager = new MergeManager(table_per_mn);
  tab_hdr_list = new TableHeader*[table_per_mn];
  tab_list = new TableEntry*[table_per_mn];
  stash_list = new Stash*[table_per_mn];

  // TODO: Current code does not check component boundaries
  for (uint16_t i = 0; i < table_per_mn; ++i) {
    tab_hdr_list[i] = (TableHeader*)(base_addr[MRType::TABLE]+TableHeader::get_start_offset(table_per_mn, i, base_size[MRType::TABLE]));
    tab_list[i] = (TableEntry*)(base_addr[MRType::TABLE]+TableHeader::get_start_offset(table_per_mn, i, base_size[MRType::TABLE])+sizeof(TableHeader));
    stash_list[i] = (Stash*)(base_addr[MRType::STASH]+Stash::get_start_offset(table_per_mn, i, base_size[MRType::STASH]));
  }

  dir_alloc_num = (uint64_t*)base_addr[MRType::DIRECTORY];
  buc_alloc_num = (uint64_t*)base_addr[MRType::BUCKET];
  kv_data_alloc_num  = (uint64_t*)base_addr[MRType::KV_DATA];

  *dir_alloc_num = 0;
  *buc_alloc_num = 0; 
  *kv_data_alloc_num = 0;
}

MemoryServer::~MemoryServer(void) {
  uint64_t memory_consumption = 0;

  for (uint16_t i = 0; i < table_per_mn; ++i) {
    memory_consumption += sizeof(TableHeader);
    memory_consumption += tab_hdr_list[i]->get_tab_size();
    memory_consumption += sizeof(Stash);
  }

  memory_consumption += (*dir_alloc_num)*sizeof(Directory);
  memory_consumption += (*buc_alloc_num)*sizeof(Bucket);

  assert(client_stats_list);
  uint64_t kv_data_size = client_stats_list[0].kv_data_size;
  memory_consumption += (*kv_data_alloc_num)*kv_data_size;

  fprintf(stdout, "[STATISTICS] Total memory consumption: %lu B\n", memory_consumption);

  std::pair<double, uint64_t> over_merge_error = merge_manager->get_over_merge_error();
  std::pair<double, uint64_t> under_merge_error = merge_manager->get_under_merge_error();
  std::pair<double, uint64_t> over_append_error = merge_manager->get_over_append_error();
  std::pair<double, uint64_t> under_append_error = merge_manager->get_under_append_error();

  fprintf(stdout, "[STATISTICS] Average over estimated merge error: %f %% (%lu times)\n", over_merge_error.first, over_merge_error.second);
  fprintf(stdout, "[STATISTICS] Average under estimated merge error: %f %% (%lu times)\n", under_merge_error.first, under_merge_error.second);
  fprintf(stdout, "[STATISTICS] Average over estimated append error: %f entries (%lu times)\n", over_append_error.first, over_append_error.second);
  fprintf(stdout, "[STATISTICS] Average under estimated append error: %f entries (%lu times)\n", under_append_error.first, under_append_error.second);
}

void MemoryServer::exit_background_merge(void) {
  stop_merge = true;
}

void* MemoryServer::master_busy_wait(void) {
  reply_and_connect();
  clear_udp_socket();
  broadcast_signal(SignalType::START);

  struct Signal sig;
  uint16_t ready_client_num = 0;
  uint16_t halted_client_num = 0;
  uint16_t ended_client_num = 0;
  std::string mn_core = std::to_string(MN_CORE);
  std::string total_core = std::to_string(CPU_NUM * CORE_PER_CPU * HYPERTHREAD_PER_CORE);
  while (true) {
    bool ret = recv_signal(sig);

    if (ret) {
      SignalType sig_type = (SignalType)sig.signal;

      switch (sig_type) {
        case SignalType::INIT_DONE:
          broadcast_signal(SignalType::START);
#ifdef STATS
          start_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
#endif
          std::system(("../script/start_cpu_util_log.sh LOAD " + mn_core).c_str());
          background = std::thread(&MemoryServer::background_merge, this);
          break;
        case SignalType::READY:
          ready_client_num++;

          if (ready_client_num == client_num) {
            broadcast_signal(SignalType::START);
          }

          break; 
        case SignalType::HALT:
          halted_client_num++;

          if (halted_client_num == client_num) {
            std::system(("../script/stop_cpu_util_log.sh LOAD " + mn_core + " " + total_core).c_str());
            std::system(("../script/start_cpu_util_log.sh TXN " + mn_core).c_str());
            halted_client_num = 0;
            broadcast_signal(SignalType::START);
          }

          break;
        case SignalType::END:
          ended_client_num++;

          if (ended_client_num == client_num) {
            std::system(("../script/stop_cpu_util_log.sh TXN " + mn_core + " " + total_core).c_str());
            broadcast_signal(SignalType::START);
            recv_stats();
            exit_background_merge();
            background.join();   

            return NULL;
          }

          break;
        default:
          continue;
      }
    }
  }
}

void* MemoryServer::busy_wait(void) {
  reply_and_connect();
  clear_udp_socket();
  
  std::string mn_core = std::to_string(MN_CORE);
  std::string total_core = std::to_string(CPU_NUM * CORE_PER_CPU * HYPERTHREAD_PER_CORE);
  std::system(("../script/start_cpu_util_log.sh LOAD " + mn_core).c_str());
  background = std::thread(&MemoryServer::background_merge, this);

  struct Signal sig;
  uint16_t halted_client_num = 0;
  uint16_t ended_client_num = 0;
  while (true) {
    bool ret = recv_signal(sig);

    if (ret) {
      SignalType sig_type = (SignalType)sig.signal;

      switch (sig_type) {
        case SignalType::HALT:
          halted_client_num++;

          if (halted_client_num == client_num) {
            std::system(("../script/stop_cpu_util_log.sh LOAD " + mn_core + " " + total_core).c_str());
            std::system(("../script/start_cpu_util_log.sh TXN " + mn_core).c_str());
            halted_client_num = 0;
          }

          break;
        case SignalType::END:
          ended_client_num++;

          if (ended_client_num == client_num) {
            std::system(("../script/stop_cpu_util_log.sh TXN " + mn_core + " " + total_core).c_str());
            recv_stats();
            exit_background_merge();
            background.join();   

            return NULL;
          }

          break;
        default:
          continue;
      }
    }
  }
}

void MemoryServer::print_table(uint16_t tab_num) {
  TableHeader* tab_hdr = tab_hdr_list[tab_num];
  TableEntry* tab = tab_list[tab_num];

  fprintf(stdout, "====================  Table Information  ====================\n");
  fprintf(stdout, "Version              : %lu\n", tab_hdr->get_version());
  fprintf(stdout, "Table ID             : %lu\n", tab_hdr->get_tab_id());
  fprintf(stdout, "Minimum key          : %lu\n", tab_hdr->get_min_key());
  fprintf(stdout, "Maximum key          : %lu\n", tab_hdr->get_max_key());
  fprintf(stdout, "Stride               : %lu\n", tab_hdr->get_stride());
  fprintf(stdout, "Capacity             : %lu\n", tab_hdr->get_capacity());
  fprintf(stdout, "Maximum search length: %lu\n", tab_hdr->get_max_search_len());

  Address cur_dir_addr = tab[0].get_dir_ptr();
  fprintf(stdout, "0 >>> %lu(%lu) | ", cur_dir_addr.num, get_dir(cur_dir_addr)->get_capacity());
  for (uint64_t i = 0; i < tab_hdr->get_capacity(); ++i) {
    while (tab[i].get_dir_ptr() != cur_dir_addr) {
      cur_dir_addr = get_dir(cur_dir_addr)->get_next_dir_addr();
      fprintf(stdout, "\n%lu >>> %lu(%lu) | ", i, cur_dir_addr.num, get_dir(cur_dir_addr)->get_capacity());
    }
    fprintf(stdout, "(%lu, %lu, %lu, %lu) ", tab[i].get_dir_version(), tab[i].get_dir_ptr().sid, tab[i].get_dir_ptr().num, tab[i].get_dir_idx());
  }

  while (!get_dir(cur_dir_addr)->is_last_dir()) {
    cur_dir_addr = get_dir(cur_dir_addr)->get_next_dir_addr();
    fprintf(stdout, "\nX >>> %lu(%lu/%lu) | X", cur_dir_addr.num, get_dir(cur_dir_addr)->get_version(), get_dir(cur_dir_addr)->get_capacity());
  }

  fprintf(stdout, "\n=============================================================\n");
}

void MemoryServer::print_table(uint16_t tab_num, uint64_t idx) {
  TableHeader* tab_hdr = tab_hdr_list[tab_num];
  TableEntry* tab = tab_list[tab_num];

  fprintf(stdout, "================ Table Index %lu Information ================\n", idx);
  fprintf(stdout, "Table ID                          : %lu\n", tab_hdr->get_tab_id());
  fprintf(stdout, "TableEntry                        : (%lu, %lu, %lu, %lu)\n", tab[idx].get_dir_version(), tab[idx].get_dir_ptr().sid, tab[idx].get_dir_ptr().num, tab[idx].get_dir_idx());
  fprintf(stdout, "Table index lowest                : %lu\n", tab_hdr->get_min_key() + idx*tab_hdr->get_stride());
  fprintf(stdout, "Neighboring directory entry lowest:\n");
  
  Directory* dir = get_dir(tab[idx].get_dir_ptr());

  if (tab[idx].get_dir_idx() == 0) {
    if (dir->is_first_dir()) {
      fprintf(stdout, "(-1) None\n");
    } else {
      Directory* prev_dir = get_prev_dir(tab[idx].get_dir_ptr());
      fprintf(stdout, "(-1) %lu\n", prev_dir->get_entry_lowest(prev_dir->get_capacity()-1));
    }
  } else {
    fprintf(stdout, "(-1) %lu\n", dir->get_entry_lowest(tab[idx].get_dir_idx()-1));
  }

  fprintf(stdout, "( 0) %lu\n", dir->get_entry_lowest(tab[idx].get_dir_idx()));

  if (tab[idx].get_dir_idx() == dir->get_capacity() - 1) {
    if (dir->is_last_dir()) {
      fprintf(stdout, "(+1) None\n");
    } else {
      Directory* next_dir = get_next_dir(tab[idx].get_dir_ptr());
      fprintf(stdout, "(+1) %lu\n", next_dir->get_entry_lowest(0));
    }
  } else {
    fprintf(stdout, "(+1) %lu\n", dir->get_entry_lowest(tab[idx].get_dir_idx()+1));
  }

  fprintf(stdout, "==============================================================\n");
}

void MemoryServer::check_table_sanity(uint16_t tab_num) {
  TableHeader* tab_hdr = tab_hdr_list[tab_num];
  TableEntry* tab = tab_list[tab_num];

  uint64_t max_search_len = 0;
  uint64_t max_search_len_idx = 0;

  uint64_t dir_idx = 0;
  Address dir_addr = tab[0].get_dir_ptr();
  Directory* dir = get_dir(dir_addr);
  for (uint64_t tab_idx = 1; tab_idx < tab_hdr->get_capacity(); ++tab_idx) {
    for (; dir_idx < dir->get_capacity() + 1; dir_idx++) {
      if (dir_idx == dir->get_capacity() && !dir->is_last_dir()) {
        dir_idx = 0;
        dir_addr = dir->get_next_dir_addr();
        dir = get_dir(dir_addr);
      }

      if (dir_idx == dir->get_capacity() || tab_hdr->get_min_key() + tab_idx*tab_hdr->get_stride() < dir->get_entry_lowest(dir_idx)) {
        uint64_t correct_ver;
        uint64_t correct_num;
        uint64_t correct_idx;

        if (dir_idx == 0) {
          if (dir->is_first_dir()) {
            correct_ver = dir->get_version();
            correct_num = dir_addr.num;
            correct_idx = 0;
          } else {
            Address prev_dir_addr = dir->get_prev_dir_addr();
            Directory* prev_dir = get_dir(prev_dir_addr);
            
            correct_ver = prev_dir->get_version();
            correct_num = prev_dir_addr.num;
            correct_idx = prev_dir->get_capacity() - 1;
          }
        } else {
          correct_ver = dir->get_version();
          correct_num = dir_addr.num;
          correct_idx = dir_idx - 1;
        }

        if (tab[tab_idx].get_dir_version() != correct_ver || tab[tab_idx].get_dir_ptr().sid != sid ||
            tab[tab_idx].get_dir_ptr().num != correct_num || tab[tab_idx].get_dir_idx() != correct_idx) {
          fprintf(stdout, "[ERROR] Correct TableEntry is (%lu, %u, %lu, %lu)\n", 
                          correct_ver, sid, correct_num, correct_idx);
          print_table(tab_num, tab_idx);
          exit(1);
        }

        break;
      }
    }

    if (tab_idx != 0) {
      uint64_t cur_search_len = get_target_search_len(tab_idx);
      if (cur_search_len > max_search_len) {
        max_search_len = cur_search_len;
        max_search_len_idx = tab_idx;
      }
    }
  }

  uint64_t cur_search_len = get_target_search_len(tab_hdr->get_capacity());
  if (cur_search_len > max_search_len) {
    max_search_len = cur_search_len;
    max_search_len_idx = tab_hdr->get_capacity();
  }
  
  if (tab_hdr->get_max_search_len() != max_search_len) {
    fprintf(stdout, "[ERROR] Correct max_search_len is %lu at table entry %lu but detected %lu\n", 
                    max_search_len, max_search_len_idx, tab_hdr->get_max_search_len());
    print_table(tab_num);
    exit(1);
  }
}

struct sockaddr_in MemoryServer::pack_udp_server_info(uint16_t idx) {
  struct sockaddr_in udp_server_info;
  udp_server_info.sin_family = AF_INET;
  udp_server_info.sin_port = htons(udp_port);
  udp_server_info.sin_addr.s_addr = inet_addr(server_ips[idx]);
  return udp_server_info;
}

void MemoryServer::reply_and_connect(void) {
  struct ServerInfo server_info;
  struct ibv_qp* ib_qp = t_conn->create_qp();
  struct sockaddr_in udp_server_info = pack_udp_server_info(conn_server_thread_num);

  struct ThreadInfo thread_info;
  struct sockaddr_in thread_addr;
  socklen_t thread_addr_len = sizeof(struct sockaddr_in);

  while (!client_connection_done() || !server_connection_done()) {
    if (!server_connection_done()) {
      send_udp_thread_info(ib_qp, udp_server_info);
    }

    if (recv_udp_server_info(&server_info) &&
        t_conn->make_connection(ib_qp, udp_server_info, &server_info)) {
      conn_server_thread_num++;
      
      if (!server_connection_done()) {
        ib_qp = t_conn->create_qp();
        udp_server_info = pack_udp_server_info(conn_server_thread_num);
      }
    }

    if (recv_udp_thread_info(&thread_info, &thread_addr, &thread_addr_len)) {
      make_connection(&thread_info, &thread_addr, thread_addr_len);
    }
  }
}

/* RDMA write to server. */
void MemoryServer::write(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t size, bool signal) {
  rdma_send_write(t_conn->ib_mr, t_conn->ib_qp_list[sid], t_conn->server_info_list[sid].mr_rkey[type],
                  (uint64_t)src, t_conn->server_info_list[sid].base_addr[type] + offset, size, 0, signal);

  if (signal && !rdma_poll_completion(t_conn->ib_cq)) {
    fprintf(stderr, "[ERROR] Write failure (Target server %u / Target component %u / Target size %lu)\n", sid, type, size);   
    exit(1);
  }
}

/* RDMA read to server. */
void MemoryServer::read(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t size) {
  rdma_send_read(t_conn->ib_mr, t_conn->ib_qp_list[sid], t_conn->server_info_list[sid].mr_rkey[type],
                  (uint64_t)src, t_conn->server_info_list[sid].base_addr[type] + offset, size, 0);

  if (!rdma_poll_completion(t_conn->ib_cq)) {
    fprintf(stderr, "[ERROR] Read failure (Target server %u / Target component %u / Target size %lu)\n", sid, type, size);   
    exit(1);
  }
}

/* RDMA compare-and-swap to server. */
void MemoryServer::CAS(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t compare, uint64_t swap) {
  // RDMA CAS destination address must be aligned
  rdma_send_compare_and_swap(t_conn->ib_mr, t_conn->ib_qp_list[sid], t_conn->server_info_list[sid].mr_rkey[type],
                             (uint64_t)src, t_conn->server_info_list[sid].base_addr[type] + offset, compare, swap, 0);

  if (!rdma_poll_completion(t_conn->ib_cq)) {
    fprintf(stderr, "[ERROR] CAS failure (Target server %u / Target component %u)\n", sid, type);   
    exit(1);
  }
}

#ifndef LOCK_FREE_STASH
bool MemoryServer::lock_stash(uint16_t tab_id) {
  CAS(buf, sid, Stash::get_start_offset(table_per_mn, tab_id/server_num, base_size[MRType::STASH]), MRType::STASH, 0, 1);
  StashHeader* cmp = (StashHeader*)buf;
  if (cmp->lock == 0) {
    return true;
  }
  return false;
}

void MemoryServer::unlock_stash(uint16_t tab_id) {
  uint64_t* lock = new (buf) uint64_t(0);
  write(buf, sid, Stash::get_start_offset(table_per_mn, tab_id/server_num, base_size[MRType::STASH]), MRType::STASH, sizeof(uint64_t));
}
#endif

Address MemoryServer::alloc_dir(void) {
  uint64_t new_dir_num = __sync_fetch_and_add(dir_alloc_num, 1);
  return Address(0, sid, new_dir_num+1);
}

Bucket* MemoryServer::get_bucket(Address dest) {
  Bucket* bucket;
  do {
    read(buf, dest.sid, Bucket::get_bucket_offset(dest.num), MRType::BUCKET, sizeof(Bucket));
    bucket = (Bucket*)buf;  
  } while (!bucket->is_consistent());

  return bucket;
}

Address MemoryServer::get_target_dir_addr(uint64_t key) {
  uint64_t block = tab_hdr->get_target_block(key);

  if (tab_hdr->get_capacity() - 1 > block) {
    if (key >= get_dir(tab[block+1].get_dir_ptr())->get_entry_lowest(0)) {
      return tab[block+1].get_dir_ptr();
    }
  }

  return tab[block].get_dir_ptr();
}

uint64_t MemoryServer::get_dir_start_block(Directory* dir) {
  uint64_t block = tab_hdr->get_target_block(dir->get_entry_lowest(0));

  if (tab_hdr->get_capacity() - 1 > block && tab[block].get_dir_ptr() != dir->get_cur_dir_addr()) {
    block++;
  }

  assert(tab[block].get_dir_ptr() == dir->get_cur_dir_addr());
  return block;
}

uint64_t MemoryServer::get_target_search_len(uint64_t idx) {
  uint64_t search_len = 0;

  if (idx == tab_hdr->get_capacity()) {
    Directory* dir = get_dir(tab[idx-1].get_dir_ptr());
    search_len = dir->get_capacity();
    while (!dir->is_last_dir()) {
      dir = get_next_dir(dir->get_cur_dir_addr());
      search_len += dir->get_capacity();
    }
  } else if (idx > 0) {
    Directory* dir = get_dir(tab[idx].get_dir_ptr());
    search_len = tab[idx].get_dir_idx() + 1;
    while (tab[idx-1].get_dir_ptr() != dir->get_cur_dir_addr()) {
      dir = get_prev_dir(dir->get_cur_dir_addr());
      search_len += dir->get_capacity();
    }
  }

  return search_len - tab[idx-1].get_dir_idx();
}

uint64_t MemoryServer::count_merge_dir_e_num(uint64_t merge_stash_e_num) {
  uint64_t merge_dir_e_num = 0;
  std::set<Address> merge_dir_addr_list;

  for (uint64_t i = 0; i < merge_stash_e_num; ++i) {
    DirectoryEntry* stash_e = stash->get_target_entry(i);
    merge_dir_addr_list.insert(get_target_dir_addr(stash_e->get_lowest()));
  }

  for (auto dir_addr: merge_dir_addr_list) {
    Directory* target_dir = get_dir(dir_addr);
    merge_dir_e_num += target_dir->get_capacity();
  }

  return merge_dir_e_num;
}

void MemoryServer::merge_stash(uint64_t& tab_idx, uint64_t& old_idx, uint64_t& stash_start, uint64_t& stash_end,
                               Directory* old_dir, DirectoryEntry* stash, Directory* prev_dir, Directory* new_dir) {
  for (uint64_t new_idx = 0; new_idx < new_dir->get_capacity(); ++new_idx) {
    if (stash_start == stash_end) {
      assert(old_idx != old_dir->get_capacity());
      DirectoryEntry entry = old_dir->entry[old_idx++];
      entry.set_consistent(new_dir->get_version());
      memcpy(&new_dir->entry[new_idx], &entry, sizeof(DirectoryEntry));
    } else if (old_idx == old_dir->get_capacity()) {
      assert(stash_start != stash_end);
      DirectoryEntry entry = stash[stash_start++];
      entry.set_consistent(new_dir->get_version());
      memcpy(&new_dir->entry[new_idx], &entry, sizeof(DirectoryEntry));
    } else {
      if (stash[stash_start].get_lowest() < old_dir->get_entry_lowest(old_idx)) {
        DirectoryEntry entry = stash[stash_start++];
        entry.set_consistent(new_dir->get_version());
        memcpy(&new_dir->entry[new_idx], &entry, sizeof(DirectoryEntry));
      } else {
        DirectoryEntry entry = old_dir->entry[old_idx++];
        entry.set_consistent(new_dir->get_version());
        memcpy(&new_dir->entry[new_idx], &entry, sizeof(DirectoryEntry));
      }
    }

    while (tab_hdr->get_capacity() > tab_idx && (tab_hdr->get_min_key() + tab_idx*tab_hdr->get_stride() < new_dir->get_entry_lowest(new_idx))) {
      if (new_idx == 0) {
        if (new_dir->is_first_dir()) {
          TableEntry addr(tab_hdr->get_version(), tab[tab_idx].get_entry_version()+1, 
                          new_dir->get_version(), 0, new_dir->get_next_dir_addr().num, new_dir->get_cur_dir_addr());
          memcpy(&tab[tab_idx], &addr, sizeof(TableEntry));
        } else {
          TableEntry addr(tab_hdr->get_version(), tab[tab_idx].get_entry_version()+1, 
                          prev_dir->get_version(), prev_dir->get_capacity()-1, prev_dir->get_next_dir_addr().num, prev_dir->get_cur_dir_addr());
          memcpy(&tab[tab_idx], &addr, sizeof(TableEntry));
        }
      } else {
        TableEntry addr(tab_hdr->get_version(), tab[tab_idx].get_entry_version()+1, 
                        new_dir->get_version(), new_idx-1, new_dir->get_next_dir_addr().num, new_dir->get_cur_dir_addr());
        memcpy(&tab[tab_idx], &addr, sizeof(TableEntry));
      }

      uint64_t cur_search_len = get_target_search_len(tab_idx);
      if (cur_search_len > tab_hdr->get_max_search_len()) {
        tab_hdr->set_max_search_len(cur_search_len);
      }

      tab_idx++;
    }
  }

  // Clear out the directory and set consistency
  for (uint64_t new_idx = new_dir->get_capacity(); new_idx < DIR_CAPACITY; ++new_idx) {
    new_dir->entry[new_idx].clear();
    new_dir->entry[new_idx].set_consistent(new_dir->get_version());
  }

  assert(new_dir->is_consistent());
}

void MemoryServer::split_and_merge_stash(uint64_t tab_idx, uint64_t stash_start, uint64_t stash_end, Directory* dir, DirectoryEntry* stash) {
  // Make a copy of the original directory
  Directory* old_dir = (Directory*)malloc(sizeof(Directory));
  memcpy(old_dir, dir, sizeof(Directory));

  uint64_t old_idx = 0;
  uint64_t merge_capacity = old_dir->get_capacity() + stash_end - stash_start;

  dir->set_merge_flag();

  if (dir->is_first_dir()) {
    assert(tab_idx == 0);

    TableEntry addr(tab_hdr->get_version(), tab[tab_idx].get_entry_version()+1, 
                    dir->get_version(), 0, dir->get_next_dir_addr().num, dir->get_cur_dir_addr());
    memcpy(&tab[tab_idx], &addr, sizeof(TableEntry));
    
    tab_idx++;
  }

  Directory* last_dir;
  if (merge_capacity <= DIR_CAPACITY) {
    dir->set_capacity(merge_capacity);
    
    Directory* prev_dir = get_dir(old_dir->get_prev_dir_addr());
    merge_stash(tab_idx, old_idx, stash_start, stash_end, old_dir, stash, prev_dir, dir);

    last_dir = dir;
  } else {
    uint64_t split_dir_size;
    uint64_t required_dir_num = 1;
    for (; required_dir_num <= merge_capacity; ++required_dir_num) {
        split_dir_size = merge_capacity/required_dir_num;
        if (split_dir_size < DIR_CAPACITY) {
            break;
        }
    }

    assert(required_dir_num > 1);

    Address new_dir_addr_list[required_dir_num - 1]; // One directory is the original directory
    for (int i = 0; i < required_dir_num - 1; ++i) {
      new_dir_addr_list[i] = alloc_dir();
    }

    dir->set_capacity(split_dir_size);
    dir->set_next_dir_addr(new_dir_addr_list[0]);

    Directory* prev_dir = get_dir(old_dir->get_prev_dir_addr());
    merge_stash(tab_idx, old_idx, stash_start, stash_end, old_dir, stash, prev_dir, dir);

    for (uint64_t i = 0; i < required_dir_num - 1; ++i) {
      Directory* sib_dir;
      if (i == required_dir_num - 2) {
        sib_dir = new (get_dir(new_dir_addr_list[i])) Directory(merge_capacity - (i + 1)*split_dir_size, new_dir_addr_list[i], dir->get_cur_dir_addr(), old_dir->get_next_dir_addr());
      } else {
        sib_dir = new (get_dir(new_dir_addr_list[i])) Directory(split_dir_size, new_dir_addr_list[i], dir->get_cur_dir_addr(), new_dir_addr_list[i + 1]);
      } 

      sib_dir->set_merge_flag();

      if (!sib_dir->is_last_dir()) {
        get_dir(sib_dir->get_next_dir_addr())->set_prev_dir_addr(new_dir_addr_list[i]);
      }

      merge_stash(tab_idx, old_idx, stash_start, stash_end, old_dir, stash, dir, sib_dir);
      
      dir = sib_dir;
      last_dir = sib_dir;
    }
  }

  while (tab_hdr->get_capacity() > tab_idx && tab[tab_idx].get_dir_ptr() == old_dir->get_cur_dir_addr()) {
    TableEntry addr(tab_hdr->get_version(), tab[tab_idx].get_entry_version()+1, 
                    last_dir->get_version(), last_dir->get_capacity()-1, last_dir->get_next_dir_addr().num, last_dir->get_cur_dir_addr());
    memcpy(&tab[tab_idx], &addr, sizeof(TableEntry));

    uint64_t cur_search_len = get_target_search_len(tab_idx);
    if (cur_search_len > tab_hdr->get_max_search_len()) {
      tab_hdr->set_max_search_len(cur_search_len);
    }

    tab_idx++;
  }

  if (tab_hdr->get_capacity() >= tab_idx) {
    uint64_t cur_search_len = get_target_search_len(tab_idx);
    if (cur_search_len > tab_hdr->get_max_search_len()) {
      tab_hdr->set_max_search_len(cur_search_len);
    }
  }

  free(old_dir);
}

#ifdef LOCK_FREE_STASH
void MemoryServer::safely_clear_stash(uint64_t offset, uint64_t merge_stash_e_num) {
  // Invalidate merged stash entries
  for (int i = 0; i < merge_stash_e_num; ++i) {
    DirectoryEntry* e = stash->get_target_entry(i);
    e->set_consistent(0);
  }

  StashHeader cmp_hdr;
  StashHeader swap_hdr;
  StashHeader* new_hdr = new (buf) StashHeader;

  do {
    cmp_hdr = stash->hdr;
    swap_hdr.start = (cmp_hdr.start + merge_stash_e_num) % STASH_CAPACITY;
    swap_hdr.entry_used = cmp_hdr.entry_used > STASH_CAPACITY ? STASH_CAPACITY - merge_stash_e_num : cmp_hdr.entry_used - merge_stash_e_num;
    CAS(buf, sid, offset, MRType::STASH, cmp_hdr.val, swap_hdr.val);
  } while (cmp_hdr.val != new_hdr->val);
}
#endif

void MemoryServer::background_merge(void) {
  stop_merge = false;
  
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(MN_CORE, &cpuset);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    fprintf(stdout, "[ERROR] Failed binding core (error: %s)\n", strerror(errno));
  }

  // Target merge table number
  uint64_t est_merge_stash_e_num[table_per_mn];
  uint64_t est_merge_dir_e_num[table_per_mn];

#ifndef MERGE_SCHEDULE
  uint16_t tab_num = 0;

  while (!stop_merge) {
    tab_num = (tab_num + 1) % table_per_mn;

    if (stash_list[tab_num]->get_entry_used() >= STASH_CAPACITY) {
#else
  for (uint16_t i = 0; i < table_per_mn; ++i) {
    merge_manager->start_append_timer(i, stash_list[i]->get_entry_used());
  }

  while (!stop_merge) {
    for (uint16_t i = 0; i < table_per_mn; ++i) {
      est_merge_stash_e_num[i] = stash_list[i]->get_entry_used();

      merge_manager->end_append_timer(i, stash_list[i]->get_entry_used());
      merge_manager->start_append_timer(i, stash_list[i]->get_entry_used());
    }

    std::vector<uint16_t> candidate = merge_manager->get_merge_candidate(est_merge_stash_e_num);

    for (auto candidate_tab_num : candidate) {
      tab_hdr = tab_hdr_list[candidate_tab_num];
      tab = tab_list[candidate_tab_num];
      stash = stash_list[candidate_tab_num];

      est_merge_stash_e_num[candidate_tab_num] = stash->get_entry_used();
      est_merge_dir_e_num[candidate_tab_num] = count_merge_dir_e_num(est_merge_stash_e_num[candidate_tab_num]);
    }
    
    std::vector<uint16_t> schedule = merge_manager->schedule_merge(candidate, est_merge_stash_e_num, est_merge_dir_e_num);
  
    for (auto tab_num : schedule) {
#endif
      tab_hdr = tab_hdr_list[tab_num];
      tab = tab_list[tab_num];
      stash = stash_list[tab_num];

#ifndef LOCK_FREE_STASH
      while (!lock_stash(tab_hdr->get_tab_id())) {
        continue;
      }
#endif

#ifndef LOCK_FREE_STASH
      uint64_t merge_stash_e_num = stash->get_entry_used();
#else
      uint64_t merge_stash_e_num = stash->get_safe_entry_used();
#endif
      uint64_t merge_dir_e_num = 0;

      if (merge_stash_e_num == 0) {
#ifndef LOCK_FREE_STASH
        unlock_stash(tab_hdr->get_tab_id());
#endif
        continue;
      }

      // Create snapshot of stash directory
      DirectoryEntry* snapshot = stash->create_snapshot(merge_stash_e_num);

#ifdef MERGE_SCHEDULE
      merge_manager->start_merge_timer(tab_num);
#endif

      // Merge directories
      uint64_t merge_start_stash_idx = 0;
      Address merge_start_dir_addr = get_target_dir_addr(snapshot[merge_start_stash_idx].get_lowest());
      Directory* target_dir = get_dir(merge_start_dir_addr);

      for (uint64_t i = 1; i < merge_stash_e_num + 1; ++i) {
        Address cur_dir_addr = get_target_dir_addr(snapshot[i].get_lowest());
        if (i == merge_stash_e_num || merge_start_dir_addr != cur_dir_addr) {
          merge_dir_e_num += target_dir->get_capacity();

          split_and_merge_stash(get_dir_start_block(target_dir), merge_start_stash_idx, i, target_dir, snapshot);

          merge_start_stash_idx = i;
          merge_start_dir_addr = cur_dir_addr;
          target_dir = get_dir(merge_start_dir_addr);
        }
      }

      delete[] snapshot;

#ifdef MERGE_SCHEDULE
      merge_manager->end_merge_timer(tab_num, est_merge_stash_e_num[tab_num], est_merge_dir_e_num[tab_num], merge_stash_e_num, merge_dir_e_num, stash->get_entry_used());
#endif

#ifndef LOCK_FREE_STASH
      stash->commit_merge(merge_stash_e_num);  
      unlock_stash(tab_hdr->get_tab_id());
#else
      safely_clear_stash(Stash::get_start_offset(table_per_mn, tab_num, base_size[MRType::STASH]), merge_stash_e_num);
#endif

      bool initial_loop = true;
      while (tab_hdr->get_max_search_len() > MAX_SEARCH_LENGTH) {
        tab_hdr->set_revamp_flag();
        tab_hdr->set_max_search_len(0);

        if (initial_loop) {
          // Get minimum and maximum key
          Directory* first_dir = get_dir(tab[0].get_dir_ptr());
          Bucket* first_bucket = get_bucket(first_dir->entry[0].ptr);
          uint64_t new_min_key = first_bucket->min_key();
          
          Directory* last_dir = get_last_dir();
          Bucket* last_bucket = get_bucket(last_dir->entry[last_dir->get_capacity()-1].ptr);
          uint64_t new_max_key = last_bucket->max_key();

          tab_hdr->set_min_key(new_min_key);
          tab_hdr->set_max_key(new_max_key);

          initial_loop = false;
        }
        
        tab_hdr->double_capacity();
        tab_hdr->set_stride((tab_hdr->get_max_key() - tab_hdr->get_min_key()) / tab_hdr->get_capacity());

        // Create new table
        uint64_t dir_idx = 0;
        Address dir_addr = tab[0].get_dir_ptr();
        Directory* dir = get_dir(dir_addr);
        Address prev_dir_addr = dir->get_next_dir_addr();

        // Update first table entry
        TableEntry first_addr(tab_hdr->get_version(), tab[0].get_entry_version()+1, 
                              dir->get_version(), 0, prev_dir_addr.num, dir_addr);
        memcpy(&tab[0], &first_addr, sizeof(TableEntry));

        for (uint64_t tab_idx = 1; tab_idx < tab_hdr->get_capacity(); ++tab_idx) {
          for (; dir_idx < dir->get_capacity() + 1; dir_idx++) {
            if (dir_idx == dir->get_capacity() && !dir->is_last_dir()) {
              dir_idx = 0;
              dir_addr = dir->get_next_dir_addr();
              dir = get_dir(dir_addr);
              prev_dir_addr = dir->get_next_dir_addr();
            }

            if (dir_idx == dir->get_capacity() || tab_hdr->get_min_key() + tab_idx*tab_hdr->get_stride() < dir->get_entry_lowest(dir_idx)) {
              if (dir_idx == 0) {
                if (dir->is_first_dir()) {
                  TableEntry addr(tab_hdr->get_version(), tab[tab_idx].get_entry_version()+1, 
                                  dir->get_version(), 0, prev_dir_addr.num, dir_addr);
                  memcpy(&tab[tab_idx], &addr, sizeof(TableEntry));
                } else {
                  Address prev_dir_addr = dir->get_prev_dir_addr();
                  Directory* prev_dir = get_dir(prev_dir_addr);

                  TableEntry addr(tab_hdr->get_version(), tab[tab_idx].get_entry_version()+1, 
                                  prev_dir->get_version(), prev_dir->get_capacity()-1, prev_dir->get_next_dir_addr().num, prev_dir_addr);
                  memcpy(&tab[tab_idx], &addr, sizeof(TableEntry));
                }
              } else {
                TableEntry addr(tab_hdr->get_version(), tab[tab_idx].get_entry_version()+1, 
                                dir->get_version(), dir_idx-1, prev_dir_addr.num, dir_addr);
                memcpy(&tab[tab_idx], &addr, sizeof(TableEntry));
              }

              break;
            }
          }

          if (tab_idx != 0) {
            uint64_t cur_search_len = get_target_search_len(tab_idx);
            if (cur_search_len > tab_hdr->get_max_search_len()) {
              tab_hdr->set_max_search_len(cur_search_len);
            }
          }
        }

        uint64_t cur_search_len = get_target_search_len(tab_hdr->get_capacity());
        if (cur_search_len > tab_hdr->get_max_search_len()) {
          tab_hdr->set_max_search_len(cur_search_len);
        }

        tab_hdr->set_consistent(tab_hdr->get_version());
      }

      assert(tab_hdr->get_max_search_len() <= MAX_SEARCH_LENGTH);
      assert(tab_hdr->get_capacity() <= max_tab_capacity);

      broadcast_table_update(tab_hdr->get_tab_id());

      //check_table_sanity(tab_num);

#ifdef STATS
      uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count() - start_timestamp;
      
      if (tab_hdr->get_capacity() == 1) {
        fprintf(stdout, "%lu MERGE %lu %lu %lu\n", timestamp, tab_hdr->get_capacity(), tab_hdr->get_max_search_len(), tab_hdr->get_max_search_len());
      } else {
        double search_len_sum = 0;
        for (uint64_t i = 0; i < tab_hdr->get_capacity(); ++i) {
          search_len_sum += get_target_search_len(i);
        }

        fprintf(stdout, "%lu MERGE %lu %lu %f\n", timestamp, tab_hdr->get_capacity(), tab_hdr->get_max_search_len(), search_len_sum / (double)(tab_hdr->get_capacity() - 1));
      }
#endif
    }
  }
}

inline Directory* MemoryServer::get_dir(Address addr) {
  return (Directory*)(base_addr[MRType::DIRECTORY] + Directory::get_dir_hdr_offset(addr.num));
}

inline Directory* MemoryServer::get_prev_dir(Address addr) {
  return get_dir(get_dir(addr)->get_prev_dir_addr());
}

inline Directory* MemoryServer::get_next_dir(Address addr) {
  return get_dir(get_dir(addr)->get_next_dir_addr());
}

inline Directory* MemoryServer::get_last_dir(void) {
  Address last_dir_addr = tab[tab_hdr->get_capacity()-1].get_dir_ptr();
  Directory* last_dir = get_dir(last_dir_addr);

  while (!last_dir->is_last_dir()) {
    last_dir_addr = last_dir->get_next_dir_addr();
    last_dir = get_dir(last_dir_addr);
  }

  return last_dir;
}