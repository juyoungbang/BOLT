#ifndef _CLIENT_H
#define _CLIENT_H

#include <vector>
#include <infiniband/verbs.h>

#include "utils.h"
#include "config.h"
#include "rdma.h"
#include "cache.h"
#include "buffer.h"
#include "table.h"
#include "bucket.h"
#include "local_lock_table.h"
#include "local_allocator.h"
#include "idx_cache.h"
#include "batch.h"
#include "bolt.h"
#include "address.h"
#include "thread_connection.h"

/* Maps thread ID to logical CPU ID in the desired assignment order **
** - Round-robin across CPUs and cores excluding last core per CPU  **
** - Then assign last cores of each CPU                             */                     
int map_thread_to_logical_cpu_rr(int tid);

/* Maps thread ID to logical CPU ID in the desired assignment order           **
** - Use only the cores in the designated numa node                           **
** - Delay activating hyperthreading until there is not enough physical cores */      
int map_thread_to_logical_cpu_numa(int tid, int numa_node) ;

struct OnChipLocalLock {
  std::atomic<uint64_t> ticket_lock;
  bool hand_over;
  uint8_t hand_time;
};

class Client {
public:
  Client(const struct ClientConfig& conf, uint64_t thread_num, uint16_t _coroutine_num);
  ~Client(void);

  void init_bolt(void);
  void init_cache(void);
  void reset_cache(void);

  using WorkFunc = std::function<void (Client*, const Request&, int, uint64_t, CoroPull*, uint16_t)>;
  void run_coroutine(CoroFunc func, uint16_t coro_cnt, WorkFunc work_func, int phase, const std::vector<Request>& requests); 
  void run_router_coroutine(CoroRouterFunc func, uint16_t coro_cnt, WorkFunc work_func, int phase, Queue* gq); 

  void insert(const Key_t& key, Value_t value, CoroPull* sink = nullptr);
  Value_t search(const Key_t& key, CoroPull* sink = nullptr);
  uint64_t range_query(const Key_t& from, const Key_t& to, Value_t* res, CoroPull* sink = nullptr);

  void reset_thread(void);
  void register_thread(uint64_t tid);
  void clear_statistics(void);

  void send_signal(SignalType sig);
  void broadcast_signal(SignalType sig);  
  void wait_signal(SignalType sig);
  void send_stats(ClientStats* stats);

  CacheStats* get_cache_statistics(void) {
    assert(index_cache != NULL);
    return index_cache->get_statistics();
  }

  void release_cache_leader(uint64_t tid) {
    assert(index_cache != NULL);
    index_cache->release_leader(tid);
  }

private:
  uint16_t cid; // Client IDs must be sequential values from 0 to client_num-1
  uint16_t server_num;
  uint16_t client_num;
  char server_ips[16][16];
  uint64_t thread_num;

  uint64_t table_per_mn;
  uint64_t range_size;

  uint64_t cache_size;
  uint16_t coroutine_num;

  bool is_roce;
  uint16_t udp_port;
  uint32_t udp_sock;

  std::vector<ThreadConnection*> t_conns;
  std::vector<LocalAllocator*> local_allocators;

  static thread_local Buffer* buf;
  static thread_local uint64_t t_conn_id;
  static thread_local uint64_t t_numa_id;
  static thread_local ThreadConnection* t_conn;
  static thread_local LocalAllocator* local_allocator;

  static thread_local std::vector<CoroPush> workers;
  static thread_local CoroQueue busy_waiting_queue;

  void coro_worker(CoroPull &sink, RequstGen *gen, uint16_t coro_id, WorkFunc work_func, int phase);
  void coro_router_worker(CoroPull &sink, RequstGen *gen, uint16_t coro_id, WorkFunc work_func, int phase);

  /* Functions for network management. */
  bool timeout_udp_socket(uint64_t timeout_sec = 0);
  void send_udp_info(uint64_t tid, struct ibv_qp* ib_qp, struct sockaddr_in udp_server_info);
  bool recv_udp_info(struct ServerInfo* info);
  bool recv_signal(struct Signal& sig);
  void write(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t size, CoroPull* sink, bool signal = true);
  void batched_write(Batch* batch, CoroPull* sink);
  void read(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t size, CoroPull* sink);
  void batched_read(Batch* batch, CoroPull* sink);
  void CAS(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t compare, uint64_t swap, CoroPull* sink);
  void masked_CAS(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t compare, uint64_t swap, uint64_t mask, CoroPull* sink);
  void FAA(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t add, CoroPull* sink);
  void masked_FAA(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t add, uint64_t mask, CoroPull* sink);

  std::atomic<uint64_t> global_signal_lock;
  std::atomic<uint64_t>* numa_signal_lock[CPU_NUM];
  std::atomic<uint64_t> local_tab_locks[TOTAL_TAB_NUM];
  
  Cache* index_cache;
  OnChipLocalLock** local_locks;
  LocalLockTable* local_lock_table;
#ifdef SPECULATIVE_READ
  IdxCache* key_cache;
#endif

  /* Functions for index management. */
  void check_tab_update(uint16_t tab_id, CoroPull* sink);
  bool sync_tab(uint16_t tab_id, CoroPull* sink);
#ifndef LOCK_FREE_STASH
  void lock_stash(void* buffer, uint16_t sid, uint64_t offset, CoroPull* sink);
  void unlock_stash(void* buffer, uint16_t sid, uint64_t offset, CoroPull* sink);
#endif
  bool alloc_stash(void* buffer, uint16_t sid, uint64_t& offset, CoroPull* sink);
  bool acquire_local_lock(Address lock_addr, CoroPull* sink);
  bool can_hand_over(Address lock_addr);
  void releases_local_lock(Address lock_addr);
  void lock_bucket(void* buffer, Address lock_addr, CoroPull* sink);
  void unlock_bucket(void* buffer, Address lock_addr, CoroPull* sink);
  void write_and_unlock_bucket(void* bucket_buffer, uint16_t bucket_sid, uint64_t bucket_offset, uint64_t bucket_size, void* lock_buffer, Address lock_addr, CoroPull* sink);
#ifdef SPECULATIVE_READ
  bool speculative_read(Bucket* bucket, Address& dest, const Key_t& k, Value_t& v, int& speculative_idx, bool for_write, CoroPull* sink);
#endif
  Bucket* read_bucket(void* buffer, Address dest, bool locked, CoroPull* sink);
  Address alloc_bucket(void* buffer, uint16_t sid, CoroPull* sink);
#ifdef VAR_VALUE_SIZE
  DataPtr alloc_kv_data(void* buffer, CoroPull* sink);
  void write_kv_data(Value_t& value, CoroPull* sink);
#endif
  bool search_table(const Key_t& k, uint16_t tab_id, TableEntry& low_addr, TableEntry& hi_addr, CoroPull* sink);
  Address search_directory(const Key_t& key, const CacheEntry* entry, TableEntry tab_entry, uint64_t low_idx, uint64_t hi_idx, int cache_flag, CoroPull* sink);
  Address search_stash(const Key_t& key, uint16_t sid, uint64_t offset, CoroPull* sink);

  inline int dump_or_update_cache(uint16_t tab_id, const CacheEntry* entry);
  inline bool is_consistent(DirectoryEntry* dir, uint64_t length);
  inline Directory* read_directory_and_update_cache(void* buffer, const CacheEntry* entry, Address dir_addr, bool new_entry, CoroPull* sink);
  inline DirectoryEntry* read_directory_partial(void* buffer, Address dir_addr, uint64_t s_idx, uint64_t length, CoroPull* sink);

  /* Helper functions. */
  inline uint16_t get_tab_id(const Key_t& key);
  inline uint16_t get_tab_sid(uint16_t tab_id);
  inline uint64_t get_tab_start_offset(uint16_t sid, uint16_t tab_id);
  inline uint64_t get_stash_start_offset(uint16_t sid, uint16_t tab_id);
  inline RDMAInfo* create_batch(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t size);
  inline void yield_coroutine(CoroPull* sink);
};

#endif // _CLIENT_H