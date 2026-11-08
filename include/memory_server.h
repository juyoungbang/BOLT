#ifndef _MEMORY_SERVER_H
#define _MEMORY_SERVER_H

#include <thread>

#include "rdma.h"
#include "server.h"
#include "merge_manager.h"

void* master_server_main(void * server_arg);
void* memory_server_main(void * server_arg);

class MemoryServer : public Server {
public:
  MemoryServer(const struct ServerConfig& conf);
  ~MemoryServer(void);

  void exit_background_merge(void);
  void* master_busy_wait(void);
  void* busy_wait(void);

  void print_table(uint16_t tab_num);
  void print_table(uint16_t tab_num, uint64_t idx);
  void check_table_sanity(uint16_t tab_num);

private:
  /* Members for network management. */
  struct sockaddr_in pack_udp_server_info(uint16_t idx);
  void reply_and_connect(void);
  void write(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t size, bool signal = true);
  void read(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t size);
  void CAS(void* src, uint16_t sid, uint64_t offset, MRType type, uint64_t compare, uint64_t swap);

  /* Members for background merging. */  
  volatile bool stop_merge;
  std::thread background;

  void* unlock_buffer;

  uint64_t table_per_mn;
  uint64_t max_tab_capacity;
  MergeManager* merge_manager;
  TableHeader** tab_hdr_list;
  TableEntry** tab_list;
  Stash** stash_list;
  uint64_t* dir_alloc_num;
  uint64_t* buc_alloc_num;
  uint64_t* kv_data_alloc_num;

  static thread_local TableHeader* tab_hdr;
  static thread_local TableEntry* tab;
  static thread_local Stash* stash;

#ifndef LOCK_FREE_STASH
  bool lock_stash(uint16_t tab_id);
  void unlock_stash(uint16_t tab_id);
#endif
  Address alloc_dir(void);
  Bucket* get_bucket(Address dest);
  Address get_target_dir_addr(uint64_t key);
  uint64_t get_dir_start_block(Directory* dir);
  uint64_t get_target_search_len(uint64_t idx);
  uint64_t count_merge_dir_e_num(uint64_t merge_stash_e_num);
  void merge_stash(uint64_t& tab_idx, uint64_t& old_idx, uint64_t& stash_start, uint64_t& stash_end,
                   Directory* old_dir, DirectoryEntry* stash, Directory* prev_dir, Directory* new_dir);
  void split_and_merge_stash(uint64_t tab_idx, uint64_t stash_start, uint64_t stash_end, Directory* dir, DirectoryEntry* stash);
#ifdef LOCK_FREE_STASH
  void safely_clear_stash(uint64_t offset, uint64_t snapshot_capacity);
#endif
  void background_merge(void);

  /* Helper functions. */
  inline Directory* get_dir(Address addr);
  inline Directory* get_prev_dir(Address addr);
  inline Directory* get_next_dir(Address addr);
  inline Directory* get_last_dir(void);

  /* Stash status logging. */
  uint64_t start_timestamp;
};

#endif // _MEMORY_SERVER_H