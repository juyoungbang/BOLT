#ifndef _UTILS_H
#define _UTILS_H

#include <queue>
#include <netinet/in.h>
#include <boost/coroutine2/all.hpp>

using CoroPush = boost::coroutines2::coroutine<int>::push_type;
using CoroPull = boost::coroutines2::coroutine<int>::pull_type;

using CoroQueue = std::queue<uint16_t>;

enum MsgType {
  SIGNAL,
  SERVER_INFO,
  THREAD_INFO,
  CLIENT_STAT
};

enum SignalType {
  INIT_DONE,
  READY,
  START,
  HALT,
  END,
  TABLE_UPDATE
};

enum RetryType {
  TAB_META_STALE,
  TAB_ENTRY_STALE,
  WAITING_DIR_UPDATE,
  FOUND_WRONG_BUCKET,
  STASH_SEARCH_FAILURE
};

enum BenchType {
  REAL,
  YCSB,
  TWITTER,
  RDMA_TEST
};

struct CacheStats {
  uint64_t cache_size;
  uint64_t free_size;
  uint64_t skiplist_node_num;

  uint64_t tab_num;
  uint64_t* tab_size;
  bool* tab_cached;
};

struct Signal {
  uint8_t msg;
  uint16_t id;
  uint8_t signal;

  /* For table udpate signal. */
  uint16_t tab_id;

  Signal(void) : msg(MsgType::SIGNAL) { }
};

struct ServerInfo {
  uint8_t msg;
  uint16_t sid;
  uint16_t lid;
  uint8_t  gid[16];
  uint8_t  gid_idx;
  uint32_t qp_num;
  uint32_t mr_rkey[6];
  uint64_t base_size[6];
  uint64_t base_addr[6];

  ServerInfo(void) : msg(MsgType::SERVER_INFO) { }
};

struct ThreadInfo {
  uint8_t msg;
  bool is_server;
  uint16_t id;
  uint64_t tid;
  uint16_t lid;
  uint8_t  gid[16];
  uint8_t  gid_idx;
  uint32_t qp_num;
  uint32_t mr_rkey;
  uint64_t base_addr;
  uint64_t thread_num;

  ThreadInfo(void) : msg(MsgType::THREAD_INFO) { }
};

struct ClientStats {
  uint8_t msg;
  uint16_t cid;
  uint16_t metric;
  
  uint64_t thread_num;
  uint64_t cache_size; // MB
  uint64_t value_size; // B
  uint64_t kv_data_size; // B

  char load_workload_path[100];
  char txn_workload_path[100];

  uint64_t bulk_ops;
  
  uint64_t load_ops;
  uint64_t load_successed_insert;
  uint64_t load_successed_search;
  uint64_t load_successed_others;
  uint64_t load_start;
  uint64_t load_end;
  double load_cache_hit_ratio;
  
  uint64_t txn_ops;
  uint64_t txn_successed_insert;
  uint64_t txn_successed_search;
  uint64_t txn_successed_others;
  uint64_t txn_start;
  uint64_t txn_end;
  double txn_cache_hit_ratio;

  ClientStats(void) : msg(MsgType::CLIENT_STAT) { }
};

static uint64_t htonll(uint64_t val) {
  return (((uint64_t) htonl(val)) << 32) + htonl(val >> 32);
}
 
static uint64_t ntohll(uint64_t val) {
  return (((uint64_t) ntohl(val)) << 32) + ntohl(val >> 32);
}

inline double weighted_avg(double avg_n, double weight_new, double x_new) {
  return (1 - weight_new)*avg_n + weight_new*x_new;
}

inline double incr_avg(double avg_n, uint64_t& n, double x_new) {
  return avg_n + (x_new - avg_n) / ++n;
}

#endif // _UTILS_H