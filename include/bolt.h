#ifndef _BOLT_H
#define _BOLT_H

#include <cmath>
#include <limits>
#include <vector>
#include <functional>
#include <optional>

#include "batch.h"

/* Environment parameters. */
#define CPU_NUM 2
#define CORE_PER_CPU 16
#define HYPERTHREAD_PER_CORE 2
#define MN_CORE 0
#define LOC_ROUTER_CORE 63
#define ON_CHIP_SIZE 256*1024

/* Design choice. */
#define CACHE_BYPASS
#define TAB_PARTIAL_READ
#define DIR_PARTIAL_READ
#define LOCK_FREE_STASH
#define MERGE_SCHEDULE
//#define SPECULATIVE_READ
//#define VAR_VALUE_SIZE

/* User adjustable parameters. */
constexpr uint64_t TOTAL_TAB_NUM = 2048; // G
constexpr uint64_t STASH_CAPACITY = 128;
constexpr uint64_t BUCKET_CAPACITY = 62;
constexpr uint64_t MAX_SEARCH_LENGTH = 128;
constexpr uint64_t MERGE_ALPHA_ENTRY = 10;
constexpr double   MERGE_TRIGGER_UTIL = 0.5;
constexpr uint64_t MAX_HANDOVER_TIME = 8;
constexpr uint64_t GHOST_CACHE_SIZE = 300 * 1024;

constexpr uint64_t MAX_CORO_NUM = 8;
constexpr uint64_t LOCAL_LOCK_NUM = 4 * 1024 * 1024;
constexpr uint64_t HOTSPOTBUFFER_SIZE = 30 * 1024 * 1024;
constexpr uint64_t MEMORY_CHUNK_SIZE  = 16 * 1024 * 1024;

/* Fixed values. */
using Key_t = uint64_t;
using Value_t = uint64_t;

constexpr Key_t    MIN = std::numeric_limits<Key_t>::min() >> 8;
constexpr Key_t    MAX = std::numeric_limits<Key_t>::max() >> 8;
constexpr Key_t    INVALID_KEY = 0;
constexpr Value_t  INVALID_VAL = 0;

constexpr uint64_t MB = 1024 * 1024;
constexpr uint64_t MAX_THREAD_NUM = CPU_NUM * CORE_PER_CPU * HYPERTHREAD_PER_CORE;
constexpr uint64_t ON_CHIP_LOCK_NUM = ON_CHIP_SIZE/sizeof(uint64_t);

/* Do NOT change this. */
constexpr uint64_t DIR_CAPACITY = MAX_SEARCH_LENGTH*2;

/* Activated when variable value size option is on. */
constexpr uint64_t SIMULATED_VALUE_SIZE = 8;
constexpr uint64_t KV_DATA_SIZE = sizeof(uint64_t)*2 + SIMULATED_VALUE_SIZE;

/* MN binded core should be avaialable. */
static_assert(MN_CORE <= MAX_THREAD_NUM, "XXX");

/* Table entry can only express up to this limit. */
static_assert(DIR_CAPACITY <= std::pow(2, 16), "XXX");

/* StashHeader can only express up to this limit. */
static_assert(STASH_CAPACITY < std::pow(2, 32), "XXX");

/* This is required for MAX to successfully indicate invalid directory number. */
static_assert(MAX > std::pow(2, 44), "XXX");

/* Memory regions of BOLT. */
enum MRType {
  TABLE,
  DIRECTORY,
  STASH,
  BUCKET,
  KV_DATA,
  ON_CHIP
};

enum OpType : uint8_t {
  INSERT,
  SEARCH,
  UPDATE,
  READ_MODIFY_WRITE,
  RANGE_QUERY
};

struct Request {
  OpType op;
  uint16_t range_size;
  Key_t key;
};

class RequstGen {
public:
  RequstGen() = default;
  virtual std::optional<Request> next() { return Request{}; }
  virtual bool is_finished() { return false; }
};

class Queue {
public:
  Queue() = default;
  virtual void push(Request req) { }
  virtual std::optional<Request> pop_blocking() { return Request{}; }
  virtual void close() { }
  virtual bool is_closed() { return false; }
};

using CoroFunc = std::function<RequstGen* (uint16_t, uint16_t, const std::vector<Request>&)>;
using CoroRouterFunc = std::function<RequstGen* (Queue*)>;

struct u64 {
  uint64_t ver : 8;
  uint64_t val : 56;

  u64() : ver(0), val(0) { }
} __attribute__((packed));

struct two_ver_u64 {
  uint64_t n_ver : 4;
  uint64_t e_ver : 4;
  uint64_t val : 56;

  two_ver_u64() : n_ver(0), e_ver(0), val(0) { }
} __attribute__((packed));

#endif // _BOLT_H