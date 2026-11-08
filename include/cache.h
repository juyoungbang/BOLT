#ifndef _CACHE_H
#define _CACHE_H

#include "city.h"
#include "utils.h"
#include "table.h"
#include "directory.h"
#include "wrlock.h"
#include "cache_entry.h"
#include "third_party/inlineskiplist.h"

#include <atomic>
#include <queue>
#include <vector>
#include <unistd.h>
#include <sys/mman.h>

constexpr double CACHE_FREQ_WEIGHT = 0.90;
constexpr uint64_t CACHE_PROMOTE_THRESHOLD = 1;
constexpr uint64_t FREE_THRESHOLD = 5ULL * 10000000;

extern thread_local std::random_device rd;
extern thread_local std::mt19937_64 gen;
extern thread_local std::uniform_int_distribution<uint64_t> dis;

using CacheSkipList = InlineSkipList<CacheEntryComparator>;

static inline unsigned long long asm_rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

__inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

inline void mfence() { asm volatile("mfence" ::: "memory"); }
inline void compiler_barrier() { asm volatile("" ::: "memory"); }

class Cache {
public:
  Cache(uint64_t _table_num, uint64_t _cache_size, uint64_t _thread_num);
  ~Cache(void);

  bool is_full(void);

  TableHeader get_tab_header(uint16_t tab_id);
  bool promote_tab_to_cache(uint16_t tab_id, bool signal, uint64_t cache_usage);
  void update_tab_to_cache(uint16_t tab_id, TableHeader* hdr, void* tab_entry_buffer);
  bool search_tab_from_cache(const Key_t& k, uint16_t tab_id, TableEntry& low_addr, TableEntry& hi_addr, uint16_t tid);
  
  bool lock_entry(const CacheEntry* entry);
  bool add_to_cache(Directory* dir);
  bool promote_dir_to_cache(Address dir_addr, uint64_t cache_usage);
  bool update_and_unlock_to_cache(const CacheEntry* entry, Directory* dir);
  const CacheEntry* search_from_cache(const Key_t& k, TableEntry low_addr, TableEntry hi_addr, Address* addr, bool& uncertain, uint64_t tid);
  const std::vector<CacheEntry*> search_range_from_cache(const Key_t& from, const Key_t& to, struct ScanAddress &from_addr,
                                                         struct ScanAddress &to_addr, std::vector<Address> &addr, uint16_t tid);
  uint64_t invalidate(const CacheEntry* entry);

  void lazy_free_cache(uint64_t tid);
  void release_leader(uint64_t tid);

  void print_statistics(bool verbose = false); 
  CacheStats* get_statistics(void); 

private:
  uint64_t table_num;
  uint64_t range_size;

  volatile bool need_handover;
  volatile uint64_t leader;

  uint64_t cache_size;
  std::atomic<int64_t> free_size;
  std::atomic<int64_t> skiplist_node_cnt;
  uint64_t thread_num;

  CacheTable** tab;
  CacheSkipList* skiplist;
  CacheEntryComparator cmp;
  Allocator alloc;

#ifdef CACHE_BYPASS
  /* Ghost cache is not fully consistent but is acceptable. */
  double* accessed_cache_freq;
  uint64_t ghost_capacity;
  GhostEntry* ghost_cache;
#endif

  /* Delayed free queue. */
  WRLock tab_free_lock;
  WRLock dir_free_lock;
  std::queue<std::pair<CacheTable*, uint64_t>> delay_tab_free_list;
  std::queue<std::pair<void*, uint64_t>> delay_dir_free_list;

  double calc_avg_cache_freq(void);

  uint16_t get_tab_id(const uint64_t& val);
  bool add_entry(CacheDirectory *ptr);
  CacheSkipList::Iterator find_entry(const uint64_t& lowest);
  const std::vector<CacheDirectory*> find_range_entry(const uint64_t& from, const uint64_t& to);

#ifdef CACHE_BYPASS
  double get_freq_from_ghost(Address addr);
  bool reset_ghost_entry(Address addr, GhostEntry& ghost_entry);
#endif

  CacheTable* get_tab_evict_candidate(double& freq);
  const CacheEntry* get_a_random_entry(double& freq);
  const CacheEntry* get_dir_evict_candidate(double& freq);
  int64_t evict_one(void);
  void consume_cache(uint64_t size);
};

inline Cache::Cache(uint64_t _table_num, uint64_t _cache_size, uint64_t _thread_num)
: table_num(_table_num), need_handover(false), leader(0), cache_size(_cache_size) {
  range_size = (MAX + 1 - MIN) / table_num;

#ifdef CACHE_BYPASS
  assert(cache_size > GHOST_CACHE_SIZE);
  free_size.store(cache_size - GHOST_CACHE_SIZE);
#else
  free_size.store(cache_size);
#endif
  skiplist_node_cnt.store(0);
  thread_num = _thread_num;

  tab = new CacheTable*[table_num];
  for (uint16_t i = 0; i < table_num; ++i) {
    tab[i] = NULL;
  }

  skiplist = new CacheSkipList(cmp, &alloc, 21);

#ifdef CACHE_BYPASS
  accessed_cache_freq = new double[thread_num];
  for (int i = 0; i < thread_num; ++i) {
    accessed_cache_freq[i] = 0.0;
  }

  ghost_capacity = GHOST_CACHE_SIZE/sizeof(GhostEntry);
  ghost_cache = new GhostEntry[ghost_capacity];
  for (uint64_t i = 0; i < ghost_capacity; ++i) {
    ghost_cache[i].addr = Address::null();
    ghost_cache[i].invoc = 0;
    ghost_cache[i].time = std::chrono::high_resolution_clock::now();
  }
#endif
}

inline Cache::~Cache(void) { }

inline bool Cache::is_full(void) {
  return free_size.load() <= 0;
}

inline TableHeader Cache::get_tab_header(uint16_t tab_id) {
  return tab[tab_id]->hdr;
}

inline bool Cache::promote_tab_to_cache(uint16_t tab_id, bool signal, uint64_t cache_usage) {
#ifdef CACHE_BYPASS
  int64_t diff = tab[tab_id]->entry ? tab[tab_id]->get_tab_size() - cache_usage : sizeof(TableHeader) - cache_usage;
  uint64_t required_cache_size = diff >= 0 ? 0 : std::abs(diff);

  int64_t cur_free_size = free_size.load();
  if (cur_free_size > 0 && cur_free_size >= required_cache_size) { // Be aware of different data type
    return true;
  }

  double avg_cache_freq = calc_avg_cache_freq();
  if (tab[tab_id]->get_cache_freq() <= CACHE_PROMOTE_THRESHOLD*avg_cache_freq) {
    return false;
  }

  return true;
#else
  return true;
#endif
}

/* Current caching mechanism assumes that the table can be cached as a whole in all cases. */
inline void Cache::update_tab_to_cache(uint16_t tab_id, TableHeader* hdr, void* tab_entry_buffer) {
  CacheTable* new_tab = (CacheTable*)malloc(sizeof(CacheTable));
  new_tab->init(hdr);

  if (tab_entry_buffer) {
    new_tab->entry = (TableEntry*)malloc(new_tab->get_tab_size());
    memcpy(new_tab->entry, tab_entry_buffer, new_tab->get_tab_size());
  }

retry:
  CacheTable* old_tab = tab[tab_id];
  if (old_tab) {
    new_tab->inherit_cache_freq(old_tab);
  }

  if (__sync_val_compare_and_swap(&tab[tab_id], old_tab, new_tab) != old_tab) {
    goto retry;
  }

  uint64_t old_tab_size = 0;
  if (old_tab != NULL) {
    tab_free_lock.wLock();
    delay_tab_free_list.push(std::make_pair(old_tab, asm_rdtsc()));
    tab_free_lock.wUnlock();

    old_tab_size = old_tab->entry ? old_tab->get_tab_size() : sizeof(TableHeader);
  }
 
  uint64_t new_tab_size = new_tab->entry ? new_tab->get_tab_size() : sizeof(TableHeader);

  assert(new_tab_size < cache_size); 

  int64_t cache_size_diff = old_tab_size - new_tab_size;
  if (cache_size_diff >= 0) {
    free_size.fetch_add(cache_size_diff);
  } else {
    consume_cache(std::abs(cache_size_diff));
  }
}

inline bool Cache::search_tab_from_cache(const Key_t& k, uint16_t tab_id, TableEntry& low_addr, TableEntry& hi_addr, uint16_t tid) {
  CacheTable* tab_ptr = tab[tab_id];

  double freq;
  bool valid_freq = tab_ptr->incr_invocation(freq);

  if (!tab_ptr->is_fully_cached()) {
    return false;
  }

#ifdef CACHE_BYPASS
  if (valid_freq) accessed_cache_freq[tid] = weighted_avg(accessed_cache_freq[tid], CACHE_FREQ_WEIGHT, freq);
#endif

  uint64_t tab_idx = tab_ptr->get_target_block(k);

  low_addr = tab_ptr->get_entry(tab_idx);
  if (tab_idx == tab_ptr->get_capacity() - 1) {
    hi_addr = tab_ptr->get_entry(tab_idx);
    hi_addr.dir_idx = DIR_CAPACITY - 1;
  } else {
    hi_addr = tab_ptr->get_entry(tab_idx+1);
  }

  return true;
}

inline bool Cache::lock_entry(const CacheEntry* entry) {
  assert(entry);

  if (__sync_bool_compare_and_swap(&(entry->lock), 0, 1)) {
    if (entry->ptr == nullptr) {
      return true;
    } else {
      entry->lock = 0;
    }
  }

  return false;
}

inline bool Cache::add_to_cache(Directory* dir) {
  CacheDirectory* new_dir = (CacheDirectory *)malloc(sizeof(CacheDirectory));
  new_dir->init(dir);

#ifdef CACHE_BYPASS
  GhostEntry old_entry;
  if (reset_ghost_entry(new_dir->get_cur_dir_addr(), old_entry)) {
    new_dir->inherit_cache_freq(&old_entry);
  }
#endif

  if (this->add_entry(new_dir)) {
    skiplist_node_cnt.fetch_add(1); 

    consume_cache(sizeof(CacheDirectory));

    return true;
  } else { // Unnecessary read may occur if a directory is new to cache
    free(new_dir);
    return false;
  }
}

inline bool Cache::promote_dir_to_cache(Address dir_addr, uint64_t cache_usage) {
#ifdef CACHE_BYPASS
  int64_t cur_free_size = free_size.load();
  if (cur_free_size > 0 && cur_free_size >= cache_usage) { // Be aware of different data type
    return true;
  } 
  
  double avg_cache_freq = calc_avg_cache_freq();
  if (get_freq_from_ghost(dir_addr) <= CACHE_PROMOTE_THRESHOLD*avg_cache_freq) {
    return false;
  }

  return true;
#else
  return true;
#endif
}

inline bool Cache::update_and_unlock_to_cache(const CacheEntry* entry, Directory* dir) {
  assert(entry);
  assert(entry->dir_addr == dir->get_cur_dir_addr());
  assert(entry->ptr == nullptr);
  assert(entry->lock == 1);

  CacheDirectory* new_dir = (CacheDirectory *)malloc(sizeof(CacheDirectory));
  new_dir->init(dir);

#ifdef CACHE_BYPASS
  GhostEntry old_entry;
  if (reset_ghost_entry(new_dir->get_cur_dir_addr(), old_entry)) {
    new_dir->inherit_cache_freq(&old_entry);
  }
#endif

  entry->lowest = dir->get_entry_lowest(0);
  entry->ptr = new_dir;
  entry->lock = 0;

  consume_cache(sizeof(CacheDirectory));

  return true;
}

inline const CacheEntry* Cache::search_from_cache(const Key_t& k, TableEntry low_addr, TableEntry hi_addr, Address* addr, bool& uncertain, uint64_t tid) {
  lazy_free_cache(tid);

  auto it = find_entry(k);

  uncertain = false;
  CacheEntry* entry = nullptr;
  if (it.Valid()) {
    entry = (CacheEntry*)it.key();
  } else { // Cache is in initial empty status
    uncertain = true;
    return entry;
  }

  assert(entry);
  CacheDirectory* dir = entry->ptr;

  if (hi_addr.get_dir_ptr() == entry->dir_addr) {
    if (!dir) { // Stale
      assert(entry && *addr == Address::null());
      return entry;
    }
  } else if (low_addr.get_dir_ptr() == entry->dir_addr) {
    it.Prev();

    CacheEntry* prev_entry = nullptr;
    if (it.Valid()) {
      prev_entry = (CacheEntry*)it.key();
    }

    if (prev_entry && hi_addr.get_dir_ptr() == prev_entry->dir_addr) { // Entry correctly found
      if (!dir) { // Stale
        assert(entry && *addr == Address::null());
        return entry;
      }
    } else { // hi_addr never been read to cache
      if (!dir) { // Stale and needs to read low_addr, hi_addr anyway
        uncertain = true;
        assert(entry && *addr == Address::null());
        return entry;
      } else if (dir->entry[dir->get_capacity() - 1].get_lowest() < k) { // Fresh but low_addr, hi_addr uncertain
        uncertain = true;
      }
    }
  } else {
    uncertain = true;
    assert(*addr == Address::null());
    return nullptr;
  }

  double freq;
  bool valid_freq = dir->incr_invocation(freq);
#ifdef CACHE_BYPASS
  if (valid_freq) accessed_cache_freq[tid] = weighted_avg(accessed_cache_freq[tid], CACHE_FREQ_WEIGHT, freq);
#endif

  uint64_t low, hi;
  if (hi_addr.get_dir_ptr() == entry->dir_addr) {
    low = low_addr == hi_addr ? low_addr.get_dir_idx() : 0;
    hi = hi_addr.get_dir_idx() + 1;
  } else {
    low = low_addr.get_dir_idx();
    hi = low_addr == hi_addr ? hi_addr.get_dir_idx() + 1 : dir->get_capacity();
  }

  hi = hi > dir->get_capacity() ? dir->get_capacity() : hi;

  while (low < hi) {
    size_t mid = (low + hi) >> 1;

    if (k < dir->get_entry_lowest(mid)) {
      hi = mid;
    } else {
      *addr = dir->entry[mid].ptr;
      low = mid + 1;
    }
  }

  compiler_barrier();
  if (!entry->ptr) { // check if it is freed.
    *addr = Address::null();
  }

  return entry;
}

inline const std::vector<CacheEntry *> Cache::search_range_from_cache(const Key_t &from, const Key_t &to, struct ScanAddress &from_addr,
                                                                      struct ScanAddress &to_addr, std::vector<Address> &addr, uint16_t tid) {
  std::vector<CacheEntry*> res;
  auto entry = find_range_entry(from, to);

  if (entry.size() == 0) return res;

  size_t from_mid, to_mid;

  // Search "from"
  assert(entry[0]);
  if (entry[0]->get_entry_lowest(0) <= to) {
    uint64_t low = 0;
    uint64_t hi = entry[0]->get_capacity();

    double freq;
    bool valid_freq = entry[0]->incr_invocation(freq);
#ifdef CACHE_BYPASS
    if (valid_freq) accessed_cache_freq[tid] = weighted_avg(accessed_cache_freq[tid], CACHE_FREQ_WEIGHT, freq);
#endif

    while (low < hi) {
      size_t mid = (low + hi) >> 1;
      
      if (from < entry[0]->get_entry_lowest(mid)) {
        hi = mid;
      } else {
        from_mid = mid;
        low = mid + 1;
      }
    }

    compiler_barrier();
    if (!entry[0]) { // check if it is freed.
      addr[0] = Address::null();
    }
  } else {
    return res;
  }

SEARCH_TO:
  // Search "to"
  assert(entry.back());
  if (entry.back()->get_entry_lowest(0) <= to) {
    uint64_t low = 0;
    uint64_t hi = entry.back()->get_capacity();

    double freq;
    bool valid_freq = entry.back()->incr_invocation(freq);
#ifdef CACHE_BYPASS
    if (valid_freq) accessed_cache_freq[tid] = weighted_avg(accessed_cache_freq[tid], CACHE_FREQ_WEIGHT, freq);
#endif

    while (low < hi) {
      size_t mid = (low + hi) >> 1;
      
      if (to < entry.back()->get_entry_lowest(mid)) {
        hi = mid;
      } else {
        to_mid = mid;
        low = mid + 1;
      }
    }

    compiler_barrier();
    if (!entry.back()) { // check if it is freed.
      addr.back() = Address::null();
    }
  }

RANGE_END:
  if (entry[0]->get_cur_dir_addr() == entry.back()->get_cur_dir_addr() ) { // [from, to] at same directory
    for (size_t i = from_mid; i <= to_mid; ++i) {
      addr.push_back(entry[0]->entry[i].ptr);
    }
  } else { // [from, to] at different directory
    for (size_t i = from_mid; i < entry[0]->get_capacity(); ++i) {
      addr.push_back(entry[0]->entry[i].ptr);
    }

    for (size_t i = 1; i < entry.size() - 1; ++i) {
      for (size_t j = 0; j < entry[i]->get_capacity(); ++j) {
        assert(entry[i]->get_entry_lowest(j) >= from && entry[i]->get_entry_lowest(j) <= to);
        addr.push_back(entry[i]->entry[j].ptr);
      }
    }

    for (size_t i = 0; i <= to_mid; ++i) {
      addr.push_back(entry.back()->entry[i].ptr);
    }
  }

  return res;
}

inline uint64_t Cache::invalidate(const CacheEntry* entry) {
  auto ptr = entry->ptr;

  if (ptr == nullptr) {
    return 0;
  }

  if (__sync_bool_compare_and_swap(&(entry->lock), 0, 1)) {
    if (__sync_bool_compare_and_swap(&(entry->ptr), ptr, 0)) {
      entry->lock = 0;
      free_size.fetch_add(sizeof(CacheDirectory));

      dir_free_lock.wLock();
      delay_dir_free_list.push(std::make_pair(ptr, asm_rdtsc()));
      dir_free_lock.wUnlock();
      return sizeof(CacheDirectory);
    }

    entry->lock = 0;
  }

  return 0;
}

inline void Cache::lazy_free_cache(uint64_t tid) {
  // Please ensure the leader thread can make progress
  if (need_handover == true && tid != MAX_THREAD_NUM) {
    if (__sync_bool_compare_and_swap(&need_handover, true, false)) {
      __sync_val_compare_and_swap(&leader, leader, tid);
    }
  }

  if (tid != leader) {
    return;
  }

  while (!delay_tab_free_list.empty()) {
    tab_free_lock.wLock();
    auto p = delay_tab_free_list.front();
    if (asm_rdtsc() - p.second > FREE_THRESHOLD) {
      if (p.first->entry) {
        free(p.first->entry);
      }

      free(p.first);

      delay_tab_free_list.pop();
    } else {
      tab_free_lock.wUnlock();
      break;
    }
    tab_free_lock.wUnlock();
  }

  while (!delay_dir_free_list.empty()) {
    dir_free_lock.wLock();
    auto p = delay_dir_free_list.front();
    if (asm_rdtsc() - p.second > FREE_THRESHOLD) {
      free(p.first);

      delay_dir_free_list.pop();
    } else {
      dir_free_lock.wUnlock();
      break;
    }
    dir_free_lock.wUnlock();
  }  
}

inline void Cache::release_leader(uint64_t tid) {
  if (tid == leader) {
    __sync_bool_compare_and_swap(&need_handover, false, true);
  }  
}

/* This function is not thread safe. */
inline void Cache::print_statistics(bool verbose) {
  uint64_t total_tab_size = 0;
  uint64_t total_cached_tab_size = 0;
  for (uint16_t i = 0; i < table_num; ++i) {
    total_tab_size += tab[i]->get_tab_size();
    
    if (tab[i]->entry) {
      total_cached_tab_size += tab[i]->get_tab_size();
    } else {
      total_cached_tab_size += sizeof(TableHeader);
    }
  }

  fprintf(stdout, "\n[Cache statistics]\n");
  fprintf(stdout, "Total cache size: %lu MB\n", cache_size/MB); 
  fprintf(stdout, "  >>> Free cache size: %ld\n", free_size.load());
  fprintf(stdout, "  >>> Used cache size (table): %ld\n", total_cached_tab_size);
  fprintf(stdout, "  >>> Used cache size (directory): %ld\n", cache_size - GHOST_CACHE_SIZE - total_cached_tab_size - free_size.load());
  fprintf(stdout, "Skiplist node number: %ld\n", skiplist_node_cnt.load()); 
  
  fprintf(stdout, "\n[Table statistics]\n");
  fprintf(stdout, "Total table size: %lu B (%lu B cached)\n", total_tab_size, total_cached_tab_size);
  if (verbose) {
    for (uint16_t i = 0; i < table_num; ++i) {
      if (tab[i]->entry) {
        fprintf(stdout, "  >>> Table %u: %lu B (cached)\n", i, tab[i]->get_tab_size());
      } else {
        fprintf(stdout, "  >>> Table %u: %lu B (not cached)\n", i, tab[i]->get_tab_size());
      }
    }
  }
}

/* This function is not thread safe. */
inline CacheStats* Cache::get_statistics(void) {
  CacheStats* cache_stats = new CacheStats;

  cache_stats->cache_size = cache_size;
  cache_stats->free_size = free_size.load();
  cache_stats->skiplist_node_num = skiplist_node_cnt.load();

  cache_stats->tab_num = table_num;
  cache_stats->tab_size = new uint64_t[table_num];
  cache_stats->tab_cached = new bool[table_num];

  for (uint16_t i = 0; i < table_num; ++i) {
    assert(tab[i]);
    cache_stats->tab_size[i] = tab[i]->get_tab_size();
    
    if (tab[i]->entry) {
      cache_stats->tab_cached[i] = true;
    } else {
      cache_stats->tab_cached[i] = false;
    }
  }

  return cache_stats;
}

#ifdef CACHE_BYPASS
inline double Cache::calc_avg_cache_freq(void) {
  double avg = 0;
  for (int i = 0; i < thread_num; ++i) {
    avg += accessed_cache_freq[i];
  }

  return avg/thread_num;
}
#endif

inline uint16_t Cache::get_tab_id(const uint64_t& val) {
  uint16_t tab_id = val / range_size;
  return tab_id < table_num ? tab_id : table_num - 1;
}

inline bool Cache::add_entry(CacheDirectory* ptr) {
  // TODO: Memory leak
  auto buf = skiplist->AllocateKey(sizeof(CacheEntry));
  auto& new_entry = *(CacheEntry*)buf;

  new_entry.dir_addr = ptr->get_cur_dir_addr();
  new_entry.lock = 0;
  new_entry.lowest = ptr->get_entry_lowest(0);
  new_entry.ptr = ptr;

  return skiplist->InsertConcurrently(buf);
}

inline CacheSkipList::Iterator Cache::find_entry(const uint64_t& lowest) {
  CacheSkipList::Iterator iter(skiplist);

  CacheEntry e;
  e.lowest = lowest;

  iter.Seek((char *)&e);

  if (!iter.Valid()) {
    iter.SeekToLast();
  }

  return iter;
}

inline const std::vector<CacheDirectory*> Cache::find_range_entry(const uint64_t& from, const uint64_t& to) {
  std::vector<CacheDirectory*> res;
  CacheSkipList::Iterator iter(skiplist);

  CacheEntry e;
  e.lowest = from;

  iter.Seek((char *)&e);

  while (iter.Valid()) {
    auto val = (CacheEntry *)iter.key();
    if (val->lowest > to) {
      break;
    } else if (val->ptr) {
      res.push_back(val->ptr);
    }
    iter.Prev();
  }

  return res;
}

#ifdef CACHE_BYPASS
/* Duration 0 indicates insufficient statistics.                              **
** In this case, this function returns 0 which forces the entry to be dumped. */
inline double Cache::get_freq_from_ghost(Address addr) {
  addr.ver = 0; // Clearing version is essential for correct operation
  uint64_t hash_idx = CityHash64((char *)&addr, sizeof(addr)) % ghost_capacity;

retry:
  Address ghost_addr = ghost_cache[hash_idx].addr;
  if (ghost_addr == addr) { // Ghost cache hit
    goto finish;
  } else { // Ghost cache miss
    if (__sync_val_compare_and_swap(&ghost_cache[hash_idx].addr.val, ghost_addr.val, addr.val) == ghost_addr.val) {
      ghost_cache[hash_idx].invoc.store(0);
      ghost_cache[hash_idx].time = std::chrono::high_resolution_clock::now();

      goto finish;
    } else {
      goto retry;
    }
  }

finish:  
  double invoc = ghost_cache[hash_idx].invoc.fetch_add(1) + 1;
  double duration = std::chrono::duration_cast<time_unit>(std::chrono::high_resolution_clock::now() - ghost_cache[hash_idx].time).count();
  
  if (duration) {
    return invoc/duration;
  } else {
    return 0;
  }
}

inline bool Cache::reset_ghost_entry(Address addr, GhostEntry& ghost_entry) {
  addr.ver = 0; // Clearing version is essential for correct CAS operation
  uint64_t hash_idx = CityHash64((char *)&addr, sizeof(addr)) % ghost_capacity;

  ghost_entry.invoc = ghost_cache[hash_idx].invoc.load();
  ghost_entry.time = ghost_cache[hash_idx].time;

  Address reset_addr = Address::null();
  if (__sync_val_compare_and_swap(&ghost_cache[hash_idx].addr.val, addr.val, reset_addr.val) == addr.val) { // Reset success
    return true;
  } else { // History not found
    return false;
  }
}
#endif

inline CacheTable* Cache::get_tab_evict_candidate(double& freq) {
  CacheTable* target_tab = NULL;
  for (uint16_t i = 0; i < table_num; ++i) {
    CacheTable* cand_tab = tab[i];
    if (cand_tab->entry) {
      if (!target_tab || cand_tab->get_cache_freq() < freq) {
        target_tab = cand_tab;
        freq = cand_tab->get_cache_freq();
      }
    }
  }

  return target_tab;
}

inline const CacheEntry* Cache::get_a_random_entry(double& freq) {
  int retry = 0;
retry:
  retry++;
  if (retry > 1000) {
    return nullptr;
  }

  auto k = (dis(gen) % (tab[table_num-1]->get_max_key() - tab[0]->get_min_key())) + tab[0]->get_min_key();
  auto it = this->find_entry(k);
  if (!it.Valid()) {
    goto retry;
  }

  auto e = (CacheEntry*)it.key();

  auto ptr = e->ptr;
  if (!ptr) {
    goto retry;
  }

  freq = ptr->get_cache_freq();
  if (e->ptr != ptr) {
    goto retry;
  }

  return e;
}

inline const CacheEntry* Cache::get_dir_evict_candidate(double& freq) {
  double freq1, freq2;
  const CacheEntry* e1 = get_a_random_entry(freq1);
  const CacheEntry* e2 = get_a_random_entry(freq2);

  const CacheEntry* target_e;
  if (e1 && (!e2 || freq1 <= freq2)) {
    freq = freq1;
    target_e = e1;
  } else if (e2 && (!e1 || freq1 > freq2)) {
    freq = freq2;
    target_e = e2;
  } else {
    return nullptr;
  }

  return target_e;
}

inline int64_t Cache::evict_one(void) {
retry_tab:
  double cand_tab_freq;
  CacheTable* cand_tab = get_tab_evict_candidate(cand_tab_freq);

retry_dir:
  double cand_dir_freq;
  const CacheEntry* cand_dir = get_dir_evict_candidate(cand_dir_freq);

  if (cand_dir && (!cand_tab || cand_dir_freq <= cand_tab_freq)) { // Final candidate is directory
    if (!invalidate(cand_dir)) {
      goto retry_dir;
    }

    return sizeof(CacheDirectory);
  } else if (cand_tab && (!cand_dir || cand_dir_freq > cand_tab_freq)) { // Final candiate is table
    uint64_t old_tab_size = cand_tab->get_tab_size();

    CacheTable* new_tab = (CacheTable*)malloc(sizeof(CacheTable));
    new_tab->init(&cand_tab->hdr);

    if (__sync_val_compare_and_swap(&tab[cand_tab->get_tab_id()], cand_tab, new_tab) != cand_tab) {
      free(new_tab);
      goto retry_tab;
    } else {
      tab_free_lock.wLock();
      delay_tab_free_list.push(std::make_pair(cand_tab, asm_rdtsc()));
      tab_free_lock.wUnlock();
    }

    free_size.fetch_add(old_tab_size-sizeof(TableHeader));
    
    return old_tab_size-sizeof(TableHeader);
  } else {
    return 0;
  }
}

inline void Cache::consume_cache(uint64_t size) {
  auto v = free_size.fetch_add(-size);

  int64_t need_to_evict = 0;
  if (v <= 0) {
    need_to_evict = size;
  } else if (v < size) {
    need_to_evict = size - v;
  }

  while (need_to_evict > 0) {
    need_to_evict -= evict_one();
  }
}

#endif // _CACHE_H