#ifndef _CACHE_ENTRY_H
#define _CACHE_ENTRY_H

#include "bolt.h"

using time_unit = std::chrono::milliseconds; 

struct GhostEntry {
  Address addr;
  std::atomic<uint64_t> invoc;
  std::chrono::high_resolution_clock::time_point time;
};

struct alignas(64) CacheTable {
  TableHeader hdr;
  TableEntry* entry;
  std::atomic<uint64_t> invoc;
  std::chrono::high_resolution_clock::time_point time;

  inline void init(TableHeader* _hdr) {
    memcpy(&hdr, _hdr, sizeof(TableHeader));
    entry = nullptr;
    
    invoc.store(1);
    time = std::chrono::high_resolution_clock::now();
  }

  inline bool is_fully_cached(void) {
    return entry;
  }

  inline uint64_t get_tab_id(void) {
    return hdr.get_tab_id();
  }

  inline uint64_t get_min_key(void) {
    return hdr.get_min_key();
  }

  inline uint64_t get_max_key(void) {
    return hdr.get_max_key();
  }

  inline uint64_t get_capacity(void) {
    return hdr.get_capacity();
  }

  /* Duration 0 indicates insufficient statistics.                                  **
  ** In this case, this function returns towards forcing the current cached status. */
  inline double get_cache_freq(void) {
    double duration = std::chrono::duration_cast<time_unit>(std::chrono::high_resolution_clock::now() - time).count();
    if (is_fully_cached() || duration) {
      return invoc.load()/duration;
    } else {
      return 0;
    }
  }

  inline void inherit_cache_freq(CacheTable* old_tab) {
    invoc.store(old_tab->invoc);
    time = old_tab->time;
  }

  /* Return whether sufficient statistics has been accumulated after invocation increment. */
  inline bool incr_invocation(double& freq) {
    uint64_t updated_invoc = invoc.fetch_add(1) + 1;
    double duration = std::chrono::duration_cast<time_unit>(std::chrono::high_resolution_clock::now() - time).count();
    
    if (duration) {
      freq = updated_invoc/duration;
      return true;
    } else {
      return false;
    }    
  }

  inline uint64_t get_tab_size(void) {
    return sizeof(TableHeader) + hdr.get_tab_size();
  }

  inline uint64_t get_target_block(Key_t key) {
    return hdr.get_target_block(key);
  }

  inline TableEntry get_entry(uint64_t idx) {
    return entry[idx];
  }
};

struct alignas(64) CacheDirectory {
  DirectoryHeader hdr;
  DirectoryEntry entry[DIR_CAPACITY];
  std::atomic<uint64_t> invoc;
  std::chrono::high_resolution_clock::time_point time;

  inline void init(Directory* dir) {
    memcpy(&hdr, &(dir->hdr), sizeof(DirectoryHeader));
    memcpy(entry, dir->entry, dir->get_capacity()*sizeof(DirectoryEntry));

    invoc.store(1);
    time = std::chrono::high_resolution_clock::now();
  }
  
  inline uint64_t get_version(void) {
    return hdr.get_version();
  }

  inline uint64_t get_capacity(void) {
    return hdr.get_capacity();
  }

  inline Address get_cur_dir_addr(void) {
    return hdr.cur_dir_addr;
  }

  /* Duration 0 indicates insufficient statistics.                                       **
  ** In this case, this function returns inf which forces the entry to be kept in cache. */
  inline double get_cache_freq(void) {
    double duration = std::chrono::duration_cast<time_unit>(std::chrono::high_resolution_clock::now() - time).count();
    return invoc.load()/duration;
  }

  inline void inherit_cache_freq(GhostEntry* old_entry) {
    invoc.store(old_entry->invoc);
    time = old_entry->time;
  }

  /* Return whether sufficient statistics has been accumulated after invocation increment. */
  inline bool incr_invocation(double& freq) {
    uint64_t updated_invoc = invoc.fetch_add(1) + 1;
    double duration = std::chrono::duration_cast<time_unit>(std::chrono::high_resolution_clock::now() - time).count();
    
    if (duration) {
      freq = updated_invoc/duration;
      return true;
    } else {
      return false;
    }    
  }

  inline uint64_t get_entry_lowest(uint64_t idx) {
    return entry[idx].get_lowest();
  }
};

struct CacheEntry {
  Address dir_addr;
  mutable uint64_t lock;
  mutable uint64_t lowest;
  mutable CacheDirectory *ptr;
} __attribute__((packed));

static_assert(sizeof(CacheEntry) == 4 * sizeof(uint64_t), "XXX");

inline std::ostream &operator<<(std::ostream &os, const CacheEntry &obj) {
  os << "(" << obj.lowest << ")";
  return os;
}

inline static CacheEntry Decode(const char *val) { return *(CacheEntry *)val; }

struct CacheEntryComparator {
  typedef CacheEntry DecodedType;

  static DecodedType decode_key(const char *b) { return Decode(b); }

  /* We manage skiplist in descending order. */
  int cmp(const DecodedType a_v, const DecodedType b_v) const {
    if (a_v.lowest > b_v.lowest) {
      return -1;
    } else if (a_v.lowest < b_v.lowest) {
      return +1;
    } else {
      return 0;
    }
  }

  int operator()(const char *a, const char *b) const {
    return cmp(Decode(a), Decode(b));
  }

  int operator()(const char *a, const DecodedType b) const {
    return cmp(Decode(a), b);
  }
};

#endif // _CACHE_ENTRY_H