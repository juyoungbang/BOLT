#ifndef _DIRECTORY_H
#define _DIRECTORY_H

#include "bolt.h"
#include "address.h"

const uint64_t STASH_FAA_MASK = (1UL << 63) & (1UL << 31);
const uint64_t STASH_USED_MASK = (~0UL << 32);

struct DirectoryEntry {
  u64 lowest;
  Address ptr;

  DirectoryEntry(void) {
    ptr = Address::null();
  }

  DirectoryEntry(uint64_t _lowest, Address bucket_addr) {
    lowest.val = _lowest;
    ptr = bucket_addr;
  }
  
  inline uint64_t get_version(void) {
    return lowest.ver;
  }
  
  inline uint64_t get_lowest(void) {
    return lowest.val;
  }

  inline bool is_consistent(uint64_t version) {
    if (lowest.ver != version || ptr.ver != version) { 
      return false; 
    }
    
    return true;
  }

  inline void set_consistent(uint64_t version) {
    lowest.ver = version;
    ptr.ver = version;
  }

  inline void clear(void) {
    ptr = Address::null();
  }

  inline void operator=(const DirectoryEntry &entry) {
    lowest.val = entry.lowest.val;
    ptr = entry.ptr;
  } 
};

struct less_than_entry {
  inline bool operator() (const DirectoryEntry& e1, const DirectoryEntry& e2) {
    return (e1.lowest.val < e2.lowest.val);
  }
};

struct DirectoryHeader {
  u64 capacity;
  Address cur_dir_addr;
  Address prev_dir_addr;
  Address next_dir_addr;
  
  DirectoryHeader(void) {
    capacity.val = 1;
    cur_dir_addr = Address::null();
    prev_dir_addr = Address::null();
    next_dir_addr = Address::null();
  }

  inline bool is_first_dir(void) {
    return prev_dir_addr == Address::null();
  }

  inline bool is_last_dir(void) {
    return next_dir_addr == Address::null();
  }
  
  inline uint64_t get_capacity(void) {
    return capacity.val;
  }

  inline uint64_t get_dir_size(void) {
    return capacity.val*sizeof(DirectoryEntry);
  }

  /* Return the representative version of the header. */
  inline uint64_t get_version(void) {
    return capacity.ver;
  }

  /* Increment the representative version of the header. */
  inline uint64_t incr_version(void) {
    return ++capacity.ver;
  }

  inline bool is_consistent(uint64_t version) {
    if (capacity.ver != version ||
        cur_dir_addr.ver != version ||
        prev_dir_addr.ver != version ||
        next_dir_addr.ver != version) { 
      return false; 
    }
    
    return true;
  }
  
  inline void set_consistent(uint64_t version) {
    capacity.ver = version;
    cur_dir_addr.ver = version;
    prev_dir_addr.ver = version;
    next_dir_addr.ver = version;
  }
};

struct Directory {
  DirectoryHeader hdr;
  DirectoryEntry entry[DIR_CAPACITY];

  Directory(Address _cur_dir_addr) {
    hdr.cur_dir_addr = _cur_dir_addr;
  }

  Directory(uint64_t _capacity, Address _cur_dir_addr, Address _prev_dir_addr, Address _next_dir_addr) {
    hdr.capacity.val = _capacity;
    hdr.cur_dir_addr = _cur_dir_addr;
    hdr.prev_dir_addr = _prev_dir_addr;
    hdr.next_dir_addr = _next_dir_addr;
  }

  /* Get the offset of the target directory number. */
  inline static uint64_t get_dir_hdr_offset(uint64_t num) {
    return sizeof(uint64_t) + num*sizeof(Directory);
  }

  /* Get the offset of target directory entry. */
  inline static uint64_t get_dir_offset(uint64_t num, uint64_t idx) {
    return get_dir_hdr_offset(num) + sizeof(DirectoryHeader) + idx*sizeof(DirectoryEntry);
  }

  inline bool is_first_dir(void) {
    return hdr.is_first_dir();
  }

  inline bool is_last_dir(void) {
    return hdr.is_last_dir();
  }

  inline uint64_t get_capacity(void) {
    return hdr.capacity.val;
  }

  inline void set_capacity(uint64_t _capacity) {
    hdr.capacity.val = _capacity;
  }

  inline uint64_t get_dir_size(void) {
    return hdr.get_dir_size();
  }

  inline Address get_cur_dir_addr(void) {
    return hdr.cur_dir_addr;
  }

  inline Address get_prev_dir_addr(void) {
    return hdr.prev_dir_addr;
  }

  /* Changing version might cause consistency issue. */
  inline void set_prev_dir_addr(Address addr) {
    hdr.prev_dir_addr = addr;
  }

  inline Address get_next_dir_addr(void) {
    return hdr.next_dir_addr;
  }

  /* Changing version might cause consistency issue. */
  inline void set_next_dir_addr(Address addr) {
    hdr.next_dir_addr = addr;
  }

  inline uint64_t get_entry_lowest(uint64_t idx) {
    return entry[idx].get_lowest();
  }

  inline uint64_t get_version(void) {
    return hdr.get_version();
  }

  inline void set_merge_flag(void) {
    hdr.set_consistent(hdr.incr_version());
  }

  inline bool is_consistent(void) {
    uint64_t version = get_version();

    if (!hdr.is_consistent(version)) {
      return false;
    }

    for (int i = 0; i < DIR_CAPACITY; ++i) {
      if (!entry[i].is_consistent(version)) {
        return false;
      }
    }

    return true;
  }

  inline void set_consistent(uint64_t version) {
    hdr.set_consistent(version);
    for (int i = 0; i < DIR_CAPACITY; ++i) {
      entry[i].set_consistent(version);
    }
  }
};

struct StashHeader {
#ifndef LOCK_FREE_STASH
  uint64_t lock;
#endif
  union {
    struct {
      uint64_t start : 32;
      uint64_t entry_used : 32;
    };
    uint64_t val;
  };

  StashHeader(void) : val(0) {
#ifndef LOCK_FREE_STASH
  lock = 0;
#endif
  }

  inline bool full(void) {
    return entry_used >= STASH_CAPACITY;
  }

  inline bool empty(void) {
    return entry_used == 0;
  }

  inline bool stall(void) {
    return entry_used > STASH_CAPACITY;
  }

  static inline uint64_t get_stash_size(void) {
    return STASH_CAPACITY*sizeof(DirectoryEntry);
  }  
  
  /* Return index in a circular array. */
  inline int transpose_idx(int idx) {
    return (start + idx) % STASH_CAPACITY; 
  }
};

struct Stash {
  StashHeader hdr;
  DirectoryEntry entry[STASH_CAPACITY];

  /* Get the offset of target stash. */
  inline static uint64_t get_start_offset(uint64_t table_per_mn, uint16_t tab_num, uint64_t base_size) {
    uint64_t quotient = base_size / table_per_mn;
    uint64_t remainder = quotient % 8;

    if (remainder == 0) {
      return tab_num * quotient;
    } else {
      return tab_num * (quotient + 8 - remainder);
    }
  }

  /* Get the offset of target stash index. */
  inline static uint64_t get_stash_offset(uint64_t idx) {
    return sizeof(StashHeader) + idx*sizeof(DirectoryEntry);
  }

  inline bool full(void) {
    return hdr.full();
  }

  inline bool empty(void) {
    return hdr.empty();
  }

  inline bool stall(void) {
    return hdr.stall();
  }

  inline uint64_t get_start(void) {
    return hdr.start;
  }

  inline uint64_t get_entry_used(void) {
    return hdr.entry_used <= STASH_CAPACITY ? hdr.entry_used : STASH_CAPACITY;
  }

  /* Return entry in a circular array. */
  inline DirectoryEntry* get_target_entry(int idx) {
    return &entry[hdr.transpose_idx(idx)];
  }

#ifdef LOCK_FREE_STASH
  /* Fully updated entry should have version 1. */
  uint64_t get_safe_entry_used(void) {
    uint64_t safe_entry_used = 0;
    for (; safe_entry_used < STASH_CAPACITY; ++safe_entry_used) {
      DirectoryEntry* e = get_target_entry(safe_entry_used);
      if (!e->is_consistent(1)) {
        break;
      }
    }    

    return safe_entry_used;
  }
#endif

  DirectoryEntry* create_snapshot(uint64_t snapshot_capacity) {
    DirectoryEntry* snapshot = new DirectoryEntry[snapshot_capacity];
    if (hdr.start + snapshot_capacity > STASH_CAPACITY) {
      std::copy(entry+hdr.start, entry+STASH_CAPACITY, snapshot);
      std::copy(entry, entry+hdr.transpose_idx(snapshot_capacity), snapshot+STASH_CAPACITY-hdr.start);
    } else {
      std::copy(entry+hdr.start, entry+hdr.start+snapshot_capacity, snapshot);
    }

    // Sort snapshot
    std::sort(snapshot, snapshot+snapshot_capacity, less_than_entry());  

    return snapshot;
  }

#ifndef LOCK_FREE_STASH
  void commit_merge(uint64_t snapshot_capacity) {
    hdr.start = (hdr.start + snapshot_capacity) % STASH_CAPACITY;
    hdr.entry_used -= snapshot_capacity;
  }
#endif
};

#endif // _DIRECTORY_H