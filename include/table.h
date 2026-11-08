#ifndef _TABLE_H
#define _TABLE_H

#include "directory.h"
#include "bolt.h"

class TableEntry {
public:
  union {
    struct {
      uint64_t n_ver : 4;
      uint64_t e_ver : 4;
      uint64_t dir_ver : 8; // Version of the directory pointing to
      uint64_t dir_idx : 16; // Index of the entry within directory pointing to
      uint64_t next_dir_num : 32; // Address of the next directory that ptr is pointing to
    };
    uint64_t val;
  };

  Address ptr;

  TableEntry(void) 
  : val(0) { }

  TableEntry(uint64_t _sid, uint64_t _num) 
  : val(0) {
    ptr.sid = _sid;
    ptr.num = _num;
  }

  TableEntry(uint64_t _n_ver, uint64_t _e_ver, uint64_t _dir_ver, uint64_t _dir_idx, uint64_t _next_dir_num, Address _ptr) 
  : n_ver(_n_ver), e_ver(_e_ver), dir_ver(_dir_ver), dir_idx(_dir_idx), next_dir_num(_next_dir_num), ptr(_n_ver, _e_ver, _ptr.sid, _ptr.num) { }

  inline uint64_t get_tab_version(void) {
    return n_ver;
  }

  inline uint64_t get_entry_version(void) {
    return e_ver;
  }

  inline uint64_t get_dir_version(void) {
    return dir_ver;
  }

  inline uint64_t get_dir_idx(void) {
    return dir_idx;
  }
  
  inline uint64_t get_next_dir_num(void) {
    return next_dir_num;
  }

  inline Address get_dir_ptr(void) {
    return ptr;
  }

  inline bool is_node_consistent(uint64_t node_version) {
    if (n_ver != node_version ||
        ptr.n_ver != node_version) {
      return false; 
    }
    
    return true;
  }
  
  inline bool is_entry_consistent(void) {
    if (e_ver != ptr.e_ver) {
      return false; 
    }
    
    return true;
  }

  inline void operator=(const TableEntry &addr) {
    val = addr.val;
    ptr = addr.ptr;
  }
} __attribute__((packed));

inline bool operator==(const TableEntry &lhs, const TableEntry &rhs) {
  return lhs.ptr == rhs.ptr;
}

inline bool operator!=(const TableEntry &lhs, const TableEntry &rhs) {
  return lhs.ptr != rhs.ptr;
}

struct ScanAddress {
  uint16_t tab_id;
  TableEntry low;
  TableEntry hi;
};

struct TableHeader {
  two_ver_u64 tab_id;
  two_ver_u64 min_key;
  two_ver_u64 max_key;
  two_ver_u64 stride;
  two_ver_u64 capacity;

  two_ver_u64 max_search_len; // This metadata has no roles on client side

  TableHeader(uint16_t _tab_id, uint64_t _min_key, uint64_t _max_key) { 
    tab_id.val = _tab_id;
    min_key.val = _min_key;
    max_key.val = _max_key;
    capacity.val = 1;
    max_search_len.val = 1;

    stride.val = (max_key.val - min_key.val) / capacity.val;
  }

  /* Get the offset of target table. */
  inline static uint64_t get_start_offset(uint64_t table_per_mn, uint16_t tab_num, uint64_t base_size) {
    uint64_t quotient = base_size / table_per_mn;
    uint64_t remainder = quotient % 8;

    if (remainder == 0) {
      return tab_num * quotient;
    } else {
      return tab_num * (quotient + 8 - remainder);
    }
  }

  /* Get the offset of target table index. */
  inline static uint64_t get_tab_offset(uint64_t idx) {
    return sizeof(TableHeader) + idx*sizeof(TableEntry);
  }

  /* Return the representative node version of the header. */
  inline uint64_t get_version(void) {
    return tab_id.n_ver;
  }

  inline uint64_t get_tab_id(void) {
    return tab_id.val;
  }

  inline uint64_t get_min_key(void) {
    return min_key.val;
  }

  inline void set_min_key(uint64_t _min_key) {
    min_key.val =_min_key;
  }

  inline uint64_t get_max_key(void) {
    return max_key.val;
  }

  inline void set_max_key(uint64_t _max_key) {
    max_key.val =_max_key;
  }

  inline uint64_t get_stride(void) {
    return stride.val;
  }

  inline void set_stride(uint64_t _stride) {
    stride.val =_stride;
  }

  inline uint64_t get_capacity(void) {
    return capacity.val;
  }

  inline void double_capacity(void) {
    capacity.val *= 2;
  }

  inline uint64_t get_max_search_len(void) {
    return max_search_len.val;
  }

  inline void set_max_search_len(uint64_t _max_search_len) {
    max_search_len.val =_max_search_len;
  }

  inline void set_revamp_flag(void) {
    ++tab_id.n_ver;
  }

  inline uint64_t get_tab_size(void) {
    return capacity.val*(sizeof(TableEntry));
  }

  inline bool is_consistent(void) {
    uint64_t version = get_version();

    if (tab_id.n_ver != version ||
        min_key.n_ver != version ||
        max_key.n_ver != version ||
        stride.n_ver != version ||
        capacity.n_ver != version ||
        max_search_len.n_ver != version) {
      return false; 
    }

    return true;    
  }
  
  inline void set_consistent(uint64_t node_version) {
    tab_id.n_ver = node_version;
    min_key.n_ver = node_version;
    max_key.n_ver = node_version;
    stride.n_ver = node_version;
    capacity.n_ver = node_version;
    max_search_len.n_ver = node_version;
  }

  inline uint64_t get_target_block(Key_t key) {
    if (key < min_key.val) {
      return 0;
    } 
    
    uint64_t tab_idx = (key - min_key.val) / stride.val;
    if (tab_idx >= capacity.val) {
      return capacity.val - 1;
    } else {
      return tab_idx;
    }
  }
};

#endif // _TABLE_H