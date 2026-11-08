#ifndef _BUCKET_H
#define _BUCKET_H

#include "bolt.h"

struct Pair {
  union {
    struct {
      uint64_t kn_ver : 4;
      uint64_t ke_ver : 4;
      Key_t key : 56;
    };
  };

  union {
    struct {
      uint64_t vn_ver : 4;
      uint64_t ve_ver : 4;
      Value_t value : 56; 
    };
  };

  Pair(void)
  : kn_ver(0), ke_ver(0), key(INVALID_KEY), vn_ver(0), ve_ver(0), value(INVALID_VAL) { }

  inline bool is_consistent(void) {
    if (kn_ver != vn_ver ||
        ke_ver != ve_ver) {
      return false; 
    }
    
    return true;
  }

  inline bool is_consistent(uint64_t node_version) {
    if (kn_ver != node_version ||
        vn_ver != node_version ||
        ke_ver != ve_ver) {
      return false; 
    }
    
    return true;
  }

  inline void set_node_version_consistent(uint64_t node_version) {
    kn_ver = node_version;
    vn_ver = node_version;
  }

  inline void set_entry_version_consistent(void) {
    ke_ver++;
    ve_ver = ke_ver;
  }
} __attribute__((packed));

struct BucketHeader {
  two_ver_u64 lowest;
  two_ver_u64 highest;

  BucketHeader(uint64_t _lowest, uint64_t _highest) {
    lowest.val = _lowest;
    highest.val = _highest;
  }

  /* Return the representative node version of the header. */
  inline uint64_t get_node_version(void) {
    return lowest.n_ver;
  }

  /* Increment and return the representative version of the header. */
  inline uint64_t increase_node_version(void) {
    return ++lowest.n_ver;
  }
  
  inline bool is_consistent(uint64_t node_version) {
    if (lowest.n_ver != node_version ||
        highest.n_ver != node_version) {
      return false; 
    }
    
    return true;
  }

  inline void set_consistent(uint64_t node_version) {
    lowest.n_ver = node_version;
    highest.n_ver = node_version;
  }
};

struct Bucket {
  BucketHeader hdr;
  Pair slot[BUCKET_CAPACITY];

  Bucket(uint64_t _lowest, uint64_t _highest) 
  : hdr(_lowest, _highest) {  }

  /* Get the offset of target bucket number. */
  inline static uint64_t get_bucket_offset(uint64_t num) {
    return sizeof(uint64_t) + num*sizeof(Bucket);
  }
  
  /* Get the offset of target pair index. */
  inline static uint64_t get_pair_offset(uint64_t idx) {
    return sizeof(BucketHeader) + idx*sizeof(Pair);
  }

  inline uint64_t get_lowest(void) {
    return hdr.lowest.val;
  }

  inline uint64_t get_highest(void) {
    return hdr.highest.val;
  }

  inline bool is_consistent(void) {
    uint64_t node_version = hdr.get_node_version();

    if (!hdr.is_consistent(node_version)) {
      return false;
    }

    for (int i = 0; i < BUCKET_CAPACITY; ++i) {
      if (!slot[i].is_consistent(node_version)) {
        return false;
      }
    }

    return true;
  }

  inline void set_consistent(void) {
    uint64_t node_version = hdr.increase_node_version();

    hdr.set_consistent(node_version);
    for (int i = 0; i < BUCKET_CAPACITY; ++i) {
      slot[i].set_node_version_consistent(node_version);
    }
  }

  Key_t min_key(void) {
    Key_t min_key = slot[0].key;
    for (int i = 1; i < BUCKET_CAPACITY; ++i) {
      if (slot[i].key != INVALID_KEY && slot[i].key < min_key) {
        min_key = slot[i].key;
      }
    }

    assert(min_key != INVALID_KEY);
    return min_key;
  }

  Key_t max_key(void) {
    Key_t max_key = slot[0].key;
    for (int i = 1; i < BUCKET_CAPACITY; ++i) {
      if (slot[i].key != INVALID_KEY && slot[i].key > max_key) {
        max_key = slot[i].key;
      }
    }

    assert(max_key != INVALID_KEY);
    return max_key;
  }

  bool insert(const Key_t& key, const Value_t& value, int& loc) {
    // Initialize location
    loc = -1; 

    // Start searching position
    for (int i = 0; i < BUCKET_CAPACITY; ++i) {
      if (slot[i].key != INVALID_KEY) {
        if (slot[i].key == key) { // Update
          loc = i;
          break;
        }
      } else if (loc == -1) {
        loc = i;
      }
    }

    if (loc != -1) {
      slot[loc].key = key;
      slot[loc].value = value;
      slot[loc].set_entry_version_consistent();

      return true;
    }
    
    return false;
  }

  Bucket* split_and_insert(void* buffer, const Key_t& key, const Value_t& value) {
    std::sort(slot, slot+BUCKET_CAPACITY, [](const Pair &a, const Pair &b) { return a.key < b.key; });

    Bucket* new_bucket = new (buffer) Bucket(slot[BUCKET_CAPACITY/2].key, hdr.highest.val);
    std::copy(slot+BUCKET_CAPACITY/2, slot+BUCKET_CAPACITY, new_bucket->slot);

    hdr.highest.val = new_bucket->get_lowest() - 1;
    for (int i = BUCKET_CAPACITY/2; i < BUCKET_CAPACITY; ++i) {
      slot[i].key = INVALID_KEY;
    }

    int loc;
    if (key >= new_bucket->get_lowest()) {
      new_bucket->insert(key, value, loc);
    } else {
      insert(key, value, loc);
    }

    set_consistent();
    new_bucket->set_consistent();

    return new_bucket;
  }

  Value_t search(const Key_t& key, int& loc) {
    for (int i = 0; i < BUCKET_CAPACITY; ++i) {
      if (slot[i].key != INVALID_KEY && slot[i].key == key) {
        loc = i;
        return slot[i].value;
      }
    }

    return INVALID_VAL;
  }

  void range_query(const Key_t& from, const Key_t& to, uint64_t& found, Value_t* res) {
    for (int i = 0; i < BUCKET_CAPACITY; ++i) {
      if (slot[i].key != INVALID_KEY && slot[i].key >= from && slot[i].key <= to) {
        res[found++] = slot[i].value;
      }
    }
  }
};

#endif // _BUCKET_H