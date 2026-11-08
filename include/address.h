#ifndef _ADDRESS_H
#define _ADDRESS_H

class Address {
public:
  union {
    uint8_t ver; // Version
    struct {
      uint64_t n_ver : 4; // Node version
      uint64_t e_ver : 4; // Entry version
      uint64_t sid : 24; // Target server ID
      uint64_t num : 32; // Number of the component pointing to
    };
    uint64_t val;
  };  

  Address(void)
  : val(0) { }

  Address(uint64_t _ver, uint64_t _sid, uint64_t _num) {
    ver = _ver;
    sid = _sid;
    num = _num;
  }

  Address(uint64_t _n_ver, uint64_t _e_ver, uint64_t _sid, uint64_t _num) {
    n_ver = _n_ver;
    e_ver = _e_ver;
    sid = _sid;
    num = _num;
  }

  uint64_t to_uint64(void) const { 
    return val; 
  }

  static Address null(void) {
    static Address zero;
    zero.val = -1;

    return zero;
  }

  inline void operator=(const Address &addr) {
    sid = addr.sid;
    num = addr.num;
  }
} __attribute__((packed));

inline bool operator==(const Address &lhs, const Address &rhs) {
  return (lhs.sid == rhs.sid) && (lhs.num == rhs.num);
}

inline bool operator!=(const Address &lhs, const Address &rhs) {
  return !(lhs == rhs);
}

inline bool operator<(const Address &lhs, const Address &rhs) {
  return lhs.num < rhs.num; 
}

#ifdef VAR_VALUE_SIZE
class DataPtr {
public:
  union {
    struct {
      uint64_t sid : 24; // Target server ID
      uint64_t num : 32; // Number of the data pointing to
      uint64_t dummy : 8; // Dummy bit for version
    };
    uint64_t val;
  };

  DataPtr(uint64_t _sid, uint64_t _num) : sid(_sid), num(_num) { }

  operator uint64_t() const { return val; }
} __attribute__((packed));

static_assert(sizeof(DataPtr) == 8);

class KVData {
public:
  uint64_t rest_of_key_size;
  uint64_t value_size;
  union {
    Value_t value;
    uint8_t padding[SIMULATED_VALUE_SIZE];
  };

  /* Get the offset of target KV data number. */
  inline static uint64_t get_kv_data_offset(uint64_t num) {
    return sizeof(uint64_t) + num*KV_DATA_SIZE;
  }

  KVData(const Value_t value) : rest_of_key_size(0), value_size(SIMULATED_VALUE_SIZE), value(value) { }
} __attribute__((packed));

static_assert(sizeof(KVData) == KV_DATA_SIZE);
#endif

#endif // _ADDRESS_H