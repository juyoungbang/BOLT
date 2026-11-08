/* This code is derived from CHIME. */

#ifndef _HASH_H
#define _HASH_H

#include "bolt.h"
#include "address.h"
#include "city.h"

inline uint64_t get_hashed_local_lock_index(const Key_t& k) {
  return CityHash64((char *)&k, sizeof(k)) % LOCAL_LOCK_NUM;
  /*uint64_t res = 0, cnt = 0;
  for (auto a : k) if (cnt ++ < 8) res = (res << 8) + a;
  return res % define::kLocalLockNum;*/
}

inline uint64_t get_hashed_local_lock_index(const Address& addr) {
  return CityHash64((char *)&addr, sizeof(addr)) % LOCAL_LOCK_NUM;
  //return addr.to_uint64() % define::kLocalLockNum;
}

/*inline uint64_t get_hashed_leaf_entry_index(const Key_t& k) {
  return CityHash64((char *)&k, sizeof(k)) % define::leafSpanSize;
}*/

#define HASH_TABLE_SIZE 1000000
#define HASH_BUCKET_SIZE 16

inline uint64_t get_hashed_cache_table_index(const Address& bucket_addr, int kv_idx) {
  // auto info = std::make_pair(leaf_addr, kv_idx);
  // return CityHash64((char *)&info, sizeof(info)) % HASH_TABLE_SIZE;
  return (bucket_addr.to_uint64() + kv_idx) % HASH_TABLE_SIZE;
}

#endif // _HASH_H