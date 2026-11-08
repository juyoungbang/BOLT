/* This code is derived from Sherman. */

#ifndef _LOCAL_ALLOCATOR_H
#define _LOCAL_ALLOCATOR_H

#include "bolt.h"
#include "address.h"

#include <vector>
#include <list>

constexpr uint64_t MEMORY_CHUNK_BUCKET_NUM = MEMORY_CHUNK_SIZE/sizeof(Bucket);
constexpr uint64_t MEMORY_CHUNK_KV_DATA_NUM = MEMORY_CHUNK_SIZE/KV_DATA_SIZE;

class LocalAllocator {
public:
  LocalAllocator(void) {
    for (int i = 0; i < 2; ++i) {
      head[i] = Address::null();
      cur[i] = Address::null();
      lock[i] = 0;
    }
  }

  using FreeList = std::list<std::pair<Address, bool>>;
  
  bool lock_global_alloc(bool is_bucket = true) {
    int idx = get_idx(is_bucket);

    if (__sync_bool_compare_and_swap(&lock[idx], 0, 1)) {
      return true;
    } else {
      return false;
    }
  }

  void unlock_global_alloc(bool is_bucket = true) {
    int idx = get_idx(is_bucket);
    assert(__sync_bool_compare_and_swap(&lock[idx], 1, 0));
  }
  
  Address malloc(bool& need_chunk, bool is_bucket = true) {
    int idx = get_idx(is_bucket);
    int chunk_num = is_bucket ? MEMORY_CHUNK_BUCKET_NUM : MEMORY_CHUNK_KV_DATA_NUM;

    Address res = cur[idx];
    if (head[idx] == Address::null() || (cur[idx].num + 1 > head[idx].num + chunk_num)) {
      need_chunk = true;
    } else {
      need_chunk = false;
      cur[idx].num += 1;
    }

    if (need_chunk) {
      for (auto iter = free_list.begin(); iter != free_list.end(); ++iter) {
        if (iter->second == is_bucket) {
          res = iter->first;
          free_list.erase(iter);
          need_chunk = false;
          break;
        }
      }
    }

    return res;
  }

  void set_chunk(Address& addr, bool is_bucket = true) {
    int idx = get_idx(is_bucket);
    head[idx] = addr;
    cur[idx] = addr;
  }

  void free(const Address& addr, bool is_bucket = true) {
    free_list.push_back(std::make_pair(addr, is_bucket));
  }

private:
  Address head[2];
  Address cur[2];
  uint64_t lock[2];
  FreeList free_list;

  int get_idx(bool is_bucket) {
    return is_bucket ? 0 : 1;
  }
};

#endif // _LOCAL_ALLOCATOR_H