#ifndef _BATCH_H
#define _BATCH_H

#include "rdma.h"
#include <vector>

class Batch {  
private:
  uint16_t sid;
  int batch_num;
  std::vector<RDMAInfo*> batch_list;

public:
  Batch(uint16_t _sid, int _batch_num)
  : sid(_sid), batch_num(_batch_num) {
    batch_list.reserve(batch_num);
  }

  ~Batch(void) {
    for (int i = 0; i < batch_list.size(); ++i) {
      delete batch_list[i];
    }

    batch_list.clear();
  }

  inline uint16_t get_sid(void) {
    return sid;
  }

  inline int get_batch_num(void) {
    return batch_num;
  }

  inline RDMAInfo** get_batch_list(void) {
    return &batch_list[0];
  }

  inline uint64_t get_src(int idx) {
    return batch_list[idx]->src;
  }

  inline uint64_t get_dest(int idx) {
    return batch_list[idx]->dest;
  }

  inline std::vector<RDMAInfo*>::const_iterator get_it(int idx) {
    return batch_list.begin() + idx;
  }

  inline uint64_t get_size(int idx) {
    return batch_list[idx]->size;
  }

  void add(RDMAInfo* batch) {
    batch_list.push_back(batch);
  }
};

#endif // _BATCH_H