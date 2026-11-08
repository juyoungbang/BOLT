#ifndef _BUFFER_H
#define _BUFFER_H

class Buffer {
private:
  uint64_t size;
  uint64_t begin_addr;
  uint64_t end_addr;
  uint64_t next_alloc_addr;

public:
  void init_buffer(uint64_t _size, uint64_t _begin_addr) {
    size = _size;
    begin_addr = _begin_addr;
    end_addr = begin_addr + size;
    next_alloc_addr = begin_addr;
  }

  inline uint64_t get_cur_addr(void) {
    return next_alloc_addr;
  }

  inline void clear(void) {
    next_alloc_addr = begin_addr;
  }

  inline void revert_to(uint64_t addr) {
    next_alloc_addr = addr;
  }
  
  inline void* alloc(uint64_t size) {
    uint64_t alloc_addr = next_alloc_addr;
    next_alloc_addr += size;
    assert(next_alloc_addr < end_addr);
    return (void*)alloc_addr;
  }

  inline void free(uint64_t size) {
    next_alloc_addr -= size;
  }
};

#endif // _BUFFER_H