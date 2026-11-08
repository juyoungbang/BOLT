/* This code is derived from Sherman. */

#ifndef _WRLOCK_H
#define _WRLOCK_H

#include <atomic>
#include <immintrin.h>

class alignas(64) WRLock {
private:
  std::atomic<uint16_t> l;
  const static uint16_t UNLOCKED = 0;
  const static uint16_t LOCKED = 1;

public:
  WRLock() { init(); }

  bool is_unlock() {
    return l.load(std::memory_order_relaxed) == UNLOCKED;
  }

  void init() {
    l.store(UNLOCKED, std::memory_order_relaxed);
  }

  void wLock() {
    while (true) {
      while (l.load(std::memory_order_relaxed) != UNLOCKED) {
        _mm_pause();
      }

      uint16_t f = UNLOCKED;
      if (l.compare_exchange_weak(f, LOCKED,
                                  std::memory_order_acquire,
                                  std::memory_order_relaxed)) {
        break;
      }
    }
  }

  bool try_wLock() {
    if (l.load(std::memory_order_relaxed) != UNLOCKED)
      return false;

    uint16_t f = UNLOCKED;
    return l.compare_exchange_strong(f, LOCKED,
                                     std::memory_order_acquire,
                                     std::memory_order_relaxed);
  }

  void rLock() {
    while (true) {
      uint16_t v;
      while ((v = l.load(std::memory_order_relaxed)) == LOCKED) {
        _mm_pause();
      }

      uint16_t b = v + 2;

      if (l.compare_exchange_weak(v, b,
                                  std::memory_order_acquire,
                                  std::memory_order_relaxed)) {
        break;
      }
    }
  }

  bool try_rLock() {
    uint16_t v = l.load(std::memory_order_relaxed);

    while (v != LOCKED) {
      uint16_t b = v + 2;
      if (l.compare_exchange_weak(v, b,
                                  std::memory_order_acquire,
                                  std::memory_order_relaxed)) {
        return true;
      }
    }

    return false;
  }

  void rUnlock() {
    l.fetch_sub(2, std::memory_order_release);
  }

  void wUnlock() {
    l.store(UNLOCKED, std::memory_order_release);
  }
};

#endif // _WRLOCK_H