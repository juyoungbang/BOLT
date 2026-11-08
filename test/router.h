#ifndef _ROUTER_H
#define _ROUTER_H

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <thread>
#include <chrono>
#include <array>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

class RequestQueue : public Queue {
public:
  RequestQueue() { reset(); }

  uint64_t get_deq_num() {
    return deq_num;
  }

  void push(Request req) {
    while (true) {
      uint64_t t = tail.load(std::memory_order_relaxed);
      uint64_t h = head.load(std::memory_order_acquire);

      if (t - h < CAPACITY) {
        q[t & MASK] = std::move(req);
        tail.store(t+1, std::memory_order_release);
        return;
      }
    }
  }

  std::optional<Request> pop_blocking() {
    while (true) {
      uint64_t h = head.load(std::memory_order_relaxed);
      uint64_t t = tail.load(std::memory_order_acquire);

      if (h != t) {
        Request req = std::move(q[h & MASK]);
        head.store(h+1, std::memory_order_release);

        if (req.op == OpType::READ_MODIFY_WRITE) {
          deq_num += 2;
        } else {
          deq_num++;
        }

        return req;
      }

      if (closed.load(std::memory_order_acquire)) {
        if (h != tail.load(std::memory_order_acquire)) {
          continue;
        }

        return std::nullopt;
      }
    }
  }

  void reset() {
    closed.store(false, std::memory_order_release);
    head.store(0, std::memory_order_release);
    tail.store(0, std::memory_order_release);
    deq_num = 0;
  }

  void close() {
    closed.store(true, std::memory_order_release);
  }

  bool is_closed() {
    return closed.load(std::memory_order_acquire);
  }

private:
  static constexpr uint64_t CAPACITY = (1UL << 25);
  static constexpr uint64_t MASK = CAPACITY-1;
  static_assert((CAPACITY & (CAPACITY-1)) == 0);

  alignas(64) std::atomic<bool> closed;
  alignas(64) std::atomic<uint64_t> head;
  alignas(64) std::atomic<uint64_t> tail;

  alignas(64) std::array<Request, CAPACITY> q;
  alignas(64) uint64_t deq_num;
};

class RequsetRouterBench : public RequstGen {
public:
  RequsetRouterBench(Queue* gq) : gq(gq) { }

  std::optional<Request> next() override {
    return gq->pop_blocking();
  }

  bool is_finished() {
    return gq->is_closed();
  }

private:
  Queue* gq;
};

static RequstGen* coro_router_func(Queue* gq) {
  return new RequsetRouterBench(gq);
}

class RateLimiter {
public:
  explicit RateLimiter(double qps) : qps(qps), init(false) { }

  void throttle(void) {
    if (qps <= 0.0) {
      return;
    }

    auto now = std::chrono::steady_clock::now();
    if (!init) {
      init = true;
      next = now;
    }

    const auto interval = std::chrono::duration<double>(1.0 / qps);
    if (now < next) {
      std::this_thread::sleep_until(next);
      next += std::chrono::duration_cast<std::chrono::steady_clock::duration>(interval);
    } else {
      next = now + std::chrono::duration_cast<std::chrono::steady_clock::duration>(interval);
    }
  }

private:
  double qps;
  bool init;
  std::chrono::steady_clock::time_point next;
};

class RouterPolicy {
public:
  virtual ~RouterPolicy() = default;
  virtual uint16_t pick_server(const Request& req, uint64_t server_num) = 0;
};

class RoundRobinPolicy : public RouterPolicy {
public:
  uint16_t pick_server(const Request& req, uint64_t server_num) {
    uint16_t target = cur;
    cur = (cur + 1) % server_num;
    return target;
  }

private:
  uint16_t cur{0};
};

class LogicalPartitionPolicy : public RouterPolicy {
public:
  uint16_t pick_server(const Request& req, uint64_t server_num) {
    for (int i = 0; i < sm.size()-1; ++i) {
      if (sm[i].first <= req.key && sm[i+1].first > req.key) {
        return sm[i].second;
      }
    }

    fprintf(stdout, "[ERROR] Key %lu not associated to any partition\n", req.key);
    exit(1);
  }

  void set_server_mapping(std::vector<std::pair<uint64_t, uint16_t>> server_mapping) {
    sm = server_mapping;
  }

private:
  std::vector<std::pair<uint64_t, uint16_t>> sm;
};

void run_local_router(const std::vector<std::string>& ips, std::vector<RequestQueue*>& gqs, uint16_t cur_cid,
                      std::vector<Request>& requests, RouterPolicy& policy,
                      double qps, bool end_flag, std::atomic<bool>& execute) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(LOC_ROUTER_CORE, &cpuset);

  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    fprintf(stdout, "[ERROR] Failed binding core (error: %s)\n", strerror(errno));
  }

  if (ips.empty()) {
    fprintf(stdout, "[ERROR] No server IPs provided\n");
    exit(1);
  }

  RateLimiter limiter(qps);

  int cur_tid = 0;
  uint64_t num_threads = gqs.size();

  while (!execute.load()) {
    continue;
  }

  fprintf(stdout, "[NOTICE] Launching request router at core %d\n", LOC_ROUTER_CORE);

  for (uint64_t i = 0; i < requests.size(); ++i) {
    limiter.throttle();

    uint16_t cid = policy.pick_server(requests[i], ips.size());

    if (cid != cur_cid) continue;
    gqs[cur_tid]->push(requests[i]);
    cur_tid = (cur_tid + 1) % num_threads;
  }

  for (int i = 0; i < num_threads; ++i) {
    gqs[i]->close();
  }
}

#endif // _ROUTER_H