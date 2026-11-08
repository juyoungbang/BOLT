#include <vector>
#include <string>
#include <iostream>
#include <algorithm>
#include <numa.h>
#include <pthread.h>

#include "config.h"
#include "cache.h"
#include "client.h"
#include "router.h"
#include "log_writer.h"
#include "key_generator.h"

//////////////////// workload parameters /////////////////////
#define ROUTER
const double qps = 0.0;

#define USE_CORO
const uint16_t num_coroutines = 2;
//////////////////// workload parameters /////////////////////

extern uint64_t successed_insert_cnt[3][MAX_THREAD_NUM][8];
extern uint64_t successed_search_cnt[3][MAX_THREAD_NUM][8];
extern uint64_t successed_others_cnt[3][MAX_THREAD_NUM][8];

extern uint64_t rdma_write_num[MAX_THREAD_NUM][8];
extern uint64_t rdma_write_size[MAX_THREAD_NUM][8];
extern uint64_t rdma_read_num[MAX_THREAD_NUM][8];
extern uint64_t rdma_read_size[MAX_THREAD_NUM][8];
extern uint64_t rdma_atomic_num[MAX_THREAD_NUM][8];
extern uint64_t rdma_atomic_size[MAX_THREAD_NUM][8];

extern uint64_t cache_hit[MAX_THREAD_NUM][8];
extern uint64_t cache_miss[MAX_THREAD_NUM][8];

extern uint64_t stash_stall_num[MAX_THREAD_NUM][8];

struct ClientConfig conf;
Client* client;

uint64_t num_threads;
std::string workload_path;
std::string load_workload_path;
std::string txn_workload_path;
std::vector<Request> bulk_requests;
std::vector<Request> load_requests;
std::vector<Request> txn_requests;
std::vector<Request>* rr_load_requests;
std::vector<Request>* rr_txn_requests;

pthread_t* tid_list;

pthread_barrier_t load_ready_barrier;
pthread_barrier_t load_done_barrier;
pthread_barrier_t txn_ready_barrier;
pthread_barrier_t txn_done_barrier;

struct timespec load_start, load_end;
struct timespec txn_start, txn_end;

double load_cache_hit_ratio;
double txn_cache_hit_ratio;

#ifdef ROUTER
std::vector<RequestQueue*> gqs;
uint64_t gq_load_deq;
uint64_t gq_txn_deq;
#endif

BenchType benchmark_type(std::string name) {
  if (name == "real") {
    return BenchType::REAL;
  } else if (name == "ycsb") {
    return BenchType::YCSB;
  } else if (name == "twitter") {
    return BenchType::TWITTER;
  } else {
    assert(false);
  }
}

void read_real_workload(uint64_t num_ops, uint64_t num_bulk_ops, std::string query_dist, double update_ratio) {
  bulk_requests.reserve(num_bulk_ops);
  load_requests.reserve(num_ops - num_bulk_ops); 

  std::ifstream ifs;
  ifs.open(workload_path);

  if (!ifs.is_open()) {
    fprintf(stdout, "[ERROR] Failed opening file %s (error: %s)\n", workload_path.c_str(), strerror(errno));
    exit(1);
  }

  if (skip_BOM(ifs)) {
    fprintf(stdout, "[NOTICE] Removed BOM in target workload\n");
  }

  fprintf(stdout, "[NOTICE] Start reading %lu operations\n", num_ops);

  std::vector<Request> requests(num_ops);
  for (uint64_t i = 0; i < requests.size(); ++i) {
    ifs >> requests[i].key;
    requests[i].op = OpType::INSERT;

    if (requests[i].key > MAX) {
      fprintf(stdout, "[ERROR] Out of range key (%lu) detected (max key: %lu)\n", requests[i].key, MAX);
    }
  }

  std::copy(requests.begin(), requests.begin() + num_bulk_ops, std::back_inserter(bulk_requests));
  std::copy(requests.begin() + num_bulk_ops, requests.end(), std::back_inserter(load_requests));

  if (query_dist == "uniform") {
    txn_requests = get_search_keys(0, requests, requests.size(), requests.size());
  } else if (query_dist == "zipfian") {
    txn_requests = get_search_keys_zipf(0, requests, requests.size(), requests.size());
  } else {
    assert(false);
  }

  requests.clear();
  requests.shrink_to_fit();

  uint64_t update_op_num = txn_requests.size()*update_ratio;
  uint64_t search_op_num = txn_requests.size() - update_op_num; 
  for (uint64_t i = 0; i < txn_requests.size(); ++i) {
    if (i < update_op_num) {
      txn_requests[i].op = OpType::UPDATE;
    } else {
      txn_requests[i].op = OpType::SEARCH;
    }
  }

  std::mt19937_64 gen(0);
  std::shuffle(txn_requests.begin(), txn_requests.end(), gen);

  ifs.close();
}

void read_ycsb_workload(std::string workload_name, uint64_t num_bulk_ops, uint64_t num_load_ops, uint64_t num_txn_ops) {
  bulk_requests.resize(num_bulk_ops);
  load_requests.resize(num_load_ops - num_bulk_ops);
  txn_requests.resize(num_txn_ops);

  std::ifstream ifs;
  ifs.open(load_workload_path);

  if (!ifs.is_open()) {
    fprintf(stdout, "[ERROR] Failed opening file %s (error: %s)\n", load_workload_path.c_str(), strerror(errno));
    exit(1);
  }

  if (skip_BOM(ifs)) {
    fprintf(stdout, "[NOTICE] Removed BOM in target load workload\n");
  }

  fprintf(stdout, "[NOTICE] Start reading %lu load operations\n", num_load_ops);

  uint64_t key;
  std::string str_op;
  for (uint64_t i = 0; i < num_load_ops; ++i) {
    ifs >> str_op >> key;
    assert(str_op == "INSERT");

    if (i < num_bulk_ops) {
      bulk_requests[i].key = key;
      bulk_requests[i].op = OpType::INSERT;
    } else {
      load_requests[i-num_bulk_ops].key = key;
      load_requests[i-num_bulk_ops].op = OpType::INSERT;
    }

    if (key > MAX) {
      fprintf(stdout, "[ERROR] Out of range key (%lu) detected (max key: %lu)\n", key, MAX);
    }
  }

  ifs.close();

  ifs.open(txn_workload_path);

  if (!ifs.is_open()) {
    fprintf(stdout, "[ERROR] Failed opening file %s (error: %s)\n", txn_workload_path.c_str(), strerror(errno));
    exit(1);
  }

  if (skip_BOM(ifs)) {
    fprintf(stdout, "[NOTICE] Removed BOM in target txn workload\n");
  }

  fprintf(stdout, "[NOTICE] Start reading %lu txn operations\n", num_txn_ops);

  for (uint64_t i = 0; i < num_txn_ops; ++i) {
    ifs >> str_op >> txn_requests[i].key;

    if (str_op == "INSERT") {
      txn_requests[i].op = OpType::INSERT;
    } else if (str_op == "READ") {
      txn_requests[i].op = OpType::SEARCH;
    } else if (str_op == "UPDATE") {
      txn_requests[i].op = OpType::UPDATE;
    } else if (str_op == "SCAN") {
      txn_requests[i].op = OpType::RANGE_QUERY;
      ifs >> txn_requests[i].range_size;
    } else {
      fprintf(stdout, "[ERROR] Unknown operation type '%s'\n", str_op.c_str());
      exit(1);
    }

    if (txn_requests[i].key > MAX) {
      fprintf(stdout, "[ERROR] Out of range key (%lu) detected (max key: %lu)\n", txn_requests[i].key, MAX);
    }
  }

  ifs.close();
}

void read_twitter_workload(uint64_t num_ops, uint64_t num_bulk_ops, uint64_t num_load_ops) {
  bulk_requests.resize(num_bulk_ops);
  load_requests.resize(num_load_ops);
  txn_requests.resize(num_ops-num_bulk_ops-num_load_ops);

  std::ifstream ifs;
  ifs.open(workload_path);

  if (!ifs.is_open()) {
    fprintf(stdout, "[ERROR] Failed opening file %s (error: %s)\n", workload_path.c_str(), strerror(errno));
    exit(1);
  }

  if (skip_BOM(ifs)) {
    fprintf(stdout, "[NOTICE] Removed BOM in target workload\n");
  }

  fprintf(stdout, "[NOTICE] Start reading %lu operations\n", num_ops);

  uint64_t key;
  std::string str_op;
  for (uint64_t i = 0; i < num_ops; ++i) {
    ifs >> str_op >> key;

    OpType op;
    if (str_op == "insert") {
      op = OpType::INSERT;
    } else if (str_op == "get" || str_op == "gets") {
      op = OpType::SEARCH;
    } else {
      op = OpType::UPDATE;
    }

    if (i < num_bulk_ops) {
      bulk_requests[i].key = key;
      bulk_requests[i].op = op;
    } else if (i < num_bulk_ops + num_load_ops) {
      load_requests[i-num_bulk_ops].key = key;
      load_requests[i-num_bulk_ops].op = op;
    } else {
      txn_requests[i-num_bulk_ops-num_load_ops].key = key;
      txn_requests[i-num_bulk_ops-num_load_ops].op = op;
    }

    if (key > MAX) {
      fprintf(stdout, "[ERROR] Out of range key (%lu) detected (max key: %lu)\n", key, MAX);
    }
  }

  ifs.close();
}

struct ThreadArgs {
  uint16_t cid;
  int tid;
};

class RequsetGenBench : public RequstGen {
public:
  RequsetGenBench(uint16_t coro_id, uint16_t coro_cnt, const std::vector<Request>& requests)
  : coro_id(coro_id), requests(requests) {
    start = requests.begin();
    uint64_t requests_per_coroutine = requests.size()/coro_cnt;

    for (int i = 0; i < coro_id; ++i) {
      start += (i < requests.size()%coro_cnt) ? requests_per_coroutine+1 : requests_per_coroutine;
    }

    end = (coro_id < requests.size()%coro_cnt) ? start+requests_per_coroutine+1 : start+requests_per_coroutine;
  }

  std::optional<Request> next() override {
    Request r = *start;
    start++;

    return r;
  }

  bool is_finished() {
    return start == end;
  }

private:
  uint16_t coro_id;

  std::vector<Request> requests;
  std::vector<Request>::const_iterator start;
  std::vector<Request>::const_iterator end;
};

RequstGen* coro_func(uint16_t coro_id, uint16_t coro_cnt, const std::vector<Request>& requests) {
  return new RequsetGenBench(coro_id, coro_cnt, requests);
}

/*
phase 0: Bulk loading
phase 1: Load phase
phase 2: Transaction phase
*/
void op_func(Client* client, const Request& r, int phase, uint64_t tid, CoroPull* sink = nullptr, uint16_t coro_id = 0) {
  if (r.op == OpType::INSERT) {
    client->insert(r.key, reinterpret_cast<Value_t>(r.key), sink);
    successed_insert_cnt[phase][tid][0]++;
  } else if (r.op == OpType::SEARCH) {
    auto ret = client->search(r.key, sink);
    if (ret == reinterpret_cast<Value_t>(r.key)) {
      successed_search_cnt[phase][tid][0]++;
    }
  } else if (r.op == OpType::UPDATE) {
    client->insert(r.key, reinterpret_cast<Value_t>(r.key), sink);
    successed_others_cnt[phase][tid][0]++;
  } else if (r.op == OpType::RANGE_QUERY) {
    assert(r.range_size > 0);
    Value_t ret[1000];
    auto res = client->range_query(r.key, r.key + r.range_size, ret, sink);
    successed_others_cnt[phase][tid][0]++;
  } else {
    assert(false);
  }
}

void* run_thread(void* _thread_args) {
  struct ThreadArgs* thread_args = (struct ThreadArgs*)_thread_args;
  uint16_t cid = thread_args->cid;
  int tid = thread_args->tid;

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(map_thread_to_logical_cpu_rr(tid), &cpuset);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    fprintf(stdout, "[ERROR] Failed binding core (error: %s)\n", strerror(errno));
  }

  client->register_thread(tid);

#ifdef ROUTER
  RoundRobinPolicy rr;
  std::atomic<bool> execute = false;
  std::thread req_router;
  std::vector<std::string> ips;
#endif

  if (tid == 0) {
#ifdef ROUTER
    for (int i = 0; i < conf.client_num; i++) {
      ips.emplace_back(conf.client_ips[i]);
    }
    
    req_router = std::thread([&]{ run_local_router(ips, gqs, cid, load_requests, rr, qps, false, execute); });
#endif

    client->send_signal(SignalType::READY);

    // Wait for all clients to be ready
    client->wait_signal(SignalType::START);
  }

  pthread_barrier_wait(&load_ready_barrier);

  if (tid == 0) {
#ifdef ROUTER
    execute = true;
#endif

    fprintf(stdout, "[NOTICE] Start load\n");
    clock_gettime(CLOCK_REALTIME, &load_start);
  }

#ifndef ROUTER
#ifdef USE_CORO
  if (rr_load_requests[tid].size()) {
    client->run_coroutine(coro_func, num_coroutines, op_func, 1, rr_load_requests[tid]); 
  }
#else
  for (uint64_t i = 0; i < rr_load_requests[tid].size(); ++i) {
    op_func(client, rr_load_requests[tid][i], 1, tid);
  }
#endif
#else
#ifdef USE_CORO
  client->run_router_coroutine(coro_router_func, num_coroutines, op_func, 1, gqs[tid]); 
#else
  while (true) {
    auto r = gqs[tid]->pop_blocking();
    if (r.has_value()) {
      op_func(client, r.value(), 1, tid);
    } else if (gqs[tid]->is_closed()) {
      break;
    }
  }
#endif
#endif

  client->release_cache_leader(tid);

  // Wait for all the threads to finish load
  pthread_barrier_wait(&load_done_barrier);

  if (tid == 0) {
    clock_gettime(CLOCK_REALTIME, &load_end);  

#ifdef ROUTER
    req_router.join();
    execute = false;
    req_router = std::thread([&]{ run_local_router(ips, gqs, cid, txn_requests, rr, qps, true, execute); });

    gq_load_deq = 0;
    for (uint64_t i = 0; i < num_threads; ++i) {
      gq_load_deq += gqs[i]->get_deq_num();
      gqs[i]->reset();
    }
#endif

    // Note master server the end of load
    client->broadcast_signal(SignalType::HALT);

    client->wait_signal(SignalType::START);

    uint64_t total_rdma_write_num = 0;
    uint64_t total_rdma_write_size = 0;
    uint64_t total_rdma_read_num = 0;
    uint64_t total_rdma_read_size = 0;
    uint64_t total_rdma_atomic_num = 0;
    uint64_t total_rdma_atomic_size = 0;
    uint64_t total_cache_miss = 0;
    uint64_t total_cache_hit = 0;
    uint64_t total_stash_stall_num = 0;
    for (int i = 0; i < MAX_THREAD_NUM; ++i) {
      total_rdma_write_num += rdma_write_num[i][0];
      total_rdma_write_size += rdma_write_size[i][0];
      total_rdma_read_num += rdma_read_num[i][0];
      total_rdma_read_size += rdma_read_size[i][0];
      total_rdma_atomic_num += rdma_atomic_num[i][0];
      total_rdma_atomic_size += rdma_atomic_size[i][0];
      total_cache_miss += cache_miss[i][0];
      total_cache_hit += cache_hit[i][0];      
      total_stash_stall_num += stash_stall_num[i][0];
    }  

    load_cache_hit_ratio = 100 * (double)total_cache_hit / (double)(total_cache_hit + total_cache_miss);

    fprintf(stdout, "======================== Statistics ========================\n");    
#ifdef STATS 
    fprintf(stdout, "Total RDMA write request number : %lu\n", total_rdma_write_num);
    fprintf(stdout, "Total RDMA write request size(B): %lu\n", total_rdma_write_size);
    fprintf(stdout, "Total RDMA read request number : %lu\n", total_rdma_read_num);
    fprintf(stdout, "Total RDMA read request size(B): %lu\n", total_rdma_read_size);
    fprintf(stdout, "Total RDMA atomic request number : %lu\n", total_rdma_atomic_num);
    fprintf(stdout, "Total RDMA atomic request size(B): %lu\n", total_rdma_atomic_size);
#endif
    fprintf(stdout, "Total cache hit : %lu\n", total_cache_hit);
    fprintf(stdout, "Total cache miss : %lu\n", total_cache_miss);
    fprintf(stdout, "Total stash stall number : %lu\n", total_stash_stall_num);
    fprintf(stdout, "============================================================\n"); 

    client->clear_statistics();
  }

  // Wait for all the threads to be ready for txn
  pthread_barrier_wait(&txn_ready_barrier);

  if (tid == 0) {
#ifdef ROUTER
    execute = true;
#endif

    fprintf(stdout, "[NOTICE] Start txn\n");
    clock_gettime(CLOCK_REALTIME, &txn_start);    
  }

#ifndef ROUTER
#ifdef USE_CORO
  if (rr_txn_requests[tid].size()) {
    client->run_coroutine(coro_func, num_coroutines, op_func, 2, rr_txn_requests[tid]); 
  }
#else
  for (uint64_t i = 0; i < rr_txn_requests[tid].size(); ++i) {
    op_func(client, rr_txn_requests[tid][i], 2, tid);
  }
#endif
#else
#ifdef USE_CORO
  client->run_router_coroutine(coro_router_func, num_coroutines, op_func, 2, gqs[tid]); 
#else
  while (true) {
    auto r = gqs[tid]->pop_blocking();
    if (r.has_value()) {
      op_func(client, r.value(), 2, tid);
    } else if (gqs[tid]->is_closed()) {
      break;
    }
  }
#endif
#endif

  client->release_cache_leader(tid);

  // Wait for all the threads to finish txn
  pthread_barrier_wait(&txn_done_barrier);

  if (tid == 0) {
    clock_gettime(CLOCK_REALTIME, &txn_end);
  
#ifdef ROUTER
    req_router.join();

    gq_txn_deq = 0;
    for (uint64_t i = 0; i < num_threads; ++i) {
      gq_txn_deq += gqs[i]->get_deq_num();
      gqs[i]->reset();
    }
#endif

    client->broadcast_signal(SignalType::END);
    client->wait_signal(SignalType::START);
  }

  return NULL;
}

int main(int argc, char* argv[]) {
  if (argc != 13) {
    fprintf(stderr, "[ERROR] Twelve arguments are required but received %d\n", argc - 1);
    exit(1);
  }

  load_client_config(argv[1], &conf);
  BenchType bench = benchmark_type(argv[2]);
  num_threads = atol(argv[3]);
  std::string workload_dir = argv[4];
  std::string workload_name = argv[5];
  uint64_t num_ops = atol(argv[6]);
  uint64_t cache_size = atol(argv[7]);
  uint64_t value_size = atol(argv[8]);
  std::string query_dist = argv[9]; // For real workload only
  double update_ratio = atof(argv[10]); // For real workload only
  uint64_t num_txn_ops = atol(argv[11]); // For YCSB workload only
  uint64_t load_percent = atol(argv[12]); // For twitter workload only

  if (bench == BenchType::YCSB && workload_name.front() == 'e') {
#ifdef USE_CORO
    assert(false);
#endif
  }

  if (value_size != sizeof(Value_t)) {
#ifndef VAR_VALUE_SIZE
    assert(false);
#else
    assert(value_size == SIMULATED_VALUE_SIZE);
#endif
  }

  // Should not exceed maximum thread number
  assert(num_threads <= MAX_THREAD_NUM);

  // Set cache size
  conf.cache_size = cache_size * MB;

  // Set single-thread bulk loading number
  uint64_t num_bulk_ops = 5000000;

  LogWriter* lw = new LogWriter("CLIENT");

  if (bench == BenchType::REAL) {
    workload_path = workload_dir + "/" + workload_name;
    read_real_workload(num_ops, num_bulk_ops, query_dist, update_ratio);

    lw->print_client_info(num_threads, conf.cache_size, workload_path.c_str(), bulk_requests.size(), load_requests.size(), txn_requests.size(), query_dist.c_str(), update_ratio);
    lw->LOG_client_info(num_threads, conf.cache_size, workload_path.c_str(), bulk_requests.size(), load_requests.size(), txn_requests.size(), query_dist.c_str(), update_ratio);
  } else if (bench == BenchType::YCSB) {
    load_workload_path = workload_dir + "/load_randint_workload" + workload_name;
    txn_workload_path = workload_dir + "/txn_randint_workload" + workload_name;
    read_ycsb_workload(workload_name, num_bulk_ops, num_ops, num_txn_ops);

    lw->print_ycsb_client_info(num_threads, conf.cache_size, load_workload_path.c_str(), txn_workload_path.c_str(), bulk_requests.size(), load_requests.size(), num_txn_ops);
    lw->LOG_ycsb_client_info(num_threads, conf.cache_size, load_workload_path.c_str(), txn_workload_path.c_str(), bulk_requests.size(), load_requests.size(), num_txn_ops);
  } else if (bench == BenchType::TWITTER) {
    workload_path = workload_dir + "/" + workload_name;
    read_twitter_workload(num_ops, num_bulk_ops, num_ops*load_percent/100);

    lw->print_twitter_client_info(num_threads, conf.cache_size, value_size, workload_path.c_str(), bulk_requests.size(), load_requests.size(), txn_requests.size());
    lw->LOG_twitter_client_info(num_threads, conf.cache_size, value_size, workload_path.c_str(), bulk_requests.size(), load_requests.size(), txn_requests.size());
  }

#ifndef ROUTER
  KeyGenerator<Request, Value_t> key_gen;
#ifdef USE_CORO
  fprintf(stdout, "[NOTICE] Start dividing keys to %lu threads (%u coroutine/thread)\n", num_threads, num_coroutines);
  rr_load_requests = key_gen.gen_key_multi_client(load_requests, load_requests.size(), conf.client_num, num_threads, num_coroutines, conf.cid);
  rr_txn_requests = key_gen.gen_key_multi_client(txn_requests, txn_requests.size(), conf.client_num, num_threads, num_coroutines, conf.cid);
#else
  fprintf(stdout, "[NOTICE] Start dividing keys to %lu threads (coroutine disabled)\n", num_threads);
  rr_load_requests = key_gen.gen_key_multi_client(load_requests, load_requests.size(), conf.client_num, num_threads, 1, conf.cid);
  rr_txn_requests = key_gen.gen_key_multi_client(txn_requests, txn_requests.size(), conf.client_num, num_threads, 1, conf.cid);
#endif

  load_requests.clear();
  load_requests.shrink_to_fit();

  txn_requests.clear();
  txn_requests.shrink_to_fit();
#else
  gqs.resize(num_threads);
  for (int i = 0; i < num_threads; ++i) {
    int numa_id = numa_node_of_cpu(map_thread_to_logical_cpu_rr(i));
    void* mem = numa_alloc_onnode(sizeof(RequestQueue), numa_id);
    gqs[i] = new (mem) RequestQueue;
  }
#endif

  for (int i = 0; i < 3; ++i) {
    for (int j = 0; j < MAX_THREAD_NUM; ++j) {
      successed_insert_cnt[i][j][0] = 0;
      successed_search_cnt[i][j][0] = 0;
      successed_others_cnt[i][j][0] = 0;
    }
  }

  fprintf(stdout, "[NOTICE] Starting client\n");
#ifdef USE_CORO
  client = new Client(conf, num_threads, num_coroutines);
#else
  client = new Client(conf, num_threads, 1);
#endif

  if (conf.cid == 0) {
    fprintf(stdout, "[NOTICE] Start initializing index structure\n");
    client->init_bolt();
  }

  client->init_cache();  

  if (conf.cid == 0) {
    client->register_thread(0);

    fprintf(stdout, "[NOTICE] Start bulk loading %lu operations\n", num_bulk_ops);

    assert(bulk_requests.size() == num_bulk_ops);
    for (uint64_t i = 0; i < bulk_requests.size(); ++i) {
      op_func(client, bulk_requests[i], 0, 0);
    }

    client->reset_thread();
  }

  client->clear_statistics();

  tid_list = new pthread_t[num_threads];
  struct ThreadArgs* thread_args_list = new ThreadArgs[num_threads];

  pthread_barrier_init(&load_ready_barrier, NULL, num_threads);
  pthread_barrier_init(&load_done_barrier, NULL, num_threads);
  pthread_barrier_init(&txn_ready_barrier, NULL, num_threads);
  pthread_barrier_init(&txn_done_barrier, NULL, num_threads);

  for (int i = 0; i < num_threads; ++i) {
    thread_args_list[i].cid = conf.cid;
    thread_args_list[i].tid = i;

    pthread_t tid;
    pthread_create(&tid, NULL, run_thread, &thread_args_list[i]);

    tid_list[i] = tid;
  }

  uint64_t load_total_requests = 0;
  uint64_t txn_total_requests = 0;
  uint64_t load_successed_insert_cnt = 0;
  uint64_t load_successed_search_cnt = 0;
  uint64_t load_successed_others_cnt = 0;
  uint64_t txn_successed_insert_cnt = 0;
  uint64_t txn_successed_search_cnt = 0;
  uint64_t txn_successed_others_cnt = 0;
  for (int i = 0; i < num_threads; ++i) {
    pthread_join(tid_list[i], NULL);

#ifndef ROUTER
    load_total_requests += rr_load_requests[i].size();
    txn_total_requests += rr_txn_requests[i].size();
#else
    load_total_requests = gq_load_deq;
    txn_total_requests = gq_txn_deq;
#endif
    load_successed_insert_cnt += successed_insert_cnt[1][i][0];
    load_successed_search_cnt += successed_search_cnt[1][i][0];
    load_successed_others_cnt += successed_others_cnt[1][i][0];
    txn_successed_insert_cnt += successed_insert_cnt[2][i][0];
    txn_successed_search_cnt += successed_search_cnt[2][i][0];
    txn_successed_others_cnt += successed_others_cnt[2][i][0];
  }

  uint64_t load_time = (load_end.tv_sec - load_start.tv_sec) * 1000000000 + (load_end.tv_nsec - load_start.tv_nsec); 
  uint64_t txn_time = (txn_end.tv_sec - txn_start.tv_sec) * 1000000000 + (txn_end.tv_nsec - txn_start.tv_nsec);

  lw->LOG("Average load throughput(op/nsec): %.3e", (double)load_total_requests/(double)load_time);
  if (bench == BenchType::TWITTER) {
    double load_insert_ratio = (double)load_successed_insert_cnt / (double)load_total_requests;
    double load_search_ratio = (double)load_successed_search_cnt / (double)load_total_requests;
    double load_other_ratio = (double)load_successed_others_cnt / (double)load_total_requests;
    lw->LOG(">>> %lu insert (%.2f %%), %lu search (%.2f %%), %lu others (%.2f %%)", load_successed_insert_cnt, 100*load_insert_ratio,
                                                                                    load_successed_search_cnt, 100*load_search_ratio,
                                                                                    load_successed_others_cnt, 100*load_other_ratio);
  } else {
    uint64_t load_successed_requests = load_successed_insert_cnt + load_successed_search_cnt + load_successed_others_cnt;
    lw->LOG(">>> Total %lu operation: %lu successed / %lu failed", load_total_requests, load_successed_requests, load_total_requests-load_successed_requests);
  }

  lw->LOG("Average txn throughput(op/nsec): %.3e", (double)txn_total_requests/(double)txn_time);
  if (bench == BenchType::TWITTER) {
    double txn_insert_ratio = (double)txn_successed_insert_cnt / (double)txn_total_requests;
    double txn_search_ratio = (double)txn_successed_search_cnt / (double)txn_total_requests;
    double txn_other_ratio = (double)txn_successed_others_cnt / (double)txn_total_requests;
    lw->LOG(">>> %lu insert (%.2f %%), %lu search (%.2f %%), %lu others (%.2f %%)", txn_successed_insert_cnt, 100*txn_insert_ratio,
                                                                                    txn_successed_search_cnt, 100*txn_search_ratio,
                                                                                    txn_successed_others_cnt, 100*txn_other_ratio);
  } else {
    uint64_t txn_successed_requests = txn_successed_insert_cnt + txn_successed_search_cnt + txn_successed_others_cnt;
    lw->LOG(">>> Total %lu operation: %lu successed / %lu failed", txn_total_requests, txn_successed_requests, txn_total_requests-txn_successed_requests);
  }

  lw->LOG_client_cache_info(client->get_cache_statistics());

  uint64_t total_rdma_write_num = 0;
  uint64_t total_rdma_write_size = 0;
  uint64_t total_rdma_read_num = 0;
  uint64_t total_rdma_read_size = 0;
  uint64_t total_rdma_atomic_num = 0;
  uint64_t total_rdma_atomic_size = 0;
  uint64_t total_cache_miss = 0;
  uint64_t total_cache_hit = 0;
  uint64_t total_stash_stall_num = 0;
  for (int i = 0; i < MAX_THREAD_NUM; ++i) {
    total_rdma_write_num += rdma_write_num[i][0];
    total_rdma_write_size += rdma_write_size[i][0];
    total_rdma_read_num += rdma_read_num[i][0];
    total_rdma_read_size += rdma_read_size[i][0];
    total_rdma_atomic_num += rdma_atomic_num[i][0];
    total_rdma_atomic_size += rdma_atomic_size[i][0];
    total_cache_miss += cache_miss[i][0];
    total_cache_hit += cache_hit[i][0];      
    total_stash_stall_num += stash_stall_num[i][0];
  }  

  txn_cache_hit_ratio = 100 * (double)total_cache_hit / (double)(total_cache_hit + total_cache_miss);

  fprintf(stdout, "======================== Statistics ========================\n");  
#ifdef STATS 
  fprintf(stdout, "Total RDMA write request number : %lu\n", total_rdma_write_num);
  fprintf(stdout, "Total RDMA write request size(B): %lu\n", total_rdma_write_size);
  fprintf(stdout, "Total RDMA read request number : %lu\n", total_rdma_read_num);
  fprintf(stdout, "Total RDMA read request size(B): %lu\n", total_rdma_read_size);
  fprintf(stdout, "Total RDMA atomic request number : %lu\n", total_rdma_atomic_num);
  fprintf(stdout, "Total RDMA atomic request size(B): %lu\n", total_rdma_atomic_size);
#endif
  fprintf(stdout, "Total cache hit : %lu\n", total_cache_hit);
  fprintf(stdout, "Total cache miss : %lu\n", total_cache_miss);
  fprintf(stdout, "Total stash stall number : %lu\n", total_stash_stall_num);
  fprintf(stdout, "============================================================\n"); 

  ClientStats stats;
  stats.cid = conf.cid;
  stats.metric = (uint16_t)bench;
  stats.thread_num = num_threads;
  stats.cache_size = conf.cache_size;
#ifdef VAR_VALUE_SIZE
  stats.value_size = SIMULATED_VALUE_SIZE;
  stats.kv_data_size = KV_DATA_SIZE;
#else
  stats.value_size = sizeof(Value_t);
  stats.kv_data_size = 0;
#endif
  stats.bulk_ops = bulk_requests.size();
  stats.load_ops = load_total_requests;
  stats.load_successed_insert = load_successed_insert_cnt; 
  stats.load_successed_search = load_successed_search_cnt;
  stats.load_successed_others = load_successed_others_cnt; 
  stats.load_start = load_start.tv_sec * 1000000000 + load_start.tv_nsec; 
  stats.load_end = load_end.tv_sec * 1000000000 + load_end.tv_nsec; 
  stats.load_cache_hit_ratio = load_cache_hit_ratio;
  stats.txn_ops = txn_total_requests;
  stats.txn_successed_insert = txn_successed_insert_cnt; 
  stats.txn_successed_search = txn_successed_search_cnt;
  stats.txn_successed_others = txn_successed_others_cnt; 
  stats.txn_start = txn_start.tv_sec * 1000000000 + txn_start.tv_nsec; 
  stats.txn_end = txn_end.tv_sec * 1000000000 + txn_end.tv_nsec; 
  stats.txn_cache_hit_ratio = txn_cache_hit_ratio;
  
  if (bench == BenchType::REAL) {
    std::string workload_info = workload_path + " (" + query_dist +
                                ", update=" + std::to_string((uint64_t)(100*update_ratio)) +
                                "%, search=" + std::to_string(100-(uint64_t)(100*update_ratio)) + "%)";

    strcpy(stats.load_workload_path, workload_info.c_str());
    strcpy(stats.txn_workload_path, workload_info.c_str());
  } else if (bench == BenchType::YCSB) {
    strcpy(stats.load_workload_path, load_workload_path.c_str());
    strcpy(stats.txn_workload_path, txn_workload_path.c_str());
  } else {
    strcpy(stats.load_workload_path, workload_path.c_str());
  }
  
  client->send_stats(&stats);

  delete lw;
  delete client;

  return 0;
}