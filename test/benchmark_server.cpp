#include <iostream>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

#include "config.h"
#include "server.h"
#include "memory_server.h"
#include "log_writer.h"

static void type1(uint16_t client_num, LogWriter* lw, ClientStats* stats_list, bool verbose) {
  BenchType metric = (BenchType)stats_list[0].metric;

  uint64_t total_load_ops = 0;
  uint64_t total_load_done_ops = 0;
  double total_load_cache_hit_ratio = 0;
  uint64_t total_txn_ops = 0;
  uint64_t total_txn_done_ops = 0;
  double total_txn_cache_hit_ratio = 0;

  double cum_load_throughput = 0;
  double cum_txn_throughput = 0;

  for (int i = 0; i < client_num; ++i) {
    uint16_t cid = stats_list[i].cid;

    uint64_t load_ops = stats_list[i].load_ops;
    uint64_t load_done_ops = stats_list[i].load_successed_insert + stats_list[i].load_successed_search + stats_list[i].load_successed_others;
    uint64_t load_time = stats_list[i].load_end - stats_list[i].load_start;
    double load_cache_hit_ratio = stats_list[i].load_cache_hit_ratio;

    uint64_t txn_ops = stats_list[i].txn_ops;
    uint64_t txn_done_ops = stats_list[i].txn_successed_insert + stats_list[i].txn_successed_search + stats_list[i].txn_successed_others;
    uint64_t txn_time = stats_list[i].txn_end - stats_list[i].txn_start;
    double txn_cache_hit_ratio = stats_list[i].txn_cache_hit_ratio;  

    if (verbose) {
      lw->LOG("[Client %d throughput] Load: %.3e (op/nsec)\tTxn: %.3e (op/nsec)", 
              cid, (double)load_ops/(double)load_time, (double)txn_ops/(double)txn_time);
      lw->LOG("[Client %d cache hit ratio] Load: %.2f %\tTxn: %.2f %", 
              cid, load_cache_hit_ratio, txn_cache_hit_ratio);
      lw->LOG("[Client %d operation status] Load: %lu/%lu\tTxn: %lu/%lu\n", 
              cid, load_done_ops, load_ops, txn_done_ops, txn_ops);
    }

    total_load_ops += load_ops;
    total_load_done_ops += load_done_ops;
    total_load_cache_hit_ratio += load_cache_hit_ratio;

    total_txn_ops += txn_ops;
    total_txn_done_ops += txn_done_ops;
    total_txn_cache_hit_ratio += txn_cache_hit_ratio;

    cum_load_throughput += ((double)load_ops/(double)load_time);
    cum_txn_throughput += ((double)txn_ops/(double)txn_time);
  }

  uint64_t earliest_load = UINT64_MAX;
  uint64_t latest_load = 0;
  uint64_t earliest_txn = UINT64_MAX;
  uint64_t latest_txn = 0;

  for (int i = 0; i < client_num; i++) {
    uint64_t e_load = stats_list[i].load_start;
    uint64_t l_load = stats_list[i].load_end;
    uint64_t e_txn = stats_list[i].txn_start;
    uint64_t l_txn = stats_list[i].txn_end;
    
    if (e_load < earliest_load) earliest_load = e_load;
    if (l_load > latest_load) latest_load = l_load;
    if (e_txn  < earliest_txn) earliest_txn = e_txn;
    if (l_txn  > latest_txn) latest_txn = l_txn;
  }

  uint64_t load_end2end_time = latest_load - earliest_load;
  uint64_t txn_end2end_time = latest_txn - earliest_txn;

  lw->LOG("[End-to-end throughput] Load: %.3e (op/nsec)\tTxn: %.3e (op/nsec)", 
          (double)total_load_ops/(double)load_end2end_time, (double)total_txn_ops/(double)txn_end2end_time);
  lw->LOG("[Cumulative throughput] Load: %.3e (op/nsec)\tTxn: %.3e (op/nsec)", 
          cum_load_throughput, cum_txn_throughput);
  lw->LOG("[Average cache hit ratio] Load: %.2f %\tTxn: %.2f %", 
          total_load_cache_hit_ratio/(double)client_num, total_txn_cache_hit_ratio/(double)client_num);
  lw->LOG("[Total operation status] Load: %lu/%lu\tTxn: %lu/%lu", 
          total_load_done_ops, total_load_ops, total_txn_done_ops, total_txn_ops);
}

static void type2(uint16_t client_num, LogWriter* lw, ClientStats* stats_list, bool verbose) {
  BenchType metric = (BenchType)stats_list[0].metric;
  assert(metric == BenchType::TWITTER);

  uint64_t total_load_ops = 0;
  uint64_t total_load_insert_ops = 0;
  uint64_t total_load_search_ops = 0;
  uint64_t total_load_others_ops = 0;
  double total_load_cache_hit_ratio = 0;
  uint64_t total_txn_ops = 0;
  uint64_t total_txn_insert_ops = 0;
  uint64_t total_txn_search_ops = 0;
  uint64_t total_txn_others_ops = 0;
  double total_txn_cache_hit_ratio = 0;

  double cum_load_throughput = 0;
  double cum_txn_throughput = 0;

  for (int i = 0; i < client_num; ++i) {
    uint16_t cid = stats_list[i].cid;

    uint64_t load_ops = stats_list[i].load_ops;
    uint64_t load_insert_ops = stats_list[i].load_successed_insert;
    uint64_t load_search_ops = stats_list[i].load_successed_search;
    uint64_t load_others_ops = stats_list[i].load_successed_others;
    uint64_t load_time = stats_list[i].load_end - stats_list[i].load_start;
    double load_cache_hit_ratio = stats_list[i].load_cache_hit_ratio;

    uint64_t txn_ops = stats_list[i].txn_ops;
    uint64_t txn_insert_ops = stats_list[i].txn_successed_insert;
    uint64_t txn_search_ops = stats_list[i].txn_successed_search;
    uint64_t txn_others_ops = stats_list[i].txn_successed_others;
    uint64_t txn_time = stats_list[i].txn_end - stats_list[i].txn_start;
    double txn_cache_hit_ratio = stats_list[i].txn_cache_hit_ratio;

    if (verbose) {
      double load_insert_ratio = (double)load_insert_ops / (double)load_ops;
      double load_search_ratio = (double)load_search_ops / (double)load_ops;
      double load_others_ratio = (double)load_others_ops / (double)load_ops;
      double txn_insert_ratio = (double)txn_insert_ops / (double)txn_ops;
      double txn_search_ratio = (double)txn_search_ops / (double)txn_ops;
      double txn_others_ratio = (double)txn_others_ops / (double)txn_ops;

      lw->LOG("[Client %d throughput] Load: %.3e (op/nsec)\tTxn: %.3e (op/nsec)", 
              cid, (double)load_ops/(double)load_time, (double)txn_ops/(double)txn_time);
      lw->LOG("[Client %d cache hit ratio] Load: %.2f %\tTxn: %.2f %",
              cid, load_cache_hit_ratio, txn_cache_hit_ratio);
      lw->LOG("[Client %d operation status] Load: %lu operations\tTxn: %lu operations", cid, load_ops, txn_ops);
      lw->LOG(">>> Load: %lu insert (%.2f %%), %lu search (%.2f %%), %lu others (%.2f %%)", load_insert_ops, 100*load_insert_ratio,
                                                                                            load_search_ops, 100*load_search_ratio,
                                                                                            load_others_ops, 100*load_others_ratio);
      lw->LOG(">>> Txn : %lu insert (%.2f %%), %lu search (%.2f %%), %lu others (%.2f %%)\n", txn_insert_ops, 100*txn_insert_ratio,
                                                                                              txn_search_ops, 100*txn_search_ratio,
                                                                                              txn_others_ops, 100*txn_others_ratio);
    }

    total_load_ops += load_ops;
    total_load_insert_ops += load_insert_ops;
    total_load_search_ops += load_search_ops;
    total_load_others_ops += load_others_ops;
    total_load_cache_hit_ratio += load_cache_hit_ratio;

    total_txn_ops += txn_ops;
    total_txn_insert_ops += txn_insert_ops;
    total_txn_search_ops += txn_search_ops;
    total_txn_others_ops += txn_others_ops;
    total_txn_cache_hit_ratio += txn_cache_hit_ratio;

    cum_load_throughput += ((double)load_ops/(double)load_time);
    cum_txn_throughput += ((double)txn_ops/(double)txn_time);
  }

  double total_load_insert_ratio = (double)total_load_insert_ops / (double)total_load_ops;
  double total_load_search_ratio = (double)total_load_search_ops / (double)total_load_ops;
  double total_load_others_ratio = (double)total_load_others_ops / (double)total_load_ops;
  double total_txn_insert_ratio = (double)total_txn_insert_ops / (double)total_txn_ops;
  double total_txn_search_ratio = (double)total_txn_search_ops / (double)total_txn_ops;
  double total_txn_others_ratio = (double)total_txn_others_ops / (double)total_txn_ops;

  uint64_t earliest_load = UINT64_MAX;
  uint64_t latest_load = 0;
  uint64_t earliest_txn = UINT64_MAX;
  uint64_t latest_txn = 0;

  for (int i = 0; i < client_num; i++) {
    uint64_t e_load = stats_list[i].load_start;
    uint64_t l_load = stats_list[i].load_end;
    uint64_t e_txn = stats_list[i].txn_start;
    uint64_t l_txn = stats_list[i].txn_end;
    
    if (e_load < earliest_load) earliest_load = e_load;
    if (l_load > latest_load) latest_load = l_load;
    if (e_txn  < earliest_txn) earliest_txn = e_txn;
    if (l_txn  > latest_txn) latest_txn = l_txn;
  }

  uint64_t load_end2end_time = latest_load - earliest_load;
  uint64_t txn_end2end_time = latest_txn - earliest_txn;

  lw->LOG("[End-to-end throughput] Load: %.3e (op/nsec)\tTxn: %.3e (op/nsec)", 
          (double)total_load_ops/(double)load_end2end_time, (double)total_txn_ops/(double)txn_end2end_time);
  lw->LOG("[Cumulative throughput] Load: %.3e (op/nsec)\tTxn: %.3e (op/nsec)", 
          cum_load_throughput, cum_txn_throughput);
  lw->LOG("[Average cache hit ratio] Load: %.2f %\tTxn: %.2f %", 
          total_load_cache_hit_ratio/(double)client_num, total_txn_cache_hit_ratio/(double)client_num);
  lw->LOG("[Total operation status] Load: %lu operations\tTxn: %lu operations", total_load_ops, total_txn_ops);
  lw->LOG(">>> Load: %lu insert (%.2f %%), %lu search (%.2f %%), %lu others (%.2f %%)", total_load_insert_ops, 100*total_load_insert_ratio,
                                                                                        total_load_search_ops, 100*total_load_search_ratio,
                                                                                        total_load_others_ops, 100*total_load_others_ratio);
  lw->LOG(">>> Txn : %lu insert (%.2f %%), %lu search (%.2f %%), %lu others (%.2f %%)", total_txn_insert_ops, 100*total_txn_insert_ratio,
                                                                                        total_txn_search_ops, 100*total_txn_search_ratio,
                                                                                        total_txn_others_ops, 100*total_txn_others_ratio);  
}

static void type3(uint16_t client_num, LogWriter* lw, ClientStats* stats_list, bool verbose) {
  BenchType metric = (BenchType)stats_list[0].metric;

  uint64_t total_ops = 0;

  double cum_latency = 0;
  double cum_throughput = 0;

  for (int i = 0; i < client_num; ++i) {
    uint16_t cid = stats_list[i].cid;

    uint64_t ops = stats_list[i].load_ops;
    uint64_t time = stats_list[i].load_end - stats_list[i].load_start;

    if (verbose) {
      lw->LOG("[Client %d latency] %.3e (nsec/op)", cid, (double)stats_list[i].txn_ops/(double)stats_list[i].thread_num);
      lw->LOG("[Client %d throughput] %.3e (op/nsec)", cid, (double)ops/(double)time);
      lw->LOG("[Client %d operation status] %lu/%lu", cid, ops, ops);
    }

    total_ops += ops;

    cum_latency += ((double)stats_list[i].txn_ops/(double)stats_list[i].thread_num);
    cum_throughput += ((double)ops/(double)time);
  }

  uint64_t earliest = UINT64_MAX;
  uint64_t latest = 0;

  for (int i = 0; i < client_num; i++) {
    uint64_t e = stats_list[i].load_start;
    uint64_t l = stats_list[i].load_end;
    
    if (e < earliest) earliest = e;
    if (l > latest) latest = l;
  }

  uint64_t end2end_time = latest- earliest;

  lw->LOG("[Average latency] %.3e (op/nsec)", cum_latency/(double)client_num);
  lw->LOG("[End-to-end throughput] %.3e (op/nsec)", (double)total_ops/(double)end2end_time);
  lw->LOG("[Cumulative throughput] %.3e (op/nsec)", cum_throughput);
  lw->LOG("[Total operation status]  %lu/%lu", total_ops, total_ops);
}

static void log_stats(uint16_t server_num, uint16_t client_num, ClientStats* stats_list, bool verbose=true) {
  LogWriter* lw = new LogWriter("SERVER");
  BenchType metric = (BenchType)stats_list[0].metric;

  uint64_t total_thread_num = 0;
  for (int i = 0; i < client_num; ++i) {
    total_thread_num += stats_list[i].thread_num;
  }
  
  if (metric == BenchType::REAL) {
    lw->LOG_server_info(server_num, client_num, total_thread_num, stats_list[0].cache_size, 
                        stats_list[0].load_workload_path, stats_list[0].bulk_ops);
    type1(client_num, lw, stats_list, verbose);
  } else if (metric == BenchType::YCSB) {
    lw->LOG_ycsb_server_info(server_num, client_num, total_thread_num, stats_list[0].cache_size, 
                             stats_list[0].load_workload_path, stats_list[0].txn_workload_path, stats_list[0].bulk_ops);
    type1(client_num, lw, stats_list, verbose);
  } else if (metric == BenchType::TWITTER) {
    lw->LOG_twitter_server_info(server_num, client_num, total_thread_num, stats_list[0].cache_size, 
                                stats_list[0].value_size, stats_list[0].load_workload_path, stats_list[0].bulk_ops);
    type2(client_num, lw, stats_list, verbose);
  } else if (metric == BenchType::RDMA_TEST) {
    lw->LOG_server_info(server_num, client_num, total_thread_num, stats_list[0].cache_size, 
                        stats_list[0].load_workload_path, stats_list[0].bulk_ops);
    type3(client_num, lw, stats_list, verbose);  
  } else {
    fprintf(stderr, "[ERROR] Undefined metric type detected\n");
    exit(1);
  }

  delete lw;
}

int main(int argc, char *argv[]) {
  if (argc != 2) {
    fprintf(stderr, "[ERROR] Server configuration path is not set\n");
    exit(1);
  }

  struct ServerConfig conf;
  load_server_config(argv[1], &conf);

  fprintf(stdout, "===================== Starting Server %d =====================\n", conf.sid);  

  MemoryServer* server = new MemoryServer(conf);
  
  if (conf.sid == MASTER_SERVER_ID) {
    pthread_t server_tid;
    pthread_create(&server_tid, NULL, master_server_main, server);  
    pthread_join(server_tid, NULL);
    log_stats(conf.server_num, conf.client_num, server->client_stats_list);
  } else {
    pthread_t server_tid;
    pthread_create(&server_tid, NULL, memory_server_main, server);  
    pthread_join(server_tid, NULL);
  }

  delete server;

  fprintf(stdout, "=====================  Ending Server %d  =====================\n", conf.sid);

  return 0;
}