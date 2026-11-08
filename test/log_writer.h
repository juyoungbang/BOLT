#ifndef _LOG_WRITER_H
#define _LOG_WRITER_H

#include <cstdarg>
#include <string>
#include <sstream>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>

#include "bucket.h"

using namespace std;

class LogWriter {
private:
  FILE* fpt;

public:
  LogWriter(string name) {
    time_t timer = time(NULL);
    struct tm* t = localtime(&timer);

    char date_c_str[20];
    strftime(date_c_str, 20, "%Y%m%d_%H:%M:%S", t);
    string date(date_c_str);

    ostringstream ss;
    ss << "../test/result/BOLT_" << name << "_LOG_" << date << ".txt";

    fpt = fopen(ss.str().c_str(), "w");
  };

  ~LogWriter(void) {
    fclose(fpt);
  };

  void LOG(const char* format, ...) {
    char new_format[256];
    sprintf(new_format, "%s\n", format);

    va_list args;
    va_start(args, format);
    vfprintf(fpt, new_format, args);
    va_end(args);
  };

  void LOG_error(const char* format, ...) {
    char new_format[256];
    sprintf(new_format, "[ERROR] %s\n", format);

    va_list args;
    va_start(args, format);
    vfprintf(fpt, new_format, args);
    va_end(args);
  };

  void LOG_notice(const char* format, ...) {
    char new_format[256];
    sprintf(new_format, "[NOTICE] %s\n", format);

    va_list args;
    va_start(args, format);
    vfprintf(fpt, new_format, args);
    va_end(args);
  };

  void LOG_server_info(uint16_t server_num, uint16_t client_num, uint64_t thread_num, uint64_t cache_size, const char* workload_path, uint64_t bulk_keys) {
    fprintf(fpt, "==================== Server Information ====================\n");
    fprintf(fpt, "Server number        : %u\n", server_num);
    fprintf(fpt, "Client number        : %u (total %lu threads)\n", client_num, thread_num);
    fprintf(fpt, "Cache size           : %lu MB\n", cache_size/MB);
    fprintf(fpt, "Total table number   : %lu\n", TOTAL_TAB_NUM);
    fprintf(fpt, "Maximum search length: %lu\n", MAX_SEARCH_LENGTH);
    fprintf(fpt, "Keys per bucket      : %lu (%lu B)\n", BUCKET_CAPACITY, sizeof(Bucket));
    fprintf(fpt, "Workload             : %s\n", workload_path);
    fprintf(fpt, "Bulk load key number : %lu\n", bulk_keys);
    fprintf(fpt, "============================================================\n");
  }

  void LOG_ycsb_server_info(uint16_t server_num, uint16_t client_num, uint64_t thread_num, uint64_t cache_size, const char* load_workload_path, const char* txn_workload_path, uint64_t bulk_keys) {
    fprintf(fpt, "==================== Server Information ====================\n");
    fprintf(fpt, "Server number        : %u\n", server_num);
    fprintf(fpt, "Client number        : %u (total %lu threads)\n", client_num, thread_num);
    fprintf(fpt, "Cache size           : %lu MB\n", cache_size/MB);
    fprintf(fpt, "Total table number   : %lu\n", TOTAL_TAB_NUM);
    fprintf(fpt, "Maximum search length: %lu\n", MAX_SEARCH_LENGTH);
    fprintf(fpt, "Keys per bucket      : %lu (%lu B)\n", BUCKET_CAPACITY, sizeof(Bucket));
    fprintf(fpt, "Load workload        : %s\n", load_workload_path);
    fprintf(fpt, "Txn workload         : %s\n", txn_workload_path);
    fprintf(fpt, "Bulk load key number : %lu\n", bulk_keys);
    fprintf(fpt, "============================================================\n");
  }

  void LOG_twitter_server_info(uint16_t server_num, uint16_t client_num, uint64_t thread_num, uint64_t cache_size, uint64_t value_size, const char* workload_path, uint64_t bulk_keys) {
    fprintf(fpt, "==================== Server Information ====================\n");
    fprintf(fpt, "Server number        : %u\n", server_num);
    fprintf(fpt, "Client number        : %u (total %lu threads)\n", client_num, thread_num);
    fprintf(fpt, "Cache size           : %lu MB\n", cache_size/MB);
    fprintf(fpt, "Total table number   : %lu\n", TOTAL_TAB_NUM);
    fprintf(fpt, "Maximum search length: %lu\n", MAX_SEARCH_LENGTH);
    fprintf(fpt, "Keys per bucket      : %lu (%lu B)\n", BUCKET_CAPACITY, sizeof(Bucket));
    fprintf(fpt, "Value size           : %lu B\n", value_size);
    fprintf(fpt, "Workload             : %s\n", workload_path);
    fprintf(fpt, "Bulk load key number : %lu\n", bulk_keys);
    fprintf(fpt, "============================================================\n");
  }

  void LOG_client_info(uint64_t thread_num, uint64_t cache_size, const char* workload_path, uint64_t bulk_keys, uint64_t load_keys, uint64_t txn_keys, const char* dist, double update_ratio) {
    fprintf(fpt, "==================== Client Information ====================\n");
    fprintf(fpt, "Thread number        : %lu\n", thread_num);
    fprintf(fpt, "Cache size           : %lu MB\n", cache_size/MB);
    fprintf(fpt, "Total table number   : %lu\n", TOTAL_TAB_NUM);
    fprintf(fpt, "Maximum search length: %lu\n", MAX_SEARCH_LENGTH);
    fprintf(fpt, "Keys per bucket      : %lu (%lu B)\n", BUCKET_CAPACITY, sizeof(Bucket));
    fprintf(fpt, "Load workload        : %s (%lu keys)\n", workload_path, load_keys);
    fprintf(fpt, "Txn workload         : %s (%s, %lu keys)\n", workload_path, dist, txn_keys);
    fprintf(fpt, "  >> Update : %lu keys (%lu%%)\n", (uint64_t)(txn_keys*update_ratio), (uint64_t)(100*update_ratio));
    fprintf(fpt, "  >> Search : %lu keys (%lu%%)\n", txn_keys - (uint64_t)(txn_keys*update_ratio), 100-(uint64_t)(100*update_ratio));
    fprintf(fpt, "Bulk load key number : %lu\n", bulk_keys);
    fprintf(fpt, "============================================================\n");
  }

  void LOG_ycsb_client_info(uint64_t thread_num, uint64_t cache_size, const char* load_workload_path, const char* txn_workload_path, uint64_t bulk_keys, uint64_t load_keys, uint64_t txn_keys) {
    fprintf(fpt, "==================== Client Information ====================\n");
    fprintf(fpt, "Thread number        : %lu\n", thread_num);
    fprintf(fpt, "Cache size           : %lu MB\n", cache_size/MB);
    fprintf(fpt, "Total table number   : %lu\n", TOTAL_TAB_NUM);
    fprintf(fpt, "Maximum search length: %lu\n", MAX_SEARCH_LENGTH);
    fprintf(fpt, "Keys per bucket      : %lu (%lu B)\n", BUCKET_CAPACITY, sizeof(Bucket));
    fprintf(fpt, "Load workload        : %s (%lu keys)\n", load_workload_path, load_keys);
    fprintf(fpt, "Txn workload         : %s (%lu keys)\n", txn_workload_path, txn_keys);
    fprintf(fpt, "Bulk load key number : %lu\n", bulk_keys);
    fprintf(fpt, "============================================================\n");
  }

  void LOG_twitter_client_info(uint64_t thread_num, uint64_t cache_size, uint64_t value_size, const char* workload_path, uint64_t bulk_keys, uint64_t load_keys, uint64_t txn_keys) {
    fprintf(fpt, "==================== Client Information ====================\n");
    fprintf(fpt, "Thread number        : %lu\n", thread_num);
    fprintf(fpt, "Cache size           : %lu MB\n", cache_size/MB);
    fprintf(fpt, "Total table number   : %lu\n", TOTAL_TAB_NUM);
    fprintf(fpt, "Maximum search length: %lu\n", MAX_SEARCH_LENGTH);
    fprintf(fpt, "Keys per bucket      : %lu (%lu B)\n", BUCKET_CAPACITY, sizeof(Bucket));
    fprintf(fpt, "Value size           : %lu B\n", value_size);
    fprintf(fpt, "Workload             : %s\n", workload_path);
    fprintf(fpt, "  >> Load : %lu keys\n", load_keys);
    fprintf(fpt, "  >> Txn  : %lu keys\n", txn_keys);
    fprintf(fpt, "Bulk load key number : %lu\n", bulk_keys);
    fprintf(fpt, "============================================================\n");
  }

  void LOG_client_cache_info(CacheStats* cache_stats) {
    uint64_t total_tab_size = 0;
    uint64_t total_cached_tab_size = 0;
    for (uint16_t i = 0; i < cache_stats->tab_num; ++i) {
      total_tab_size += cache_stats->tab_size[i];

      if (cache_stats->tab_cached[i]) {
        total_cached_tab_size += cache_stats->tab_size[i];
      } else {
        total_cached_tab_size += sizeof(TableHeader);
      }
    }

    fprintf(fpt, "\n[Cache statistics]\n");
    fprintf(fpt, "Total cache size: %lu MB\n", cache_stats->cache_size/MB); 
    fprintf(fpt, "  >>> Free cache size: %ld\n", cache_stats->free_size);
    fprintf(fpt, "  >>> Used cache size (table): %ld\n", total_cached_tab_size);
    fprintf(fpt, "  >>> Used cache size (directory): %ld\n", cache_stats->cache_size - GHOST_CACHE_SIZE - total_cached_tab_size - cache_stats->free_size);
    fprintf(fpt, "Skiplist node number: %ld\n", cache_stats->skiplist_node_num); 
    
    fprintf(fpt, "\n[Table statistics]\n");
    fprintf(fpt, "Total table size: %lu B (%lu B cached)\n", total_tab_size, total_cached_tab_size);
    for (uint16_t i = 0; i < cache_stats->tab_num; ++i) {
      if (cache_stats->tab_cached[i]) {
        fprintf(fpt, "  >>> Table %u: %lu B (cached)\n", i, cache_stats->tab_size[i]);
      } else {
        fprintf(fpt, "  >>> Table %u: %lu B (not cached)\n", i, cache_stats->tab_size[i]);
      }
    }
  }

  void print_client_info(uint64_t thread_num, uint64_t cache_size, const char* workload_path, uint64_t bulk_keys, uint64_t load_keys, uint64_t txn_keys, const char* dist, double update_ratio) {
    fprintf(stdout, "==================== Client Information ====================\n");
    fprintf(stdout, "Thread number        : %lu\n", thread_num);
    fprintf(stdout, "Cache size           : %lu MB\n", cache_size/MB);
    fprintf(stdout, "Total table number   : %lu\n", TOTAL_TAB_NUM);
    fprintf(stdout, "Maximum search length: %lu\n", MAX_SEARCH_LENGTH);
    fprintf(stdout, "Keys per bucket      : %lu (%lu B)\n", BUCKET_CAPACITY, sizeof(Bucket));
    fprintf(stdout, "Load workload        : %s (%lu keys)\n", workload_path, load_keys);
    fprintf(stdout, "Txn workload         : %s (%s, %lu keys)\n", workload_path, dist, txn_keys);
    fprintf(stdout, "  >> Update : %lu keys (%lu%%)\n", (uint64_t)(txn_keys*update_ratio), (uint64_t)(100*update_ratio));
    fprintf(stdout, "  >> Search : %lu keys (%lu%%)\n", txn_keys - (uint64_t)(txn_keys*update_ratio), 100-(uint64_t)(100*update_ratio));
    fprintf(stdout, "Bulk load key number : %lu\n", bulk_keys);
    fprintf(stdout, "============================================================\n");
  }

  void print_ycsb_client_info(uint64_t thread_num, uint64_t cache_size, const char* load_workload_path, const char* txn_workload_path, uint64_t bulk_keys, uint64_t load_keys, uint64_t txn_keys) {
    fprintf(stdout, "==================== Client Information ====================\n");
    fprintf(stdout, "Thread number        : %lu\n", thread_num);
    fprintf(stdout, "Cache size           : %lu MB\n", cache_size/MB);
    fprintf(stdout, "Total table number   : %lu\n", TOTAL_TAB_NUM);
    fprintf(stdout, "Maximum search length: %lu\n", MAX_SEARCH_LENGTH);
    fprintf(stdout, "Keys per bucket      : %lu (%lu B)\n", BUCKET_CAPACITY, sizeof(Bucket));
    fprintf(stdout, "Load workload        : %s (%lu keys)\n", load_workload_path, load_keys);
    fprintf(stdout, "Txn workload         : %s (%lu keys)\n", txn_workload_path, txn_keys);
    fprintf(stdout, "Bulk load key number : %lu\n", bulk_keys);
    fprintf(stdout, "============================================================\n");
  }

  void print_twitter_client_info(uint64_t thread_num, uint64_t cache_size, uint64_t value_size, const char* workload_path, uint64_t bulk_keys, uint64_t load_keys, uint64_t txn_keys) {
    fprintf(stdout, "==================== Client Information ====================\n");
    fprintf(stdout, "Thread number        : %lu\n", thread_num);
    fprintf(stdout, "Cache size           : %lu MB\n", cache_size/MB);
    fprintf(stdout, "Total table number   : %lu\n", TOTAL_TAB_NUM);
    fprintf(stdout, "Maximum search length: %lu\n", MAX_SEARCH_LENGTH);
    fprintf(stdout, "Keys per bucket      : %lu (%lu B)\n", BUCKET_CAPACITY, sizeof(Bucket));
    fprintf(stdout, "Value size           : %lu B\n", value_size);
    fprintf(stdout, "Workload             : %s\n", workload_path);
    fprintf(stdout, "  >> Load : %lu keys\n", load_keys);
    fprintf(stdout, "  >> Txn  : %lu keys\n", txn_keys);
    fprintf(stdout, "Bulk load key number : %lu\n", bulk_keys);
    fprintf(stdout, "============================================================\n");
  }
};

#endif // _LOG_WRITER_H