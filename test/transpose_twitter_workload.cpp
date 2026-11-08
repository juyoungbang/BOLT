#include <vector>
#include <unordered_set>
#include <string>
#include <sstream>
#include <fstream>
#include <iostream>
#include <assert.h>

#include "bolt.h"
#include "key_generator.h"

#define PRINT_PERCENT 5

int main(int argc, char *argv[]) {
  if (argc != 4) {
    fprintf(stderr, "[ERROR] Three arguments are required but received %d\n", argc - 1);
    exit(1);
  }

  std::string workload_dir = argv[1];
  std::string workload_name = argv[2];
  uint64_t num_keys = atol(argv[3]);

  std::string workload_path = workload_dir + "/" + workload_name;
  std::string new_workload_path = workload_dir + "/" + workload_name + ".new";

  FILE* fpt;
  fpt = fopen(new_workload_path.c_str(), "w");

  if (!fpt) {
    fprintf(stdout, "[ERROR] Failed opening file %s (error: %s)\n", new_workload_path.c_str(), strerror(errno));
    exit(1);
  }

  std::unordered_set<uint64_t> seen_keys;

  std::ifstream ifs;
  ifs.open(workload_path);

  if (!ifs.is_open()) {
    fprintf(stdout, "[ERROR] Failed opening file %s (error: %s)\n", workload_path.c_str(), strerror(errno));
    exit(1);
  }

  if (skip_BOM(ifs)) {
    fprintf(stdout, "[NOTICE] Removed BOM in target load workload\n");
  }

  fprintf(stdout, "[NOTICE] Start reading %lu keys\n", num_keys);

  uint64_t progress = 0;
  uint64_t percent = num_keys * ((double)PRINT_PERCENT/100.0);

  std::string line;
  while (std::getline(ifs, line)) {
    if (progress % percent == 0) {
      fprintf(stdout, "Progress (");
      for (uint64_t j = 0; j < progress/percent; j++) fprintf(stdout, "■");
      for (uint64_t j = 0; j < 100/PRINT_PERCENT - progress/percent; j++) fprintf(stdout, "▢");
      fprintf(stdout, ") / Estimated insertion keys: %lu (%.2f %%)\n", seen_keys.size(), 100*(double)seen_keys.size()/(double)progress);
    }

    std::stringstream ss(line);

    std::string field;
    std::vector<std::string> fields(7);
    for (int i = 0; i < 7; ++i) {
      std::getline(ss, fields[i], ',');
    }

    std::string key = fields[1];
    std::string op = fields[5];

    uint64_t int_key = str2int_xor(key);

    if (seen_keys.find(int_key) == seen_keys.end()) {
      op = "insert";
      seen_keys.insert(int_key);
    }

    fprintf(fpt, "%s %lu\n", op.c_str(), int_key);

    progress++;
  }

  fprintf(stdout, "Estimated insertion keys within workload: %lu (%.2f %%)\n", seen_keys.size(), 100*(double)seen_keys.size()/(double)num_keys);

  fclose(fpt);

  return 0;
}