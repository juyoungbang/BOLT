#ifndef _MERGE_MANAGER_H
#define _MERGE_MANAGER_H

#include "bolt.h"

#include <cmath>
#include <vector>
#include <chrono>
#include <utility>

constexpr double MERGE_WEIGHT = 0.9;
constexpr auto INVALID_TIME = std::chrono::high_resolution_clock::time_point::min();

class MergeManager {
public:
  MergeManager(uint64_t _table_per_mn) {
    table_per_mn = _table_per_mn;
    
    unit_merge_t = new double[table_per_mn];
    unit_append_t = new double[table_per_mn];
    unit_merge_num = new uint64_t[table_per_mn];
    unit_append_num = new uint64_t[table_per_mn];

    over_merge_error = new double[table_per_mn];
    under_merge_error = new double[table_per_mn];
    over_merge_num = new uint64_t[table_per_mn];
    under_merge_num = new uint64_t[table_per_mn];

    over_append_error = new double[table_per_mn];
    under_append_error = new double[table_per_mn];
    over_append_num = new uint64_t[table_per_mn];
    under_append_num = new uint64_t[table_per_mn];

    merge_start = new std::chrono::high_resolution_clock::time_point[table_per_mn];
    merge_duration = new double[table_per_mn];

    append_start = new std::chrono::high_resolution_clock::time_point[table_per_mn];
    start_stash_e_num = new uint64_t[table_per_mn];
    append_duration = new double[table_per_mn];

    for (uint16_t i = 0; i < table_per_mn; ++i) {
      unit_merge_t[i] = 0;
      unit_append_t[i] = 0;
      unit_merge_num[i] = 0;
      unit_append_num[i] = 0;
      over_merge_error[i] = 0;
      under_merge_error[i] = 0;
      over_merge_num[i] = 0;
      under_merge_num[i] = 0;
      over_append_error[i] = 0;
      under_append_error[i] = 0;
      over_append_num[i] = 0;
      under_append_num[i] = 0;
    }
  }

  ~MergeManager(void) { }

  uint64_t get_start_stash_e_num(uint16_t tab_num) {
    return start_stash_e_num[tab_num];
  }

  std::pair<double, uint64_t> get_over_merge_error(void) {
    double error_sum = 0;
    uint64_t error_num = 0;
    for (uint16_t i = 0; i < table_per_mn; ++i) {
      error_sum += over_merge_error[i];
      error_num += over_merge_num[i];
    }

    return std::make_pair(error_sum/table_per_mn, error_num);
  }

  std::pair<double, uint64_t> get_under_merge_error(void) { 
    double error_sum = 0;
    uint64_t error_num = 0;
    for (uint16_t i = 0; i < table_per_mn; ++i) {
      error_sum += under_merge_error[i];
      error_num += under_merge_num[i];
    }

    return std::make_pair(error_sum/table_per_mn, error_num);
  }

  std::pair<double, uint64_t> get_over_append_error(void) {
    double error_sum = 0;
    uint64_t error_num = 0;
    for (uint16_t i = 0; i < table_per_mn; ++i) {
      error_sum += over_append_error[i];
      error_num += over_append_num[i];
    }

    return std::make_pair(error_sum/table_per_mn, error_num);
  }

  std::pair<double, uint64_t> get_under_append_error(void) {
    double error_sum = 0;
    uint64_t error_num = 0;
    for (uint16_t i = 0; i < table_per_mn; ++i) {
      error_sum += under_append_error[i];
      error_num += under_append_num[i];
    }

    return std::make_pair(error_sum/table_per_mn, error_num);
  }

  void start_merge_timer(uint16_t tab_num) {
    merge_start[tab_num] = std::chrono::high_resolution_clock::now();
  }

  void end_merge_timer(uint16_t tab_num, uint64_t est_merge_stash_e_num, uint64_t est_merge_dir_e_num, uint64_t merge_stash_e_num, uint64_t merge_dir_e_num, uint64_t end_stash_e_num) {
    merge_duration[tab_num] = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - merge_start[tab_num]).count();
    update_unit_merge_t(tab_num, est_merge_stash_e_num, est_merge_dir_e_num, merge_stash_e_num, merge_dir_e_num, end_stash_e_num);
  }

  void start_append_timer(uint16_t tab_num, uint64_t _start_stash_e_num) {
    append_start[tab_num] = std::chrono::high_resolution_clock::now();
    start_stash_e_num[tab_num] = _start_stash_e_num;
  }

  void end_append_timer(uint16_t tab_num, uint64_t end_stash_e_num) {
    append_duration[tab_num] = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - append_start[tab_num]).count();
    update_unit_append_t(tab_num, start_stash_e_num[tab_num], end_stash_e_num);
  }

  std::vector<uint16_t> get_merge_candidate(uint64_t* merge_stash_e_num) {
    std::vector<uint16_t> candidate;
    std::vector<double> remain_t_list(table_per_mn);
    for (uint16_t i = 0; i < table_per_mn; ++i) {
      if (!is_ready(i)) {
        if (merge_stash_e_num[i] >= MERGE_TRIGGER_UTIL*STASH_CAPACITY) {
          std::vector<uint16_t> single_candidate = {i};
          return single_candidate;
        }

        continue;
      } else if (merge_stash_e_num[i]) {
        candidate.push_back(i);
        remain_t_list[i] = estimate_stash_full_t(i, STASH_CAPACITY - merge_stash_e_num[i]);
      }
    }

    sort(candidate.begin(), candidate.end(), [&](int i, int j){ return remain_t_list[i] < remain_t_list[j]; });

    if (candidate.size() > table_per_mn) {
      candidate.erase(candidate.begin()+table_per_mn, candidate.end());
    }

    assert(candidate.size() <= table_per_mn);
    return candidate;
  }

  std::vector<uint16_t> schedule_merge(std::vector<uint16_t> candidate, uint64_t* merge_stash_e_num, uint64_t* merge_dir_e_num) {
    std::vector<uint16_t> schedule;

    if (candidate.size() == 0) {
      return schedule;
    }

    std::vector<std::pair<double, double>> estimation(candidate.size()); // (stash full remaining time, merge time)
    for (int i = 0; i < candidate.size(); ++i) {
      if (!is_ready(candidate[i])) {
        if (merge_stash_e_num[candidate[i]] >= MERGE_TRIGGER_UTIL*STASH_CAPACITY) {
          assert(candidate.size() == 1);
          return candidate;
        }

        continue;
      } else if (merge_stash_e_num[candidate[i]]) {
        double merge_t = estimate_merge_t(candidate[i], merge_stash_e_num[candidate[i]], merge_dir_e_num[candidate[i]]);
        double remain_t = estimate_stash_full_t(candidate[i], STASH_CAPACITY - merge_stash_e_num[candidate[i]]);

        if (i == 0) {
          estimation[i] = std::make_pair(remain_t, merge_t);
        } else {
          estimation[i] = std::make_pair(remain_t, estimation[i-1].second+merge_t);
        }
      }
    }

    for (int i = candidate.size()-1; i >= 0; --i) {
      if (estimation[i].second > estimation[i].first) {
        for (uint16_t j = 0; j <= i; j++) {
          schedule.push_back(candidate[j]);
        }

        break;
      }
    }

    return schedule;
  }

private:
  uint64_t table_per_mn;

  double* unit_merge_t; // Estimated merge time for a single entry
  double* unit_append_t; // Estimated stash append time for a single entry
  uint64_t* unit_merge_num;
  uint64_t* unit_append_num;

  /* Statistics. */
  double* over_merge_error;
  double* under_merge_error;
  uint64_t* over_merge_num;
  uint64_t* under_merge_num;

  double* over_append_error;
  double* under_append_error;
  uint64_t* over_append_num;
  uint64_t* under_append_num;

  std::chrono::high_resolution_clock::time_point* merge_start;
  double* merge_duration;

  std::chrono::high_resolution_clock::time_point* append_start;
  uint64_t* start_stash_e_num;
  double* append_duration;

  inline bool is_ready(uint16_t tab_num) {
    return unit_merge_t[tab_num] && unit_append_t[tab_num];
  }

  /* Default policy takes incremental average. */
  void update_unit_merge_t(uint16_t tab_num, uint64_t est_merge_stash_e_num, uint64_t est_merge_dir_e_num, uint64_t merge_stash_e_num, uint64_t merge_dir_e_num, uint64_t end_stash_e_num) {
    assert(est_merge_stash_e_num != 0);
    assert(merge_stash_e_num != 0);

    if (is_ready(tab_num)) {
      double new_merge_error = 100*(estimate_merge_t(tab_num, est_merge_stash_e_num, est_merge_dir_e_num) - merge_duration[tab_num]) / merge_duration[tab_num];
      if (new_merge_error < 0) {
        under_merge_error[tab_num] = incr_avg(under_merge_error[tab_num], under_merge_num[tab_num], new_merge_error);
      } else {
        over_merge_error[tab_num] = incr_avg(over_merge_error[tab_num], over_merge_num[tab_num], new_merge_error);
      }

      double new_append_error = (merge_duration[tab_num] / unit_append_t[tab_num]) - (end_stash_e_num - merge_stash_e_num);
      if (new_append_error < 0) {
        under_append_error[tab_num] = incr_avg(under_append_error[tab_num], under_append_num[tab_num], new_append_error);
      } else {
        over_append_error[tab_num] = incr_avg(over_append_error[tab_num], over_append_num[tab_num], new_append_error);
      }
    }

    double new_unit_merge_t = merge_duration[tab_num] / (merge_stash_e_num + merge_dir_e_num);
    unit_merge_t[tab_num] = weighted_avg(unit_merge_t[tab_num], MERGE_WEIGHT, new_unit_merge_t);
  }
  
  /* Current policy takes incremental average. */
  void update_unit_append_t(uint16_t tab_num, uint64_t start_stash_e_num, uint64_t end_stash_e_num) {
    if (start_stash_e_num < end_stash_e_num && end_stash_e_num < STASH_CAPACITY) {
      double new_unit_append_t = append_duration[tab_num] / (end_stash_e_num - start_stash_e_num);
      unit_append_t[tab_num] = weighted_avg(unit_append_t[tab_num], MERGE_WEIGHT, new_unit_append_t);
    }
  }

  double estimate_merge_t(uint16_t tab_num, uint64_t merge_stash_e_num, uint64_t merge_dir_e_num) {
    assert(is_ready(tab_num));
    return unit_merge_t[tab_num]*(merge_stash_e_num + merge_dir_e_num);
  }
  
  double estimate_stash_full_t(uint16_t tab_num, uint64_t remain_stash_e_num) {
    assert(is_ready(tab_num));
    if (remain_stash_e_num > MERGE_ALPHA_ENTRY)
      return unit_append_t[tab_num]*(remain_stash_e_num-MERGE_ALPHA_ENTRY);
    else
      return 0;
  }
};

#endif // _MERGE_MANAGER_H