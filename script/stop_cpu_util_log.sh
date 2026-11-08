#!/bin/bash

PHASE=$1

# Only MN_CORE was measured; convert its utilization to a full-machine figure
MN_CORE=$2
TOTAL_CORE=$3

log_path=../test/result/BOLT_SERVER_CPU_UTIL_${PHASE}.log

pkill -9 -f mpstat

awk -v x=${PHASE} -v core=${MN_CORE} -v total=${TOTAL_CORE} '
  $1 ~ /^[0-9]/ && $2 == core {
    util += (100 - $NF)
    count++
  }
  END {
    if (count > 0)
      printf("CPU average utilization during %s: %.4f %%\n", x, (util / count) / total)
    else
      printf("CPU had no samples during %s\n", x)
  }
' ${log_path}

sudo rm  ${log_path}