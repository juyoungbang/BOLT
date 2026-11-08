#!/bin/bash

PHASE=$1

# Only measure the core that runs the memory node
MN_CORE=$2

log_path=../test/result/BOLT_SERVER_CPU_UTIL_${PHASE}.log

if [ -f ${log_path} ]; then
  sudo rm ${log_path}
fi

touch ${log_path}
LC_ALL=C mpstat -P ${MN_CORE} 1 > ${log_path} &