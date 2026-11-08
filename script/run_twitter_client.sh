#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

STATS=$1
THREAD=$2
CACHE_SIZE=$3 # MB
WORKLOAD_DIR=$4 # Path to the directory where workload exists
WORKLOAD_NAME=$5 # Workload name
NUM_KEYS=$6 # Number of keys
VALUE_SIZE=$7 # Value size
LOAD_PERCENT=$8 # Percentage of operations to warm up the index

WORKLOAD_PATH=${WORKLOAD_DIR}/${WORKLOAD_NAME}

if [ ! -f ${WORKLOAD_PATH} ]; then
  echo "[ERROR] No workload named ${WORKLOAD_PATH}"
  exit 1;  
fi

if [ ! -d "${REPO_ROOT}/build" ]; then
  mkdir "${REPO_ROOT}/build"
fi

cd ${REPO_ROOT}/build
cmake -DSTATS=${STATS} ..
make clean
make benchmark_client

if [ ! -d "${REPO_ROOT}/test/result" ]; then
  mkdir "${REPO_ROOT}/test/result"
fi

echo 2000 | tee /proc/sys/vm/nr_hugepages >/dev/null
sleep 1
hugepages=`grep HugePages_Total /proc/meminfo`
echo "[NOTICE] Finished setting huge pages (${hugepages})"

./benchmark_client ${REPO_ROOT}/client_config.json twitter ${THREAD} ${WORKLOAD_DIR} ${WORKLOAD_NAME} ${NUM_KEYS} ${CACHE_SIZE} ${VALUE_SIZE} 0 0 0 ${LOAD_PERCENT}

echo 0 | tee /proc/sys/vm/nr_hugepages >/dev/null
sleep 1
hugepages=`grep HugePages_Total /proc/meminfo`
echo "[NOTICE] Released huge pages (${hugepages})"