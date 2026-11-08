#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

STATS=$1
THREAD=$2
CACHE_SIZE=$3 # MB
WORKLOAD_DIR=$4 # Path to the directory where workload exists
WORKLOAD_NAME=$5 # Workload name
QUERY_DIST=$6 # Query distribution (e.g., uniform, zipfian)
UPDATE_RATIO=$7 # Ratio of update operation (e.g., 0.3 -> 30% update, 70% search)

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

num_keys=$(cat ${WORKLOAD_PATH} | wc -l)

echo 2000 | tee /proc/sys/vm/nr_hugepages >/dev/null
sleep 1
hugepages=`grep HugePages_Total /proc/meminfo`
echo "[NOTICE] Finished setting huge pages (${hugepages})"

./benchmark_client ${REPO_ROOT}/client_config.json real ${THREAD} ${WORKLOAD_DIR} ${WORKLOAD_NAME} ${num_keys} ${CACHE_SIZE} 8 ${QUERY_DIST} ${UPDATE_RATIO} 0 0

echo 0 | tee /proc/sys/vm/nr_hugepages >/dev/null
sleep 1
hugepages=`grep HugePages_Total /proc/meminfo`
echo "[NOTICE] Released huge pages (${hugepages})"