#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

STATS=$1
THREAD=$2
CACHE_SIZE=$3 # MB
WORKLOAD_DIR=$4 # Path to the directory where workload exists
WORKLOAD_NAME=$5 # Workload name (e.g., a, b, c, d)

load_workload_path=${WORKLOAD_DIR}/load_randint_workload${WORKLOAD_NAME}
txn_workload_path=${WORKLOAD_DIR}/txn_randint_workload${WORKLOAD_NAME}

if [ ! -f ${load_workload_path} ]; then
  echo "[ERROR] YCSB load workload (${load_workload_path}) missing"
  exit 1;  
fi

if [ ! -f ${txn_workload_path} ]; then
  echo "[ERROR] YCSB txn workload (${txn_workload_path}) missing"
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

load_num_keys=$(cat ${load_workload_path} | wc -l)
txn_num_keys=$(cat ${txn_workload_path} | wc -l)

echo 2000 | tee /proc/sys/vm/nr_hugepages >/dev/null
sleep 1
hugepages=`grep HugePages_Total /proc/meminfo`
echo "[NOTICE] Finished setting huge pages (${hugepages})"

./benchmark_client  ${REPO_ROOT}/client_config.json ycsb ${THREAD} ${WORKLOAD_DIR} ${WORKLOAD_NAME} ${load_num_keys} ${CACHE_SIZE} 8 0 0 ${txn_num_keys} 0

echo 0 | tee /proc/sys/vm/nr_hugepages >/dev/null
sleep 1
hugepages=`grep HugePages_Total /proc/meminfo`
echo "[NOTICE] Released huge pages (${hugepages})"