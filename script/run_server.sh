#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

STATS=$1

if [ ! -d "${REPO_ROOT}/build" ]; then
  mkdir "${REPO_ROOT}/build"
fi

cd ${REPO_ROOT}/build
cmake -DSTATS=${STATS} ..
make clean
make benchmark_server

if [ ! -d "${REPO_ROOT}/test/result" ]; then
  mkdir "${REPO_ROOT}/test/result"
fi

echo 130000 | tee /proc/sys/vm/nr_hugepages >/dev/null
sleep 1
hugepages=`grep HugePages_Total /proc/meminfo`
echo "[NOTICE] Finished setting huge pages (${hugepages})"

./benchmark_server ${REPO_ROOT}/server_config.json

echo 0 | tee /proc/sys/vm/nr_hugepages >/dev/null
sleep 1
hugepages=`grep HugePages_Total /proc/meminfo`
echo "[NOTICE] Released huge pages (${hugepages})"