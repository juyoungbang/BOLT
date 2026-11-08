#!/bin/bash

WORKLOAD_DIR=/mnt/ssd/twitter # Path to the directory where Twitter workload exists
WORKLOAD_NAME=cluster3.sort # Twitter workload name

workload_path=${WORKLOAD_DIR}/${WORKLOAD_NAME}

if [ ! -f ${workload_path} ]; then
  echo "[ERROR] Twitter workload (${workload_path}) missing"
  exit 1;  
fi

if [ ! -d "../build" ]; then
  mkdir "../build"
fi

cd ../build
cmake ..
make transpose_twitter_workload 

num_keys=$(cat ${workload_path} | wc -l)

./transpose_twitter_workload ${WORKLOAD_DIR} ${WORKLOAD_NAME} ${num_keys}