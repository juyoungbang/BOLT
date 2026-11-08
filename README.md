# BOLT

A disaggregated-memory **ordered index** accessed over one-sided RDMA.

## Overview

BOLT is an ordered index for disaggregated memory.
Compute nodes (**clients**) issue one-sided RDMA verbs directly 
against memory nodes (**memory servers**) that hold the index.

## Architecture

| Component | Files | Role |
|-----------|-------|------|
| Client | `include/client.h`, `src/client.cpp` | Compute node client. |
| Memory server | `include/memory_server.h`, `src/memory_server.cpp` | Owns the index memory regions and runs a background merge thread. |
| Index layout | `include/table.h`, `directory.h`, `bucket.h` | Defines the three-level index structure. |
| Client cache | `include/cache.h` | Manages the compute-node cache. |
| RDMA transport | `src/rdma.cpp`, `include/thread_connection.h` | Provides one-sided RDMA operations for remote data access. |

## Requirements

- An RDMA-capable NIC (Mellanox). Install the driver stack with `script/install_mlnx.sh`,
  which installs MLNX_OFED 4.9-5.1.0.0.
- CMake ≥ 3.16 and a C++17 compiler.
- Libraries: libibverbs, libnuma, Google CityHash, Boost, and Intel TBB.

Run `script/install_libs.sh` and `script/install_mlnx.sh` on each node to install the
dependencies and RDMA drivers.

## Build

```sh
mkdir build && cd build
cmake ..
make benchmark_server benchmark_client
```

## Configuration

Runtime settings live in `server_config.json` and `client_config.json`. Memory nodes (MNs)
read `server_config.json`; compute nodes (CNs) read `client_config.json`. Set each on the
node it applies to.

| Field | Description |
|-------|-------------|
| `server_id` / `client_id` | This node's id (server 0 is the master). |
| `udp_port` | Port used for out-of-band setup . |
| `server_num` / `client_num` | Number of memory servers / clients. |
| `server_ips` / `client_ips` | IP address lists (lengths must match the counts above). |
| `ib_dev_id`, `ib_port_id`, `ib_gid_idx` | RDMA device/port/GID selection. |
| `conn_type` | `"IB"` for InfiniBand or `"RoCE"`. |

### Environment settings (`include/bolt.h`)

A few compile-time settings at the top of `include/bolt.h` must match your machines:

| Macro | Meaning |
|-------|---------|
| `CPU_NUM`, `CORE_PER_CPU`, `HYPERTHREAD_PER_CORE` | CPU topology of the node (sockets, physical cores per socket, threads per core). |
| `MN_CORE` | Core on the memory node dedicated to the background merge. |
| `LOC_ROUTER_CORE` | Core on the compute node dedicated to request routing. |
| `ON_CHIP_SIZE` | Size (bytes) of the NIC on-chip device memory used for locks. |

### Workload

A YCSB run uses two plain-text files in the workload directory, named by workload letter
`<name>` (e.g. `a`, `b`, `c`):

- `load_randint_workload<name>` — the load phase. Every line is `INSERT <key>`.
- `txn_randint_workload<name>` — the transaction phase. Each line is one operation:
  `INSERT <key>`, `READ <key>`, `UPDATE <key>`, or `SCAN <key> <range_size>`.

Example YCSB workloads are available [here](https://drive.google.com/drive/folders/1Kk28ARs5zJmaNE9GophGuqAgXnlubUMf?usp=drive_link).
Download them into your workload directory before running.

### Single node

Start a memory server, then a client:

```sh
# On each memory node: 
sudo script/run_server.sh <STATS>

# On each compute node (YCSB example):
sudo script/run_ycsb_client.sh <STATS> <THREAD> <CACHE_SIZE> <WORKLOAD_DIR> <WORKLOAD_NAME>
```

`run_server.sh` parameters:

| Parameter | Meaning |
|-----------|---------|
| `STATS` | `1` to compile with statistics collection, `0` otherwise. |

`run_ycsb_client.sh` parameters:

| Parameter | Meaning |
|-----------|---------|
| `STATS` | `1` to compile with statistics collection, `0` otherwise. |
| `THREAD` | Number of client threads. |
| `CACHE_SIZE` | Client index-cache size in MB. |
| `WORKLOAD_DIR` | Directory holding the workload files. |
| `WORKLOAD_NAME` | Workload letter, e.g. `c` (selects `load_randint_workload<name>` / `txn_randint_workload<name>`). |

For example: `sudo script/run_ycsb_client.sh 0 63 4096 /mnt/ssd/data c`

## Running

Running requires root because the scripts reserve huge pages. Results are written to `test/result/`.
