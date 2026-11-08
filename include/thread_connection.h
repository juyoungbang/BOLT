/* This code is derived from Sherman. */

#ifndef _THREAD_CONNECTION_H
#define _THREAD_CONNECTION_H

#include <numa.h>
#include <infiniband/verbs.h>

#include "config.h"
#include "bolt.h"
#include "utils.h"
#include "rdma.h"

class ThreadConnection {
private:
  uint16_t server_num;

  uint64_t ib_dev_id;
  uint64_t ib_port_id;
  uint8_t  ib_gid_idx;

  bool is_roce;

  uint64_t base_size;
  uint64_t base_addr;

  struct ibv_context* ib_ctx;
  struct ibv_pd* ib_pd;
  struct ibv_port_attr ib_port_attr;
  union  ibv_gid ib_gid;
  struct ibv_cq* ib_recv_cq; // Receive queue
  struct ibv_cq* ib_cq; // Send queue
  struct ibv_mr* ib_mr;
  std::vector<struct ibv_qp*> ib_qp_list;
  std::vector<struct ServerInfo> server_info_list;

  /* For RDMA RECV/SEND verbs. */
  uint64_t recv_size;
  uint64_t recv_addr;

  struct ibv_sge sge[TOTAL_TAB_NUM+1];
  struct ibv_recv_wr recv_wr[TOTAL_TAB_NUM+1];

  friend class Client;
  friend class Server;
  friend class MemoryServer;

public:
  ThreadConnection(const struct ServerConfig& conf, bool share_cq = true);
  ThreadConnection(const struct ClientConfig& conf, uint64_t core_id, bool share_cq = true);
  ~ThreadConnection(void);

  struct ibv_qp* create_qp(void);
  bool make_connection(struct ibv_qp* ib_qp, struct sockaddr_in udp_server_info, struct ServerInfo* msg);

  /* For RDMA RECV/SEND verbs. */
  void init_message_passing(void);
  void* get_recv_buffer(uint64_t idx);
  uint64_t get_send_buffer(uint64_t idx);
};

#endif // _THREAD_CONNECTION_H