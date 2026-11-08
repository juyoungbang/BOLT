#ifndef _SERVER_H
#define _SERVER_H

#include <vector>
#include <iostream>
#include <numa.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>

#include "utils.h"
#include "config.h"
#include "rdma.h"
#include "table.h"
#include "directory.h"
#include "bucket.h"
#include "bolt.h"
#include "thread_connection.h"

class Server {
public:
  struct ClientStats* client_stats_list;
  
  Server(const struct ServerConfig& conf);
  ~Server(void);

  virtual void* busy_wait(void) = 0;

protected:
  uint16_t sid;
  uint16_t client_num; 
  uint16_t server_num;
  char server_ips[16][16];

  uint32_t ib_dev_id;
  uint32_t ib_port_id;
  uint8_t  ib_gid_idx;

  bool is_roce;
  uint16_t udp_port;
  uint32_t udp_sock;

  void* buf;
  ThreadConnection* t_conn;

  uint64_t* client_thread_num;
  uint64_t* conn_client_thread_num;
  uint64_t conn_server_thread_num;

  uint64_t base_size[6];
  uint64_t base_addr[6];

  struct ibv_context* ib_ctx;
  struct ibv_pd* ib_pd;
  struct ibv_port_attr ib_port_attr;
  union  ibv_gid ib_gid;
  struct ibv_cq* ib_cq;
  struct ibv_mr* ib_mr[6];
  std::vector<struct ibv_qp*> server_ib_qp_list;
  
  /* For RDMA RECV/SEND verbs. */
  uint64_t msg_size;
  uint64_t msg_addr;
  struct ibv_mr* msg_mr;
  std::vector<std::vector<struct ibv_qp*>> client_ib_qp_list;

  uint64_t recv_size;
  uint64_t recv_addr;
  struct ibv_cq* recv_cq;
  struct ibv_cq* send_cq;

  struct ibv_sge* sge;
  struct ibv_recv_wr* recv_wr;

  bool recv_udp_thread_info(struct ThreadInfo* info, struct sockaddr_in* thread_addr, socklen_t* thread_addr_len);
  void send_udp_thread_info(struct ibv_qp* ib_qp, struct sockaddr_in udp_server_info);
  void clear_udp_socket(void);
  bool recv_udp_server_info(struct ServerInfo* info);
  bool recv_signal(struct Signal& sig);
  void recv_stats(void);
  void broadcast_signal(SignalType sig);
  void broadcast_table_update(uint16_t tab_id);
  bool client_connection_done(void);
  bool server_connection_done(void);
  bool make_connection(struct ThreadInfo* msg, struct sockaddr_in* thread_addr, socklen_t thread_addr_len);

  /* For RDMA RECV/SEND verbs. */
  void init_message_passing(void);
  inline void* get_recv_buffer(uint64_t idx);
  inline uint64_t get_send_buffer(uint64_t idx);
};

#endif // _SERVER_H