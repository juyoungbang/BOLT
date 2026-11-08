#include "server.h"

Server::Server(const struct ServerConfig& conf) : client_stats_list(NULL) {
  sid = conf.sid;
  client_num = conf.client_num;
  server_num = conf.server_num;

  for (int i = 0; i < server_num; ++i) {
    strncpy(server_ips[i], conf.server_ips[i], 16);
  }

  ib_dev_id = conf.ib_dev_id;
  ib_port_id = conf.ib_port_id;
  ib_gid_idx = conf.ib_gid_idx;

  is_roce = conf.is_roce;
  udp_port = conf.udp_port;
  udp_sock = socket(AF_INET, SOCK_DGRAM, 0);
  
  t_conn = new ThreadConnection(conf);
  buf = (void*)t_conn->base_addr;

  client_thread_num = new uint64_t[client_num]{0}; 
  conn_client_thread_num = new uint64_t[client_num]{0}; 
  conn_server_thread_num = 0;

  struct timeval timeout;
  timeout.tv_sec  = 1;
  timeout.tv_usec = 0;
  setsockopt(udp_sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(struct timeval));

  struct sockaddr_in udp_server_info;
  udp_server_info.sin_family = AF_INET;
  udp_server_info.sin_port = htons(udp_port);
  udp_server_info.sin_addr.s_addr = htons(INADDR_ANY);

  int ret = bind(udp_sock, (struct sockaddr *)&udp_server_info, sizeof(struct sockaddr_in)); 
  if (ret < 0) {
    fprintf(stderr, "[ERROR] Failed binding UDP socket (error: %s)\n", strerror(errno));
  }

  base_size[MRType::TABLE] = conf.tab_size;
  base_size[MRType::DIRECTORY] = conf.dir_size;
  base_size[MRType::STASH] = conf.stash_size;
  base_size[MRType::BUCKET] = conf.bucket_size;
  base_size[MRType::KV_DATA] = conf.kv_data_size;
  base_size[MRType::ON_CHIP] = ON_CHIP_SIZE;

  // Allocate memory region for table
  base_addr[MRType::TABLE] = (uint64_t)mmap(NULL, base_size[MRType::TABLE], PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  if (base_addr[MRType::TABLE] == (uint64_t)MAP_FAILED) {
    fprintf(stderr, "[ERROR] %luB table mmap failure (error: %s)\n", base_size[MRType::TABLE], strerror(errno));
  }

  // Allocate memory region for directories
  base_addr[MRType::DIRECTORY] = (uint64_t)mmap(NULL, base_size[MRType::DIRECTORY], PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  if (!base_addr[MRType::DIRECTORY] == (uint64_t)MAP_FAILED) {
    fprintf(stderr, "[ERROR] %luB directory mmap failure (error: %s)\n", base_size[MRType::DIRECTORY], strerror(errno));
  }

  // Allocate memory region for stash directory
  base_addr[MRType::STASH] = (uint64_t)mmap(NULL, base_size[MRType::STASH], PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  if (!base_addr[MRType::STASH] == (uint64_t)MAP_FAILED) {
    fprintf(stderr, "[ERROR] %luB stash directory mmap failure (error: %s)\n", base_size[MRType::STASH], strerror(errno));
  }

  // Allocate memory region for buckets
  base_addr[MRType::BUCKET] = (uint64_t)mmap(NULL, base_size[MRType::BUCKET], PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  if (!base_addr[MRType::BUCKET] == (uint64_t)MAP_FAILED) {
    fprintf(stderr, "[ERROR] %luB bucket mmap failure (error: %s)\n", base_size[MRType::BUCKET], strerror(errno));
  }

  // Allocate memory region for KV data
  base_addr[MRType::KV_DATA] = (uint64_t)mmap(NULL, base_size[MRType::KV_DATA], PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  if (!base_addr[MRType::KV_DATA] == (uint64_t)MAP_FAILED) {
    fprintf(stderr, "[ERROR] %luB KV data mmap failure (error: %s)\n", base_size[MRType::KV_DATA], strerror(errno));
  }

  base_addr[MRType::ON_CHIP] = 0;

  // Initialize RDMA
  ib_ctx = rdma_create_context(ib_dev_id, ib_port_id, &ib_port_attr, &ib_gid, ib_gid_idx, is_roce);
  ib_pd = rdma_allocate_pd(ib_ctx);
  ib_cq = rdma_create_server_cq(ib_ctx);

  ib_mr[MRType::TABLE] = rdma_register_mr(ib_pd, base_addr[MRType::TABLE], base_size[MRType::TABLE]);
  ib_mr[MRType::DIRECTORY] = rdma_register_mr(ib_pd, base_addr[MRType::DIRECTORY], base_size[MRType::DIRECTORY]);
  ib_mr[MRType::STASH] = rdma_register_mr(ib_pd, base_addr[MRType::STASH], base_size[MRType::STASH]);
  ib_mr[MRType::BUCKET] = rdma_register_mr(ib_pd, base_addr[MRType::BUCKET], base_size[MRType::BUCKET]);
  ib_mr[MRType::KV_DATA] = rdma_register_mr(ib_pd, base_addr[MRType::KV_DATA], base_size[MRType::KV_DATA]);
  ib_mr[MRType::ON_CHIP] = rdma_register_on_chip_mr(ib_ctx, ib_pd, base_addr[MRType::ON_CHIP], base_size[MRType::ON_CHIP]);

  msg_size = conf.base_size;
  msg_addr = (uint64_t)mmap(NULL, msg_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  if (!msg_addr == (uint64_t)MAP_FAILED) {
    fprintf(stderr, "[ERROR] %luB message buffer mmap failure (error: %s)\n", msg_size, strerror(errno));
  }

  recv_cq = rdma_create_server_cq(ib_ctx);
  send_cq = rdma_create_server_cq(ib_ctx);
  msg_mr = rdma_register_mr(ib_pd, msg_addr, msg_size);
};

Server::~Server(void) {
  delete[] client_stats_list;
  
  if (ib_pd) {
    ibv_dealloc_pd(ib_pd);
  }

  if (ib_ctx) {
    ibv_close_device(ib_ctx);
  } 
};

bool Server::recv_udp_thread_info(struct ThreadInfo* info, struct sockaddr_in* thread_addr, socklen_t* thread_addr_len) {
  uint8_t type;
  auto ret = recvfrom(udp_sock, &type, sizeof(uint8_t), MSG_PEEK, NULL, NULL);

  if (ret > 0 && (MsgType)type == MsgType::THREAD_INFO) {
    recvfrom(udp_sock, info, sizeof(ThreadInfo), 0, (struct sockaddr*)thread_addr, thread_addr_len);
    return true;
  } 

  return false;
};

void Server::send_udp_thread_info(struct ibv_qp* ib_qp, struct sockaddr_in udp_server_info) {
  struct ThreadInfo info;

  info.msg = MsgType::THREAD_INFO;
  info.is_server = true;
  
  info.id = htons(sid);
  info.tid = htonll(0);
  info.lid = htons(ib_port_attr.lid);
  info.qp_num = htonl(ib_qp->qp_num);
  info.base_addr = htonll(t_conn->base_addr);
  info.thread_num = htonll(1);

  if (is_roce) {
    memcpy(info.gid, (uint8_t*)(&ib_gid), sizeof(uint8_t)*16);
    info.gid_idx = ib_gid_idx;
  } else {
    memset(info.gid, 0, sizeof(uint8_t)*16);
    info.gid_idx = 0;  
  }
  
  int send_len = sendto(udp_sock, &info, sizeof(ThreadInfo), 0, (struct sockaddr*)&udp_server_info, sizeof(struct sockaddr_in));

  if (send_len < 0) {
    fprintf(stderr, "[ERROR] Failed sending UDP server information (error: %s)\n", strerror(errno));
  }
};

void Server::clear_udp_socket(void) {
  uint64_t buffer;
  while (recvfrom(udp_sock, &buffer, sizeof(uint64_t), 0, NULL, NULL) > 0) {
    continue;
  }

  close(udp_sock);
  init_message_passing();
};

bool Server::recv_udp_server_info(struct ServerInfo* info) {
  uint8_t type;
  auto ret = recvfrom(udp_sock, &type, sizeof(uint8_t), MSG_PEEK, NULL, NULL);

  if (ret > 0 && (MsgType)type == MsgType::SERVER_INFO) {
    recvfrom(udp_sock, info, sizeof(ServerInfo), 0, NULL, NULL);
    return true;
  }

  return false;
};

bool Server::recv_signal(struct Signal& sig) {
  uint64_t wr_id;
  auto ret = rdma_poll_once_with_id(recv_cq, wr_id);

  if (ret) {
    sig = *(Signal *)get_recv_buffer(wr_id);
    assert(sig.msg == MsgType::SIGNAL);   

    struct ibv_recv_wr* bad_wr;
    if (ibv_post_recv(client_ib_qp_list[sig.id][client_thread_num[sig.id]-1], &recv_wr[wr_id], &bad_wr)) {
      fprintf(stderr, "[ERROR] Failed RDMA RECV (error: %s)\n", strerror(errno));
    }

    return true;
  }

  return false;
};  

void Server::recv_stats(void) {
  client_stats_list = new ClientStats[client_num];

  ClientStats stats;
  uint16_t received_clients = 0;
  while (received_clients < client_num) {
    uint64_t wr_id;
    auto ret = rdma_poll_once_with_id(recv_cq, wr_id);

    if (ret) {
      stats = *(ClientStats *)get_recv_buffer(wr_id);
      assert(stats.msg == MsgType::CLIENT_STAT);   

      struct ibv_recv_wr* bad_wr;
      if (ibv_post_recv(client_ib_qp_list[stats.cid][client_thread_num[stats.cid]-1], &recv_wr[wr_id], &bad_wr)) {
        fprintf(stderr, "[ERROR] Failed RDMA RECV (error: %s)\n", strerror(errno));
      }

      client_stats_list[received_clients] = stats;
      received_clients++;
    }
  }
}

void Server::broadcast_signal(SignalType sig_type) {
  sleep(3);

  fprintf(stdout, "[NOTICE] Broadcasting signal %d to %u clients\n", sig_type, client_num);

  // Broadcast to all clients
  for (uint16_t i = 0; i < client_num; ++i) {
    uint64_t sig_buffer = get_send_buffer(i);

    struct Signal* sig = (Signal *)sig_buffer;
    sig->id = sid;
    sig->signal = (uint8_t)sig_type;

    while (true) {
      rdma_post_send(client_ib_qp_list[i][client_thread_num[i]-1], sig_buffer,
                    sizeof(Signal), msg_mr->lkey, i);

      if (rdma_poll_completion(send_cq)) {
        break;
      } else {
        fprintf(stderr, "[CAUTION] Retry broadcasting signal (error: %s)\n", strerror(errno));
      }
    }
  }
};

void Server::broadcast_table_update(uint16_t tab_id) {
  //fprintf(stdout, "[NOTICE] Broadcasting table ID %u update to %lu clients\n", tab_id, client_num);

  // Broadcast to all clients
  for (uint16_t i = 0; i < client_num; ++i) {
    uint64_t sig_buffer = get_send_buffer(i);

    struct Signal* sig = (Signal *)sig_buffer;
    sig->id = sid;
    sig->signal = (uint8_t)SignalType::TABLE_UPDATE;
    sig->tab_id = tab_id;
  
    while (true) {
      rdma_post_send(client_ib_qp_list[i][client_thread_num[i]-1], sig_buffer,
                     sizeof(Signal), msg_mr->lkey, i);

      if (rdma_poll_completion(send_cq)) {
        break;
      } else {
        fprintf(stderr, "[CAUTION] Retry broadcasting table update (error: %s)\n", strerror(errno));
      }
    }
  }
};

bool Server::client_connection_done(void) { 
  for (int i = 0; i < client_num; ++i) {
    assert(client_thread_num[i] >= conn_client_thread_num[i]);
    if (client_thread_num[i] == 0 || client_thread_num[i] > conn_client_thread_num[i]) {
      return false;
    } 
  }

  return true;
};

bool Server::server_connection_done(void) { 
  if (server_num > conn_server_thread_num) {
    return false;
  }

  return true;
};

bool Server::make_connection(struct ThreadInfo* msg, struct sockaddr_in* thread_addr, socklen_t thread_addr_len) {
  // Retrieve remote QP information
  bool is_server = msg->is_server;

  uint16_t id = ntohs(msg->id);
  uint64_t tid = ntohll(msg->tid);
  uint16_t lid = ntohs(msg->lid);

  uint8_t gid[16];
  memcpy((char *)gid, (char *)msg->gid, sizeof(uint8_t)*16);

  uint8_t  gid_idx = msg->gid_idx;
  uint32_t qp_num = ntohl(msg->qp_num);
  
  struct ibv_qp* ib_qp;
  if (is_server) {
    if (server_ib_qp_list.size() <= id) {
      server_ib_qp_list.resize(id + 1);
    }

    ib_qp = server_ib_qp_list[id];
  } else {
    if (client_ib_qp_list.size() <= id) {
      client_ib_qp_list.resize(id + 1);
    } 
    
    if (client_ib_qp_list[id].size() <= tid) {
      client_ib_qp_list[id].resize(tid + 1);
    }

    ib_qp = client_ib_qp_list[id][tid];

    if (client_thread_num[id] == 0) {
      client_thread_num[id] = ntohll(msg->thread_num);
    }
  }

  if (!is_server && ib_qp) {
    return false;
  } else {
    if (tid == client_thread_num[id]-1) {
      ib_qp = rdma_create_qp(IBV_QPT_RC, ib_ctx, ib_pd, send_cq, recv_cq);
    } else {
      ib_qp = rdma_create_qp(IBV_QPT_RC, ib_ctx, ib_pd, ib_cq, ib_cq);
    }

    // Finalize QP connection
    rdma_modify_qp_to_init(ib_qp, ib_port_id);
    rdma_modify_qp_to_rtr(ib_qp, ib_port_id, qp_num, lid, gid, gid_idx, is_roce);
    rdma_modify_qp_to_rts(ib_qp);

    if (is_server) {
      server_ib_qp_list[id] = ib_qp;
    } else {
      client_ib_qp_list[id][tid] = ib_qp;
    }
  }

  // Send reply to remote client
  struct ServerInfo reply;

  reply.sid = htons(sid);
  reply.lid = htons(ib_port_attr.lid);
  
  if (is_roce) {
    memcpy(reply.gid, (uint8_t*)&(ib_gid), sizeof(uint8_t)*16);
    reply.gid_idx = gid_idx;
  } else {
    memset(reply.gid, 0, sizeof(uint8_t)*16);
    reply.gid_idx = 0;
  }

  reply.qp_num = htonl(ib_qp->qp_num);

  if (tid != client_thread_num[id]-1) {
    reply.mr_rkey[MRType::TABLE] = htonl(ib_mr[MRType::TABLE]->rkey);
    reply.mr_rkey[MRType::DIRECTORY] = htonl(ib_mr[MRType::DIRECTORY]->rkey);
    reply.mr_rkey[MRType::STASH] = htonl(ib_mr[MRType::STASH]->rkey);
    reply.mr_rkey[MRType::BUCKET] = htonl(ib_mr[MRType::BUCKET]->rkey);
    reply.mr_rkey[MRType::KV_DATA] = htonl(ib_mr[MRType::KV_DATA]->rkey);
    reply.mr_rkey[MRType::ON_CHIP] = htonl(ib_mr[MRType::ON_CHIP]->rkey);

    reply.base_size[MRType::TABLE] = htonll(base_size[MRType::TABLE]);
    reply.base_size[MRType::DIRECTORY] = htonll(base_size[MRType::DIRECTORY]);
    reply.base_size[MRType::STASH] = htonll(base_size[MRType::STASH]);
    reply.base_size[MRType::BUCKET] = htonll(base_size[MRType::BUCKET]);
    reply.base_size[MRType::KV_DATA] = htonll(base_size[MRType::KV_DATA]);
    reply.base_size[MRType::ON_CHIP] = htonll(base_size[MRType::ON_CHIP]);

    reply.base_addr[MRType::TABLE] = htonll(base_addr[MRType::TABLE]);
    reply.base_addr[MRType::DIRECTORY] = htonll(base_addr[MRType::DIRECTORY]);
    reply.base_addr[MRType::STASH] = htonll(base_addr[MRType::STASH]);
    reply.base_addr[MRType::BUCKET] = htonll(base_addr[MRType::BUCKET]);
    reply.base_addr[MRType::KV_DATA] = htonll(base_addr[MRType::KV_DATA]);
    reply.base_addr[MRType::ON_CHIP] = htonll(base_addr[MRType::ON_CHIP]);
  }

  int send_len = sendto(udp_sock, &reply, sizeof(ServerInfo), 0, (struct sockaddr*)thread_addr, thread_addr_len);

  if (is_server) {
    //fprintf(stdout, "[NOTICE] Replied to new server thread (Server ID: %hd)\n", id);
  } else {
    conn_client_thread_num[id]++;
    if (client_thread_num[id] == conn_client_thread_num[id]) {
      fprintf(stdout, "[NOTICE] Replied to all client threads (Client ID: %hd)\n", id);
    }
  }

  return true;
};   

void Server::init_message_passing(void) {
  recv_size = msg_size/2;
  recv_addr = msg_addr + (msg_size - recv_size);

  assert(msg_size-recv_size > sizeof(ClientStats)*client_num);
  assert(recv_size > sizeof(Signal)*client_num);

  sge = new struct ibv_sge[client_num];
  recv_wr = new struct ibv_recv_wr[client_num];
  memset(sge, 0, sizeof(ibv_sge)*client_num);
  memset(recv_wr, 0, sizeof(ibv_recv_wr)*client_num);

  for (int i = 0; i < client_num; ++i) {
    sge[i].addr = recv_addr + i*recv_size/client_num;
    sge[i].length = sizeof(ClientStats);
    sge[i].lkey = msg_mr->lkey;

    recv_wr[i].wr_id = i;
    recv_wr[i].sg_list = &sge[i];
    recv_wr[i].num_sge = 1;

    struct ibv_recv_wr* bad_wr;
    if (ibv_post_recv(client_ib_qp_list[i][client_thread_num[i]-1], &recv_wr[i], &bad_wr)) {
      fprintf(stderr, "[ERROR] Failed RDMA RECV (error: %s)\n", strerror(errno));
    }
  }
};  

inline void* Server::get_recv_buffer(uint64_t idx) {
  return (void *)(recv_addr + idx*recv_size/client_num);
};

inline uint64_t Server::get_send_buffer(uint64_t idx) {
  return msg_addr + idx*(msg_size-recv_size)/client_num;
};