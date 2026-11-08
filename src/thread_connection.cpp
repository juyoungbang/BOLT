/* This code is derived from Sherman. */

#include "thread_connection.h"

ThreadConnection::ThreadConnection(const struct ServerConfig& conf, bool share_cq) {
  server_num = conf.server_num;

  ib_dev_id = conf.ib_dev_id;
  ib_port_id = conf.ib_port_id;
  ib_gid_idx = conf.ib_gid_idx;

  is_roce = conf.is_roce;
  base_size = conf.base_size;

  // Allocate memory region for IO buffer
  base_addr = (uint64_t)mmap(NULL, base_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  if (base_addr == (uint64_t)MAP_FAILED) {
    fprintf(stderr, "[ERROR] %luB IO Buffer mmap failure (error: %s)\n", base_size, strerror(errno));
  }

  // Initialize RDMA
  ib_ctx = rdma_create_context(ib_dev_id, ib_port_id, &ib_port_attr, &ib_gid, ib_gid_idx, is_roce);
  ib_pd = rdma_allocate_pd(ib_ctx);
  ib_recv_cq = rdma_create_client_cq(ib_ctx);
  ib_cq = share_cq ? ib_recv_cq : rdma_create_client_cq(ib_ctx);
  ib_mr = rdma_register_mr(ib_pd, base_addr, base_size);
}

ThreadConnection::ThreadConnection(const struct ClientConfig& conf, uint64_t core_id, bool share_cq) {
  server_num = conf.server_num;

  ib_dev_id = conf.ib_dev_id;
  ib_port_id = conf.ib_port_id;
  ib_gid_idx = conf.ib_gid_idx;

  is_roce = conf.is_roce;
  base_size = conf.base_size;

  struct bitmask* bm = numa_allocate_nodemask();
  numa_bitmask_clearall(bm);
  numa_bitmask_setbit(bm, numa_node_of_cpu(core_id));
  numa_bind(bm);
  numa_run_on_node(numa_node_of_cpu(core_id));

  // Allocate memory region for IO buffer
  base_addr = (uint64_t)mmap(NULL, base_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  if (base_addr == (uint64_t)MAP_FAILED) {
    fprintf(stderr, "[ERROR] %luB IO Buffer mmap failure (error: %s)\n", base_size, strerror(errno));
  }

  std::memset((void*)base_addr, 0, base_size);
  numa_free_nodemask(bm);

  // Initialize RDMA
  ib_ctx = rdma_create_context(ib_dev_id, ib_port_id, &ib_port_attr, &ib_gid, ib_gid_idx, is_roce);
  ib_pd = rdma_allocate_pd(ib_ctx);
  ib_cq = rdma_create_client_cq(ib_ctx);
  ib_recv_cq = rdma_create_client_cq(ib_ctx);
  ib_cq = share_cq ? ib_recv_cq : rdma_create_client_cq(ib_ctx);
  ib_mr = rdma_register_mr(ib_pd, base_addr, base_size);
}

// TODO: Need to safely free memory region
ThreadConnection::~ThreadConnection(void) {
  if (ib_ctx) {
    ibv_close_device(ib_ctx);
  }

  if (ib_pd) {
    ibv_dealloc_pd(ib_pd);
  }
}

struct ibv_qp* ThreadConnection::create_qp(void) {
  return rdma_create_qp(IBV_QPT_RC, ib_ctx, ib_pd, ib_cq, ib_recv_cq);
}

bool ThreadConnection::make_connection(struct ibv_qp* ib_qp, struct sockaddr_in udp_server_info, struct ServerInfo* msg) {
  uint16_t sid = ntohs(msg->sid);
  uint16_t lid = ntohs(msg->lid);

  uint8_t gid[16];
  memcpy((char *)gid, (char *)msg->gid, sizeof(uint8_t)*16);

  uint8_t gid_idx = msg->gid_idx;
  uint32_t qp_num = ntohl(msg->qp_num);

  if (ib_qp_list.size() <= sid) {
    ib_qp_list.resize(sid + 1);
  }

  if (ib_qp_list[sid]) {
    return false;
  }

  ib_qp_list[sid] = ib_qp;

  // Finalize QP connection
  rdma_modify_qp_to_init(ib_qp_list[sid], ib_port_id);
  rdma_modify_qp_to_rtr(ib_qp_list[sid], ib_port_id, qp_num, lid, gid, gid_idx, is_roce); 
  rdma_modify_qp_to_rts(ib_qp_list[sid]);

  // Save remote server QP information
  if (server_info_list.size() <= sid) {
    server_info_list.resize(sid + 1);
  }
  
  server_info_list[sid].sid = sid;
  server_info_list[sid].lid = lid;
  memcpy(server_info_list[sid].gid, gid, sizeof(uint8_t)*16);
  server_info_list[sid].gid_idx = gid_idx;
  server_info_list[sid].qp_num = qp_num;

  server_info_list[sid].mr_rkey[MRType::TABLE] = ntohl(msg->mr_rkey[MRType::TABLE]);
  server_info_list[sid].mr_rkey[MRType::DIRECTORY] = ntohl(msg->mr_rkey[MRType::DIRECTORY]);
  server_info_list[sid].mr_rkey[MRType::STASH] = ntohl(msg->mr_rkey[MRType::STASH]);
  server_info_list[sid].mr_rkey[MRType::BUCKET] = ntohl(msg->mr_rkey[MRType::BUCKET]);
  server_info_list[sid].mr_rkey[MRType::KV_DATA] = ntohl(msg->mr_rkey[MRType::KV_DATA]);
  server_info_list[sid].mr_rkey[MRType::ON_CHIP] = ntohl(msg->mr_rkey[MRType::ON_CHIP]);

  server_info_list[sid].base_size[MRType::TABLE] = ntohll(msg->base_size[MRType::TABLE]);
  server_info_list[sid].base_size[MRType::DIRECTORY] = ntohll(msg->base_size[MRType::DIRECTORY]);
  server_info_list[sid].base_size[MRType::STASH] = ntohll(msg->base_size[MRType::STASH]);
  server_info_list[sid].base_size[MRType::BUCKET] = ntohll(msg->base_size[MRType::BUCKET]);
  server_info_list[sid].base_size[MRType::KV_DATA] = ntohll(msg->base_size[MRType::KV_DATA]);
  server_info_list[sid].base_size[MRType::ON_CHIP] = ntohll(msg->base_size[MRType::ON_CHIP]);

  server_info_list[sid].base_addr[MRType::TABLE] = ntohll(msg->base_addr[MRType::TABLE]);
  server_info_list[sid].base_addr[MRType::DIRECTORY] = ntohll(msg->base_addr[MRType::DIRECTORY]);
  server_info_list[sid].base_addr[MRType::STASH] = ntohll(msg->base_addr[MRType::STASH]);
  server_info_list[sid].base_addr[MRType::BUCKET] = ntohll(msg->base_addr[MRType::BUCKET]);
  server_info_list[sid].base_addr[MRType::KV_DATA] = ntohll(msg->base_addr[MRType::KV_DATA]);
  server_info_list[sid].base_addr[MRType::ON_CHIP] = ntohll(msg->base_addr[MRType::ON_CHIP]);

  //fprintf(stdout, "[NOTICE] Connected to server (Server ID: %hd)\n", sid);

  return true;
}

void ThreadConnection::init_message_passing(void) {
  recv_size = base_size/2;
  recv_addr = base_addr + (base_size - recv_size);

  assert(base_size-recv_size > sizeof(ClientStats)*server_num);
  assert(recv_size > sizeof(Signal)*(TOTAL_TAB_NUM+1));

  for (int i = 0; i < TOTAL_TAB_NUM+1; ++i) {
    memset(&sge[i], 0, sizeof(sge[i]));
    sge[i].addr = recv_addr + i*recv_size/(TOTAL_TAB_NUM+1);
    sge[i].length = sizeof(Signal);
    sge[i].lkey = ib_mr->lkey;

    memset(&recv_wr[i], 0, sizeof(recv_wr[i]));
    recv_wr[i].wr_id = i;
    recv_wr[i].sg_list = &sge[i];
    recv_wr[i].num_sge = 1;

    uint16_t tab_sid = (i == TOTAL_TAB_NUM) ? MASTER_SERVER_ID : i % server_num;

    struct ibv_recv_wr *bad_wr;
    if (ibv_post_recv(ib_qp_list[tab_sid], &recv_wr[i], &bad_wr)) {
      fprintf(stderr, "[ERROR] Failed RDMA RECV (error: %s)\n", strerror(errno));
    }
  }
}

void* ThreadConnection::get_recv_buffer(uint64_t idx) {
  return (void *)(recv_addr + idx*recv_size/(TOTAL_TAB_NUM+1));
}

uint64_t ThreadConnection::get_send_buffer(uint64_t idx) {
  return base_addr + idx*(base_size-recv_size)/server_num;
}