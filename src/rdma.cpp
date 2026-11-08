#include <iostream>

#include "rdma.h"

static const char* layer_enum2str(uint8_t layer_enum) {
  switch (layer_enum)
  {
    case IBV_LINK_LAYER_INFINIBAND: return "InfiniBand";
    case IBV_LINK_LAYER_ETHERNET:   return "Ethernet (RoCE)";
    default:                        return "Unknown";
  }
}

static int mtu_enum2bytes(enum ibv_mtu mtu_enum) {
  switch (mtu_enum) {
    case IBV_MTU_256:  return 256;
    case IBV_MTU_512:  return 512;
    case IBV_MTU_1024: return 1024;
    case IBV_MTU_2048: return 2048;
    case IBV_MTU_4096: return 4096;
    default:           return -1;
  }
}

struct ibv_context* rdma_create_context(uint32_t ib_dev_id, uint32_t ib_port_id, struct ibv_port_attr* ib_port_attr, union ibv_gid* ib_gid, uint8_t ib_gid_idx, bool is_roce) {
  int num_device;
  struct ibv_device** ib_dev_list = ibv_get_device_list(&num_device);

  if (!ib_dev_list) {
    fprintf(stderr, "[ERROR] Failed to get IB device list (error: %s)\n", strerror(errno));
  }

  if (!num_device) {
    fprintf(stderr, "[ERROR] Found %d devices\n", num_device);
  }

  if (ib_dev_id > num_device) {
    fprintf(stderr, "[ERROR] IB device not found\n");
  }

  uint32_t _ib_dev_id = 0;
  for (int i = 0; i < num_device; ++i) {
    if (ibv_get_device_name(ib_dev_list[i])[5] == (char)(ib_dev_id + 48)) {
      _ib_dev_id = i;
      break;
    }
  }

  struct ibv_device* ib_dev = ib_dev_list[_ib_dev_id];
  struct ibv_context* ib_ctx = ibv_open_device(ib_dev);

  if (!ib_ctx) {
    fprintf(stderr, "[ERROR] Failed opening IB device (error: %s)\n", strerror(errno));
  }

  ibv_free_device_list(ib_dev_list);

  if (ibv_query_port(ib_ctx, ib_port_id, ib_port_attr)) {
    fprintf(stderr, "[ERROR] Failed to query IB port (error: %s)\n", strerror(errno));
  }

  fprintf(stdout, "[NOTICE] Running with port %u (link layer: %s / active mtu: %d)\n",
                  ib_port_id, layer_enum2str(ib_port_attr->link_layer), mtu_enum2bytes(ib_port_attr->active_mtu));

  if (is_roce == true) {
    if (ibv_query_gid(ib_ctx, ib_port_id, ib_gid_idx, ib_gid)) {
      fprintf(stderr, "[ERROR] Failed to query IB GID (error: %s)\n", strerror(errno));
    }
  } else {
    ib_gid = nullptr;
  }

  return ib_ctx;
}

struct ibv_pd* rdma_allocate_pd(struct ibv_context* ib_ctx) {
  struct ibv_pd* ib_pd = ibv_alloc_pd(ib_ctx);

  if (!ib_pd) {
    fprintf(stderr, "[ERROR] Failed allocating protection domain (error: %s)\n", strerror(errno));
  }

  return ib_pd;
}

struct ibv_cq* rdma_create_server_cq(struct ibv_context* ib_ctx) {
  ibv_cq* ib_cq = ibv_create_cq(ib_ctx, 4096, NULL, NULL, 0);

  if (!ib_cq) {
    fprintf(stderr, "[ERROR] Failed creating completion queue (error: %s)\n", strerror(errno));
  }

  return ib_cq;
}

struct ibv_cq* rdma_create_client_cq(struct ibv_context* ib_ctx) {
  ibv_cq* ib_cq = ibv_create_cq(ib_ctx, 4096, NULL, NULL, 0);

  if (!ib_cq) {
    fprintf(stderr, "[ERROR] Failed creating completion queue (error: %s)\n", strerror(errno));
  }

  return ib_cq;
}

struct ibv_mr* rdma_register_mr(struct ibv_pd* ib_pd, uint64_t baseAddr, uint64_t memory_size) {
  struct ibv_mr* ib_mr = ibv_reg_mr(ib_pd, (void *)baseAddr, memory_size,
                                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                                    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);

  if (!ib_mr) {
    fprintf(stderr, "[ERROR] Memory registration failed (error: %s)\n", strerror(errno));
  }

  return ib_mr;
}

struct ibv_mr* rdma_register_on_chip_mr(struct ibv_context* ib_ctx, struct ibv_pd* ib_pd, uint64_t baseAddr, uint64_t memory_size) {
  struct ibv_exp_device_attr attrs;
  memset(&attrs, 0, sizeof(attrs));

  attrs.comp_mask = IBV_EXP_DEVICE_ATTR_UMR;
  attrs.comp_mask |= IBV_EXP_DEVICE_ATTR_MAX_DM_SIZE;

  if (ibv_exp_query_device(ib_ctx, &attrs)) {
    fprintf(stderr, "[ERROR] Failed to query device attributes (error: %s)\n", strerror(errno));
  }

  if (!(attrs.comp_mask & IBV_EXP_DEVICE_ATTR_MAX_DM_SIZE)) {
    fprintf(stderr, "[ERROR] Does not support on-chip memory (error: %s)\n", strerror(errno));
  } else {
    uint64_t max_on_chip_size = attrs.max_dm_size;
    fprintf(stdout, "[NOTICE] Current RNIC has %lu KB on-chip memory\n", max_on_chip_size/1024);
  }

  /* Device memory allocation request. */
  struct ibv_exp_alloc_dm_attr ib_dm_attr;
  memset(&ib_dm_attr, 0, sizeof(ib_dm_attr));

  ib_dm_attr.length = memory_size;

  struct ibv_exp_dm* ib_dm = ibv_exp_alloc_dm(ib_ctx, &ib_dm_attr);
  if (!ib_dm) {
    fprintf(stderr, "[ERROR] Failed allocating on-chip memory (error: %s)\n", strerror(errno));
  }

  /* Device memory registration as memory region. */
  struct ibv_exp_reg_mr_in ib_mr_in;
  memset(&ib_mr_in, 0, sizeof(ib_mr_in));

  ib_mr_in.pd = ib_pd;
  ib_mr_in.addr = (void*)baseAddr;
  ib_mr_in.length = memory_size;
  ib_mr_in.exp_access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
  ib_mr_in.create_flags = 0;
  ib_mr_in.dm = ib_dm;
  ib_mr_in.comp_mask = IBV_EXP_REG_MR_DM;

  struct ibv_mr* ib_mr = ibv_exp_reg_mr(&ib_mr_in);
  if (!ib_mr) {
    fprintf(stderr, "[ERROR] On-chip memory registration failed (error: %s)\n", strerror(errno));
  }

  char* buffer = (char*)malloc(memory_size);
  memset(buffer, 0, memory_size);

  struct ibv_exp_memcpy_dm_attr cpy_attr;
  memset(&cpy_attr, 0, sizeof(cpy_attr));

  cpy_attr.memcpy_dir = IBV_EXP_DM_CPY_TO_DEVICE;
  cpy_attr.host_addr = (void*)buffer;
  cpy_attr.length = memory_size;
  cpy_attr.dm_offset = 0;

  ibv_exp_memcpy_dm(ib_dm, &cpy_attr);

  free(buffer);

  return ib_mr;
}

struct ibv_qp* rdma_create_qp(ibv_qp_type type, struct ibv_context* ib_ctx, struct ibv_pd* ib_pd, struct ibv_cq* send_cq, struct ibv_cq* recv_cq) {
  struct ibv_exp_qp_init_attr attr;
  memset(&attr, 0, sizeof(attr));

  attr.qp_type = type;
  attr.sq_sig_all = 0;
  attr.send_cq = send_cq;
  attr.recv_cq = recv_cq;
  attr.pd = ib_pd;
  attr.comp_mask = IBV_EXP_QP_INIT_ATTR_CREATE_FLAGS | IBV_EXP_QP_INIT_ATTR_PD | IBV_EXP_QP_INIT_ATTR_ATOMICS_ARG;
  attr.max_atomic_arg = 32;

  attr.cap.max_send_wr = 4096;
  attr.cap.max_recv_wr = 4096;
  attr.cap.max_send_sge = 1;
  attr.cap.max_recv_sge = 1;
  attr.cap.max_inline_data = MAX_INLINE_RDMA;

  struct ibv_qp* ib_qp = ibv_exp_create_qp(ib_ctx, &attr);

  if (!ib_qp) {
    fprintf(stderr, "[ERROR] Failed creating queue pair (error: %s)\n", strerror(errno));
  }

  return ib_qp;
}

void rdma_destroy_qp(struct ibv_qp* ib_qp) {
  ibv_destroy_qp(ib_qp);
}

void rdma_modify_qp_to_init(struct ibv_qp* ib_qp, uint32_t ib_port_id) {
  struct ibv_qp_attr attr;
  memset(&attr, 0, sizeof(attr));

  attr.qp_state = IBV_QPS_INIT;
  attr.port_num = ib_port_id;
  attr.pkey_index = 0;
  attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | 
                         IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

  if (ibv_modify_qp(ib_qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
    fprintf(stderr, "[ERROR] Failed to modify QP state to INIT (error: %s)\n", strerror(errno));
  }
}

void rdma_modify_qp_to_rtr(struct ibv_qp* ib_qp, uint32_t ib_port_id, uint32_t rc_qp_num, uint16_t rc_lid, uint8_t* gid, uint8_t ib_gid_idx, bool is_roce) {
  struct ibv_qp_attr attr;
  memset(&attr, 0, sizeof(attr));

  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = IBV_MTU_4096;
  attr.dest_qp_num = rc_qp_num;
  attr.rq_psn = 3185; 
  attr.max_dest_rd_atomic = 16;
  attr.min_rnr_timer = 12;

  attr.ah_attr.is_global = 0;
  attr.ah_attr.dlid = rc_lid;
  attr.ah_attr.sl = 0;
  attr.ah_attr.src_path_bits = 0;
  attr.ah_attr.port_num = ib_port_id;

  if (is_roce == true) {
    attr.ah_attr.is_global = 1;
    memcpy(&attr.ah_attr.grh.dgid, gid, 16);
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.hop_limit  = 1;
    attr.ah_attr.grh.sgid_index = ib_gid_idx;
    attr.ah_attr.grh.traffic_class = 0;
  }

  int attr_mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                  IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

  if (ibv_modify_qp(ib_qp, &attr, attr_mask)) {
    fprintf(stderr, "[ERROR] Failed to modify QP state to RTR (error: %s)\n", strerror(errno));
  }
}

void rdma_modify_qp_to_rts(struct ibv_qp* ib_qp) {
  struct ibv_qp_attr attr;
  memset(&attr, 0, sizeof(attr));

  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 14;
  attr.retry_cnt = 7;
  attr.rnr_retry = 7;
  attr.sq_psn = 3185;
  attr.max_rd_atomic = 16;

  int attr_mask = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                  IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

  if (ibv_modify_qp(ib_qp, &attr, attr_mask)) {
    fprintf(stderr, "[ERROR] Failed to modify QP state to RTS (error: %s)\n", strerror(errno));
  }
}

void rdma_modify_ud_to_rts(struct ibv_qp* ib_qp, uint32_t ib_port_id) {
  assert(ib_qp->qp_type == IBV_QPT_UD);

  // RESET -> INIT
  struct ibv_qp_attr attr;
  memset(&attr, 0, sizeof(attr));

  attr.qp_state = IBV_QPS_INIT;
  attr.port_num = ib_port_id;
  attr.pkey_index = 0;
  attr.qkey = 0x11111111;

  if (ibv_modify_qp(ib_qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY)) {
    fprintf(stderr, "[ERROR] Failed to modify UD state to INIT (error: %s)\n", strerror(errno));
  }

  // INIT -> RTR
  memset(&attr, 0, sizeof(attr));

  attr.qp_state = IBV_QPS_RTR;

  if (ibv_modify_qp(ib_qp, &attr, IBV_QP_STATE)) {
    fprintf(stderr, "[ERROR] Failed to modify UD state to RTR (error: %s)\n", strerror(errno));
  }

  // RTR -> RTS
  memset(&attr, 0, sizeof(attr));

  attr.qp_state = IBV_QPS_RTS;
  attr.sq_psn = 3185;
  
  if (ibv_modify_qp(ib_qp, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN)) {
    fprintf(stderr, "[ERROR] Failed to modify UD state to RTS (error: %s)\n", strerror(errno));
  }
}

bool rdma_send_write(struct ibv_mr* ib_mr, struct ibv_qp* ib_qp, uint32_t rkey, uint64_t src, uint64_t dest, uint64_t size, uint64_t wr_id, bool signal) {
  struct ibv_sge sge;
  struct ibv_send_wr send_wr;
  struct ibv_send_wr* bad_wr;
  memset(&sge, 0, sizeof(sge));
  memset(&send_wr, 0, sizeof(send_wr));

  sge.addr   = src;
  sge.length = size;
  sge.lkey   = ib_mr->lkey;

  send_wr.wr_id      = wr_id;
  send_wr.next       = NULL;
  send_wr.sg_list    = &sge;
  send_wr.num_sge    = 1;
  send_wr.opcode     = IBV_WR_RDMA_WRITE;
  send_wr.send_flags = signal ? IBV_SEND_SIGNALED : 0;
  if (size <= MAX_INLINE_RDMA) {
    send_wr.send_flags |= IBV_SEND_INLINE;
  }

  send_wr.wr.rdma.remote_addr = dest;
  send_wr.wr.rdma.rkey = rkey;

  if (ibv_post_send(ib_qp, &send_wr, &bad_wr)) {
    fprintf(stderr, "[ERROR] Failed RDMA write (error: %s)\n", strerror(errno));
    return false;
  }

  return true;
}

bool rdma_send_read(struct ibv_mr* ib_mr, struct ibv_qp* ib_qp, uint32_t rkey, uint64_t src, uint64_t dest, uint64_t size, uint64_t wr_id) {
  struct ibv_sge sge;
  struct ibv_send_wr send_wr;
  struct ibv_send_wr* bad_wr;
  memset(&sge, 0, sizeof(sge));
  memset(&send_wr, 0, sizeof(send_wr));

  sge.addr   = src;
  sge.length = size;
  sge.lkey   = ib_mr->lkey;

  send_wr.wr_id      = wr_id;
  send_wr.next       = NULL;
  send_wr.sg_list    = &sge;
  send_wr.num_sge    = 1;
  send_wr.opcode     = IBV_WR_RDMA_READ;
  send_wr.send_flags = IBV_SEND_SIGNALED;

  send_wr.wr.rdma.remote_addr = dest;
  send_wr.wr.rdma.rkey = rkey;

  if (ibv_post_send(ib_qp, &send_wr, &bad_wr)) {
    fprintf(stderr, "[ERROR] Failed RDMA read (error: %s)\n", strerror(errno));
    return false;
  }

  return true;
}

bool rdma_send_batched_write(struct ibv_mr* ib_mr, struct ibv_qp* ib_qp, int batch_num, RDMAInfo** batch_list, uint64_t wr_id) {
  struct ibv_sge sge[batch_num];
  struct ibv_send_wr send_wr[batch_num];
  struct ibv_send_wr* bad_wr;
  memset(sge, 0, sizeof(sge));
  memset(send_wr, 0, sizeof(send_wr));

  for (int i = 0; i < batch_num; ++i) {
    sge[i].addr   = batch_list[i]->src;
    sge[i].length = batch_list[i]->size;
    sge[i].lkey   = ib_mr->lkey;

    send_wr[i].wr_id      = wr_id;
    send_wr[i].next       = (i == batch_num-1) ? NULL : &send_wr[i+1];
    send_wr[i].sg_list    = &sge[i];
    send_wr[i].num_sge    = 1;
    send_wr[i].opcode     = IBV_WR_RDMA_WRITE;
    send_wr[i].send_flags = (i == batch_num-1) ? IBV_SEND_SIGNALED : 0;
    if (batch_list[i]->size <= MAX_INLINE_RDMA) {
      send_wr[i].send_flags |= IBV_SEND_INLINE;
    }

    send_wr[i].wr.rdma.remote_addr = batch_list[i]->dest;
    send_wr[i].wr.rdma.rkey = batch_list[i]->rkey;
  }

  if (ibv_post_send(ib_qp, &send_wr[0], &bad_wr)) {
    fprintf(stderr, "[ERROR] Failed RDMA batched write (error: %s)\n", strerror(errno));
    return false;
  }

  return true;
}

bool rdma_send_batched_read(struct ibv_mr* ib_mr, struct ibv_qp* ib_qp, int batch_num, RDMAInfo** batch_list, uint64_t wr_id) {
  struct ibv_sge sge[batch_num];
  struct ibv_send_wr send_wr[batch_num];
  struct ibv_send_wr* bad_wr;
  memset(sge, 0, sizeof(sge));
  memset(send_wr, 0, sizeof(send_wr));

  for (int i = 0; i < batch_num; ++i) {
    sge[i].addr   = batch_list[i]->src;
    sge[i].length = batch_list[i]->size;
    sge[i].lkey   = ib_mr->lkey;

    send_wr[i].wr_id      = wr_id;
    send_wr[i].next       = (i == batch_num-1) ? NULL : &send_wr[i+1];
    send_wr[i].sg_list    = &sge[i];
    send_wr[i].num_sge    = 1;
    send_wr[i].opcode     = IBV_WR_RDMA_READ;
    send_wr[i].send_flags = (i == batch_num-1) ? IBV_SEND_SIGNALED : 0;

    send_wr[i].wr.rdma.remote_addr = batch_list[i]->dest;
    send_wr[i].wr.rdma.rkey = batch_list[i]->rkey;
  }

  if (ibv_post_send(ib_qp, &send_wr[0], &bad_wr)) {
    fprintf(stderr, "[ERROR] Failed RDMA batched read (error: %s)\n", strerror(errno));
    return false;
  }

  return true;
}

bool rdma_send_compare_and_swap(struct ibv_mr* ib_mr, struct ibv_qp* ib_qp, uint32_t rkey, uint64_t src, uint64_t dest, uint64_t compare, uint64_t swap, uint64_t wr_id) {
  struct ibv_sge sge;
  struct ibv_send_wr send_wr;
  struct ibv_send_wr* bad_wr;
  memset(&sge, 0, sizeof(sge));
  memset(&send_wr, 0, sizeof(send_wr));

  sge.addr   = src;
  sge.length = 8;
  sge.lkey   = ib_mr->lkey;

  send_wr.wr_id      = wr_id;
  send_wr.next       = NULL;
  send_wr.sg_list    = &sge;
  send_wr.num_sge    = 1;
  send_wr.opcode     = IBV_WR_ATOMIC_CMP_AND_SWP;
  send_wr.send_flags = IBV_SEND_SIGNALED;

  send_wr.wr.atomic.remote_addr = dest;
  send_wr.wr.atomic.rkey = rkey;
  send_wr.wr.atomic.compare_add = compare;
  send_wr.wr.atomic.swap = swap;

  if (ibv_post_send(ib_qp, &send_wr, &bad_wr)) {
    fprintf(stderr, "[ERROR] Failed RDMA CAS (error: %s)\n", strerror(errno));
    return false;
  }

  return true;
}

bool rdma_send_masked_compare_and_swap(struct ibv_mr* ib_mr, struct ibv_qp* ib_qp, uint32_t rkey, uint64_t src, uint64_t dest, uint64_t compare, uint64_t swap, uint64_t mask, uint64_t wr_id) {
  struct ibv_sge sge;
  struct ibv_exp_send_wr send_wr;
  struct ibv_exp_send_wr* bad_wr;
  memset(&sge, 0, sizeof(sge));
  memset(&send_wr, 0, sizeof(send_wr));

  sge.addr   = src;
  sge.length = 8;
  sge.lkey   = ib_mr->lkey;

  send_wr.wr_id      = wr_id;
  send_wr.next       = NULL;
  send_wr.sg_list    = &sge;
  send_wr.num_sge    = 1;
  send_wr.exp_opcode = IBV_EXP_WR_EXT_MASKED_ATOMIC_CMP_AND_SWP;
  send_wr.exp_send_flags = IBV_EXP_SEND_EXT_ATOMIC_INLINE | IBV_EXP_SEND_SIGNALED;
  
  send_wr.ext_op.masked_atomics.log_arg_sz = 3; 
  send_wr.ext_op.masked_atomics.remote_addr = dest;
  send_wr.ext_op.masked_atomics.rkey = rkey;

  send_wr.ext_op.masked_atomics.wr_data.inline_data.op.cmp_swap.compare_val = compare;
  send_wr.ext_op.masked_atomics.wr_data.inline_data.op.cmp_swap.swap_val = swap;

  send_wr.ext_op.masked_atomics.wr_data.inline_data.op.cmp_swap.compare_mask = mask;
  send_wr.ext_op.masked_atomics.wr_data.inline_data.op.cmp_swap.swap_mask = mask;

  if (ibv_exp_post_send(ib_qp, &send_wr, &bad_wr)) {
    fprintf(stderr, "[ERROR] Failed RDMA masked CAS (error: %s)\n", strerror(errno));
    return false;
  }

  return true;
}

bool rdma_send_fetch_and_add(struct ibv_mr* ib_mr, struct ibv_qp* ib_qp, uint32_t rkey, uint64_t src, uint64_t dest, uint64_t add, uint64_t wr_id) {
  struct ibv_sge sge;
  struct ibv_send_wr send_wr;
  struct ibv_send_wr* bad_wr;
  memset(&sge, 0, sizeof(sge));
  memset(&send_wr, 0, sizeof(send_wr));

  sge.addr   = src;
  sge.length = 8;
  sge.lkey   = ib_mr->lkey;

  send_wr.wr_id      = wr_id;
  send_wr.next       = NULL;
  send_wr.sg_list    = &sge;
  send_wr.num_sge    = 1;
  send_wr.opcode     = IBV_WR_ATOMIC_FETCH_AND_ADD;
  send_wr.send_flags = IBV_SEND_SIGNALED;

  send_wr.wr.atomic.remote_addr = dest;
  send_wr.wr.atomic.rkey = rkey;
  send_wr.wr.atomic.compare_add = add;

  if (ibv_post_send(ib_qp, &send_wr, &bad_wr)) {
    fprintf(stderr, "[ERROR] Failed RDMA FAA (error: %s)\n", strerror(errno));
    return false;
  }

  return true;
}

bool rdma_send_masked_fetch_and_add(struct ibv_mr* ib_mr, struct ibv_qp* ib_qp, uint32_t rkey, uint64_t src, uint64_t dest, uint64_t add, uint64_t mask, uint64_t wr_id) {
  struct ibv_sge sge;
  struct ibv_exp_send_wr send_wr;
  struct ibv_exp_send_wr* bad_wr;
  memset(&sge, 0, sizeof(sge));
  memset(&send_wr, 0, sizeof(send_wr));

  sge.addr   = src;
  sge.length = 8;
  sge.lkey   = ib_mr->lkey;

  send_wr.wr_id          = wr_id;
  send_wr.next           = NULL;
  send_wr.sg_list        = &sge;
  send_wr.num_sge        = 1;
  send_wr.exp_opcode     = IBV_EXP_WR_EXT_MASKED_ATOMIC_FETCH_AND_ADD;
  send_wr.exp_send_flags = IBV_EXP_SEND_EXT_ATOMIC_INLINE | IBV_EXP_SEND_SIGNALED;

  send_wr.ext_op.masked_atomics.log_arg_sz = 3;
  send_wr.ext_op.masked_atomics.remote_addr = dest;
  send_wr.ext_op.masked_atomics.rkey = rkey;

  send_wr.ext_op.masked_atomics.wr_data.inline_data.op.fetch_add.add_val = add;
  send_wr.ext_op.masked_atomics.wr_data.inline_data.op.fetch_add.field_boundary = mask;

  if (ibv_exp_post_send(ib_qp, &send_wr, &bad_wr)) {
    fprintf(stderr, "[ERROR] Failed RDMA masked FAA (error: %s)\n", strerror(errno));
    return false;
  }

  return true;
}

bool rdma_post_send(struct ibv_qp* ib_qp, uint64_t src, uint64_t size, uint32_t lkey, uint64_t wr_id) {
  struct ibv_sge sge;
  struct ibv_send_wr send_wr;
  struct ibv_send_wr* bad_wr;
  memset(&sge, 0, sizeof(sge));
  memset(&send_wr, 0, sizeof(send_wr));
  
  sge.addr = src;
  sge.length = size;
  sge.lkey = lkey;

  send_wr.wr_id = wr_id;
  send_wr.sg_list = &sge;
  send_wr.num_sge = 1;
  send_wr.opcode = IBV_WR_SEND;
  send_wr.send_flags = IBV_SEND_SIGNALED;
  
  if (ibv_post_send(ib_qp, &send_wr, &bad_wr)) {
    fprintf(stderr, "[ERROR] Failed RDMA SEND (error: %s)\n", strerror(errno));
    return false;
  }

  return true;
}

bool rdma_post_recv(struct ibv_qp* ib_qp, uint64_t src, uint64_t size, uint32_t lkey, uint64_t wr_id) {
  struct ibv_sge sge;
  struct ibv_recv_wr recv_wr;
  struct ibv_recv_wr* bad_wr;
  memset(&sge, 0, sizeof(sge));
  memset(&recv_wr, 0, sizeof(recv_wr));

  sge.addr = src;
  sge.length = size;
  sge.lkey = lkey;

  recv_wr.wr_id = wr_id;
  recv_wr.sg_list = &sge;
  recv_wr.num_sge = 1;

  if (ibv_post_recv(ib_qp, &recv_wr, &bad_wr)) {
    fprintf(stderr, "[ERROR] Failed RDMA RECV (error: %s)\n", strerror(errno));
    return false;
  }

  return true;
}

bool rdma_poll_completion(struct ibv_cq* ib_cq) {
  struct ibv_wc ib_wc;

  int recv = 0;
  while (!recv) {
    recv = ibv_poll_cq(ib_cq, 1, &ib_wc);
  }

  if (recv <= 0 || ib_wc.status != ibv_wc_status::IBV_WC_SUCCESS) {
    fprintf(stderr, "[ERROR] Failed polling completion (error: %s)\n", ibv_wc_status_str(ib_wc.status));
    return false;
  }

  return true;
}

bool rdma_poll_once_with_id(struct ibv_cq* ib_cq, uint64_t &wr_id) {
  struct ibv_wc ib_wc;

  int recv = ibv_poll_cq(ib_cq, 1, &ib_wc);

  if (recv < 0) {
    fprintf(stderr, "[ERROR] Failed polling completion (error: %s)\n", ibv_wc_status_str(ib_wc.status));
    return false;
  }

  wr_id = ib_wc.wr_id;

  return recv == 1;
}