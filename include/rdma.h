#ifndef _RDMA_H
#define _RDMA_H

#include <assert.h>
#include <stdint.h>
#include <infiniband/verbs.h>

#define MAX_INLINE_RDMA 220

struct RDMAInfo {
  uint32_t rkey;
  uint64_t src;
  uint64_t dest;
  uint64_t size;

  RDMAInfo(void) { }

  inline void operator=(const RDMAInfo& obj) {
    rkey = obj.rkey;
    src = obj.src;
    dest = obj.dest;
    size = obj.size;
  } 
};

struct ibv_context* rdma_create_context(uint32_t ib_dev_id, uint32_t ib_port_id, struct ibv_port_attr* ib_port_attr, union ibv_gid* ib_gid, uint8_t ib_gid_idx, bool is_roce);
struct ibv_pd* rdma_allocate_pd(struct ibv_context* ib_ctx);
struct ibv_cq* rdma_create_server_cq(struct ibv_context* ib_ctx);
struct ibv_cq* rdma_create_client_cq(struct ibv_context* ib_ctx);
struct ibv_mr* rdma_register_mr(struct ibv_pd* ib_pd, uint64_t baseAddr, uint64_t memory_size);
struct ibv_mr* rdma_register_on_chip_mr(struct ibv_context* ib_ctx, struct ibv_pd* ib_pd, uint64_t baseAddr, uint64_t memory_size);
struct ibv_qp* rdma_create_qp(ibv_qp_type type, struct ibv_context* ib_ctx, struct ibv_pd* ib_pd, struct ibv_cq* send_cq, struct ibv_cq* recv_cq);

void rdma_destroy_qp(struct ibv_qp* ib_qp);

void rdma_modify_qp_to_init(struct ibv_qp* ib_qp, uint32_t ib_port_id);
void rdma_modify_qp_to_rtr(struct ibv_qp* ib_qp, uint32_t ib_port_id, uint32_t rc_qp_num, uint16_t rc_lid, uint8_t* gid, uint8_t ib_gid_idx, bool is_roce);
void rdma_modify_qp_to_rts(struct ibv_qp* ib_qp);
void rdma_modify_ud_to_rts(struct ibv_qp* ib_qp, uint32_t ib_port_id);

bool rdma_send_write(struct ibv_mr*ib_mr, struct ibv_qp* ib_qp, uint32_t rkey, uint64_t src, uint64_t dest, uint64_t size, uint64_t wr_id, bool signal);
bool rdma_send_read(struct ibv_mr* ib_mr, struct ibv_qp* ib_qp, uint32_t rkey, uint64_t src, uint64_t dest, uint64_t size, uint64_t wr_id);
bool rdma_send_batched_write(struct ibv_mr* ib_mr, struct ibv_qp* ib_qp, int batch_num, RDMAInfo** batch_list, uint64_t wr_id);
bool rdma_send_batched_read(struct ibv_mr* ib_mr, struct ibv_qp* ib_qp, int batch_num, RDMAInfo** batch_list, uint64_t wr_id);
bool rdma_send_compare_and_swap(struct ibv_mr* ib_mr, struct ibv_qp* ib_qp, uint32_t rkey, uint64_t src, uint64_t dest, uint64_t compare, uint64_t swap, uint64_t wr_id);
bool rdma_send_masked_compare_and_swap(struct ibv_mr* ib_mr, struct ibv_qp* ib_qp, uint32_t rkey, uint64_t src, uint64_t dest, uint64_t compare, uint64_t swap, uint64_t mask, uint64_t wr_id);
bool rdma_send_fetch_and_add(struct ibv_mr* ib_mr, struct ibv_qp* ib_qp, uint32_t rkey, uint64_t src, uint64_t dest, uint64_t add, uint64_t wr_id);
bool rdma_send_masked_fetch_and_add(struct ibv_mr* ib_mr, struct ibv_qp* ib_qp, uint32_t rkey, uint64_t src, uint64_t dest, uint64_t add, uint64_t mask, uint64_t wr_id);
bool rdma_post_send(struct ibv_qp* ib_qp, uint64_t src, uint64_t size, uint32_t lkey, uint64_t wr_id);
bool rdma_post_recv(struct ibv_qp* ib_qp, uint64_t src, uint64_t size, uint32_t lkey, uint64_t wr_id);
bool rdma_poll_completion(struct ibv_cq* ib_cq);
bool rdma_poll_once_with_id(struct ibv_cq* ib_cq, uint64_t &wr_id);

#endif // _RDMA_H