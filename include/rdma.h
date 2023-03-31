#ifndef _RDMA_H_
#define _RDMA_H_
#include <rdma/rdma_cma.h>
#include <sys/types.h>
#include <semaphore.h>

/*
 * These states are used to signal events between the completion handler
 * and the main client or server thread.
 *
 * Once CONNECTED, they cycle through RDMA_READ_ADV, RDMA_WRITE_ADV, 
 * and RDMA_WRITE_COMPLETE for each ping.
 */
enum rdma_ch_state {
	IDLE = 1,
	CONNECT_REQUEST,
	ADDR_RESOLVED,
	ROUTE_RESOLVED,
	CONNECTED,
	RDMA_READ_ADV,
	RDMA_READ_COMPLETE,
	RDMA_WRITE_ADV,
	RDMA_WRITE_COMPLETE,
	DISCONNECTED,
	ERROR
};

struct comm_rdma_mr_info {
	char char_buf[256];
	__be64 addr;
	__be64 length;
	__be64 buf; // TODO: to be deleted. // Buffer address.
	__be32 rkey;
	__be32 size;
};

/** RDMA channel control block */
struct rdma_ch_cb {
	int server; /* 0 iff client */
	pthread_t cqthread;
	pthread_t server_thread;
	struct ibv_comp_channel *channel;
	struct ibv_cq *cq;
	struct ibv_pd *pd;
	struct ibv_qp *qp;

	struct ibv_recv_wr rq_wr; /* recv work request record */
	struct ibv_sge recv_sgl; /* recv single SGE */
	struct comm_rdma_mr_info recv_buf; /* malloc'd buffer */
	struct ibv_mr *recv_mr; /* MR associated with this buffer */

	struct ibv_send_wr sq_wr; /* send work request record */
	struct ibv_sge send_sgl;
	struct comm_rdma_mr_info send_buf; /* single send buf */
	struct ibv_mr *send_mr;

	struct ibv_send_wr rdma_sq_wr; /* rdma work request record */
	struct ibv_sge rdma_sgl; /* rdma single SGE */
	char *rdma_buf; /* used as rdma sink */
	struct ibv_mr *rdma_mr;

	uint32_t remote_rkey; /* remote guys RKEY */
	uint64_t remote_addr; /* remote guys TO */
	uint32_t remote_len; /* remote guys LEN */

	// TODO: To be deleted. Used by client.
	char *start_buf; /* rdma read src */
	struct ibv_mr *start_mr;

	enum rdma_ch_state state; /* used for cond/signalling */
	sem_t sem;

	struct sockaddr_storage sin;
	struct sockaddr_storage ssource;
	__be16 port; /* dst port in NBO */
	int verbose; /* verbose logging */
	int self_create_qp; /* Create QP not via cma */
	int count; /* ping count */
	int size; /* ping data size */
	int validate; /* validate ping data */

	/* CM stuff */
	pthread_t cmthread;
	struct rdma_event_channel *cm_channel;
	struct rdma_cm_id *cm_id; /* connection on client side,*/
	/* listener on service side. */
	struct rdma_cm_id *child_cm_id; /* connection on server side */
};

int init_rdma_ch(int port, int server, char *ip_addr, int msgbuf_cnt);

#endif