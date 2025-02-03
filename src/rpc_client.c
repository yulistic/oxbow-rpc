#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "global.h"
#include "rpc.h"
#include "rdma.h"
#include "shmem.h"
#include "shmem_cm.h"
#include "bit_array.h"

// Overwrite global print config.
// #define ENABLE_PRINT 1
#include "log.h"

/**
 * @brief Callback function of RPC layer. It frees RPC layer resources.
 * 
 * @param arg 
 */
static void client_rpc_rdma_msg_handler(void *arg)
{
	struct rpc_msg_handler_param *rpc_pa;
	rpc_pa = (struct rpc_msg_handler_param *)arg;

	// Free msg buffer bitmap.
	free_msgbuf_id(rpc_pa->client_rpc_ch, rpc_pa->msgbuf_id);

	// Call user-defined callback.
	rpc_pa->user_msg_handler_cb((void *)rpc_pa->param);

	free(arg);
}

static void client_rpc_shmem_msg_handler(struct rpc_ch_info *client_rpc_ch,
					 int msgbuf_id)
{
	// Free msg buffer bitmap.
	free_msgbuf_id(client_rpc_ch, msgbuf_id);
}

/**
 * @brief 
 * 
 * @param rpc_ch 
 * @param msgbuf_id 
 * @param callback  If true, call user callback function.
 */
void wait_rpc_shmem_response(struct rpc_ch_info *rpc_ch, int msgbuf_id,
			     int callback)
{
	sem_t *sem;
	struct shmem_ch_cb *cb;
	struct shmem_msg *shmem_msg;
	struct rpc_msg *rpc_msg;

	assert(rpc_ch->ch_type == RPC_CH_SHMEM);

	cb = (struct shmem_ch_cb *)rpc_ch->ch_cb;
	sem = &cb->buf_ctxs[msgbuf_id].evt->client_sem;
	shmem_msg = cb->buf_ctxs[msgbuf_id].resp_buf;

	// Wait for server's post.
	log_debug("Waiting for the server's response. Sem-addr=0x%lx", sem);
	rpc_sem_wait(sem);
	log_debug("Resume.");

	// Execute callback functions

	// OPTIMIZE: Can we remove malloc & memcpy overhead?
	// FIXME: currently copy fixed size.
	rpc_msg = calloc(1, cb->msgbuf_size);
	memcpy(&rpc_msg->data[0], &shmem_msg->data[0], cb->msgdata_size);

	rpc_msg->header.client_rpc_ch = rpc_ch;
	rpc_msg->header.seqn = shmem_msg->seq_num;

	// Free msgbuf.
	client_rpc_shmem_msg_handler(rpc_ch, msgbuf_id);

	// User defined callback function.
	if (callback)
		cb->user_msg_handler_cb((void *)rpc_msg);

	free(rpc_msg);
}

/**
 * @brief Initialize RPC client.
 * 
 * @param ch_type Channel type. For example, RDMA, shared memory, and so on.
 * @param target Server ip_addr for RDMA connection, cm_socket_name for SHMEM connection.
 * @param port 
 * @param max_msgdata_size The maximum size of a msg data in byte.
 * @param msg_handler Message handler callback function.
 * @param worker_thpool A worker thread pool that executes the handler callback function.
 * @return struct rpc_ch_info* RPC channel information. It is used to send a message to the counterpart.
 */
struct rpc_ch_info *init_rpc_client(enum rpc_channel_type ch_type, char *target,
				    int port, int max_msgdata_size,
				    void (*msg_handler)(void *data),
				    threadpool worker_thpool,
				    key_t shm_key_seed)
{
	struct rdma_ch_attr rdma_attr;
	struct shmem_ch_attr shmem_attr;
	struct rpc_ch_info *rpc_ch;
	int is_server;

	rpc_ch = calloc(1, sizeof *rpc_ch);
	rpc_ch->ch_type = ch_type;
	rpc_ch->msgbuf_bitmap = bit_array_create(RPC_MSG_BUF_NUM);
	pthread_spin_init(&rpc_ch->msgbuf_bitmap_lock, PTHREAD_PROCESS_PRIVATE);

	// Print for test.
	printf("Message buffer bitmaps: ");
	bit_array_print(rpc_ch->msgbuf_bitmap, stdout);
	fputc('\n', stdout);

	is_server = 0;

	switch (ch_type) {
	case RPC_CH_RDMA:
		rdma_attr.server = is_server;
		rdma_attr.msgbuf_cnt = RPC_MSG_BUF_NUM;
		rdma_attr.msgdata_size = max_msgdata_size;
		strcpy(rdma_attr.ip_addr, target);
		rdma_attr.port = port;
		rdma_attr.rpc_msg_handler_cb = client_rpc_rdma_msg_handler;
		rdma_attr.user_msg_handler_cb = msg_handler;
		rdma_attr.msg_handler_thpool = worker_thpool;

		rpc_ch->ch_cb = init_rdma_ch(&rdma_attr);
		break;

	case RPC_CH_SHMEM:
		shmem_attr.shm_key_seed = shm_key_seed;
		shmem_attr.server = is_server;
		shmem_attr.msgbuf_cnt = RPC_MSG_BUF_NUM;
		shmem_attr.msgdata_size = max_msgdata_size;
		shmem_attr.rpc_msg_handler_cb = NULL;
		shmem_attr.user_msg_handler_cb = msg_handler;
		// Worker thread not required. A requester thread executes
		// callback function.
		shmem_attr.msg_handler_thpool = NULL;
		strcpy(shmem_attr.cm_socket_name, target);

		rpc_ch->ch_cb = init_shmem_ch(&shmem_attr);
		break;

	default:
		log_error("Invalid channel type for RPC.");
		goto err;
	}

	if (!rpc_ch->ch_cb) {
		goto err;
	}

	return rpc_ch;

err:
	free(rpc_ch);
	return NULL;
}

void destroy_rpc_client(struct rpc_ch_info *rpc_ch)
{
	switch (rpc_ch->ch_type) {
	case RPC_CH_RDMA:
		destroy_rdma_client((struct rdma_ch_cb *)rpc_ch->ch_cb);
		break;

	case RPC_CH_SHMEM:
		destroy_shmem_client((struct shmem_ch_cb *)rpc_ch->ch_cb);
		break;

	default:
		log_error("Invalid channel type for RPC.");
	}
	pthread_spin_destroy(&rpc_ch->msgbuf_bitmap_lock);
	bit_array_free(rpc_ch->msgbuf_bitmap);
	free(rpc_ch);
}

/**
 * @brief  Send an RPC message to server.
 * 
 * @param req_param Parameters required to send a message to server.
 * 
 * @return int msgbuf_id is returned. (Required by SHMEM channel).
 */
int send_rpc_msg_to_server(struct rpc_req_param *req_param)
{
	int msgbuf_id;
	struct rpc_ch_info *rpc_ch;

	rpc_ch = req_param->rpc_ch;

	// Alloc a message buffer id.
	msgbuf_id = alloc_msgbuf_id(rpc_ch);

	switch (rpc_ch->ch_type) {
	case RPC_CH_RDMA:
		send_rdma_msg((struct rdma_ch_cb *)rpc_ch->ch_cb, rpc_ch,
			      req_param->data, req_param->sem, msgbuf_id, 0);
		break;

	case RPC_CH_SHMEM:
		send_shmem_msg((struct shmem_ch_cb *)rpc_ch->ch_cb, rpc_ch,
			       req_param->data, msgbuf_id);
		break;

	default:
		log_error("Invalid channel type for RPC.");
	}

	return msgbuf_id;
}
