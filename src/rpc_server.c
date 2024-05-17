#include <stdio.h>
#include <stdlib.h>
#include "global.h"
#include "rpc.h"
#include "rdma.h"
#include "shmem.h"

// Overwrite global print config.
// #define ENABLE_PRINT 1
#include "log.h"

void server_rpc_msg_handler(void *arg)
{
	struct rpc_msg_handler_param *rpc_pa;
	rpc_pa = (struct rpc_msg_handler_param *)arg;

	// Nothing to do currently.

	// Call user-defined callback.
	rpc_pa->user_msg_handler_cb((void *)rpc_pa->param);

	free(arg);
}

/**
 * @brief Initialize RPC server.
 * 
 * @param ch_type Channel type. For example, RDMA, shared memory, and so on.
 * @param target cm socket name for SHMEM connection. Not used for RDMA connection.
 * @param port 
 * @param max_msgdata_size The maximum size of a msg data in byte.
 * @param msg_handler Message handler callback function.
 * @param worker_thpool A worker thread pool that executes the handler callback function.
 * @return 0 on success.
 */
int init_rpc_server(enum rpc_channel_type ch_type, char *target, int port,
		    int max_msgdata_size, void (*msg_handler)(void *data),
		    threadpool worker_thpool)
{
	struct rdma_ch_attr rdma_attr;
	struct shmem_ch_attr shmem_attr;
	struct rpc_ch_info *rpc_ch;
	int is_server;

	rpc_ch = calloc(1, sizeof *rpc_ch);
	rpc_ch->ch_type = ch_type;
	is_server = 1;

	switch (ch_type) {
	case RPC_CH_RDMA:
		rdma_attr.server = is_server;
		rdma_attr.msgbuf_cnt = RPC_MSG_BUF_NUM;
		rdma_attr.msgdata_size = max_msgdata_size;
		rdma_attr.port = port;
		rdma_attr.rpc_msg_handler_cb = server_rpc_msg_handler;
		rdma_attr.user_msg_handler_cb = msg_handler;
		rdma_attr.msg_handler_thpool = worker_thpool;

		rpc_ch->ch_cb = init_rdma_ch(&rdma_attr);
		break;

	case RPC_CH_SHMEM:
		shmem_attr.server = is_server;
		shmem_attr.msgbuf_cnt = RPC_MSG_BUF_NUM;
		shmem_attr.msgdata_size = max_msgdata_size;
		shmem_attr.rpc_msg_handler_cb = server_rpc_msg_handler;
		shmem_attr.user_msg_handler_cb = msg_handler;
		shmem_attr.msg_handler_thpool = worker_thpool;
		strcpy(shmem_attr.cm_socket_name, target);

		rpc_ch->ch_cb = init_shmem_ch(&shmem_attr);
		break;

	default:
		log_error("Invalid channel type for RPC.");
		return -1;
	}

	return 0;
}

/**
 * @brief Send a response to client.
 * 
 * @param resp_param Parameters required to send a response to the client.
 */
void send_rpc_response_to_client(struct rpc_resp_param *resp_param)
{
	switch (resp_param->rpc_ch->ch_type) {
	case RPC_CH_RDMA:
		send_rdma_msg((struct rdma_ch_cb *)resp_param->rpc_ch->ch_cb,
			      resp_param->client_rpc_ch_addr, resp_param->data,
			      resp_param->sem, resp_param->msgbuf_id,
			      resp_param->seqn);
		break;
	case RPC_CH_SHMEM:
		send_shmem_response(
			(struct shmem_ch_cb *)resp_param->rpc_ch->ch_cb,
			resp_param->client_rpc_ch_addr, resp_param->data,
			resp_param->client_id, resp_param->msgbuf_id,
			resp_param->seqn);
		break;

	default:
		log_error("Invalid channel type for RPC.");
	}
}

void destroy_rpc_server(struct rpc_ch_info *rpc_ch)
{
	// TODO: To be implemented.
	free(rpc_ch);
}

//TODO: Currently, msgdata size is fixed.
int get_max_msgdata_size(struct rpc_ch_info *rpc_ch)
{
	int ret;
	switch (rpc_ch->ch_type) {
	case RPC_CH_RDMA:
		ret = ((struct rdma_ch_cb *)rpc_ch->ch_cb)->msgdata_size;
		break;

	case RPC_CH_SHMEM:
		ret = ((struct shmem_ch_cb *)rpc_ch->ch_cb)->msgdata_size;
		break;

	default:
		log_error("Invalid channel type for RPC.");
		ret = -1;
	}

	if (!ret)
		log_error(
			"Max msg data size is 0. msgdata_size is not set in this ch_cb.");
	return ret;
}
