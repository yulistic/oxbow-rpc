#include <stdio.h>
#include <stdlib.h>
#include "rpc.h"
#include "log.h"
#include "rdma.h"
#include "shmem.h"

void server_rpc_msg_handler(void *arg)
{
	struct rpc_msg_handler_param *rpc_pa;
	rpc_pa = (struct rpc_msg_handler_param *)arg;

	// Nothing to do currently.

	// Call user-defined callback.
	rpc_pa->user_msg_handler_cb((void *)rpc_pa->param);

	free(arg);
}

int init_rpc_server(enum rpc_channel_type ch_type, char *target, int port,
		    void (*msg_handler)(void *data), threadpool worker_thpool)
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
		rdma_attr.msgbuf_size = RPC_MSG_BUF_SIZE;
		rdma_attr.port = port;
		rdma_attr.rpc_msg_handler_cb = server_rpc_msg_handler;
		rdma_attr.user_msg_handler_cb = msg_handler;
		rdma_attr.msg_handler_thpool = worker_thpool;

		rpc_ch->ch_cb = init_rdma_ch(&rdma_attr);
		break;

	case RPC_CH_SHMEM:
		shmem_attr.server = is_server;
		shmem_attr.msgbuf_cnt = RPC_MSG_BUF_NUM;
		shmem_attr.msgbuf_size = RPC_MSG_BUF_SIZE;
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
 * @param rpc_ch 
 * @param client_rpc_ch_addr  Client's rpc_ch address.
 * @param data 
 * @param sem Client's sem address.
 * @param client_id client id (= cb id). To identify which client sent a message
 * in SHMEM channel.
 * @param msgbuf_id msg buffer where client's msg arrived.
 * @param seqn seqn of the client's request.
 */
void send_rpc_response_to_client(struct rpc_ch_info *rpc_ch,
				 void *client_rpc_ch_addr, char *data,
				 sem_t *sem, int client_id, int msgbuf_id,
				 uint64_t seqn)
{
	switch (rpc_ch->ch_type) {
	case RPC_CH_RDMA:
		send_rdma_msg((struct rdma_ch_cb *)rpc_ch->ch_cb,
			      client_rpc_ch_addr, data, sem, msgbuf_id, seqn);
		break;
	case RPC_CH_SHMEM:
		send_shmem_response((struct shmem_ch_cb *)rpc_ch->ch_cb,
				    client_rpc_ch_addr, data, sem, client_id,
				    msgbuf_id, seqn);
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