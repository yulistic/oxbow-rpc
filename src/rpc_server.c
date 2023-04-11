#include <stdio.h>
#include <stdlib.h>
#include "rpc.h"
#include "log.h"
#include "rdma.h"

void server_rpc_msg_handler(void *arg)
{
	struct rpc_msg_handler_param *rpc_pa;
	rpc_pa = (struct rpc_msg_handler_param *)arg;

	// Nothing to do currently.

	// Call user-defined callback.
	rpc_pa->msg_handler_cb((void *)rpc_pa->param);

	free(arg);
}

int init_rpc_server(enum rpc_channel_type ch_type, int port,
		    void (*msg_handler)(void *data), threadpool worker_thpool)
{
	struct rdma_ch_attr attr;
	struct rpc_ch_info *rpc_ch;
	int is_server;

	rpc_ch = calloc(1, sizeof *rpc_ch);
	rpc_ch->ch_type = ch_type;
	is_server = 1;

	switch (ch_type) {
	case RPC_CH_RDMA:

		attr.server = is_server;
		attr.msgbuf_cnt = RPC_MSG_BUF_NUM;
		attr.msgbuf_size = RPC_MSG_BUF_SIZE;
		attr.port = port;
		attr.rpc_msg_handler_cb = server_rpc_msg_handler;
		attr.msg_handler_cb = msg_handler;
		attr.msg_handler_thpool = worker_thpool;

		rpc_ch->ch_cb = init_rdma_ch(&attr);
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
 * @param msgbuf_id msg buffer where client's msg arrived.
 */
void send_rpc_response_to_client(struct rpc_ch_info *rpc_ch,
				 void *client_rpc_ch_addr, char *data,
				 sem_t *sem, int msgbuf_id)
{
	switch (rpc_ch->ch_type) {
	case RPC_CH_RDMA:
		send_rdma_msg((struct rdma_ch_cb *)rpc_ch->ch_cb,
			      client_rpc_ch_addr, data, sem, msgbuf_id);
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