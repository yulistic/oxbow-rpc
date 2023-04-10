#include <stdio.h>
#include <stdlib.h>
#include "rpc.h"
#include "log.h"
#include "rdma.h"

struct rpc_ch_info *init_rpc_client(enum rpc_channel_type ch_type,
				    char *ip_addr, int port,
				    void (*msg_handler)(void *data),
				    threadpool worker_thpool)
{
	struct rdma_ch_attr attr;
	struct rpc_ch_info *rpc_ch;
	int is_server;

	rpc_ch = calloc(1, sizeof *rpc_ch);
	rpc_ch->ch_type = ch_type;
	is_server = 0;

	switch (ch_type) {
	case RPC_CH_RDMA:
		attr.server = is_server;
		attr.msgbuf_cnt = RPC_MSG_BUF_NUM;
		attr.msgbuf_size = RPC_MSG_BUF_SIZE;
		strcpy(attr.ip_addr, ip_addr);
		attr.port = port;
		attr.msg_handler_cb = msg_handler;
		attr.msg_handler_thpool = worker_thpool;

		rpc_ch->ch_cb = init_rdma_ch(&attr);
		break;

	default:
		log_error("Invalid channel type for RPC.");
		return NULL;
	}

	return rpc_ch;
}

void destroy_rpc_client(struct rpc_ch_info *rpc_ch)
{
	switch (rpc_ch->ch_type) {
	case RPC_CH_RDMA:
		destroy_rdma_client((struct rdma_ch_cb *)rpc_ch->ch_cb);
		break;

	default:
		log_error("Invalid channel type for RPC.");
	}
	free(rpc_ch);
}

/**
 * @brief Send message to server.
 * 
 * @param rpc_ch 
 * @param data 
 * @param sem It is used to wait until server's response (ack) arrives.
 */
void send_rpc_msg(struct rpc_ch_info *rpc_ch, char *data, sem_t *sem)
{
	switch (rpc_ch->ch_type) {
	case RPC_CH_RDMA:
		// Currently, there is only one msgbuf.
		// If we implement multiple msg buffers, RPC client has to choose
		// which msg buffer to use.
		send_rdma_msg((struct rdma_ch_cb *)rpc_ch->ch_cb, data, sem, 0);
		break;

	default:
		log_error("Invalid channel type for RPC.");
	}
}