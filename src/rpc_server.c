#include <stdio.h>
#include "rpc.h"
#include "log.h"
#include "rdma.h"

int init_rpc_server(enum rpc_channel_type ch_type, int port,
		    void (*msg_handler)(void *data), threadpool worker_thpool)
{
	struct rpc_ch_info *rpc_ch;
	int is_server;

	rpc_ch = calloc(1, sizeof *rpc_ch);
	rpc_ch->ch_type = ch_type;
	is_server = 1;

	switch (ch_type) {
	case RPC_CH_RDMA:
		struct rdma_ch_attr attr;

		attr.server = is_server;
		attr.msgbuf_cnt = RPC_MSG_BUF_NUM;
		attr.msgbuf_size = RPC_MSG_BUF_SIZE;
		attr.port = port;
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

void destroy_rpc_server(struct rpc_ch_info *rpc_ch)
{
	// TODO: To be implemented.
	free(rpc_ch);
}