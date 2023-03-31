#include <stdio.h>
#include "rpc.h"
#include "log.h"
#include "rdma.h"

int init_rpc_server(enum rpc_channel_type ch_type, int port)
{
	int is_server;

	is_server = 1;

	switch (ch_type) {
	case RPC_CH_RDMA:
		init_rdma_ch(port, is_server, NULL, RPC_MSG_BUF_NUM);
		break;

	default:
		log_error("Invalid channel type for RPC.");
		return -1;
	}
}