#ifndef _RPC_H_
#define _RPC_H_

#define RPC_MSG_BUF_NUM 8 // The number of msg buffers per connection.

enum rpc_channel_type {
	RPC_CH_RDMA = 1,
	// SOCKET,
	// SHMEM
};

int init_rpc_server(enum rpc_channel_type ch_type, int port);
int init_rpc_client(enum rpc_channel_type ch_type, char *ip_addr, int port);

#endif