#ifndef _RPC_H_
#define _RPC_H_

#include <semaphore.h>
#include <stdint.h>
#include "thpool.h"

#define RPC_MSG_BUF_NUM                                                        \
	1 // The number of msg buffers per connection. Only 1 supported, for now.
#define RPC_MSG_BUF_SIZE 1024 // Max msg buffer size including headers.

enum rpc_channel_type {
	RPC_CH_RDMA = 1,
	// SOCKET,
	// SHMEM
};

struct msg_handler_param {
	void *ch_cb;
	struct rpc_msg *msg;
};

// It stores identical data with struct rdma_msg but in little endian order.
struct __attribute__((packed)) rpc_msg {
	uint64_t seqn;
	sem_t *sem; // Client's semaphore address.
	char data[]; // Flexible array.
};

struct rpc_ch_info {
	enum rpc_channel_type ch_type;
	void *ch_cb; // Channel control block.
};

/**
 * @brief
 * 
 * @param ch_type Channel type. For example, RDMA, shared memory, and so on.
 * @param port 
 * @param msg_handler Message handler callback function.
 * @param worker_thpool A worker thread pool that executes the handler callback function.
 * @return 0 on success.
 */
int init_rpc_server(enum rpc_channel_type ch_type, int port,
		    void (*msg_handler)(void *data), threadpool worker_thpool);
/**
 * @brief 
 * 
 * @param ch_type Channel type. For example, RDMA, shared memory, and so on.
 * @param ip_addr Server IP address.
 * @param port 
 * @param msg_handler Message handler callback function.
 * @param worker_thpool A worker thread pool that executes the handler callback function.
 * @return struct rpc_ch_info* RPC channel information. It is used to send a message to the counterpart.
 */
struct rpc_ch_info *init_rpc_client(enum rpc_channel_type ch_type,
				    char *ip_addr, int port,
				    void (*msg_handler)(void *data),
				    threadpool worker_thpool);

void send_rpc_msg(struct rpc_ch_info *rpc_ch, char *data, sem_t *sem);
void destroy_rpc_client(struct rpc_ch_info *rpc_ch);

#endif