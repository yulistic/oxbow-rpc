#ifndef _RPC_H_
#define _RPC_H_

#include <semaphore.h>
#include <stdint.h>
#include <pthread.h>
#include "log.h"
#include "thpool.h"
#include "bit_array.h"

#define RPC_MSG_BUF_NUM                                                        \
	1 // The number of msg buffers per connection. Only 1 supported, for now.
#define RPC_MSG_BUF_SIZE 1024 // Max msg buffer size including headers.

enum rpc_channel_type {
	RPC_CH_RDMA = 1,
	// SOCKET,
	// SHMEM
};

struct rpc_ch_info {
	enum rpc_channel_type ch_type;
	void *ch_cb; // Channel control block.
	BIT_ARRAY *msgbuf_bitmap;
	pthread_spinlock_t msgbuf_bitmap_lock;
};

// Call graph of callback functions:
// CQ event ->
// rpc_msg_handler_cb() (rpc layer) ->
// msg_handler() (user defined) ->

// Parameter for rpc layer's msg handler callback.
struct rpc_msg_handler_param {
	int msgbuf_id;
	struct rpc_ch_info *client_rpc_ch;
	struct msg_handler_param *param; // Passed to user's msg handler callback.
	void (*msg_handler_cb)(void *param); // user's msg handler callback func.
};

// Parameter for user's msg handler callback.
struct msg_handler_param {
	int msgbuf_id;
	void *ch_cb;
	struct rpc_msg *msg;
};

struct __attribute__((packed)) rpc_msg_header {
	uint64_t seqn;
	struct rpc_ch_info *
		client_rpc_ch; // Client's address should be delivered through server's response.
	sem_t *sem; // Client's semaphore address.
};

// It stores identical data with struct rdma_msg but in little endian order.
struct __attribute__((packed)) rpc_msg {
	struct rpc_msg_header header;
	char data[]; // Flexible array.
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

void send_rpc_msg_to_server(struct rpc_ch_info *rpc_ch, char *data, sem_t *sem);
void send_rpc_response_to_client(struct rpc_ch_info *rpc_ch,
				 void *client_rpc_ch_addr, char *data,
				 sem_t *sem, int msgbuf_id);
void destroy_rpc_client(struct rpc_ch_info *rpc_ch);

uint64_t alloc_msgbuf_id(struct rpc_ch_info *rpc_ch);
void free_msgbuf_id(struct rpc_ch_info *rpc_ch, uint64_t bit_id);
#endif