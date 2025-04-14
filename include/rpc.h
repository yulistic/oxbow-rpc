#ifndef _RPC_H_
#define _RPC_H_

#include <semaphore.h>
#include <stdint.h>
#include <pthread.h>
#include "global.h"
#include "thpool.h"
#include <stdatomic.h>
// It is bound to RDMA_SQ_DEPTH and RDMA_RQ_DEPTH defined in rdma.c.
// Currently, it is 192 which is the same as Data fetcher buffer number in Oxbow.
#define RPC_RDMA_MSG_BUF_NUM 192

// #define RPC_SHMEM_MSG_BUF_NUM 16384
#define RPC_SHMEM_MSG_BUF_NUM 32786
extern atomic_int g_rpc_shmem_msgbuf_id; // For shmem channel.
extern atomic_long g_rpc_shmem_msgbuf_count;

enum rpc_channel_type {
	RPC_CH_RDMA = 1,
	RPC_CH_SHMEM,
	// SOCKET,
	// SHMEM
};

struct rpc_ch_info {
	enum rpc_channel_type ch_type;
	void *ch_cb; // Channel control block.
	void *msgbuf_bitmap; // BIT_ARRAY*
	pthread_spinlock_t msgbuf_bitmap_lock;
};

/*
 *  Request: Client -> Server
 *  Response: Server -> Client
 */
struct rpc_req_param {
	struct rpc_ch_info *rpc_ch; // client's current rpc channel.
	char *data; // msg data to send.
	sem_t *sem; /* address of client's semaphore address. (Required by RDMA channel)
		     * It is used to wait until server's response (ack) arrives.
		     */
};

struct rpc_resp_param {
	struct rpc_ch_info *rpc_ch; // server's current rpc channel.
	void *client_rpc_ch_addr; // address of client's rpc channel (rpc_ch). (Passed when requested.)
	char *data; // msg data to send.
	sem_t *sem; // address of client's semaphore address. (Required by RDMA channel)
	int client_id; // client id (= cb id). To identify which client sent a message in SHMEM channel.
	int msgbuf_id; // msg buffer where client's msg arrived.
	uint64_t seqn; // seqn of the client's request.
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
	void (*user_msg_handler_cb)(
		void *param); // user's msg handler callback func.
};

// Parameter for user's msg handler callback.
struct msg_handler_param {
	int client_id; // Used by SHMEM channel.
	int msgbuf_id;
	void *ch_cb;
	struct rpc_msg *msg;
};

struct __attribute__((packed)) rpc_msg_header {
	uint64_t seqn;
	struct rpc_ch_info *
		client_rpc_ch; // Client's address should be delivered through server's response.
	sem_t *sem; // Client's semaphore address (Used by rdma channel).
};

// It stores identical data with struct rdma_msg but in little endian order.
struct __attribute__((packed)) rpc_msg {
	struct rpc_msg_header header;
	char data[]; // Flexible array.
};

int init_rpc_server(enum rpc_channel_type ch_type, char *target, int port,
		    int max_msgdata_size, void (*msg_handler)(void *data),
		    threadpool worker_thpool, void (*on_connect)(void *arg),
		    void *conn_arg, void (*on_disconnect)(void *arg),
		    void *disconn_arg, key_t shm_key_seed);

struct rpc_ch_info *init_rpc_client(enum rpc_channel_type ch_type, char *target,
				    int port, int max_msgdata_size,
				    void (*msg_handler)(void *data),
				    threadpool worker_thpool,
				    key_t shm_key_seed);

int send_rpc_msg_to_server(struct rpc_req_param *req_param);
void send_rpc_response_to_client(struct rpc_resp_param *resp_param);
void destroy_rpc_client(struct rpc_ch_info *rpc_ch);

uint64_t alloc_msgbuf_id(struct rpc_ch_info *rpc_ch);
void free_msgbuf_id(struct rpc_ch_info *rpc_ch, uint64_t bit_id);
void wait_rpc_shmem_response(struct rpc_ch_info *rpc_ch, int msgbuf_id,
			     int callback);
int trywait_rpc_shmem_response(struct rpc_ch_info *rpc_ch, int msgbuf_id,
			       int callback);
int get_max_msgdata_size(struct rpc_ch_info *rpc_ch);

// Busy wait for a given time before sem_wait (sleep).
static inline void busywait_sem_wait(sem_t *sem, long wait_time_usec)
{
	struct timespec start, end;

	if (wait_time_usec == 0)
		goto sleep_and_wait;

	clock_gettime(CLOCK_MONOTONIC, &start);

	while (1) {
		if (sem_trywait(sem) == 0)
			return;

		clock_gettime(CLOCK_MONOTONIC, &end);

		long elapsed_time = (end.tv_sec - start.tv_sec) * 1000000L +
				    (end.tv_nsec - start.tv_nsec) / 1000L;
		if (elapsed_time >= wait_time_usec)
			break;
	}

sleep_and_wait:
	sem_wait(sem);
}

#if (SEMA_MODE == 0) // Always sleep.

static inline void rpc_sem_wait(sem_t *sem)
{
	sem_wait(sem);
}

#elif (SEMA_MODE == 1) // Hybrid polling.

#define BUSYWAIT_TIME_MICROSEC 1000 // 1 millisecond.

static inline void rpc_sem_wait(sem_t *sem)
{
	busywait_sem_wait(sem, BUSYWAIT_TIME_MICROSEC);
}

#elif (SEMA_MODE == 2) // Always busywait.

static inline void rpc_sem_wait(sem_t *sem)
{
	busywait_sem_wait(sem, 100000000); // 100 seconds.
}

#else
#error "Invalid SEMA_MODE value. Must be 0, 1, or 2."
#endif

/**
 * @brief Non-blocking. Only test and return immediately.
 * 
 * @param sem 
 * @return int
 */
static inline int rpc_sem_trywait(sem_t *sem)
{
	return sem_trywait(sem);
}

#endif
