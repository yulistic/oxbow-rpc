#ifndef _SHMEM_H_
#define _SHMEM_H_
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <semaphore.h>
#include <stdatomic.h>
#include "thpool.h"
#include "bit_array.h"

#define MAX_CLIENT_CONNECTION 128

// An arbitrary id that does not overlap with clients'.
#define SHMEM_CQ_CB_ID (MAX_CLIENT_CONNECTION + 1)

/**
 * @brief Flag to notify a message arrival event.
 * Let's make server directly wakes up client's thread for better latency.
 */
struct shmem_evt_flag {
	// uint64_t client_evt;
	sem_t client_sem;
	char pad[32]; // Make server_evt aligned in a 64-bit cache line.
	atomic_ulong server_evt;
};

// It is stored in the shmem seg.
struct shmem_msg {
	uint64_t seq_num; // sequence number of this msgbuf.
	struct rpc_ch_info *rpc_ch; // Client's rpc_ch_info address.
	sem_t *sem; // Client's semaphore address. // TODO: we might not need this because server directly post a sema.
	char data[]; // Data. Flexible array.
};

struct shmem_ch_attr {
	int server; /* 0 iff client */
	int msgbuf_cnt; // The number of msg buffers.
	int msgdata_size; // The size of a message data.
	key_t shm_key_seed; // Base key for shared memory segments.
	void (*rpc_msg_handler_cb)(
		void *rpc_param); // rpc layer callback function.
	void (*user_msg_handler_cb)(void *param); // user callback function.
	threadpool msg_handler_thpool; // threadpool to execute msg handler fn.
	char cm_socket_name[108]; // shmem socket name for cm.
	void (*on_connect)(
		void *arg); // callback function when client is connected.
	void *conn_arg; // Argument for on_connect callback.
	void (*on_disconnect)(
		void *arg); // callback function when client is disconnected.
	void *disconn_arg; // Argument for on_disconnect callback.
};

struct shmem_client_state {
	int cb_id; // a.k.a. client_id.
	int client_cm_fd; // CM fd.
	key_t shmem_key;
	int shmem_id;
	char *shmem_addr;
	struct shmem_msgbuf_ctx *buf_ctxs; // msgbuf contexts.
};

// Per-server
struct shmem_server_state {
	int server_id;
	BIT_ARRAY *client_bitmap; // Modified by cmthread, read by ehthread.
	pthread_t ehthread; // event handler thread.
	pthread_t cmthread; // communication manager thread.
	pthread_t ccthread; // client checker thread. To detect client's disconnection.
	// atomic_int s_cb_id; // For allocation. per-server.
	int cq_cb_id;
	key_t cq_key;
	int cq_shmem_id;
	char *cq_shmem_addr;
	sem_t *cq_sem; // It is allocated in shared memory.
	struct shmem_client_state
		*clients[MAX_CLIENT_CONNECTION]; // array of pointers.
};

struct shmem_msgbuf_ctx {
	uint64_t seqn; // Allocated by client.
	struct shmem_msg *req_buf;
	struct shmem_msg *resp_buf;
	struct shmem_evt_flag *evt;
};

// Per-client.
struct shmem_ch_cb {
	struct shmem_ch_attr attr;
	int server; /* 0 iff client */
	pthread_t ehthread; // TODO: required?
	pthread_t server_thread;
	pthread_t server_daemon;

	// BIT_ARRAY *msgbuf_bitmap; // Set to 1 if a new msg arrived.
	int msgbuf_cnt; // Total number of msg buffers.
	int msgbuf_size; // A size of a msg buffer including headers. [msgbuf] = [msgheader] + [msgdata]
	int msgheader_size; // A size of a header in a msg (msg buffer size - data size).
	int msgdata_size; // A size of data in a msg (msg buffer size - header size). Given by user.
	void (*rpc_msg_handler_cb)(void *rpc_pa); // rpc layer callback.
	void (*user_msg_handler_cb)(void *param); // user's msg handler callback.
	void (*on_connect)(
		void *arg); // callback function when client is connected.
	void *conn_arg; // Argument for on_connect callback.
	void (*on_disconnect)(
		void *arg); // callback function when client is disconnected.
	void *disconn_arg; // Argument for on_disconnect callback.
	threadpool msg_handler_thpool; // threadpool to execute msg handler fn.

	char cm_socket_name[108]; // shmem socket name for cm.
	key_t shm_key_seed; // Base key for shared memory segments.

	// for client use.
	// int cb_id; // Client's cb_id;
	key_t shm_key;
	uint64_t shm_size;
	int shmem_id;
	char *shmem_addr;
	key_t cq_shm_key;
	int cq_shmem_id;
	char *cq_shmem_addr;
	sem_t *server_cq_sem;
	struct shmem_msgbuf_ctx *buf_ctxs; // msgbuf contexts.

	// for server use.
	struct shmem_server_state
		*server_state; // For now, it points to the server state.
};

struct shmem_ch_cb *init_shmem_ch(struct shmem_ch_attr *attr);
int send_shmem_msg(struct shmem_ch_cb *cb, struct rpc_ch_info *rpc_ch,
		   char *data, int msgbuf_id);
int send_shmem_response(struct shmem_ch_cb *cb, struct rpc_ch_info *rpc_ch,
			char *data, int client_id, int msgbuf_id,
			uint64_t seqn);
void register_client(struct shmem_ch_cb *cb, int client_fd, key_t *shm_key,
		     key_t *cq_shm_key);
void deregister_client_with_sockfd(struct shmem_ch_cb *server_cb,
				   int client_sockfd);
void deregister_client_with_key(struct shmem_ch_cb *server_cb,
				key_t client_key);
void destroy_shmem_client(struct shmem_ch_cb *cb);
#endif