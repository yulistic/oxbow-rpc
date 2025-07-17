#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <stdatomic.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <assert.h>
#include "global.h"
#include "shmem.h"
#include "shmem_cm.h"
#include "rpc.h"
#include "profiling.h"

// Per file debug print setup.
// #define ENABLE_PRINT 1
#include "log.h"

/**
 * @brief
 * There is only one server in a channel.
 * Per-server:
 * 	- ch id
 * 	- struct shmem_server_state
 * 	- cm thread
 * 	- event handler thread
 * 
 * A server connects with multiple clients (a.k.a. connection).
 * Per-client:
 * 	- client id
 * 	- struct shmem_ch_cb (control block)
 * 	- a bit of server's client bitmap
 * 
 * A client has multiple message buffers.
 * Per-msgbuf:
 * 	- msgbuf id
 * 	- a bit of connection's msgbuf bitmap
 * 	- shmem id
 * 	- shmem addr
 */

static int initialized = 0;
static atomic_uint g_server_cnt; // The number of server created.
static atomic_int g_server_id;

/**
 * @brief Calculate the total size of notification queue in shared memory
 * This includes both the queue structure and the notifications array
 * 
 * @return size_t Total notification queue size in bytes
 */
static inline size_t get_notification_queue_size(void)
{
	return sizeof(struct msg_notification_queue) +
	       MSG_NOTIFICATION_QUEUE_SIZE * sizeof(struct msg_notification);
}

static inline uint64_t alloc_seqn(struct shmem_msgbuf_ctx *msgbuf)
{
	return msgbuf->seqn++;
}

/**
 * @brief Client sends a shared memory message (request) to Server.
 * 
 * @param cb 
 * @param rpc_ch 
 * @param data 
 * @param msgbuf_id 
 * @return int 
 */
int send_shmem_msg(struct shmem_ch_cb *cb, struct rpc_ch_info *rpc_ch,
		   char *data, int msgbuf_id)
{
	uint64_t seqn;
	struct shmem_msgbuf_ctx *mb_ctx;
	struct shmem_msg *msg;

	if (msgbuf_id >= cb->msgbuf_cnt) {
		log_error(
			"msg buffer id(%d) exceeds total msg buffer count(%d).",
			msgbuf_id, cb->msgbuf_cnt);
		return 0;
	}

	mb_ctx = &cb->buf_ctxs[msgbuf_id];

	seqn = alloc_seqn(mb_ctx);

	msg = mb_ctx->req_buf;
	msg->seq_num = seqn;
	msg->rpc_ch = rpc_ch;

	// Copy and send fixed size, currently.
	// FIXME: Can we copy only the meaningful data? memset will be required.
	// memset(&msg->data[0], 0, cb->msgdata_size);
	memcpy(&msg->data[0], data, cb->msgdata_size);

	// Event-Driven Direct Notification: Add to server's notification queue
	// This eliminates the need for O(n²) scanning on the server side
	rpc_assert(cb->server_notif_queue);
	int ret = msg_notification_queue_push(cb->server_notif_queue,
					      cb->client_id, msgbuf_id);
	if (ret != 0) {
		log_warn(
			"Failed to add notification to queue for client %d buffer %d",
			cb->client_id, msgbuf_id);
	}

	// log_info("Sending SHMEM msg: seqn=%lu rpc_ch_addr=%lx data=\"%s\"",
	// 	 seqn, (uint64_t)rpc_ch, msg->data);

	// Post global sem to notify server an event arrived.
	sem_post(cb->server_cq_sem);

	return cb->msgbuf_size;
}

static int alloc_server_id(void)
{
	return atomic_fetch_add(&g_server_id, 1);
}

// Allocate a control block id. It is used in the key generation.
// static int alloc_cb_id(struct shmem_server_state *server)
// {
// 	return atomic_fetch_add(&server->s_cb_id, 1);
// }

/**
 * @brief Create a shm seg object. (called by server)
 * 
 * @param key 
 * @param size in byte.
 * @return int 
 */
int create_shm_seg(key_t key, uint64_t size)
{
	int shmid, err_num;

	// Create shared memory segment
	shmid = shmget(key, size, IPC_CREAT | 0666);
	if (shmid < 0) {
		err_num = errno;
		if (err_num == EINVAL) {
			log_error(
				"There exists a segment with the same key and different size."
				" key=0x%lx size=%lu Please remove that segment using a command:"
				" ipcrm -m <shmid>",
				key, size);
		} else {
			log_error(
				"Creating shared memory segment (shmget) failed. Errno=%s",
				strerror(errno));
		}
		return -1;
	}

	log_debug("shmget: shmid=%d", shmid);
	return shmid;
}

// Called by client.
int get_shm_seg(key_t key, uint64_t size)
{
	int shmid, err_num;

	shmid = shmget(key, size, 0666);
	if (shmid < 0) {
		err_num = errno;
		if (err_num == EINVAL) {
			log_error(
				"There exists a segment with the same key and different size."
				" key=0x%lx size=%lu Please check the segment using a command: `ipcs`"
				" and remove that segment using a command: `ipcrm -m <shmid>`",
				key, size);
		} else {
			log_error(
				"Getting shared memory segment (shmget) failed. Errno=%s",
				strerror(errno));
		}
		return -1;
	}

	log_debug("shmget: shmid=%d", shmid);
	return shmid;
}

void remove_shm_seg(int shmid)
{
	int ret;

	// Remove shared memory segment
	ret = shmctl(shmid, IPC_RMID, NULL);
	if (ret == -1)
		log_error("Removing shared memory segment (shmctl) failed.");
}

char *attach_shm_seg(int shmid)
{
	char *shmaddr;

	// Attach shared memory segment
	shmaddr = shmat(shmid, NULL, 0);
	if (shmaddr == (char *)-1) {
		log_error(
			"Attaching shared memory segment (shmat) failed. errno=%d",
			errno);
		return NULL;
	}
	return shmaddr;
}

void detach_shm_seg(char *shmaddr)
{
	int ret;

	// Detach shared memory segment
	ret = shmdt(shmaddr);
	if (ret == -1)
		log_error(
			"Detaching shared memory segment (shmdt) failed. errno=%d",
			errno);
}

//Client function.
int attach_client_shmem(struct shmem_ch_cb *cb)
{
	int shmid;
	char *shmaddr;

	// Get and attach shmem for message buffers.
	shmid = get_shm_seg(cb->shm_key, cb->shm_size);

	if (shmid == -1) {
		log_error("Getting client's shmem(shmget) failed.");
		goto err1;
	}
	cb->shmem_id = shmid;

	shmaddr = attach_shm_seg(shmid);
	if (!shmaddr) {
		log_error("Attaching client's shmem(shmat) failed.");
		goto err2;
	}
	cb->shmem_addr = shmaddr;

	log_debug("Attached shmem_addr=0x%lx", shmaddr);

	// Get and attach shmem for server's event semaphore (cq) and notification queue.
	// Calculate size for semaphore + notification queue in shared memory
	size_t cq_shm_size = sizeof(sem_t) + get_notification_queue_size();

	shmid = get_shm_seg(cb->cq_shm_key, cq_shm_size);

	if (shmid == -1) {
		log_error("Getting client's CQ shmem(shmget) failed.");
		goto err3;
	}
	cb->cq_shmem_id = shmid;

	shmaddr = attach_shm_seg(shmid);
	if (!shmaddr) {
		log_error("Attaching client's CQ shmem(shmat) failed.");
		goto err4;
	}
	cb->cq_shmem_addr = shmaddr;
	log_debug("Attached cq_shmem_addr=0x%lx", shmaddr);

	// Setup semaphore pointer (at beginning of shared memory)
	cb->server_cq_sem = (sem_t *)shmaddr;

	// Setup notification queue pointer (after semaphore)
	char *notif_queue_addr = shmaddr + sizeof(sem_t);
	cb->server_notif_queue =
		(struct msg_notification_queue *)notif_queue_addr;

	log_info("Client connected to Event-Driven notification queue at 0x%lx",
		 (unsigned long)cb->server_notif_queue);

	return 0;

err4:
	remove_shm_seg(cb->cq_shmem_id);
err3:
	detach_shm_seg(cb->shmem_addr);
err2:
	remove_shm_seg(cb->shmem_id);
err1:
	return -1;
}

/**
 * @brief Set message buffer contexts 
 * 
 * @param mb_ctx Message buffer contexts
 * @param shm_addr Base shared memory address  
 * @param msgbuf_size Size of each message buffer
 * @param msgbuf_cnt Number of message buffers
 * @param init_sem Initialize semaphore if 1
 */
void set_shmem_msgbuf_ctx(struct shmem_msgbuf_ctx *mb_ctx, char *shm_addr,
			  int msgbuf_size, int msgbuf_cnt, int init_sem)
{
	int i;
	char *cb_p;
	struct shmem_evt_flag *ef;

	// Point msgbuf start
	cb_p = shm_addr;

	// Point evt flag start (after message buffers)
	ef = (struct shmem_evt_flag *)(shm_addr + 2 * msgbuf_size * msgbuf_cnt);

	for (i = 0; i < msgbuf_cnt; i++) {
		mb_ctx[i].req_buf = (struct shmem_msg *)cb_p;
		cb_p += msgbuf_size;

		mb_ctx[i].resp_buf = (struct shmem_msg *)cb_p;
		cb_p += msgbuf_size;

		if (init_sem)
			sem_init(&ef->client_sem, 1, 0);
		mb_ctx[i].evt = ef;
		ef++; // advance sizeof(struct shmem_evt_flag) bytes.
	}
}

// Server function.
void init_shmem_msgbuf_ctx_in_server(struct shmem_ch_cb *server_cb,
				     struct shmem_client_state *client)
{
	set_shmem_msgbuf_ctx(client->buf_ctxs, client->shmem_addr,
			     server_cb->msgbuf_size, server_cb->msgbuf_cnt, 0);
}

// Client function.
/**
 * @brief Set send and recv buffer addresses.
 * 
 * @param cb 
 */
void init_shmem_msgbuf_ctx_in_client(struct shmem_ch_cb *cb)
{
	set_shmem_msgbuf_ctx(cb->buf_ctxs, cb->shmem_addr, cb->msgbuf_size,
			     cb->msgbuf_cnt, 1);
}

/**
 * @brief Generate shmem key.
 * 
 * @param cb_id 
 * @return key_t 
 */
key_t generate_shm_key(struct shmem_ch_cb *cb, int cb_id)
{
	// FIXME: Use cb_id as a key. If we create a key with hashing, server
	// should manage mapping table between cb_id and key.
	return cb_id + cb->shm_key_seed;
}

/**
 * @brief Get the cb_id (client_id) with cm sockfd.
 * 
 * @param server_cb 
 * @param client_fd 
 * @return int 
 */
int get_cb_id_with_sockfd(struct shmem_ch_cb *server_cb, int client_fd)
{
	struct shmem_client_state **clients;
	int i;

	clients = server_cb->server_state->clients;

	for (i = 0; i < MAX_CLIENT_CONNECTION; i++) {
		if (clients[i] != NULL && clients[i]->client_cm_fd == client_fd)
			return i;
	}
	return -1;
}

/**
 * @brief Get the cb id with key.
 * 
 * @param key 
 * @return int 
 */
int get_cb_id_with_key(key_t key, key_t seed)
{
	// FIXME: Use cb_id as a key. If we create a key with hashing, server
	// should manage mapping table between cb_id and key.
	return (int)(key - seed);
}

/**
 * @brief Register a client. It initializes per-client data structures.
 * We use shared memory as below.
 *  Lower                                             Higher
 * | <recv buffers> | <send_buffers> | <msgbuf bitmap area> |
 * 
 * <msgbuf bitmap area> stores a BIT_ARRAY structure and its bits.
 *  Lower                                    Higher
 * | <BIT_ARRAY structure> | <bits (a.k.a. words)> |
 * 
 * @param cb Server's cb.
 * @param client_fd Client's socket fd. (Required on deregistering.)
 * @param shm_key Where the registered shmem key is stored.
 * @param cq_shm_key Where cq shmem key is stored.
 */
void register_client(struct shmem_ch_cb *cb, int client_fd, key_t *shm_key,
		     key_t *cq_shm_key)
{
	uint64_t msgbuf_evt_flags_size, total_msgbuf_size, shm_size;
	// BIT_ARRAY *tmp_msgbuf_bitmap, *msgbuf_bitmap;
	// char *tmp;
	bit_index_t cb_id;
	struct shmem_client_state *client;
	struct shmem_server_state *server;

	server = cb->server_state;

	// Alloc cb_id. We don't need locking even though the bit is set at the
	// end of this function by calling bit_array_set_bit() because only this
	// thread modify it sequentially.
	bit_array_find_first_clear_bit(server->client_bitmap, &cb_id);
	log_debug("Allocated cb_id=%d", cb_id);

	server->clients[cb_id] = calloc(1, sizeof(struct shmem_client_state));

	client = server->clients[cb_id];
	if (!client) {
		log_error("calloc failed.");
		goto err1;
	}

	client->client_cm_fd = client_fd;

	client->buf_ctxs =
		calloc(cb->msgbuf_cnt, sizeof(struct shmem_msgbuf_ctx));
	if (!client->buf_ctxs) {
		log_error("calloc failed.");
		goto err2;
	}

	// tmp_msgbuf_bitmap = bit_array_create(cb->msgbuf_cnt);

	// Calculate required memory size.

	// Use bitarray to reduce memory footprint.
	// log_debug("sizeof *tmp_msgbuf_bitmap->words =%lu",
	// 	  sizeof *tmp_msgbuf_bitmap->words);
	// msgbuf_evt_flags_size =
	// 	sizeof(BIT_ARRAY) +
	// 	sizeof *tmp_msgbuf_bitmap->words; // refer to bit_array.h

	// We use uint64_t to store a bit to avoid continual cache invalidation.
	msgbuf_evt_flags_size = sizeof(struct shmem_evt_flag) * cb->msgbuf_cnt;
	total_msgbuf_size =
		cb->msgbuf_size * cb->msgbuf_cnt * 2 /* send & recv */;

	shm_size = total_msgbuf_size + msgbuf_evt_flags_size;

	log_debug(
		"msgbuf_evt_flags_size=%lu total_msgbuf_size=%lu shm_size=%lu",
		msgbuf_evt_flags_size, total_msgbuf_size, shm_size);

	// Create & attach shmem seg.
	client->cb_id = cb_id;
	client->shmem_key = generate_shm_key(cb, cb_id);

	client->shmem_id = create_shm_seg(client->shmem_key, shm_size);
	if (client->shmem_id == -1) {
		log_error("shm_get failed. cb_id=%d shmem_key=%lu", cb_id,
			  client->shmem_key);
		goto err3;
	}
	client->shmem_addr = attach_shm_seg(client->shmem_id);
	if (!client->shmem_addr) {
		log_error("shm_att failed. cb_id=%d shmem_key=%lu shmem_id=%d",
			  cb_id, client->shmem_key, client->shmem_id);
		goto err4;
	}

	// Initializing shmem_seg is required because global event handler checks
	// its content after the client-bitmap is set.
	memset(client->shmem_addr, 0, shm_size);

	// Setup msgbuf bitmap. (When using bitarray to reduce memory footprint)
	// tmp = client->shmem_addr;
	// tmp += msgbuf_size; // Where the BIT_ARRAY of msgbuf bitmap is stored.
	// msgbuf_bitmap = (BIT_ARRAY *)tmp;

	// memcpy(msgbuf_bitmap, tmp_msgbuf_bitmap,
	//        sizeof(BIT_ARRAY)); // copy the created bitmap.
	// tmp += sizeof(BIT_ARRAY); // Where the bits(words) start.
	// msgbuf_bitmap->words = tmp;

	// bit_array_free(tmp_msgbuf_bitmap); // Free temp bitmap.

	// Set client's msgbuf ctx.
	init_shmem_msgbuf_ctx_in_server(cb, client);

	// Set client bitmap.
	bit_array_set_bit(server->client_bitmap, cb_id);

#if ENABLE_PROFILING
	// Update active client count
	atomic_fetch_add(&g_server_prof.active_clients, 1);
#endif

	log_info("Client registered. client-id=%d shmem-id=%d shmem-addr=0x%lx",
		 cb_id, client->shmem_id, (uint64_t)client->shmem_addr);

	*shm_key = client->shmem_key;
	*cq_shm_key = server->cq_key;
	return;
err4:
	remove_shm_seg(client->shmem_id);
err3:
	free(client->buf_ctxs);
err2:
	free(client);
err1:
	return;
}

void deregister_client_with_sockfd(struct shmem_ch_cb *server_cb,
				   int client_sockfd)
{
	struct shmem_server_state *server;
	int cb_id;

	server = server_cb->server_state;
	cb_id = get_cb_id_with_sockfd(server_cb, client_sockfd);
	if (cb_id < 0) {
		log_info("No client (sockfd=%d) found.", client_sockfd);
		return;
	}

	bit_array_clear_bit(server->client_bitmap, cb_id);

	// Update active client count
#if ENABLE_PROFILING
	if (atomic_load(&g_server_prof.active_clients) > 0) {
		atomic_fetch_sub(&g_server_prof.active_clients, 1);
	}
#endif

	free(server->clients[cb_id]);
	server->clients[cb_id] = NULL;
}

// Deregister_client.
void deregister_client_with_key(struct shmem_ch_cb *server_cb, key_t client_key)
{
	struct shmem_server_state *server;
	int cb_id;

	server = server_cb->server_state;
	cb_id = get_cb_id_with_key(client_key, server_cb->shm_key_seed);

	bit_array_clear_bit(server->client_bitmap, cb_id);

#if ENABLE_PROFILING
	// Update active client count
	if (atomic_load(&g_server_prof.active_clients) > 0) {
		atomic_fetch_sub(&g_server_prof.active_clients, 1);
	}
#endif

	free(server->clients[cb_id]);
	server->clients[cb_id] = NULL;
}

// Server function.
static int handle_client_msg(struct shmem_ch_cb *cb,
			     struct shmem_client_state *client, int msgbuf_id)
{
	struct rpc_msg_handler_param *rpc_param;
	struct msg_handler_param *param;
	struct rpc_msg *msg;
	struct shmem_msgbuf_ctx *mb_ctx;
	int ret;

	// Profile total message processing time
	PROF_START(total_start);

	mb_ctx = &client->buf_ctxs[msgbuf_id];

	// Profile memory allocation time
	PROF_START(alloc_start);

	// These are freed in the handler callback function.
	rpc_param = calloc(1, sizeof *rpc_param);
	if (!rpc_param) {
		ret = -ENOMEM;
		goto err1;
	}
	param = calloc(1, sizeof *param);
	if (!param) {
		ret = -ENOMEM;
		goto err2;
	}
	msg = calloc(1, cb->msgbuf_size);
	if (!msg) {
		ret = -ENOMEM;
		goto err3;
	}

	PROF_END_UPDATE(alloc_start, &g_server_prof.msg_alloc);

	msg->header.seqn = mb_ctx->req_buf->seq_num;
	msg->header.client_rpc_ch = mb_ctx->req_buf->rpc_ch;

	// Profile message copy time
	PROF_START(copy_start);

	// Copy and send fixed size, currently.
	// OPTIMIZE: Can we copy only the meaningful data? memset will be required.
	// memset(...);
	memcpy(&msg->data[0], &mb_ctx->req_buf->data[0], cb->msgdata_size);

	PROF_END_UPDATE(copy_start, &g_server_prof.msg_copy);

	param->client_id = client->cb_id;
	param->msgbuf_id = msgbuf_id;
	param->ch_cb = (struct rdma_ch_cb *)cb;
	param->msg = msg;

	rpc_param->msgbuf_id = msgbuf_id;
	rpc_param->client_rpc_ch = mb_ctx->req_buf->rpc_ch;
	rpc_param->param = param;
	rpc_param->user_msg_handler_cb = cb->user_msg_handler_cb;

	log_debug("Received msgbuf_id=%d seqn=%lu data=%s rpc_ch=0x%lx",
		  msgbuf_id, msg->header.seqn, msg->data,
		  (uint64_t)rpc_param->client_rpc_ch);

	// Profile thread pool dispatch time
	PROF_START(dispatch_start);

	// Execute RPC callback function in a worker thread.
	if (cb->rpc_msg_handler_cb)
		thpool_add_work(cb->msg_handler_thpool, cb->rpc_msg_handler_cb,
				(void *)rpc_param);

	PROF_END_UPDATE(dispatch_start, &g_server_prof.msg_handler_dispatch);

	// Update total processing time and message count
	PROF_END_UPDATE(total_start, &g_server_prof.total_msg_processing);

#if ENABLE_PROFILING
	atomic_fetch_add(&g_server_prof.total_messages_processed, 1);
#endif

	return 0;
err3:
	free(param);
err2:
	free(rpc_param);
err1:
	return ret;
}

static void *handle_event(void *arg)
{
	struct shmem_ch_cb *cb;
	struct shmem_server_state *server;

	cb = (struct shmem_ch_cb *)arg;
	server = cb->server_state;

	if (cb->on_connect)
		cb->on_connect(cb->conn_arg);

	rpc_assert(server->notif_queue);

	while (1) {
		pthread_testcancel();

		// Producer will post sem.
		rpc_sem_wait(server->cq_sem);

		// Event-Driven Direct Notification: Process messages directly from queue
		// This replaces O(n²) scanning with O(1) direct processing
		struct msg_notification notification;
		int processed_count = 0;

		// Process all available notifications from the queue
		while (msg_notification_queue_pop(server->notif_queue,
						  &notification) == 0) {
			// Profile message buffer scanning
			PROF_START(scan_start);

			int client_id = notification.client_id;
			int buffer_id = notification.buffer_id;

#ifdef RPC_VALIDATION
			// Validate client_id
			if (client_id < 0 ||
			    client_id >= MAX_CLIENT_CONNECTION ||
			    !server->clients[client_id]) {
				log_error(
					"Invalid client_id %d in notification. (buffer_id=%d)",
					client_id, buffer_id);
				assert(0);
			}
#endif
			struct shmem_client_state *client =
				server->clients[client_id];

#ifdef RPC_VALIDATION
			// Validate buffer_id
			if (buffer_id < 0 || buffer_id >= cb->msgbuf_cnt) {
				log_error("Invalid buffer_id %d for client %d",
					  buffer_id, client_id);
				assert(0);
			}
#endif
			log_debug(
				"[Event-Driven] Processing message from Client %d Buffer %d",
				client_id, buffer_id);

			// Process the message directly
			handle_client_msg(cb, client, buffer_id);

			processed_count++;

			PROF_END_UPDATE(scan_start, &g_server_prof.msgbuf_scan);
		}

		log_debug(
			"[Event-Driven] Processed %d messages directly from notification queue",
			processed_count);
	}

	if (cb->on_disconnect)
		cb->on_disconnect(cb->disconn_arg);

	return NULL;
}

// For test.
// #define MSG_INTERVAL_MICROSEC 1000

// Server function.
int send_shmem_response(struct shmem_ch_cb *cb, struct rpc_ch_info *rpc_ch,
			char *data, int client_id, int msgbuf_id, uint64_t seqn)
{
	struct shmem_msgbuf_ctx *mb_ctx;
	struct shmem_server_state *server;
	struct shmem_client_state *client;
	struct shmem_msg *msg;

	server = cb->server_state;
	client = server->clients[client_id];
	mb_ctx = &client->buf_ctxs[msgbuf_id];

	msg = mb_ctx->resp_buf;

	msg->seq_num = seqn;
	msg->rpc_ch = rpc_ch;

	// printf("Sending SHMEM msg: seqn=%lu &msg->data[0]=0x%lx data=\"%s\"(0x%lx) cb->msgdata_size=%u\n",
	//        msg->seq_num, (uint64_t)&msg->data[0], data, (uint64_t)data,
	//        cb->msgdata_size);

	// Copy and send fixed size, currently.
	// FIXME: Can we copy only the meaningful data? memset will be required.
	// memset(&msg->data[0], 0, cb->msgdata_size);
	// NOTE: If size of data < cb->msgdata_size, we should send data upto size of 'data'.
	memcpy(&msg->data[0], data, cb->msgdata_size);

	// log_info("Sending SHMEM msg: seqn=%lu rpc_ch_addr=0x%lx data=\"%s\"",
	// 	 msg->seq_num, (uint64_t)rpc_ch, msg->data);

	// For test.
	// Interval between messages. (To measure sleep overhead.)
	// usleep(MSG_INTERVAL_MICROSEC);

	// Notify client by post client's sem directly.
	log_debug("Post sema of Client %d: address=0x%lx", client_id,
		  &mb_ctx->evt->client_sem);
	sem_post(&mb_ctx->evt->client_sem);
	return cb->msgbuf_size;
}

// For server.
static void init_shmem_server(struct shmem_ch_cb *cb)
{
	struct shmem_server_state *server;
	int ret, server_cnt, pshared;

	if (!initialized) {
		atomic_init(&g_server_id, 1); // Start from 1.
		atomic_init(&g_server_cnt, 0);
		initialized = 1;

		// Initialize profiling
		init_profiling();
	}

	server = calloc(1, sizeof(struct shmem_server_state));
	if (!server) {
		log_error("Memory allocation failed.");
		return;
	}

	cb->server_state = server;

	// Some per-server initializations.
	server->server_id = alloc_server_id();
	server->client_bitmap = bit_array_create(MAX_CLIENT_CONNECTION);
	// atomic_init(&server->s_cb_id, 0);

	server->cq_cb_id = SHMEM_CQ_CB_ID;
	rpc_assert(server->cq_cb_id > MAX_CLIENT_CONNECTION);

	// Create CQ shmem for per server cq event thread.
	server->cq_key = generate_shm_key(cb, server->cq_cb_id);

	// Calculate size for semaphore + notification queue in shared memory
	size_t cq_shm_size = sizeof(sem_t) + get_notification_queue_size();

	server->cq_shmem_id = create_shm_seg(server->cq_key, cq_shm_size);
	if (server->cq_shmem_id == -1) {
		log_error("Getting server's CQ shmem(shmget) failed.");
		goto err1;
	}

	server->cq_shmem_addr = attach_shm_seg(server->cq_shmem_id);
	if (!server->cq_shmem_addr) {
		log_error("Attaching server's CQ shmem(shmat) failed.");
		goto err2;
	}

	// Locate semaphore at the beginning of shared memory
	server->cq_sem = (sem_t *)server->cq_shmem_addr;
	pshared = 1;
	sem_init(server->cq_sem, pshared, 0);

	// Setup notification queue in shared memory (after semaphore)
	char *notif_queue_addr = server->cq_shmem_addr + sizeof(sem_t);
	server->notif_queue = (struct msg_notification_queue *)notif_queue_addr;

	// Initialize notification queue structure in shared memory
	// IMPORTANT: The order of initialization must match the struct definition in shmem.h
	atomic_init(&server->notif_queue->head, 0);
	atomic_init(&server->notif_queue->tail, 0);
	server->notif_queue->capacity = MSG_NOTIFICATION_QUEUE_SIZE;
	server->notif_queue->mask = MSG_NOTIFICATION_QUEUE_SIZE - 1;

	// Initialize sequence numbers for all slots to 0
	struct msg_notification *notifications =
		get_notifications_array(server->notif_queue);
	for (int i = 0; i < MSG_NOTIFICATION_QUEUE_SIZE; i++) {
		atomic_init(&notifications[i].sequence, 0);
	}

	log_info(
		"Initialized Event-Driven notification queue in shared memory at 0x%lx",
		(unsigned long)server->notif_queue);

	ret = pthread_create(&server->ehthread, NULL, handle_event, (void *)cb);
	if (ret) {
		printf("Creating event handler thread failed.\n");
		goto err3;
	}
	log_info("Running event handler thread of server %d.",
		 server->server_id);

	// A thread for checkout client's disconnection.
	// ret = pthread_create(&server->ccthread, NULL,
	// 		     check_client_disconnection, (void *)cb);
	// if (ret) {
	// 	printf("Creating client checker thread failed.\n");
	// 	goto err3;
	// }
	// log_info("Running client checker thread of server %d.",
	// 	 server->server_id);

	// create global cm thread.
	ret = pthread_create(&server->cmthread, NULL, shmem_cm_thread,
			     (void *)cb);
	if (ret) {
		printf("Creating cm thread failed.\n");
		goto err4;
	}
	log_info("[CM] Running cm thread of server %d.", server->server_id);

	server_cnt = atomic_fetch_add(&g_server_cnt, 1);
	log_info("Server %d created. server_cnt=%d", server->server_id,
		 server_cnt);
#ifdef RPC_VALIDATION
	log_warn(
		"RPC_VALIDATION is enabled. Turn it off for better performance.");
#endif

	return;
err4:
	pthread_cancel(server->ehthread);
	pthread_join(server->ehthread, NULL);
err3:
	// Notification queue is in shared memory, will be cleaned up with CQ segment
	detach_shm_seg(server->cq_shmem_addr);
err2:
	remove_shm_seg(server->cq_shmem_id);
err1:
	free(server);
}

static void destroy_shmem_server(struct shmem_server_state *server)
{
	int server_cnt_before;
	// TODO: To be implemented.
	// TODO: Free some resources.

	// Clean up notification queue (now in shared memory, no need to free)
	if (server->notif_queue) {
		log_info(
			"Notification queue in shared memory will be cleaned up with CQ segment");
		server->notif_queue = NULL;
	}

	// Free CQ channel.
	server_cnt_before = atomic_fetch_sub(&g_server_cnt, 1);
	if (server_cnt_before == 1) {
		//TODO: Need to destroy cm thread?
		// destroy_cm_thread()
		// initialized = 0;
		;
	}

	free(server);
}

static inline int msgheader_size(void)
{
	struct shmem_msg msg;
	return (int)((uint64_t)&msg.data - (uint64_t)&msg);
}

static inline int msgdata_size(int msgbuf_size)
{
	return msgbuf_size - msgheader_size();
}

static inline int msgbuf_size(int msgdata_size)
{
	return msgdata_size + msgheader_size();
}

struct shmem_ch_cb *init_shmem_ch(struct shmem_ch_attr *attr)
{
	struct shmem_ch_cb *cb;
	int ret;

	cb = calloc(1, sizeof(struct shmem_ch_cb));
	if (!cb) {
		ret = -ENOMEM;
		goto err;
	}

	// Store the seed value
	cb->shm_key_seed = attr->shm_key_seed;

	cb->server = attr->server;
	cb->msgbuf_cnt = attr->msgbuf_cnt;
	cb->msgheader_size = msgheader_size();
	cb->msgdata_size = attr->msgdata_size;
	cb->msgbuf_size = msgbuf_size(attr->msgdata_size);
	cb->rpc_msg_handler_cb = attr->rpc_msg_handler_cb;
	cb->user_msg_handler_cb = attr->user_msg_handler_cb;
	cb->msg_handler_thpool = attr->msg_handler_thpool;
	cb->on_connect = attr->on_connect;
	cb->conn_arg = attr->conn_arg;
	cb->on_disconnect = attr->on_disconnect;
	cb->disconn_arg = attr->disconn_arg;
	strcpy(cb->cm_socket_name, attr->cm_socket_name);

	if (cb->server) {
		init_shmem_server(cb); // init cb->server_state
	} else {
		// Initialize client-specific fields
		cb->client_cm_fd = -1;

		cb->buf_ctxs =
			calloc(cb->msgbuf_cnt, sizeof(struct shmem_msgbuf_ctx));
		if (!cb->buf_ctxs) {
			ret = -ENOMEM;
			log_error("calloc failed. errno=%d", errno);
			goto err1;
		}

		// req.client_cb_addr = &cb;
		ret = connect_to_shmem_server(cb, &cb->shm_key, &cb->shm_size,
					      &cb->cq_shm_key);
		if (ret) {
			log_error("connect to shmem server failed.");
			goto err2;
		}

		// Calculate client_id from shm_key for Event-Driven notification
		cb->client_id =
			get_cb_id_with_key(cb->shm_key, cb->shm_key_seed);
		log_info("Client initialized with ID: %d", cb->client_id);

		ret = attach_client_shmem(cb);
		if (ret) {
			log_error("attach client shmem failed.");
			goto err2;
		}

		init_shmem_msgbuf_ctx_in_client(cb);
	}

	return cb;

err2:
	free(cb->buf_ctxs);
err1:
	free(cb);
err:
	printf("Initializing shmem channel failed. ret=%d\n", ret);
	return NULL;
}

void destroy_shmem_client(struct shmem_ch_cb *cb)
{
	struct shmem_cm_request req;
	int ret;

	if (cb->server) {
		log_warn("destroy_shmem_client() is for client only.");
		return;
	}

	// Send DEREGISTER message to server if CM connection is valid
	if (cb->client_cm_fd >= 0) {
		req.op = DEREGISTER;
		req.client_key = cb->shm_key;

		ret = write(cb->client_cm_fd, &req,
			    sizeof(struct shmem_cm_request));
		if (ret == -1) {
			log_warn("Failed to send DEREGISTER message to server");
		} else {
			log_debug("Sent DEREGISTER message to server");
		}

		// Close the connection management socket
		close(cb->client_cm_fd);
		log_debug("Closed client CM socket fd=%d", cb->client_cm_fd);
		cb->client_cm_fd = -1;
	}

	// Detach shared memory segments
	if (cb->shmem_addr) {
		detach_shm_seg(cb->shmem_addr);
		log_debug("Detached shmem_addr=0x%lx", cb->shmem_addr);
	}

	if (cb->cq_shmem_addr) {
		detach_shm_seg(cb->cq_shmem_addr);
		log_debug("Detached cq_shmem_addr=0x%lx", cb->cq_shmem_addr);
	}

	// Free message buffer contexts
	if (cb->buf_ctxs) {
		free(cb->buf_ctxs);
		cb->buf_ctxs = NULL;
		log_debug("Freed buf_ctxs");
	}

	// Note: The cb structure itself is freed by the RPC layer (destroy_rpc_client)
}

// ==================== Event-Driven Direct Notification Implementation ====================

/**
 * @brief Push a notification to the queue.
 * This is a lock-free, multiple-producer safe implementation.
 * Called by clients to notify the server of new messages. It uses a
 * compare-and-swap (CAS) loop to atomically claim a slot in the queue.
 *
 * @param queue         Notification queue.
 * @param client_id     Client ID that sent the message.
 * @param buffer_id     Buffer ID containing the message.
 * @return int          0 on success, -1 on error (e.g., queue full).
 */
int msg_notification_queue_push(struct msg_notification_queue *queue,
				int client_id, int buffer_id)
{
	unsigned long long tail, head, next_tail;

	if (!queue)
		return -1;

	log_info("[NOTIF_QUEUE] Pushing notification: Client %d Buffer %d",
		 client_id, buffer_id);

	// High-performance lock-free Multiple Producer implementation
	// Optimized memory ordering for better performance while maintaining correctness
	while (1) {
		// Load current tail with relaxed ordering for better performance
		tail = atomic_load_explicit(&queue->tail, memory_order_relaxed);

		// Load current head with acquire ordering to see consumer updates
		head = atomic_load_explicit(&queue->head, memory_order_acquire);

		next_tail = tail + 1;

		// Check if queue is full (tail is capacity ahead of head)
		if (__builtin_expect(next_tail - head >= queue->capacity, 0)) {
			log_warn(
				"Notification queue is full! Consider increasing queue size.");
			return -1; // Queue full
		}

		// Try to atomically claim this tail position with optimized ordering
		// Success: acquire-release ensures proper synchronization
		// Failure: relaxed is sufficient for retry
		if (atomic_compare_exchange_weak_explicit(
			    &queue->tail, &tail, next_tail,
			    memory_order_acq_rel, // success: acquire-release
			    memory_order_relaxed)) { // failure: relaxed for retry

			// Successfully claimed slot[tail]
			// Calculate slot index
			uint32_t slot_index = tail & queue->mask;

			struct msg_notification *notifications =
				get_notifications_array(queue);

			// Write data atomically with proper ordering
			notifications[slot_index].client_id = client_id;
			notifications[slot_index].buffer_id = buffer_id;

			// Use sequence number to signal that the data is ready.
			// This is essential for correctness in a multi-producer scenario.
			uint64_t sequence_number =
				(tail >> __builtin_ctzl(queue->capacity)) + 1;

			// Store sequence number last with release ordering
			// This ensures all data is visible before sequence becomes valid
			atomic_store_explicit(
				&notifications[slot_index].sequence,
				sequence_number, memory_order_release);

			log_debug(
				"[NOTIF_QUEUE] Successfully pushed: Client %d Buffer %d to slot %u (head=%llu, tail=%llu->%llu, seq=%llu)",
				client_id, buffer_id, slot_index, head, tail,
				next_tail, sequence_number);

			break; // Success, exit loop
		}

		// Exponential backoff on contention to reduce cache line bouncing
		// This improves performance under high contention
		static __thread int backoff_count = 0;
		for (int i = 0; i < (1 << (backoff_count & 7)); i++) {
			__builtin_ia32_pause(); // CPU hint for spin-wait loops
		}
		backoff_count++;
	}

	return 0;
}

/**
 * @brief Pop a notification from the queue.
 * This is a lock-free, single-consumer safe implementation.
 * Called by the server to get the next message to process.
 *
 * @param queue         Notification queue.
 * @param notification  Output parameter for the notification data.
 * @return int          0 on success, -1 if the queue is empty or the next item is not ready.
 */
int msg_notification_queue_pop(struct msg_notification_queue *queue,
			       struct msg_notification *notification)
{
	unsigned long long head, tail;

	rpc_assert(queue);
	rpc_assert(notification);

	// Single Consumer (SC) implementation with optimized ordering
	// Load current head with relaxed ordering (single consumer)
	head = atomic_load_explicit(&queue->head, memory_order_relaxed);
	// Load current tail with acquire ordering to see producer updates
	tail = atomic_load_explicit(&queue->tail, memory_order_acquire);

	// Check if queue is empty
	if (__builtin_expect(head == tail, 0)) {
		log_debug("[NOTIF_QUEUE] Queue is empty (head=%llu, tail=%llu)",
			  head, tail);
		return -1; // Queue empty
	}

	// Read notification data
	struct msg_notification *notifications = get_notifications_array(queue);

	// Calculate slot index
	uint32_t slot_index = head & queue->mask;

	// The sequence number indicates if the producer has finished writing.
	// We must check it to prevent reading partially written data.
	uint64_t expected_sequence =
		(head >> __builtin_ctzl(queue->capacity)) + 1;

	// Verify sequence number matches expected value with acquire ordering
	uint64_t actual_sequence = atomic_load_explicit(
		&notifications[slot_index].sequence, memory_order_acquire);

	if (__builtin_expect(actual_sequence != expected_sequence, 0)) {
		// This is not a fatal error. It's a normal condition in a racy,
		// non-blocking queue where the consumer checks a slot that the
		// producer has claimed but not yet finished writing to. The
		// consumer will simply try again later.
		log_debug(
			"[NOTIF_QUEUE] Sequence mismatch at slot %u: expected=%llu, actual=%llu",
			slot_index, expected_sequence, actual_sequence);
		return -1; // Data not ready or corrupted
	}

	// Read validated data (data is guaranteed valid due to sequence check)
	notification->client_id = notifications[slot_index].client_id;
	notification->buffer_id = notifications[slot_index].buffer_id;
	notification->sequence = actual_sequence; // Copy sequence for debugging

	log_debug(
		"[NOTIF_QUEUE] Popped notification: Client %d Buffer %d from slot %u (head=%llu->%llu, tail=%llu, seq=%llu)",
		notification->client_id, notification->buffer_id, slot_index,
		head, head + 1, tail, actual_sequence);

	// Update head with release ordering to make our consumption visible to producers
	atomic_store_explicit(&queue->head, head + 1, memory_order_release);

	return 0;
}

/**
 * @brief Check if notification queue is empty
 * 
 * @param queue Notification queue
 * @return int 1 if empty, 0 if not empty, -1 on error
 */
int msg_notification_queue_is_empty(struct msg_notification_queue *queue)
{
	unsigned long long head, tail;

	if (!queue)
		return -1;

	// Use acquire ordering to ensure we see the latest updates from producers and consumer
	head = atomic_load_explicit(&queue->head, memory_order_acquire);
	tail = atomic_load_explicit(&queue->tail, memory_order_acquire);

	return (head == tail) ? 1 : 0;
}
