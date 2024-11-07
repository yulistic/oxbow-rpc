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

// Per file debug print setup.
// #define ENABLE_PRINT 1
#include "log.h"

#define SHM_KEY_SEED 9367 // arbitrary value.

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

static inline uint64_t alloc_seqn(struct shmem_msgbuf_ctx *msgbuf)
{
	return msgbuf->seqn++;
}

static inline void set_evt_arrival_flag(sem_t *sem)
{
	sem_post(sem);
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

	// Notify server which msgbuf has a new message.
	mb_ctx->evt->server_evt = 1;

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

	// Get and attach shmem for server's event semaphore (cq).
	shmid = get_shm_seg(cb->cq_shm_key,
			    sizeof(sem_t)); // Rounded up to PAGESIZE.

	if (shmid == -1) {
		log_error("Getting client's shmem(shmget) failed.");
		goto err3;
	}
	cb->cq_shmem_id = shmid;

	shmaddr = attach_shm_seg(shmid);
	if (!shmaddr) {
		log_error("Attaching client's shmem(shmat) failed.");
		goto err4;
	}
	cb->cq_shmem_addr = shmaddr;
	log_debug("Attached cq_shmem_addr=0x%lx", shmaddr);

	cb->server_cq_sem =
		(sem_t *)shmaddr; // sem is stored at the beginning of the shmem

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
 * @brief Set the shmem msgbuf ctx object
 * 
 * @param mb_ctx 
 * @param shm_addr 
 * @param msgbuf_size 
 * @param msgbuf_cnt 
 * @param init_sem Initialize sem if 1.
 */
void set_shmem_msgbuf_ctx(struct shmem_msgbuf_ctx *mb_ctx, char *shm_addr,
			  int msgbuf_size, int msgbuf_cnt, int init_sem)
{
	int i;
	char *cb_p;
	struct shmem_evt_flag *ef;

	// Point msgbuf start.
	cb_p = shm_addr;

	// Point evt flag start.
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
key_t generate_shm_key(int cb_id)
{
	// FIXME: Use cb_id as a key. If we create a key with hashing, server
	// should manage mapping table between cb_id and key.
	return cb_id + SHM_KEY_SEED;
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
int get_cb_id_with_key(key_t key)
{
	// FIXME: Use cb_id as a key. If we create a key with hashing, server
	// should manage mapping table between cb_id and key.
	return (int)(key - SHM_KEY_SEED);
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
	client->shmem_key = generate_shm_key(cb_id);

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
	free(server->clients[cb_id]);
	server->clients[cb_id] = NULL;
}

// Deregister_client.
void deregister_client_with_key(struct shmem_ch_cb *server_cb, key_t client_key)
{
	struct shmem_server_state *server;
	int cb_id;

	server = server_cb->server_state;
	cb_id = get_cb_id_with_key(client_key);

	bit_array_clear_bit(server->client_bitmap, cb_id);
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

	mb_ctx = &client->buf_ctxs[msgbuf_id];

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

	msg->header.seqn = mb_ctx->req_buf->seq_num;
	msg->header.client_rpc_ch = mb_ctx->req_buf->rpc_ch;

	// Copy and send fixed size, currently.
	// OPTIMIZE: Can we copy only the meaningful data? memset will be required.
	// memset(...);
	memcpy(&msg->data[0], &mb_ctx->req_buf->data[0], cb->msgdata_size);

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

	// Execute RPC callback function in a worker thread.
	if (cb->rpc_msg_handler_cb)
		thpool_add_work(cb->msg_handler_thpool, cb->rpc_msg_handler_cb,
				(void *)rpc_param);
	return 0;
err3:
	free(param);
err2:
	free(rpc_param);
err1:
	return ret;
}

// It checks whether there is a message arrived. Handle one if there exists.
static int handle_arrived_msgs(struct shmem_ch_cb *cb, int client_id)
{
	int i, handled;
	struct shmem_server_state *server;
	struct shmem_msgbuf_ctx *mb_ctx;
	struct shmem_client_state *client;

	server = cb->server_state;
	client = server->clients[client_id];

	handled = 0;

	// Check msgbuf flags to find out whether a message arrived.
	for (i = 0; i < cb->msgbuf_cnt; i++) {
		mb_ctx = &client->buf_ctxs[i];
		if (mb_ctx->evt->server_evt) { // msg arrived.
			log_debug(
				"[msgbuf] Client %d has a message in msgbuf %d",
				client_id, i);
			handle_client_msg(cb, client, i);
			// clear flag.
			mb_ctx->evt->server_evt = 0;
			handled++;
		}
	}

	return handled;
}

static void *handle_event(void *arg)
{
	struct shmem_ch_cb *cb;
	struct shmem_server_state *server;
	bit_index_t cur, next;
	int handled, handled_total;

	cb = (struct shmem_ch_cb *)arg;
	server = cb->server_state;

	while (1) {
		pthread_testcancel();

		// Producer will post sem.
		sem_wait(server->cq_sem);

		// Lookup client bitmap.
		cur = 0;
		next = 0;
		handled_total = 0;

		// Read only. No locking required.
		while (bit_array_find_next_set_bit(server->client_bitmap, cur,
						   &next)) {
			log_debug("[Event handler] Client %d is registered.",
				  next);

			// Handle incoming message.
			handled = handle_arrived_msgs(cb, next);
			handled_total += handled;
			cur = next + 1;
			log_debug(
				"[Event handler] Handled %d events of Client %d.",
				handled, next);
		}
		log_debug("[Event handler] Total %d events handled.",
			  handled_total);
	}

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
	assert(server->cq_cb_id > MAX_CLIENT_CONNECTION);

	// Create CQ shmem for per server cq event thread.
	server->cq_key = generate_shm_key(server->cq_cb_id);
	server->cq_shmem_id =
		create_shm_seg(server->cq_key,
			       sizeof(sem_t)); // Rounded up to PAGESIZE.
	if (server->cq_shmem_id == -1) {
		log_error("Getting client's shmem(shmget) failed.");
		goto err1;
	}

	server->cq_shmem_addr = attach_shm_seg(server->cq_shmem_id);
	if (!server->cq_shmem_addr) {
		log_error("Attaching server's shmem(shmat) failed.");
		goto err2;
	}

	// Locate semaphore in the shared memory.
	server->cq_sem = (sem_t *)server->cq_shmem_addr;
	pshared = 1;
	sem_init(server->cq_sem, pshared, 0);

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

	return;
err4:
	pthread_cancel(server->ehthread);
	pthread_join(server->ehthread, NULL);
err3:
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

	cb->server = attr->server;
	cb->msgbuf_cnt = attr->msgbuf_cnt;
	cb->msgheader_size = msgheader_size();
	cb->msgdata_size = attr->msgdata_size;
	cb->msgbuf_size = msgbuf_size(attr->msgdata_size);
	cb->rpc_msg_handler_cb = attr->rpc_msg_handler_cb;
	cb->user_msg_handler_cb = attr->user_msg_handler_cb;
	cb->msg_handler_thpool = attr->msg_handler_thpool;
	strcpy(cb->cm_socket_name, attr->cm_socket_name);

	if (cb->server) {
		init_shmem_server(cb); // init cb->server_state
	} else {
		cb->buf_ctxs =
			calloc(cb->msgbuf_cnt, sizeof(struct shmem_msgbuf_ctx));
		if (!cb->buf_ctxs) {
			ret = -ENOMEM;
			log_error("calloc failed.");
			goto err1;
		}

		// req.client_cb_addr = &cb;
		ret = connect_to_shmem_server(cb, &cb->shm_key, &cb->shm_size,
					      &cb->cq_shm_key);
		if (ret) {
			log_error("connect to shmem server failed.");
			goto err2;
		}

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
	// TODO: Instead of this function, we require a function that detaches
	// shm and used by a client.

	// remove_shm_seg(cb->shmem_id);
	// TODO: Destroy client channel.
}
