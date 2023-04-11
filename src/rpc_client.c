#include <stdio.h>
#include <stdlib.h>
#include "rpc.h"
#include "log.h"
#include "rdma.h"

/**
 * @brief Callback function of RPC layer. It frees RPC layer resources.
 * 
 * @param arg 
 */
void client_rpc_msg_handler(void *arg)
{
	struct rpc_msg_handler_param *rpc_pa;
	rpc_pa = (struct rpc_msg_handler_param *)arg;

	// Free msg buffer bitmap.
	free_msgbuf_id(rpc_pa->client_rpc_ch, rpc_pa->msgbuf_id);

	// Call user-defined callback.
	rpc_pa->msg_handler_cb((void *)rpc_pa->param);

	free(arg);
}

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
	rpc_ch->msgbuf_bitmap = bit_array_create(RPC_MSG_BUF_NUM);
	pthread_spin_init(&rpc_ch->msgbuf_bitmap_lock, PTHREAD_PROCESS_PRIVATE);

	// Print for test.
	bit_array_print(rpc_ch->msgbuf_bitmap, stdout);
	fputc('\n', stdout);

	is_server = 0;

	switch (ch_type) {
	case RPC_CH_RDMA:
		attr.server = is_server;
		attr.msgbuf_cnt = RPC_MSG_BUF_NUM;
		attr.msgbuf_size = RPC_MSG_BUF_SIZE;
		strcpy(attr.ip_addr, ip_addr);
		attr.port = port;
		attr.rpc_msg_handler_cb = client_rpc_msg_handler;
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
	pthread_spin_destroy(&rpc_ch->msgbuf_bitmap_lock);
	bit_array_free(rpc_ch->msgbuf_bitmap);
	free(rpc_ch);
}

/**
 * @brief Send message to server.
 * 
 * @param rpc_ch 
 * @param data 
 * @param sem It is used to wait until server's response (ack) arrives.
 */
void send_rpc_msg_to_server(struct rpc_ch_info *rpc_ch, char *data, sem_t *sem)
{
	int msgbuf_id;

	// Alloc a message buffer id.
	msgbuf_id = alloc_msgbuf_id(rpc_ch);

	switch (rpc_ch->ch_type) {
	case RPC_CH_RDMA:
		send_rdma_msg((struct rdma_ch_cb *)rpc_ch->ch_cb, rpc_ch, data,
			      sem, msgbuf_id);
		break;

	default:
		log_error("Invalid channel type for RPC.");
	}
}