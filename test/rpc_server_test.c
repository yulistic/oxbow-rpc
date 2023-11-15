#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "test_global.h"
#include "log.h"
#include "rpc.h"
#include "thpool.h"

threadpool handler_thpool;
enum rpc_channel_type ch_type;

void server_msg_handler(void *arg)
{
	struct msg_handler_param *param;
	struct rpc_msg *msg;
	struct rpc_ch_info rpc_ch = { 0 };
	char *data = "Hello client. I received your message.";
	struct rpc_resp_param resp_param;

	param = (struct msg_handler_param *)arg;
	msg = param->msg;

	log_debug(
		"[rpc_ch_SERVER_test] received from Client %d: seqn=%lu sem_addr(rdma channel only)=%lx data=%s",
		param->client_id, msg->header.seqn, (uint64_t)msg->header.sem,
		msg->data);

	rpc_ch.ch_cb = param->ch_cb;
	rpc_ch.msgbuf_bitmap = NULL;

	/* Send reply to the client. */
	switch (ch_type) {
	case RPC_CH_RDMA:
		// ch_cb is passed as a parameter because it is different for each client.
		rpc_ch.ch_type = RPC_CH_RDMA;

		resp_param = (struct rpc_resp_param){
			.rpc_ch = &rpc_ch,
			.client_rpc_ch_addr = msg->header.client_rpc_ch,
			.data = data,
			.sem = (sem_t *)msg->header.sem,
			.client_id = 0, // Not used.
			.msgbuf_id = param->msgbuf_id,
			.seqn = msg->header.seqn
		};

		send_rpc_response_to_client(&resp_param);
		break;

	case RPC_CH_SHMEM:
		rpc_ch.ch_type = RPC_CH_SHMEM;

		resp_param = (struct rpc_resp_param){
			.rpc_ch = &rpc_ch,
			.client_rpc_ch_addr = msg->header.client_rpc_ch,
			.data = data,
			.sem = NULL, // Not used.
			.client_id = param->client_id,
			.msgbuf_id = param->msgbuf_id,
			.seqn = msg->header.seqn
		};

		send_rpc_response_to_client(&resp_param);
		break;
	}

	free(msg);
	free(param);
}

int main(int argc, char **argv)
{
	int ret;

	if (argc < 2) {
		printf("Usage: %s [rdma|shmem]\n", argv[0]);
		return 1;
	}

	if (strcmp(argv[1], "rdma") == 0) {
		log_info("Channel type : RDMA");
		ch_type = RPC_CH_RDMA;
	} else if (strcmp(argv[1], "shmem") == 0) {
		log_info("Channel type : Shared memory");
		ch_type = RPC_CH_SHMEM;
	} else {
		printf("Usage: %s [rdma|shmem]\n", argv[0]);
		return 1;
	}

	handler_thpool = thpool_init(1, "handler");

	// run rpc_server.
	switch (ch_type) {
	case RPC_CH_RDMA:
		ret = init_rpc_server(RPC_CH_RDMA, NULL, g_port,
				      MAX_MSG_DATA_SIZE, server_msg_handler,
				      handler_thpool);
		break;
	case RPC_CH_SHMEM:
		ret = init_rpc_server(RPC_CH_SHMEM, g_shmem_path, 0,
				      MAX_MSG_DATA_SIZE, server_msg_handler,
				      handler_thpool);
		break;
	}

	if (ret) {
		log_error("init rpc server failed. ret=%d", ret);
		return -1;
	}

	while (1) {
		sleep(1);
	}

	log_info("Exiting server.");

	thpool_wait(handler_thpool);
	thpool_destroy(handler_thpool);

	return 0;
}