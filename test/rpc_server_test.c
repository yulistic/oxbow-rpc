#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "log.h"
#include "rpc.h"
#include "thpool.h"
// #include "test_global.h"

threadpool handler_thpool;
enum rpc_channel_type ch_type;

void server_msg_handler(void *arg)
{
	struct msg_handler_param *param;
	struct rpc_msg *msg;
	struct rpc_ch_info rpc_ch = { 0 };
	char *data = "Hello client. I received your message.";

	param = (struct msg_handler_param *)arg;
	msg = param->msg;

	log_debug(
		"[rpc_ch_SERVER_test] received from Client %d: seqn=%lu sem_addr=%lx data=%s",
		param->client_id, msg->header.seqn, (uint64_t)msg->header.sem,
		msg->data);

	rpc_ch.ch_cb = param->ch_cb;
	rpc_ch.msgbuf_bitmap = NULL;

	/* Send reply to the client. */
	switch (ch_type) {
	case RPC_CH_RDMA:
		// ch_cb is passed as a parameter because it is different for each client.
		rpc_ch.ch_type = RPC_CH_RDMA;

		send_rpc_response_to_client(&rpc_ch, msg->header.client_rpc_ch,
					    data, (sem_t *)msg->header.sem, 0,
					    param->msgbuf_id, msg->header.seqn);
		break;
	case RPC_CH_SHMEM:
		rpc_ch.ch_type = RPC_CH_SHMEM;
		send_rpc_response_to_client(&rpc_ch, msg->header.client_rpc_ch,
					    data, (sem_t *)msg->header.sem,
					    param->client_id, param->msgbuf_id,
					    msg->header.seqn);
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

	handler_thpool = thpool_init(1);

	// run rpc_server.
	switch (ch_type) {
	case RPC_CH_RDMA:
		ret = init_rpc_server(RPC_CH_RDMA, NULL, 7174,
				      server_msg_handler, handler_thpool);
		break;
	case RPC_CH_SHMEM:
		ret = init_rpc_server(RPC_CH_SHMEM, "/tmp/rpc_test_cm", 0,
				      server_msg_handler, handler_thpool);
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