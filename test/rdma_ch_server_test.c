#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "log.h"
#include "rpc.h"
#include "thpool.h"
// #include "test_global.h"

threadpool handler_thpool;

void server_msg_handler(void *arg)
{
	struct msg_handler_param *param;
	struct rpc_msg *msg;
	struct rpc_ch_info rpc_ch;
	char *data = "Hello client. I received your message.";

	param = (struct msg_handler_param *)arg;
	msg = param->msg;

	log_debug(
		"(rdma_ch_SERVER_test) received: seqn=%lu sem_addr=%lx data=%s",
		msg->seqn, (uint64_t)msg->sem, msg->data);

	/* Send reply to the client. */
	// ch_cb is passed as a parameter because it is different for each client.
	rpc_ch.ch_cb = param->ch_cb;
	rpc_ch.ch_type = RPC_CH_RDMA;

	send_rpc_msg(&rpc_ch, data, (sem_t *)msg->sem);

	free(msg);
	free(param);
}

int main(int argc, char **argv)
{
	int ret;

	// To get rid of unused parameter warning.
	argc = argc;
	argv = argv;

	handler_thpool = thpool_init(1);

	// run rpc_server.
	ret = init_rpc_server(RPC_CH_RDMA, 7174, server_msg_handler,
			      handler_thpool);

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