#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "log.h"
#include "rpc.h"
#include "thpool.h"

void client_msg_handler(void *arg)
{
	struct msg_handler_param *param;
	struct rpc_msg *msg;
	struct rpc_ch_info rpc_ch;

	param = (struct msg_handler_param *)arg;
	msg = param->msg;

	log_debug(
		"(rdma_ch_CLIENT_test) received: seqn=%lu sem_addr=%lx data=%s\n",
		msg->seqn, (uint64_t)msg->sem, msg->data);

	// post sema.

	free(msg);
	free(param);
}

int main(int argc, char **argv)
{
	char *ip_addr = "192.168.14.113";
	char *data = "hello world";
	struct rpc_ch_info *rpc_cli_ch;
	sem_t sem;
	threadpool handler_thpool;
	int ret;

	// To get rid of unused parameter warning.
	argc = argc;
	argv = argv;

	sleep(1);

	handler_thpool = thpool_init(1);
	sem_init(&sem, 0, 0);

	rpc_cli_ch = init_rpc_client(RPC_CH_RDMA, ip_addr, 7174,
				     client_msg_handler, handler_thpool);

	if (rpc_cli_ch == NULL) {
		printf("init_rpc_client failed.\n");
		ret = -1;
		goto out;
	}

	// Send a message.
	send_rpc_msg(rpc_cli_ch, data, &sem);

	pause();

	destroy_rpc_client(rpc_cli_ch);
	ret = 0;
out:
	thpool_wait(handler_thpool);
	thpool_destroy(handler_thpool);

	return ret;
}