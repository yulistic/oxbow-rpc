#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "log.h"
#include "rpc.h"
#include "thpool.h"

// Called in a worker thread.
void client_rdma_msg_handler(void *arg)
{
	struct msg_handler_param *param;
	struct rpc_msg *msg;
	sem_t *sem;

	param = (struct msg_handler_param *)arg;
	msg = param->msg;

	sem = (sem_t *)param->msg->header.sem;
	log_debug(
		"(rdma_ch_CLIENT_test) received: seqn=%lu sem_addr=%lx data=%s\n",
		msg->header.seqn, (uint64_t)msg->header.sem, msg->data);

	// post sema to resume the requesting thread.
	sem_post(sem);

	free(msg);
	free(param);
}

// Called in the requester thread.
void client_shmem_msg_handler(void *arg)
{
	struct rpc_msg *msg;
	msg = (struct rpc_msg *)arg;

	log_debug("(shmem_ch_CLIENT_test) received: seqn=%lu data=%s",
		  msg->header.seqn, msg->data);
}

int main(int argc, char **argv)
{
	char *ip_addr = "192.168.14.113";
	char *data = "hello world";
	struct rpc_ch_info *rpc_cli_ch;
	threadpool handler_thpool;
	int ret, msgbuf_id;
	enum rpc_channel_type ch_type;
	sem_t sem;

	// To get rid of unused parameter warning.
	argc = argc;
	argv = argv;

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

	switch (ch_type) {
	case RPC_CH_RDMA:
		rpc_cli_ch = init_rpc_client(RPC_CH_RDMA, ip_addr, 7174,
					     client_rdma_msg_handler,
					     handler_thpool);
		log_info("Client is connected to server.");

		// Send a message.
		sem_init(&sem, 0, 0);
		log_info("Sending RPC message:%s", data);
		send_rpc_msg_to_server(rpc_cli_ch, data, &sem);

		log_info("Waiting server response.");
		sem_wait(&sem);
		log_info("Resume the main thread.");

		break;
	case RPC_CH_SHMEM:
		rpc_cli_ch = init_rpc_client(
			RPC_CH_SHMEM, "/tmp/rpc_test_cm", 0,
			client_shmem_msg_handler,
			NULL); // handler thpool is not required.
		log_info("Client is connected to server.");

		// Send a message.
		log_info("Sending RPC message:%s", data);
		msgbuf_id = send_rpc_msg_to_server(rpc_cli_ch, data, NULL);

		log_info("Waiting server response.");
		wait_rpc_shmem_response(rpc_cli_ch, msgbuf_id);
		log_info("Resume the main thread.");

		break;
	}

	if (rpc_cli_ch == NULL) {
		printf("init_rpc_client failed.\n");
		ret = -1;
		goto out;
	}

	destroy_rpc_client(rpc_cli_ch);
	ret = 0;
out:
	thpool_wait(handler_thpool);
	thpool_destroy(handler_thpool);

	return ret;
}