/* Run the rpc server first. (rpc_server_test.c) */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "test_global.h"
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
void rpc_shmem_client_handler(void *arg)
{
	struct rpc_msg *msg;
	msg = (struct rpc_msg *)arg;

	log_debug("(shmem_ch_CLIENT_test) received: seqn=%lu data=%s",
		  msg->header.seqn, msg->data);
}

int main(int argc, char **argv)
{
	char *data = "hello world";
	struct rpc_ch_info *rpc_cli_ch;
	threadpool handler_thpool;
	int ret, i;
	int msgbuf_id[RPC_SHMEM_MSG_BUF_NUM];
	enum rpc_channel_type ch_type;
	sem_t sem;
	struct rpc_req_param req_param;

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

	switch (ch_type) {
	case RPC_CH_RDMA:
		rpc_cli_ch = init_rpc_client(RPC_CH_RDMA, g_ip_addr, g_port,
					     MAX_MSG_DATA_SIZE,
					     client_rdma_msg_handler,
					     handler_thpool, 0);
		log_info("Client is connected to server.");

		// Send a message.
		sem_init(&sem, 0, 0);

		// Set param.
		req_param = (struct rpc_req_param){ .rpc_ch = rpc_cli_ch,
						    .data = data,
						    .sem = &sem };

		log_info("Sending RPC message:%s", data);
		send_rpc_msg_to_server(&req_param);

		log_info("Waiting server response.");
		sem_wait(&sem);
		log_info("Resume the main thread.");

		break;
	case RPC_CH_SHMEM:
		rpc_cli_ch = init_rpc_client(RPC_CH_SHMEM, g_shmem_path, 0,
					     MAX_MSG_DATA_SIZE,
					     rpc_shmem_client_handler,
					     handler_thpool, SHM_KEY_SEED);
		log_info("Client is connected to server.");

		// Set param.
		req_param = (struct rpc_req_param){ .rpc_ch = rpc_cli_ch,
						    .data = data,
						    .sem = NULL };

		// Send messages (consume msg buffers).
		for (i = 0; i < RPC_SHMEM_MSG_BUF_NUM + 1; i++) {
			log_info("Sending RPC message:%s", data);
			msgbuf_id[i] = send_rpc_msg_to_server(&req_param);
			log_info("msgbuf_id[%d]=%d", i, msgbuf_id);
		}

		log_info("All RPC messages send.");
		sleep(3);

		// Free msg buffers.
		for (i = 0; i < RPC_SHMEM_MSG_BUF_NUM + 1; i++) {
			log_info("Waiting server response. i=%d", i);
			wait_rpc_shmem_response(rpc_cli_ch, msgbuf_id[i], 1);
		}
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