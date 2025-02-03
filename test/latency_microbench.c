#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include "rpc.h"
#include "log.h"
#include "thpool.h"
#include "time_stats.h"

#define RDMA_PORT 7174
#define MSG_CNT 100000
#define LAT_PROFILE

#define SHM_KEY_SEED 9367 // arbitrary value.

struct bench_config {
	int tput; // Run throughput bench.
	int msg_size; // in byte.
	// int msg_buf_cnt;
	int server;
	int rdma;
	// int ring_buf_cnt;
	int server_thread_num;
	int client_thread_num;
	char server_ip_addr[20];
};

char *g_shmem_path = "/tmp/oxbow-rpc_latency_bench";

struct rpc_ch_info *g_rpc_cli_ch = NULL;
struct bench_config g_conf = { 0 };
struct time_stats g_stat;

#ifdef LAT_PROFILE
struct time_stats polling_stat;
struct time_stats server_stat;
#endif

threadpool server_handler_thpool;
threadpool client_handler_thpool; // Required when RDMA channel is used.
threadpool client_sender_thpool;

void print_configs()
{
	printf("------------ Configs ------------\n");
	printf("msg_size : %d\n", g_conf.msg_size);
	if (g_conf.server) {
		printf("server or client : server\n");
	} else {
		printf("server or client : client\n");
		printf("server ip address : %s\n", g_conf.server_ip_addr);
	}

	if (g_conf.rdma) {
		printf("channel type : rdma\n");
	} else {
		printf("channel type : shmem\n");
	}
	printf("client_thread_num : %d\n", g_conf.client_thread_num);
	printf("server_thread_num : %d\n", g_conf.server_thread_num);
	printf("---------------------------------\n");
}

void parse_opts(int argc, char **argv)
{
	int opt;

	while ((opt = getopt(argc, argv, "b:c:i:m:n:rst:h")) != -1) {
		// -1 means getopt() parse all options
		switch (opt) {
		case 'b': // # of message buffers.
			printf(" -b is not supported. It is ignored.\n");
			;
			break;

		case 'c': // circular buffer size.
			printf(" -c is not supported. It is ignored.\n");
			;
			break;

		case 'i': // # of client threads.
			strcpy(g_conf.server_ip_addr, optarg);
			break;

		case 'm': // size of messages
			g_conf.msg_size = atoi(optarg);
			if (g_conf.msg_size < 64) {
				log_error(
					"Message size (-m) should be larger than 64.");
				exit(1);
			}

			break;

		case 'n': // # of client threads.
			g_conf.client_thread_num = atoi(optarg);
			break;

		case 'p': // Run throughput bench. (Otherwise, run latency bench.)
			g_conf.tput = 1;
			break;

		case 'r': // use rdma channel.
			g_conf.rdma = 1;
			break;

		case 's': // is server.
			g_conf.server = 1;
			break;

		case 't': // # of server worker threads.
			g_conf.server_thread_num = atoi(optarg);
			break;

		case 'h':
			printf("Usage: %s [options]\n", argv[1]);
			printf(" -i : server ip address. (required by client)\n");
			printf(" -m <size> : message size in byte. (It should be larger than 64. default : 64)\n");
			printf(" -n <num> : # of client threads. (Both requester and handler threads. default : 1)\n");
			printf(" -p : run throughput bench. (w/o this option : run latency bench, client option)\n");
			printf(" -r : use RDMA channel. (w/o this option : SHMEM channel)\n");
			printf(" -s : run as a server. (w/o this option : client)\n");
			printf(" -t <num> : # of server threads. (default : 1)\n");

			exit(1);
			break;

		case '?':
			printf("Unknown option : %c", optopt);
			break;
		}
	}
}

void server_msg_handler(void *arg)
{
	struct msg_handler_param *param;
	struct rpc_msg *msg;
	struct rpc_ch_info rpc_ch = { 0 };
	struct rpc_resp_param resp_param = { 0 };
	char *send_buf;

#ifdef LAT_PROFILE
	time_stats_start(&server_stat);
#endif
	send_buf = calloc(1, g_conf.msg_size);

	param = (struct msg_handler_param *)arg;
	msg = param->msg;

	log_debug(
		"[RPC server] received from Client %d: seqn=%lu sem_addr(rdma channel only)=%lx data=%s",
		param->client_id, msg->header.seqn, (uint64_t)msg->header.sem,
		msg->data);

	rpc_ch.ch_cb = param->ch_cb;
	rpc_ch.msgbuf_bitmap = NULL;

	memcpy(send_buf, msg->data, g_conf.msg_size);

	/* Send reply to the client. */
	if (g_conf.rdma) {
		// ch_cb is passed as a parameter because it is different for each client.
		rpc_ch.ch_type = RPC_CH_RDMA;
		resp_param = (struct rpc_resp_param){
			.rpc_ch = &rpc_ch,
			.client_rpc_ch_addr = msg->header.client_rpc_ch,
			.data = send_buf,
			.sem = (sem_t *)msg->header.sem,
			.client_id = 0, // Not used.
			.msgbuf_id = param->msgbuf_id,
			.seqn = msg->header.seqn
		};
		send_rpc_response_to_client(&resp_param);

	} else { // shmem
		rpc_ch.ch_type = RPC_CH_SHMEM;
		resp_param = (struct rpc_resp_param){
			.rpc_ch = &rpc_ch,
			.client_rpc_ch_addr = msg->header.client_rpc_ch,
			.data = send_buf,
			.sem = NULL, // Not used.
			.client_id = param->client_id,
			.msgbuf_id = param->msgbuf_id,
			.seqn = msg->header.seqn
		};
		send_rpc_response_to_client(&resp_param);
	}
#ifdef LAT_PROFILE
	time_stats_stop(&server_stat);
#endif

	free(msg);
	free(param);
}

void run_server(void)
{
	int ret;

#ifdef LAT_PROFILE
	time_stats_init(&server_stat, MSG_CNT);
#endif

	server_handler_thpool =
		thpool_init(g_conf.server_thread_num, "serv_handler");

	if (g_conf.rdma) {
		ret = init_rpc_server(RPC_CH_RDMA, NULL, RDMA_PORT,
				      g_conf.msg_size, server_msg_handler,
				      server_handler_thpool, NULL, NULL, NULL,
				      NULL, NULL);
	} else {
		ret = init_rpc_server(RPC_CH_SHMEM, g_shmem_path, 0,
				      g_conf.msg_size, server_msg_handler,
				      server_handler_thpool, NULL, NULL, NULL,
				      NULL, SHM_KEY_SEED);
	}

	if (ret) {
		log_error("init rpc server failed. ret=%d", ret);
		exit(1);
	}

	while (1) {
		sleep(1000);
	}

	thpool_wait(server_handler_thpool);
	thpool_destroy(server_handler_thpool);

	printf("Bye.");
}

// Called in a worker thread.
void client_rdma_msg_handler(void *arg)
{
	struct msg_handler_param *param;
	struct rpc_msg *msg;
	sem_t *sem;

	param = (struct msg_handler_param *)arg;
	msg = param->msg;

	sem = (sem_t *)param->msg->header.sem;
	log_debug("(RPC rdma client) received: seqn=%lu sem_addr=%lx data=%s\n",
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

	log_debug("(RPC shmem client) received: seqn=%lu data=%s",
		  msg->header.seqn, msg->data);
}

void init_client(void)
{
	client_handler_thpool =
		thpool_init(g_conf.client_thread_num, "cli_handler");
	client_sender_thpool =
		thpool_init(g_conf.client_thread_num, "cli_sender");

	if (g_conf.rdma) {
		g_rpc_cli_ch =
			init_rpc_client(RPC_CH_RDMA, g_conf.server_ip_addr,
					RDMA_PORT, g_conf.msg_size,
					client_rdma_msg_handler,
					client_handler_thpool, 0);
		log_info("Client is connected to server.");
	} else {
		g_rpc_cli_ch =
			init_rpc_client(RPC_CH_SHMEM, g_shmem_path, 0,
					g_conf.msg_size,
					rpc_shmem_client_handler,
					client_handler_thpool, SHM_KEY_SEED);
		log_info("Client is connected to server.");
	}

	if (g_rpc_cli_ch == NULL) {
		printf("init_rpc_client failed.\n");
		exit(1);
	}
}

void send_msg(void *arg)
{
	char *send_buf;
	unsigned long i;
	sem_t sem;
	int msgbuf_id;
	struct rpc_req_param req_param;

	i = (unsigned long)arg;

	send_buf = calloc(1, g_conf.msg_size);
	sprintf(send_buf, "%lu", i);

	if (!g_conf.tput)
		time_stats_start(&g_stat);

	if (g_conf.rdma) {
		// Init sema.
		sem_init(&sem, 0, 0);

		// Set param.
		req_param = (struct rpc_req_param){ .rpc_ch = g_rpc_cli_ch,
						    .data = send_buf,
						    .sem = &sem };

		// Send message to server.
		log_info("Sending RPC message (%lu): %s", i, send_buf);
		send_rpc_msg_to_server(&req_param);

		log_info("Waiting server response.");
		sem_wait(&sem);
		log_info("Resume the main thread.");

	} else {
		// Set param.
		req_param = (struct rpc_req_param){
			.rpc_ch = g_rpc_cli_ch,
			.data = send_buf,
			.sem = NULL // Not used.
		};

		// Send message to server.
		log_info("Sending RPC message (%lu): %s", i, send_buf);
		msgbuf_id = send_rpc_msg_to_server(&req_param);

		log_info("Waiting server response.");

#ifdef LAT_PROFILE
		time_stats_start(&polling_stat);
#endif

		wait_rpc_shmem_response(g_rpc_cli_ch, msgbuf_id, 1);

#ifdef LAT_PROFILE
		time_stats_stop(&polling_stat);
#endif

		log_info("Resume the main thread.");
	}

	if (!g_conf.tput)
		time_stats_stop(&g_stat);

	free(send_buf);
}

void run_client(void)
{
	unsigned long i;

	if (g_conf.tput) {
		time_stats_init(&g_stat, 1);
		time_stats_start(&g_stat);
	} else {
		time_stats_init(&g_stat, MSG_CNT);
#ifdef LAT_PROFILE
		time_stats_init(&polling_stat, MSG_CNT);
#endif
	}

	for (i = 0; i < MSG_CNT; i++) {
		thpool_add_work(client_sender_thpool, send_msg, (void *)i);
	}

	thpool_wait(client_sender_thpool);
	thpool_wait(client_handler_thpool);

	if (g_conf.tput) {
		time_stats_stop(&g_stat);
	}
}

void destroy_client(void)
{
	thpool_destroy(client_sender_thpool);
	thpool_destroy(client_handler_thpool);
}

void report_client_stats(void)
{
	double tput_exe_time;
	if (g_conf.tput) {
		tput_exe_time = time_stats_get_avg(&g_stat);
		printf("----------------- Aggregated throughput\n");
		printf("%3.3f\n", (float)MSG_CNT / (float)tput_exe_time);

	} else {
		time_stats_print(&g_stat, "----------------- Average latency");
#ifdef LAT_PROFILE
		time_stats_print(&polling_stat,
				 "----------------- Average polling time");
#endif
	}
}

void report_server_stats(void)
{
	if (g_conf.tput) {
	} else {
#ifdef LAT_PROFILE
		time_stats_print(
			&server_stat,
			"----------------- Average server handler time");
#endif
	}
}

void signal_handler(int signum)
{
	if (g_conf.server) {
		report_server_stats();
	}

	raise(SIGTERM);
}

int main(int argc, char **argv)
{
	struct sigaction action;

	log_set_level(LOGC_ERROR);

	// Set default configs.
	g_conf.tput = 0;
	g_conf.msg_size = 64;
	g_conf.server = 0;
	g_conf.rdma = 0;
	g_conf.client_thread_num = 1;
	g_conf.server_thread_num = 1;

	parse_opts(argc, argv);

	memset(&action, 0, sizeof(action));
	if (g_conf.server) {
		action.sa_handler = signal_handler;
	}
	sigaction(SIGUSR1, &action, NULL);
	sigaction(SIGINT, &action, NULL);

	if (!g_conf.tput) {
		printf("There is one thread in the latency bench.\n");
		g_conf.client_thread_num = 1;
	}

	print_configs();

	if (g_conf.server) {
		run_server();
	} else {
		init_client();
		run_client();
		destroy_client();
		report_client_stats();
	}
}
