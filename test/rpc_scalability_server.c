#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdatomic.h>
#include "test_global.h"
#include "log.h"
#include "rpc.h"
#include "thpool.h"
#include "profiling.h"

// Test configuration
#define MESSAGES_PER_CLIENT 1000
#define MAX_CLIENTS 20
#define WARMUP_MESSAGES 100
#define FIXED_MSG_SIZE 64 // Fixed message size in bytes

// Global variables
threadpool handler_thpool;
enum rpc_channel_type ch_type;
volatile int running = 1;
volatile int test_started = 0;
volatile int warmup_complete = 0;

// Statistics
atomic_long total_messages_received;
atomic_long total_messages_sent;
atomic_long warmup_messages_received;
struct timespec test_start_time;
struct timespec test_end_time;
pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;

void signal_handler(int sig)
{
	running = 0;
	log_info("Received signal %d, shutting down server...", sig);
}

void server_msg_handler(void *arg)
{
	struct msg_handler_param *param;
	struct rpc_msg *msg;
	struct rpc_ch_info rpc_ch = { 0 };
	char data[FIXED_MSG_SIZE];
	struct rpc_resp_param resp_param;

	param = (struct msg_handler_param *)arg;
	msg = param->msg;

	// Create fixed-size response message (64B)
	memset(data, 0, FIXED_MSG_SIZE);
	snprintf(data, FIXED_MSG_SIZE, "ACK_RESPONSE_CLIENT_%d",
		 param->client_id);

	// Fill remaining bytes with pattern to ensure 64B usage
	int msg_len = strlen(data);
	if (msg_len < FIXED_MSG_SIZE - 1) {
		for (int j = msg_len; j < FIXED_MSG_SIZE - 1; j++) {
			data[j] = 'B' + (j % 26);
		}
		data[FIXED_MSG_SIZE - 1] = '\0';
	}

	// Check if this is a warmup message
	if (strncmp(msg->data, "WARMUP", 6) == 0) {
		atomic_fetch_add(&warmup_messages_received, 1);
		if (atomic_load(&warmup_messages_received) >= WARMUP_MESSAGES) {
			warmup_complete = 1;
			log_info("Warmup complete, starting actual test...");
		}
	} else if (strncmp(msg->data, "START", 5) == 0) {
		// Start test timing
		if (!test_started) {
			test_started = 1;
			clock_gettime(CLOCK_MONOTONIC, &test_start_time);
			log_info("Test started");
		}
	} else if (strncmp(msg->data, "END", 3) == 0) {
		// End test timing
		clock_gettime(CLOCK_MONOTONIC, &test_end_time);
		log_info("Test ended");
	} else if (strncmp(msg->data, "PRINT_STATS_REQUEST", 19) == 0) {
		// Print server performance statistics
		log_info(
			"Received stats request from client, printing performance statistics...");
		print_server_stats_manual();
	} else if (strncmp(msg->data, "RESET_STATS_REQUEST", 19) == 0) {
		// Reset server performance statistics
		log_info(
			"Received reset stats request from client, resetting performance statistics...");
		reset_profiling_stats();
	} else if (strncmp(msg->data, "TEST", 4) == 0) {
		// Regular test message
		atomic_fetch_add(&total_messages_received, 1);
	}

	// log_debug("[SERVER] received from Client %d: seqn=%lu data=%s",
	// 	  param->client_id, msg->header.seqn, msg->data);

	rpc_ch.ch_cb = param->ch_cb;
	rpc_ch.msgbuf_bitmap = NULL;

	// Send reply to the client
	switch (ch_type) {
	case RPC_CH_RDMA:
		rpc_ch.ch_type = RPC_CH_RDMA;
		resp_param = (struct rpc_resp_param){
			.rpc_ch = &rpc_ch,
			.client_rpc_ch_addr = msg->header.client_rpc_ch,
			.data = data,
			.sem = (sem_t *)msg->header.sem,
			.client_id = 0,
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
			.sem = NULL,
			.client_id = param->client_id,
			.msgbuf_id = param->msgbuf_id,
			.seqn = msg->header.seqn
		};
		send_rpc_response_to_client(&resp_param);
		break;
	}

	atomic_fetch_add(&total_messages_sent, 1);

	free(msg);
	free(param);
}

void print_statistics()
{
	if (!test_started) {
		log_info("Test not started yet");
		return;
	}

	double elapsed_time =
		(test_end_time.tv_sec - test_start_time.tv_sec) +
		(test_end_time.tv_nsec - test_start_time.tv_nsec) /
			1000000000.0;

	long messages_received = atomic_load(&total_messages_received);
	long messages_sent = atomic_load(&total_messages_sent);

	double throughput_received = messages_received / elapsed_time;
	double throughput_sent = messages_sent / elapsed_time;

	log_info("=== SERVER STATISTICS ===");
	log_info("Test duration: %.3f seconds", elapsed_time);
	log_info("Messages received: %ld", messages_received);
	log_info("Messages sent: %ld", messages_sent);
	log_info("Throughput (received): %.2f messages/sec",
		 throughput_received);
	log_info("Throughput (sent): %.2f messages/sec", throughput_sent);
	log_info("========================");
}

int main(int argc, char **argv)
{
	int ret = 0;

	if (argc < 2) {
		printf("Usage: %s [rdma|shmem]\n", argv[0]);
		return 1;
	}

	if (strcmp(argv[1], "rdma") == 0) {
		log_info("Channel type: RDMA");
		ch_type = RPC_CH_RDMA;
	} else if (strcmp(argv[1], "shmem") == 0) {
		log_info("Channel type: Shared memory");
		ch_type = RPC_CH_SHMEM;
	} else {
		printf("Usage: %s [rdma|shmem]\n", argv[0]);
		return 1;
	}

	// Setup signal handler
	signal(SIGINT, signal_handler);
	signal(SIGTERM, signal_handler);

	// Initialize thread pool with more workers for better scalability
	handler_thpool = thpool_init(8, "handler");

	log_info("Starting RPC scalability test server...");
	log_info(
		"Configuration: MAX_CLIENTS=%d, MESSAGES_PER_CLIENT=%d, MESSAGE_SIZE=%dB",
		MAX_CLIENTS, MESSAGES_PER_CLIENT, FIXED_MSG_SIZE);

	// Initialize RPC server
	switch (ch_type) {
	case RPC_CH_RDMA:
		ret = init_rpc_server(RPC_CH_RDMA, NULL, g_port,
				      MAX_MSG_DATA_SIZE, server_msg_handler,
				      handler_thpool, NULL, NULL, NULL, NULL,
				      NULL);
		break;
	case RPC_CH_SHMEM:
		ret = init_rpc_server(RPC_CH_SHMEM, g_shmem_path, 0,
				      MAX_MSG_DATA_SIZE, server_msg_handler,
				      handler_thpool, NULL, NULL, NULL, NULL,
				      SHM_KEY_SEED);
		break;
	}

	if (ret) {
		log_error("Failed to initialize RPC server. ret=%d", ret);
		return -1;
	}

	log_info("RPC server initialized successfully. Waiting for clients...");

	// Main server loop
	while (running) {
		sleep(1);

		// Print periodic statistics
		if (test_started && atomic_load(&total_messages_received) > 0) {
			static int last_print = 0;
			long current_messages =
				atomic_load(&total_messages_received);
			if (current_messages - last_print >= 1000) {
				log_info("Progress: %ld messages received",
					 current_messages);
				last_print = current_messages;
			}
		}
	}

	log_info("Shutting down server...");

	// Print final statistics
	print_statistics();

	// Cleanup
	thpool_wait(handler_thpool);
	thpool_destroy(handler_thpool);

	log_info("Server shutdown complete");

	return 0;
}