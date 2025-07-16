#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/wait.h>
#include "test_global.h"
#include "log.h"
#include "rpc.h"
#include "thpool.h"
#include "profiling.h"

// Test configuration
#define MESSAGES_PER_CLIENT 1000
#define MAX_CLIENTS 20
#define WARMUP_MESSAGES 100
#define TEST_DURATION_SEC 10
#define FIXED_MSG_SIZE 64 // Fixed message size in bytes

// Global variables
enum rpc_channel_type ch_type;
volatile int running = 1;
volatile int test_complete = 0;

// Statistics
atomic_long total_messages_sent;
atomic_long total_messages_received;
atomic_long total_response_time_ns;

struct client_stats {
	int client_id;
	long messages_sent;
	long messages_received;
	double avg_response_time_ms;
	double throughput_sent;
	double throughput_received;
};

struct client_thread_param {
	int client_id;
	int num_messages;
	struct client_stats *stats;
	struct rpc_ch_info *rpc_ch;
	threadpool handler_thpool;
	pthread_mutex_t *stats_mutex;
};

// RDMA client message handler
void client_rdma_msg_handler(void *arg)
{
	struct msg_handler_param *param;
	struct rpc_msg *msg;
	sem_t *sem;

	param = (struct msg_handler_param *)arg;
	msg = param->msg;

	sem = (sem_t *)param->msg->header.sem;
	log_debug("(CLIENT) received: seqn=%lu data=%s", msg->header.seqn,
		  msg->data);

	atomic_fetch_add(&total_messages_received, 1);

	// Signal the requesting thread
	sem_post(sem);

	free(msg);
	free(param);
}

// SHMEM client message handler
void client_shmem_msg_handler(void *arg)
{
	struct rpc_msg *msg;
	msg = (struct rpc_msg *)arg;

	// log_debug("(CLIENT) received: seqn=%lu data=%s", msg->header.seqn,
	// 	  msg->data);

	atomic_fetch_add(&total_messages_received, 1);
}

void *client_thread_func(void *arg)
{
	struct client_thread_param *param = (struct client_thread_param *)arg;
	struct rpc_ch_info *rpc_ch = param->rpc_ch;
	char msg_data[FIXED_MSG_SIZE];
	int i;
	struct timespec start_time, end_time;
	long total_latency_ns = 0;
	int messages_sent = 0;

	log_info("Client %d thread started", param->client_id);

	clock_gettime(CLOCK_MONOTONIC, &start_time);

	for (i = 0; i < param->num_messages && running; i++) {
		struct timespec msg_start, msg_end;
		clock_gettime(CLOCK_MONOTONIC, &msg_start);

		// Create fixed-size message (64B)
		memset(msg_data, 0, FIXED_MSG_SIZE);
		snprintf(msg_data, FIXED_MSG_SIZE, "TEST_MSG_CLIENT_%d_SEQ_%d",
			 param->client_id, i);

		// Fill remaining bytes with pattern to ensure 64B usage
		int msg_len = strlen(msg_data);
		if (msg_len < FIXED_MSG_SIZE - 1) {
			for (int j = msg_len; j < FIXED_MSG_SIZE - 1; j++) {
				msg_data[j] = 'A' + (j % 26);
			}
			msg_data[FIXED_MSG_SIZE - 1] = '\0';
		}

		switch (ch_type) {
		case RPC_CH_RDMA: {
			sem_t sem;
			sem_init(&sem, 0, 0);

			struct rpc_req_param req_param = { .rpc_ch = rpc_ch,
							   .data = msg_data,
							   .sem = &sem };

			send_rpc_msg_to_server(&req_param);
			atomic_fetch_add(&total_messages_sent, 1);
			messages_sent++;

			// Wait for response
			sem_wait(&sem);

			clock_gettime(CLOCK_MONOTONIC, &msg_end);
			long latency_ns = (msg_end.tv_sec - msg_start.tv_sec) *
						  1000000000L +
					  (msg_end.tv_nsec - msg_start.tv_nsec);
			total_latency_ns += latency_ns;

			sem_destroy(&sem);
		} break;

		case RPC_CH_SHMEM: {
			struct rpc_req_param req_param = { .rpc_ch = rpc_ch,
							   .data = msg_data,
							   .sem = NULL };

			int msgbuf_id = send_rpc_msg_to_server(&req_param);
			atomic_fetch_add(&total_messages_sent, 1);
			messages_sent++;

			// Wait for response
			wait_rpc_shmem_response(rpc_ch, msgbuf_id, 1);

			clock_gettime(CLOCK_MONOTONIC, &msg_end);
			long latency_ns = (msg_end.tv_sec - msg_start.tv_sec) *
						  1000000000L +
					  (msg_end.tv_nsec - msg_start.tv_nsec);
			total_latency_ns += latency_ns;
		} break;
		}

		// Small delay to avoid overwhelming the server
		usleep(1000); // 1ms
	}

	clock_gettime(CLOCK_MONOTONIC, &end_time);

	double elapsed_time =
		(end_time.tv_sec - start_time.tv_sec) +
		(end_time.tv_nsec - start_time.tv_nsec) / 1000000000.0;

	// Update statistics
	pthread_mutex_lock(param->stats_mutex);
	param->stats->client_id = param->client_id;
	param->stats->messages_sent = messages_sent;
	param->stats->messages_received =
		messages_sent; // Assuming all messages get responses
	param->stats->avg_response_time_ms =
		(double)total_latency_ns / messages_sent / 1000000.0;
	param->stats->throughput_sent = messages_sent / elapsed_time;
	param->stats->throughput_received = messages_sent / elapsed_time;
	pthread_mutex_unlock(param->stats_mutex);

	log_info(
		"Client %d completed: %d messages, %.2f msgs/sec, %.2f ms avg latency",
		param->client_id, messages_sent, param->stats->throughput_sent,
		param->stats->avg_response_time_ms);

	return NULL;
}

void run_scalability_test(int num_clients)
{
	pthread_t *client_threads;
	struct client_thread_param *thread_params;
	struct client_stats *client_stats;
	struct rpc_ch_info **rpc_clients;
	threadpool *handler_thpools;
	pthread_mutex_t stats_mutex = PTHREAD_MUTEX_INITIALIZER;
	int i;
	struct timespec test_start, test_end;

	// Reset profiling statistics for this test
	reset_profiling_stats();

	log_info("Starting scalability test with %d clients", num_clients);

	// Allocate memory for client data
	client_threads = malloc(num_clients * sizeof(pthread_t));
	thread_params =
		malloc(num_clients * sizeof(struct client_thread_param));
	client_stats = malloc(num_clients * sizeof(struct client_stats));
	rpc_clients = malloc(num_clients * sizeof(struct rpc_ch_info *));
	handler_thpools = malloc(num_clients * sizeof(threadpool));

	// Initialize clients
	for (i = 0; i < num_clients; i++) {
		handler_thpools[i] = thpool_init(1, "client_handler");

		switch (ch_type) {
		case RPC_CH_RDMA:
			rpc_clients[i] =
				init_rpc_client(RPC_CH_RDMA, g_ip_addr, g_port,
						MAX_MSG_DATA_SIZE,
						client_rdma_msg_handler,
						handler_thpools[i], 0);
			break;

		case RPC_CH_SHMEM:
			rpc_clients[i] = init_rpc_client(
				RPC_CH_SHMEM, g_shmem_path, 0,
				MAX_MSG_DATA_SIZE, client_shmem_msg_handler,
				handler_thpools[i], SHM_KEY_SEED);
			break;

		default:
			log_error("Invalid channel type.");
			exit(1);
		}

		if (!rpc_clients[i]) {
			log_error("Failed to initialize RPC client %d", i);
			exit(1);
		}

		// Initialize client stats
		client_stats[i].client_id = i;
		client_stats[i].messages_sent = 0;
		client_stats[i].messages_received = 0;
		client_stats[i].avg_response_time_ms = 0.0;
		client_stats[i].throughput_sent = 0.0;
		client_stats[i].throughput_received = 0.0;
	}

	// Send reset stats request to server using the first client connection
	if (num_clients > 0 && rpc_clients[0]) {
		char reset_msg[] = "RESET_STATS_REQUEST";
		struct rpc_req_param reset_req = { .rpc_ch = rpc_clients[0],
						   .data = reset_msg };
		send_rpc_msg_to_server(&reset_req);

		// Give server time to process reset
		usleep(100000); // 100ms
		log_info("Sent reset stats request to server using client 0");

		// To free msgbuf.
		wait_rpc_shmem_response(rpc_clients[0], 0, 0);
	}

	// Reset statistics
	atomic_store(&total_messages_sent, 0);
	atomic_store(&total_messages_received, 0);
	atomic_store(&total_response_time_ns, 0);

	// Start test timing
	clock_gettime(CLOCK_MONOTONIC, &test_start);

	// Create client threads
	for (i = 0; i < num_clients; i++) {
		thread_params[i].client_id = i;
		thread_params[i].num_messages = MESSAGES_PER_CLIENT;
		thread_params[i].stats = &client_stats[i];
		thread_params[i].rpc_ch = rpc_clients[i];
		thread_params[i].handler_thpool = handler_thpools[i];
		thread_params[i].stats_mutex = &stats_mutex;

		pthread_create(&client_threads[i], NULL, client_thread_func,
			       &thread_params[i]);
	}

	// Wait for all client threads to complete
	for (i = 0; i < num_clients; i++) {
		pthread_join(client_threads[i], NULL);
	}

	clock_gettime(CLOCK_MONOTONIC, &test_end);

	// Calculate and print results
	double total_elapsed_time =
		(test_end.tv_sec - test_start.tv_sec) +
		(test_end.tv_nsec - test_start.tv_nsec) / 1000000000.0;

	long total_sent = atomic_load(&total_messages_sent);
	long total_received = atomic_load(&total_messages_received);

	double overall_throughput = total_sent / total_elapsed_time;

	printf("SCALABILITY_TEST_RESULT,%d,%.3f,%ld,%ld,%.2f\n", num_clients,
	       total_elapsed_time, total_sent, total_received,
	       overall_throughput);

	log_info("=== SCALABILITY TEST RESULTS ===");
	log_info("Clients: %d", num_clients);
	log_info("Total time: %.3f seconds", total_elapsed_time);
	log_info("Messages sent: %ld", total_sent);
	log_info("Messages received: %ld", total_received);
	log_info("Overall throughput: %.2f messages/sec", overall_throughput);

	// Print per-client statistics
	printf("CLIENT_STATS,ClientID,MessagesSent,MessagesReceived,AvgLatency(ms),Throughput(msgs/sec)\n");
	for (i = 0; i < num_clients; i++) {
		printf("CLIENT_STATS,%d,%ld,%ld,%.2f,%.2f\n",
		       client_stats[i].client_id, client_stats[i].messages_sent,
		       client_stats[i].messages_received,
		       client_stats[i].avg_response_time_ms,
		       client_stats[i].throughput_sent);
	}

	// Send stats request to server after test completion
	if (num_clients > 0 && rpc_clients[0]) {
		char stats_msg[] = "PRINT_STATS_REQUEST";

		switch (ch_type) {
		case RPC_CH_RDMA: {
			sem_t sem;
			sem_init(&sem, 0, 0);
			struct rpc_req_param req_param = {
				.rpc_ch = rpc_clients[0],
				.data = stats_msg,
				.sem = &sem
			};
			send_rpc_msg_to_server(&req_param);
			sem_wait(&sem);
			sem_destroy(&sem);
		} break;

		case RPC_CH_SHMEM: {
			struct rpc_req_param req_param = {
				.rpc_ch = rpc_clients[0],
				.data = stats_msg,
				.sem = NULL
			};
			int msgbuf_id = send_rpc_msg_to_server(&req_param);
			wait_rpc_shmem_response(rpc_clients[0], msgbuf_id, 1);
		} break;
		}

		log_info("Stats request sent to server for %d clients test",
			 num_clients);
	}

	// Cleanup
	for (i = 0; i < num_clients; i++) {
		destroy_rpc_client(rpc_clients[i]);
		thpool_destroy(handler_thpools[i]);
	}

	free(client_threads);
	free(thread_params);
	free(client_stats);
	free(rpc_clients);
	free(handler_thpools);

	log_info("Test with %d clients completed", num_clients);
}

int main(int argc, char **argv)
{
	int start_clients = 1;
	int end_clients = MAX_CLIENTS;
	int step = 1;

	if (argc < 2) {
		printf("Usage: %s [rdma|shmem] [start_clients] [end_clients] [step]\n",
		       argv[0]);
		printf("Example: %s rdma 1 10 2  (test 1,3,5,7,9 clients)\n",
		       argv[0]);
		return 1;
	}

	if (strcmp(argv[1], "rdma") == 0) {
		log_info("Channel type: RDMA");
		ch_type = RPC_CH_RDMA;
	} else if (strcmp(argv[1], "shmem") == 0) {
		log_info("Channel type: Shared memory");
		ch_type = RPC_CH_SHMEM;
	} else {
		printf("Usage: %s [rdma|shmem] [start_clients] [end_clients] [step]\n",
		       argv[0]);
		return 1;
	}

	if (argc >= 3)
		start_clients = atoi(argv[2]);
	if (argc >= 4)
		end_clients = atoi(argv[3]);
	if (argc >= 5)
		step = atoi(argv[4]);

	if (start_clients < 1 || end_clients > MAX_CLIENTS || step < 1) {
		printf("Invalid parameters: start_clients=%d, end_clients=%d, step=%d\n",
		       start_clients, end_clients, step);
		return 1;
	}

	log_info("Starting RPC scalability test client...");
	log_info(
		"Configuration: %d to %d clients (step %d), %d messages per client, %dB message size",
		start_clients, end_clients, step, MESSAGES_PER_CLIENT,
		FIXED_MSG_SIZE);

	// Print CSV header
	printf("SCALABILITY_TEST_RESULT,NumClients,Duration(sec),MessagesSent,MessagesReceived,Throughput(msgs/sec)\n");

	// Run tests with different number of clients
	for (int num_clients = start_clients; num_clients <= end_clients;
	     num_clients += step) {
		log_info("Running test with %d clients...", num_clients);
		run_scalability_test(num_clients);

		// Sleep between tests to let server recover and clean up clients
		sleep(3);
	}

	log_info("All scalability tests completed");

	return 0;
}