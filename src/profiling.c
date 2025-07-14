#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include "profiling.h"
#include "log.h"

#if ENABLE_PROFILING

// Global profiling instances
struct server_profiling g_server_prof;
struct client_profiling g_client_prof;

void init_profiling(void)
{
	// Initialize server profiling
	memset(&g_server_prof, 0, sizeof(g_server_prof));

	strcpy(g_server_prof.msgbuf_scan.name, "MsgBuf Scan");
	strcpy(g_server_prof.msg_alloc.name, "Msg Alloc");
	strcpy(g_server_prof.msg_copy.name, "Msg Copy");
	strcpy(g_server_prof.msg_handler_dispatch.name, "Handler Dispatch");
	strcpy(g_server_prof.total_msg_processing.name, "Total Msg Processing");

	// Set initial min values to max
	atomic_store(&g_server_prof.msgbuf_scan.min_time_ns, ULONG_MAX);
	atomic_store(&g_server_prof.msg_alloc.min_time_ns, ULONG_MAX);
	atomic_store(&g_server_prof.msg_copy.min_time_ns, ULONG_MAX);
	atomic_store(&g_server_prof.msg_handler_dispatch.min_time_ns,
		     ULONG_MAX);
	atomic_store(&g_server_prof.total_msg_processing.min_time_ns,
		     ULONG_MAX);

	pthread_mutex_init(&g_server_prof.stats_lock, NULL);
	g_server_prof.last_stats_print_time = get_timestamp_ns();

	// Initialize client profiling
	memset(&g_client_prof, 0, sizeof(g_client_prof));

	strcpy(g_client_prof.msgbuf_alloc.name, "MsgBuf Alloc");
	strcpy(g_client_prof.msg_send.name, "Msg Send");
	strcpy(g_client_prof.response_wait.name, "Response Wait");
	strcpy(g_client_prof.total_rpc_time.name, "Total RPC Time");

	atomic_store(&g_client_prof.msgbuf_alloc.min_time_ns, ULONG_MAX);
	atomic_store(&g_client_prof.msg_send.min_time_ns, ULONG_MAX);
	atomic_store(&g_client_prof.response_wait.min_time_ns, ULONG_MAX);
	atomic_store(&g_client_prof.total_rpc_time.min_time_ns, ULONG_MAX);

	pthread_mutex_init(&g_client_prof.stats_lock, NULL);
	g_client_prof.last_stats_print_time = get_timestamp_ns();

	printf("[PROFILING] Performance profiling initialized\n");
}

void update_perf_stats(struct perf_stats *stats, uint64_t duration_ns)
{
	if (!stats)
		return;

	atomic_fetch_add(&stats->count, 1);
	atomic_fetch_add(&stats->total_time_ns, duration_ns);

	// Update min value atomically
	uint64_t current_min = atomic_load(&stats->min_time_ns);
	while (duration_ns < current_min) {
		if (atomic_compare_exchange_weak(&stats->min_time_ns,
						 &current_min, duration_ns)) {
			break;
		}
	}

	// Update max value atomically
	uint64_t current_max = atomic_load(&stats->max_time_ns);
	while (duration_ns > current_max) {
		if (atomic_compare_exchange_weak(&stats->max_time_ns,
						 &current_max, duration_ns)) {
			break;
		}
	}
}

static void print_perf_stats(struct perf_stats *stats)
{
	uint64_t count = atomic_load(&stats->count);
	if (count == 0) {
		printf("[PROF] %-20s: No data\n", stats->name);
		return;
	}

	uint64_t total_ns = atomic_load(&stats->total_time_ns);
	uint64_t min_ns = atomic_load(&stats->min_time_ns);
	uint64_t max_ns = atomic_load(&stats->max_time_ns);

	double avg_ms = ns_to_ms(total_ns / count);
	double min_ms = ns_to_ms(min_ns);
	double max_ms = ns_to_ms(max_ns);

	printf("[PROF] %-20s: count=%lu avg=%.3fms min=%.3fms max=%.3fms total=%.3fms\n",
	       stats->name, count, avg_ms, min_ms, max_ms, ns_to_ms(total_ns));
}

void print_server_stats(void)
{
	pthread_mutex_lock(&g_server_prof.stats_lock);

	uint64_t current_time = get_timestamp_ns();
	uint64_t time_diff_ns =
		current_time - g_server_prof.last_stats_print_time;
	double time_diff_sec = time_diff_ns / 1000000000.0;

	if (time_diff_sec < PROF_PRINT_INTERVAL_SEC) {
		pthread_mutex_unlock(&g_server_prof.stats_lock);
		return;
	}

	printf("=== SERVER PERFORMANCE STATISTICS ===\n");

	print_perf_stats(&g_server_prof.msgbuf_scan);
	print_perf_stats(&g_server_prof.msg_alloc);
	print_perf_stats(&g_server_prof.msg_copy);
	print_perf_stats(&g_server_prof.msg_handler_dispatch);
	print_perf_stats(&g_server_prof.total_msg_processing);

	uint64_t total_msgs =
		atomic_load(&g_server_prof.total_messages_processed);
	uint64_t active_clients = atomic_load(&g_server_prof.active_clients);
	double msgs_per_sec = total_msgs / time_diff_sec;

	printf("[PROF] Active clients: %lu\n", active_clients);
	printf("[PROF] Total messages: %lu\n", total_msgs);
	printf("[PROF] Messages/sec: %.2f\n", msgs_per_sec);
	printf("=====================================\n");

	g_server_prof.last_stats_print_time = current_time;

	pthread_mutex_unlock(&g_server_prof.stats_lock);
}

void print_server_stats_manual(void)
{
	pthread_mutex_lock(&g_server_prof.stats_lock);

	uint64_t current_time = get_timestamp_ns();
	uint64_t time_diff_ns =
		current_time - g_server_prof.last_stats_print_time;
	double time_diff_sec = time_diff_ns / 1000000000.0;

	// Use a large buffer to construct the entire output atomically
	char output_buffer[4096];
	int offset = 0;

	offset +=
		snprintf(output_buffer + offset, sizeof(output_buffer) - offset,
			 "=== SERVER PERFORMANCE STATISTICS (MANUAL) ===\n");

	// Helper macro to append performance stats to buffer
#define APPEND_PERF_STATS(stats)                                                                           \
	do {                                                                                               \
		uint64_t count = atomic_load(&(stats)->count);                                             \
		if (count > 0) {                                                                           \
			uint64_t total_ns =                                                                \
				atomic_load(&(stats)->total_time_ns);                                      \
			uint64_t min_ns = atomic_load(&(stats)->min_time_ns);                              \
			uint64_t max_ns = atomic_load(&(stats)->max_time_ns);                              \
                                                                                                           \
			double avg_ms = ns_to_ms(total_ns) / count;                                        \
			double min_ms = ns_to_ms(min_ns);                                                  \
			double max_ms = ns_to_ms(max_ns);                                                  \
			double total_ms = ns_to_ms(total_ns);                                              \
                                                                                                           \
			offset += snprintf(                                                                \
				output_buffer + offset,                                                    \
				sizeof(output_buffer) - offset,                                            \
				"[PROF] %-18s: count=%lu avg=%.3fms min=%.3fms max=%.3fms total=%.3fms\n", \
				(stats)->name, count, avg_ms, min_ms, max_ms,                              \
				total_ms);                                                                 \
		}                                                                                          \
	} while (0)

	APPEND_PERF_STATS(&g_server_prof.msgbuf_scan);
	APPEND_PERF_STATS(&g_server_prof.msg_alloc);
	APPEND_PERF_STATS(&g_server_prof.msg_copy);
	APPEND_PERF_STATS(&g_server_prof.msg_handler_dispatch);
	APPEND_PERF_STATS(&g_server_prof.total_msg_processing);

#undef APPEND_PERF_STATS

	uint64_t total_msgs =
		atomic_load(&g_server_prof.total_messages_processed);
	uint64_t active_clients = atomic_load(&g_server_prof.active_clients);

	// Use actual time difference for throughput calculation
	double msgs_per_sec =
		(time_diff_sec > 0.0) ? total_msgs / time_diff_sec : 0.0;

	offset +=
		snprintf(output_buffer + offset, sizeof(output_buffer) - offset,
			 "[PROF] Active clients: %lu\n", active_clients);
	offset +=
		snprintf(output_buffer + offset, sizeof(output_buffer) - offset,
			 "[PROF] Total messages: %lu\n", total_msgs);
	offset +=
		snprintf(output_buffer + offset, sizeof(output_buffer) - offset,
			 "[PROF] Messages/sec: %.2f\n", msgs_per_sec);
	offset +=
		snprintf(output_buffer + offset, sizeof(output_buffer) - offset,
			 "=============================================\n");

	// Output the entire buffer at once and flush immediately
	printf("%s", output_buffer);
	fflush(stdout);

	pthread_mutex_unlock(&g_server_prof.stats_lock);
}

void print_client_stats(void)
{
	pthread_mutex_lock(&g_client_prof.stats_lock);

	uint64_t current_time = get_timestamp_ns();
	uint64_t time_diff_ns =
		current_time - g_client_prof.last_stats_print_time;
	double time_diff_sec = time_diff_ns / 1000000000.0;

	if (time_diff_sec < PROF_PRINT_INTERVAL_SEC) {
		pthread_mutex_unlock(&g_client_prof.stats_lock);
		return;
	}

	printf("=== CLIENT PERFORMANCE STATISTICS ===\n");

	print_perf_stats(&g_client_prof.msgbuf_alloc);
	print_perf_stats(&g_client_prof.msg_send);
	print_perf_stats(&g_client_prof.response_wait);
	print_perf_stats(&g_client_prof.total_rpc_time);

	uint64_t total_requests =
		atomic_load(&g_client_prof.total_requests_sent);
	uint64_t total_responses =
		atomic_load(&g_client_prof.total_responses_received);
	double req_per_sec = total_requests / time_diff_sec;

	printf("[PROF] Total requests: %lu\n", total_requests);
	printf("[PROF] Total responses: %lu\n", total_responses);
	printf("[PROF] Requests/sec: %.2f\n", req_per_sec);
	printf("=====================================\n");

	g_client_prof.last_stats_print_time = current_time;

	pthread_mutex_unlock(&g_client_prof.stats_lock);
}

void print_client_stats_manual(void)
{
	pthread_mutex_lock(&g_client_prof.stats_lock);

	uint64_t current_time = get_timestamp_ns();
	uint64_t time_diff_ns =
		current_time - g_client_prof.last_stats_print_time;
	double time_diff_sec = time_diff_ns / 1000000000.0;

	printf("=== CLIENT PERFORMANCE STATISTICS (MANUAL) ===\n");

	print_perf_stats(&g_client_prof.msgbuf_alloc);
	print_perf_stats(&g_client_prof.msg_send);
	print_perf_stats(&g_client_prof.response_wait);
	print_perf_stats(&g_client_prof.total_rpc_time);

	uint64_t total_requests =
		atomic_load(&g_client_prof.total_requests_sent);
	uint64_t total_responses =
		atomic_load(&g_client_prof.total_responses_received);

	// Use actual time difference for throughput calculation
	double req_per_sec =
		(time_diff_sec > 0.0) ? total_requests / time_diff_sec : 0.0;

	printf("[PROF] Total requests: %lu\n", total_requests);
	printf("[PROF] Total responses: %lu\n", total_responses);
	printf("[PROF] Requests/sec: %.2f\n", req_per_sec);
	printf("==============================================\n");
	fflush(stdout);

	pthread_mutex_unlock(&g_client_prof.stats_lock);
}

void reset_profiling_stats(void)
{
	// Reset server stats
	pthread_mutex_lock(&g_server_prof.stats_lock);

	atomic_store(&g_server_prof.msgbuf_scan.count, 0);
	atomic_store(&g_server_prof.msgbuf_scan.total_time_ns, 0);
	atomic_store(&g_server_prof.msgbuf_scan.min_time_ns, ULONG_MAX);
	atomic_store(&g_server_prof.msgbuf_scan.max_time_ns, 0);

	atomic_store(&g_server_prof.msg_alloc.count, 0);
	atomic_store(&g_server_prof.msg_alloc.total_time_ns, 0);
	atomic_store(&g_server_prof.msg_alloc.min_time_ns, ULONG_MAX);
	atomic_store(&g_server_prof.msg_alloc.max_time_ns, 0);

	atomic_store(&g_server_prof.msg_copy.count, 0);
	atomic_store(&g_server_prof.msg_copy.total_time_ns, 0);
	atomic_store(&g_server_prof.msg_copy.min_time_ns, ULONG_MAX);
	atomic_store(&g_server_prof.msg_copy.max_time_ns, 0);

	atomic_store(&g_server_prof.msg_handler_dispatch.count, 0);
	atomic_store(&g_server_prof.msg_handler_dispatch.total_time_ns, 0);
	atomic_store(&g_server_prof.msg_handler_dispatch.min_time_ns,
		     ULONG_MAX);
	atomic_store(&g_server_prof.msg_handler_dispatch.max_time_ns, 0);

	atomic_store(&g_server_prof.total_msg_processing.count, 0);
	atomic_store(&g_server_prof.total_msg_processing.total_time_ns, 0);
	atomic_store(&g_server_prof.total_msg_processing.min_time_ns,
		     ULONG_MAX);
	atomic_store(&g_server_prof.total_msg_processing.max_time_ns, 0);

	atomic_store(&g_server_prof.total_messages_processed, 0);

	g_server_prof.last_stats_print_time = get_timestamp_ns();

	pthread_mutex_unlock(&g_server_prof.stats_lock);

	// Reset client stats
	pthread_mutex_lock(&g_client_prof.stats_lock);

	atomic_store(&g_client_prof.msgbuf_alloc.count, 0);
	atomic_store(&g_client_prof.msgbuf_alloc.total_time_ns, 0);
	atomic_store(&g_client_prof.msgbuf_alloc.min_time_ns, ULONG_MAX);
	atomic_store(&g_client_prof.msgbuf_alloc.max_time_ns, 0);

	atomic_store(&g_client_prof.msg_send.count, 0);
	atomic_store(&g_client_prof.msg_send.total_time_ns, 0);
	atomic_store(&g_client_prof.msg_send.min_time_ns, ULONG_MAX);
	atomic_store(&g_client_prof.msg_send.max_time_ns, 0);

	atomic_store(&g_client_prof.response_wait.count, 0);
	atomic_store(&g_client_prof.response_wait.total_time_ns, 0);
	atomic_store(&g_client_prof.response_wait.min_time_ns, ULONG_MAX);
	atomic_store(&g_client_prof.response_wait.max_time_ns, 0);

	atomic_store(&g_client_prof.total_rpc_time.count, 0);
	atomic_store(&g_client_prof.total_rpc_time.total_time_ns, 0);
	atomic_store(&g_client_prof.total_rpc_time.min_time_ns, ULONG_MAX);
	atomic_store(&g_client_prof.total_rpc_time.max_time_ns, 0);

	atomic_store(&g_client_prof.total_requests_sent, 0);
	atomic_store(&g_client_prof.total_responses_received, 0);

	g_client_prof.last_stats_print_time = get_timestamp_ns();

	pthread_mutex_unlock(&g_client_prof.stats_lock);

	printf("[PROFILING] Statistics reset\n");
	fflush(stdout);
}

#endif // ENABLE_PROFILING
