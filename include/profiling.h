#ifndef _PROFILING_H_
#define _PROFILING_H_

#include <time.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

// Enable/disable profiling at compile time
#ifndef ENABLE_PROFILING
#define ENABLE_PROFILING 0
#endif

#if ENABLE_PROFILING

// High-resolution timer utilities
static inline uint64_t get_timestamp_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static inline double ns_to_ms(uint64_t ns)
{
	return ns / 1000000.0;
}

// Performance statistics structure
struct perf_stats {
	atomic_ulong count;
	atomic_ulong total_time_ns;
	atomic_ulong min_time_ns;
	atomic_ulong max_time_ns;
	char name[64];
};

// Server-side profiling metrics
struct server_profiling {
	struct perf_stats msgbuf_scan; // Time to scan message buffers
	struct perf_stats msg_alloc; // Memory allocation time
	struct perf_stats msg_copy; // Message copy time
	struct perf_stats msg_handler_dispatch; // Thread pool dispatch time
	struct perf_stats total_msg_processing; // End-to-end message processing

	// Per-client statistics
	atomic_ulong active_clients;
	atomic_ulong total_messages_processed;

	// Last statistics print time
	uint64_t last_stats_print_time;
	pthread_mutex_t stats_lock;
};

// Client-side profiling metrics
struct client_profiling {
	struct perf_stats msgbuf_alloc; // Message buffer allocation time
	struct perf_stats msg_send; // Message send time
	struct perf_stats response_wait; // Response wait time
	struct perf_stats total_rpc_time; // End-to-end RPC time

	atomic_ulong total_requests_sent;
	atomic_ulong total_responses_received;
	atomic_ulong requests_per_second;

	uint64_t last_stats_print_time;
	pthread_mutex_t stats_lock;
};

// Global profiling instances
extern struct server_profiling g_server_prof;
extern struct client_profiling g_client_prof;

// Profiling macros
#define PROF_START(var) uint64_t var = get_timestamp_ns()

#define PROF_END_UPDATE(start_var, stats_ptr)                                  \
	do {                                                                   \
		uint64_t end_time = get_timestamp_ns();                        \
		uint64_t duration = end_time - start_var;                      \
		update_perf_stats(stats_ptr, duration);                        \
	} while (0)

#define PROF_PRINT_INTERVAL_SEC 10

// Function declarations
void init_profiling(void);
void update_perf_stats(struct perf_stats *stats, uint64_t duration_ns);
void print_server_stats(void);
void print_server_stats_manual(void);
void print_client_stats(void);
void print_client_stats_manual(void);
void reset_profiling_stats(void);

#else // ENABLE_PROFILING disabled

#define PROF_START(var)                                                        \
	do {                                                                   \
	} while (0)
#define PROF_END_UPDATE(start_var, stats_ptr)                                  \
	do {                                                                   \
	} while (0)
#define init_profiling()                                                       \
	do {                                                                   \
	} while (0)
#define print_server_stats()                                                   \
	do {                                                                   \
	} while (0)
#define print_server_stats_manual()                                            \
	do {                                                                   \
	} while (0)
#define print_client_stats()                                                   \
	do {                                                                   \
	} while (0)
#define reset_profiling_stats()                                                \
	do {                                                                   \
	} while (0)

#endif // ENABLE_PROFILING

#endif // _PROFILING_H_