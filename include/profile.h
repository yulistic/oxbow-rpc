#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// #define MSG_PROFILE 1
#define MAX_TIMER_LABELS 100

typedef struct {
	char label[64];
	long total_microseconds;
	int count;
	struct timespec start_time;
	int active;
} TimerEntry;

static TimerEntry timers[MAX_TIMER_LABELS];
static int timer_count = 0;

static int get_timer_index(const char *label)
{
	for (int i = 0; i < timer_count; ++i) {
		if (strcmp(timers[i].label, label) == 0)
			return i;
	}
	if (timer_count < MAX_TIMER_LABELS) {
		strncpy(timers[timer_count].label, label,
			sizeof(timers[timer_count].label) - 1);
		timers[timer_count].total_microseconds = 0;
		timers[timer_count].count = 0;
		timers[timer_count].active = 0;
		return timer_count++;
	}
	fprintf(stderr, "Too many timer labels!\n");
	exit(1);
}

static long diff_microseconds(struct timespec start, struct timespec end)
{
	return (end.tv_sec - start.tv_sec) * 1000000L +
	       (end.tv_nsec - start.tv_nsec) / 1000L;
}

static void timer_start(const char *label)
{
	int idx = get_timer_index(label);
	clock_gettime(CLOCK_MONOTONIC, &timers[idx].start_time);
	timers[idx].active = 1;
}

static void timer_end(const char *label)
{
	struct timespec end_time;
	int idx = get_timer_index(label);

	if (!timers[idx].active) {
		fprintf(stderr,
			"TIMER_END called before TIMER_START for '%s'\n",
			label);
		return;
	}

	clock_gettime(CLOCK_MONOTONIC, &end_time);
	long duration = diff_microseconds(timers[idx].start_time, end_time);
	timers[idx].total_microseconds += duration;
	timers[idx].count += 1;
	timers[idx].active = 0;
}

static void print_timer_summary(void)
{
	printf("\n=== Timer Summary ===\n");
	for (int i = 0; i < timer_count; ++i) {
		if (timers[i].count > 0) {
			long avg =
				timers[i].total_microseconds / timers[i].count;
			printf("[%s] calls: %d, total: %ld us, avg: %ld us\n",
			       timers[i].label, timers[i].count,
			       timers[i].total_microseconds, avg);
		}
	}
}

#ifdef MSG_PROFILE
__attribute__((constructor)) static void register_timer_summary()
{
	atexit(print_timer_summary);
}

#define TIMER_START(label) timer_start(label)
#define TIMER_END(label) timer_end(label)
#else
#define TIMER_START(label)                                                     \
	do {                                                                   \
	} while (0)
#define TIMER_END(label)                                                       \
	do {                                                                   \
	} while (0)
#endif
