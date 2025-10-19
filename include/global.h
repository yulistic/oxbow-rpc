#ifndef _RPC_GLOBAL_H_
#define _RPC_GLOBAL_H_

#include <sys/syscall.h>

// Global print option. It can be overwritten in each file before including log.h.
#ifndef ENABLE_PRINT
#define ENABLE_PRINT 0
#endif

// 0: always sleep, 1: hybrid polling, 2: always busywait.
#define RPC_SEMA_MODE 0

#define RPC_ENABLE_ASSERT
#ifdef RPC_ENABLE_ASSERT
#define rpc_assert(cond) assert(cond)
#else
#define rpc_assert(cond)
#endif

/* Some validation. */
// #define RPC_VALIDATION

#define get_tid() syscall(__NR_gettid)

#endif
