#ifndef _RPC_GLOBAL_H_
#define _RPC_GLOBAL_H_

// Global print option. It can be overwritten in each file before including log.h.
#ifndef ENABLE_PRINT
#define ENABLE_PRINT 0
#endif

#define SEMA_MODE 0 // 0: always sleep, 1: hybrid polling, 2: always busywait.

#endif
