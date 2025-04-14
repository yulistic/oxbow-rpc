#include <stdatomic.h>
#include <stdio.h>
#include <unistd.h> // sleep
#include "global.h"
#include "rpc.h"
#include "bit_array.h"

// Overwrite global print config.
// #define ENABLE_PRINT 1
#include "log.h"

atomic_int g_rpc_shmem_msgbuf_id;
atomic_long g_rpc_shmem_msgbuf_count;

// TODO: Need to profile this lock contention.
uint64_t alloc_msgbuf_id(struct rpc_ch_info *rpc_ch)
{
	uint64_t bit_id;
	int index, ret;

	ret = 0;
	index = atomic_fetch_add(&g_rpc_shmem_msgbuf_id, 1);
	index = index % RPC_SHMEM_MSG_BUF_NUM;
	atomic_fetch_add(&g_rpc_shmem_msgbuf_count, 1);
	while (1) {
		pthread_spin_lock(&rpc_ch->msgbuf_bitmap_lock);
		ret = bit_array_find_first_clear_bit(rpc_ch->msgbuf_bitmap,
						     &bit_id);
		// ret = bit_array_find_next_clear_bit(rpc_ch->msgbuf_bitmap,
		// 				    index, &bit_id);
		if (ret)
			bit_array_set_bit(rpc_ch->msgbuf_bitmap, bit_id);
		pthread_spin_unlock(&rpc_ch->msgbuf_bitmap_lock);

		if (ret)
			break;
		else {
			log_warn("Failed to alloc a msgbuf id. (sleep 1 sec)");
			sleep(1);
		}
	}

	// log_debug("[MSGBUF] alloc msgbuf=%lu", bit_id);

	return bit_id;
}

void free_msgbuf_id(struct rpc_ch_info *rpc_ch, uint64_t bit_id)
{
	atomic_fetch_sub(&g_rpc_shmem_msgbuf_count, 1);
	if (atomic_load(&g_rpc_shmem_msgbuf_count) >
	    (RPC_SHMEM_MSG_BUF_NUM / 2)) {
		log_warn("[%s]: %ld", __func__, g_rpc_shmem_msgbuf_count);
	}
	// log_debug("[MSGBUF] free msgbuf=%lu", bit_id);
	pthread_spin_lock(&rpc_ch->msgbuf_bitmap_lock);
	bit_array_clear_bit(rpc_ch->msgbuf_bitmap, bit_id);
	pthread_spin_unlock(&rpc_ch->msgbuf_bitmap_lock);
}
