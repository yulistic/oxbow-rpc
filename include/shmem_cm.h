#ifndef _SHMEM_CM_H_
#define _SHMEM_CM_H_
#include <sys/types.h>
#include <stdint.h>

enum shmem_cm_msg_op { REGISTER = 1, DEREGISTER, REGISTERED, DEREGISTERED };

struct shmem_cm_request {
	enum shmem_cm_msg_op op;
	// struct shmem_ch_cb *client_cb_addr;
	key_t client_key; // For deregister.
};
struct shmem_cm_response {
	enum shmem_cm_msg_op op;
	key_t client_key; // For register.
	key_t cq_key; // For register.
	uint64_t shm_size; // For register.
};

void *shmem_cm_thread(void *arg);
// void *check_client_disconnection(void *arg);
int connect_to_shmem_server(void *arg, key_t *shm_key, uint64_t *shm_size,
			    key_t *cq_shm_key);
#endif