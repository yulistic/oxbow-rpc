#ifndef __TEST__GLOBAL_H__
#define __TEST__GLOBAL_H__

#define MAX_MSG_DATA_SIZE 4096
#define SHM_KEY_SEED 9367 // arbitrary value.
#define SHM_KEY_SEED2 9368 // arbitrary value.

/* Configurations. */
// char *g_ip_addr = "192.168.14.113";
char *g_ip_addr = ""; // Change to your IP address.
int g_port = 7176;
char *g_shmem_path = "/tmp/rpc_test_cm";
char *g_shmem_path2 = "/tmp/rpc_test_cm2";

#endif /* __TEST__GLOBAL_H__ */
