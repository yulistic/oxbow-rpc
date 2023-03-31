#include <stdio.h>
#include "unistd.h"
#include "rpc.h"

int main(int argc, char **argv)
{
	char *ip_addr = "192.168.14.113";

	sleep(2);

	init_rpc_client(RPC_CH_RDMA, ip_addr, 7174);

	return 0;
}