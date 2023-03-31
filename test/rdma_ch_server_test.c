#include <stdio.h>
#include "rpc.h"
// #include "test_global.h"

int main(int argc, char **argv)
{
	// run rpc_server.
	init_rpc_server(RPC_CH_RDMA, 7174);
	return 0;
}