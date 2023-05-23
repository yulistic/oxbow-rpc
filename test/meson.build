test_inc = include_directories('.')

test_inc_dirs = [inc, log_inc, thpool_inc, test_inc, rdma_inc, bitarr_inc]

## Tests
rpc_server_test_exe = executable('rpc_server_test', 'rpc_server_test.c', include_directories : test_inc_dirs, link_with : oxbow_rpc_lib)
rpc_client_test_exe = executable('rpc_client_test', 'rpc_client_test.c', include_directories : test_inc_dirs, link_with : oxbow_rpc_lib)


# Do test.
test('rpc_server_test', rpc_server_test_exe, args : ['rdma'], suite : 'rpc', is_parallel : true, priority : -50) # default priority = 0
test('rpc_client_test', rpc_client_test_exe, args : ['rdma'], suite : 'rpc',
is_parallel : true)

test('rpc_server_test', rpc_server_test_exe, args : ['shmem'], suite : 'rpc', is_parallel : true, priority : -50) # default priority = 0
test('rpc_client_test', rpc_client_test_exe, args : ['shmem'], suite : 'rpc', is_parallel : true)

# test('file_basic', file_basic_test, env : ['LD_PRELOAD=' + build_root + '/liboxbow_devfs.so'])
# test('dir_test', dir_test)
# test('spdk_test', spdk_test_exe, env : ['LD_PRELOAD=' + build_root + '/liboxbow_devfs.so'])
