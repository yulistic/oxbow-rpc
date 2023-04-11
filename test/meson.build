test_inc = include_directories('.')

test_inc_dirs = [inc, log_inc, thpool_inc, test_inc, rdma_inc, bitarr_inc]

## Tests
rdma_ch_server_test_exe = executable('rdma_ch_server_test', 'rdma_ch_server_test.c', include_directories : test_inc_dirs, link_with : oxbow_rpc_lib)
rdma_ch_client_test_exe = executable('rdma_ch_client_test', 'rdma_ch_client_test.c', include_directories : test_inc_dirs, link_with : oxbow_rpc_lib)


# Do test.
test('rdma_ch_server_test', rdma_ch_server_test_exe, suite : 'rdma', is_parallel : true, priority : -50) # default priority = 0
test('rdma_ch_client_test', rdma_ch_client_test_exe, suite : 'rdma', is_parallel : true)

# test('file_basic', file_basic_test, env : ['LD_PRELOAD=' + build_root + '/liboxbow_devfs.so'])
# test('dir_test', dir_test)
# test('spdk_test', spdk_test_exe, env : ['LD_PRELOAD=' + build_root + '/liboxbow_devfs.so'])
