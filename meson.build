project('oxbow-rpc', 'c',
  version : '0.1',
  default_options : ['warning_level=3'])

proj_root = meson.source_root()
build_root = meson.build_root()
current_dir = meson.current_source_dir()

# message('proj_root: ' + proj_root)
# message('build_root: ' + build_root)

cc = meson.get_compiler('c')

### - *_inc : header files
### - *_lib : library
### - *_dep : dependencies (to library)

# add_global_arguments('-DFLAG=value', language : 'c')

# Linking shared libraries.
## rdma-core (a user-level rdma) libraries.
rdma_inc = include_directories('lib/rdma-core/build/include')
ibverbs_dep = dependency('libibverbs', method : 'pkg-config', static : false)
rdmacm_dep = dependency('librdmacm', method : 'pkg-config', static : false)

## Logging library.
log_inc = include_directories('lib/log.c/src')

## C-Thread-Pool library.
thpool_inc = include_directories('lib/c-thread-pool')

## BitArray (bitmap) library.
bitarr_inc = include_directories('lib/BitArray')
bitarr_dir = proj_root + '/lib/BitArray'
bitarr_dep = declare_dependency(
  link_args : ['-L' + bitarr_dir, '-lbitarr'],
  include_directories : bitarr_inc
  )
# Or, add bitarr_lib instead of bitarr_dep
# bitarr_lib = cc.find_library('bitarr', dirs : [bitarr_dir])

## Source code headers.
inc = include_directories('include')

## Source files.
oxbow_rpc_lib_srcs = [ \
'lib/c-thread-pool/thpool.c',
'lib/log.c/src/log.c',
'src/rpc_common.c',
'src/rpc_server.c',
'src/rpc_client.c',
'src/channel/rdma.c',
'src/channel/shmem.c',
'src/channel/shmem_cm.c',
]

oxbow_rpc_lib = shared_library('oxbow_rpc',
                          oxbow_rpc_lib_srcs,
                          # version : '1.1.1',
                          # soversion : '0',
                          include_directories : [inc, log_inc, thpool_inc, rdma_inc, bitarr_inc],
                          dependencies : [ibverbs_dep, rdmacm_dep, bitarr_dep], # Shared library
                          c_args: ['-DLOG_USE_COLOR'], # Print with color by log.c library.
                          # link_args : ['-Wl,-rpath=' + rdma_lib_dir],
                          )

subdir('test')