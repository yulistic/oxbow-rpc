# RPC Performance Profiling Guide

This document describes how to use the built-in performance profiling system to analyze RPC bottlenecks.

## Overview

The profiling system measures key performance metrics in both server and client components:

### Server Metrics
- **MsgBuf Scan**: Time spent scanning message buffers for new arrivals
- **Msg Alloc**: Memory allocation time for message structures
- **Msg Copy**: Time spent copying message data
- **Handler Dispatch**: Thread pool dispatch overhead
- **Total Msg Processing**: End-to-end message processing time
- **Active Clients**: Number of currently connected clients
- **Messages/sec**: Message processing throughput

### Client Metrics
- **MsgBuf Alloc**: Message buffer allocation time
- **Msg Send**: Time to send messages to server
- **Response Wait**: Time waiting for server responses
- **Total RPC Time**: End-to-end RPC call duration
- **Requests/sec**: Request sending throughput

## Building with Profiling

Profiling is enabled by default. To build with meson:

```bash
# Setup build directory
meson setup build

# Compile
meson compile -C build
```

To disable profiling (for production builds):

```bash
# Clean and setup with profiling disabled
rm -rf build
meson setup build -Dc_args="-DENABLE_PROFILING=0"
meson compile -C build
```

Alternative quick build script:

```bash
./build.sh
```

## Using Profiling

### Manual Statistics Control

Statistics are printed manually when requested. For the scalability test, statistics are automatically printed when the client sends a stats request to the server after each test run.

```c
#include "profiling.h"

// Print current statistics (manual, no time interval check)
print_server_stats_manual();
print_client_stats_manual();

// Print with 10-second interval check (for periodic printing)
print_server_stats();
print_client_stats();

// Reset all statistics
reset_profiling_stats();
```

### Example Output

```
=== SERVER PERFORMANCE STATISTICS (MANUAL) ===
[PROF] MsgBuf Scan        : count=1234 avg=0.025ms min=0.001ms max=0.150ms total=30.850ms
[PROF] Msg Alloc          : count=1234 avg=0.012ms min=0.005ms max=0.080ms total=14.808ms
[PROF] Msg Copy           : count=1234 avg=0.003ms min=0.001ms max=0.015ms total=3.702ms
[PROF] Handler Dispatch   : count=1234 avg=0.008ms min=0.002ms max=0.030ms total=9.872ms
[PROF] Total Msg Processing: count=1234 avg=0.048ms min=0.015ms max=0.200ms total=59.232ms
[PROF] Active clients: 5
[PROF] Total messages: 1234
[PROF] Messages/sec: 123.40
=============================================
```

### Scalability Test Integration

When running the scalability test, server statistics are automatically printed after each client count test completes:

```bash
# Run scalability test - server stats will be printed after each test
./rpc_scalability_client shmem 1 10 2
```

## Analyzing Bottlenecks

### High MsgBuf Scan Time
- **Problem**: Linear search through message buffers is slow
- **Solution**: Consider implementing circular buffer or bit scanning optimizations

### High Msg Alloc Time
- **Problem**: Dynamic memory allocation overhead
- **Solutions**: 
  - Pre-allocate message pools
  - Use stack allocation for small messages
  - Implement custom memory allocators

### High Msg Copy Time
- **Problem**: Large message copying overhead
- **Solutions**:
  - Reduce message sizes
  - Use zero-copy techniques
  - Optimize memory layout

### High Handler Dispatch Time
- **Problem**: Thread pool overhead
- **Solutions**:
  - Tune thread pool size
  - Consider lockless queues
  - Use dedicated threads for hot paths

### Low Messages/sec
- **Problem**: Overall throughput bottleneck
- **Analysis**: Check which metric has the highest average time
- **Solutions**: Focus optimization on the slowest component

## Example Analysis Workflow

1. **Baseline Measurement**
   ```bash
   # Run your RPC workload and observe initial statistics
   ./your_rpc_server
   ./your_rpc_client
   ```

2. **Identify Bottleneck**
   ```
   # Look for highest average times in statistics output
   [PROF] MsgBuf Scan        : avg=0.150ms  <-- HIGH
   [PROF] Msg Alloc          : avg=0.012ms  
   [PROF] Msg Copy           : avg=0.003ms  
   [PROF] Handler Dispatch   : avg=0.008ms  
   ```

3. **Apply Optimizations**
   - Focus on the component with highest average time
   - Make targeted code changes

4. **Measure Improvement**
   ```bash
   # Reset statistics and re-measure
   reset_profiling_stats();
   # Run workload again and compare results
   ```

## Configuration

### Profiling Interval
The interval checking is only used by `print_server_stats()` and `print_client_stats()` functions (not the manual versions):

```c
// In profiling.h
#define PROF_PRINT_INTERVAL_SEC 10  // Check interval for automatic printing
```

### Enabling/Disabling at Runtime
```c
// Disable profiling temporarily
#define ENABLE_PROFILING 0
```

## Performance Impact

The profiling system is designed to have minimal overhead:
- Uses high-resolution monotonic clock
- Atomic operations for thread safety
- Approximately 50-100ns overhead per measurement

For production deployments, consider disabling profiling to eliminate any overhead.

## Troubleshooting

### No Statistics Output
- Check that `init_profiling()` was called during initialization
- Verify that `ENABLE_PROFILING` is set to 1
- Ensure log level allows INFO messages

### Statistics Reset Too Frequently  
- Check if multiple threads are calling `print_*_stats()` 
- Verify the print interval setting

### Inaccurate Timing
- Ensure system has monotonic clock support
- Check for clock adjustments during measurement
- Consider increasing measurement duration for better accuracy 