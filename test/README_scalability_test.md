# RPC Scalability Test

***This document is written by AI.**

This directory contains scalability test programs for measuring the throughput and performance of the RPC module as the number of clients increases from 1 to 20.

## Files

- `rpc_scalability_server.c` - Server program that handles multiple clients
- `rpc_scalability_client.c` - Client program that simulates multiple clients
- `run_scalability_test.sh` - Automated test script
- `README_scalability_test.md` - This file

## Quick Start

1. **Make the script executable:**
   ```bash
   chmod +x test/run_scalability_test.sh
   ```

2. **Run the test with default settings (SHMEM channel, 1-10 clients):**
   ```bash
   ./test/run_scalability_test.sh
   ```

3. **Run with custom parameters:**
   ```bash
   # Test RDMA channel from 1 to 20 clients with step 2
   ./test/run_scalability_test.sh --channel rdma --start 1 --end 20 --step 2
   
   # Test SHMEM channel from 1 to 5 clients
   ./test/run_scalability_test.sh --channel shmem --start 1 --end 5
   ```

## Manual Testing

If you prefer to run the tests manually:

1. **Build the programs:**
   ```bash
   meson compile -C build
   ```

2. **Start the server:**
   ```bash
   # For SHMEM channel
   ./build/test/rpc_scalability_server shmem
   
   # For RDMA channel
   ./build/test/rpc_scalability_server rdma
   ```

3. **Run the client (in another terminal):**
   ```bash
   # Test 1 to 10 clients with step 1
   ./build/test/rpc_scalability_client shmem 1 10 1
   
   # Test 1 to 20 clients with step 2
   ./build/test/rpc_scalability_client rdma 1 20 2
   ```

## Script Options

```bash
Usage: ./test/run_scalability_test.sh [OPTIONS]

Options:
  -c, --channel    Channel type (rdma|shmem) [default: shmem]
  -s, --start      Starting number of clients [default: 1]
  -e, --end        Ending number of clients [default: 10]
  --step           Step size for client count [default: 1]
  -i, --ip         Server IP address [default: 127.0.0.1]
  -t, --time       Server run time in seconds [default: 60]
  -h, --help       Show this help message
```

## Test Configuration

The test behavior can be modified by changing these constants in the source files:

- `MESSAGES_PER_CLIENT` - Number of messages each client sends (default: 1000)
- `MAX_CLIENTS` - Maximum number of clients supported (default: 20)
- `WARMUP_MESSAGES` - Number of warmup messages (default: 100)

## Results

The test generates several output files:

- `scalability_results_YYYYMMDD_HHMMSS/` - Results directory
  - `server_[channel].log` - Server log file
  - `client_[channel].log` - Client log file
  - `results_[channel].csv` - CSV file with performance metrics

### CSV Format

The CSV file contains two types of records:

1. **SCALABILITY_TEST_RESULT** - Overall test results
   ```
   SCALABILITY_TEST_RESULT,NumClients,Duration(sec),MessagesSent,MessagesReceived,Throughput(msgs/sec)
   ```

2. **CLIENT_STATS** - Per-client statistics
   ```
   CLIENT_STATS,ClientID,MessagesSent,MessagesReceived,AvgLatency(ms),Throughput(msgs/sec)
   ```

## Expected Results

The test measures:
- **Throughput** - Messages per second
- **Latency** - Average response time
- **Scalability** - How performance changes with client count

Typical results might show:
- Linear scaling up to a certain point
- Saturation or degradation beyond server capacity
- Different behavior between RDMA and SHMEM channels

## Troubleshooting

### Common Issues

1. **Server fails to start:**
   - Check if ports are available (RDMA)
   - Ensure shared memory files are cleaned up (SHMEM)
   - Verify IP address configuration

2. **Client connection fails:**
   - Ensure server is running
   - Check network connectivity (RDMA)
   - Verify shared memory permissions (SHMEM)

3. **Build errors:**
   - Make sure all dependencies are installed
   - Check that meson build is properly configured

### SHMEM Channel Issues

```bash
# Clean up shared memory files
rm -f /tmp/rpc_test_cm /tmp/rpc_test_cm2

# Check shared memory usage
ipcs -m
```

### RDMA Channel Issues

```bash
# Check RDMA devices
ibv_devices

# Check network connectivity
ping [server_ip]
```

## Performance Analysis

The script provides basic analysis including:
- Maximum throughput achieved
- Linear scaling efficiency
- Per-client performance breakdown

For more detailed analysis, you can process the CSV files with tools like:
- Excel/LibreOffice Calc
- Python pandas
- R
- GNU plot

Example Python analysis:
```python
import pandas as pd
import matplotlib.pyplot as plt

# Load results
df = pd.read_csv('results_shmem.csv')
results = df[df['Type'] == 'SCALABILITY_TEST_RESULT']

# Plot throughput vs clients
plt.plot(results['NumClients'], results['Throughput'])
plt.xlabel('Number of Clients')
plt.ylabel('Throughput (msgs/sec)')
plt.title('RPC Scalability Test Results')
plt.show()
```

## Customization

To modify the test for your specific requirements:

1. **Change test parameters** in the source files
2. **Modify message sizes** by updating `MAX_MSG_DATA_SIZE`
3. **Adjust test duration** by changing `MESSAGES_PER_CLIENT`
4. **Add custom metrics** by extending the statistics collection

## Notes

- The test is designed for measuring local performance characteristics
- Network latency will significantly affect RDMA results
- SHMEM tests are limited to single-machine scenarios
- Server thread pool size (default: 8) may need adjustment for high client counts
- Results may vary based on system resources and configuration 