#!/bin/bash

# RPC Scalability Test Script
# This script runs the scalability tests for both RDMA and SHMEM channels

set -e

# Default parameters
CHANNEL_TYPE="shmem" # default to shmem as it's easier to test
START_CLIENTS=1
END_CLIENTS=10
STEP=1
SERVER_IP="127.0.0.1"
TEST_DURATION=600 # seconds

# Parse command line arguments
while [[ $# -gt 0 ]]; do
	case $1 in
	-c | --channel)
		CHANNEL_TYPE="$2"
		shift 2
		;;
	-s | --start)
		START_CLIENTS="$2"
		shift 2
		;;
	-e | --end)
		END_CLIENTS="$2"
		shift 2
		;;
	--step)
		STEP="$2"
		shift 2
		;;
	-i | --ip)
		SERVER_IP="$2"
		shift 2
		;;
	-t | --time)
		TEST_DURATION="$2"
		shift 2
		;;
	-h | --help)
		echo "Usage: $0 [OPTIONS]"
		echo "Options:"
		echo "  -c, --channel    Channel type (rdma|shmem) [default: shmem]"
		echo "  -s, --start      Starting number of clients [default: 1]"
		echo "  -e, --end        Ending number of clients [default: 10]"
		echo "  --step           Step size for client count [default: 1]"
		echo "  -i, --ip         Server IP address [default: 127.0.0.1]"
		echo "  -t, --time       Server run time in seconds [default: 60]"
		echo "  -h, --help       Show this help message"
		echo ""
		echo "Example:"
		echo "  $0 --channel shmem --start 1 --end 20 --step 2"
		echo "  $0 --channel rdma --ip 192.168.1.100 --start 1 --end 10"
		exit 0
		;;
	*)
		echo "Unknown option $1"
		exit 1
		;;
	esac
done

# Validate parameters
if [[ "$CHANNEL_TYPE" != "rdma" && "$CHANNEL_TYPE" != "shmem" ]]; then
	echo "Error: Channel type must be 'rdma' or 'shmem'"
	exit 1
fi

if [[ $START_CLIENTS -lt 1 || $END_CLIENTS -lt $START_CLIENTS || $STEP -lt 1 ]]; then
	echo "Error: Invalid client count parameters"
	exit 1
fi

echo "=== RPC Scalability Test ==="
echo "Channel type: $CHANNEL_TYPE"
echo "Client range: $START_CLIENTS to $END_CLIENTS (step $STEP)"
echo "Server IP: $SERVER_IP"
echo "Test duration: $TEST_DURATION seconds"
echo "Message size: 64B (fixed)"
echo "==============================="

# Update IP address in test_global.h if using RDMA
if [[ "$CHANNEL_TYPE" == "rdma" ]]; then
	echo "Updating IP address in test_global.h..."
	sed -i "s/char \*g_ip_addr = \".*\";/char *g_ip_addr = \"$SERVER_IP\";/" test_global.h
fi

# Build the test programs
echo "Building test programs..."
if ! meson compile -C build; then
	echo "Error: Failed to build test programs"
	exit 1
fi

# Create results directory
# RESULTS_DIR="scalability_results_$(date +%Y%m%d_%H%M%S)"
RESULTS_DIR="scalability_results"
mkdir -p "$RESULTS_DIR"

# Start server in background
echo "Starting server..."
SERVER_LOG="$RESULTS_DIR/server_${CHANNEL_TYPE}.log"
if [[ "$CHANNEL_TYPE" == "shmem" ]]; then
	# Clean up existing shared memory files
	rm -f /tmp/rpc_test_cm /tmp/rpc_test_cm2
fi

timeout $TEST_DURATION ./build/test/rpc_scalability_server $CHANNEL_TYPE >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

# Wait for server to start
sleep 3

# Check if server is running
if ! kill -0 $SERVER_PID 2>/dev/null; then
	echo "Error: Server failed to start"
	cat "$SERVER_LOG"
	exit 1
fi

echo "Server started (PID: $SERVER_PID)"

# Run client tests
echo "Running client tests..."
CLIENT_LOG="$RESULTS_DIR/client_${CHANNEL_TYPE}.log"
CSV_RESULTS="$RESULTS_DIR/results_${CHANNEL_TYPE}.csv"

# Run the scalability test
if ./build/test/rpc_scalability_client $CHANNEL_TYPE $START_CLIENTS $END_CLIENTS $STEP >"$CLIENT_LOG" 2>&1; then
	echo "Client tests completed successfully"

	# Extract CSV results
	echo "Extracting results..."
	grep "^SCALABILITY_TEST_RESULT\|^CLIENT_STATS" "$CLIENT_LOG" >"$CSV_RESULTS"

	echo "Results saved to: $CSV_RESULTS"
	echo ""
	echo "Summary:"
	echo "--------"
	head -1 "$CSV_RESULTS"
	tail -n +2 "$CSV_RESULTS" | grep "^SCALABILITY_TEST_RESULT" | while IFS=',' read -r prefix clients duration sent received throughput; do
		printf "Clients: %2d | Duration: %6.2fs | Messages: %6d | Throughput: %8.2f msgs/sec\n" \
			"$clients" "$duration" "$sent" "$throughput"
	done
else
	echo "Error: Client tests failed"
	cat "$CLIENT_LOG"
	EXIT_CODE=1
fi

# Stop server
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

# Clean up shared memory files if using SHMEM
if [[ "$CHANNEL_TYPE" == "shmem" ]]; then
	rm -f /tmp/rpc_test_cm /tmp/rpc_test_cm2
fi

echo "Test completed. Results in: $RESULTS_DIR"

# Generate simple analysis if possible
if command -v python3 &>/dev/null; then
	echo "Generating analysis..."
	python3 -c "
import csv
import sys

try:
    with open('$CSV_RESULTS', 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        
        results = []
        for row in reader:
            if row[0] == 'SCALABILITY_TEST_RESULT':
                results.append({
                    'clients': int(row[1]),
                    'duration': float(row[2]),
                    'sent': int(row[3]),
                    'received': int(row[4]),
                    'throughput': float(row[5])
                })
        
        if results:
            print('\n=== SCALABILITY ANALYSIS ===')
            print('Max throughput: {:.2f} msgs/sec with {} clients'.format(
                max(r['throughput'] for r in results),
                next(r['clients'] for r in results if r['throughput'] == max(r['throughput'] for r in results))
            ))
            
            print('Linear scaling efficiency:')
            baseline = results[0]['throughput']
            for r in results:
                expected = baseline * r['clients']
                efficiency = (r['throughput'] / expected) * 100 if expected > 0 else 0
                print('  {} clients: {:.1f}%'.format(r['clients'], efficiency))
            
except Exception as e:
    print('Analysis failed:', e)
"
fi

exit ${EXIT_CODE:-0}
