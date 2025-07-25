#!/bin/bash

# Simple Queue Service Benchmark Suite
# Comprehensive performance testing against industry standards

API_KEY="test-api-key"
BASE_URL="http://localhost:8080"
RESULTS_DIR="benchmark-results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create results directory
mkdir -p $RESULTS_DIR

echo "🚀 Simple Queue Service Benchmark Suite"
echo "========================================"
echo "Timestamp: $(date)"
echo "Results will be saved to: $RESULTS_DIR"

# Utility functions
log_result() {
    echo "$1" | tee -a "$RESULTS_DIR/benchmark_$TIMESTAMP.log"
}

measure_time() {
    local start_time=$(date +%s.%N)
    eval "$1"
    local end_time=$(date +%s.%N)
    echo "$(echo "$end_time - $start_time" | bc -l)"
}

# Test 1: Throughput Benchmark - Message Sending
test_throughput_send() {
    log_result "\n=== THROUGHPUT TEST: Message Sending ==="
    
    # Create test queue
    curl -s -X POST $BASE_URL/api/v1/queues \
        -H "Content-Type: application/json" \
        -H "X-API-Key: $API_KEY" \
        -d '{
            "queueName": "throughput-test-queue",
            "attributes": {
                "visibilityTimeoutSeconds": 30,
                "messageRetentionPeriod": 1209600
            }
        }' > /dev/null

    # Test different message sizes and volumes
    for msg_size in 16 256 1024 4096; do
        for batch_count in 10 50 100 500 1000; do
            log_result "\nTesting: $batch_count messages of ${msg_size} bytes each"
            
            # Generate message body of specified size
            msg_body=$(head -c $msg_size < /dev/zero | tr '\0' 'A')
            
            start_time=$(date +%s.%N)
            
            # Send messages concurrently
            for i in $(seq 1 $batch_count); do
                curl -s -X POST $BASE_URL/api/v1/queues/throughput-test-queue/messages \
                    -H "Content-Type: application/json" \
                    -H "X-API-Key: $API_KEY" \
                    -d "{\"messageBody\": \"$msg_body\", \"delaySeconds\": 0, \"attributes\": {\"test\": \"throughput\", \"size\": \"$msg_size\", \"index\": \"$i\"}}" > /dev/null &
            done
            wait
            
            end_time=$(date +%s.%N)
            duration=$(echo "$end_time - $start_time" | bc -l)
            throughput=$(echo "scale=2; $batch_count / $duration" | bc -l)
            
            log_result "Messages: $batch_count, Size: ${msg_size}B, Duration: ${duration}s, Throughput: ${throughput} msg/s"
            echo "$msg_size,$batch_count,$duration,$throughput" >> "$RESULTS_DIR/throughput_send_$TIMESTAMP.csv"
        done
    done
}

# Test 2: Latency Benchmark
test_latency() {
    log_result "\n=== LATENCY TEST: End-to-End Message Latency ==="
    
    # Create latency test queue
    curl -s -X POST $BASE_URL/api/v1/queues \
        -H "Content-Type: application/json" \
        -H "X-API-Key: $API_KEY" \
        -d '{
            "queueName": "latency-test-queue",
            "attributes": {
                "visibilityTimeoutSeconds": 30,
                "messageRetentionPeriod": 1209600
            }
        }' > /dev/null

    declare -a latencies=()
    
    # Run 100 latency measurements
    for i in $(seq 1 100); do
        msg_body="Latency test message $i $(date +%s.%N)"
        
        start_time=$(date +%s.%N)
        
        # Send message
        response=$(curl -s -X POST $BASE_URL/api/v1/queues/latency-test-queue/messages \
            -H "Content-Type: application/json" \
            -H "X-API-Key: $API_KEY" \
            -d "{\"messageBody\": \"$msg_body\", \"delaySeconds\": 0, \"attributes\": {\"test\": \"latency\", \"timestamp\": \"$start_time\"}}")
        
        send_time=$(date +%s.%N)
        send_latency=$(echo "$send_time - $start_time" | bc -l)
        
        # Convert to milliseconds
        send_latency_ms=$(echo "$send_latency * 1000" | bc -l)
        latencies+=($send_latency_ms)
        
        echo "$i,$send_latency_ms" >> "$RESULTS_DIR/latency_$TIMESTAMP.csv"
        
        if [ $((i % 10)) -eq 0 ]; then
            log_result "Completed $i latency tests..."
        fi
    done
    
    # Calculate statistics
    sorted_latencies=($(printf '%s\n' "${latencies[@]}" | sort -n))
    count=${#sorted_latencies[@]}
    
    min_latency=${sorted_latencies[0]}
    max_latency=${sorted_latencies[$((count-1))]}
    
    # Calculate percentiles
    p50_idx=$((count * 50 / 100))
    p95_idx=$((count * 95 / 100))
    p99_idx=$((count * 99 / 100))
    
    p50_latency=${sorted_latencies[$p50_idx]}
    p95_latency=${sorted_latencies[$p95_idx]}
    p99_latency=${sorted_latencies[$p99_idx]}
    
    # Calculate average
    sum=0
    for latency in "${latencies[@]}"; do
        sum=$(echo "$sum + $latency" | bc -l)
    done
    avg_latency=$(echo "scale=3; $sum / $count" | bc -l)
    
    log_result "\nLatency Statistics (ms):"
    log_result "Min: $min_latency"
    log_result "Average: $avg_latency"
    log_result "P50: $p50_latency"
    log_result "P95: $p95_latency"
    log_result "P99: $p99_latency"
    log_result "Max: $max_latency"
}

# Test 3: Concurrent Connections
test_concurrent_connections() {
    log_result "\n=== CONCURRENT CONNECTIONS TEST ==="
    
    # Create concurrent test queue
    curl -s -X POST $BASE_URL/api/v1/queues \
        -H "Content-Type: application/json" \
        -H "X-API-Key: $API_KEY" \
        -d '{
            "queueName": "concurrent-test-queue",
            "attributes": {
                "visibilityTimeoutSeconds": 30,
                "messageRetentionPeriod": 1209600
            }
        }' > /dev/null

    for concurrent_users in 5 10 25 50 100; do
        log_result "\nTesting with $concurrent_users concurrent connections..."
        
        start_time=$(date +%s.%N)
        
        for i in $(seq 1 $concurrent_users); do
            (
                for j in $(seq 1 10); do
                    curl -s -X POST $BASE_URL/api/v1/queues/concurrent-test-queue/messages \
                        -H "Content-Type: application/json" \
                        -H "X-API-Key: $API_KEY" \
                        -d "{\"messageBody\": \"Concurrent test user $i message $j\", \"delaySeconds\": 0, \"attributes\": {\"user\": \"$i\", \"msg\": \"$j\"}}" > /dev/null
                done
            ) &
        done
        wait
        
        end_time=$(date +%s.%N)
        duration=$(echo "$end_time - $start_time" | bc -l)
        total_messages=$((concurrent_users * 10))
        throughput=$(echo "scale=2; $total_messages / $duration" | bc -l)
        
        log_result "Concurrent Users: $concurrent_users, Messages: $total_messages, Duration: ${duration}s, Throughput: ${throughput} msg/s"
        echo "$concurrent_users,$total_messages,$duration,$throughput" >> "$RESULTS_DIR/concurrent_$TIMESTAMP.csv"
    done
}

# Test 4: Storage Tier Performance
test_storage_performance() {
    log_result "\n=== STORAGE TIER PERFORMANCE TEST ==="
    
    # Get initial storage stats
    storage_stats_before=$(curl -s -X GET $BASE_URL/api/v1/storage/stats -H "X-API-Key: $API_KEY")
    log_result "Storage stats before test: $storage_stats_before"
    
    # Send 1000 messages to test storage efficiency
    start_time=$(date +%s.%N)
    for i in $(seq 1 1000); do
        curl -s -X POST $BASE_URL/api/v1/queues/throughput-test-queue/messages \
            -H "Content-Type: application/json" \
            -H "X-API-Key: $API_KEY" \
            -d "{\"messageBody\": \"Storage test message $i\", \"delaySeconds\": 0, \"attributes\": {\"test\": \"storage\", \"index\": \"$i\"}}" > /dev/null &
        
        if [ $((i % 100)) -eq 0 ]; then
            wait
            log_result "Sent $i messages..."
        fi
    done
    wait
    
    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc -l)
    throughput=$(echo "scale=2; 1000 / $duration" | bc -l)
    
    # Get final storage stats
    storage_stats_after=$(curl -s -X GET $BASE_URL/api/v1/storage/stats -H "X-API-Key: $API_KEY")
    
    log_result "1000 messages stored in ${duration}s (${throughput} msg/s)"
    log_result "Storage stats after test: $storage_stats_after"
}

# Test 5: System Resource Usage
test_resource_usage() {
    log_result "\n=== SYSTEM RESOURCE USAGE TEST ==="
    
    # Monitor system resources during load test
    log_result "Monitoring system resources during 30-second load test..."
    
    # Start background resource monitoring
    (
        for i in $(seq 1 30); do
            timestamp=$(date +%s)
            memory_usage=$(ps aux | grep '[j]ava.*bootRun' | awk '{sum += $6} END {print sum/1024}' 2>/dev/null || echo "0")
            cpu_usage=$(ps aux | grep '[j]ava.*bootRun' | awk '{sum += $3} END {print sum}' 2>/dev/null || echo "0")
            echo "$timestamp,$memory_usage,$cpu_usage" >> "$RESULTS_DIR/resources_$TIMESTAMP.csv"
            sleep 1
        done
    ) &
    
    resource_monitor_pid=$!
    
    # Generate load for 30 seconds
    timeout 30s bash -c '
        while true; do
            curl -s -X POST $BASE_URL/api/v1/queues/throughput-test-queue/messages \
                -H "Content-Type: application/json" \
                -H "X-API-Key: $API_KEY" \
                -d "{\"messageBody\": \"Load test message\", \"delaySeconds\": 0, \"attributes\": {\"test\": \"load\"}}" > /dev/null &
        done
    '
    
    wait $resource_monitor_pid
    log_result "Resource monitoring completed. Data saved to resources_$TIMESTAMP.csv"
}

# Main benchmark execution
main() {
    # Check if backend is running
    if ! curl -s $BASE_URL/actuator/health > /dev/null; then
        log_result "❌ Backend is not running at $BASE_URL"
        log_result "Please start the backend with: ./gradlew bootRun"
        exit 1
    fi
    
    log_result "✅ Backend is running and healthy"
    
    # Initialize CSV headers
    echo "message_size,message_count,duration,throughput" > "$RESULTS_DIR/throughput_send_$TIMESTAMP.csv"
    echo "test_number,latency_ms" > "$RESULTS_DIR/latency_$TIMESTAMP.csv"
    echo "concurrent_users,total_messages,duration,throughput" > "$RESULTS_DIR/concurrent_$TIMESTAMP.csv"
    echo "timestamp,memory_mb,cpu_percent" > "$RESULTS_DIR/resources_$TIMESTAMP.csv"
    
    # Run all tests
    test_throughput_send
    test_latency
    test_concurrent_connections
    test_storage_performance
    test_resource_usage
    
    log_result "\n🎉 Benchmark suite completed!"
    log_result "Results saved to $RESULTS_DIR/"
    log_result "View results with: cat $RESULTS_DIR/benchmark_$TIMESTAMP.log"
}

# Run the benchmark suite
main