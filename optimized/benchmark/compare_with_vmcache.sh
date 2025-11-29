#!/bin/bash

# Direct Comparison: Your Tree vs VMCache
# This script runs identical workloads on both systems for direct performance comparison

set -e

# Configuration
COMPARISON_DIR="/home/skarim/Code/haldendb_ex/haldendb_pvt/benchmark/vmcache_comparison"
VMCACHE_DIR="$COMPARISON_DIR/vmcache"
YOUR_BENCHMARK_DIR="/home/skarim/Code/haldendb_ex/haldendb_pvt/benchmark/build"
RESULTS_DIR="$COMPARISON_DIR/results"

# Create timestamped results directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
COMPARISON_RESULTS_DIR="$RESULTS_DIR/comparison_${TIMESTAMP}"
mkdir -p "$COMPARISON_RESULTS_DIR"

echo "=========================================="
echo "VMCache vs Your Tree Comparison"
echo "Results Directory: $COMPARISON_RESULTS_DIR"
echo "=========================================="

# Test configurations - matching both systems' capabilities
THREAD_COUNTS=(1 4 8 16)
MEMORY_SIZES_GB=(1 2 4 8)  # Physical memory sizes
VIRTUAL_SIZES_GB=(16 32 64)  # Virtual memory sizes (for VMCache)
RUN_DURATION=30  # seconds
RECORD_COUNTS=(100000 500000 1000000)

# Perf events for comparison
PERF_EVENTS="cache-misses,cache-references,cycles,instructions,page-faults,L1-dcache-load-misses,TLB-load-misses,LLC-load-misses"

# Function to run VMCache benchmark
run_vmcache_benchmark() {
    local threads=$1
    local phys_gb=$2
    local virt_gb=$3
    local datasize=$4
    local workload=$5  # "tpcc" or "rndread"
    local test_name=$6
    
    echo "Running VMCache: $test_name"
    echo "Threads: $threads, Physical: ${phys_gb}GB, Virtual: ${virt_gb}GB, Data: $datasize"
    
    local output_file="$COMPARISON_RESULTS_DIR/vmcache_${test_name}.txt"
    local perf_file="$COMPARISON_RESULTS_DIR/vmcache_${test_name}.perf"
    
    cd "$VMCACHE_DIR"
    
    # Run with perf profiling
    perf stat -e "$PERF_EVENTS" -o "$perf_file" \
        numactl --cpunodebind=1 --membind=1 \
        env THREADS=$threads \
            PHYSGB=$phys_gb \
            VIRTGB=$virt_gb \
            DATASIZE=$datasize \
            RUNFOR=$RUN_DURATION \
            RNDREAD=$([[ "$workload" == "rndread" ]] && echo 1 || echo 0) \
            EXMAP=1 \
            ./vmcache > "$output_file" 2>&1
    
    echo "VMCache benchmark completed: $test_name"
}

# Function to run your tree benchmark  
run_your_tree_benchmark() {
    local threads=$1
    local memory_size=$2
    local records=$3
    local operation=$4
    local test_name=$5
    
    echo "Running Your Tree: $test_name"
    echo "Threads: $threads, Memory: ${memory_size}B, Records: $records, Operation: $operation"
    
    local output_file="$COMPARISON_RESULTS_DIR/yourtree_${test_name}.txt"
    local perf_file="$COMPARISON_RESULTS_DIR/yourtree_${test_name}.perf"
    
    cd "$YOUR_BENCHMARK_DIR"
    
    # Build your tree with cache
    make clean > /dev/null 2>&1
    cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__ENABLE_ASSERTS__"
    make -j$(nproc)
    
    # Calculate cache size (25% of records for fair comparison)
    local cache_size=$((records * 25 / 100))
    
    # Run with perf profiling
    perf stat -e "$PERF_EVENTS" -o "$perf_file" \
        numactl --cpunodebind=1 --membind=1 \
        ./benchmark \
        --config "bm_cache" \
        --cache-type "LRU" \
        --storage-type "VolatileStorage" \
        --cache-size "$cache_size" \
        --page-size 4096 \
        --memory-size "$memory_size" \
        --tree-type "BplusTreeSOA" \
        --key-type "uint64_t" \
        --value-type "uint64_t" \
        --operation "$operation" \
        --degree 128 \
        --records "$records" \
        --runs 3 \
        --threads "$threads" \
        --output-dir "$COMPARISON_RESULTS_DIR" > "$output_file" 2>&1
    
    echo "Your tree benchmark completed: $test_name"
}

# Function to extract key metrics from perf output
extract_perf_metrics() {
    local perf_file=$1
    local system_name=$2
    local test_name=$3
    
    if [ -f "$perf_file" ]; then
        echo "=== $system_name - $test_name ===" >> "$COMPARISON_RESULTS_DIR/metrics_summary.txt"
        
        # Extract key metrics
        grep -E "(cache-misses|cache-references|cycles|instructions|page-faults|TLB-load-misses)" "$perf_file" | \
        while read line; do
            echo "$line" >> "$COMPARISON_RESULTS_DIR/metrics_summary.txt"
        done
        
        echo "" >> "$COMPARISON_RESULTS_DIR/metrics_summary.txt"
    fi
}

echo "=========================================="
echo "Starting Comparison Benchmarks"
echo "=========================================="

# Initialize metrics summary
echo "Performance Metrics Comparison" > "$COMPARISON_RESULTS_DIR/metrics_summary.txt"
echo "Generated: $(date)" >> "$COMPARISON_RESULTS_DIR/metrics_summary.txt"
echo "========================================" >> "$COMPARISON_RESULTS_DIR/metrics_summary.txt"

# Test 1: Single-threaded comparison
echo "Test 1: Single-threaded Performance"
run_vmcache_benchmark 1 2 16 500000 "rndread" "single_thread_rndread"
run_your_tree_benchmark 1 2147483648 500000 "search_random" "single_thread_search"

extract_perf_metrics "$COMPARISON_RESULTS_DIR/vmcache_single_thread_rndread.perf" "VMCache" "single_thread_rndread"
extract_perf_metrics "$COMPARISON_RESULTS_DIR/yourtree_single_thread_search.perf" "YourTree" "single_thread_search"

# Test 2: Multi-threaded scalability
echo "Test 2: Multi-threaded Scalability"
for threads in 4 8 16; do
    test_name="multithread_${threads}t"
    run_vmcache_benchmark $threads 4 32 1000000 "rndread" "$test_name"
    run_your_tree_benchmark $threads 4294967296 1000000 "search_random" "$test_name"
    
    extract_perf_metrics "$COMPARISON_RESULTS_DIR/vmcache_${test_name}.perf" "VMCache" "$test_name"
    extract_perf_metrics "$COMPARISON_RESULTS_DIR/yourtree_${test_name}.perf" "YourTree" "$test_name"
done

# Test 3: Memory pressure test
echo "Test 3: Memory Pressure Test"
run_vmcache_benchmark 8 1 16 2000000 "rndread" "memory_pressure"
run_your_tree_benchmark 8 1073741824 2000000 "search_random" "memory_pressure"

extract_perf_metrics "$COMPARISON_RESULTS_DIR/vmcache_memory_pressure.perf" "VMCache" "memory_pressure"
extract_perf_metrics "$COMPARISON_RESULTS_DIR/yourtree_memory_pressure.perf" "YourTree" "memory_pressure"

# Test 4: TPC-C style workload (if applicable)
echo "Test 4: TPC-C Style Workload"
run_vmcache_benchmark 4 2 16 10 "tpcc" "tpcc_workload"
run_your_tree_benchmark 4 2147483648 500000 "insert" "insert_workload"

extract_perf_metrics "$COMPARISON_RESULTS_DIR/vmcache_tpcc_workload.perf" "VMCache" "tpcc_workload"
extract_perf_metrics "$COMPARISON_RESULTS_DIR/yourtree_insert_workload.perf" "YourTree" "insert_workload"

echo "=========================================="
echo "Generating Comparison Report"
echo "=========================================="

# Create comparison report
cat > "$COMPARISON_RESULTS_DIR/comparison_report.md" << 'EOF'
# VMCache vs Your Tree Performance Comparison

## Test Environment
- **Date**: $(date)
- **System**: $(uname -a)
- **CPU**: $(lscpu | grep "Model name" | cut -d: -f2 | xargs)
- **Memory**: $(free -h | grep "Mem:" | awk '{print $2}')

## Test Configurations

### VMCache Configuration
- Uses ExMap kernel module for virtual memory management
- Direct I/O with hardware TLB acceleration
- Anonymous memory mapping with explicit control

### Your Tree Configuration  
- LRU cache with VolatileStorage
- Direct pointer arithmetic for memory access
- User-space memory management

## Key Metrics Comparison

The following metrics were collected for each test:
- **cache-misses**: L1/L2/L3 cache misses
- **cache-references**: Total cache references
- **cycles**: CPU cycles consumed
- **instructions**: Instructions executed
- **page-faults**: Page faults (important for VMCache)
- **TLB-load-misses**: TLB misses (VMCache advantage)

## Results Summary

See individual test files and metrics_summary.txt for detailed results.

### Expected Performance Characteristics:

**VMCache Advantages:**
- Lower TLB misses (hardware acceleration)
- Better scalability with high thread counts
- More efficient large working set handling

**Your Tree Advantages:**
- Lower setup overhead
- More predictable performance
- Better portability
- Simpler debugging

EOF

echo "=========================================="
echo "Comparison Complete!"
echo "=========================================="
echo "Results directory: $COMPARISON_RESULTS_DIR"
echo ""
echo "Key files:"
echo "- comparison_report.md: Overview and analysis"
echo "- metrics_summary.txt: Extracted performance metrics"
echo "- vmcache_*.txt: VMCache benchmark outputs"
echo "- yourtree_*.txt: Your tree benchmark outputs"
echo "- *.perf: Detailed perf profiling data"
echo ""
echo "To analyze results:"
echo "cd $COMPARISON_RESULTS_DIR"
echo "cat metrics_summary.txt"
echo "cat comparison_report.md"