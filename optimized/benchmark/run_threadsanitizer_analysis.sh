#!/bin/bash

# ThreadSanitizer Analysis Script for Concurrent Deadlock Debugging
# This script builds the benchmark with ThreadSanitizer and runs targeted tests
# to isolate the exact race conditions and deadlock patterns in concurrent mode.

set -e  # Exit on any error

# Configuration
BENCHMARK_DIR="/home/skarim/Code/haldendb_ex/haldendb_pvt/benchmark/build"
BENCHMARK_EXEC="$BENCHMARK_DIR/benchmark"

# Create timestamped output directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
TSAN_OUTPUT_DIR="$BENCHMARK_DIR/tsan_analysis_${TIMESTAMP}"

echo "=========================================="
echo "ThreadSanitizer Analysis for Concurrent Deadlock"
echo "=========================================="
echo "Output Directory: $TSAN_OUTPUT_DIR"
echo "Timestamp: $TIMESTAMP"
echo "=========================================="

# Create output directory
mkdir -p "$TSAN_OUTPUT_DIR"

# Function to build with ThreadSanitizer
build_with_threadsanitizer() {
    echo "Building with ThreadSanitizer..."
    cd "$BENCHMARK_DIR"
    
    # Clean previous build
    make clean > /dev/null 2>&1 || true
    
    # Configure with ThreadSanitizer
    echo "Configuring with ThreadSanitizer flags..."
    cmake .. -DCMAKE_BUILD_TYPE=Debug \
             -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__CACHE_COUNTERS__ -D__ENABLE_ASSERTS__ -fsanitize=thread -g -O1 -fno-omit-frame-pointer" \
             -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"
    
    # Build
    echo "Building..."
    make -j$(nproc)
    
    if [ $? -eq 0 ]; then
        echo "✓ Build completed successfully with ThreadSanitizer"
    else
        echo "✗ Build failed"
        exit 1
    fi
    
    echo ""
}

# Function to run a single ThreadSanitizer test
run_tsan_test() {
    local test_name="$1"
    local cache_size="$2"
    local threads="$3"
    local records="$4"
    local degree="$5"
    local timeout_sec="${6:-60}"
    
    echo "Running ThreadSanitizer Test: $test_name"
    echo "  Cache Size: $cache_size"
    echo "  Threads: $threads"
    echo "  Records: $records"
    echo "  Degree: $degree"
    echo "  Timeout: ${timeout_sec}s"
    
    local log_file="$TSAN_OUTPUT_DIR/tsan_${test_name}.log"
    local summary_file="$TSAN_OUTPUT_DIR/tsan_${test_name}_summary.txt"
    
    # Set ThreadSanitizer options for comprehensive analysis
    export TSAN_OPTIONS="halt_on_error=1:abort_on_error=1:print_stacktrace=1:second_deadlock_stack=1:detect_deadlocks=1:report_bugs=1:history_size=7:io_sync=1"
    
    cd "$BENCHMARK_DIR"
    
    echo "Starting test (timeout: ${timeout_sec}s)..."
    timeout ${timeout_sec}s ./benchmark \
        --config "bm_cache" \
        --cache-type "CLOCK" \
        --storage-type "VolatileStorage" \
        --cache-size "$cache_size" \
        --page-size "4096" \
        --memory-size "134217728" \
        --tree-type "BplusTreeSOA" \
        --key-type "uint64_t" \
        --value-type "uint64_t" \
        --operation "insert" \
        --degree "$degree" \
        --records "$records" \
        --runs "1" \
        --threads "$threads" \
        --output-dir "$TSAN_OUTPUT_DIR" 2>&1 | tee "$log_file"
    
    local exit_code=$?
    
    # Create summary
    echo "Test: $test_name" > "$summary_file"
    echo "Exit Code: $exit_code" >> "$summary_file"
    echo "Parameters: cache_size=$cache_size, threads=$threads, records=$records, degree=$degree" >> "$summary_file"
    echo "Log File: $log_file" >> "$summary_file"
    echo "" >> "$summary_file"
    
    # Extract ThreadSanitizer warnings
    if grep -q "WARNING: ThreadSanitizer:" "$log_file"; then
        echo "ThreadSanitizer Issues Found:" >> "$summary_file"
        grep -A 10 "WARNING: ThreadSanitizer:" "$log_file" >> "$summary_file"
    else
        echo "No ThreadSanitizer warnings found" >> "$summary_file"
    fi
    
    # Extract deadlock information
    if grep -q "DEADLOCK" "$log_file"; then
        echo "" >> "$summary_file"
        echo "Deadlock Information:" >> "$summary_file"
        grep -A 20 "DEADLOCK" "$log_file" >> "$summary_file"
    fi
    
    # Extract data race information
    if grep -q "data race" "$log_file"; then
        echo "" >> "$summary_file"
        echo "Data Race Information:" >> "$summary_file"
        grep -A 15 "data race" "$log_file" >> "$summary_file"
    fi
    
    echo "Test completed: $test_name (exit code: $exit_code)"
    echo "  Log: $log_file"
    echo "  Summary: $summary_file"
    echo ""
    
    unset TSAN_OPTIONS
    return $exit_code
}

# Main analysis function
run_analysis() {
    echo "Starting comprehensive ThreadSanitizer analysis..."
    echo ""
    
    # Build with ThreadSanitizer
    build_with_threadsanitizer
    
    # Test 1: Minimal contention (baseline)
    echo "=========================================="
    echo "Test 1: Minimal Contention (Baseline)"
    echo "=========================================="
    run_tsan_test "minimal_contention" 200 2 5000 128 30
    
    # Test 2: High contention (small cache, many threads)
    echo "=========================================="
    echo "Test 2: High Contention (Small Cache)"
    echo "=========================================="
    run_tsan_test "high_contention" 25 8 10000 64 60
    
    # Test 3: Cache pressure (medium cache, medium threads, many records)
    echo "=========================================="
    echo "Test 3: Cache Pressure"
    echo "=========================================="
    run_tsan_test "cache_pressure" 100 4 20000 128 90
    
    # Test 4: Extreme contention (tiny cache, many threads)
    echo "=========================================="
    echo "Test 4: Extreme Contention (Tiny Cache)"
    echo "=========================================="
    run_tsan_test "extreme_contention" 10 12 5000 32 45
    
    # Test 5: Large degree (different code paths)
    echo "=========================================="
    echo "Test 5: Large Degree (Different Code Paths)"
    echo "=========================================="
    run_tsan_test "large_degree" 50 6 15000 256 75
}

# Generate final report
generate_report() {
    echo "=========================================="
    echo "Generating Final ThreadSanitizer Report"
    echo "=========================================="
    
    local report_file="$TSAN_OUTPUT_DIR/TSAN_ANALYSIS_REPORT.md"
    
    cat > "$report_file" << EOF
# ThreadSanitizer Analysis Report

**Generated:** $(date)
**Output Directory:** $TSAN_OUTPUT_DIR

## Summary

This report contains ThreadSanitizer analysis results for the concurrent CLOCK cache deadlock issue.

## Test Results

EOF
    
    # Add results for each test
    for summary_file in "$TSAN_OUTPUT_DIR"/tsan_*_summary.txt; do
        if [ -f "$summary_file" ]; then
            echo "### $(basename "$summary_file" _summary.txt)" >> "$report_file"
            echo '```' >> "$report_file"
            cat "$summary_file" >> "$report_file"
            echo '```' >> "$report_file"
            echo "" >> "$report_file"
        fi
    done
    
    cat >> "$report_file" << EOF

## Analysis Instructions

1. **Look for ThreadSanitizer warnings** in the log files:
   - \`WARNING: ThreadSanitizer: data race\` - indicates race conditions
   - \`WARNING: ThreadSanitizer: lock-order-inversion\` - indicates potential deadlocks
   - \`WARNING: ThreadSanitizer: thread leak\` - indicates thread management issues

2. **Check for deadlock patterns**:
   - Look for circular lock dependencies
   - Identify the specific mutexes involved
   - Note the call stacks where locks are acquired

3. **Examine data races**:
   - Check which variables are accessed without proper synchronization
   - Look for atomic operations that might need stronger memory ordering

4. **Key files to examine**:
   - CLOCKCache.hpp: Cache-level locking (m_mtxCache)
   - CLOCKCacheObject.hpp: Object-level locking (m_mtx)
   - IndexNode.hpp: Tree node locking patterns

## Next Steps

Based on the ThreadSanitizer findings:
1. Fix lock ordering issues by establishing a consistent hierarchy
2. Address data races in atomic operations
3. Review condition variable usage patterns
4. Consider lock-free alternatives for high-contention operations

EOF
    
    echo "✓ Final report generated: $report_file"
    echo ""
    echo "=========================================="
    echo "ThreadSanitizer Analysis Complete"
    echo "=========================================="
    echo "Results directory: $TSAN_OUTPUT_DIR"
    echo "Main report: $report_file"
    echo ""
    echo "Key files to examine:"
    ls -la "$TSAN_OUTPUT_DIR"/*.log 2>/dev/null | head -10 || echo "No log files found"
    echo ""
    echo "To view the main report:"
    echo "  cat '$report_file'"
    echo ""
    echo "To examine specific test logs:"
    echo "  less '$TSAN_OUTPUT_DIR/tsan_<test_name>.log'"
}

# Main execution
main() {
    echo "ThreadSanitizer Analysis for HaldenDB Concurrent Cache Deadlock"
    echo "================================================================"
    echo ""
    
    # Check if benchmark directory exists
    if [ ! -d "$BENCHMARK_DIR" ]; then
        echo "Error: Benchmark directory not found: $BENCHMARK_DIR"
        echo "Please ensure the benchmark is built first."
        exit 1
    fi
    
    # Run the analysis
    run_analysis
    
    # Generate final report
    generate_report
    
    echo "Analysis completed successfully!"
    echo "Check the report and log files for detailed race condition information."
}

# Run main function
main "$@"