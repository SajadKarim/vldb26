#!/bin/bash

# Comparison Script: Device-Aware vs Fixed Policy Selection
# This script demonstrates the performance difference between:
# 1. Device-aware automatic policy selection
# 2. Fixed policy selection (always using the same policy regardless of workload/storage)

BENCHMARK_DIR="/home/skarim/Code/haldendb_ex/haldendb_pvt_imp/benchmark/build"
BENCHMARK_EXEC="$BENCHMARK_DIR/benchmark"
POLICY_SELECTOR="$BENCHMARK_DIR/policy_selector_cli"

# Create timestamped output directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULTS_DIR="$BENCHMARK_DIR/comparison_results_${TIMESTAMP}"
mkdir -p "$RESULTS_DIR"

echo "=========================================="
echo "Device-Aware vs Fixed Policy Comparison"
echo "Results Directory: $RESULTS_DIR"
echo "=========================================="

# Test configuration (subset for quick comparison)
YCSB_WORKLOADS=("ycsb_a" "ycsb_c" "ycsb_e")
STORAGE_TYPES=("VolatileStorage" "PMemStorage")
FIXED_POLICIES=("LRU" "A2Q" "CLOCK")
FIXED_CONFIG="non_concurrent_default"

TREE_TYPE="BplusTreeSOA"
DEGREE=16
RECORDS=100000  # Smaller for quick comparison
RUNS=3
THREADS=4
PAGE_SIZE=4096
MEMORY_SIZE=34359738368
CACHE_SIZE_PERCENTAGE="1%"

DATA_BASE_DIR="/home/skarim/benchmark_data"

# Function to calculate cache size
calculate_cache_size() {
    local percentage=$1
    local record_count=$2
    local degree=${3:-64}
    
    local percent_value=${percentage%\%}
    local leaf_pages=$((record_count / (degree - 1)))
    [ $leaf_pages -lt 1 ] && leaf_pages=1
    
    local estimated_total_pages=$((leaf_pages * 115 / 100))
    local cache_size=$((estimated_total_pages * percent_value / 100))
    
    [ $cache_size -lt 10 ] && cache_size=10
    [ $cache_size -gt $estimated_total_pages ] && cache_size=$estimated_total_pages
    
    echo $cache_size
}

# Function to build configuration
build_cache_configuration() {
    local config_type=$1
    cd "$BENCHMARK_DIR"
    make clean > /dev/null 2>&1
    
    local RELEASE_OPTS="-O3 -DNDEBUG -march=native -mtune=native -mavx2"
    
    case "$config_type" in
        "non_concurrent_default")
            cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ $RELEASE_OPTS" > /dev/null 2>&1
            ;;
        "non_concurrent_relaxed")
            cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__SELECTIVE_UPDATE__ $RELEASE_OPTS" > /dev/null 2>&1
            ;;
        "non_concurrent_lru_metadata_update_in_order")
            cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__UPDATE_IN_ORDER__ $RELEASE_OPTS" > /dev/null 2>&1
            ;;
        *)
            cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ $RELEASE_OPTS" > /dev/null 2>&1
            ;;
    esac
    
    make -j$(nproc) > /dev/null 2>&1
    cd - > /dev/null
}

# Function to extract throughput from benchmark output
extract_throughput() {
    local output_file=$1
    # Extract throughput value (ops/sec) from benchmark output
    grep -oP "Throughput.*?(\d+\.?\d*)" "$output_file" | grep -oP "\d+\.?\d*" | head -1
}

# Function to run single benchmark
run_benchmark() {
    local workload=$1
    local storage=$2
    local policy=$3
    local config=$4
    local output_file=$5
    
    local cache_size=$(calculate_cache_size "$CACHE_SIZE_PERCENTAGE" "$RECORDS" "$DEGREE")
    
    cd "$DATA_BASE_DIR"
    
    "$BENCHMARK_EXEC" \
        --config "bm_tree_with_cache_ycsb" \
        --tree-type "$TREE_TYPE" \
        --cache-type "$policy" \
        --storage-type "$storage" \
        --cache-size "$cache_size" \
        --page-size "$PAGE_SIZE" \
        --memory-size "$MEMORY_SIZE" \
        --key-type "uint64_t" \
        --value-type "uint64_t" \
        --operation "$workload" \
        --degree "$DEGREE" \
        --records "$RECORDS" \
        --runs "$RUNS" \
        --threads "$THREADS" \
        > "$output_file" 2>&1
    
    cd - > /dev/null
}

# Create comparison CSV
COMPARISON_CSV="$RESULTS_DIR/comparison_results.csv"
echo "Workload,Storage,DeviceAware_Policy,DeviceAware_Config,DeviceAware_Throughput,Fixed_LRU_Throughput,Fixed_A2Q_Throughput,Fixed_CLOCK_Throughput,Best_Fixed,Improvement_vs_Best" > "$COMPARISON_CSV"

echo ""
echo "Running Comparison Benchmarks..."
echo "================================"
echo ""

# Run comparisons
for workload in "${YCSB_WORKLOADS[@]}"; do
    for storage in "${STORAGE_TYPES[@]}"; do
        echo "Testing: $workload x $storage"
        echo "------------------------------"
        
        # Get device-aware recommendation
        policy_output=$("$POLICY_SELECTOR" --workload "$workload" --storage "$storage")
        IFS=',' read -r da_policy da_config <<< "$policy_output"
        
        echo "  Device-Aware: $da_policy ($da_config)"
        
        # Run device-aware benchmark
        build_cache_configuration "$da_config"
        da_output="$RESULTS_DIR/${workload}_${storage}_device_aware.txt"
        run_benchmark "$workload" "$storage" "$da_policy" "$da_config" "$da_output"
        da_throughput=$(extract_throughput "$da_output")
        
        echo "    Throughput: $da_throughput ops/sec"
        
        # Run fixed policy benchmarks
        declare -A fixed_throughputs
        
        for fixed_policy in "${FIXED_POLICIES[@]}"; do
            echo "  Fixed $fixed_policy: $FIXED_CONFIG"
            build_cache_configuration "$FIXED_CONFIG"
            fixed_output="$RESULTS_DIR/${workload}_${storage}_fixed_${fixed_policy}.txt"
            run_benchmark "$workload" "$storage" "$fixed_policy" "$FIXED_CONFIG" "$fixed_output"
            fixed_throughput=$(extract_throughput "$fixed_output")
            fixed_throughputs[$fixed_policy]=$fixed_throughput
            echo "    Throughput: $fixed_throughput ops/sec"
        done
        
        # Find best fixed policy
        best_fixed_policy=""
        best_fixed_throughput=0
        for policy in "${FIXED_POLICIES[@]}"; do
            throughput=${fixed_throughputs[$policy]}
            if (( $(echo "$throughput > $best_fixed_throughput" | bc -l) )); then
                best_fixed_throughput=$throughput
                best_fixed_policy=$policy
            fi
        done
        
        # Calculate improvement
        improvement=$(echo "scale=2; (($da_throughput - $best_fixed_throughput) / $best_fixed_throughput) * 100" | bc -l)
        
        echo "  Best Fixed: $best_fixed_policy ($best_fixed_throughput ops/sec)"
        echo "  Improvement: ${improvement}%"
        echo ""
        
        # Write to CSV
        echo "$workload,$storage,$da_policy,$da_config,$da_throughput,${fixed_throughputs[LRU]},${fixed_throughputs[A2Q]},${fixed_throughputs[CLOCK]},$best_fixed_policy,$improvement" >> "$COMPARISON_CSV"
    done
done

echo ""
echo "=========================================="
echo "Comparison Complete!"
echo "=========================================="
echo ""
echo "Results saved to: $COMPARISON_CSV"
echo ""
echo "Summary:"
cat "$COMPARISON_CSV" | column -t -s ','
echo ""

# Generate summary statistics
echo "Generating Summary Statistics..."
echo "================================"

SUMMARY_FILE="$RESULTS_DIR/summary.txt"
{
    echo "Device-Aware vs Fixed Policy Comparison Summary"
    echo "Generated: $(date)"
    echo "=============================================="
    echo ""
    echo "Configuration:"
    echo "  Records: $RECORDS"
    echo "  Runs: $RUNS"
    echo "  Threads: $THREADS"
    echo "  Cache Size: $CACHE_SIZE_PERCENTAGE"
    echo ""
    echo "Results:"
    echo "--------"
    
    # Calculate average improvement
    avg_improvement=$(awk -F',' 'NR>1 {sum+=$NF; count++} END {if(count>0) print sum/count; else print 0}' "$COMPARISON_CSV")
    
    echo "Average Improvement over Best Fixed Policy: ${avg_improvement}%"
    echo ""
    
    # Count wins
    wins=$(awk -F',' 'NR>1 {if($NF>0) count++} END {print count}' "$COMPARISON_CSV")
    total=$(awk 'NR>1' "$COMPARISON_CSV" | wc -l)
    
    echo "Device-Aware wins: $wins / $total scenarios"
    echo ""
    
    echo "Detailed Results:"
    echo "-----------------"
    cat "$COMPARISON_CSV" | column -t -s ','
    
} > "$SUMMARY_FILE"

cat "$SUMMARY_FILE"

echo ""
echo "Full summary saved to: $SUMMARY_FILE"
echo ""