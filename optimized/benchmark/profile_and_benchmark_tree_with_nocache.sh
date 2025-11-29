#!/bin/bash

# Profiling Benchmark Script
# This script runs individual tree/operation combinations with perf profiling

# Configuration
BENCHMARK_DIR="/home/skarim/Code/haldendb_ex/haldendb_pvt/benchmark/build"
BENCHMARK_EXEC="$BENCHMARK_DIR/benchmark"

# Create timestamped output directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
PROFILE_OUTPUT_DIR="$BENCHMARK_DIR/profiling_results_${TIMESTAMP}"

# Create output directory
mkdir -p "$PROFILE_OUTPUT_DIR"

echo "=========================================="
echo "Profiling Results Directory: $PROFILE_OUTPUT_DIR"
echo "=========================================="



# Tree types to profile
TREES=("BplusTreeSOA" "BplusTreeAOS" "BepsilonTreeSOA" "BepsilonTreeAOS" "BepsilonTreeSOALazyNodes" "BepsilonTreeSOALazyIndex" "BepsilonTreeSOAII")

# Degrees to test (subset for profiling)
DEGREES=(32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256)

# Operations to profile
OPERATIONS=("insert" "search_random" "search_sequential" "search_uniform" "search_zipfian" "delete")

# Key-Value type combinations
declare -A KEY_VALUE_COMBOS
KEY_VALUE_COMBOS["uint64_t_uint64_t"]="uint64_t uint64_t"
KEY_VALUE_COMBOS["uint64_t_char16"]="uint64_t char16"
KEY_VALUE_COMBOS["char16_char16"]="char16 char16"

# Record count for profiling (smaller for detailed analysis)
RECORDS=(500000 1000000 5000000)
RUNS=${RUNS:-5}  # Default to 1, but allow override via environment variable

# Perf events to collect
PERF_EVENTS="cache-misses,cache-references,cycles,instructions,branch-misses,page-faults,L1-dcache-load-misses,L1-dcache-loads,LLC-load-misses,LLC-loads"

# Function to run single benchmark with perf profiling
run_profiled_benchmark() {
    local tree_type=$1
    local key_type=$2
    local value_type=$3
    local operation=$4
    local degree=$5
    local records=$6
    
    local profile_name="${tree_type}_${key_type}_${value_type}_${operation}_${degree}_${records}"
    local perf_output="$PROFILE_OUTPUT_DIR/${profile_name}.prf"
    local perf_data="$PROFILE_OUTPUT_DIR/${profile_name}.data"
    
    echo "=========================================="
    echo "Profiling: $tree_type - $operation - Degree $degree"
    echo "Key: $key_type, Value: $value_type, Records: $records"
    echo "Output: $perf_output"
    echo "=========================================="
    
    # Run benchmark with perf stat (statistical profiling)
    cd "$BENCHMARK_DIR"
    perf stat -e "$PERF_EVENTS" \
              -o "$perf_output" \
              numactl --cpunodebind=1 --membind=1 \
              ./benchmark \
              --config "bm_nocache" \
              --tree-type "$tree_type" \
              --key-type "$key_type" \
              --value-type "$value_type" \
              --operation "$operation" \
              --degree "$degree" \
              --records "$records" \
              --runs "$RUNS" \
              --output-dir "$PROFILE_OUTPUT_DIR"
    
    # No need to move CSV files anymore - they're created directly in the target directory
    
    # Also run with perf record for detailed analysis (optional)
    if [ "$DETAILED_PROFILING" = "true" ]; then
        echo "Running detailed profiling with perf record..."
        perf record -e cycles,cache-misses \
                    -o "$perf_data" \
                    numactl --cpunodebind=1 --membind=1 \
                    ./benchmark \
                    --config "bm_nocache" \
                    --tree-type "$tree_type" \
                    --key-type "$key_type" \
                    --value-type "$value_type" \
                    --operation "$operation" \
                    --degree "$degree" \
                    --records "$records" \
                    --runs "$RUNS" \
                    --output-dir "$PROFILE_OUTPUT_DIR"
        
        # Clean up any additional CSV files from detailed profiling
        local additional_csvs=$(ls -t "$PROFILE_OUTPUT_DIR"/${profile_name}_*.csv 2>/dev/null | tail -n +2)
        for csv_file in $additional_csvs; do
            [ -f "$csv_file" ] && rm "$csv_file"
        done
    fi
    
    # Rename the CSV file to match the perf file naming convention (remove timestamp)
    local latest_csv=$(ls -t "$PROFILE_OUTPUT_DIR"/${profile_name}_*.csv 2>/dev/null | head -1)
    if [ -f "$latest_csv" ]; then
        local target_csv="$PROFILE_OUTPUT_DIR/${profile_name}.csv"
        mv "$latest_csv" "$target_csv"
        echo "CSV file renamed to: $(basename "$target_csv")"
    fi
    
    echo "Profiling completed for $profile_name"
    echo ""
}

# Function to run profiling for all combinations
run_full_profiling() {
    echo "Starting comprehensive profiling..."
    echo "Trees: ${TREES[*]}"
    echo "Degrees: ${DEGREES[*]}"
    echo "Operations: ${OPERATIONS[*]}"
    echo "Records: ${RECORDS[*]}"
    echo ""
    
    local total_combinations=0
    local current_combination=0
    
    # Calculate total combinations
    for combo_name in "${!KEY_VALUE_COMBOS[@]}"; do
        IFS=' ' read -r key_type value_type <<< "${KEY_VALUE_COMBOS[$combo_name]}"
        for tree in "${TREES[@]}"; do
            for degree in "${DEGREES[@]}"; do
                for operation in "${OPERATIONS[@]}"; do
                    for records in "${RECORDS[@]}"; do
                        ((total_combinations++))
                    done
                done
            done
        done
    done
    
    echo "Total combinations to profile: $total_combinations"
    echo ""
    
    # Run profiling for each combination
    for combo_name in "${!KEY_VALUE_COMBOS[@]}"; do
        IFS=' ' read -r key_type value_type <<< "${KEY_VALUE_COMBOS[$combo_name]}"
        
        echo "Processing key-value combination: $key_type -> $value_type"
        
        for tree in "${TREES[@]}"; do
            for degree in "${DEGREES[@]}"; do
                for records in "${RECORDS[@]}"; do
                    for operation in "${OPERATIONS[@]}"; do
                        ((current_combination++))
                        echo "Progress: $current_combination/$total_combinations"
                        
                        run_profiled_benchmark "$tree" "$key_type" "$value_type" "$operation" "$degree" "$records"
                        
                        # Small delay between runs to let system settle
                        sleep 10
                    done
                done
            done
        done
    done
    
    echo "=========================================="
    echo "Profiling completed!"
    echo "Results saved in: $PROFILE_OUTPUT_DIR"
    echo "Total profiles generated: $total_combinations"
    echo "=========================================="
}

# Function to run profiling for specific configuration
run_single_profiling() {
    local tree_type=$1
    local key_type=${2:-"uint64_t"}
    local value_type=${3:-"uint64_t"}
    local operation=$4
    local degree=${5:-64}
    local records=${6:-${RECORDS[0]}}  # Use first record count as default
    local runs=${7:-$RUNS}  # Allow override of runs for single profiling
    
    if [ -z "$tree_type" ] || [ -z "$operation" ]; then
        echo "Usage: $0 single <tree_type> [key_type] [value_type] <operation> [degree] [records] [runs]"
        echo "Example: $0 single BplusTreeSOA uint64_t uint64_t insert 64 100000 3"
        exit 1
    fi
    
    # Temporarily override RUNS for this single run
    local original_runs=$RUNS
    RUNS=$runs
    
    run_profiled_benchmark "$tree_type" "$key_type" "$value_type" "$operation" "$degree" "$records"
    
    # Restore original RUNS
    RUNS=$original_runs
}

# Function to run quick benchmark for all configurations with 100K and 500K records
run_quick_benchmark() {
    echo "=========================================="
    echo "Quick Benchmark Run - All Configurations"
    echo "=========================================="
    echo "Trees: ${TREES[*]}"
    echo "Degrees: ${DEGREES[*]}"
    echo "Operations: ${OPERATIONS[*]}"
    echo "Record Counts: 100000, 500000"
    echo "Key/Value Types: uint64_t/uint64_t"
    echo "Runs per config: 1"
    
    local record_counts=(100000 500000)
    local total_combinations=$((${#TREES[@]} * ${#DEGREES[@]} * ${#OPERATIONS[@]} * ${#record_counts[@]}))
    echo "Total combinations: $total_combinations"
    echo "=========================================="
    
    # Create consolidated CSV file
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local consolidated_csv="$PROFILE_OUTPUT_DIR/quick_benchmark_results_${timestamp}.csv"
    echo "tree_type,key_type,value_type,workload_type,record_count,degree,operation,time_us,throughput_ops_sec,test_run_id,timestamp" > "$consolidated_csv"
    
    local current_combination=0
    local start_time=$(date +%s)
    
    for records in "${record_counts[@]}"; do
        echo ""
        echo "=== Processing Record Count: $records ==="
        
        for tree in "${TREES[@]}"; do
            for degree in "${DEGREES[@]}"; do
                for operation in "${OPERATIONS[@]}"; do
                    ((current_combination++))
                    
                    echo "[$current_combination/$total_combinations] Testing: $tree - $operation - Degree $degree - Records $records"
                    
                    cd "$BENCHMARK_DIR"
                    
                    # Run benchmark without perf profiling for speed
                    ./benchmark \
                        --config "bm_nocache" \
                        --tree-type "$tree" \
                        --key-type "uint64_t" \
                        --value-type "uint64_t" \
                        --operation "$operation" \
                        --degree "$degree" \
                        --records "$records" \
                        --runs 1 > /dev/null 2>&1
                    
                    # Find the most recent CSV file and append to consolidated results
                    local latest_csv=$(ls -t benchmark_single_*.csv 2>/dev/null | head -1)
                    if [ -f "$latest_csv" ]; then
                        # Skip header and append data
                        tail -n +2 "$latest_csv" >> "$consolidated_csv"
                        rm "$latest_csv"  # Clean up individual file
                    else
                        echo "  WARNING: No CSV output found for this configuration"
                    fi
                    
                    # Small delay to let system settle
                    sleep 0.5
                done
            done
        done
    done
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    local minutes=$((duration / 60))
    local seconds=$((duration % 60))
    
    echo ""
    echo "=========================================="
    echo "Quick Benchmark Run Completed!"
    echo "=========================================="
    echo "Total combinations tested: $total_combinations"
    echo "Total execution time: ${minutes}m ${seconds}s"
    echo "Results saved to: $consolidated_csv"
    
    # Generate summary statistics if Python is available
    echo ""
    echo "Generating summary statistics..."
    python3 -c "
import pandas as pd
import sys

try:
    df = pd.read_csv('$consolidated_csv')
    
    print('\\n=== SUMMARY STATISTICS ===')
    print(f'Total test runs: {len(df)}')
    print(f'Trees tested: {df[\"tree_type\"].nunique()}')
    print(f'Operations tested: {df[\"operation\"].nunique()}')
    print(f'Degrees tested: {df[\"degree\"].nunique()}')
    print(f'Record counts tested: {df[\"record_count\"].nunique()}')
    
    print('\\n=== TOP 10 HIGHEST THROUGHPUT CONFIGURATIONS ===')
    top_throughput = df.nlargest(10, 'throughput_ops_sec')[['tree_type', 'operation', 'degree', 'record_count', 'throughput_ops_sec']]
    print(top_throughput.to_string(index=False))
    
    print('\\n=== TREE TYPE COMPARISON (Average Throughput) ===')
    tree_comparison = df.groupby('tree_type')['throughput_ops_sec'].mean().sort_values(ascending=False)
    for tree, throughput in tree_comparison.items():
        print(f'{tree:25}: {throughput:>12,.0f} ops/sec')
    
    print('\\n=== OPERATION COMPARISON (Average Throughput) ===')
    op_comparison = df.groupby('operation')['throughput_ops_sec'].mean().sort_values(ascending=False)
    for op, throughput in op_comparison.items():
        print(f'{op:20}: {throughput:>12,.0f} ops/sec')
        
except Exception as e:
    print(f'Error generating summary: {e}')
    print('Raw CSV file is available at: $consolidated_csv')
" 2>/dev/null || echo "Python pandas not available. Raw results in: $consolidated_csv"
    
    echo ""
    echo "Quick benchmark run completed successfully!"
    echo "Check the consolidated results at: $consolidated_csv"
}

# Function to analyze perf results
analyze_results() {
    echo "Analyzing perf results..."
    
    # Create summary CSV
    local summary_file="$PROFILE_OUTPUT_DIR/perf_summary.csv"
    echo "tree_type,key_type,value_type,operation,degree,records,cache_misses,cache_references,cache_miss_rate,cycles,instructions,ipc,branch_misses,page_faults" > "$summary_file"
    
    for perf_file in "$PROFILE_OUTPUT_DIR"/perf_*.txt; do
        if [ -f "$perf_file" ]; then
            # Extract configuration from filename
            local basename=$(basename "$perf_file" .txt)
            local config=${basename#perf_}
            
            # Parse perf output and extract metrics
            local cache_misses=$(grep "cache-misses" "$perf_file" | awk '{print $1}' | tr -d ',')
            local cache_references=$(grep "cache-references" "$perf_file" | awk '{print $1}' | tr -d ',')
            local cycles=$(grep "cycles" "$perf_file" | awk '{print $1}' | tr -d ',')
            local instructions=$(grep "instructions" "$perf_file" | awk '{print $1}' | tr -d ',')
            local branch_misses=$(grep "branch-misses" "$perf_file" | awk '{print $1}' | tr -d ',')
            local page_faults=$(grep "page-faults" "$perf_file" | awk '{print $1}' | tr -d ',')
            
            # Calculate derived metrics
            local cache_miss_rate=""
            local ipc=""
            
            if [ -n "$cache_misses" ] && [ -n "$cache_references" ] && [ "$cache_references" != "0" ]; then
                cache_miss_rate=$(echo "scale=4; $cache_misses / $cache_references * 100" | bc -l)
            fi
            
            if [ -n "$cycles" ] && [ -n "$instructions" ] && [ "$cycles" != "0" ]; then
                ipc=$(echo "scale=4; $instructions / $cycles" | bc -l)
            fi
            
            # Parse configuration (tree_keytype_valuetype_operation_degree_records)
            IFS='_' read -r tree_type key_type value_type operation degree records <<< "$config"
            
            # Add to summary
            echo "$tree_type,$key_type,$value_type,$operation,$degree,$records,$cache_misses,$cache_references,$cache_miss_rate,$cycles,$instructions,$ipc,$branch_misses,$page_faults" >> "$summary_file"
        fi
    done
    
    echo "Analysis completed. Summary saved to: $summary_file"
}

# Main script logic
case "${1:-full}" in
    "full")
        run_full_profiling
        analyze_results
        ;;
    "single")
        shift
        run_single_profiling "$@"
        ;;
    "quick")
        run_quick_benchmark
        ;;
    "analyze")
        analyze_results
        ;;
    "help"|"--help"|"-h")
        echo "Usage: $0 [command] [options]"
        echo ""
        echo "Commands:"
        echo "  full                    Run full profiling suite (default)"
        echo "  single <tree> <op>      Run single configuration"
        echo "  quick                   Run all trees/operations for 100K and 500K records"
        echo "  analyze                 Analyze existing perf results"
        echo "  help                    Show this help"
        echo ""
        echo "Environment variables:"
        echo "  DETAILED_PROFILING=true Enable perf record (detailed profiling)"
        echo "  RUNS=<number>           Set number of runs (default: 1)"
        echo ""
        echo "Examples:"
        echo "  $0                                          # Run full profiling"
        echo "  $0 single BplusTreeSOA insert              # Profile single config"
        echo "  $0 single BplusTreeSOA uint64_t uint64_t search_random 128 3  # 3 runs"
        echo "  $0 quick                                    # Quick run all configs (100K+500K)"
        echo "  DETAILED_PROFILING=true $0 full            # Full with detailed profiling"
        echo "  RUNS=5 $0 full                             # Full profiling with 5 runs each"
        ;;
    *)
        echo "Unknown command: $1"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac