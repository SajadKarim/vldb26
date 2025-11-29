#!/bin/bash

# Device-Aware YCSB Benchmark Script
# This script demonstrates automatic cache policy selection using DeviceAwarePolicy
# It queries the policy_selector_cli to determine optimal cache configurations
# and automatically rebuilds with the correct flags for each workload/storage combination

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="$SCRIPT_DIR/build"
BENCHMARK_EXEC="$BENCHMARK_DIR/benchmark"
POLICY_SELECTOR="$BENCHMARK_DIR/policy_selector_cli"

# Create timestamped output directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
PROFILE_OUTPUT_DIR="$BENCHMARK_DIR/device_aware_ycsb_results_${TIMESTAMP}"
mkdir -p "$PROFILE_OUTPUT_DIR"

echo "=========================================="
echo "Device-Aware YCSB Benchmark"
echo "Results Directory: $PROFILE_OUTPUT_DIR"
echo "=========================================="

# Benchmark configuration
YCSB_WORKLOADS=("ycsb_a" "ycsb_b" "ycsb_c" "ycsb_d" "ycsb_e" "ycsb_f")
STORAGE_TYPES=("VolatileStorage" "PMemStorage" "FileStorage")
TREE_TYPE="BplusTreeSOA"
DEGREE=24
RECORDS=500000
RUNS=10
THREADS=1
PAGE_SIZE=4096
MEMORY_SIZE=34359738368
CACHE_SIZE_PERCENTAGES=("2%" "10%" "25%")

# Data directory configuration
DATA_BASE_DIR="${BENCHMARK_DATA_DIR:-$HOME/benchmark_data}"
DATA_DIR="$DATA_BASE_DIR/data"
YCSB_DIR="$DATA_BASE_DIR/ycsb"

# Perf events to collect (cache-focused)
PERF_EVENTS="cache-misses,cache-references,cycles,instructions,branch-misses,page-faults,L1-dcache-load-misses,L1-dcache-loads,LLC-load-misses,LLC-loads,L1-icache-load-misses,L1-icache-loads"

# Function to calculate cache size from percentage
calculate_cache_size() {
    local percentage=$1
    local record_count=$2
    local degree=${3:-64}
    
    local percent_value=${percentage%\%}
    local leaf_pages=$((record_count / (degree - 1)))
    if [ $leaf_pages -lt 1 ]; then
        leaf_pages=1
    fi
    
    local estimated_total_pages=$((leaf_pages * 115 / 100))
    local cache_size=$((estimated_total_pages * percent_value / 100))
    
    if [ $cache_size -lt 10 ]; then
        cache_size=10
    fi
    
    if [ $cache_size -gt $estimated_total_pages ]; then
        cache_size=$estimated_total_pages
    fi
    
    echo $cache_size
}

# Function to build with specific cache configuration
build_cache_configuration() {
    local config_type=$1
    
    echo "========================================="
    echo "Building Cache Configuration: $config_type"
    echo "========================================="
    
    cd "$BENCHMARK_DIR"
    echo "Cleaning previous build..."
    make clean
    
    local RELEASE_OPTS="-O3 -DNDEBUG -march=native -mtune=native -mavx2"
    
    case "$config_type" in
        "non_concurrent_default")
            cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__  -D__CACHE_COUNTERS__ $RELEASE_OPTS"
            ;;
        "non_concurrent_relaxed")
            cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__SELECTIVE_UPDATE__  -D__CACHE_COUNTERS__ $RELEASE_OPTS"
            ;;
        "non_concurrent_lru_metadata_update_in_order")
            cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__UPDATE_IN_ORDER__  -D__CACHE_COUNTERS__ $RELEASE_OPTS"
            ;;
        "non_concurrent_lru_metadata_update_in_order_and_relaxed")
            cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__UPDATE_IN_ORDER__ -D__SELECTIVE_UPDATE__  -D__CACHE_COUNTERS__ $RELEASE_OPTS"
            ;;
        "non_concurrent_a2q_ghost_q_enabled")
            cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__MANAGE_GHOST_Q__  -D__CACHE_COUNTERS__ $RELEASE_OPTS"
            ;;
        "non_concurrent_a2q_ghost_q_enabled_and_relaxed")
            cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__MANAGE_GHOST_Q__ -D__SELECTIVE_UPDATE__  -D__CACHE_COUNTERS__ $RELEASE_OPTS"
            ;;
        "non_concurrent_clock_with_buffer")
            cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CLOCK_WITH_BUFFER__  -D__CACHE_COUNTERS__ $RELEASE_OPTS"
            ;;
        "concurrent_default")
            cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__  -D__CACHE_COUNTERS__ $RELEASE_OPTS"
            ;;
        *)
            echo "Unknown configuration: $config_type"
            cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__  -D__CACHE_COUNTERS__ $RELEASE_OPTS"
            ;;
    esac
    
    echo "Compiling with $(nproc) parallel jobs..."
    make -j$(nproc)
    
    if [ $? -eq 0 ]; then
        echo "✓ Build completed successfully for $config_type"
    else
        echo "✗ Build failed for $config_type"
        exit 1
    fi
    
    cd - > /dev/null
}

# Function to run benchmark with device-aware policy selection
run_device_aware_benchmark() {
    local workload=$1
    local storage=$2
    local cache_size_percentage=$3
    
    echo ""
    echo "=========================================="
    echo "Workload: $workload"
    echo "Storage: $storage"
    echo "Cache Size: $cache_size_percentage"
    echo "=========================================="
    
    # Query the policy selector for optimal configuration
    local policy_output=$("$POLICY_SELECTOR" --workload "$workload" --storage "$storage")
    
    if [ $? -ne 0 ]; then
        echo "✗ Failed to query policy selector"
        return 1
    fi
    
    # Parse the output (format: "POLICY,CONFIG")
    IFS=',' read -r policy_type config_name <<< "$policy_output"
    
    echo "Device-Aware Selection:"
    echo "  Policy: $policy_type"
    echo "  Config: $config_name"
    
    # Get verbose information for logging
    local verbose_output=$("$POLICY_SELECTOR" --workload "$workload" --storage "$storage" --verbose)
    echo "$verbose_output" > "$PROFILE_OUTPUT_DIR/${workload}_${storage}_${cache_size_percentage}_policy_selection.txt"
    
    # Build with the selected configuration
    build_cache_configuration "$config_name"
    
    # Calculate cache size
    local cache_size=$(calculate_cache_size "$cache_size_percentage" "$RECORDS" "$DEGREE")
    
    # Create output directory for this run
    local run_dir="$PROFILE_OUTPUT_DIR/${workload}_${storage}_${cache_size_percentage}_${policy_type}_${config_name}"
    mkdir -p "$run_dir"
    
    echo ""
    echo "Running benchmark..."
    echo "  Cache Type: $policy_type"
    echo "  Cache Size: $cache_size"
    echo "  Records: $RECORDS"
    echo "  Runs: $RUNS"
    echo "  Threads: $THREADS"
    
    # Change to data directory for benchmark execution
    cd "$DATA_BASE_DIR"
    
    # Run the benchmark with perf profiling
    local output_file="$run_dir/benchmark_output.txt"
    local perf_output="$run_dir/${config_name}_${TREE_TYPE}_${policy_type}_${storage}_${workload}.prf"
    
    perf stat -e "$PERF_EVENTS" \
              -o "$perf_output" \
              numactl --cpunodebind=0 --membind=0 \
              "$BENCHMARK_EXEC" \
              --config "bm_cache_ycsb" \
              --tree-type "$TREE_TYPE" \
              --cache-type "$policy_type" \
              --storage-type "$storage" \
              --cache-size "$cache_size" \
              --page-size "$PAGE_SIZE" \
              --memory-size "$MEMORY_SIZE" \
              --key-type "uint64_t" \
              --value-type "uint64_t" \
              --workload-type "$workload" \
              --degree "$DEGREE" \
              --records "$RECORDS" \
              --runs "$RUNS" \
              --threads "$THREADS" \
              --output-dir "$run_dir" \
              --config-name "device_aware_${config_name}" \
              --cache-size-percentage "$cache_size_percentage" \
              --cache-page-limit "$cache_size" \
              > "$output_file" 2>&1
    
    if [ $? -eq 0 ]; then
        echo "✓ Benchmark completed successfully"
        
        # List generated files
        echo ""
        echo "Generated files:"
        ls -lh "$run_dir"/*.csv "$run_dir"/*.prf 2>/dev/null || echo "  (CSV/PRF files will be in: $run_dir)"
        
        # Extract key metrics from CSV if available
        local csv_file=$(ls "$run_dir"/*.csv 2>/dev/null | head -1)
        if [ -f "$csv_file" ]; then
            echo ""
            echo "Results Summary (from CSV):"
            head -20 "$csv_file"
        fi
    else
        echo "✗ Benchmark failed"
        echo "Check output: $output_file"
        cat "$output_file"
    fi
    
    cd - > /dev/null
    echo ""
}

# Function to combine all CSV files with perf data
combine_csv_files() {
    local target_dir=${1:-$PROFILE_OUTPUT_DIR}
    
    echo "=========================================="
    echo "Combining all CSV files from YCSB benchmark runs with perf data"
    echo "=========================================="
    
    # Check if target directory exists and has content
    if [ ! -d "$target_dir" ] || [ -z "$(ls -A "$target_dir" 2>/dev/null)" ]; then
        echo "Target directory empty or not found: $target_dir"
        return 1
    fi
    
    # Create combined CSV file with timestamp
    local merge_timestamp=$(date +"%Y%m%d_%H%M%S")
    local combined_csv="$target_dir/combined_device_aware_ycsb_results_with_perf_${merge_timestamp}.csv"
    local temp_dir="$target_dir/temp_merge_$$"
    mkdir -p "$temp_dir"
    
    # Initialize header written flag
    local header_written=false
    local total_files=0
    local processed_files=0
    
    # Count total CSV files first
    echo "Scanning for CSV files in: $target_dir"
    for csv_file in "$target_dir"/*/*.csv; do
        if [ -f "$csv_file" ]; then
            ((total_files++))
        fi
    done
    
    if [ $total_files -eq 0 ]; then
        echo "No CSV files found in $target_dir"
        echo "Make sure benchmark runs have completed successfully."
        rm -rf "$temp_dir"
        return 1
    fi
    
    echo "Found $total_files CSV files to merge"
    echo "Combined file will be saved as: $combined_csv"
    echo ""
    
    # Process each CSV file
    for csv_file in "$target_dir"/*/*.csv; do
        if [ -f "$csv_file" ]; then
            ((processed_files++))
            local folder_name=$(basename "$(dirname "$csv_file")")
            local base_name=$(basename "$csv_file" .csv)
            
            # Look for corresponding .prf file
            # First try with the same base name as CSV
            local prf_file="$(dirname "$csv_file")/${base_name}.prf"
            
            # If not found, try to find any .prf file in the same directory
            if [ ! -f "$prf_file" ]; then
                prf_file=$(find "$(dirname "$csv_file")" -name "*.prf" -type f | head -1)
            fi
            
            local temp_csv="$temp_dir/temp_${processed_files}.csv"
            
            echo "Processing [$processed_files/$total_files]: $folder_name"
            echo "  CSV: $(basename "$csv_file")"
            if [ -f "$prf_file" ]; then
                echo "  PRF: $(basename "$prf_file")"
                
                # Extract perf metrics from .prf file
                local cache_misses=$(grep "cache-misses" "$prf_file" | awk '{print $1}' | sed 's/,//g')
                local cache_references=$(grep "cache-references" "$prf_file" | awk '{print $1}' | sed 's/,//g')
                local cycles=$(grep -w "cycles" "$prf_file" | awk '{print $1}' | sed 's/,//g')
                local instructions=$(grep "instructions" "$prf_file" | awk '{print $1}' | sed 's/,//g')
                local branch_misses=$(grep "branch-misses" "$prf_file" | awk '{print $1}' | sed 's/,//g')
                local page_faults=$(grep "page-faults" "$prf_file" | awk '{print $1}' | sed 's/,//g')
                local l1_dcache_misses=$(grep "L1-dcache-load-misses" "$prf_file" | awk '{print $1}' | sed 's/,//g')
                local l1_dcache_loads=$(grep "L1-dcache-loads" "$prf_file" | awk '{print $1}' | sed 's/,//g')
                local llc_misses=$(grep "LLC-load-misses" "$prf_file" | awk '{print $1}' | sed 's/,//g')
                local llc_loads=$(grep "LLC-loads" "$prf_file" | awk '{print $1}' | sed 's/,//g')
                
                # Add perf columns to CSV
                if [ "$header_written" = false ]; then
                    # Read header from CSV and add perf columns
                    head -1 "$csv_file" | awk -v OFS=',' '{print $0,"perf_cache_misses","perf_cache_references","perf_cycles","perf_instructions","perf_branch_misses","perf_page_faults","perf_l1_dcache_misses","perf_l1_dcache_loads","perf_llc_misses","perf_llc_loads"}' > "$combined_csv"
                    header_written=true
                fi
                
                # Add perf data to each data row (config_name already has "device_aware_" prefix)
                tail -n +2 "$csv_file" | awk -v OFS=',' -v cm="$cache_misses" -v cr="$cache_references" -v cy="$cycles" -v ins="$instructions" -v bm="$branch_misses" -v pf="$page_faults" -v l1m="$l1_dcache_misses" -v l1l="$l1_dcache_loads" -v llcm="$llc_misses" -v llcl="$llc_loads" 'BEGIN{FS=","} {print $0,cm,cr,cy,ins,bm,pf,l1m,l1l,llcm,llcl}' >> "$combined_csv"
            else
                echo "  PRF: Not found"
                
                # No perf data available, add empty columns
                if [ "$header_written" = false ]; then
                    head -1 "$csv_file" | awk -v OFS=',' '{print $0,"perf_cache_misses","perf_cache_references","perf_cycles","perf_instructions","perf_branch_misses","perf_page_faults","perf_l1_dcache_misses","perf_l1_dcache_loads","perf_llc_misses","perf_llc_loads"}' > "$combined_csv"
                    header_written=true
                fi
                
                # Add empty perf columns (config_name already has "device_aware_" prefix)
                tail -n +2 "$csv_file" | awk -v OFS=',' 'BEGIN{FS=","} {print $0,"","","","","","","","","",""}' >> "$combined_csv"
            fi
        fi
    done
    
    # Clean up temp directory
    rm -rf "$temp_dir"
    
    echo ""
    echo "=========================================="
    echo "CSV Merge Complete!"
    echo "=========================================="
    echo "Combined file: $combined_csv"
    echo "Total files processed: $processed_files"
    echo "=========================================="
}

# Main execution
echo ""
echo "Starting Device-Aware YCSB Benchmark Suite"
echo "==========================================="
echo ""

# Create summary file
SUMMARY_FILE="$PROFILE_OUTPUT_DIR/benchmark_summary.txt"
echo "Device-Aware YCSB Benchmark Summary" > "$SUMMARY_FILE"
echo "Generated: $(date)" >> "$SUMMARY_FILE"
echo "========================================" >> "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"

# Run benchmarks for all workload/storage/cache-size combinations
for workload in "${YCSB_WORKLOADS[@]}"; do
    for storage in "${STORAGE_TYPES[@]}"; do
        for cache_size_pct in "${CACHE_SIZE_PERCENTAGES[@]}"; do
            echo "[$workload x $storage x $cache_size_pct]" >> "$SUMMARY_FILE"
            
            # Query policy selection
            policy_output=$("$POLICY_SELECTOR" --workload "$workload" --storage "$storage")
            IFS=',' read -r policy_type config_name <<< "$policy_output"
            
            echo "  Policy: $policy_type" >> "$SUMMARY_FILE"
            echo "  Config: $config_name" >> "$SUMMARY_FILE"
            echo "  Cache Size: $cache_size_pct" >> "$SUMMARY_FILE"
            echo "" >> "$SUMMARY_FILE"
            
            # Run the benchmark
            run_device_aware_benchmark "$workload" "$storage" "$cache_size_pct"
        done
    done
done

echo ""
echo "=========================================="
echo "All Benchmarks Complete!"
echo "=========================================="
echo "Results directory: $PROFILE_OUTPUT_DIR"
echo "Summary file: $SUMMARY_FILE"
echo ""

# Display the decision matrix used
echo "Policy Decision Matrix Used:"
echo "----------------------------"
"$POLICY_SELECTOR" --print-matrix | tee "$PROFILE_OUTPUT_DIR/decision_matrix.txt"

echo ""
echo ""

# Combine all CSV files with perf data
combine_csv_files "$PROFILE_OUTPUT_DIR"

echo ""
echo "To view detailed results:"
echo "  cat $SUMMARY_FILE"
echo "  ls -lh $PROFILE_OUTPUT_DIR"
echo ""
echo "Combined CSV file with perf data:"
echo "  ls -lh $PROFILE_OUTPUT_DIR/combined_device_aware_ycsb_results_with_perf_*.csv"