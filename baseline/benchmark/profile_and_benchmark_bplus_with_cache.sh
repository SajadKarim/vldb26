#!/bin/bash

# BPlusStore Cache Profiling Benchmark Script
# This script runs comprehensive cache benchmarks with different cache types, storage types, and parameters
# Adapted for BPlusStore with LRU, SSARC, and CLOCK cache implementations
#
# Environment Variables:
# - THREADS: Array of thread counts for concurrent operations (default: defined in script)

# Configuration
# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="$SCRIPT_DIR/build"
BENCHMARK_EXEC="$BENCHMARK_DIR/benchmark"

# Data file path configuration
# Accept from environment variable or use default
# Usage: DATA_PATH=/path/to/data ./script.sh [COMMAND]
# Default: /home/skarim/benchmark_data/data
DATA_PATH="${DATA_PATH:-/home/skarim/benchmark_data/data}"

# Create data directory if it doesn't exist
mkdir -p "$DATA_PATH"

# Create timestamped output directory (only if not running merge command)
if [ "${1:-full}" != "merge" ]; then
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    PROFILE_OUTPUT_DIR="$BENCHMARK_DIR/cache_profiling_results_${TIMESTAMP}"
    
    # Create output directory
    mkdir -p "$PROFILE_OUTPUT_DIR"
    
    echo "=========================================="
    echo "BPlusStore Cache Profiling Results Directory: $PROFILE_OUTPUT_DIR"
    echo "Data Files Directory: $DATA_PATH"
    echo "=========================================="
else
    # For merge command, we'll determine the directory later
    PROFILE_OUTPUT_DIR=""
fi

# Cache-specific configuration arrays
CACHE_TYPES=("LRU" "A2Q" "CLOCK")  # CLOCK cache now enabled and fixed
STORAGE_TYPES=("VolatileStorage" "PMemStorage" "FileStorage")
CACHE_SIZE_PERCENTAGES=("2%" "10%" "25%")  # Cache sizes as percentages of estimated B+ tree pages
PAGE_SIZES=(4096)
MEMORY_SIZES=(34359738368)  # 1GB default

# Tree types to test (BPlusStore configurations)
TREES=("BPlusStore")

# Degrees to test
DEGREES=(24)

# Operations to profile
OPERATIONS=("insert" "search_random" "search_sequential" "search_uniform" "search_zipfian" "delete")

# Key-Value type combinations
declare -A KEY_VALUE_COMBOS
#KEY_VALUE_COMBOS["int_int"]="int int"
KEY_VALUE_COMBOS["uint64_t_uint64_t"]="uint64_t uint64_t"
#KEY_VALUE_COMBOS["char16_char16"]="char16 char16"
#KEY_VALUE_COMBOS["uint64_t_char16"]="uint64_t char16"

# Record count for profiling
RECORDS=(500000)
RUNS=${RUNS:-5}  # Default to 3, but allow override via environment variable
THREADS=(4)

# Perf events to collect (cache-focused)
PERF_EVENTS="cache-misses,cache-references,cycles,instructions,branch-misses,page-faults,L1-dcache-load-misses,L1-dcache-loads,LLC-load-misses,LLC-loads"

# Function to calculate actual cache size from percentage and estimated page count
calculate_cache_size() {
    local percentage=$1
    local record_count=$2
    local degree=${3:-64}  # Default degree if not provided
    
    # Remove the % symbol and convert to decimal
    local percent_value=${percentage%\%}
    
    # Estimate the number of pages in the B+ tree
    # For a B+ tree with degree d:
    # - Leaf pages: approximately record_count / (d-1) 
    # - Internal pages: approximately leaf_pages / d (for each level)
    # - Total pages is roughly 1.1 to 1.2 times the leaf pages (accounting for internal nodes)
    
    local leaf_pages=$((record_count / (degree - 1)))
    if [ $leaf_pages -lt 1 ]; then
        leaf_pages=1
    fi
    
    # Estimate total pages (leaf + internal nodes)
    # Using a conservative multiplier of 1.15 for internal nodes
    local estimated_total_pages=$((leaf_pages * 115 / 100))
    
    # Calculate cache size as percentage of estimated total pages
    local cache_size=$((estimated_total_pages * percent_value / 100))
    
    # Ensure minimum cache size for meaningful testing
    if [ $cache_size -lt 10 ]; then
        cache_size=10
    fi
    
    # Ensure cache size doesn't exceed total estimated pages (would give 100% hit ratio)
    if [ $cache_size -gt $estimated_total_pages ]; then
        cache_size=$estimated_total_pages
    fi
    
    echo $cache_size
}

# Function to post-process CSV file to rename cache_size to cache_page_limit and add cache_size percentage
post_process_csv_with_cache_info() {
    local csv_file=$1
    local cache_size_percentage=$2
    
    if [ ! -f "$csv_file" ]; then
        echo "CSV file not found: $csv_file"
        return 1
    fi
    
    # Convert percentage string (e.g., "5%") to decimal (e.g., "0.05")
    local percent_value=${cache_size_percentage%\%}
    local decimal_value=$(echo "scale=4; $percent_value / 100" | bc -l | awk '{printf "%.4f", $0}')
    
    # Create temporary file
    local temp_file="${csv_file}.tmp"
    
    # Process the CSV file
    {
        # Read and modify header
        IFS= read -r header_line
        # Replace cache_size with cache_page_limit and add new cache_size column after storage_type
        modified_header=$(echo "$header_line" | sed 's/cache_size/cache_page_limit/' | sed 's/storage_type,/storage_type,cache_size,/')
        echo "$modified_header"
        
        # Process data lines
        while IFS= read -r line; do
            # Add the cache_size percentage value after storage_type column (4th column)
            # Split the line into fields
            IFS=',' read -ra fields <<< "$line"
            
            # Insert cache_size percentage after storage_type (index 2, so insert at index 3)
            new_line=""
            for i in "${!fields[@]}"; do
                if [ $i -eq 3 ]; then
                    # After storage_type, add cache_size percentage
                    new_line="${new_line}${decimal_value},"
                fi
                new_line="${new_line}${fields[$i]}"
                if [ $i -lt $((${#fields[@]} - 1)) ]; then
                    new_line="${new_line},"
                fi
            done
            echo "$new_line"
        done
    } < "$csv_file" > "$temp_file"
    
    # Replace original file with modified version
    mv "$temp_file" "$csv_file"
    
    # Fix formatting if needed
    fix_cache_size_formatting "$csv_file"
    
    echo "Post-processed CSV file: $(basename "$csv_file") - Added cache_size percentage: $decimal_value"
}

# Function to fix cache_size formatting in existing CSV files (change .0500 to 0.0500)
fix_cache_size_formatting() {
    local csv_file=$1
    
    if [ ! -f "$csv_file" ]; then
        echo "CSV file not found: $csv_file"
        return 1
    fi
    
    # Check if file has the new structure and needs formatting fix
    local header_line=$(head -n 1 "$csv_file")
    if [[ "$header_line" == *"cache_page_limit"* ]] && [[ "$header_line" == *"cache_size"* ]]; then
        # Check if any line has .0500 format (missing leading zero)
        if grep -q ",.0[0-9][0-9][0-9]," "$csv_file"; then
            echo "Fixing cache_size formatting in: $(basename "$csv_file")"
            # Use sed to add leading zero to cache_size values
            sed -i 's/,\.\([0-9][0-9][0-9][0-9]\),/,0.\1,/g' "$csv_file"
            echo "Fixed cache_size formatting: $(basename "$csv_file")"
        fi
    fi
}

# Function to post-process existing CSV files that have old structure (for legacy files)
post_process_legacy_csv_with_cache_info() {
    local csv_file=$1
    local cache_size_percentage=${2:-"5%"}  # Default to 5% if not provided
    
    if [ ! -f "$csv_file" ]; then
        echo "CSV file not found: $csv_file"
        return 1
    fi
    
    # Check if file already has the new structure
    local header_line=$(head -n 1 "$csv_file")
    if [[ "$header_line" == *"cache_page_limit"* ]] && [[ "$header_line" == *"cache_size"* ]]; then
        echo "CSV file already has new structure: $(basename "$csv_file")"
        # Fix formatting if needed
        fix_cache_size_formatting "$csv_file"
        return 0
    fi
    
    # Convert percentage string (e.g., "5%") to decimal (e.g., "0.05")
    local percent_value=${cache_size_percentage%\%}
    local decimal_value=$(echo "scale=4; $percent_value / 100" | bc -l | awk '{printf "%.4f", $0}')
    
    echo "Post-processing legacy CSV file: $(basename "$csv_file") with cache_size percentage: $decimal_value"
    
    # Create temporary file
    local temp_file="${csv_file}.tmp"
    
    # Process the CSV file
    {
        # Read and modify header
        IFS= read -r header_line
        # Replace cache_size with cache_page_limit and add new cache_size column after storage_type
        modified_header=$(echo "$header_line" | sed 's/cache_size/cache_page_limit/' | sed 's/storage_type,/storage_type,cache_size,/')
        echo "$modified_header"
        
        # Process data lines
        while IFS= read -r line; do
            # Add the cache_size percentage value after storage_type column (4th column)
            # Split the line into fields
            IFS=',' read -ra fields <<< "$line"
            
            # Insert cache_size percentage after storage_type (index 2, so insert at index 3)
            new_line=""
            for i in "${!fields[@]}"; do
                if [ $i -eq 3 ]; then
                    # After storage_type, add cache_size percentage
                    new_line="${new_line}${decimal_value},"
                fi
                new_line="${new_line}${fields[$i]}"
                if [ $i -lt $((${#fields[@]} - 1)) ]; then
                    new_line="${new_line},"
                fi
            done
            echo "$new_line"
        done
    } < "$csv_file" > "$temp_file"
    
    # Replace original file with modified version
    mv "$temp_file" "$csv_file"
    
    echo "Post-processed legacy CSV file: $(basename "$csv_file") - Added cache_size percentage: $decimal_value"
}

# Function to build with cache support
build_cache_configuration() {
    local config_type=$1  # "non_concurrent" or "concurrent"
    
    echo "========================================="
    echo "Building BPlusStore Cache Configuration: $config_type"
    echo "========================================="
    
    cd "$BENCHMARK_DIR"
    
    # Clean previous build
    make clean > /dev/null 2>&1
    
    # Define optimization flags for Release builds
    local RELEASE_OPTS="-O3 -DNDEBUG -march=native"

    # Configure and build based on type
    if [ "$config_type" = "non_concurrent_default" ]; then
        echo "Building with cache + non_concurrent_default + cache counters ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    elif [ "$config_type" = "concurrent_default" ]; then
        echo "Building with cache + concurrent_default + cache counters ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    else
        echo "Building with cache + default + cache counters ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    fi
    
    make -j$(nproc)
    
    if [ $? -eq 0 ]; then
        echo "Build completed successfully for $config_type cache configuration"
    else
        echo "Build failed for $config_type cache configuration"
        exit 1
    fi
    
    cd - > /dev/null
    echo ""
}

# Function to run single cache benchmark with perf profiling
run_cache_profiled_benchmark() {
    local tree_type=$1
    local cache_type=$2
    local storage_type=$3
    local cache_size=$4
    local page_size=$5
    local memory_size=$6
    local key_type=$7
    local value_type=$8
    local operation=$9
    local degree=${10}
    local records=${11}
    local config_name=${12}
    local thread_count=${13}
    local cache_size_percentage=${14}  # New parameter for percentage
    
    local profile_name="${tree_type}_${cache_type}_${storage_type}_${cache_size}_${page_size}_${memory_size}_${key_type}_${value_type}_${operation}_${degree}_${records}_threads${thread_count}"
    
    # Create a separate folder for this individual run
    local run_folder="$PROFILE_OUTPUT_DIR/${config_name}_${profile_name}"
    mkdir -p "$run_folder"
    
    local perf_output="$run_folder/${config_name}_${profile_name}.prf"
    local perf_data="$run_folder/${config_name}_${profile_name}.data"
    
    echo "=========================================="
    echo "BPlusStore Cache Profiling: $tree_type - $operation - Degree $degree"
    echo "Cache: $cache_type (Size: $cache_size), Storage: $storage_type"
    echo "Page Size: $page_size, Memory Size: $memory_size"
    echo "Key: $key_type, Value: $value_type, Records: $records"
    echo "Threads: $thread_count"
    echo "Run Folder: $run_folder"
    echo "Output: $perf_output"
    echo "=========================================="
    
    # Run benchmark with perf stat (statistical profiling)
    cd "$BENCHMARK_DIR"
    perf stat -e "$PERF_EVENTS" \
              -o "$perf_output" \
              numactl --cpunodebind=0 --membind=0 \
              ./benchmark \
              --config "bm_cache" \
              --cache-type "$cache_type" \
              --storage-type "$storage_type" \
              --cache-size "$cache_size" \
              --cache-percentage "$cache_size_percentage" \
              --page-size "$page_size" \
              --memory-size "$memory_size" \
              --tree-type "$tree_type" \
              --key-type "$key_type" \
              --value-type "$value_type" \
              --operation "$operation" \
              --degree "$degree" \
              --records "$records" \
              --runs "$RUNS" \
              --threads "$thread_count" \
              --output-dir "$run_folder" \
              --config-name "$config_name" \
              --data-path "$DATA_PATH"
    
    # Also run with perf record for detailed analysis (optional)
    if [ "$DETAILED_PROFILING" = "true" ]; then
        echo "Running detailed profiling with perf record..."
        perf record -e cycles,cache-misses \
                    -o "$perf_data" \
                    numactl --cpunodebind=0 --membind=0 \
                    ./benchmark \
                    --config "bm_cache" \
                    --cache-type "$cache_type" \
                    --storage-type "$storage_type" \
                    --cache-size "$cache_size" \
                    --cache-percentage "$cache_size_percentage" \
                    --page-size "$page_size" \
                    --memory-size "$memory_size" \
                    --tree-type "$tree_type" \
                    --key-type "$key_type" \
                    --value-type "$value_type" \
                    --operation "$operation" \
                    --degree "$degree" \
                    --records "$records" \
                    --runs "$RUNS" \
                    --threads "$thread_count" \
                    --output-dir "$run_folder" \
                    --config-name "$config_name" \
                    --data-path "$DATA_PATH"
    fi
    
    # Rename the CSV file to match the perf file naming convention
    local latest_csv=$(ls -t "$run_folder"/benchmark_*.csv 2>/dev/null | head -1)
    if [ -f "$latest_csv" ]; then
        local target_csv="$run_folder/${config_name}_${profile_name}.csv"
        mv "$latest_csv" "$target_csv"
        echo "CSV file renamed to: $(basename "$target_csv")"
        
        # CSV file is already correctly formatted by the C++ benchmark code
        echo "CSV file ready: $(basename "$target_csv")"
    else
        echo "WARNING: No CSV file found in $run_folder with pattern benchmark_*.csv"
        echo "Checking for any CSV files in the directory:"
        ls -la "$run_folder"/*.csv 2>/dev/null || echo "No CSV files found at all"
    fi
    
    echo "BPlusStore cache profiling completed for $profile_name"
    
    # Brief sleep to let system settle between benchmarks
    sleep 2
    echo ""
}

# Function to run comprehensive cache profiling with custom parameters
run_full_cache_profiling_with_params() {
    local config_type=${1:-"non_concurrent_default"}
    local -n cache_types_ref=$2
    local -n storage_types_ref=$3
    local -n cache_size_percentages_ref=$4
    local -n page_sizes_ref=$5
    local -n memory_sizes_ref=$6
    local -n trees_ref=$7
    local -n degrees_ref=$8
    local -n operations_ref=$9
    local -n key_value_combos_ref=${10}
    local -n records_ref=${11}
    local config_suffix=${12:-""}
    local -n threads_ref=${13:-THREADS}
    
    echo "Starting comprehensive BPlusStore cache profiling for $config_type configuration${config_suffix:+ ($config_suffix)}..."
    echo "Cache Types: ${cache_types_ref[*]}"
    echo "Storage Types: ${storage_types_ref[*]}"
    echo "Cache Size Percentages: ${cache_size_percentages_ref[*]}"
    echo "Page Sizes: ${page_sizes_ref[*]}"
    echo "Memory Sizes: ${memory_sizes_ref[*]}"
    echo "Trees: ${trees_ref[*]}"
    echo "Degrees: ${degrees_ref[*]}"
    echo "Operations: ${operations_ref[*]}"
    echo "Records: ${records_ref[*]}"
    echo "Threads: ${threads_ref[*]}"
    echo ""
    
    # Build the appropriate configuration
    build_cache_configuration "$config_type"

    local total_combinations=0
    local current_combination=0
    
    # Calculate total combinations
    for combo_name in "${!key_value_combos_ref[@]}"; do
        IFS=' ' read -r key_type value_type <<< "${key_value_combos_ref[$combo_name]}"
        for tree in "${trees_ref[@]}"; do
            for cache_type in "${cache_types_ref[@]}"; do
                for storage_type in "${storage_types_ref[@]}"; do
                    for cache_size_percentage in "${cache_size_percentages_ref[@]}"; do
                        for page_size in "${page_sizes_ref[@]}"; do
                            for memory_size in "${memory_sizes_ref[@]}"; do
                                for degree in "${degrees_ref[@]}"; do
                                    for operation in "${operations_ref[@]}"; do
                                        for records in "${records_ref[@]}"; do
                                            for thread_count in "${threads_ref[@]}"; do
                                                ((total_combinations++))
                                            done
                                        done
                                    done
                                done
                            done
                        done
                    done
                done
            done
        done
    done
    
    echo "Total combinations to profile for $config_type${config_suffix:+ ($config_suffix)}: $total_combinations"
    echo ""
    
    # Create config-specific identifier for output files
    local config_id="${config_type}${config_suffix}"
    
    # Run profiling for each combination
    for combo_name in "${!key_value_combos_ref[@]}"; do
        IFS=' ' read -r key_type value_type <<< "${key_value_combos_ref[$combo_name]}"
        
        echo "Processing key-value combination: $key_type -> $value_type"
        
        for tree in "${trees_ref[@]}"; do
            for cache_type in "${cache_types_ref[@]}"; do
                for storage_type in "${storage_types_ref[@]}"; do
                    for cache_size_percentage in "${cache_size_percentages_ref[@]}"; do
                        for page_size in "${page_sizes_ref[@]}"; do
                            for degree in "${degrees_ref[@]}"; do
                                for records in "${records_ref[@]}"; do
                                    for memory_size in "${memory_sizes_ref[@]}"; do
                                        # Calculate actual cache size from percentage, record count, and degree
                                        local actual_cache_size=$(calculate_cache_size "$cache_size_percentage" "$records" "$degree")
                                        
                                        echo "Cache size calculation: $cache_size_percentage of estimated pages for $records records (degree $degree) = $actual_cache_size entries"                                
                                    
                                        for operation in "${operations_ref[@]}"; do
                                            for thread_count in "${threads_ref[@]}"; do
                                                ((current_combination++))
                                                echo "Progress: $current_combination/$total_combinations ($config_id)"
                                                
                                                # Convert percentage to decimal for C++ code
                                                local percent_value=${cache_size_percentage%\%}
                                                local decimal_value=$(echo "scale=4; $percent_value / 100" | bc -l | awk '{printf "%.4f", $0}')
                                                
                                                run_cache_profiled_benchmark "$tree" "$cache_type" "$storage_type" "$actual_cache_size" "$page_size" "$memory_size" "$key_type" "$value_type" "$operation" "$degree" "$records" "$config_id" "$thread_count" "$decimal_value"
                                                
                                                # Small delay between runs to let system settle
                                                sleep 2
                                            done
                                        done
                                    done
                                done
                            done
                        done
                    done
                done
            done
        done
    done
    
    echo "=========================================="
    echo "BPlusStore cache profiling completed for $config_id!"
    echo "Results saved in: $PROFILE_OUTPUT_DIR"
    echo "Total profiles generated: $total_combinations"
    echo "=========================================="
}

# Function to run single-threaded cache profiling
run_full_cache_profiling_single_threaded() {
    # Create a local single-threaded array
    local SINGLE_THREADS=(1)
    local CACHE_TYPES_LOCAL=("LRU")  # Test all cache types

    echo "Running single-threaded BPlusStore cache profiling..."
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_default"    
    run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local CACHE_TYPES_LOCAL=("A2Q")  # Test all cache types

    echo "Running single-threaded BPlusStore cache profiling..."
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_default"    
    run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")  # Test all cache types

    echo "Running single-threaded BPlusStore cache profiling..."
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_default"    
    run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
}

# Function to run multi-threaded cache profiling
run_full_cache_profiling_multi_threaded() {
    echo "Running multi-threaded BPlusStore cache profiling..."
    echo "Using threads: ${THREADS[*]}"
    
    local CACHE_TYPES_LOCAL=("LRU")
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="concurrent_default"    
    run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("A2Q")
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="concurrent_default"    
    run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="concurrent_default"    
    run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" THREADS
}

# Function to run quick cache benchmark for all configurations
run_quick_cache_benchmark() {
    local config_type=${1:-"non_concurrent_default"}
    
    echo "=========================================="
    echo "Quick BPlusStore Cache Benchmark Run - $config_type Configuration"
    echo "=========================================="
    echo "Cache Types: ${CACHE_TYPES[*]}"
    echo "Storage Types: VolatileStorage"
    echo "Cache Sizes: 5%, 15%"
    echo "Trees: BPlusStore"
    echo "Degrees: 64, 128"
    echo "Operations: ${OPERATIONS[*]}"
    echo "Record Counts: 100000, 500000"
    echo "Key/Value Types: int/int"
    echo "Thread Counts: 1, 2, 4"
    echo "Runs per config: 1"
    
    # Build the appropriate configuration
    build_cache_configuration "$config_type"
    
    local quick_cache_percentages=("5%" "15%")
    local quick_storage_types=("VolatileStorage")
    local quick_trees=("BPlusStore")
    local quick_degrees=(64 128)
    local record_counts=(100000 500000)
    local quick_threads=(1 2 4)
    local total_combinations=$((${#CACHE_TYPES[@]} * ${#quick_storage_types[@]} * ${#quick_cache_percentages[@]} * ${#quick_trees[@]} * ${#quick_degrees[@]} * ${#OPERATIONS[@]} * ${#record_counts[@]} * ${#quick_threads[@]}))
    echo "Total combinations: $total_combinations"
    echo "=========================================="
    
    # Create consolidated CSV file
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local consolidated_csv="$PROFILE_OUTPUT_DIR/quick_cache_benchmark_${config_type}_${timestamp}.csv"
    echo "tree_type,cache_type,storage_type,cache_size,key_type,value_type,operation,record_count,degree,time_us,throughput_ops_sec,test_run_id,timestamp" > "$consolidated_csv"
    
    local current_combination=0
    local start_time=$(date +%s)
    
    for records in "${record_counts[@]}"; do
        echo ""
        echo "=== Processing Record Count: $records ==="
        
        for tree in "${quick_trees[@]}"; do
            for cache_type in "${CACHE_TYPES[@]}"; do
                for storage_type in "${quick_storage_types[@]}"; do
                    for cache_size_percentage in "${quick_cache_percentages[@]}"; do
                        local actual_cache_size=$(calculate_cache_size "$cache_size_percentage" "$records")
                        for degree in "${quick_degrees[@]}"; do
                            for operation in "${OPERATIONS[@]}"; do
                                for thread_count in "${quick_threads[@]}"; do
                                    ((current_combination++))
                                    
                                    echo "[$current_combination/$total_combinations] Testing: $tree - $cache_type/$storage_type (Cache:$actual_cache_size) - $operation - Degree $degree - Records $records - Threads $thread_count"
                                    
                                    cd "$BENCHMARK_DIR"
                                    
                                    # Run benchmark without perf profiling for speed
                                    ./benchmark \\
                                        --config "bm_cache" \\
                                        --cache-type "$cache_type" \\
                                        --storage-type "$storage_type" \\
                                        --cache-size "$actual_cache_size" \\
                                        --tree-type "$tree" \\
                                        --key-type "int" \\
                                        --value-type "int" \\
                                        --operation "$operation" \\
                                        --degree "$degree" \\
                                        --records "$records" \\
                                        --runs 1 \\
                                        --threads "$thread_count" \\
                                        --data-path "$DATA_PATH" > /dev/null 2>&1
                                
                                    # Find the most recent CSV file and append to consolidated results
                                    local latest_csv=$(ls -t benchmark_*.csv 2>/dev/null | head -1)
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
    echo "Quick BPlusStore Cache Benchmark Run Completed!"
    echo "=========================================="
    echo "Total combinations tested: $total_combinations"
    echo "Total execution time: ${minutes}m ${seconds}s"
    echo "Results saved to: $consolidated_csv"
    echo ""
}

# Function to analyze cache perf results
analyze_cache_results() {
    echo "Analyzing BPlusStore cache perf results..."
    
    # Create summary CSV
    local summary_file="$PROFILE_OUTPUT_DIR/cache_perf_summary.csv"
    echo "tree_type,cache_type,storage_type,cache_size,key_type,value_type,operation,degree,records,cache_misses,cache_references,cache_miss_rate,cycles,instructions,ipc,branch_misses,page_faults" > "$summary_file"
    
    for perf_file in "$PROFILE_OUTPUT_DIR"/*/*.prf; do
        if [ -f "$perf_file" ]; then
            # Extract configuration from filename
            local basename=$(basename "$perf_file" .prf)
            
            # Parse perf output and extract metrics
            local cache_misses=$(grep "cache-misses" "$perf_file" | awk '{print $1}' | tr -d ',')
            local cache_references=$(grep "cache-references" "$perf_file" | awk '{print $1}' | tr -d ',')
            local cycles=$(grep "cycles" "$perf_file" | awk '{print $1}' | tr -d ',')
            local instructions=$(grep "instructions" "$perf_file" | awk '{print $1}' | tr -d ',')
            local branch_misses=$(grep "branch-misses" "$perf_file" | awk '{print $1}' | tr -d ',')
            local page_faults=$(grep "page-faults" "$perf_file" | awk '{print $1}' | tr -d ',')
            
            # Calculate derived metrics
            local cache_miss_rate=0
            local ipc=0
            
            if [ -n "$cache_references" ] && [ "$cache_references" -gt 0 ]; then
                cache_miss_rate=$(echo "scale=4; $cache_misses / $cache_references * 100" | bc -l 2>/dev/null || echo "0")
            fi
            
            if [ -n "$cycles" ] && [ "$cycles" -gt 0 ]; then
                ipc=$(echo "scale=4; $instructions / $cycles" | bc -l 2>/dev/null || echo "0")
            fi
            
            # Extract configuration details from filename
            echo "$basename,$cache_misses,$cache_references,$cache_miss_rate,$cycles,$instructions,$ipc,$branch_misses,$page_faults" >> "$summary_file"
        fi
    done
    
    echo "BPlusStore cache performance analysis completed. Summary saved to: $summary_file"
}

# Function to merge all CSV files from benchmark runs into a single combined CSV file
# Function to parse perf data from .prf file into CSV format
parse_perf_data() {
    local prf_file=$1
    
    if [ ! -f "$prf_file" ]; then
        echo "0,0,0,0,0,0,0,0,0,0,0.0,0.0,0.0"
        return
    fi
    
    # Initialize variables with default values
    local cache_misses=0
    local cache_references=0
    local cycles=0
    local instructions=0
    local branch_misses=0
    local page_faults=0
    local l1_dcache_load_misses=0
    local l1_dcache_loads=0
    local llc_load_misses=0
    local llc_loads=0
    local time_elapsed=0.0
    local user_time=0.0
    local sys_time=0.0
    
    # Parse the perf data
    while IFS= read -r line; do
        # Remove leading/trailing whitespace and commas
        line=$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//;s/,//g')
        
        if [[ $line =~ ^([0-9]+)[[:space:]]+cache-misses ]]; then
            cache_misses=${BASH_REMATCH[1]}
        elif [[ $line =~ ^([0-9]+)[[:space:]]+cache-references ]]; then
            cache_references=${BASH_REMATCH[1]}
        elif [[ $line =~ ^([0-9]+)[[:space:]]+cycles ]]; then
            cycles=${BASH_REMATCH[1]}
        elif [[ $line =~ ^([0-9]+)[[:space:]]+instructions ]]; then
            instructions=${BASH_REMATCH[1]}
        elif [[ $line =~ ^([0-9]+)[[:space:]]+branch-misses ]]; then
            branch_misses=${BASH_REMATCH[1]}
        elif [[ $line =~ ^([0-9]+)[[:space:]]+page-faults ]]; then
            page_faults=${BASH_REMATCH[1]}
        elif [[ $line =~ ^([0-9]+)[[:space:]]+L1-dcache-load-misses ]]; then
            l1_dcache_load_misses=${BASH_REMATCH[1]}
        elif [[ $line =~ ^([0-9]+)[[:space:]]+L1-dcache-loads ]]; then
            l1_dcache_loads=${BASH_REMATCH[1]}
        elif [[ $line =~ ^([0-9]+)[[:space:]]+LLC-load-misses ]]; then
            llc_load_misses=${BASH_REMATCH[1]}
        elif [[ $line =~ ^([0-9]+)[[:space:]]+LLC-loads ]]; then
            llc_loads=${BASH_REMATCH[1]}
        elif [[ $line =~ ^[[:space:]]*([0-9]+\.[0-9]+)[[:space:]]+seconds[[:space:]]+time[[:space:]]+elapsed ]]; then
            time_elapsed=${BASH_REMATCH[1]}
        elif [[ $line =~ ^[[:space:]]*([0-9]+\.[0-9]+)[[:space:]]+seconds[[:space:]]+user ]]; then
            user_time=${BASH_REMATCH[1]}
        elif [[ $line =~ ^[[:space:]]*([0-9]+\.[0-9]+)[[:space:]]+seconds[[:space:]]+sys ]]; then
            sys_time=${BASH_REMATCH[1]}
        fi
    done < "$prf_file"
    
    # Output as CSV format
    echo "$cache_misses,$cache_references,$cycles,$instructions,$branch_misses,$page_faults,$l1_dcache_load_misses,$l1_dcache_loads,$llc_load_misses,$llc_loads,$time_elapsed,$user_time,$sys_time"
}

# Function to merge CSV with perf data
merge_csv_with_perf_data() {
    local csv_file=$1
    local prf_file=$2
    local output_file=$3
    
    if [ ! -f "$csv_file" ]; then
        echo "CSV file not found: $csv_file"
        return 1
    fi
    
    # Parse perf data
    local perf_data=$(parse_perf_data "$prf_file")
    
    # Read the CSV file
    local header_line=$(head -n 1 "$csv_file")
    
    # Add perf columns to header
    local new_header="${header_line},perf_cache_misses,perf_cache_references,perf_cycles,perf_instructions,perf_branch_misses,perf_page_faults,perf_l1_dcache_load_misses,perf_l1_dcache_loads,perf_llc_load_misses,perf_llc_loads,perf_time_elapsed,perf_user_time,perf_sys_time"
    
    # Write new header
    echo "$new_header" > "$output_file"
    
    # Process data lines
    tail -n +2 "$csv_file" | while IFS= read -r line; do
        echo "${line},${perf_data}" >> "$output_file"
    done
}

merge_csv_files() {
    local target_dir=${1:-$PROFILE_OUTPUT_DIR}
    
    echo "=========================================="
    echo "Merging all CSV files from benchmark runs with perf data"
    echo "=========================================="
    
    # If target_dir doesn't exist, try to find the most recent results directory with CSV files
    if [ ! -d "$target_dir" ] || [ -z "$(ls -A "$target_dir" 2>/dev/null)" ]; then
        echo "Target directory empty or not found: $target_dir"
        echo "Looking for existing results directories with CSV files..."
        
        local found_dir=""
        for dir in $(ls -td "$BENCHMARK_DIR"/cache_profiling_results_* 2>/dev/null); do
            if [ -d "$dir" ] && [ -n "$(find "$dir" -name "*.csv" -type f 2>/dev/null | head -1)" ]; then
                found_dir="$dir"
                break
            fi
        done
        
        if [ -n "$found_dir" ]; then
            target_dir="$found_dir"
            echo "Using results directory with CSV files: $target_dir"
        else
            echo "No results directories with CSV files found in $BENCHMARK_DIR"
            return 1
        fi
    fi
    
    # Create combined CSV file with timestamp
    local merge_timestamp=$(date +"%Y%m%d_%H%M%S")
    local combined_csv="$target_dir/combined_benchmark_results_with_perf_${merge_timestamp}.csv"
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
            local prf_file="$(dirname "$csv_file")/${base_name}.prf"
            local temp_csv="$temp_dir/temp_${processed_files}.csv"
            
            echo "Processing [$processed_files/$total_files]: $folder_name"
            
            # Post-process legacy CSV files to add cache_size percentage if needed
            post_process_legacy_csv_with_cache_info "$csv_file" "5%"
            
            # Merge CSV with perf data
            merge_csv_with_perf_data "$csv_file" "$prf_file" "$temp_csv"
            
            if [ "$header_written" = false ]; then
                # Write header from first processed file
                head -n 1 "$temp_csv" > "$combined_csv"
                header_written=true
                echo "  Header with perf columns written"
            fi
            
            # Append data lines (skip header)
            tail -n +2 "$temp_csv" >> "$combined_csv"
            echo "  Data with perf metrics appended"
        fi
    done
    
    # Clean up temp directory
    rm -rf "$temp_dir"
    
    # Count total lines in combined file
    local total_lines=$(wc -l < "$combined_csv")
    local data_lines=$((total_lines - 1))  # Subtract header line
    
    echo ""
    echo "=========================================="
    echo "CSV Merge with Perf Data Completed Successfully!"
    echo "=========================================="
    echo "Files processed: $processed_files"
    echo "Combined file: $combined_csv"
    echo "Total lines: $total_lines (1 header + $data_lines data lines)"
    echo "Perf columns added: perf_cache_misses, perf_cache_references, perf_cycles, perf_instructions,"
    echo "                   perf_branch_misses, perf_page_faults, perf_l1_dcache_load_misses,"
    echo "                   perf_l1_dcache_loads, perf_llc_load_misses, perf_llc_loads,"
    echo "                   perf_time_elapsed, perf_user_time, perf_sys_time"
    echo "=========================================="
    
    return 0
}

# Function to show usage
show_usage() {
    echo "Usage: [ENV_VARS] $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  full                        Run comprehensive cache profiling (both single and multi-threaded)"
    echo "  single_threaded             Run single-threaded cache profiling"
    echo "  multi_threaded              Run multi-threaded cache profiling"
    echo "  quick [config_type]         Run quick cache benchmark (default: non_concurrent_default)"
    echo "  analyze                     Analyze existing perf results"
    echo "  merge                       Merge all CSV files from existing benchmark runs"
    echo "  help                        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 full                     # Full profiling with default data path"
    echo "  DATA_PATH=/home/skarim/benchmark_data/data $0 full  # Full profiling with custom data path"
    echo "  $0 single_threaded          # Single-threaded profiling only"
    echo "  $0 multi_threaded           # Multi-threaded profiling only"
    echo "  $0 quick                    # Quick benchmark with default configuration"
    echo "  $0 analyze                  # Analyze existing results"
    echo "  $0 merge                    # Merge all CSV files from existing benchmark runs"
    echo ""
    echo "Environment Variables:"
    echo "  DATA_PATH=path              Set data files directory (default: /home/skarim/benchmark_data/data)"
    echo "  RUNS=N                      Set number of runs per configuration (default: 3)"
    echo "  DETAILED_PROFILING=true     Enable detailed perf record profiling"
    echo ""
    echo "Cache Types: ${CACHE_TYPES[*]}"
    echo "Storage Types: ${STORAGE_TYPES[*]}"
    echo "Cache Size Percentages: ${CACHE_SIZE_PERCENTAGES[*]}"
}

# Main script logic
case "${1:-full}" in
    "full")
        run_full_cache_profiling_single_threaded
        run_full_cache_profiling_multi_threaded
        echo ""
        echo "Both single-threaded and multi-threaded profiling completed. Merging all CSV files..."
        merge_csv_files
        ;;
    "single_threaded")
        run_full_cache_profiling_single_threaded
        echo ""
        echo "Single-threaded profiling completed. Merging CSV files..."
        merge_csv_files
        analyze_cache_results
        ;;
    "multi_threaded")
        run_full_cache_profiling_multi_threaded
        echo ""
        echo "Multi-threaded profiling completed. Merging CSV files..."
        merge_csv_files
        analyze_cache_results
        ;;
    "quick")
        run_quick_cache_benchmark "${2:-non_concurrent_default}"
        ;;
    "analyze")
        analyze_cache_results
        ;;
    "merge")
        merge_csv_files
        ;;
    "help"|"-h"|"--help")
        show_usage
        ;;
    *)
        echo "Unknown command: $1"
        show_usage
        exit 1
        ;;
esac

echo ""
echo "BPlusStore cache benchmark script completed!"
echo "Results directory: $PROFILE_OUTPUT_DIR"