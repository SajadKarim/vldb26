#!/bin/bash

# YCSB Cache Profiling Benchmark Script
# This script runs comprehensive YCSB workload benchmarks with different cache types, storage types, and parameters
# Based on profile_and_benchmark_tree_with_cache.sh but adapted for YCSB workload testing
#
# Features:
# - Tests YCSB workloads A, B, C, D, E, F
# - Configurable data path: Benchmark data is stored in /home/skarim/benchmark_data
# - Automatic data generation: Checks if data exists and generates it if missing
# - Data reuse: Existing data is reused across benchmark runs for consistency
#
# Environment Variables:
# - THREADS: Array of thread counts for concurrent operations (default: defined in script)

# Configuration
# Get the directory where this script is located (benchmark directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="$SCRIPT_DIR/build"
BENCHMARK_EXEC="$BENCHMARK_DIR/benchmark"

# Create timestamped output directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
PROFILE_OUTPUT_DIR="$BENCHMARK_DIR/ycsb_cache_profiling_results_${TIMESTAMP}"

# Create output directory
mkdir -p "$PROFILE_OUTPUT_DIR"

echo "=========================================="
echo "YCSB Cache Profiling Results Directory: $PROFILE_OUTPUT_DIR"
echo "=========================================="

# Cache-specific configuration arrays
CACHE_TYPES=("LRU" "A2Q" "CLOCK")
STORAGE_TYPES=("VolatileStorage" "PMemStorage" "FileStorage")
CACHE_SIZE_PERCENTAGES=("2%" "10%", "25%")  # Cache sizes as percentages of estimated B+ tree pages
PAGE_SIZES=(4096)
MEMORY_SIZES=(34359738368) #(2147483648)  # 512MB, 1GB, 2GB

# BiStorage-specific configuration
# Primary storage configuration
BISTORAGE_PRIMARY_STORAGE="VolatileStorage"
BISTORAGE_PRIMARY_READ_COST=10
BISTORAGE_PRIMARY_WRITE_COST=10

# Secondary storage configuration
BISTORAGE_SECONDARY_STORAGE="PMemStorage"
BISTORAGE_SECONDARY_READ_COST=300
BISTORAGE_SECONDARY_WRITE_COST=300

# Tree types to test (focus on main ones for cache testing)
TREES=("BplusTreeSOA")

# Degrees to test (subset for cache profiling)
DEGREES=(24)

# YCSB Workloads to profile
YCSB_WORKLOADS=("ycsb_a" "ycsb_c" "ycsb_d")

# Key-Value type combinations (only implemented combinations)
declare -A KEY_VALUE_COMBOS
KEY_VALUE_COMBOS["uint64_t_uint64_t"]="uint64_t uint64_t"
#KEY_VALUE_COMBOS["char16_char16"]="char16 char16"
#KEY_VALUE_COMBOS["uint64_t_char16"]="uint64_t char16"

# Record count for profiling
RECORDS=(500000)
RUNS=${RUNS:-5}  # Default to 5, but allow override via environment variable
THREADS=(1)
#THREADS=(4)

# Data directory configuration
# Use environment variable if set, otherwise default to ~/benchmark_data
DATA_BASE_DIR="${BENCHMARK_DATA_DIR:-/tmp/benchmark_data}"
DATA_DIR="$DATA_BASE_DIR/data"
YCSB_DIR="$DATA_BASE_DIR/ycsb"

# Perf events to collect (cache-focused)
PERF_EVENTS="cache-misses,cache-references,cycles,instructions,branch-misses,page-faults,L1-dcache-load-misses,L1-dcache-loads,LLC-load-misses,LLC-loads,L1-icache-load-misses,L1-icache-loads"

# Function to calculate actual cache size from percentage and memory size
# Function to calculate actual cache size from percentage and memory size
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

# Function to ensure data directories exist and generate data if needed
ensure_benchmark_data() {
    echo "=========================================="
    echo "Checking benchmark data availability..."
    echo "=========================================="
    echo "Data directory: $DATA_DIR"
    echo "YCSB directory: $YCSB_DIR"
    
    # Create base directories if they don't exist
    mkdir -p "$DATA_BASE_DIR"
    mkdir -p "$DATA_DIR"
    mkdir -p "$YCSB_DIR"
    
    # Check if data directories have any files
    local data_files_count=$(find "$DATA_DIR" -name "*.dat" 2>/dev/null | wc -l)
    local ycsb_files_count=$(find "$YCSB_DIR" -name "*.dat" 2>/dev/null | wc -l)
    
    echo "Found $data_files_count basic workload files"
    echo "Found $ycsb_files_count YCSB workload files"
    
    # If no data files exist, generate them
    if [ $data_files_count -eq 0 ] || [ $ycsb_files_count -eq 0 ]; then
        echo ""
        echo "Generating missing benchmark data..."
        echo "This may take a few minutes..."
        
        # Change to the data directory
        cd "$DATA_BASE_DIR"
        
        # Run benchmark executable to generate data (using a minimal configuration)
        echo "Generating basic workload and YCSB data..."
        "$BENCHMARK_EXEC" --config "bm_nocache" --tree-type "BplusTreeSOA" --key-type "uint64_t" --value-type "uint64_t" --operation "insert" --degree "64" --records "100000" --runs "1" > /dev/null 2>&1
        
        # Check if data was generated successfully
        local new_data_files_count=$(find "$DATA_DIR" -name "*.dat" 2>/dev/null | wc -l)
        local new_ycsb_files_count=$(find "$YCSB_DIR" -name "*.dat" 2>/dev/null | wc -l)
        
        echo "Data generation completed!"
        echo "Generated $new_data_files_count basic workload files"
        echo "Generated $new_ycsb_files_count YCSB workload files"
        
        # Return to benchmark directory
        cd "$BENCHMARK_DIR" > /dev/null
    else
        echo "Benchmark data already exists, skipping generation."
    fi
    
    echo "=========================================="
}

# Function to build with cache support
build_cache_configuration() {
    
    #return

    local config_type=$1  # "basic" or "concurrent"
    
    echo "========================================="
    echo "Building Cache Configuration: $config_type"
    echo "========================================="
    
    cd "$BENCHMARK_DIR"
    
    # Clean previous build
    make clean > /dev/null 2>&1
    
    # Define optimization flags for Release builds
    local RELEASE_OPTS="-O3 -DNDEBUG -march=native -mtune=native -mavx2"

    # Configure and build based on type
    if [ "$config_type" = "non_concurrent_default" ]; then
        echo "Building with cache + non_concurrent_default ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ $RELEASE_OPTS"
    elif [ "$config_type" = "non_concurrent_cwe" ]; then
        echo "Building with cache + non_concurrent_relaxed ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ $RELEASE_OPTS"
        #cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__COST_WEIGHTED_EVICTION__ $RELEASE_OPTS"
    elif [ "$config_type" = "non_concurrent_relaxed" ]; then
        echo "Building with cache + non_concurrent_relaxed ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__SELECTIVE_UPDATE__ $RELEASE_OPTS"
    # LRU Specific
    elif [ "$config_type" = "non_concurrent_lru_metadata_update_in_order" ]; then
        echo "Building with cache + non_concurrent_lru_metadata_update_in_order ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__UPDATE_IN_ORDER__ $RELEASE_OPTS"
    elif [ "$config_type" = "non_concurrent_lru_metadata_update_in_order_and_relaxed" ]; then
        echo "Building with cache + non_concurrent_lru_metadata_update_in_order_and_relaxed ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__UPDATE_IN_ORDER__ -D__SELECTIVE_UPDATE__ $RELEASE_OPTS"
    # A2Q Specific
    elif [ "$config_type" = "non_concurrent_a2q_ghost_q_enabled" ]; then
        echo "Building with cache + non_concurrent_a2q_ghost_q_enabled ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__MANAGE_GHOST_Q__ $RELEASE_OPTS"
    elif [ "$config_type" = "non_concurrent_a2q_ghost_q_enabled_and_relaxed" ]; then
        echo "Building with cache + non_concurrent_a2q_ghost_q_enabled_and_relaxed ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__MANAGE_GHOST_Q__ -D__SELECTIVE_UPDATE__ $RELEASE_OPTS"
    # Concurrent ones
    elif [ "$config_type" = "concurrent_default" ]; then
        echo "Building with cache + concurrent_default ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ $RELEASE_OPTS"
    elif [ "$config_type" = "concurrent_relaxed" ]; then
        echo "Building with cache + concurrent_relaxed ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__SELECTIVE_UPDATE__ $RELEASE_OPTS"
    # LRU Specific
    elif [ "$config_type" = "concurrent_lru_metadata_update_in_order" ]; then
        echo "Building with cache + concurrent_lru_metadata_update_in_order ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__UPDATE_IN_ORDER__ $RELEASE_OPTS"
    elif [ "$config_type" = "concurrent_lru_metadata_update_in_order_and_relaxed" ]; then
        echo "Building with cache + concurrent_lru_metadata_update_in_order_and_relaxed ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__UPDATE_IN_ORDER__ -D__SELECTIVE_UPDATE__ $RELEASE_OPTS"
    # A2Q Specific
    elif [ "$config_type" = "concurrent_a2q_ghost_q_enabled" ]; then
        echo "Building with cache + concurrent_a2q_ghost_q_enabled ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__MANAGE_GHOST_Q__ $RELEASE_OPTS"
    elif [ "$config_type" = "concurrent_a2q_ghost_q_enabled_and_relaxed" ]; then
        echo "Building with cache + concurrent_a2q_ghost_q_enabled_and_relaxed ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__MANAGE_GHOST_Q__ -D__SELECTIVE_UPDATE__ $RELEASE_OPTS"
    # CLOCK Specific __CLOCK_WITH_BUFFER__ is with concurrent
    elif [ "$config_type" = "non_concurrent_clock_cwe" ]; then
        echo "Building with cache + non_concurrent_clock_cwe ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__COST_WEIGHTED_EVICTION__ $RELEASE_OPTS"
    elif [ "$config_type" = "concurrent_clock_buffer_enabled" ]; then
        echo "Building with cache + concurrent_clock_buffer_enabled ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__CLOCK_WITH_BUFFER__ $RELEASE_OPTS"
    elif [ "$config_type" = "concurrent_clock_buffer_enabled_and_relaxed" ]; then
        echo "Building with cache + concurrent_clock_buffer_enabled_and_relaxed ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__CLOCK_WITH_BUFFER__ -D__SELECTIVE_UPDATE__ $RELEASE_OPTS"

    # default
    else
        echo "Building with cache + non_concurrent ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ $RELEASE_OPTS"
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

# Function to run single YCSB cache benchmark with perf profiling
run_ycsb_profiled_benchmark() {
    local tree_type=$1
    local cache_type=$2
    local storage_type=$3
    local cache_size=$4
    local page_size=$5
    local memory_size=$6
    local key_type=$7
    local value_type=$8
    local workload=$9
    local degree=${10}
    local records=${11}
    local config_name=${12}
    local thread_count=${13}
    local cache_size_percentage=${14}
    
    local profile_name="${tree_type}_${cache_type}_${storage_type}_${cache_size}_${page_size}_${memory_size}_${key_type}_${value_type}_${workload}_${degree}_${records}_threads${thread_count}"
    
    # Create a separate folder for this individual run
    local run_folder="$PROFILE_OUTPUT_DIR/${config_name}_${profile_name}"
    mkdir -p "$run_folder"
    
    local perf_output="$run_folder/${config_name}_${profile_name}.prf"
    local perf_data="$run_folder/${config_name}_${profile_name}.data"
    
    echo "=========================================="
    echo "YCSB Cache Profiling: $tree_type - $workload - Degree $degree"
    echo "Cache: $cache_type (Size: $cache_size), Storage: $storage_type"
    echo "Page Size: $page_size, Memory Size: $memory_size"
    echo "Key: $key_type, Value: $value_type, Records: $records"
    echo "Threads: $thread_count"
    echo "Run Folder: $run_folder"
    echo "Output: $perf_output"
    echo "=========================================="
    
    # Ensure benchmark data exists and is accessible
    ensure_benchmark_data
    
    # Set up data path for benchmark execution
    cd "$DATA_BASE_DIR"
    
    # Run benchmark with perf stat (statistical profiling)
    # Execute from data directory so benchmark can find the data folders
    cd "$DATA_BASE_DIR"
    perf stat -e "$PERF_EVENTS" \
              -o "$perf_output" \
              numactl --cpunodebind=0 --membind=0 \
              "$BENCHMARK_EXEC" \
              --config "bm_cache_ycsb" \
              --cache-type "$cache_type" \
              --storage-type "$storage_type" \
              --cache-size "$cache_size" \
              --page-size "$page_size" \
              --memory-size "$memory_size" \
              --tree-type "$tree_type" \
              --key-type "$key_type" \
              --value-type "$value_type" \
              --workload-type "$workload" \
              --degree "$degree" \
              --records "$records" \
              --runs "$RUNS" \
              --threads "$thread_count" \
              --output-dir "$run_folder" \
              --config-name "$config_name" \
              --cache-size-percentage "$cache_size_percentage" \
              --cache-page-limit "$cache_size"
    
    # Return to benchmark directory
    cd "$BENCHMARK_DIR" > /dev/null
    
    echo "Profiling completed for $profile_name"
    echo ""
}

# Function to run single YCSB BiStorage cache benchmark with perf profiling
run_ycsb_bistorage_profiled_benchmark() {
    local tree_type=$1
    local cache_type=$2
    local primary_type=$3
    local secondary_type=$4
    local cache_size=$5
    local page_size=$6
    local memory_size=$7
    local key_type=$8
    local value_type=$9
    local workload=${10}
    local degree=${11}
    local records=${12}
    local config_name=${13}
    local thread_count=${14}
    local cache_size_percentage=${15}
    local primary_read_cost=${16}
    local primary_write_cost=${17}
    local secondary_read_cost=${18}
    local secondary_write_cost=${19}
    
    # Construct storage name from primary and secondary types
    local storage_name="BiStorage_${primary_type}_${secondary_type}"
    
    local profile_name="${tree_type}_${cache_type}_${storage_name}_${cache_size}_${page_size}_${memory_size}_${key_type}_${value_type}_${workload}_${degree}_${records}_threads${thread_count}"
    
    # Create a separate folder for this individual run
    local run_folder="$PROFILE_OUTPUT_DIR/${config_name}_${profile_name}"
    mkdir -p "$run_folder"
    
    local perf_output="$run_folder/${config_name}_${profile_name}.prf"
    local perf_data="$run_folder/${config_name}_${profile_name}.data"
    
    echo "=========================================="
    echo "YCSB BiStorage Cache Profiling: $tree_type - $workload - Degree $degree"
    echo "Cache: $cache_type (Size: $cache_size)"
    echo "Primary Storage: $primary_type"
    echo "Secondary Storage: $secondary_type"
    echo "Page Size: $page_size, Memory Size: $memory_size"
    echo "Key: $key_type, Value: $value_type, Records: $records"
    echo "Threads: $thread_count"
    echo "Costs - Primary R/W: $primary_read_cost/$primary_write_cost ns"
    echo "Costs - Secondary R/W: $secondary_read_cost/$secondary_write_cost ns"
    echo "Run Folder: $run_folder"
    echo "Output: $perf_output"
    echo "=========================================="
    
    # Ensure benchmark data exists and is accessible
    ensure_benchmark_data
    
    # Set up data path for benchmark execution
    cd "$DATA_BASE_DIR"
    
    # Run benchmark with perf stat (statistical profiling)
    # Note: Storage paths are defined in common.h and used internally by the benchmark
    perf stat -e "$PERF_EVENTS" \
              -o "$perf_output" \
              numactl --cpunodebind=0 --membind=0 \
              "$BENCHMARK_EXEC" \
              --config "bm_cache_bistorage_ycsb" \
              --cache-type "$cache_type" \
              --primary-storage-type "$primary_type" \
              --secondary-storage-type "$secondary_type" \
              --primary-read-cost "$primary_read_cost" \
              --primary-write-cost "$primary_write_cost" \
              --secondary-read-cost "$secondary_read_cost" \
              --secondary-write-cost "$secondary_write_cost" \
              --cache-size "$cache_size" \
              --page-size "$page_size" \
              --memory-size "$memory_size" \
              --tree-type "$tree_type" \
              --key-type "$key_type" \
              --value-type "$value_type" \
              --workload-type "$workload" \
              --degree "$degree" \
              --records "$records" \
              --runs "$RUNS" \
              --threads "$thread_count" \
              --output-dir "$run_folder" \
              --config-name "$config_name" \
              --cache-size-percentage "$cache_size_percentage" \
              --cache-page-limit "$cache_size"
    
    # Rename the CSV file to match the perf file naming convention
    local latest_csv=$(ls -t "$run_folder"/benchmark_bistorage_*.csv 2>/dev/null | head -1)
    if [ -f "$latest_csv" ]; then
        local target_csv="$run_folder/${config_name}_${profile_name}.csv"
        mv "$latest_csv" "$target_csv"
        echo "CSV file renamed to: $(basename "$target_csv")"
    fi
    
    echo "YCSB BiStorage cache profiling completed for $profile_name"
    
    # Return to benchmark directory
    cd "$BENCHMARK_DIR" > /dev/null
    
    # Brief sleep to let system settle between benchmarks
    sleep 2
    echo ""
}

# Function to run comprehensive YCSB cache profiling with custom parameters
run_full_ycsb_cache_profiling_with_params() {
    local config_type=${1:-"basic"}  # "basic" or "concurrent"
    local -n cache_types_ref=$2
    local -n storage_types_ref=$3
    local -n cache_size_percentages_ref=$4
    local -n page_sizes_ref=$5
    local -n memory_sizes_ref=$6
    local -n trees_ref=$7
    local -n degrees_ref=$8
    local -n ycsb_workloads_ref=$9
    local -n key_value_combos_ref=${10}
    local -n records_ref=${11}
    local config_suffix=${12:-""}  # Optional suffix for output files
    local -n threads_ref=${13:-THREADS}  # Optional threads parameter, defaults to global THREADS
    
    echo "Starting comprehensive YCSB cache profiling for $config_type configuration${config_suffix:+ ($config_suffix)}..."
    echo "Cache Types: ${cache_types_ref[*]}"
    echo "Storage Types: ${storage_types_ref[*]}"
    echo "Cache Size Percentages: ${cache_size_percentages_ref[*]}"
    echo "Page Sizes: ${page_sizes_ref[*]}"
    echo "Memory Sizes: ${memory_sizes_ref[*]}"
    echo "Trees: ${trees_ref[*]}"
    echo "Degrees: ${degrees_ref[*]}"
    echo "YCSB Workloads: ${ycsb_workloads_ref[*]}"
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
                                    for workload in "${ycsb_workloads_ref[@]}"; do
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
                                        # Calculate actual cache size from percentage and memory size
                                        local actual_cache_size=$(calculate_cache_size "$cache_size_percentage" "$records" "$degree")
                                        
                                        echo "Cache size calculation: $cache_size_percentage of estimated pages for $records records (degree $degree) = $actual_cache_size entries"
                                    
                                        for workload in "${ycsb_workloads_ref[@]}"; do
                                            for thread_count in "${threads_ref[@]}"; do
                                                ((current_combination++))
                                                echo "Progress: $current_combination/$total_combinations ($config_id)"
                                                
                                                run_ycsb_profiled_benchmark "$tree" "$cache_type" "$storage_type" "$actual_cache_size" "$page_size" "$memory_size" "$key_type" "$value_type" "$workload" "$degree" "$records" "$config_id" "$thread_count" "$cache_size_percentage"
                                                
                                                # Small delay between runs to let system settle
                                                #sleep 5
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
    echo "YCSB cache profiling completed for $config_id!"
    echo "Results saved in: $PROFILE_OUTPUT_DIR"
    echo "Total profiles generated: $total_combinations"
    echo "=========================================="
}

# Function to run single-threaded YCSB cache profiling
run_full_ycsb_cache_profiling_single_threaded() {
    # Create a local single-threaded array
    local SINGLE_THREADS=(1)
    local CACHE_TYPES_LOCAL=("LRU")

    echo "Running single-threaded YCSB cache profiling..."
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_default"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    #return

    local CACHE_TYPES_LOCAL=("A2Q")

    echo "Running single-threaded YCSB cache profiling..."
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_default"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")

    echo "Running single-threaded YCSB cache profiling..."
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_default"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    run_full_ycsb_cache_profiling_single_threaded_ex
}

run_full_ycsb_cache_profiling_single_threaded_ex() {
    # Create a local single-threaded array
    local SINGLE_THREADS=(1)
    local CACHE_TYPES_LOCAL=("LRU")

    echo "Running single-threaded YCSB cache profiling (extended)..."
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_relaxed"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    
    local CACHE_TYPES_LOCAL=("A2Q")

    echo "Running single-threaded YCSB cache profiling (extended)..."
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_relaxed"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")

    echo "Running single-threaded YCSB cache profiling (extended)..."
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_relaxed"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    
    local CACHE_TYPES_LOCAL=("LRU")

    local config_type="non_concurrent_lru_metadata_update_in_order"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local config_type="non_concurrent_lru_metadata_update_in_order_and_relaxed"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local CACHE_TYPES_LOCAL=("A2Q")

    local config_type="non_concurrent_a2q_ghost_q_enabled"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local config_type="non_concurrent_a2q_ghost_q_enabled_and_relaxed"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
}

run_full_ycsb_cache_profiling_multi_threaded() {
    echo "Running multi-threaded YCSB cache profiling..."
    echo "Using threads: ${THREADS[*]}"
    
    # Create a local single-threaded array
    local CACHE_TYPES_LOCAL=("LRU")
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"

    echo "Running multi-threaded YCSB cache profiling..."
    
    local config_type="concurrent_default"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("A2Q")
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"

    echo "Running multi-threaded YCSB cache profiling..."
    
    local config_type="concurrent_default"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"

    echo "Running multi-threaded YCSB cache profiling..."
    
    local config_type="concurrent_default"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    run_full_ycsb_cache_profiling_multi_threaded_ex
}

run_full_ycsb_cache_profiling_multi_threaded_ex() {
    echo "Running multi-threaded YCSB cache profiling (extended)..."
    echo "Using threads: ${THREADS[*]}"
    
    # Create a local single-threaded array
    local CACHE_TYPES_LOCAL=("LRU")
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"

    echo "Running multi-threaded YCSB cache profiling (extended)..."
    
    local config_type="concurrent_relaxed"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("A2Q")
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"

    echo "Running multi-threaded YCSB cache profiling (extended)..."
    
    local config_type="concurrent_relaxed"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"

    echo "Running multi-threaded YCSB cache profiling (extended)..."
    
    local config_type="concurrent_relaxed"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("LRU")

    local config_type="concurrent_lru_metadata_update_in_order"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local config_type="concurrent_lru_metadata_update_in_order_and_relaxed"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("A2Q")

    local config_type="concurrent_a2q_ghost_q_enabled"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local config_type="concurrent_a2q_ghost_q_enabled_and_relaxed"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")

    local config_type="concurrent_clock_buffer_enabled"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local config_type="concurrent_clock_buffer_enabled_and_relaxed"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS
}

# Function to run single-threaded YCSB cache profiling
run_full_ycsb_cache_profiling_thread_scaling() {
    # Create a local single-threaded array
    local SINGLE_THREADS=(1)
    local CACHE_TYPES_LOCAL=("LRU")

    echo "Running single-threaded YCSB cache profiling..."
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_default"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local CACHE_TYPES_LOCAL=("A2Q")

    echo "Running single-threaded YCSB cache profiling..."
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_default"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")

    echo "Running single-threaded YCSB cache profiling..."
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_default"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    # Create a local single-threaded array
    local SINGLE_THREADS=(1)
    local CACHE_TYPES_LOCAL=("LRU")

    echo "Running single-threaded YCSB cache profiling (extended)..."
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_relaxed"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    
    local CACHE_TYPES_LOCAL=("A2Q")

    echo "Running single-threaded YCSB cache profiling (extended)..."
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_relaxed"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")

    echo "Running single-threaded YCSB cache profiling (extended)..."
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_relaxed"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    
    local CACHE_TYPES_LOCAL=("LRU")

    local config_type="non_concurrent_lru_metadata_update_in_order"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local config_type="non_concurrent_lru_metadata_update_in_order_and_relaxed"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local CACHE_TYPES_LOCAL=("A2Q")

    local config_type="non_concurrent_a2q_ghost_q_enabled"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local config_type="non_concurrent_a2q_ghost_q_enabled_and_relaxed"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    echo "Running multi-threaded YCSB cache profiling..."
    echo "Using threads: ${THREADS[*]}"
    
    # Create a local single-threaded array
    local CACHE_TYPES_LOCAL=("LRU")
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"

    echo "Running multi-threaded YCSB cache profiling..."
    
    local config_type="concurrent_default"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("A2Q")
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"

    echo "Running multi-threaded YCSB cache profiling..."
    
    local config_type="concurrent_default"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"

    echo "Running multi-threaded YCSB cache profiling..."
    
    local config_type="concurrent_default"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    echo "Running multi-threaded YCSB cache profiling (extended)..."
    echo "Using threads: ${THREADS[*]}"
    
    # Create a local single-threaded array
    local CACHE_TYPES_LOCAL=("LRU")
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"

    echo "Running multi-threaded YCSB cache profiling (extended)..."
    
    local config_type="concurrent_relaxed"    
    run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("A2Q")
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"

    echo "Running multi-threaded YCSB cache profiling (extended)..."
    
    local config_type="concurrent_relaxed"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")
    echo "Using cache types: ${CACHE_TYPES_LOCAL[*]}"

    echo "Running multi-threaded YCSB cache profiling (extended)..."
    
    local config_type="concurrent_relaxed"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("LRU")

    local config_type="concurrent_lru_metadata_update_in_order"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local config_type="concurrent_lru_metadata_update_in_order_and_relaxed"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("A2Q")

    local config_type="concurrent_a2q_ghost_q_enabled"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local config_type="concurrent_a2q_ghost_q_enabled_and_relaxed"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")

    local config_type="concurrent_clock_buffer_enabled"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local config_type="concurrent_clock_buffer_enabled_and_relaxed"    
    #run_full_ycsb_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS
}

# Function to run comprehensive YCSB BiStorage cache profiling with custom parameters
run_full_ycsb_bistorage_cache_profiling_with_params() {
    local config_type=${1:-"basic"}  # "basic" or "concurrent"
    local -n cache_types_ref=$2
    local primary_storage=$3
    local primary_read_cost=$4
    local primary_write_cost=$5
    local secondary_storage=$6
    local secondary_read_cost=$7
    local secondary_write_cost=$8
    local -n cache_size_percentages_ref=$9
    local -n page_sizes_ref=${10}
    local -n memory_sizes_ref=${11}
    local -n trees_ref=${12}
    local -n degrees_ref=${13}
    local -n ycsb_workloads_ref=${14}
    local -n key_value_combos_ref=${15}
    local -n records_ref=${16}
    local config_suffix=${17:-""}
    local -n threads_ref=${18:-THREADS}
    
    echo "Starting comprehensive YCSB BiStorage cache profiling for $config_type configuration${config_suffix:+ ($config_suffix)}..."
    echo "Cache Types: ${cache_types_ref[*]}"
    echo "Primary Storage: $primary_storage (R/W costs: $primary_read_cost/$primary_write_cost ns)"
    echo "Secondary Storage: $secondary_storage (R/W costs: $secondary_read_cost/$secondary_write_cost ns)"
    echo "Cache Size Percentages: ${cache_size_percentages_ref[*]}"
    echo "Page Sizes: ${page_sizes_ref[*]}"
    echo "Memory Sizes: ${memory_sizes_ref[*]}"
    echo "Trees: ${trees_ref[*]}"
    echo "Degrees: ${degrees_ref[*]}"
    echo "YCSB Workloads: ${ycsb_workloads_ref[*]}"
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
                for cache_size_percentage in "${cache_size_percentages_ref[@]}"; do
                    for page_size in "${page_sizes_ref[@]}"; do
                        for memory_size in "${memory_sizes_ref[@]}"; do
                            for degree in "${degrees_ref[@]}"; do
                                for workload in "${ycsb_workloads_ref[@]}"; do
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
    
    echo "Total YCSB BiStorage combinations to profile for $config_type${config_suffix:+ ($config_suffix)}: $total_combinations"
    echo ""
    
    # Create config-specific identifier for output files
    local config_id="${config_type}${config_suffix}"
    
    # Run profiling for each combination
    for combo_name in "${!key_value_combos_ref[@]}"; do
        IFS=' ' read -r key_type value_type <<< "${key_value_combos_ref[$combo_name]}"
        
        echo "Processing key-value combination: $key_type -> $value_type"
        
        for tree in "${trees_ref[@]}"; do
            for cache_type in "${cache_types_ref[@]}"; do
                echo "YCSB BiStorage Configuration:"
                echo "  Primary: $primary_storage (R/W costs: $primary_read_cost/$primary_write_cost ns)"
                echo "  Secondary: $secondary_storage (R/W costs: $secondary_read_cost/$secondary_write_cost ns)"
                
                for cache_size_percentage in "${cache_size_percentages_ref[@]}"; do
                    for page_size in "${page_sizes_ref[@]}"; do
                        for degree in "${degrees_ref[@]}"; do
                            for records in "${records_ref[@]}"; do
                                for memory_size in "${memory_sizes_ref[@]}"; do
                                    # Calculate actual cache size from percentage and memory size
                                    local actual_cache_size=$(calculate_cache_size "$cache_size_percentage" "$records" "$degree")
                                    
                                    echo "YCSB BiStorage cache size calculation: $cache_size_percentage of estimated pages for $records records (degree $degree) = $actual_cache_size entries"
                                
                                    for workload in "${ycsb_workloads_ref[@]}"; do
                                        for thread_count in "${threads_ref[@]}"; do
                                            ((current_combination++))
                                            echo "Progress: $current_combination/$total_combinations ($config_id)"
                                            
                                            run_ycsb_bistorage_profiled_benchmark \
                                                "$tree" "$cache_type" \
                                                "$primary_storage" "$secondary_storage" \
                                                "$actual_cache_size" "$page_size" "$memory_size" \
                                                "$key_type" "$value_type" "$workload" \
                                                "$degree" "$records" "$config_id" "$thread_count" \
                                                "$cache_size_percentage" \
                                                "$primary_read_cost" "$primary_write_cost" \
                                                "$secondary_read_cost" "$secondary_write_cost"
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
    echo "YCSB BiStorage cache profiling completed for $config_id!"
    echo "Results saved in: $PROFILE_OUTPUT_DIR"
    echo "Total profiles generated: $total_combinations"
    echo "=========================================="
}

# Function to run single-threaded YCSB BiStorage cache profiling
run_full_ycsb_bistorage_cache_profiling_single_threaded() {
    # Create a local single-threaded array
    local SINGLE_THREADS=(1)
    local CACHE_TYPES_LOCAL=("LRU")
    local config_type="non_concurrent_lru_metadata_update_in_order"    
    run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "FileStorage" 5 5 "VolatileStorage" 0 0   CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    local config_type="non_concurrent_lru_metadata_update_in_order_and_relaxed"    
    run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "FileStorage" 5 5 "VolatileStorage" 0 0   CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    local CACHE_TYPES_LOCAL=("A2Q")
    local config_type="non_concurrent_a2q_ghost_q_enabled"    
    run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "FileStorage" 5 5 "VolatileStorage" 0 0   CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    local config_type="non_concurrent_a2q_ghost_q_enabled_and_relaxed"    
    run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "FileStorage" 5 5 "VolatileStorage" 0 0   CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    local config_type="non_concurrent_default"    
    run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "FileStorage" 5 5 "VolatileStorage" 0 0   CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    local config_type="non_concurrent_relaxed"    
    run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "FileStorage" 5 5 "VolatileStorage" 0 0   CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    local CACHE_TYPES_LOCAL=("CLOCK")
    local config_type="non_concurrent_default"    
    run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "FileStorage" 5 5 "VolatileStorage" 0 0   CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    local config_type="non_concurrent_relaxed"    
    run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "FileStorage" 5 5 "VolatileStorage" 0 0   CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    local config_type="non_concurrent_cwe"    
    run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "FileStorage" 5 5 "VolatileStorage" 0 0   CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    #run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "FileStorage" 5 5 "VolatileStorage" 0 0 CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    #run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "VolatileStorage" "$BISTORAGE_PRIMARY_READ_COST" "$BISTORAGE_PRIMARY_WRITE_COST" "FileStorage" "$BISTORAGE_SECONDARY_READ_COST" "$BISTORAGE_SECONDARY_WRITE_COST" CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    #run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "PMemStorage" "$BISTORAGE_PRIMARY_READ_COST" "$BISTORAGE_PRIMARY_WRITE_COST" "FileStorage" "$BISTORAGE_SECONDARY_READ_COST" "$BISTORAGE_SECONDARY_WRITE_COST" CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local CACHE_TYPES_LOCAL=("A2Q")

    echo "Running single-threaded YCSB BiStorage cache profiling with A2Q..."
    echo "Using threads: ${SINGLE_THREADS[*]}"
    
    local config_type="non_concurrent_default"    
    #run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "$BISTORAGE_PRIMARY_STORAGE" "$BISTORAGE_PRIMARY_READ_COST" "$BISTORAGE_PRIMARY_WRITE_COST" "$BISTORAGE_SECONDARY_STORAGE" "$BISTORAGE_SECONDARY_READ_COST" "$BISTORAGE_SECONDARY_WRITE_COST" CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    #run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "VolatileStorage" "$BISTORAGE_PRIMARY_READ_COST" "$BISTORAGE_PRIMARY_WRITE_COST" "PMemStorage" "$BISTORAGE_SECONDARY_READ_COST" "$BISTORAGE_SECONDARY_WRITE_COST" CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    #run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "VolatileStorage" "$BISTORAGE_PRIMARY_READ_COST" "$BISTORAGE_PRIMARY_WRITE_COST" "FileStorage" "$BISTORAGE_SECONDARY_READ_COST" "$BISTORAGE_SECONDARY_WRITE_COST" CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    #run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "PMemStorage" "$BISTORAGE_PRIMARY_READ_COST" "$BISTORAGE_PRIMARY_WRITE_COST" "FileStorage" "$BISTORAGE_SECONDARY_READ_COST" "$BISTORAGE_SECONDARY_WRITE_COST" CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")

    echo "Running single-threaded YCSB BiStorage cache profiling with CLOCK..."
    echo "Using threads: ${SINGLE_THREADS[*]}"
    
    local config_type="non_concurrent_default"    
    #run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "$BISTORAGE_PRIMARY_STORAGE" "$BISTORAGE_PRIMARY_READ_COST" "$BISTORAGE_PRIMARY_WRITE_COST" "$BISTORAGE_SECONDARY_STORAGE" "$BISTORAGE_SECONDARY_READ_COST" "$BISTORAGE_SECONDARY_WRITE_COST" CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    #run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "VolatileStorage" "$BISTORAGE_PRIMARY_READ_COST" "$BISTORAGE_PRIMARY_WRITE_COST" "PMemStorage" "$BISTORAGE_SECONDARY_READ_COST" "$BISTORAGE_SECONDARY_WRITE_COST" CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    #run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "VolatileStorage" "$BISTORAGE_PRIMARY_READ_COST" "$BISTORAGE_PRIMARY_WRITE_COST" "FileStorage" "$BISTORAGE_SECONDARY_READ_COST" "$BISTORAGE_SECONDARY_WRITE_COST" CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    #run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "PMemStorage" "$BISTORAGE_PRIMARY_READ_COST" "$BISTORAGE_PRIMARY_WRITE_COST" "FileStorage" "$BISTORAGE_SECONDARY_READ_COST" "$BISTORAGE_SECONDARY_WRITE_COST" CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
}

# Function to run multi-threaded YCSB BiStorage cache profiling
run_full_ycsb_bistorage_cache_profiling_multi_threaded() {
    echo "Running multi-threaded YCSB BiStorage cache profiling..."
    echo "Using threads: ${THREADS[*]}"
    
    local CACHE_TYPES_LOCAL=("LRU")
    echo "Running multi-threaded YCSB BiStorage cache profiling with LRU..."

    local config_type="concurrent_default"    
    run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "$BISTORAGE_PRIMARY_STORAGE" "$BISTORAGE_PRIMARY_READ_COST" "$BISTORAGE_PRIMARY_WRITE_COST" "$BISTORAGE_SECONDARY_STORAGE" "$BISTORAGE_SECONDARY_READ_COST" "$BISTORAGE_SECONDARY_WRITE_COST" CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("A2Q")
    echo "Running multi-threaded YCSB BiStorage cache profiling with A2Q..."

    local config_type="concurrent_default"    
    run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "$BISTORAGE_PRIMARY_STORAGE" "$BISTORAGE_PRIMARY_READ_COST" "$BISTORAGE_PRIMARY_WRITE_COST" "$BISTORAGE_SECONDARY_STORAGE" "$BISTORAGE_SECONDARY_READ_COST" "$BISTORAGE_SECONDARY_WRITE_COST" CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")
    echo "Running multi-threaded YCSB BiStorage cache profiling with CLOCK..."

    local config_type="concurrent_default"    
    run_full_ycsb_bistorage_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL "$BISTORAGE_PRIMARY_STORAGE" "$BISTORAGE_PRIMARY_READ_COST" "$BISTORAGE_PRIMARY_WRITE_COST" "$BISTORAGE_SECONDARY_STORAGE" "$BISTORAGE_SECONDARY_READ_COST" "$BISTORAGE_SECONDARY_WRITE_COST" CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES YCSB_WORKLOADS KEY_VALUE_COMBOS RECORDS "" THREADS
}

# Function to combine CSV files from all benchmark runs
combine_csv_files() {
    local target_dir=${1:-$PROFILE_OUTPUT_DIR}
    
    echo "=========================================="
    echo "Combining all CSV files from YCSB benchmark runs with perf data"
    echo "=========================================="
    
    # If target_dir doesn't exist, try to find the most recent results directory with CSV files
    if [ ! -d "$target_dir" ] || [ -z "$(ls -A "$target_dir" 2>/dev/null)" ]; then
        echo "Target directory empty or not found: $target_dir"
        echo "Looking for existing results directories with CSV files..."
        
        local found_dir=""
        for dir in $(ls -td "$BENCHMARK_DIR"/ycsb_cache_profiling_results_* 2>/dev/null); do
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
    local combined_csv="$target_dir/combined_ycsb_benchmark_results_with_perf_${merge_timestamp}.csv"
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
                
                # Add perf data to each data row
                tail -n +2 "$csv_file" | awk -v OFS=',' -v cm="$cache_misses" -v cr="$cache_references" -v cy="$cycles" -v ins="$instructions" -v bm="$branch_misses" -v pf="$page_faults" -v l1m="$l1_dcache_misses" -v l1l="$l1_dcache_loads" -v llcm="$llc_misses" -v llcl="$llc_loads" '{print $0,cm,cr,cy,ins,bm,pf,l1m,l1l,llcm,llcl}' >> "$combined_csv"
            else
                echo "  PRF: Not found"
                
                # No perf data available, add empty columns
                if [ "$header_written" = false ]; then
                    head -1 "$csv_file" | awk -v OFS=',' '{print $0,"perf_cache_misses","perf_cache_references","perf_cycles","perf_instructions","perf_branch_misses","perf_page_faults","perf_l1_dcache_misses","perf_l1_dcache_loads","perf_llc_misses","perf_llc_loads"}' > "$combined_csv"
                    header_written=true
                fi
                
                # Add empty perf columns
                tail -n +2 "$csv_file" | awk -v OFS=',' '{print $0,"","","","","","","","","",""}' >> "$combined_csv"
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

# Main script logic
case "${1:-full}" in
    "full")
        run_full_ycsb_bistorage_cache_profiling_single_threaded
        echo ""
        echo "=========================================="
        echo "Combining CSV files with perf data..."
        echo "=========================================="
        combine_csv_files "$PROFILE_OUTPUT_DIR"
        ;;
    "thread_scaling")
        run_full_ycsb_cache_profiling_thread_scaling
        echo ""
        echo "=========================================="
        echo "Combining CSV files with perf data..."
        echo "=========================================="
        combine_csv_files "$PROFILE_OUTPUT_DIR"
        ;;
    "multi")
        run_full_ycsb_cache_profiling_multi_threaded
        echo ""
        echo "=========================================="
        echo "Combining CSV files with perf data..."
        echo "=========================================="
        combine_csv_files "$PROFILE_OUTPUT_DIR"
        ;;
    "bistorage_single")
        run_full_ycsb_bistorage_cache_profiling_single_threaded
        echo ""
        echo "=========================================="
        echo "Combining CSV files with perf data..."
        echo "=========================================="
        combine_csv_files "$PROFILE_OUTPUT_DIR"
        ;;
    "bistorage_multi")
        run_full_ycsb_bistorage_cache_profiling_multi_threaded
        echo ""
        echo "=========================================="
        echo "Combining CSV files with perf data..."
        echo "=========================================="
        combine_csv_files "$PROFILE_OUTPUT_DIR"
        ;;
    "bistorage")
        #run_full_ycsb_cache_profiling_single_threaded
        run_full_ycsb_bistorage_cache_profiling_single_threaded
        #run_full_ycsb_bistorage_cache_profiling_multi_threaded
        echo ""
        echo "=========================================="
        echo "Combining CSV files with perf data..."
        echo "=========================================="
        combine_csv_files "$PROFILE_OUTPUT_DIR"
        ;;
    "combine")
        combine_csv_files "$2"
        ;;
    "help"|"-h"|"--help")
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  full              - Run both single-threaded and multi-threaded YCSB benchmarks (default)"
        echo "  single            - Run only single-threaded YCSB benchmarks"
        echo "  multi             - Run only multi-threaded YCSB benchmarks"
        echo "  thread_scaling    - Run thread scaling benchmarks"
        echo "  bistorage         - Run both single-threaded and multi-threaded YCSB BiStorage benchmarks"
        echo "  bistorage_single  - Run only single-threaded YCSB BiStorage benchmarks"
        echo "  bistorage_multi   - Run only multi-threaded YCSB BiStorage benchmarks"
        echo "  combine           - Combine CSV files from existing results directory"
        echo "  help              - Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0                           # Run full benchmark suite"
        echo "  $0 single                    # Run single-threaded only"
        echo "  $0 multi                     # Run multi-threaded only"
        echo "  $0 bistorage                 # Run BiStorage benchmarks (single and multi-threaded)"
        echo "  $0 bistorage_single          # Run single-threaded BiStorage only"
        echo "  $0 bistorage_multi           # Run multi-threaded BiStorage only"
        echo "  $0 combine /path/to/results  # Combine existing results"
        ;;
    *)
        echo "Unknown command: $1"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac

echo ""
echo "YCSB cache benchmark script completed!"
echo "Results directory: $PROFILE_OUTPUT_DIR"