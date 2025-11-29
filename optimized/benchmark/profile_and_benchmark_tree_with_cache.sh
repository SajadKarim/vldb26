#!/bin/bash

# Cache Profiling Benchmark Script
# This script runs comprehensive cache benchmarks with different cache types, storage types, and parameters
# Based on profile_and_benchmark_tree_with_nocache.sh but adapted for cache testing
#
# Features:
# - Configurable data path: Benchmark data is stored in /home/skarim/Code/haldendb_ex/haldendb_pvt/benchmark/benchmark_data
# - Automatic data generation: Checks if data exists and generates it if missing
# - Data reuse: Existing data is reused across benchmark runs for consistency
#
# Environment Variables:
# - THREADS: Array of thread counts for concurrent operations (default: defined in script)

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="$SCRIPT_DIR/build"
BENCHMARK_EXEC="$BENCHMARK_DIR/benchmark"

# Data directory configuration - Accept from environment variable or use default
# Usage: DATA_PATH=/path/to/data ./profile_and_benchmark_tree_with_cache.sh
# Default: /home/skarim/benchmark_data
DATA_BASE_DIR="${DATA_PATH:-/home/skarim/benchmark_data}"
DATA_DIR="$DATA_BASE_DIR/data"
YCSB_DIR="$DATA_BASE_DIR/ycsb"

# Create data directories if they don't exist
mkdir -p "$DATA_BASE_DIR"
mkdir -p "$DATA_DIR"
mkdir -p "$YCSB_DIR"

# Create timestamped output directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
PROFILE_OUTPUT_DIR="$BENCHMARK_DIR/cache_profiling_results_${TIMESTAMP}"

# Create output directory
mkdir -p "$PROFILE_OUTPUT_DIR"

echo "=========================================="
echo "Cache Profiling Results Directory: $PROFILE_OUTPUT_DIR"
echo "Data Directory: $DATA_BASE_DIR"
echo "=========================================="

# Cache-specific configuration arrays
CACHE_TYPES=("LRU" "A2Q" "CLOCK")
STORAGE_TYPES=("VolatileStorage" "PMemStorage" "FileStorage")
CACHE_SIZE_PERCENTAGES=("2%" "10%" "25%")  # Cache sizes as percentages of estimated B+ tree pages
PAGE_SIZES=(4096)
MEMORY_SIZES=(34359738368) #(2147483648)  # 512MB, 1GB, 2GB

# Tree types to test (focus on main ones for cache testing)
TREES=("BplusTreeSOA")

# Degrees to test (subset for cache profiling)
DEGREES=(24)

# Operations to profile
OPERATIONS=("insert" "search_random" "search_sequential" "search_uniform" "search_zipfian" "delete")

# Key-Value type combinations (only implemented combinations)
declare -A KEY_VALUE_COMBOS
KEY_VALUE_COMBOS["uint64_t_uint64_t"]="uint64_t uint64_t"
#KEY_VALUE_COMBOS["char16_char16"]="char16 char16"
#KEY_VALUE_COMBOS["uint64_t_char16"]="uint64_t char16"

# Record count for profiling
RECORDS=(500000)
RUNS=${RUNS:-5}  # Default to 3, but allow override via environment variable
#THREADS=(2 4 8 12 16 20 24 28 32 36 40)
THREADS=(4)

# Perf events to collect (cache-focused)
PERF_EVENTS="cache-misses,cache-references,cycles,instructions,branch-misses,page-faults,L1-dcache-load-misses,L1-dcache-loads,LLC-load-misses,LLC-loads,L1-icache-load-misses,L1-icache-loads"

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
        
        # Change to the data directory - no symlinks needed since directories are already created
        cd "$DATA_BASE_DIR"
        
        # The data and ycsb directories are already created above, no symlinks needed
        
        # Run benchmark executable to generate data (using a minimal configuration)
        echo "Generating basic workload data..."
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
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    elif [ "$config_type" = "non_concurrent_relaxed" ]; then
        echo "Building with cache + non_concurrent_relaxed ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__SELECTIVE_UPDATE__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    # LRU Specific
    elif [ "$config_type" = "non_concurrent_lru_metadata_update_in_order" ]; then
        echo "Building with cache + non_concurrent_lru_metadata_update_in_order ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__UPDATE_IN_ORDER__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    elif [ "$config_type" = "non_concurrent_lru_metadata_update_in_order_and_relaxed" ]; then
        echo "Building with cache + non_concurrent_lru_metadata_update_in_order_and_relaxed ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__UPDATE_IN_ORDER__ -D__SELECTIVE_UPDATE__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    # A2Q Specific
    elif [ "$config_type" = "non_concurrent_a2q_ghost_q_enabled" ]; then
        echo "Building with cache + non_concurrent_a2q_ghost_q_enabled ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__MANAGE_GHOST_Q__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    elif [ "$config_type" = "non_concurrent_a2q_ghost_q_enabled_and_relaxed" ]; then
        echo "Building with cache + non_concurrent_a2q_ghost_q_enabled_and_relaxed ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__MANAGE_GHOST_Q__ -D__SELECTIVE_UPDATE__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    # Concurrent ones
    elif [ "$config_type" = "concurrent_default" ]; then
        echo "Building with cache + concurrent_default ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    elif [ "$config_type" = "concurrent_relaxed" ]; then
        echo "Building with cache + concurrent_relaxed ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__SELECTIVE_UPDATE__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    # LRU Specific
    elif [ "$config_type" = "concurrent_lru_metadata_update_in_order" ]; then
        echo "Building with cache + concurrent_lru_metadata_update_in_order ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__UPDATE_IN_ORDER__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    elif [ "$config_type" = "concurrent_lru_metadata_update_in_order_and_relaxed" ]; then
        echo "Building with cache + concurrent_lru_metadata_update_in_order_and_relaxed ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__UPDATE_IN_ORDER__ -D__SELECTIVE_UPDATE__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    # A2Q Specific
    elif [ "$config_type" = "concurrent_a2q_ghost_q_enabled" ]; then
        echo "Building with cache + concurrent_a2q_ghost_q_enabled ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__MANAGE_GHOST_Q__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    elif [ "$config_type" = "concurrent_a2q_ghost_q_enabled_and_relaxed" ]; then
        echo "Building with cache + concurrent_a2q_ghost_q_enabled_and_relaxed ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__MANAGE_GHOST_Q__ -D__SELECTIVE_UPDATE__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    # CLOCK Specific __CLOCK_WITH_BUFFER__ is with concurrent
    elif [ "$config_type" = "concurrent_clock_buffer_enabled" ]; then
        echo "Building with cache + concurrent_clock_buffer_enabled ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__CLOCK_WITH_BUFFER__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"
    elif [ "$config_type" = "concurrent_clock_buffer_enabled_and_relaxed" ]; then
        echo "Building with cache + concurrent_clock_buffer_enabled_and_relaxed ..."
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__CLOCK_WITH_BUFFER__ -D__SELECTIVE_UPDATE__ -D__CACHE_COUNTERS__ $RELEASE_OPTS"

    # default
    else
        echo "Building with cache + non_concurrent ..."
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
    local cache_size_percentage=${14}
    
    local profile_name="${tree_type}_${cache_type}_${storage_type}_${cache_size}_${page_size}_${memory_size}_${key_type}_${value_type}_${operation}_${degree}_${records}_threads${thread_count}"
    
    # Create a separate folder for this individual run
    local run_folder="$PROFILE_OUTPUT_DIR/${config_name}_${profile_name}"
    mkdir -p "$run_folder"
    
    local perf_output="$run_folder/${config_name}_${profile_name}.prf"
    local perf_data="$run_folder/${config_name}_${profile_name}.data"
    
    echo "=========================================="
    echo "Cache Profiling: $tree_type - $operation - Degree $degree"
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
    
    # Data directories are already created, no symlinks needed
    
    # Run benchmark with perf stat (statistical profiling)
    # Execute from data directory so benchmark can find the data folders
    cd "$DATA_BASE_DIR"
    perf stat -e "$PERF_EVENTS" \
              -o "$perf_output" \
              numactl --cpunodebind=0 --membind=0 \
              "$BENCHMARK_EXEC" \
              --config "bm_cache" \
              --cache-type "$cache_type" \
              --storage-type "$storage_type" \
              --cache-size "$cache_size" \
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
              --cache-size-percentage "$cache_size_percentage" \
              --cache-page-limit "$cache_size"
    
    # Also run with perf record for detailed analysis (optional)
    if [ "$DETAILED_PROFILING" = "true" ]; then
        echo "Running detailed profiling with perf record..."
        perf record -e cycles,cache-misses \
                    -o "$perf_data" \
                    numactl --cpunodebind=0 --membind=0\
                    "$BENCHMARK_EXEC" \
                    --config "bm_cache" \
                    --cache-type "$cache_type" \
                    --storage-type "$storage_type" \
                    --cache-size "$cache_size" \
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
                    --cache-size-percentage "$cache_size_percentage" \
                    --cache-page-limit "$cache_size"
    fi
    
    # Rename the CSV file to match the perf file naming convention (remove timestamp)
    local latest_csv=$(ls -t "$run_folder"/benchmark_single_*.csv 2>/dev/null | head -1)
    if [ -f "$latest_csv" ]; then
        local target_csv="$run_folder/${config_name}_${profile_name}.csv"
        mv "$latest_csv" "$target_csv"
        echo "CSV file renamed to: $(basename "$target_csv")"
    fi
    
    echo "Cache profiling completed for $profile_name"
    
    # Return to benchmark directory
    cd "$BENCHMARK_DIR" > /dev/null
    
    # Brief sleep to let system settle between benchmarks
    sleep 2
    echo ""
}

# Function to run comprehensive cache profiling with custom parameters
run_full_cache_profiling_with_params() {
    local config_type=${1:-"basic"}  # "basic" or "concurrent"
    local -n cache_types_ref=$2
    local -n storage_types_ref=$3
    local -n cache_size_percentages_ref=$4  # Now expects percentages instead of absolute sizes
    local -n page_sizes_ref=$5
    local -n memory_sizes_ref=$6
    local -n trees_ref=$7
    local -n degrees_ref=$8
    local -n operations_ref=$9
    local -n key_value_combos_ref=${10}
    local -n records_ref=${11}
    local config_suffix=${12:-""}  # Optional suffix for output files
    local -n threads_ref=${13:-THREADS}  # Optional threads parameter, defaults to global THREADS
    
    echo "Starting comprehensive cache profiling for $config_type configuration${config_suffix:+ ($config_suffix)}..."
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
                                        # Calculate actual cache size from percentage and memory size
                                        local actual_cache_size=$(calculate_cache_size "$cache_size_percentage" "$records" "$degree")
                                        
                                        echo "Cache size calculation: $cache_size_percentage of estimated pages for $records records (degree $degree) = $actual_cache_size entries"
                                    
                                        for operation in "${operations_ref[@]}"; do
                                            for thread_count in "${threads_ref[@]}"; do
                                                ((current_combination++))
                                                echo "Progress: $current_combination/$total_combinations ($config_id)"
                                                
                                                run_cache_profiled_benchmark "$tree" "$cache_type" "$storage_type" "$actual_cache_size" "$page_size" "$memory_size" "$key_type" "$value_type" "$operation" "$degree" "$records" "$config_id" "$thread_count" "$cache_size_percentage"
                                                
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
    echo "Cache profiling completed for $config_id!"
    echo "Results saved in: $PROFILE_OUTPUT_DIR"
    echo "Total profiles generated: $total_combinations"
    echo "=========================================="
}

# Function to run comprehensive cache profiling for all combinations (backward compatibility)
run_full_cache_profiling() {
    local config_type=${1:-"basic"}  # "basic" or "concurrent"
    
    # Call the parameterized version with global variables
    run_full_cache_profiling_with_params "$config_type" CACHE_TYPES STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" THREADS
}

run_full_cache_profiling_single_threaded() {
    # Create a local single-threaded array
    local SINGLE_THREADS=(1)
    local CACHE_TYPES_LOCAL=("LRU")

    echo "Running single-threaded cache profiling..."
    echo "Using threads: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_default"    
    run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local CACHE_TYPES_LOCAL=("A2Q")

    echo "Running single-threaded cache profiling..."
    echo "Using threads: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_default"    
    run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")

    echo "Running single-threaded cache profiling..."
    echo "Using threads: ${CACHE_TYPES_LOCAL[*]}"
    
    local config_type="non_concurrent_default"    
    run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS
    #local config_type="non_concurrent_relaxed"    
    #run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    #local CACHE_TYPES_LOCAL=("LRU")

    #local config_type="non_concurrent_lru_metadata_update_in_order"    
    #run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    #local config_type="non_concurrent_lru_metadata_update_in_order_and_relaxed"    
    #run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    #local CACHE_TYPES_LOCAL=("A2Q")

    #local config_type="non_concurrent_a2q_ghost_q_enabled"    
    #run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

    #local config_type="non_concurrent_a2q_ghost_q_enabled_and_relaxed"    
    #run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" SINGLE_THREADS

}

run_full_cache_profiling_multi_threaded() {
    echo "Running multi-threaded cache profiling..."
    echo "Using threads: ${THREADS[*]}"
    
    # Create a local single-threaded array
    local CACHE_TYPES_LOCAL=("LRU")
    echo "Using threads: ${CACHE_TYPES_LOCAL[*]}"

    echo "Running single-threaded cache profiling..."
    
    local config_type="concurrent_default"    
    run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("A2Q")
    echo "Using threads: ${CACHE_TYPES_LOCAL[*]}"

    echo "Running single-threaded cache profiling..."
    
    local config_type="concurrent_default"    
    run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" THREADS

    local CACHE_TYPES_LOCAL=("CLOCK")
    echo "Using threads: ${CACHE_TYPES_LOCAL[*]}"

    echo "Running single-threaded cache profiling..."
    
    local config_type="concurrent_default"    
    run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" THREADS

    #local config_type="concurrent_relaxed"    
    #run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" THREADS

    #local CACHE_TYPES_LOCAL=("LRU")

    #local config_type="concurrent_lru_metadata_update_in_order"    
    #run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" THREADS

    #local config_type="concurrent_lru_metadata_update_in_order_and_relaxed"    
    #run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" THREADS

    #local CACHE_TYPES_LOCAL=("A2Q")

    #local config_type="concurrent_a2q_ghost_q_enabled"    
    #run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" THREADS

    #local config_type="concurrent_a2q_ghost_q_enabled_and_relaxed"    
    #run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" THREADS

    #local CACHE_TYPES_LOCAL=("CLOCK")

    #local config_type="concurrent_clock_buffer_enabled"    
    #run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" THREADS

    #local config_type="concurrent_clock_buffer_enabled_and_relaxed"    
    #run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "" THREADS
}

# Function to run ThreadSanitizer analysis on concurrent deadlock issue
run_threadsanitizer_analysis() {
    echo "=========================================="
    echo "ThreadSanitizer Analysis for Concurrent Deadlock"
    echo "=========================================="
    echo "This will build with ThreadSanitizer and run a minimal test"
    echo "to isolate the exact race conditions and deadlock patterns."
    echo ""
    
    # Build with ThreadSanitizer
    build_cache_configuration "concurrent_tsan"
    
    # Set ThreadSanitizer options for better output
    export TSAN_OPTIONS="halt_on_error=1:abort_on_error=1:print_stacktrace=1:second_deadlock_stack=1:detect_deadlocks=1:report_bugs=1"
    
    # Create ThreadSanitizer output directory
    local tsan_output_dir="$PROFILE_OUTPUT_DIR/threadsanitizer_analysis"
    mkdir -p "$tsan_output_dir"
    
    echo "Running ThreadSanitizer analysis..."
    echo "Output directory: $tsan_output_dir"
    echo "ThreadSanitizer options: $TSAN_OPTIONS"
    echo ""
    
    # Run a minimal concurrent test that should trigger the deadlock
    cd "$BENCHMARK_DIR"
    
    # Test 1: Basic concurrent insert with small dataset to trigger deadlock quickly
    echo "Test 1: Basic concurrent insert (small dataset, high contention)"
    local tsan_log1="$tsan_output_dir/tsan_basic_concurrent.log"
    timeout 60s ./benchmark \
        --config "bm_cache" \
        --cache-type "CLOCK" \
        --storage-type "VolatileStorage" \
        --cache-size "50" \
        --page-size "4096" \
        --memory-size "134217728" \
        --tree-type "BplusTreeSOA" \
        --key-type "uint64_t" \
        --value-type "uint64_t" \
        --operation "insert" \
        --degree "64" \
        --records "10000" \
        --runs "1" \
        --threads "4" \
        --output-dir "$tsan_output_dir" 2>&1 | tee "$tsan_log1"
    
    local exit_code1=$?
    echo "Test 1 exit code: $exit_code1"
    
    # Test 2: Higher thread count to increase contention
    echo ""
    echo "Test 2: High thread contention (more threads, smaller cache)"
    local tsan_log2="$tsan_output_dir/tsan_high_contention.log"
    timeout 60s ./benchmark \
        --config "bm_cache" \
        --cache-type "CLOCK" \
        --storage-type "VolatileStorage" \
        --cache-size "25" \
        --page-size "4096" \
        --memory-size "67108864" \
        --tree-type "BplusTreeSOA" \
        --key-type "uint64_t" \
        --value-type "uint64_t" \
        --operation "insert" \
        --degree "32" \
        --records "5000" \
        --runs "1" \
        --threads "8" \
        --output-dir "$tsan_output_dir" 2>&1 | tee "$tsan_log2"
    
    local exit_code2=$?
    echo "Test 2 exit code: $exit_code2"
    
    # Test 3: Mixed operations to trigger different code paths
    echo ""
    echo "Test 3: Mixed operations (if supported)"
    local tsan_log3="$tsan_output_dir/tsan_mixed_ops.log"
    timeout 60s ./benchmark \
        --config "bm_cache" \
        --cache-type "CLOCK" \
        --storage-type "VolatileStorage" \
        --cache-size "100" \
        --page-size "4096" \
        --memory-size "134217728" \
        --tree-type "BplusTreeSOA" \
        --key-type "uint64_t" \
        --value-type "uint64_t" \
        --operation "insert" \
        --degree "128" \
        --records "20000" \
        --runs "1" \
        --threads "6" \
        --output-dir "$tsan_output_dir" 2>&1 | tee "$tsan_log3"
    
    local exit_code3=$?
    echo "Test 3 exit code: $exit_code3"
    
    echo ""
    echo "=========================================="
    echo "ThreadSanitizer Analysis Complete"
    echo "=========================================="
    echo "Results saved in: $tsan_output_dir"
    echo "Log files:"
    echo "  - Basic concurrent: $tsan_log1 (exit: $exit_code1)"
    echo "  - High contention: $tsan_log2 (exit: $exit_code2)"
    echo "  - Mixed operations: $tsan_log3 (exit: $exit_code3)"
    echo ""
    echo "Analysis Summary:"
    if [ $exit_code1 -ne 0 ] || [ $exit_code2 -ne 0 ] || [ $exit_code3 -ne 0 ]; then
        echo "✓ ThreadSanitizer detected issues (as expected)"
        echo "  Check the log files above for detailed race condition reports"
        echo "  Look for 'WARNING: ThreadSanitizer:' messages"
        echo "  Pay attention to 'data race' and 'lock-order-inversion' reports"
    else
        echo "⚠ No issues detected - may need longer runs or different parameters"
        echo "  Try running with more threads or smaller cache sizes"
    fi
    echo ""
    echo "Next steps:"
    echo "1. Examine the log files for specific race conditions"
    echo "2. Look for lock ordering issues between cache and object mutexes"
    echo "3. Check for data races in atomic operations"
    echo "4. Identify the exact code locations causing problems"
    echo "=========================================="
    
    # Clean up environment
    unset TSAN_OPTIONS
}

# Function to run quick cache benchmark for all configurations
run_quick_cache_benchmark() {
    local config_type=${1:-"basic"}  # "basic" or "concurrent"
    
    echo "=========================================="
    echo "Quick Cache Benchmark Run - $config_type Configuration"
    echo "=========================================="
    echo "Cache Types: ${CACHE_TYPES[*]}"
    echo "Storage Types: VolatileStorage, FileStorage"  # Limit for quick test
    echo "Cache Sizes: 100, 500"  # Limit for quick test
    echo "Trees: BplusTreeSOA"  # Limit for quick test
    echo "Degrees: 64, 128"  # Limit for quick test
    echo "Operations: ${OPERATIONS[*]}"
    echo "Record Counts: 100000, 500000"
    echo "Key/Value Types: uint64_t/uint64_t"
    echo "Thread Counts: 1, 2, 4"  # Limit for quick test
    echo "Runs per config: 1"
    
    # Build the appropriate configuration
    build_cache_configuration "$config_type"
    
    local quick_cache_percentages=("10%" "20%")  # Subset of percentages for quick testing
    local quick_storage_types=("VolatileStorage" "FileStorage")
    local quick_trees=("BplusTreeSOA")
    local quick_degrees=(64 128)
    local record_counts=(100000 500000)
    local quick_threads=(1 2 4)  # Subset of threads for quick testing
    local total_combinations=$((${#CACHE_TYPES[@]} * ${#quick_storage_types[@]} * ${#quick_cache_percentages[@]} * ${#quick_trees[@]} * ${#quick_degrees[@]} * ${#OPERATIONS[@]} * ${#record_counts[@]} * ${#quick_threads[@]}))
    echo "Total combinations: $total_combinations"
    echo "=========================================="
    
    # Create consolidated CSV file
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local consolidated_csv="$PROFILE_OUTPUT_DIR/quick_cache_benchmark_${config_type}_${timestamp}.csv"
    echo "tree_type,cache_type,storage_type,cache_size,key_type,value_type,workload_type,record_count,degree,operation,time_us,throughput_ops_sec,test_run_id,timestamp" > "$consolidated_csv"
    
    local current_combination=0
    local start_time=$(date +%s)
    
    for records in "${record_counts[@]}"; do
        echo ""
        echo "=== Processing Record Count: $records ==="
        
        for tree in "${quick_trees[@]}"; do
            for cache_type in "${CACHE_TYPES[@]}"; do
                for storage_type in "${quick_storage_types[@]}"; do
                    for cache_size in "${quick_cache_sizes[@]}"; do
                        for degree in "${quick_degrees[@]}"; do
                            for operation in "${OPERATIONS[@]}"; do
                                for thread_count in "${quick_threads[@]}"; do
                                    ((current_combination++))
                                    
                                    echo "[$current_combination/$total_combinations] Testing: $tree - $cache_type/$storage_type (Cache:$cache_size) - $operation - Degree $degree - Records $records - Threads $thread_count"
                                    
                                    cd "$BENCHMARK_DIR"
                                    
                                    # Run benchmark without perf profiling for speed
                                    ./benchmark \
                                        --config "bm_cache" \
                                        --cache-type "$cache_type" \
                                        --storage-type "$storage_type" \
                                        --cache-size "$cache_size" \
                                        --tree-type "$tree" \
                                        --key-type "uint64_t" \
                                        --value-type "uint64_t" \
                                        --operation "$operation" \
                                        --degree "$degree" \
                                        --records "$records" \
                                        --runs 1 \
                                        --threads "$thread_count" > /dev/null 2>&1
                                
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
    echo "Quick Cache Benchmark Run Completed!"
    echo "=========================================="
    echo "Total combinations tested: $total_combinations"
    echo "Total execution time: ${minutes}m ${seconds}s"
    echo "Results saved to: $consolidated_csv"
    
    # Generate summary statistics if Python is available
    echo ""
    echo "Generating cache benchmark summary statistics..."
    python3 -c "
import pandas as pd
import sys

try:
    df = pd.read_csv('$consolidated_csv')
    
    print('\\n=== CACHE BENCHMARK SUMMARY STATISTICS ===')
    print(f'Total test runs: {len(df)}')
    print(f'Cache types tested: {df[\"tree_type\"].nunique() if \"tree_type\" in df.columns else \"N/A\"}')
    print(f'Operations tested: {df[\"operation\"].nunique() if \"operation\" in df.columns else \"N/A\"}')
    print(f'Degrees tested: {df[\"degree\"].nunique() if \"degree\" in df.columns else \"N/A\"}')
    print(f'Record counts tested: {df[\"record_count\"].nunique() if \"record_count\" in df.columns else \"N/A\"}')
    
    # Check if we have cache-specific columns
    if 'cache_type' in df.columns:
        print('\\n=== CACHE TYPE COMPARISON (Average Throughput) ===')
        cache_comparison = df.groupby('cache_type')['throughput_ops_sec'].mean().sort_values(ascending=False)
        for cache, throughput in cache_comparison.items():
            print(f'{cache:15}: {throughput:>12,.0f} ops/sec')
    
    if 'storage_type' in df.columns:
        print('\\n=== STORAGE TYPE COMPARISON (Average Throughput) ===')
        storage_comparison = df.groupby('storage_type')['throughput_ops_sec'].mean().sort_values(ascending=False)
        for storage, throughput in storage_comparison.items():
            print(f'{storage:20}: {throughput:>12,.0f} ops/sec')
    
    if 'cache_size' in df.columns:
        print('\\n=== CACHE SIZE COMPARISON (Average Throughput) ===')
        cache_size_comparison = df.groupby('cache_size')['throughput_ops_sec'].mean().sort_values(ascending=False)
        for size, throughput in cache_size_comparison.items():
            print(f'Cache Size {size:4}: {throughput:>12,.0f} ops/sec')
    
    print('\\n=== TOP 10 HIGHEST THROUGHPUT CACHE CONFIGURATIONS ===')
    top_cols = ['tree_type', 'operation', 'degree', 'record_count', 'throughput_ops_sec']
    if 'cache_type' in df.columns:
        top_cols.insert(1, 'cache_type')
    if 'storage_type' in df.columns:
        top_cols.insert(2, 'storage_type')
    if 'cache_size' in df.columns:
        top_cols.insert(3, 'cache_size')
    
    available_cols = [col for col in top_cols if col in df.columns]
    top_throughput = df.nlargest(10, 'throughput_ops_sec')[available_cols]
    print(top_throughput.to_string(index=False))
    
    print('\\n=== OPERATION COMPARISON (Average Throughput) ===')
    op_comparison = df.groupby('operation')['throughput_ops_sec'].mean().sort_values(ascending=False)
    for op, throughput in op_comparison.items():
        print(f'{op:20}: {throughput:>12,.0f} ops/sec')
        
except Exception as e:
    print(f'Error generating summary: {e}')
    print('Raw CSV file is available at: $consolidated_csv')
" 2>/dev/null || echo "Python pandas not available. Raw results in: $consolidated_csv"
    
    echo ""
    echo "Quick cache benchmark run completed successfully!"
    echo "Check the consolidated results at: $consolidated_csv"
}

# Function to run cache profiling for specific configuration
run_single_cache_profiling() {
    local tree_type=$1
    local cache_type=${2:-"LRU"}
    local storage_type=${3:-"VolatileStorage"}
    local cache_size=${4:-100}
    local key_type=${5:-"uint64_t"}
    local value_type=${6:-"uint64_t"}
    local operation=$7
    local degree=${8:-64}
    local records=${9:-100000}
    local runs=${10:-$RUNS}
    local config_type=${11:-"basic"}
    
    if [ -z "$tree_type" ] || [ -z "$operation" ]; then
        echo "Usage: $0 single <tree_type> [cache_type] [storage_type] [cache_size] [key_type] [value_type] <operation> [degree] [records] [runs] [config_type]"
        echo "Example: $0 single BplusTreeSOA LRU VolatileStorage 100 uint64_t uint64_t insert 64 100000 3 basic"
        exit 1
    fi
    
    echo "Running single cache profiling configuration..."
    echo "Tree: $tree_type, Cache: $cache_type/$storage_type (Size: $cache_size)"
    echo "Key/Value: $key_type/$value_type, Operation: $operation"
    echo "Degree: $degree, Records: $records, Runs: $runs"
    echo "Config Type: $config_type"
    
    # Build the appropriate configuration
    build_cache_configuration "$config_type"
    
    # Temporarily override RUNS for this single run
    local original_runs=$RUNS
    RUNS=$runs
    
    run_cache_profiled_benchmark "$tree_type" "$cache_type" "$storage_type" "$cache_size" "2048" "1073741824" "$key_type" "$value_type" "$operation" "$degree" "$records" "$config_type"
    
    # Restore original RUNS
    RUNS=$original_runs
}

# Function to analyze cache perf results
analyze_cache_results() {
    echo "Analyzing cache perf results..."
    
    # Create summary CSV
    local summary_file="$PROFILE_OUTPUT_DIR/cache_perf_summary.csv"
    echo "tree_type,cache_type,storage_type,cache_size,key_type,value_type,operation,degree,records,cache_misses,cache_references,cache_miss_rate,cycles,instructions,ipc,branch_misses,page_faults" > "$summary_file"
    
    for perf_file in "$PROFILE_OUTPUT_DIR"/*.prf; do
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
            
            # Extract configuration details from filename (this is simplified - you may need to adjust based on actual filename format)
            echo "$basename,$cache_misses,$cache_references,$cache_miss_rate,$cycles,$instructions,$ipc,$branch_misses,$page_faults" >> "$summary_file"
        fi
    done
    
    echo "Cache performance analysis completed. Summary saved to: $summary_file"
}

# Example wrapper functions showing how to use run_full_cache_profiling_with_params

# Example 1: Small cache sizes with limited operations
run_full_cache_profiling_small() {
    local config_type=${1:-"basic"}
    
    # Define custom configuration arrays
    local small_cache_types=("LRU" "A2Q")
    local small_storage_types=("VolatileStorage")
    local small_cache_sizes=(100 250)
    local small_page_sizes=(4096)
    local small_memory_sizes=(1073741824)  # 1GB
    local small_trees=("BplusTreeSOA")
    local small_degrees=(64 128)
    local small_operations=("insert" "search_random")
    declare -A small_key_value_combos
    small_key_value_combos["uint64_t_uint64_t"]="uint64_t uint64_t"
    local small_records=(100000 500000)
    
    # Call the parameterized function
    run_full_cache_profiling_with_params "$config_type" \
        small_cache_types small_storage_types small_cache_sizes \
        small_page_sizes small_memory_sizes small_trees \
        small_degrees small_operations small_key_value_combos \
        small_records "small"
}

# Example 2: Large cache sizes with comprehensive operations
run_full_cache_profiling_large() {
    local config_type=${1:-"basic"}
    
    # Define custom configuration arrays
    local large_cache_types=("LRU" "A2Q" "CLOCK")
    local large_storage_types=("VolatileStorage" "FileStorage")
    local large_cache_sizes=(1000 2000 5000)
    local large_page_sizes=(4096 8192)
    local large_memory_sizes=(2147483648 4294967296)  # 2GB, 4GB
    local large_trees=("BplusTreeSOA")
    local large_degrees=(128 256 512)
    local large_operations=("insert" "search_random" "search_sequential" "search_uniform" "delete")
    declare -A large_key_value_combos
    large_key_value_combos["uint64_t_uint64_t"]="uint64_t uint64_t"
    large_key_value_combos["uint64_t_char16"]="uint64_t char16"
    local large_records=(1000000 2000000)
    
    # Call the parameterized function
    run_full_cache_profiling_with_params "$config_type" \
        large_cache_types large_storage_types large_cache_sizes \
        large_page_sizes large_memory_sizes large_trees \
        large_degrees large_operations large_key_value_combos \
        large_records "large"
}

# Example 3: Performance comparison focused configuration
run_full_cache_profiling_performance() {
    local config_type=${1:-"basic"}
    
    # Define custom configuration arrays for performance comparison
    local perf_cache_types=("LRU" "A2Q" "CLOCK")
    local perf_storage_types=("VolatileStorage")
    local perf_cache_sizes=(500 1000)
    local perf_page_sizes=(4096)
    local perf_memory_sizes=(2147483648)  # 2GB
    local perf_trees=("BplusTreeSOA")
    local perf_degrees=(64 128 256)
    local perf_operations=("insert" "search_random" "search_zipfian")
    declare -A perf_key_value_combos
    perf_key_value_combos["uint64_t_uint64_t"]="uint64_t uint64_t"
    local perf_records=(500000 1000000)
    
    # Call the parameterized function
    run_full_cache_profiling_with_params "$config_type" \
        perf_cache_types perf_storage_types perf_cache_sizes \
        perf_page_sizes perf_memory_sizes perf_trees \
        perf_degrees perf_operations perf_key_value_combos \
        perf_records "performance"
}

# Function to parse perf data from .prf files
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

# Function to combine all CSV files with perf data
combine_csv_files() {
    local target_dir=${1:-$PROFILE_OUTPUT_DIR}
    
    echo "=========================================="
    echo "Combining all CSV files from benchmark runs with perf data"
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
            else
                echo "  PRF: Not found (will use default values)"
            fi
            
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
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  full [basic|concurrent]     Run comprehensive cache profiling (default: basic)"
    echo "  quick [basic|concurrent]    Run quick cache benchmark (default: basic)"
    echo "  small [basic|concurrent]    Run small cache configuration profiling"
    echo "  large [basic|concurrent]    Run large cache configuration profiling"
    echo "  performance [basic|concurrent] Run performance-focused cache profiling"
    echo "  single <args>               Run single cache configuration profiling"
    echo "  tsan|threadsanitizer        Run ThreadSanitizer analysis for concurrent deadlock debugging"
    echo "  analyze                     Analyze existing perf results"
    echo "  combine [results_dir]       Combine CSV files with perf data into single file"
    echo "  help                        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 full basic               # Full profiling with basic cache configuration"
    echo "  $0 full concurrent          # Full profiling with concurrent cache configuration"
    echo "  $0 quick basic              # Quick benchmark with basic cache configuration"
    echo "  $0 tsan                     # Run ThreadSanitizer analysis to isolate deadlock issues"
    echo "  $0 single BplusTreeSOA LRU VolatileStorage 100 uint64_t uint64_t insert 64 100000 3 basic"
    echo "  $0 analyze                  # Analyze existing results"
    echo "  $0 combine                  # Combine CSV files from most recent results"
    echo "  $0 combine /path/to/results # Combine CSV files from specific results directory"
    echo ""
    echo "Environment Variables:"
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
        analyze_cache_results
        echo ""
        echo "=========================================="
        echo "Combining CSV files with perf data..."
        echo "=========================================="
        combine_csv_files "$PROFILE_OUTPUT_DIR"
        ;;
    "quick")
        run_quick_cache_benchmark "${2:-basic}"
        ;;
    "small")
        run_full_cache_profiling_small "${2:-basic}"
        analyze_cache_results
        echo ""
        echo "=========================================="
        echo "Combining CSV files with perf data..."
        echo "=========================================="
        combine_csv_files "$PROFILE_OUTPUT_DIR"
        ;;
    "large")
        run_full_cache_profiling_large "${2:-basic}"
        analyze_cache_results
        echo ""
        echo "=========================================="
        echo "Combining CSV files with perf data..."
        echo "=========================================="
        combine_csv_files "$PROFILE_OUTPUT_DIR"
        ;;
    "performance")
        run_full_cache_profiling_performance "${2:-basic}"
        analyze_cache_results
        echo ""
        echo "=========================================="
        echo "Combining CSV files with perf data..."
        echo "=========================================="
        combine_csv_files "$PROFILE_OUTPUT_DIR"
        ;;
    "single")
        shift
        run_single_cache_profiling "$@"
        ;;
    "tsan"|"threadsanitizer")
        run_threadsanitizer_analysis
        ;;
    "analyze")
        analyze_cache_results
        ;;
    "combine")
        combine_csv_files "$2"
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
echo "Cache benchmark script completed!"
echo "Results directory: $PROFILE_OUTPUT_DIR"