#!/bin/bash

# DEBUG VERSION: Cache Profiling Benchmark Script with Debug Symbols
# This script runs comprehensive cache benchmarks with different cache types, storage types, and parameters
# Based on profile_and_benchmark_tree_with_cache.sh but adapted for debugging infinite loop issues
#
# Features:
# - Configurable data path: Benchmark data is stored in /home/skarim/Code/haldendb_ex/haldendb_pvt/benchmark/benchmark_data
# - Automatic data generation: Checks if data exists and generates it if missing
# - Data reuse: Existing data is reused across benchmark runs for consistency
# - DEBUG: Added debug symbols and verbose logging to track infinite loops
#
# Environment Variables:
# - THREADS: Array of thread counts for concurrent operations (default: defined in script)

# Configuration
BENCHMARK_DIR="/home/skarim/Code/haldendb_ex/haldendb_pvt/benchmark/build"
BENCHMARK_EXEC="$BENCHMARK_DIR/benchmark"

# Create timestamped output directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
PROFILE_OUTPUT_DIR="$BENCHMARK_DIR/cache_profiling_results_debug_${TIMESTAMP}"

# Create output directory
mkdir -p "$PROFILE_OUTPUT_DIR"

echo "=========================================="
echo "DEBUG: Cache Profiling Results Directory: $PROFILE_OUTPUT_DIR"
echo "=========================================="

# Cache-specific configuration arrays
CACHE_TYPES=("LRU" "A2Q" "CLOCK")
STORAGE_TYPES=("VolatileStorage")
#CACHE_SIZE_PERCENTAGES=("2%" "5%" "10%" "20%" "30%" "40%" "50%")  # Cache sizes as percentages of memory
CACHE_SIZE_PERCENTAGES=("2%")  # Cache sizes as percentages of memory
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
RECORDS=(100000)
RUNS=${RUNS:-1}  # Default to 3, but allow override via environment variable
#THREADS=(2 4 8 12 16 20 24 28 32 36 40)
THREADS=(4)

# Data directory configuration
DATA_BASE_DIR="/home/skarim/benchmark_data"
DATA_DIR="$DATA_BASE_DIR/data"
YCSB_DIR="$DATA_BASE_DIR/ycsb"

# Perf events to collect (cache-focused)
PERF_EVENTS="cache-misses,cache-references,cycles,instructions,branch-misses,page-faults,L1-dcache-load-misses,L1-dcache-loads,LLC-load-misses,LLC-loads,L1-icache-load-misses,L1-icache-loads"

# DEBUG: Add timeout for benchmark execution to prevent infinite loops
BENCHMARK_TIMEOUT=300  # 5 minutes timeout

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
    echo "DEBUG: Checking benchmark data availability..."
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
    
    echo "DEBUG: Found $data_files_count basic workload files"
    echo "DEBUG: Found $ycsb_files_count YCSB workload files"
    
    # If no data files exist, generate them
    if [ $data_files_count -eq 0 ] || [ $ycsb_files_count -eq 0 ]; then
        echo ""
        echo "DEBUG: Generating missing benchmark data..."
        echo "This may take a few minutes..."
        
        # Change to the data directory - no symlinks needed since directories are already created
        cd "$DATA_BASE_DIR"
        
        # The data and ycsb directories are already created above, no symlinks needed
        
        # Run benchmark executable to generate data (using a minimal configuration)
        echo "DEBUG: Generating basic workload data..."
        "$BENCHMARK_EXEC" --config "bm_nocache" --tree-type "BplusTreeSOA" --key-type "uint64_t" --value-type "uint64_t" --operation "insert" --degree "64" --records "100000" --runs "1" > /dev/null 2>&1
        
        # Check if data was generated successfully
        local new_data_files_count=$(find "$DATA_DIR" -name "*.dat" 2>/dev/null | wc -l)
        local new_ycsb_files_count=$(find "$YCSB_DIR" -name "*.dat" 2>/dev/null | wc -l)
        
        echo "DEBUG: Data generation completed!"
        echo "DEBUG: Generated $new_data_files_count basic workload files"
        echo "DEBUG: Generated $new_ycsb_files_count YCSB workload files"
        
        # Return to benchmark directory
        cd "$BENCHMARK_DIR" > /dev/null
    else
        echo "DEBUG: Benchmark data already exists, skipping generation."
    fi
    
    echo "=========================================="
}

# Function to build with cache support and debug symbols
build_cache_configuration() {
    local config_type=$1  # "basic" or "concurrent"
    
    echo "========================================="
    echo "DEBUG: Building Cache Configuration: $config_type"
    echo "========================================="
    
    cd "$BENCHMARK_DIR"
    
    # Clean previous build
    make clean > /dev/null 2>&1
    
    # Define optimization flags for Debug builds with symbols
    local DEBUG_OPTS="-O0 -g3 -DDEBUG -fno-omit-frame-pointer -fno-optimize-sibling-calls"

    # Configure and build based on type
    if [ "$config_type" = "non_concurrent_default" ]; then
        echo "DEBUG: Building with cache + non_concurrent_default + debug symbols..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CACHE_COUNTERS__ $DEBUG_OPTS"
    elif [ "$config_type" = "non_concurrent_relaxed" ]; then
        echo "DEBUG: Building with cache + non_concurrent_relaxed + debug symbols..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__SELECTIVE_UPDATE__ -D__CACHE_COUNTERS__ $DEBUG_OPTS"
    # LRU Specific
    elif [ "$config_type" = "non_concurrent_lru_metadata_update_in_order" ]; then
        echo "DEBUG: Building with cache + non_concurrent_lru_metadata_update_in_order + debug symbols..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__UPDATE_IN_ORDER__ -D__CACHE_COUNTERS__ $DEBUG_OPTS"
    elif [ "$config_type" = "non_concurrent_lru_metadata_update_in_order_and_relaxed" ]; then
        echo "DEBUG: Building with cache + non_concurrent_lru_metadata_update_in_order_and_relaxed + debug symbols..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__UPDATE_IN_ORDER__ -D__SELECTIVE_UPDATE__ -D__CACHE_COUNTERS__ $DEBUG_OPTS"
    # A2Q Specific
    elif [ "$config_type" = "non_concurrent_a2q_ghost_q_enabled" ]; then
        echo "DEBUG: Building with cache + non_concurrent_a2q_ghost_q_enabled + debug symbols..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__MANAGE_GHOST_Q__ -D__CACHE_COUNTERS__ $DEBUG_OPTS"
    elif [ "$config_type" = "non_concurrent_a2q_ghost_q_enabled_and_relaxed" ]; then
        echo "DEBUG: Building with cache + non_concurrent_a2q_ghost_q_enabled_and_relaxed + debug symbols..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__MANAGE_GHOST_Q__ -D__SELECTIVE_UPDATE__ -D__CACHE_COUNTERS__ $DEBUG_OPTS"
    # Concurrent ones
    elif [ "$config_type" = "concurrent_default" ]; then
        echo "DEBUG: Building with cache + concurrent_default + debug symbols..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__CACHE_COUNTERS__ $DEBUG_OPTS"
    elif [ "$config_type" = "concurrent_relaxed" ]; then
        echo "DEBUG: Building with cache + concurrent_relaxed + debug symbols..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__SELECTIVE_UPDATE__ -D__CACHE_COUNTERS__ $DEBUG_OPTS"
    # LRU Specific
    elif [ "$config_type" = "concurrent_lru_metadata_update_in_order" ]; then
        echo "DEBUG: Building with cache + concurrent_lru_metadata_update_in_order + debug symbols..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__UPDATE_IN_ORDER__ -D__CACHE_COUNTERS__ $DEBUG_OPTS"
    elif [ "$config_type" = "concurrent_lru_metadata_update_in_order_and_relaxed" ]; then
        echo "DEBUG: Building with cache + concurrent_lru_metadata_update_in_order_and_relaxed + debug symbols..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__UPDATE_IN_ORDER__ -D__SELECTIVE_UPDATE__ -D__CACHE_COUNTERS__ $DEBUG_OPTS"
    # A2Q Specific
    elif [ "$config_type" = "concurrent_a2q_ghost_q_enabled" ]; then
        echo "DEBUG: Building with cache + concurrent_a2q_ghost_q_enabled + debug symbols..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__MANAGE_GHOST_Q__ -D__CACHE_COUNTERS__ $DEBUG_OPTS"
    elif [ "$config_type" = "concurrent_a2q_ghost_q_enabled_and_relaxed" ]; then
        echo "DEBUG: Building with cache + concurrent_a2q_ghost_q_enabled_and_relaxed + debug symbols..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__MANAGE_GHOST_Q__ -D__SELECTIVE_UPDATE__ -D__CACHE_COUNTERS__ $DEBUG_OPTS"
    # CLOCK Specific __CLOCK_WITH_BUFFER__ is with concurrent
    elif [ "$config_type" = "concurrent_clock_buffer_enabled" ]; then
        echo "DEBUG: Building with cache + concurrent_clock_buffer_enabled + debug symbols..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__CLOCK_WITH_BUFFER__ -D__CACHE_COUNTERS__ $DEBUG_OPTS"
    elif [ "$config_type" = "concurrent_clock_buffer_enabled_and_relaxed" ]; then
        echo "DEBUG: Building with cache + concurrent_clock_buffer_enabled_and_relaxed + debug symbols..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__CLOCK_WITH_BUFFER__ -D__SELECTIVE_UPDATE__ -D__CACHE_COUNTERS__ $DEBUG_OPTS"
    # ThreadSanitizer build
    elif [ "$config_type" = "concurrent_tsan" ]; then
        echo "DEBUG: Building with ThreadSanitizer for deadlock detection..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__SELECTIVE_UPDATE__ -D__CACHE_COUNTERS__ -fsanitize=thread -g3 -O1"
    # default
    else
        echo "DEBUG: Building with cache + non_concurrent + debug symbols..."
        cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CACHE_COUNTERS__ $DEBUG_OPTS"
    fi
    
    make -j$(nproc)
    
    if [ $? -eq 0 ]; then
        echo "DEBUG: Build completed successfully for $config_type cache configuration"
    else
        echo "DEBUG: Build failed for $config_type cache configuration"
        exit 1
    fi
    
    cd - > /dev/null
    echo ""
}

# Function to run single cache benchmark with perf profiling and timeout
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
    local debug_log="$run_folder/${config_name}_${profile_name}_debug.log"
    
    echo "=========================================="
    echo "DEBUG: Cache Profiling: $tree_type - $operation - Degree $degree"
    echo "DEBUG: Cache: $cache_type (Size: $cache_size), Storage: $storage_type"
    echo "DEBUG: Page Size: $page_size, Memory Size: $memory_size"
    echo "DEBUG: Key: $key_type, Value: $value_type, Records: $records"
    echo "DEBUG: Threads: $thread_count"
    echo "DEBUG: Run Folder: $run_folder"
    echo "DEBUG: Output: $perf_output"
    echo "DEBUG: Debug Log: $debug_log"
    echo "DEBUG: Timeout: ${BENCHMARK_TIMEOUT}s"
    echo "=========================================="
    
    # Ensure benchmark data exists and is accessible
    ensure_benchmark_data
    
    # Set up data path for benchmark execution
    cd "$DATA_BASE_DIR"
    
    # Data directories are already created, no symlinks needed
    
    # Create a wrapper script to run the benchmark with timeout and logging
    local wrapper_script="$run_folder/run_benchmark.sh"
    cat > "$wrapper_script" << EOF
#!/bin/bash
set -x
echo "DEBUG: Starting benchmark at \$(date)" >> "$debug_log"
echo "DEBUG: Command line arguments:" >> "$debug_log"
echo "  --config bm_cache" >> "$debug_log"
echo "  --cache-type $cache_type" >> "$debug_log"
echo "  --storage-type $storage_type" >> "$debug_log"
echo "  --cache-size $cache_size" >> "$debug_log"
echo "  --page-size $page_size" >> "$debug_log"
echo "  --memory-size $memory_size" >> "$debug_log"
echo "  --tree-type $tree_type" >> "$debug_log"
echo "  --key-type $key_type" >> "$debug_log"
echo "  --value-type $value_type" >> "$debug_log"
echo "  --operation $operation" >> "$debug_log"
echo "  --degree $degree" >> "$debug_log"
echo "  --records $records" >> "$debug_log"
echo "  --runs $RUNS" >> "$debug_log"
echo "  --threads $thread_count" >> "$debug_log"
echo "  --output-dir $run_folder" >> "$debug_log"
echo "  --config-name $config_name" >> "$debug_log"
echo "  --cache-size-percentage $cache_size_percentage" >> "$debug_log"
echo "  --cache-page-limit $cache_size" >> "$debug_log"

echo "DEBUG: Process ID: \$\$" >> "$debug_log"
echo "DEBUG: Working directory: \$(pwd)" >> "$debug_log"

exec "$BENCHMARK_EXEC" \\
    --config "bm_cache" \\
    --cache-type "$cache_type" \\
    --storage-type "$storage_type" \\
    --cache-size "$cache_size" \\
    --page-size "$page_size" \\
    --memory-size "$memory_size" \\
    --tree-type "$tree_type" \\
    --key-type "$key_type" \\
    --value-type "$value_type" \\
    --operation "$operation" \\
    --degree "$degree" \\
    --records "$records" \\
    --runs "$RUNS" \\
    --threads "$thread_count" \\
    --output-dir "$run_folder" \\
    --config-name "$config_name" \\
    --cache-size-percentage "$cache_size_percentage" \\
    --cache-page-limit "$cache_size" 2>&1 | tee -a "$debug_log"
EOF
    chmod +x "$wrapper_script"
    
    # Run benchmark with perf stat and timeout
    echo "DEBUG: Starting benchmark execution with timeout of ${BENCHMARK_TIMEOUT}s..."
    echo "DEBUG: Benchmark start time: $(date)" >> "$debug_log"
    
    # Execute from data directory so benchmark can find the data folders
    cd "$DATA_BASE_DIR"
    
    # Use timeout to prevent infinite loops
    timeout ${BENCHMARK_TIMEOUT}s perf stat -e "$PERF_EVENTS" \
              -o "$perf_output" \
              numactl --cpunodebind=0 --membind=0 \
              "$wrapper_script"
    
    local exit_code=$?
    echo "DEBUG: Benchmark exit code: $exit_code" >> "$debug_log"
    echo "DEBUG: Benchmark end time: $(date)" >> "$debug_log"
    
    if [ $exit_code -eq 124 ]; then
        echo "DEBUG: TIMEOUT! Benchmark was killed after ${BENCHMARK_TIMEOUT}s - likely infinite loop detected!"
        echo "DEBUG: TIMEOUT! Benchmark was killed after ${BENCHMARK_TIMEOUT}s - likely infinite loop detected!" >> "$debug_log"
        
        # Try to get stack trace of any remaining processes
        echo "DEBUG: Checking for remaining benchmark processes..." >> "$debug_log"
        pgrep -f "$BENCHMARK_EXEC" >> "$debug_log" 2>&1
        
        # Kill any remaining benchmark processes
        pkill -f "$BENCHMARK_EXEC" 2>/dev/null
        
        echo "INFINITE LOOP DETECTED in configuration: $config_name"
        echo "  Tree: $tree_type, Cache: $cache_type, Operation: $operation"
        echo "  Degree: $degree, Records: $records, Threads: $thread_count"
        echo "  Check debug log: $debug_log"
        
        # Return to benchmark directory
        cd "$BENCHMARK_DIR" > /dev/null
        return 1
    elif [ $exit_code -ne 0 ]; then
        echo "DEBUG: Benchmark failed with exit code $exit_code"
        echo "DEBUG: Check debug log: $debug_log"
    else
        echo "DEBUG: Benchmark completed successfully"
        
        # Rename the CSV file to match the perf file naming convention (remove timestamp)
        local latest_csv=$(ls -t "$run_folder"/benchmark_single_*.csv 2>/dev/null | head -1)
        if [ -f "$latest_csv" ]; then
            local target_csv="$run_folder/${config_name}_${profile_name}.csv"
            mv "$latest_csv" "$target_csv"
            echo "DEBUG: CSV file renamed to: $(basename "$target_csv")"
        fi
    fi
    
    echo "DEBUG: Cache profiling completed for $profile_name"
    
    # Return to benchmark directory
    cd "$BENCHMARK_DIR" > /dev/null
    
    # Brief sleep to let system settle between benchmarks
    sleep 2
    echo ""
    
    return $exit_code
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
    
    echo "DEBUG: Starting comprehensive cache profiling for $config_type configuration${config_suffix:+ ($config_suffix)}..."
    echo "DEBUG: Cache Types: ${cache_types_ref[*]}"
    echo "DEBUG: Storage Types: ${storage_types_ref[*]}"
    echo "DEBUG: Cache Size Percentages: ${cache_size_percentages_ref[*]}"
    echo "DEBUG: Page Sizes: ${page_sizes_ref[*]}"
    echo "DEBUG: Memory Sizes: ${memory_sizes_ref[*]}"
    echo "DEBUG: Trees: ${trees_ref[*]}"
    echo "DEBUG: Degrees: ${degrees_ref[*]}"
    echo "DEBUG: Operations: ${operations_ref[*]}"
    echo "DEBUG: Records: ${records_ref[*]}"
    echo "DEBUG: Threads: ${threads_ref[*]}"
    echo ""
    
    # Build the appropriate configuration
    build_cache_configuration "$config_type"

    local total_combinations=0
    local current_combination=0
    local failed_combinations=0
    
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
    
    echo "DEBUG: Total combinations to profile for $config_type${config_suffix:+ ($config_suffix)}: $total_combinations"
    echo ""
    
    # Create config-specific identifier for output files
    local config_id="${config_type}${config_suffix}"
    
    # Run profiling for each combination
    for combo_name in "${!key_value_combos_ref[@]}"; do
        IFS=' ' read -r key_type value_type <<< "${key_value_combos_ref[$combo_name]}"
        
        echo "DEBUG: Processing key-value combination: $key_type -> $value_type"
        
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
                                        
                                        echo "DEBUG: Cache size calculation: $cache_size_percentage of estimated pages for $records records (degree $degree) = $actual_cache_size entries"
                                    
                                        for operation in "${operations_ref[@]}"; do
                                            for thread_count in "${threads_ref[@]}"; do
                                                ((current_combination++))
                                                echo "DEBUG: Progress: $current_combination/$total_combinations ($config_id)"
                                                
                                                if ! run_cache_profiled_benchmark "$tree" "$cache_type" "$storage_type" "$actual_cache_size" "$page_size" "$memory_size" "$key_type" "$value_type" "$operation" "$degree" "$records" "$config_id" "$thread_count" "$cache_size_percentage"; then
                                                    ((failed_combinations++))
                                                    echo "DEBUG: FAILED combination $current_combination: $tree $cache_type $operation degree=$degree threads=$thread_count"
                                                fi
                                                
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
    echo "DEBUG: Cache profiling completed for $config_id!"
    echo "DEBUG: Results saved in: $PROFILE_OUTPUT_DIR"
    echo "DEBUG: Total profiles attempted: $total_combinations"
    echo "DEBUG: Failed profiles (timeouts/infinite loops): $failed_combinations"
    echo "DEBUG: Successful profiles: $((total_combinations - failed_combinations))"
    echo "=========================================="
}

# DEBUG: Simplified multi-threaded function to focus on problematic configurations
run_full_cache_profiling_multi_threaded_debug() {
    echo "DEBUG: Running multi-threaded cache profiling with debug symbols..."
    echo "DEBUG: Using threads: ${THREADS[*]}"
    
    # Test only the configurations that are currently active in the original script
    
    # Test 1: concurrent_relaxed with CLOCK
    echo "DEBUG: Testing concurrent_relaxed with CLOCK cache type..."
    local CACHE_TYPES_LOCAL=("CLOCK")
    local config_type="concurrent_relaxed"    
    run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "_debug1" THREADS

    # Test 2: concurrent_clock_buffer_enabled_and_relaxed with CLOCK
    echo "DEBUG: Testing concurrent_clock_buffer_enabled_and_relaxed with CLOCK cache type..."
    local CACHE_TYPES_LOCAL=("CLOCK")
    local config_type="concurrent_clock_buffer_enabled_and_relaxed"    
    run_full_cache_profiling_with_params "$config_type" CACHE_TYPES_LOCAL STORAGE_TYPES CACHE_SIZE_PERCENTAGES PAGE_SIZES MEMORY_SIZES TREES DEGREES OPERATIONS KEY_VALUE_COMBOS RECORDS "_debug2" THREADS
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  debug                       Run debug version with timeout and logging"
    echo "  help                        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 debug                    # Run debug version to identify infinite loops"
    echo ""
    echo "Environment Variables:"
    echo "  RUNS=N                      Set number of runs per configuration (default: 1)"
    echo "  BENCHMARK_TIMEOUT=N         Set timeout in seconds for each benchmark (default: 300)"
    echo ""
    echo "Cache Types: ${CACHE_TYPES[*]}"
    echo "Storage Types: ${STORAGE_TYPES[*]}"
    echo "Cache Size Percentages: ${CACHE_SIZE_PERCENTAGES[*]}"
}

# Main script logic
case "${1:-debug}" in
    "debug")
        run_full_cache_profiling_multi_threaded_debug
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
echo "DEBUG: Cache benchmark debug script completed!"
echo "DEBUG: Results directory: $PROFILE_OUTPUT_DIR"
echo "DEBUG: Check individual debug logs for detailed information about any infinite loops"