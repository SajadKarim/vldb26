#!/bin/bash

# Cache and Storage Profiling Benchmark Script
# This script builds different configurations and runs comprehensive tests
# for cache and storage combinations

# Configuration
BENCHMARK_DIR="/home/skarim/Code/haldendb_ex/haldendb_pvt/benchmark/build"
BENCHMARK_EXEC="$BENCHMARK_DIR/benchmark"

# Create timestamped output directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
PROFILE_OUTPUT_DIR="$BENCHMARK_DIR/cache_storage_profiling_results_${TIMESTAMP}"

# Create output directory
mkdir -p "$PROFILE_OUTPUT_DIR"

echo "=========================================="
echo "Cache and Storage Profiling Results Directory: $PROFILE_OUTPUT_DIR"
echo "=========================================="

# Define all build configurations with their compatible components
declare -A BUILD_CONFIGS

BUILD_CONFIGS["BASIC_CACHE"]="
flags:-D__TREE_WITH_CACHE__
trees:BplusTreeSOA,BplusTreeAOS
caches:LRU,CLOCK,A2Q
storages:VolatileStorage,FileStorage,PMemStorage
cache_sizes:100,500,1000,2000
key_types:uint64_t,char16
value_types:uint64_t,char16
operations:insert,search_random,search_sequential,search_uniform,search_zipfian,delete
degrees:64,128,256
records:100000,500000,1000000
runs:3
"

BUILD_CONFIGS["CONCURRENT_CACHE"]="
flags:-D__TREE_WITH_CACHE__ -D__CONCURRENT__
trees:BplusTreeSOA,BplusTreeAOS
caches:LRU,CLOCK,A2Q
storages:VolatileStorage,FileStorage,PMemStorage
cache_sizes:100,500,1000,2000
key_types:uint64_t,char16
value_types:uint64_t,char16
operations:insert,search_random,search_sequential,search_uniform,search_zipfian,delete
degrees:64,128,256
records:100000,500000,1000000
runs:3
"

BUILD_CONFIGS["QUICK_CACHE_TEST"]="
flags:-D__TREE_WITH_CACHE__
trees:BplusTreeSOA
caches:LRU,CLOCK,A2Q
storages:VolatileStorage,FileStorage
cache_sizes:100,500
key_types:uint64_t
value_types:uint64_t
operations:insert,search_random,delete
degrees:64,128
records:100000,500000
runs:1
"
# Generic function to parse configuration values
get_config_value() {
    local config_string=$1
    local key=$2
    echo "$config_string" | grep "^${key}:" | cut -d: -f2 | tr -d ' '
}

# Function to build specific configuration
build_configuration() {
    local config_name=$1
    local config_string=${BUILD_CONFIGS[$config_name]}
    local build_flags=$(get_config_value "$config_string" "flags")
    
    echo "========================================="
    echo "Building Configuration: $config_name"
    echo "Build Flags: $build_flags"
    echo "========================================="
    
    cd "$BENCHMARK_DIR"
    
    # Clean previous build
    make clean > /dev/null 2>&1
    
    # Configure and build
    if [ -n "$build_flags" ]; then
        cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="$build_flags"
    else
        cmake .. -DCMAKE_BUILD_TYPE=Release
    fi
    
    make -j$(nproc)
    
    if [ $? -eq 0 ]; then
        echo "Build completed successfully for $config_name"
    else
        echo "Build failed for $config_name"
        exit 1
    fi
    
    cd - > /dev/null
    echo ""
}

# Single test execution function with ALL parameters
run_single_test() {
    local tree=$1
    local cache=$2
    local storage=$3
    local cache_size=$4
    local key_type=$5
    local value_type=$6
    local operation=$7
    local degree=$8
    local record_count=$9
    local runs=${10}
    local config=${11}
    
    echo "Testing: $tree | $cache | $storage | CacheSize:$cache_size | $key_type->$value_type | $operation | Degree:$degree | Records:$record_count | Runs:$runs | Config:$config"
    
    # Create output directory for this configuration
    local output_dir="$PROFILE_OUTPUT_DIR/${config}_results"
    mkdir -p "$output_dir"
    
    cd "$BENCHMARK_DIR"
    
    # Determine config parameter based on cache type
    local benchmark_config="bm_nocache"
    if [ "$cache" != "NoCache" ]; then
        benchmark_config="bm_cache"
    fi
    
    # Run benchmark
    if [ "$cache" != "NoCache" ]; then
        # For cache configurations, pass cache and storage parameters
        ./benchmark \
            --config "$benchmark_config" \
            --cache-type "$cache" \
            --storage-type "$storage" \
            --cache-size "$cache_size" \
            --tree-type "$tree" \
            --key-type "$key_type" \
            --value-type "$value_type" \
            --operation "$operation" \
            --degree "$degree" \
            --records "$record_count" \
            --runs "$runs" \
            --output-dir "$output_dir" > /dev/null 2>&1
    else
        # For no-cache configurations
        ./benchmark \
            --config "$benchmark_config" \
            --tree-type "$tree" \
            --key-type "$key_type" \
            --value-type "$value_type" \
            --operation "$operation" \
            --degree "$degree" \
            --records "$record_count" \
            --runs "$runs" \
            --output-dir "$output_dir" > /dev/null 2>&1
    fi
    
    if [ $? -eq 0 ]; then
        echo "✓ Test completed successfully"
        
        # Handle CSV file naming similar to nocache script
        local test_name="${tree}_${cache}_${storage}_${cache_size}_${key_type}_${value_type}_${operation}_${degree}_${record_count}"
        local latest_csv=$(ls -t "$output_dir"/benchmark_single_*.csv 2>/dev/null | head -1)
        if [ -f "$latest_csv" ]; then
            local target_csv="$output_dir/${test_name}.csv"
            mv "$latest_csv" "$target_csv"
            echo "  CSV file saved as: $(basename "$target_csv")"
        else
            echo "  WARNING: No CSV output found for this test"
        fi
    else
        echo "✗ Test failed"
    fi
    
    cd - > /dev/null
    echo ""
}

# Generic function to run tests for ANY configuration
run_tests_for_config() {
    local config_name=$1
    local config_string=${BUILD_CONFIGS[$config_name]}
    
    # Parse ALL configuration values
    local trees=$(get_config_value "$config_string" "trees")
    local caches=$(get_config_value "$config_string" "caches")
    local storages=$(get_config_value "$config_string" "storages")
    local cache_sizes=$(get_config_value "$config_string" "cache_sizes")
    local key_types=$(get_config_value "$config_string" "key_types")
    local value_types=$(get_config_value "$config_string" "value_types")
    local operations=$(get_config_value "$config_string" "operations")
    local degrees=$(get_config_value "$config_string" "degrees")
    local records=$(get_config_value "$config_string" "records")
    local runs=$(get_config_value "$config_string" "runs")
    
    echo "=========================================="
    echo "Running tests for configuration: $config_name"
    echo "=========================================="
    echo "Trees: $trees"
    echo "Caches: $caches"
    echo "Storages: $storages"
    echo "Cache Sizes: $cache_sizes"
    echo "Key Types: $key_types"
    echo "Value Types: $value_types"
    echo "Operations: $operations"
    echo "Degrees: $degrees"
    echo "Records: $records"
    echo "Runs: $runs"
    echo "=========================================="
    
    # Create consolidated CSV file for this configuration
    local consolidated_csv="$PROFILE_OUTPUT_DIR/${config_name}_consolidated_results.csv"
    echo "tree_type,cache_type,storage_type,cache_size,key_type,value_type,workload_type,record_count,degree,operation,time_us,throughput_ops_sec,test_run_id,timestamp" > "$consolidated_csv"
    
    # Convert to arrays
    IFS=',' read -ra TREE_ARRAY <<< "$trees"
    IFS=',' read -ra CACHE_ARRAY <<< "$caches"
    IFS=',' read -ra STORAGE_ARRAY <<< "$storages"
    IFS=',' read -ra CACHE_SIZE_ARRAY <<< "$cache_sizes"
    IFS=',' read -ra KEY_TYPE_ARRAY <<< "$key_types"
    IFS=',' read -ra VALUE_TYPE_ARRAY <<< "$value_types"
    IFS=',' read -ra OPERATION_ARRAY <<< "$operations"
    IFS=',' read -ra DEGREE_ARRAY <<< "$degrees"
    IFS=',' read -ra RECORD_ARRAY <<< "$records"
    
    # Calculate total combinations for progress tracking
    local total_combinations=$((${#TREE_ARRAY[@]} * ${#CACHE_ARRAY[@]} * ${#STORAGE_ARRAY[@]} * ${#CACHE_SIZE_ARRAY[@]} * ${#KEY_TYPE_ARRAY[@]} * ${#VALUE_TYPE_ARRAY[@]} * ${#OPERATION_ARRAY[@]} * ${#DEGREE_ARRAY[@]} * ${#RECORD_ARRAY[@]}))
    local current_combination=0
    
    echo "Total combinations for $config_name: $total_combinations"
    echo ""
    
    local start_time=$(date +%s)
    
    # Test all combinations
    for tree in "${TREE_ARRAY[@]}"; do
        for cache in "${CACHE_ARRAY[@]}"; do
            for storage in "${STORAGE_ARRAY[@]}"; do
                for cache_size in "${CACHE_SIZE_ARRAY[@]}"; do
                    for key_type in "${KEY_TYPE_ARRAY[@]}"; do
                        for value_type in "${VALUE_TYPE_ARRAY[@]}"; do
                            for operation in "${OPERATION_ARRAY[@]}"; do
                                for degree in "${DEGREE_ARRAY[@]}"; do
                                    for record_count in "${RECORD_ARRAY[@]}"; do
                                        ((current_combination++))
                                        echo "[$current_combination/$total_combinations] Progress for $config_name"
                                        
                                        run_single_test "$tree" "$cache" "$storage" "$cache_size" "$key_type" "$value_type" "$operation" "$degree" "$record_count" "$runs" "$config_name"
                                        
                                        # Append individual CSV to consolidated results
                                        local test_name="${tree}_${cache}_${storage}_${cache_size}_${key_type}_${value_type}_${operation}_${degree}_${record_count}"
                                        local output_dir="$PROFILE_OUTPUT_DIR/${config_name}_results"
                                        local individual_csv="$output_dir/${test_name}.csv"
                                        if [ -f "$individual_csv" ]; then
                                            # Skip header and append data to consolidated CSV
                                            tail -n +2 "$individual_csv" >> "$consolidated_csv"
                                        fi
                                        
                                        # Small delay between runs to let system settle
                                        sleep 1
                                    done
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
    
    echo "Completed all tests for $config_name in ${minutes}m ${seconds}s"
    echo "Individual CSV files saved in: $PROFILE_OUTPUT_DIR/${config_name}_results/"
    echo "Consolidated results saved in: $consolidated_csv"
    
    # Generate summary statistics if the consolidated CSV has data
    if [ -f "$consolidated_csv" ] && [ $(wc -l < "$consolidated_csv") -gt 1 ]; then
        local total_tests=$(($(wc -l < "$consolidated_csv") - 1))
        echo "Total test runs completed: $total_tests"
        echo "Configuration: $config_name summary saved to: $consolidated_csv"
    fi
    echo ""
}

# Function to run specific configuration only
run_single_config() {
    local config_name=$1
    
    if [ -z "$config_name" ]; then
        echo "Available configurations:"
        for config in "${!BUILD_CONFIGS[@]}"; do
            echo "  - $config"
        done
        echo ""
        echo "Usage: $0 single <config_name>"
        echo "Example: $0 single BASIC_CACHE"
        exit 1
    fi
    
    if [ -z "${BUILD_CONFIGS[$config_name]}" ]; then
        echo "Error: Configuration '$config_name' not found"
        echo "Available configurations:"
        for config in "${!BUILD_CONFIGS[@]}"; do
            echo "  - $config"
        done
        exit 1
    fi
    
    echo "Running single configuration: $config_name"
    
    # Build the configuration
    build_configuration "$config_name"
    
    # Run all compatible tests
    run_tests_for_config "$config_name"
    
    echo "========================================="
    echo "COMPLETED SINGLE CONFIGURATION: $config_name"
    echo "Results saved in: $PROFILE_OUTPUT_DIR"
    echo "========================================="
}

# Function to list all configurations
list_configurations() {
    echo "Available build configurations:"
    echo "==============================="
    
    for config_name in "${!BUILD_CONFIGS[@]}"; do
        local config_string=${BUILD_CONFIGS[$config_name]}
        local build_flags=$(get_config_value "$config_string" "flags")
        local trees=$(get_config_value "$config_string" "trees")
        local caches=$(get_config_value "$config_string" "caches")
        
        echo ""
        echo "Configuration: $config_name"
        echo "  Build Flags: ${build_flags:-'(none)'}"
        echo "  Trees: $trees"
        echo "  Caches: $caches"
        
        # Calculate approximate test count
        local tree_count=$(echo "$trees" | tr ',' '\n' | wc -l)
        local cache_count=$(echo "$caches" | tr ',' '\n' | wc -l)
        local approx_tests=$((tree_count * cache_count * 8))  # Rough estimate
        echo "  Approx Tests: ~$approx_tests combinations"
    done
    
    echo ""
    echo "Usage:"
    echo "  $0                    # Run all configurations"
    echo "  $0 single <config>    # Run specific configuration"
    echo "  $0 list              # Show this list"
}

# Main execution function
main() {
    local command=${1:-"all"}
    
    case $command in
        "single")
            run_single_config "$2"
            ;;
        "list")
            list_configurations
            ;;
        "all"|"")
            echo "Starting comprehensive cache and storage profiling..."
            echo "Total configurations: ${#BUILD_CONFIGS[@]}"
            echo ""
            
            local overall_start_time=$(date +%s)
            
            # Run tests for each build configuration
            for config_name in "${!BUILD_CONFIGS[@]}"; do
                echo ""
                echo "========================================="
                echo "PROCESSING BUILD CONFIGURATION: $config_name"
                echo "========================================="
                
                # Build the configuration
                build_configuration "$config_name"
                
                # Run all compatible tests
                run_tests_for_config "$config_name"
                
                echo "========================================="
                echo "COMPLETED BUILD CONFIGURATION: $config_name"
                echo "========================================="
                echo ""
            done
            
            local overall_end_time=$(date +%s)
            local overall_duration=$((overall_end_time - overall_start_time))
            local overall_hours=$((overall_duration / 3600))
            local overall_minutes=$(((overall_duration % 3600) / 60))
            local overall_seconds=$((overall_duration % 60))
            
            echo "=========================================="
            echo "ALL CONFIGURATIONS COMPLETED!"
            echo "=========================================="
            echo "Total configurations processed: ${#BUILD_CONFIGS[@]}"
            echo "Total execution time: ${overall_hours}h ${overall_minutes}m ${overall_seconds}s"
            echo "Results saved in: $PROFILE_OUTPUT_DIR"
            echo "=========================================="
            ;;
        *)
            echo "Unknown command: $command"
            echo ""
            list_configurations
            exit 1
            ;;
    esac
}

# Execute main function
main "$@"