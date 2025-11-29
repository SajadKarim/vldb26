#!/bin/bash

# Combine Benchmark Results Script
# This script combines CSV benchmark results with perf profiling data into a single comprehensive CSV file
# Adapted from the reference implementation in haldendb project

# Configuration
BENCHMARK_DIR="/home/skarim/Code/haldendb_ex/haldendb_pvt/benchmark/build"

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
    local target_dir=${1:-$BENCHMARK_DIR}
    
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
    echo "Usage: $0 [RESULTS_DIRECTORY]"
    echo ""
    echo "Combines CSV benchmark results with perf profiling data into a single comprehensive CSV file."
    echo ""
    echo "Arguments:"
    echo "  RESULTS_DIRECTORY    Path to the results directory containing subdirectories with .csv and .prf files"
    echo "                       If not provided, will look for the most recent cache_profiling_results_* directory"
    echo ""
    echo "Examples:"
    echo "  $0"
    echo "  $0 /path/to/cache_profiling_results_20250924_235948"
    echo ""
    echo "The script will:"
    echo "  1. Find all .csv files in subdirectories of the results directory"
    echo "  2. Find corresponding .prf files for each .csv file"
    echo "  3. Parse perf data from .prf files"
    echo "  4. Combine CSV data with perf metrics"
    echo "  5. Create a single combined CSV file with timestamp"
}

# Main execution
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_usage
    exit 0
fi

# Use provided directory or default
RESULTS_DIR=${1:-""}

# Run the combination
combine_csv_files "$RESULTS_DIR"
exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo ""
    echo "Success! Combined CSV file has been created."
else
    echo ""
    echo "Error: Failed to combine CSV files."
    exit $exit_code
fi