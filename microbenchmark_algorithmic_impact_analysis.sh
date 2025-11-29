#!/bin/bash

################################################################################
# Microbenchmark Algorithmic Impact Analysis Script
# 
# This script orchestrates the full benchmarking and plotting workflow:
# 1. Runs baseline (BPlusStore) cache profiling benchmarks
# 2. Runs optimized (BplusTreeSOA) cache profiling benchmarks
# 3. Merges results from both benchmark runs
# 4. Generates comparison plots (Figure 3: Throughput, Figure 4: CPI)
#
# Usage:
#   ./microbenchmark_algorithmic_impact_analysis.sh [OPTIONS]
#
# Options:
#   --data-path PATH          Base data directory path (default: /home/skarim/benchmark_data)
#                             NOTE: Benchmark scripts will create /data and /ycsb subdirectories
#   --baseline-only          Run only baseline benchmarks
#   --optimized-only         Run only optimized benchmarks
#   --plots-only             Skip benchmarks and generate plots from latest CSV files
#   --help                   Show this help message
#
# Environment Variables:
#   DATA_PATH                Override base data path (same as --data-path)
#
# Data Path Handling:
#   - Base directory (e.g., /home/skarim/benchmark_data) is passed to scripts
#   - Baseline script expects: /path/to/data (appends /data if not present)
#   - Optimized script expects: /path/to/base and creates /data and /ycsb subdirectories
#   - This master script handles both cases correctly
#
# Examples:
#   # Full pipeline with default data path (/home/skarim/benchmark_data)
#   ./microbenchmark_algorithmic_impact_analysis.sh
#
#   # Full pipeline with custom base data path
#   ./microbenchmark_algorithmic_impact_analysis.sh --data-path /custom/benchmark_data
#
#   # Or using environment variable
#   DATA_PATH=/custom/benchmark_data ./microbenchmark_algorithmic_impact_analysis.sh
#
#   # Run only baseline benchmarks
#   ./microbenchmark_algorithmic_impact_analysis.sh --baseline-only
#
#   # Generate plots from existing CSV files (skip benchmarks)
#   ./microbenchmark_algorithmic_impact_analysis.sh --plots-only
#
################################################################################

set -e

# Script directory (root of vldb26 repository)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASELINE_DIR="$SCRIPT_DIR/baseline/benchmark"
OPTIMIZED_DIR="$SCRIPT_DIR/optimized/benchmark"
PLOTS_DIR="$SCRIPT_DIR/plots"

# Default data path (can be overridden via --data-path or DATA_PATH env var)
DATA_PATH_DEFAULT="/tmp/benchmark_data"

# Parse command line arguments
DATA_PATH=""
RUN_BASELINE=true
RUN_OPTIMIZED=true
RUN_PLOTS=true
PLOTS_ONLY=false

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --data-path)
                DATA_PATH="$2"
                shift 2
                ;;
            --baseline-only)
                RUN_OPTIMIZED=false
                RUN_PLOTS=false
                shift
                ;;
            --optimized-only)
                RUN_BASELINE=false
                RUN_PLOTS=false
                shift
                ;;
            --plots-only)
                RUN_BASELINE=false
                RUN_OPTIMIZED=false
                PLOTS_ONLY=true
                shift
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            *)
                echo "[ERROR] Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

show_help() {
    cat << 'EOF'
Microbenchmark Algorithmic Impact Analysis Script

Usage: ./microbenchmark_algorithmic_impact_analysis.sh [OPTIONS]

OPTIONS:
  --data-path PATH          Base data directory path (default: /home/skarim/benchmark_data)
                            Scripts will create /data and /ycsb subdirectories
  --baseline-only          Run only baseline benchmarks
  --optimized-only         Run only optimized benchmarks
  --plots-only             Skip benchmarks and generate plots from latest CSV files
  --help                   Show this help message

ENVIRONMENT VARIABLES:
  DATA_PATH                Override base data path (same as --data-path)

DATA PATH HANDLING:
  The base directory is passed to benchmark scripts:
  - Baseline script: Uses DATA_PATH/data for storing data files
  - Optimized script: Creates DATA_PATH/data and DATA_PATH/ycsb subdirectories
  
  This master script automatically handles the path conversions for each script.

EXAMPLES:
  # Full pipeline with default base path
  ./microbenchmark_algorithmic_impact_analysis.sh

  # Full pipeline with custom base data path
  ./microbenchmark_algorithmic_impact_analysis.sh --data-path /custom/benchmark_data

  # Using environment variable
  DATA_PATH=/custom/benchmark_data ./microbenchmark_algorithmic_impact_analysis.sh

  # Run only baseline benchmarks
  ./microbenchmark_algorithmic_impact_analysis.sh --baseline-only

  # Generate plots from existing CSV files
  ./microbenchmark_algorithmic_impact_analysis.sh --plots-only

OUTPUTS:
  - Baseline CSV: baseline/benchmark/cache_profiling_results_*/combined_benchmark_results_with_perf_*.csv
  - Optimized CSV: optimized/benchmark/cache_profiling_results_*/combined_benchmark_results_with_perf_*.csv
  - Plots: plots/standalone_throughput_boxplot_enhanced_labels.{png,pdf}
           plots/standalone_cpi_boxplot_enhanced_labels.{png,pdf}
EOF
}

# Determine final data path
finalize_data_path() {
    if [ -n "$DATA_PATH" ]; then
        # Use --data-path argument if provided
        :
    elif [ -n "${DATA_PATH:-}" ]; then
        # Use environment variable if set
        DATA_PATH="${DATA_PATH}"
    else
        # Use default
        DATA_PATH="$DATA_PATH_DEFAULT"
    fi
}

# Find the latest combined CSV file in a directory
find_latest_csv() {
    local base_dir=$1
    local latest_csv=""
    
    # Search for combined CSV files (look in subdirectories with timestamps)
    latest_csv=$(find "$base_dir" -name "combined_benchmark_results_with_perf_*.csv" -type f 2>/dev/null | sort -r | head -1)
    
    if [ -z "$latest_csv" ]; then
        echo ""
        return 1
    fi
    
    echo "$latest_csv"
}

# Run baseline benchmarks
run_baseline_benchmarks() {
    echo ""
    echo "PHASE 1: Running Baseline (BPlusStore) Benchmarks"
    echo ""
    
    if [ ! -d "$BASELINE_DIR" ]; then
        echo "[ERROR] Baseline benchmark directory not found: $BASELINE_DIR"
        return 1
    fi
    
    cd "$BASELINE_DIR"
    
    # Baseline expects DATA_PATH to point directly to the data directory
    # If DATA_PATH defaults to /home/skarim/benchmark_data, append /data
    local baseline_data_path="$DATA_PATH/data"
    
    echo "[INFO] Baseline benchmark script: $(pwd)"
    echo "[INFO] Data path: $baseline_data_path"
    echo "[INFO] Command: DATA_PATH=$baseline_data_path ./profile_and_benchmark_bplus_with_cache.sh full"
    echo ""
    
    # Run baseline benchmarks with "full" command
    DATA_PATH="$baseline_data_path" bash ./profile_and_benchmark_bplus_with_cache.sh full
    
    if [ $? -ne 0 ]; then
        echo "[ERROR] Baseline benchmark failed"
        return 1
    fi
    
    echo ""
    echo "[SUCCESS] Baseline benchmarks completed"
}

# Run optimized benchmarks
run_optimized_benchmarks() {
    echo ""
    echo "PHASE 2: Running Optimized (BplusTreeSOA) Benchmarks"
    echo ""
    
    if [ ! -d "$OPTIMIZED_DIR" ]; then
        echo "[ERROR] Optimized benchmark directory not found: $OPTIMIZED_DIR"
        return 1
    fi
    
    cd "$OPTIMIZED_DIR"
    
    # Optimized expects DATA_PATH to point to the base directory (NOT including /data)
    # The script will automatically create /data and /ycsb subdirectories
    local optimized_data_path="$DATA_PATH"
    
    echo "[INFO] Optimized benchmark script: $(pwd)"
    echo "[INFO] Data path: $optimized_data_path"
    echo "[INFO] Command: DATA_PATH=$optimized_data_path ./profile_and_benchmark_tree_with_cache.sh"
    echo ""
    
    # Run optimized benchmarks (uses default behavior)
    DATA_PATH="$optimized_data_path" bash ./profile_and_benchmark_tree_with_cache.sh
    
    if [ $? -ne 0 ]; then
        echo "[ERROR] Optimized benchmark failed"
        return 1
    fi
    
    echo ""
    echo "[SUCCESS] Optimized benchmarks completed"
}

# Generate plots from CSV files
generate_plots() {
    echo ""
    echo "PHASE 3: Generating Comparison Plots"
    echo ""
    
    # Check if Python dependencies are installed
    if ! python3 -c "import pandas, numpy, matplotlib" 2>/dev/null; then
        echo "[WARNING] Required Python libraries not found. Installing..."
        pip install pandas numpy matplotlib
    fi
    
    cd "$PLOTS_DIR"
    
    # Find latest CSV files from baseline and optimized
    echo "[INFO] Searching for latest benchmark CSV files..."
    
    BASELINE_CSV=$(find_latest_csv "$BASELINE_DIR")
    if [ -z "$BASELINE_CSV" ]; then
        echo "[ERROR] No baseline CSV files found in $BASELINE_DIR"
        echo "[INFO] Checked pattern: cache_profiling_results_*/combined_benchmark_results_with_perf_*.csv"
        return 1
    fi
    echo "[FOUND] Baseline CSV: $BASELINE_CSV"
    
    OPTIMIZED_CSV=$(find_latest_csv "$OPTIMIZED_DIR")
    if [ -z "$OPTIMIZED_CSV" ]; then
        echo "[ERROR] No optimized CSV files found in $OPTIMIZED_DIR"
        echo "[INFO] Checked pattern: cache_profiling_results_*/combined_benchmark_results_with_perf_*.csv"
        return 1
    fi
    echo "[FOUND] Optimized CSV: $OPTIMIZED_CSV"
    
    echo ""
    echo "Generating Figure 3: Throughput Comparison Plot"
    
    if python3 plot_figure3.py "$BASELINE_CSV" "$OPTIMIZED_CSV" "$PLOTS_DIR"; then
        echo "[SUCCESS] Figure 3 (Throughput) generated successfully"
        echo "Output: plot_figure3.{png,pdf}"
    else
        echo "[ERROR] Figure 3 generation failed"
        return 1
    fi
    
    echo ""
    echo "Generating Figure 4: CPI (Instruction Efficiency) Comparison"
    
    if python3 plot_figure4.py "$BASELINE_CSV" "$OPTIMIZED_CSV" "$PLOTS_DIR"; then
        echo "[SUCCESS] Figure 4 (CPI) generated successfully"
        echo "Output: plot_figure4.{png,pdf}"
    else
        echo "[ERROR] Figure 4 generation failed"
        return 1
    fi
    
    echo ""
    echo "[SUCCESS] All plots generated successfully!"
    echo ""
    echo "Plot files available at:"
    ls -lh "$PLOTS_DIR"/standalone_*.{png,pdf} 2>/dev/null | awk '{print "  " $9}'
}

# Print summary
print_summary() {
    local status=$1
    
    echo ""
    echo "PIPELINE SUMMARY"
    echo ""
    
    if [ $status -eq 0 ]; then
        echo "[SUCCESS] Algorithmic Impact Analysis Pipeline Executed Successfully!"
        echo ""
        echo "Output Files:"
        
        if [ "$RUN_BASELINE" = true ]; then
            BASELINE_CSV=$(find_latest_csv "$BASELINE_DIR")
            if [ -n "$BASELINE_CSV" ]; then
                echo "  - Baseline CSV: $BASELINE_CSV"
            fi
        fi
        
        if [ "$RUN_OPTIMIZED" = true ]; then
            OPTIMIZED_CSV=$(find_latest_csv "$OPTIMIZED_DIR")
            if [ -n "$OPTIMIZED_CSV" ]; then
                echo "  - Optimized CSV: $OPTIMIZED_CSV"
            fi
        fi
        
        if [ "$RUN_PLOTS" = true ] || [ "$PLOTS_ONLY" = true ]; then
            echo "  - Figure 3 (Throughput):"
            echo "      - $PLOTS_DIR/standalone_throughput_boxplot_enhanced_labels.png"
            echo "      - $PLOTS_DIR/standalone_throughput_boxplot_enhanced_labels.pdf"
            echo "  - Figure 4 (CPI):"
            echo "      - $PLOTS_DIR/standalone_cpi_boxplot_enhanced_labels.png"
            echo "      - $PLOTS_DIR/standalone_cpi_boxplot_enhanced_labels.pdf"
        fi
        
        echo ""
        echo "Next Steps:"
        echo "  - Review the generated plots in the plots/ directory"
        echo "  - Compare baseline vs optimized performance metrics"
        echo "  - Check CSV files for detailed benchmark data"
    else
        echo "[ERROR] Pipeline execution failed"
        echo ""
        echo "Please check the error messages above for details."
    fi
    
    echo ""
}

# Main execution
main() {
    echo "Microbenchmark Algorithmic Impact Analysis Workflow"
    echo ""
    
    # Parse arguments
    parse_args "$@"
    
    # Finalize data path
    finalize_data_path
    
    # Verify directory structure
    if [ ! -d "$BASELINE_DIR" ] || [ ! -d "$OPTIMIZED_DIR" ] || [ ! -d "$PLOTS_DIR" ]; then
        echo "[ERROR] Repository structure invalid"
        echo "  Baseline: $BASELINE_DIR ($([ -d "$BASELINE_DIR" ] && echo "OK" || echo "MISSING"))"
        echo "  Optimized: $OPTIMIZED_DIR ($([ -d "$OPTIMIZED_DIR" ] && echo "OK" || echo "MISSING"))"
        echo "  Plots: $PLOTS_DIR ($([ -d "$PLOTS_DIR" ] && echo "OK" || echo "MISSING"))"
        return 1
    fi
    
    echo "[INFO] Repository root: $SCRIPT_DIR"
    echo "[INFO] Data path: $DATA_PATH"
    echo ""
    
    # Run benchmarks
    if [ "$RUN_BASELINE" = true ]; then
        run_baseline_benchmarks || { print_summary 1; return 1; }
    fi
    
    if [ "$RUN_OPTIMIZED" = true ]; then
        run_optimized_benchmarks || { print_summary 1; return 1; }
    fi
    
    # Generate plots
    if [ "$RUN_PLOTS" = true ] || [ "$PLOTS_ONLY" = true ]; then
        generate_plots || { print_summary 1; return 1; }
    fi
    
    # Print summary
    print_summary 0
    return 0
}

# Execute main function
main "$@"