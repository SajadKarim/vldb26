#!/usr/bin/env python3
"""
Script to plot cache policy hit rate across different configurations.
Creates a line plot showing hit rate % for each policy variant with error bars.
Hit Rate = (cache_hits / (cache_hits + cache_misses)) * 100
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import re
from pathlib import Path
import numpy as np
import warnings
import sys
import argparse
warnings.filterwarnings('ignore')

# Set style for better-looking plots
plt.rcParams['figure.figsize'] = (16, 10)
plt.rcParams['font.size'] = 12
plt.rcParams['axes.facecolor'] = 'white'
plt.rcParams['figure.facecolor'] = 'white'

def clean_config_name(config_name):
    """
    Remove 'concurrent' and 'non_concurrent' from config name.
    Also clean up extra underscores and formatting.
    """
    # Remove concurrent/non_concurrent prefixes
    cleaned = re.sub(r'^(non_)?concurrent_', '', config_name)
    # Clean up any double underscores
    cleaned = re.sub(r'__+', '_', cleaned)
    # Remove leading/trailing underscores
    cleaned = cleaned.strip('_')
    return cleaned

def create_policy_variant(row):
    """
    Create a policy variant name from policy_name and cleaned config_name.
    Format: PolicyName_config
    """
    policy = row['policy_name']
    config = clean_config_name(row['config_name'])
    return f"{policy}_{config}"

def get_display_name(policy_variant):
    """
    Get display name for policy variant.
    """
    policy_display_names = {
        'A2Q_default': '2Q',
        'A2Q_relaxed': '2Q\n(Non-strict)',
        'A2Q_a2q_ghost_q_enabled': 'A2Q',
        'A2Q_a2q_ghost_q_enabled_and_relaxed': 'A2Q\n(Non-strict)',
        'CLOCK_default': 'CLOCK',
        'CLOCK_relaxed': 'CLOCK\n(Non-strict)',
        'CLOCK_clock_buffer_enabled': 'CLOCK\n(Buffered)',
        'CLOCK_clock_buffer_enabled_and_relaxed': 'CLOCK\n(Buffered\n+Non-strict)',
        'LRU_default': 'LRU',
        'LRU_relaxed': 'LRU\n(Non-strict)',
        'LRU_lru_metadata_update_in_order': 'LRU\n(Ordered)',
        'LRU_lru_metadata_update_in_order_and_relaxed': 'LRU\n(Ordered\n+Non-strict)',
        # Fallback for any unknown variants
        'A2Q': 'A2Q',
        'CLOCK': 'CLOCK', 
        'LRU': 'LRU'
    }
    return policy_display_names.get(policy_variant, policy_variant)

def main(csv_path=None):
    # Path to the combined CSV file
    if csv_path is None:
        # Use default path relative to script location
        script_dir = Path(__file__).parent
        csv_path = script_dir / 'data' / 'combined_benchmark_results_with_perf_20251003_110829.csv'
    else:
        csv_path = Path(csv_path)
    
    if not csv_path.exists():
        print(f"ERROR: CSV file not found at {csv_path}")
        sys.exit(1)

    print(f"Reading data from: {csv_path}")
    df = pd.read_csv(csv_path)
    
    print(f"Total rows in CSV: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    
    # Create policy variant column
    df['policy_variant'] = df.apply(create_policy_variant, axis=1)
    
    print(f"\nUnique policy variants found: {sorted(df['policy_variant'].unique())}")
    print(f"Thread counts: {sorted(df['thread_count'].unique())}")
    print(f"Operations: {sorted(df['operation'].unique())}")
    print(f"Storage types: {sorted(df['storage_type'].unique())}")
    print(f"Cache sizes: {sorted(df['cache_size'].unique())}")
    print(f"Test runs: {sorted(df['test_run_id'].unique())}")
    
    # Prepare data for plotting - keep all columns including test_run_id, cache_hits, cache_misses, and throughput
    plot_data = df[['policy_variant', 'thread_count', 'storage_type', 'cache_size', 'operation', 'test_run_id', 'cache_hits', 'cache_misses', 'throughput_ops_sec']].copy()
    
    print(f"\nData points for plotting: {len(plot_data)}")
    
    # Filter to only 1 thread and cache size 5% (0.05)
    plot_data = plot_data[plot_data['thread_count'] == 1]
    
    # Check what cache sizes are available
    print(f"\nAvailable cache sizes: {sorted(plot_data['cache_size'].unique())}")
    
    # Filter for 15% cache size
    plot_data = plot_data[plot_data['cache_size'] == 0.15]
    
    print(f"Cache size filter: 15% (0.15)")
    print(f"Data points after filtering: {len(plot_data)}")
    
    # Calculate hit rate for each data point
    plot_data['hit_rate'] = (plot_data['cache_hits'] / (plot_data['cache_hits'] + plot_data['cache_misses'])) * 100
    
    # Group by policy variant AND operation type, then calculate mean and standard error for hit rate
    policy_stats = plot_data.groupby(['policy_variant', 'operation'])['hit_rate'].agg(['mean', 'std', 'count']).reset_index()
    policy_stats['sem'] = policy_stats['std'] / np.sqrt(policy_stats['count'])  # Standard error of mean
    policy_stats = policy_stats.sort_values(['policy_variant', 'operation'])
    
    # Group by policy variant AND operation type for throughput statistics
    throughput_stats = plot_data.groupby(['policy_variant', 'operation'])['throughput_ops_sec'].agg(['mean', 'std', 'count']).reset_index()
    throughput_stats['sem'] = throughput_stats['std'] / np.sqrt(throughput_stats['count'])  # Standard error of mean
    throughput_stats = throughput_stats.sort_values(['policy_variant', 'operation'])
    
    print(f"\nPolicy variants for plotting: {sorted(policy_stats['policy_variant'].unique())}")
    print(f"Operations: {sorted(policy_stats['operation'].unique())}")
    print(f"\nHit rate statistics:")
    print(policy_stats)
    print(f"\nThroughput statistics:")
    print(throughput_stats)
    
    # Create two subplots (one for hit rate, one for throughput) with white background
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(20, 16), facecolor='white')
    ax1.set_facecolor('white')
    ax2.set_facecolor('white')
    
    # Add main title
    #fig.suptitle('Hit Ratio and Throughput of Replacement Policy Variants\n(Single Thread, Cache Size: 15%)', 
    #             fontsize=34, fontweight='bold', color='black', y=0.995)
    
    # Get unique policy variants and display names for x-axis
    unique_variants = sorted(policy_stats['policy_variant'].unique())
    display_names = [get_display_name(variant) for variant in unique_variants]
    x_positions = np.arange(len(display_names))
    
    # Get unique operations
    operations = sorted(policy_stats['operation'].unique())
    
    # Define grey/black colors and highly identifiable line styles and markers for each operation
    # Using different shades of grey/black with distinct line styles
    styles = [
        {'color': 'black', 'linestyle': '-', 'marker': 'o', 'markersize': 12},      # delete: solid line, circle
        {'color': '#2d2d2d', 'linestyle': '--', 'marker': 's', 'markersize': 11},   # insert: dashed line, square
        {'color': '#4d4d4d', 'linestyle': '-.', 'marker': '^', 'markersize': 12},   # search_random: dash-dot, triangle up
        {'color': '#6d6d6d', 'linestyle': ':', 'marker': 'D', 'markersize': 10},    # search_sequential: dotted, diamond
        {'color': '#8d8d8d', 'linestyle': '-', 'marker': 'v', 'markersize': 12},    # search_uniform: solid, triangle down
        {'color': '#adadad', 'linestyle': '--', 'marker': 'p', 'markersize': 12}    # search_zipfian: dashed, pentagon
    ]
    
    # Helper function to format operation names for legend
    def format_operation_label(operation):
        if operation == 'delete':
            return 'Delete'
        elif operation == 'insert':
            return 'Insert'
        elif operation.startswith('search_'):
            search_type = operation.replace('search_', '')
            return f'Search ({search_type})'
        else:
            return operation.capitalize()
    
    # ========== SUBPLOT 1: HIT RATE ==========
    # Plot a line for each operation type
    for idx, operation in enumerate(operations):
        op_data = policy_stats[policy_stats['operation'] == operation].sort_values('policy_variant')
        
        # Ensure we have data for all policy variants (fill missing with NaN)
        op_means = []
        op_sems = []
        for variant in unique_variants:
            variant_data = op_data[op_data['policy_variant'] == variant]
            if len(variant_data) > 0:
                op_means.append(variant_data['mean'].values[0])
                op_sems.append(variant_data['sem'].values[0])
            else:
                op_means.append(np.nan)
                op_sems.append(np.nan)
        
        style = styles[idx % len(styles)]
        
        # Plot line with error bars (using standard error)
        ax1.errorbar(
            x_positions,
            op_means,
            yerr=op_sems,
            fmt=f'{style["marker"]}{style["linestyle"]}',
            linewidth=3.0,
            markersize=style['markersize'],
            color=style['color'],
            ecolor=style['color'],
            elinewidth=2.0,
            capsize=5,
            capthick=2.0,
            label=format_operation_label(operation),
            alpha=1.0,
            markerfacecolor='white',
            markeredgewidth=2.0,
            markeredgecolor=style['color']
        )
    
    # Set labels for hit rate subplot with black text
    ax1.set_ylabel('Hit Rate (%)', fontsize=32, color='black')
    
    # Remove x-axis ticks and labels from first subplot
    ax1.set_xticks([])
    ax1.set_xticklabels([])
    
    # Format y-axis with linear scale and black text
    ax1.tick_params(axis='y', labelsize=26, colors='black')
    
    ax2.yaxis.get_offset_text().set_fontsize(24)

    # Set y-axis limits based on data range with ±2% buffer
    y_min = policy_stats['mean'].min() - 2
    y_max = 100
    ax1.set_ylim([max(0, y_min), min(100, y_max)])
    
    # Add light grey grid for better readability
    ax1.grid(True, alpha=0.4, axis='both', color='#cccccc', linestyle='-', linewidth=0.8)
    ax1.set_axisbelow(True)
    
    # Style the spines (borders) in black
    for spine in ax1.spines.values():
        spine.set_edgecolor('black')
        spine.set_linewidth(1.5)
    
    # Add legend with transparent background in 2 columns
    ax1.legend(loc='best', fontsize=26, frameon=True, fancybox=False, shadow=False, 
              framealpha=0.7, edgecolor='black', facecolor='white', ncol=2)
    
    # ========== SUBPLOT 2: THROUGHPUT ==========
    # Plot a line for each operation type
    for idx, operation in enumerate(operations):
        op_data = throughput_stats[throughput_stats['operation'] == operation].sort_values('policy_variant')
        
        # Ensure we have data for all policy variants (fill missing with NaN)
        op_means = []
        op_sems = []
        for variant in unique_variants:
            variant_data = op_data[op_data['policy_variant'] == variant]
            if len(variant_data) > 0:
                op_means.append(variant_data['mean'].values[0])
                op_sems.append(variant_data['sem'].values[0])
            else:
                op_means.append(np.nan)
                op_sems.append(np.nan)
        
        style = styles[idx % len(styles)]
        
        # Plot line with error bars (using standard error)
        ax2.errorbar(
            x_positions,
            op_means,
            yerr=op_sems,
            fmt=f'{style["marker"]}{style["linestyle"]}',
            linewidth=3.0,
            markersize=style['markersize'],
            color=style['color'],
            ecolor=style['color'],
            elinewidth=2.0,
            capsize=5,
            capthick=2.0,
            label=format_operation_label(operation),
            alpha=1.0,
            markerfacecolor='white',
            markeredgewidth=2.0,
            markeredgecolor=style['color']
        )
    
    # Set labels for throughput subplot with black text
    #ax2.set_xlabel('Replacement Policy Variants', fontsize=32, color='black')
    ax2.set_ylabel('Throughput (ops/sec)', fontsize=32, color='black')
    
    # Set x-axis ticks with black text
    ax2.set_xticks(x_positions)
    ax2.set_xticklabels(display_names, fontsize=26, rotation=90, ha='right', color='black')
    
    # Format y-axis with linear scale and black text
    ax2.tick_params(axis='y', labelsize=26, colors='black')
    ax2.tick_params(axis='x', colors='black')
    
    # Add light grey grid for better readability
    ax2.grid(True, alpha=0.4, axis='both', color='#cccccc', linestyle='-', linewidth=0.8)
    ax2.set_axisbelow(True)
    
    # Style the spines (borders) in black
    for spine in ax2.spines.values():
        spine.set_edgecolor('black')
        spine.set_linewidth(1.5)
    
    # Adjust layout to prevent label cutoff and accommodate title
    plt.tight_layout(rect=[0, 0, 1, 0.985])
    
    # Save the plot
    script_dir = Path(__file__).parent
    output_path = script_dir / "plot_figure6.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {output_path}")
    
    # Also save as PDF for better quality
    output_path_pdf = output_path.with_suffix('.pdf')
    plt.savefig(output_path_pdf, bbox_inches='tight')
    print(f"Plot also saved as PDF: {output_path_pdf}")
    
    # Show the plot
    plt.show()
    
    # Print summary statistics
    print("\n" + "="*80)
    print("HIT RATE SUMMARY STATISTICS (BY OPERATION)")
    print("="*80)
    
    for variant in sorted(policy_stats['policy_variant'].unique()):
        display_name = get_display_name(variant)
        print(f"\n{display_name} ({variant}):")
        variant_data = policy_stats[policy_stats['policy_variant'] == variant].sort_values('operation')
        for idx, row in variant_data.iterrows():
            operation = row['operation']
            print(f"  {operation}:")
            print(f"    Hit Rate Mean: {row['mean']:.2f}%")
            print(f"    Hit Rate Std: {row['std']:.2f}%")
            print(f"    Hit Rate SEM: {row['sem']:.2f}%")
            print(f"    Data Points: {int(row['count'])}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Plot cache policy hit rate across different configurations.'
    )
    parser.add_argument(
        '--csv',
        type=str,
        default=None,
        help='Path to the CSV file with benchmark results. Defaults to data/combined_benchmark_results_with_perf_20251003_110829.csv'
    )
    args = parser.parse_args()
    main(csv_path=args.csv)