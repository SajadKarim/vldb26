#!/usr/bin/env python3
"""
Script to plot YCSB workload performance across different cache policy configurations.
Creates a 3x1 subplot grid showing median throughput for each policy variant with grey bars.
Each subplot represents a different YCSB workload (ycsb_a, ycsb_c, ycsb_e).
Only plots data for 1 thread.
Simplified version for single storage type with median bar visualization.

This version also exports the data for each subplot to separate CSV files:
- ycsb_a_data.csv
- ycsb_c_data.csv
- ycsb_e_data.csv

Layout configuration:
- 1st subplot (ycsb_a): No y-label, Y-ticks with scientific notation, no x-ticks
- 2nd subplot (ycsb_c): Y-label, Y-ticks with scientific notation, no x-ticks
- 3rd subplot (ycsb_e): No y-label, Y-ticks with scientific notation, x-ticks with policy names, x-label
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

def calculate_throughput(record_count, time_us):
    """
    Calculate throughput in operations per second.
    Formula: (record_count / time_us) * 1,000,000
    """
    return (record_count / time_us) * 1_000_000

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
        'CLOCK_cwe': 'CLOCK\n(Cost-Weighted)',
        'LRU_default': 'LRU',
        'LRU_relaxed': 'LRU\n(Non-strict)',
        'LRU_lru_metadata_update_in_order': 'LRU\n(Ordered)',
        'LRU_lru_metadata_update_in_order_and_relaxed': 'LRU (Ordered+\nNon-strict)',
        # Fallback for any unknown variants
        'A2Q': 'A2Q',
        'CLOCK': 'CLOCK', 
        'LRU': 'LRU'
    }
    return policy_display_names.get(policy_variant, policy_variant)

def get_workload_display_name(workload):
    """
    Get display name for YCSB workload.
    """
    workload_names = {
        'ycsb_a': 'YCSB-A (50% Read, 50% Update)',
        'ycsb_b': 'YCSB-B (95% Read, 5% Update)',
        'ycsb_c': 'YCSB-C (100% Read)',
        'ycsb_d': 'YCSB-D (95% Read, 5% Insert)',
        'ycsb_e': 'YCSB-E (95% Scan, 5% Insert)',
        'ycsb_f': 'YCSB-F (50% Read, 50% RMW)'
    }
    return workload_names.get(workload, workload.upper())

def export_workload_data_to_csv(plot_data, workload, policy_variants, output_dir):
    """
    Export all plotted data for a workload to a CSV file.
    Includes mean, percentiles (25th, 50th, 75th) for each policy variant.
    """
    csv_rows = []
    
    # Header
    csv_rows.append(['Workload', workload])
    csv_rows.append([])  # Empty row
    csv_rows.append(['Policy Variant', 'Display Name', 'Mean Throughput (ops/sec)', 
                     'Std Dev', '25th Percentile', '50th Percentile (Median)', 
                     '75th Percentile', 'Min', 'Max', 'Count', 'All Data Points'])
    
    # Data for each policy variant
    for variant in policy_variants:
        data = plot_data[
            (plot_data['policy_variant'] == variant) & 
            (plot_data['operation'] == workload)
        ]['calculated_throughput'].values
        
        if len(data) > 0:
            mean_val = np.mean(data)
            std_val = np.std(data, ddof=1) if len(data) > 1 else 0
            p25 = np.percentile(data, 25)
            p50 = np.percentile(data, 50)
            p75 = np.percentile(data, 75)
            min_val = np.min(data)
            max_val = np.max(data)
            count = len(data)
            
            # Format all data points as a semicolon-separated string
            all_data_str = ';'.join([f"{val:.2f}" for val in data])
            
            csv_rows.append([
                variant, 
                get_display_name(variant).replace('\n', ' '),
                f"{mean_val:.2f}", 
                f"{std_val:.2f}",
                f"{p25:.2f}", 
                f"{p50:.2f}", 
                f"{p75:.2f}",
                f"{min_val:.2f}", 
                f"{max_val:.2f}", 
                count,
                all_data_str
            ])
    
    # Write to CSV file
    csv_filename = output_dir / f"{workload}_data.csv"
    
    with open(csv_filename, 'w') as f:
        for row in csv_rows:
            f.write(','.join(str(cell) for cell in row) + '\n')
    
    print(f"  Exported data to: {csv_filename}")
    return csv_filename

def plot_workload_subplot(ax, plot_data, workload, policy_variants, show_ylabel=True, show_xticks=True, show_yticks=True):
    """
    Create a bar plot showing median values for each policy variant.
    Bars are displayed in grey color.
    
    Args:
        show_ylabel: If True, show the y-axis label. Default is True.
        show_xticks: If True, show the x-axis ticks and labels. Default is True.
        show_yticks: If True, show the y-axis ticks and labels. Default is True.
    """
    # Prepare data grouped by policy variant
    bar_data = []
    bar_positions = []
    
    # Track positions and variants for x-axis labels
    policy_x_positions = []
    policy_variants_with_data = []
    
    x_pos = 0
    bar_width = 0.3
    
    best_idx = -1
    best_median = -1
    
    for variant in policy_variants:
        # Get data for this variant and workload (thread_count is always 1)
        data = plot_data[
            (plot_data['policy_variant'] == variant) & 
            (plot_data['operation'] == workload)
        ]['calculated_throughput'].values
        
        if len(data) > 0:
            median_val = np.median(data)
            bar_data.append(median_val)
            bar_positions.append(x_pos)
            policy_x_positions.append(x_pos)
            policy_variants_with_data.append(variant)
            
            # Track best performer
            if median_val > best_median:
                best_median = median_val
                best_idx = len(bar_positions) - 1
            
            x_pos += 1  # Only increment when data exists
    
    # Create bar plot with median values
    if len(bar_data) > 0:
        ax.bar(bar_positions, bar_data,
               width=bar_width,
               color='grey',
               edgecolor='black',
               linewidth=1.5,
               alpha=0.8,
               zorder=2)
        
        # Draw vertical line for best performer
        if best_idx >= 0:
            best_x_pos = bar_positions[best_idx]
            print(f"  Drawing vertical line for best performer at x={best_x_pos}")
            ax.axvline(x=best_x_pos, color='green', linestyle='--', linewidth=2.5, zorder=10, alpha=0.9)
    
    # Set labels (conditionally show y-label)
    if show_ylabel:
        ax.set_ylabel('Throughput (ops/sec)', fontsize=30)
    
    # Set x-axis ticks for policy names (conditionally show x-ticks)
    if show_xticks:
        ax.set_xticks(policy_x_positions)
        display_names = [get_display_name(variant) for variant in policy_variants_with_data]
        ax.set_xticklabels(display_names, fontsize=26, rotation=90, ha='right')
    else:
        ax.set_xticks([])
        ax.set_xticklabels([])
    
    # Format y-axis to show values in scientific notation (conditionally)
    if show_yticks:
        ax.ticklabel_format(style='scientific', axis='y', scilimits=(0,0))
        ax.tick_params(axis='y', labelsize=26)
        # Increase the size of the exponent (offset text)
        ax.yaxis.get_offset_text().set_fontsize(22)
    else:
        ax.set_yticks([])
        ax.set_yticklabels([])
    
    # Add grid for better readability
    ax.grid(True, alpha=0.3, axis='y', color='grey')
    ax.set_axisbelow(True)
    
    # Tighten x-axis limits to reduce padding
    if len(bar_positions) > 0:
        ax.set_xlim(bar_positions[0] - 0.3, bar_positions[-1] + 0.3)
    
    # Add workload title to subplot
    ax.set_title(get_workload_display_name(workload), fontsize=30, pad=2)

def main(csv_path=None):
    # Get data file from parameter or use default
    if csv_path is None:
        # Use default path relative to script location
        script_dir = Path(__file__).parent
        csv_path = script_dir / 'data' / 'combined_ycsb_benchmark_results_with_perf_20251008_152420.csv'
    else:
        csv_path = Path(csv_path)
    
    if not csv_path.exists():
        print(f"ERROR: CSV file not found at {csv_path}")
        sys.exit(1)
    
    csv_path = str(csv_path)
    print(f"Reading data from: {csv_path}")
    df = pd.read_csv(csv_path)
    
    print(f"Total rows in CSV: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    
    # Calculate throughput if not already present or recalculate for consistency
    df['calculated_throughput'] = df.apply(
        lambda row: calculate_throughput(row['record_count'], row['time_us']), 
        axis=1
    )
    
    # Create policy variant column
    df['policy_variant'] = df.apply(create_policy_variant, axis=1)
    
    print(f"\nUnique policy variants found: {sorted(df['policy_variant'].unique())}")
    print(f"Thread counts: {sorted(df['thread_count'].unique())}")
    print(f"Workloads: {sorted(df['operation'].unique())}")
    print(f"Storage types: {sorted(df['storage_type'].unique())}")
    print(f"Cache sizes: {sorted(df['cache_size'].unique())}")
    print(f"Test runs: {sorted(df['test_run_id'].unique())}")
    
    # Filter for thread_count = 1 only
    df = df[df['thread_count'] == 1]
    print(f"\nData points after filtering for 1 thread: {len(df)}")
    
    # Prepare data for plotting - keep all columns including test_run_id
    plot_data = df[['policy_variant', 'operation', 'storage_type', 'cache_size', 'test_run_id', 'calculated_throughput']].copy()
    
    print(f"\nData points for plotting: {len(plot_data)}")
    print(f"\nSample data:")
    print(plot_data.head(20))
    
    # Get unique policy variants
    policy_variants = sorted(plot_data['policy_variant'].unique())
    
    # Filter to only include ycsb_a, ycsb_c, ycsb_e
    workloads = ['ycsb_a', 'ycsb_c', 'ycsb_e']
    plot_data = plot_data[plot_data['operation'].isin(workloads)]
    
    print(f"\nPolicy variants for plotting: {policy_variants}")
    print(f"Workloads for plotting: {workloads}")
    
    # Export data for each workload to CSV
    output_dir = Path(__file__).parent
    print("\nExporting workload data to CSV files...")
    for workload in workloads:
        export_workload_data_to_csv(plot_data, workload, policy_variants, output_dir)
    
    # Create 3x1 subplot grid (3 workloads in a single column)
    fig, axes = plt.subplots(3, 1, figsize=(18, 14))
    
    # Flatten axes array for easier iteration
    axes_flat = axes.flatten()
    
    # First pass: determine global y-axis limits across all workloads
    print("\nCalculating global y-axis limits...")
    global_min = float('inf')
    global_max = float('-inf')
    
    for workload in workloads:
        workload_data = plot_data[plot_data['operation'] == workload]['calculated_throughput'].values
        if len(workload_data) > 0:
            global_min = min(global_min, workload_data.min())
            global_max = max(global_max, workload_data.max())
    
    # Add minimal padding to the limits (2% on each side)
    y_range = global_max - global_min
    global_min = global_min - (y_range * 0.02)
    global_max = global_max + (y_range * 0.02)  # Minimal padding on top
    
    print(f"Global y-axis range: {global_min:.2f} to {global_max:.2f}")
    
    # Plot each subplot for each workload
    for idx, workload in enumerate(workloads):
        print(f"\nPlotting subplot for workload: {workload}")
        # Show y-label only for the second subplot (idx == 1)
        # Show y-ticks for all subplots
        # Show x-ticks only on the third subplot (idx == 2)
        plot_workload_subplot(
            axes_flat[idx], 
            plot_data, 
            workload, 
            policy_variants,
            show_ylabel=(idx == 1),
            show_xticks=(idx == 2),
            show_yticks=True
        )
        
        # Set the same y-axis limits for all subplots
        axes_flat[idx].set_ylim(global_min, global_max)
    
    # Add x-axis label only to the third subplot
    #for idx in range(len(workloads)):
    #    if idx == 2:
    #        axes_flat[idx].set_xlabel('Replacement Policy Variants', fontsize=30)
    
    # Adjust layout to prevent label cutoff and minimize vertical spacing between subplots
    plt.tight_layout()
    plt.subplots_adjust(hspace=0.25)  # Vertical spacing between rows
    
    # Save the plot
    output_dir = Path(__file__).parent
    output_path = output_dir / "plot_figure12.png"
    
    print(f"\nSaving plot to: {output_path}")
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Plot saved successfully!")
    
    # Also save as PDF for better quality in papers
    output_path_pdf = output_dir / "plot_figure12.pdf"
    print(f"Saving PDF to: {output_path_pdf}")
    plt.savefig(output_path_pdf, bbox_inches='tight')
    print(f"PDF saved successfully!")
    
    plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate YCSB workload performance plots (simplified single storage type version).'
    )
    parser.add_argument(
        '--csv',
        type=str,
        default=None,
        help='Path to the CSV file with YCSB benchmark results. Defaults to data/combined_ycsb_benchmark_results_with_perf_20251008_152420.csv'
    )
    args = parser.parse_args()
    main(csv_path=args.csv)