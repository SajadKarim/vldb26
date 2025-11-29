#!/usr/bin/env python3
"""
Script to plot YCSB workload performance across different cache policy configurations, separated by storage type.
Creates a 1x3 subplot grid showing MEDIAN throughput for each policy variant with grouped bars for each storage device.
Shows only YCSB-A, YCSB-C, and YCSB-D workloads in a single row.
Only plots data for 1 thread.
Uses different colors/patterns for different storage types (same as box plot version).
Includes green vertical lines indicating best-performing policy for each storage type.

This version includes device_aware policy data from a separate CSV file.

This version also exports the data for each subplot to separate CSV files:
- ycsb_a_data.csv
- ycsb_c_data.csv
- ycsb_d_data.csv

Each CSV includes statistics for each policy variant across all storage types,
plus Coefficient of Variation (CV) values for storage sensitivity analysis.
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
plt.rcParams['font.size'] = 26
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
    
    Special handling for device_aware policies: all policies with config_name
    starting with 'device_aware' are normalized to just "device_aware".
    """
    policy = row['policy_name']
    config = row['config_name']
    
    # Special handling for device_aware policies
    # Check if config_name starts with 'device_aware' instead of policy_name
    if config.startswith('device_aware'):
        return 'device_aware'
    
    cleaned_config = clean_config_name(config)
    return f"{policy}_{cleaned_config}"

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
        'CLOCK_clock_buffer_enabled': 'CLOCK\n(Buffered)',
        'CLOCK_clock_buffer_enabled_and_relaxed': 'CLOCK\n(Buffered+\nNon-strict)',
        'LRU_default': 'LRU',
        'LRU_relaxed': 'LRU\n(Non-strict)',
        'LRU_lru_metadata_update_in_order': 'LRU\n(Ordered)',
        'LRU_lru_metadata_update_in_order_and_relaxed': 'LRU\n(Ordered+\nNon-strict)',
        'device_aware': 'Device\nAware',
        # Fallback for any unknown variants
        'A2Q': 'A2Q',
        'CLOCK': 'CLOCK', 
        'LRU': 'LRU'
    }
    return policy_display_names.get(policy_variant, policy_variant)

def get_storage_display_name(storage_type):
    """
    Get display name for storage type.
    """
    storage_names = {
        'VolatileStorage': 'NVDIMM',
        'PMemStorage': 'NVM',
        'FileStorage': 'SSD NVMe'
    }
    return storage_names.get(storage_type, storage_type)

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

def calculate_cv_for_policy(plot_data, workload, policy_variant, storage_types):
    """
    Calculate Coefficient of Variation (CV) for a policy variant across storage types.
    CV measures storage sensitivity: how much performance varies across storage devices.
    """
    storage_means = []
    
    for storage_type in storage_types:
        data = plot_data[
            (plot_data['policy_variant'] == policy_variant) & 
            (plot_data['operation'] == workload) &
            (plot_data['storage_type'] == storage_type)
        ]['calculated_throughput'].values
        
        if len(data) > 0:
            storage_mean = np.mean(data)
            storage_means.append(storage_mean)
    
    # Calculate CV across storage means
    if len(storage_means) > 1:
        mean_of_means = np.mean(storage_means)
        std_of_means = np.std(storage_means, ddof=1)
        if mean_of_means > 0:
            cv = (std_of_means / mean_of_means) * 100
            return cv
    
    return None

def export_workload_data_to_csv(plot_data, workload, policy_variants, storage_types, output_dir):
    """
    Export all plotted data for a workload to a CSV file.
    Includes mean, percentiles (25th, 50th, 75th) for each policy variant and storage type.
    Also includes CV values for storage sensitivity.
    """
    csv_rows = []
    
    # Header
    csv_rows.append(['Workload', workload])
    csv_rows.append([])  # Empty row
    csv_rows.append(['Policy Variant', 'Display Name', 'Storage Type', 'Storage Display Name', 
                     'Mean Throughput (ops/sec)', 'Std Dev', '25th Percentile', 
                     '50th Percentile (Median)', '75th Percentile', 'Min', 'Max', 
                     'Count', 'All Data Points'])
    
    # Data for each policy variant and storage type
    for variant in policy_variants:
        for storage_type in storage_types:
            data = plot_data[
                (plot_data['policy_variant'] == variant) & 
                (plot_data['operation'] == workload) &
                (plot_data['storage_type'] == storage_type)
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
                    storage_type,
                    get_storage_display_name(storage_type),
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
    
    # Add empty rows before CV section
    csv_rows.append([])
    csv_rows.append([])
    csv_rows.append(['Coefficient of Variation (CV) - Storage Sensitivity'])
    csv_rows.append(['Policy Variant', 'Display Name', 'CV (%)'])
    
    # Add CV values for each policy variant
    for variant in policy_variants:
        cv = calculate_cv_for_policy(plot_data, workload, variant, storage_types)
        if cv is not None:
            csv_rows.append([
                variant, 
                get_display_name(variant).replace('\n', ' '),
                f"{cv:.2f}"
            ])
    
    # Write to CSV file
    csv_filename = output_dir / f"{workload}_data.csv"
    
    with open(csv_filename, 'w') as f:
        for row in csv_rows:
            f.write(','.join(str(cell) for cell in row) + '\n')
    
    print(f"  Exported data to: {csv_filename}")
    return csv_filename

def plot_workload_subplot(ax, plot_data, workload, policy_variants, storage_types, show_ylabel=True):
    """
    Create a bar plot showing only median values for each storage type.
    Uses bars instead of boxes with the same color/pattern scheme.
    Shows green vertical lines for best-performing policies.
    
    Args:
        show_ylabel: If True, show the y-axis label. Default is True.
    """
    # Black/Grey/White color scheme with patterns for different storage types
    # VolatileStorage: Light grey with no pattern
    # PMemStorage: Dark grey with diagonal hatch
    # FileStorage: Black with cross hatch
    storage_color_map = {
        'VolatileStorage': 'lightgrey',
        'PMemStorage': 'darkgrey',
        'FileStorage': 'black'
    }
    storage_pattern_map = {
        'VolatileStorage': '',
        'PMemStorage': '///',
        'FileStorage': 'xxx'
    }
    storage_edge_map = {
        'VolatileStorage': 'black',
        'PMemStorage': 'black',
        'FileStorage': 'white'
    }
    
    # Track positions for x-axis labels
    policy_x_positions = []  # Center position for each policy group
    
    x_pos = 0
    bar_width = 1.25  # Width of each bar
    group_spacing = 5.0  # Space between policy groups
    
    # Build a mapping from (storage_type, variant) for tracking best performers
    box_index_map = {}
    
    for variant_idx, variant in enumerate(policy_variants):
        group_start = x_pos
        
        # For each storage type in this policy variant
        for storage_idx, storage_type in enumerate(storage_types):
            # Get data for this variant, workload, and storage type (thread_count is always 1)
            data = plot_data[
                (plot_data['policy_variant'] == variant) & 
                (plot_data['operation'] == workload) &
                (plot_data['storage_type'] == storage_type)
            ]['calculated_throughput'].values
            
            if len(data) > 0:
                median_val = np.median(data)
                
                # Calculate bar x position (offset within group based on storage type index)
                # Bars are centered at group_start with offset based on storage_idx
                bar_x_pos = group_start + (storage_idx - 1) * bar_width
                
                # Plot bar with median value
                ax.bar(bar_x_pos, median_val,
                       width=bar_width,
                       color=storage_color_map[storage_type],
                       hatch=storage_pattern_map[storage_type],
                       edgecolor=storage_edge_map[storage_type],
                       linewidth=1.5,
                       alpha=0.8,
                       zorder=2)
                
                # Store info for finding best performers
                box_index_map[(storage_type, variant)] = {
                    'x_pos': bar_x_pos,
                    'median': median_val,
                    'data': data
                }
            
            x_pos += 1
        
        # Calculate center position for policy label
        group_center = group_start + (len(storage_types) - 1) / 2
        policy_x_positions.append(group_center)
        
        # Add spacing between policy groups
        x_pos += (group_spacing - len(storage_types))
    
    # For each storage type, find the best-performing bar and draw a vertical line
    for storage_type in storage_types:
        # Find all bars for this storage type
        storage_bars = {k: v for k, v in box_index_map.items() if k[0] == storage_type}
        
        if storage_bars:
            # Find the bar with the highest median
            best_key = max(storage_bars.keys(), key=lambda k: storage_bars[k]['median'])
            best_info = storage_bars[best_key]
            best_x_pos = best_info['x_pos']
            
            # Draw vertical line spanning the entire plot height (from bottom to top of y-axis)
            print(f"  Drawing vertical line for {storage_type} at x={best_x_pos}")
            ax.axvline(x=best_x_pos, color='green', linestyle='--', linewidth=2.5, zorder=10, alpha=0.9)
    
    # Calculate and add Coefficient of Variation (CV) annotations
    # CV measures storage sensitivity: how much performance varies across storage devices
    for i, variant in enumerate(policy_variants):
        # Collect ALL data points across all storage types for this policy variant
        all_data = []
        max_y = 0
        
        for storage_type in storage_types:
            data = plot_data[
                (plot_data['policy_variant'] == variant) & 
                (plot_data['operation'] == workload) &
                (plot_data['storage_type'] == storage_type)
            ]['calculated_throughput'].values
            
            if len(data) > 0:
                all_data.extend(data)
                max_y = max(max_y, data.max())
        
        # Calculate Coefficient of Variation (CV) across all data points
        if len(all_data) > 0:
            mean_throughput = np.mean(all_data)
            std_throughput = np.std(all_data)
            
            # if mean_throughput > 0:
            #     cv = (std_throughput / mean_throughput) * 100
                
            #     # Create annotation text
            #     annotation_text = f"CV: {cv:.1f}%"
                
            #     # Calculate center position for this policy group
            #     group_start = i * (len(storage_types) + (5.0 - len(storage_types)))
            #     group_center = group_start + (len(storage_types) - 1) / 2
                
            #     # Position annotation above the highest point
            #     y_offset = max_y * 0.05  # 5% above the max value
            #     ax.text(
            #         group_center, 
            #         max_y + y_offset,
            #         annotation_text,
            #         ha='center',
            #         va='bottom',
            #         fontsize=16,
            #         fontweight='bold',
            #         bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='gray', alpha=0.8)
            #     )
    
    # Set labels (conditionally show y-label)
    if show_ylabel:
        ax.set_ylabel('Throughput (ops/sec)', fontsize=30)
    
    # Set primary x-axis ticks for policy names
    ax.set_xticks(policy_x_positions)
    display_names = [get_display_name(variant) for variant in policy_variants]
    ax.set_xticklabels(display_names, fontsize=26, rotation=90, ha='right')
    
    # Format y-axis to show values in scientific notation
    ax.ticklabel_format(style='scientific', axis='y', scilimits=(0,0))
    ax.tick_params(axis='y', labelsize=26)
    
    # Increase the size of the exponent (offset text)
    ax.yaxis.get_offset_text().set_fontsize(26)
    
    # Add grid for better readability
    ax.grid(True, alpha=0.3, axis='y', color='grey')
    ax.set_axisbelow(True)
    
    # Add workload title to subplot
    ax.set_title(get_workload_display_name(workload), fontsize=30, pad=10)
    
    return storage_color_map, storage_pattern_map, storage_edge_map

def main(csv_path_original=None, csv_path_device_aware=None):
    # Get data files from parameters or use defaults
    if csv_path_original is None:
        # Use default path relative to script location
        script_dir = Path(__file__).parent
        csv_path_original = script_dir / 'data' / 'combined_ycsb_benchmark_results_with_perf_20251009_185641.csv'
    else:
        csv_path_original = Path(csv_path_original)
    
    if csv_path_device_aware is None:
        # Use default path relative to script location
        script_dir = Path(__file__).parent
        csv_path_device_aware = script_dir / 'data' / 'combined_device_aware_ycsb_results_with_perf_20251010_030225.csv'
    else:
        csv_path_device_aware = Path(csv_path_device_aware)
    
    if not csv_path_original.exists():
        print(f"ERROR: Original CSV file not found at {csv_path_original}")
        sys.exit(1)
    
    if not csv_path_device_aware.exists():
        print(f"ERROR: Device aware CSV file not found at {csv_path_device_aware}")
        sys.exit(1)
    
    csv_path_original = str(csv_path_original)
    csv_path_device_aware = str(csv_path_device_aware)
    
    print(f"Reading original data from: {csv_path_original}")
    df_original = pd.read_csv(csv_path_original)
    
    print(f"Reading device_aware data from: {csv_path_device_aware}")
    df_device_aware = pd.read_csv(csv_path_device_aware)
    
    print(f"\nOriginal data - Total rows: {len(df_original)}")
    print(f"Device aware data - Total rows: {len(df_device_aware)}")
    
    # Fix thread_count in device_aware data (temporary workaround for bug)
    # Treat thread_count=4 as thread_count=1 for device_aware data
    print(f"Device aware thread counts before fix: {sorted(df_device_aware['thread_count'].unique())}")
    df_device_aware['thread_count'] = 1
    print(f"Device aware thread counts after fix: {sorted(df_device_aware['thread_count'].unique())}")
    
    # Combine both dataframes
    df = pd.concat([df_original, df_device_aware], ignore_index=True)
    
    print(f"Combined data - Total rows: {len(df)}")
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
    
    # Get unique policy variants and storage types
    policy_variants = sorted(plot_data['policy_variant'].unique())
    # Define storage types in the desired order: NVDIMM, NVM, SSD NVMe
    storage_types = ['VolatileStorage', 'PMemStorage', 'FileStorage']
    
    # Get unique workloads - filter to only show ycsb_a, ycsb_c, ycsb_d
    all_workloads = sorted(plot_data['operation'].unique())
    workloads = [w for w in ['ycsb_a', 'ycsb_c', 'ycsb_d'] if w in all_workloads]
    
    print(f"\nPolicy variants for plotting: {policy_variants}")
    print(f"Storage types for plotting: {storage_types}")
    print(f"All workloads available: {all_workloads}")
    print(f"Workloads selected for plotting: {workloads}")
    
    # Export data for each workload to CSV
    output_dir = Path(__file__).parent
    print("\nExporting workload data to CSV files...")
    for workload in workloads:
        export_workload_data_to_csv(plot_data, workload, policy_variants, storage_types, output_dir)
    
    # Create 1x3 subplot grid (3 workloads in a single row)
    fig, axes = plt.subplots(1, 3, figsize=(46, 9))
    
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
    
    # Add some padding to the limits (5% on each side)
    y_range = global_max - global_min
    global_min = global_min - (y_range * 0.05)
    global_max = global_max + (y_range * 0.15)  # Extra padding on top for CV annotations
    
    print(f"Global y-axis range: {global_min:.2f} to {global_max:.2f}")
    
    # Plot each subplot for each workload
    for idx, workload in enumerate(workloads):
        print(f"\nPlotting subplot for workload: {workload}")
        # Only show y-label for the first subplot (idx == 0)
        storage_color_map, storage_pattern_map, storage_edge_map = plot_workload_subplot(
            axes_flat[idx], 
            plot_data, 
            workload, 
            policy_variants, 
            storage_types,
            show_ylabel=(idx == 0)
        )
        
        # Set the same y-axis limits for all subplots
        axes_flat[idx].set_ylim(global_min, global_max)
        # Remove padding inside the subplot
        axes_flat[idx].margins(0.01)
    
    # Remove y-axis ticks from 2nd and 3rd subplots (indices 1 and 2)
    for idx in [1, 2]:
        axes_flat[idx].set_yticklabels([])
    
    # Add x-axis label to all subplots (single row)
    for idx in range(len(workloads)):
        if idx==1:
            axes_flat[idx].set_xlabel('Replacement Policy Variants', fontsize=30)
        else:
            axes_flat[idx].set_xlabel('', fontsize=30)
    
    # Create a single legend for the entire figure
    legend_elements = [
        plt.Rectangle((0, 0), 1, 1, 
                     facecolor=storage_color_map[st], 
                     hatch=storage_pattern_map[st], 
                     edgecolor=storage_edge_map[st],
                     linewidth=1.5,
                     alpha=0.7,
                     label=get_storage_display_name(st))
        for st in storage_types
    ]
    
    # Add legend to the last subplot (rightmost, index 2) in a single column
    axes_flat[2].legend(handles=legend_elements, loc='upper right', fontsize=26, frameon=True, 
                       ncol=1, framealpha=1)
    
    # Adjust layout to prevent label cutoff and reduce horizontal spacing between subplots
    plt.tight_layout()
    plt.subplots_adjust(wspace=0.02)  # Minimal horizontal spacing
    
    # Save the plot
    output_dir = Path(__file__).parent
    output_path = output_dir / "plot_figure13.png"
    
    print(f"\nSaving plot to: {output_path}")
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Plot saved successfully!")
    
    # Also save as PDF for better quality in papers
    output_path_pdf = output_dir / "plot_figure13.pdf"
    print(f"Saving PDF to: {output_path_pdf}")
    plt.savefig(output_path_pdf, bbox_inches='tight')
    print(f"PDF saved successfully!")
    
    plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate YCSB workload performance plots with device-aware policy comparison.'
    )
    parser.add_argument(
        '--csv-original',
        type=str,
        default=None,
        help='Path to the original CSV file with YCSB benchmark results. Defaults to data/combined_ycsb_benchmark_results_with_perf_20251009_185641.csv'
    )
    parser.add_argument(
        '--csv-device-aware',
        type=str,
        default=None,
        help='Path to the device-aware CSV file with YCSB benchmark results. Defaults to data/combined_device_aware_ycsb_results_with_perf_20251010_030225.csv'
    )
    args = parser.parse_args()
    main(csv_path_original=args.csv_original, csv_path_device_aware=args.csv_device_aware)