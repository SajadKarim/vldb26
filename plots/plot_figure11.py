#!/usr/bin/env python3
"""
Script to plot YCSB workload performance across different thread counts with Coefficient of Variation (CV).
Creates a single line plot showing throughput vs thread count for each policy variant.
Each line represents a different combination of policy configuration, storage type, and cache size.
Shows CV at each thread count to measure performance variability across test runs.
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
plt.rcParams['font.size'] = 28
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

def get_storage_short_name(storage_type):
    """
    Get short name for storage type.
    """
    storage_names = {
        'VolatileStorage': 'NVDIMM',
        'PMemStorage': 'NVM',
        'FileStorage': 'SSD'
    }
    return storage_names.get(storage_type, storage_type)

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

def get_config_display_name(config_name):
    """
    Get display name for config.
    """
    config_display_names = {
        'lru_metadata_update_in_order_and_relaxed': 'Ordered+Non-strict',
        'lru_metadata_update_in_order': 'Ordered',
        'relaxed': 'Non-strict',
        'default': 'Default'
    }
    cleaned = clean_config_name(config_name)
    return config_display_names.get(cleaned, cleaned)

def get_display_label(policy_variant, storage_type=None):
    """
    Get display label for policy variant and storage type.
    Format: PolicyName (Config) - Storage
    """
    parts = policy_variant.split('_')
    if len(parts) >= 2:
        policy = parts[0]
        # Everything after policy name is config
        config = '_'.join(parts[1:])
        config_display = get_config_display_name(config)
        label = f"{policy} ({config_display})"
    else:
        label = policy_variant
    
    if storage_type:
        storage_display = get_storage_display_name(storage_type)
        label = f"{label} - {storage_display}"
    
    return label

def calculate_throughput(record_count, time_us):
    """
    Calculate throughput in operations per second.
    Formula: (record_count / time_us) * 1,000,000
    """
    return (record_count / time_us) * 1_000_000

def plot_thread_scalability(df, output_dir):
    """
    Create a line plot showing throughput vs thread count for each policy variant.
    """
    # Create figure
    fig, ax = plt.subplots(figsize=(22, 12))
    
    # Get unique policy variants and thread counts
    policy_variants = sorted(df['policy_variant'].unique())
    thread_counts = sorted(df['thread_count'].unique())
    
    print(f"\nPolicy variants to plot: {len(policy_variants)}")
    for pv in policy_variants:
        print(f"  - {pv}")
    
    # Define grey colors and different line patterns for variants
    colors = ['dimgray', 'gray', 'darkgray']  # Grey colors for 3 variants
    linestyles = ['-', '--', '-.']  # Solid, dashed, dash-dot
    markers = ['o', 's', '^']  # Circle, square, triangle
    
    # Define different error bar styles for better distinction (grayscale)
    error_colors = ['dimgray', 'gray', 'darkgray']  # Match line colors
    error_capsize = [10, 12, 14]  # Different cap sizes
    error_linewidths = [2.5, 3.0, 3.5]  # Different line widths
    
    # Calculate horizontal offset for error bars to prevent overlap
    # Offset range: spread variants around the center point
    num_variants = len(policy_variants)
    offset_range = 1.0  # Total spread range
    if num_variants > 1:
        offset_step = offset_range / (num_variants - 1)
        offsets = [i * offset_step - offset_range/2 for i in range(num_variants)]
    else:
        offsets = [0]
    
    # Ensure we have enough styles
    while len(colors) < len(policy_variants):
        colors.extend(['dimgray', 'gray', 'darkgray'])
    while len(linestyles) < len(policy_variants):
        linestyles.extend(['-', '--', '-.'])
    while len(markers) < len(policy_variants):
        markers.extend(['o', 's', '^'])
    while len(error_colors) < len(policy_variants):
        error_colors.extend(['dimgray', 'gray', 'darkgray'])
    while len(error_capsize) < len(policy_variants):
        error_capsize.extend([10, 12, 14])
    while len(error_linewidths) < len(policy_variants):
        error_linewidths.extend([2.5, 3.0, 3.5])
    
    # Dictionary to store CV values grouped by thread count
    cv_by_thread = {tc: {} for tc in thread_counts}
    
    # Plot lines for each policy variant
    for idx, variant in enumerate(policy_variants):
        # Get data for this variant
        variant_data = df[df['policy_variant'] == variant].copy()
        
        # Group by thread_count and calculate mean of throughput
        grouped = variant_data.groupby('thread_count')['calculated_throughput'].agg(['mean', 'std', 'count'])
        
        thread_counts_data = grouped.index.values
        mean_throughput = grouped['mean'].values
        std_throughput = grouped['std'].values
        
        # Plot line with error bars
        color = colors[idx]
        linestyle = linestyles[idx]
        marker = markers[idx]
        label = get_display_label(variant)
        error_color = error_colors[idx]
        capsize = error_capsize[idx]
        error_linewidth = error_linewidths[idx]
        offset = offsets[idx]
        
        # Apply horizontal offset to thread counts for both line and error bars
        thread_counts_offset = thread_counts_data + offset
        
        # Plot the line with offset
        ax.plot(thread_counts_offset, mean_throughput, 
                color=color, linestyle=linestyle, marker=marker,
                markersize=10, linewidth=2.5, label=label,
                markerfacecolor='white', markeredgecolor=color, markeredgewidth=2,
                zorder=3)
        
        # Add error bars with same offset and distinct styles
        ax.errorbar(thread_counts_offset, mean_throughput, yerr=std_throughput,
                    fmt='none',  # Don't plot line/markers again
                    ecolor=error_color, elinewidth=error_linewidth, 
                    capsize=capsize, capthick=error_linewidth,
                    alpha=0.8, zorder=2)
        
        # Calculate CV values and store them grouped by thread count
        for i in range(len(thread_counts_data)):
            thread_count = thread_counts_data[i]
            
            # Get all data points for this variant and thread count
            thread_data = variant_data[variant_data['thread_count'] == thread_count]['calculated_throughput'].values
            
            # Calculate CV if we have data
            if len(thread_data) > 0:
                mean_val = np.mean(thread_data)
                std_val = np.std(thread_data)
                
                if mean_val > 0:
                    cv = (std_val / mean_val) * 100
                    # Store CV with marker and color info
                    cv_by_thread[thread_count][variant] = {
                        'cv': cv,
                        'marker': marker,
                        'color': color
                    }
        
        print(f"\n{variant}:")
        for i, (tc, mean, std) in enumerate(zip(thread_counts_data, mean_throughput, std_throughput)):
            # Calculate CV for this thread count
            thread_data = variant_data[variant_data['thread_count'] == tc]['calculated_throughput'].values
            if len(thread_data) > 0 and mean > 0:
                cv = (std / mean) * 100
                print(f"  Thread {tc}: {mean:.2f} ± {std:.2f} ops/sec (CV: {cv:.1f}%)")
            else:
                print(f"  Thread {tc}: {mean:.2f} ± {std:.2f} ops/sec")
    
    # Add grouped CV text boxes for each thread count
    y_min, y_max = ax.get_ylim()
    box_y_position = y_min + (y_max - y_min) * 0.15  # Position at 5% from bottom
    
    # Map matplotlib markers to Unicode symbols (hollow versions to match plot)
    marker_symbols = {
        'o': '○',  # White circle
        's': '□',  # White square
        '^': '△'   # White triangle
    }
    
    for tc in thread_counts:
        if tc in cv_by_thread and cv_by_thread[tc]:
            # Build the text with markers/symbols
            cv_lines = []
            for variant in policy_variants:
                if variant in cv_by_thread[tc]:
                    cv_info = cv_by_thread[tc][variant]
                    cv_val = cv_info['cv']
                    marker = cv_info['marker']
                    color = cv_info['color']
                    
                    # Convert matplotlib marker to Unicode symbol
                    symbol = marker_symbols.get(marker, marker)
                    
                    # Create a line with marker symbol and CV value
                    cv_lines.append(f"{symbol} {cv_val:.1f}%")
            
            if cv_lines:
                label_text = "CV:\n" + "\n".join(cv_lines)
                
                # Add text box at the thread count position
                ax.text(tc, box_y_position, label_text,
                       fontsize=24, ha='center', va='bottom',
                       bbox=dict(boxstyle='round,pad=0.4', facecolor='white', 
                                edgecolor='gray', alpha=0.85, linewidth=1),
                       color='black', zorder=5, linespacing=1.5)
    
    # Set labels (no title)
    ax.set_xlabel('Thread Count', fontsize=33)
    ax.set_ylabel('Throughput (ops/sec)', fontsize=32,)
    
    # Use linear scale to show scientific notation properly
    ax.set_yscale('linear')
    
    # Format y-axis to show values in scientific notation (same as storage plot)
    ax.ticklabel_format(style='scientific', axis='y', scilimits=(0,0))
    ax.tick_params(axis='both', labelsize=28)
    
    # Increase the size of the exponent (offset text)
    ax.yaxis.get_offset_text().set_fontsize(28)
    
    # Set x-axis to show only integer thread counts
    ax.set_xticks(thread_counts)
    
    # Add grid for better readability
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=1, which='both')
    ax.set_axisbelow(True)
    
    # Add legend (top-left inside the plot)
    ax.legend(loc='upper left', fontsize=28, frameon=True, framealpha=0.9)
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the plot
    output_path = output_dir / "plot_figure11.png"
    print(f"\nSaving plot to: {output_path}")
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Plot saved successfully!")
    
    # Also save as PDF for better quality in papers
    output_path_pdf = output_dir / "ycsb_thread_scalability_cv.pdf"
    print(f"Saving PDF to: {output_path_pdf}")
    plt.savefig(output_path_pdf, bbox_inches='tight')
    print(f"PDF saved successfully!")
    
    plt.close()

def main(csv_path=None):
    """Main function to create thread scalability plot with CV."""
    # Get data file from parameter or use default
    if csv_path is None:
        # Use default path relative to script location
        script_dir = Path(__file__).parent
        csv_path = script_dir / 'data' / 'combined_ycsb_benchmark_results_with_perf_20251015_043151.csv'
    else:
        csv_path = Path(csv_path)
    
    if not csv_path.exists():
        print(f"ERROR: CSV file not found at {csv_path}")
        sys.exit(1)

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
    print(f"Config names: {sorted(df['config_name'].unique())}")
    print(f"Test runs: {sorted(df['test_run_id'].unique())}")
    
    # Filter to only show non-strict (relaxed) variants
    #df = df[df['config_name'] == 'concurrent_relaxed'].copy()
    print(f"\nFiltered to non-strict variants only")
    print(f"Remaining policy variants: {sorted(df['policy_variant'].unique())}")
    
    # Filter to only show specific thread counts: 5, 10, 15, 30, 40
    threads_to_show = [1, 5, 20, 40]
    df = df[df['thread_count'].isin(threads_to_show)].copy()
    print(f"\nFiltered to thread counts: {threads_to_show}")
    print(f"Remaining thread counts: {sorted(df['thread_count'].unique())}")
    print(f"Rows after thread filtering: {len(df)}")
    
    # Prepare data for plotting - keep all columns including test_run_id
    plot_data = df[['policy_variant', 'thread_count', 'operation', 'storage_type', 
                    'cache_size', 'config_name', 'test_run_id', 'calculated_throughput']].copy()
    
    print(f"\nData points for plotting: {len(plot_data)}")
    print(f"\nSample data:")
    print(plot_data.head(20))
    
    # Create the plot
    output_dir = Path(__file__).parent
    plot_thread_scalability(plot_data, output_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate YCSB thread scalability plot with Coefficient of Variation.'
    )
    parser.add_argument(
        '--csv',
        type=str,
        default=None,
        help='Path to the CSV file with benchmark results. Defaults to data/combined_ycsb_benchmark_results_with_perf_20251015_043151.csv'
    )
    args = parser.parse_args()
    main(csv_path=args.csv)