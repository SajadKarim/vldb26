#!/usr/bin/env python3
"""
Script to plot cache policy performance across different configurations.
Creates a box plot showing throughput for each policy variant grouped by thread count.
Uses black/grey/white color scheme with patterns for different thread counts.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import re
from pathlib import Path
import numpy as np
import warnings
import argparse
import sys
warnings.filterwarnings('ignore')

# Set style for better-looking plots
plt.rcParams['figure.figsize'] = (20, 10)
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
        csv_path = script_dir / 'data' / 'combined_benchmark_results_with_perf_20251002_072156.csv'
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
    print(f"Operations: {sorted(df['operation'].unique())}")
    print(f"Storage types: {sorted(df['storage_type'].unique())}")
    print(f"Cache sizes: {sorted(df['cache_size'].unique())}")
    print(f"Test runs: {sorted(df['test_run_id'].unique())}")
    
    # Do NOT average across test runs - keep all individual data points
    # For the box plot, we want to show distribution across all operations, 
    # cache sizes, storage types, AND test runs for each policy_variant + thread_count combination
    print("\nKeeping all individual test runs (no averaging)...")
    
    # Prepare data for plotting - keep all columns including test_run_id
    plot_data = df[['policy_variant', 'thread_count', 'storage_type', 'cache_size', 'operation', 'test_run_id', 'calculated_throughput']].copy()
    
    # Convert thread_count to string for better grouping in the plot
    plot_data['thread_count_str'] = plot_data['thread_count'].astype(str) + ' threads'
    
    print(f"\nData points for plotting: {len(plot_data)}")
    print(f"\nSample data:")
    print(plot_data.head(20))
    
    # Create the box plot
    fig, ax = plt.subplots(figsize=(24, 10))
    
    # Get unique policy variants and thread counts
    policy_variants = sorted(plot_data['policy_variant'].unique())
    thread_counts = sorted(plot_data['thread_count'].unique())
    
    print(f"\nPolicy variants for plotting: {policy_variants}")
    print(f"Thread counts for plotting: {thread_counts}")
    
    # Black/Grey/White color scheme with patterns for different thread counts
    # 1 thread: Light grey with no pattern
    # 4 threads: Dark grey with diagonal hatch
    # 8 threads: Black with cross hatch
    thread_color_map = {1: 'lightgrey', 4: 'darkgrey', 8: 'black'}
    thread_pattern_map = {1: '', 4: '///', 8: 'xxx'}
    thread_edge_map = {1: 'black', 4: 'black', 8: 'white'}
    
    # Prepare data grouped by policy variant
    box_data = []
    box_positions = []
    box_colors = []
    box_patterns = []
    box_edge_colors = []
    scatter_positions = []
    scatter_data = []
    
    # Track positions for x-axis labels
    policy_x_positions = []  # Center position for each policy group
    thread_x_positions = []  # Position for each thread bar
    thread_labels = []  # Thread count labels (1, 4, 8)
    
    x_pos = 0
    bar_width = 0.8
    group_spacing = 4.5  # Space between policy groups
    
    for variant in policy_variants:
        group_start = x_pos
        
        # For each thread count in this policy variant
        for thread_count in thread_counts:
            # Get data for this variant and thread count
            data = plot_data[
                (plot_data['policy_variant'] == variant) & 
                (plot_data['thread_count'] == thread_count)
            ]['calculated_throughput'].values
            
            if len(data) > 0:
                box_data.append(data)
                box_positions.append(x_pos)
                box_colors.append(thread_color_map[thread_count])
                box_patterns.append(thread_pattern_map[thread_count])
                box_edge_colors.append(thread_edge_map[thread_count])
                
                # Store data for scatter plot overlay
                scatter_positions.append([x_pos] * len(data))
                scatter_data.append(data)
                
                # Store thread label position
                thread_x_positions.append(x_pos)
                thread_labels.append(str(thread_count))
            else:
                # Still add position for missing data to maintain spacing
                thread_x_positions.append(x_pos)
                thread_labels.append('')
            
            x_pos += 1
        
        # Calculate center position for policy label
        group_center = group_start + (len(thread_counts) - 1) / 2
        policy_x_positions.append(group_center)
        
        # Add spacing between policy groups
        x_pos += (group_spacing - len(thread_counts))
    
    # Create box plot
    bp = ax.boxplot(
        box_data,
        positions=box_positions,
        widths=bar_width,
        patch_artist=True,
        showfliers=False,  # Don't show outliers as separate markers
        notch=False,
        medianprops=dict(color='red', linewidth=2.5),
        boxprops=dict(linewidth=1.5),
        whiskerprops=dict(linewidth=1.5, color='black'),
        capprops=dict(linewidth=1.5, color='black')
    )
    
    # Color and pattern the boxes
    for patch, color, pattern, edge_color in zip(bp['boxes'], box_colors, box_patterns, box_edge_colors):
        patch.set_facecolor(color)
        patch.set_hatch(pattern)
        patch.set_edgecolor(edge_color)
        patch.set_linewidth(1.5)
        patch.set_alpha(0.8)
    
    # Overlay individual data points
    for positions, data in zip(scatter_positions, scatter_data):
        ax.scatter(positions, data, alpha=0.6, s=20, color='black', zorder=3, marker='o')
    
    # Calculate and add percentage gain annotations
    # For each policy variant, calculate gains: 4T over 1T, and 8T over 4T
    for i, variant in enumerate(policy_variants):
        # Get median throughput for each thread count
        medians = {}
        for thread_count in thread_counts:
            data = plot_data[
                (plot_data['policy_variant'] == variant) & 
                (plot_data['thread_count'] == thread_count)
            ]['calculated_throughput']
            
            if len(data) > 0:
                medians[thread_count] = data.median()
            else:
                medians[thread_count] = None
        
        # Calculate gains
        gain_4_over_1 = None
        gain_8_over_4 = None
        
        #if medians.get(1) is not None and medians.get(4) is not None:
        #    gain_4_over_1 = ((medians[4] - medians[1]) / medians[1]) * 100
        
        #if medians.get(4) is not None and medians.get(8) is not None:
        #    gain_8_over_4 = ((medians[8] - medians[4]) / medians[4]) * 100
        
        # Create annotation text
        annotation_lines = []
        if gain_4_over_1 is not None:
            sign_4 = '+' if gain_4_over_1 >= 0 else ''
            annotation_lines.append(f"4T: {sign_4}{gain_4_over_1:.1f}%")
        
        if gain_8_over_4 is not None:
            sign_8 = '+' if gain_8_over_4 >= 0 else ''
            annotation_lines.append(f"8T: {sign_8}{gain_8_over_4:.1f}%")
        
        # Add annotation above the policy group if we have any gains to show
        if annotation_lines:
            annotation_text = '\n'.join(annotation_lines)
            
            # Get the maximum y value for this policy group to position annotation
            max_y = 0
            for thread_count in thread_counts:
                data = plot_data[
                    (plot_data['policy_variant'] == variant) & 
                    (plot_data['thread_count'] == thread_count)
                ]['calculated_throughput']
                if len(data) > 0:
                    max_y = max(max_y, data.max())
            
            # Position annotation above the highest point
            y_offset = max_y * 0.05  # 5% above the max value
            ax.text(
                policy_x_positions[i], 
                max_y + y_offset,
                annotation_text,
                ha='center',
                va='bottom',
                fontsize=32,
                fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='gray', alpha=0.8)
            )
    
    # Create legend with patterns
    legend_elements = [
        plt.Rectangle((0, 0), 1, 1, 
                     facecolor=thread_color_map[tc], 
                     hatch=thread_pattern_map[tc], 
                     edgecolor=thread_edge_map[tc],
                     linewidth=1.5,
                     alpha=1,
                     label=f'{tc} thread(s)')
        for tc in thread_counts
    ]
    ax.legend(handles=legend_elements, loc='upper right', fontsize=32, frameon=True, 
             fancybox=True, shadow=False, framealpha=0.8)
    
    # Set labels and title
    #ax.set_xlabel('Replacement Policy Variants', fontsize=36)
    ax.set_ylabel('Throughpt (ops/sec)', fontsize=36)
    #ax.set_title(
    #    'Cache Policy Performance: Throughput Distribution by Policy Variant and Thread Count\n'
    #    '(Distribution across all operations, cache sizes, storage types, and test runs)',
    #    fontsize=28, fontweight='bold', pad=20
    #)
    
    # Set primary x-axis ticks for policy names
    ax.set_xticks(policy_x_positions)
    display_names = [get_display_name(variant) for variant in policy_variants]
    ax.set_xticklabels(display_names, fontsize=32, rotation=90, ha='right')
    
    # Format y-axis to show values in scientific notation
    ax.ticklabel_format(style='scientific', axis='y', scilimits=(0,0))
    ax.tick_params(axis='y', labelsize=32)
    
    # Increase the size of the exponent (offset text)
    ax.yaxis.get_offset_text().set_fontsize(28)
    
    # Add grid for better readability
    ax.grid(True, alpha=0.3, axis='y', color='grey')
    ax.set_axisbelow(True)
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the plot
    script_dir = Path(__file__).parent
    output_path = script_dir / "plot_figure5.png"
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
    print("SUMMARY STATISTICS")
    print("="*80)
    
    for variant in policy_variants:
        print(f"\n{variant}:")
        for thread_count in thread_counts:
            data = plot_data[
                (plot_data['policy_variant'] == variant) & 
                (plot_data['thread_count'] == thread_count)
            ]['calculated_throughput']
            
            if len(data) > 0:
                print(f"  {thread_count} threads:")
                print(f"    Count: {len(data)}")
                print(f"    Mean:  {data.mean():,.2f} ops/sec")
                print(f"    Median: {data.median():,.2f} ops/sec")
                print(f"    Std:   {data.std():,.2f} ops/sec")
                print(f"    Min:   {data.min():,.2f} ops/sec")
                print(f"    Max:   {data.max():,.2f} ops/sec")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Plot cache policy performance across different configurations.'
    )
    parser.add_argument(
        '--csv',
        type=str,
        default=None,
        help='Path to the CSV file with benchmark results. Defaults to data/combined_benchmark_results_with_perf_20251002_072156.csv'
    )
    args = parser.parse_args()
    main(csv_path=args.csv)