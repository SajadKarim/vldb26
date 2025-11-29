#!/usr/bin/env python3
"""
Script to plot YCSB workload performance across different cache policy configurations, separated by storage type.
Creates a 2x3 subplot grid showing throughput for each policy variant with separate boxes for each storage device.
Each subplot represents a different YCSB workload (ycsb_a through ycsb_f).
Only plots data for 1 thread.
Uses different colors/patterns for different storage types.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.gridspec as gridspec
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
        'CLOCK_clock_buffer_enabled_and_relaxed': 'CLOCK\n(Buffered+\nNon-strict)',
        'LRU_default': 'LRU',
        'LRU_relaxed': 'LRU\n(Non-strict)',
        'LRU_lru_metadata_update_in_order': 'LRU\n(Ordered)',
        'LRU_lru_metadata_update_in_order_and_relaxed': 'LRU\n(Ordered+\nNon-strict)',
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

def calculate_max_cv_across_cache_sizes(plot_data, policy_variants, storage_types, workload, cache_sizes):
    """
    Calculate max CV across all cache sizes for each policy variant.
    Returns a dictionary: {policy_variant: max_cv}
    """
    max_cvs = {}
    
    for variant in policy_variants:
        cvs = []
        
        # Calculate CV for each cache size
        for cache_size in cache_sizes:
            storage_means = []
            
            for storage_type in storage_types:
                data = plot_data[
                    (plot_data['policy_variant'] == variant) & 
                    (plot_data['operation'] == workload) &
                    (plot_data['cache_size'] == cache_size) &
                    (plot_data['storage_type'] == storage_type)
                ]['calculated_throughput'].values
                
                if len(data) > 0:
                    storage_mean = np.mean(data)
                    storage_means.append(storage_mean)
            
            # Calculate CV for this cache size
            if len(storage_means) > 0:
                mean_of_means = np.mean(storage_means)
                std_of_means = np.std(storage_means, ddof=1) if len(storage_means) > 1 else 0
                if mean_of_means > 0:
                    cv = (std_of_means / mean_of_means) * 100
                    cvs.append(cv)
        
        # Store the maximum CV across all cache sizes
        if len(cvs) > 0:
            max_cvs[variant] = max(cvs)
    
    return max_cvs

def export_workload_data_to_csv(plot_data, workload, policy_variants, storage_types, cache_sizes, max_cvs, output_dir):
    """
    Export all plotted data for a workload to a CSV file.
    Includes mean, percentiles (25th, 50th, 75th) for each policy variant and storage type.
    Also includes CV values at the end.
    """
    csv_rows = []
    
    # Header
    csv_rows.append(['Workload', workload])
    csv_rows.append([])  # Empty row
    csv_rows.append(['Policy Variant', 'Storage Type', 'Cache Size', 'Mean Throughput', 
                     'Std Dev', '25th Percentile', '50th Percentile (Median)', 
                     '75th Percentile', 'Min', 'Max', 'Count'])
    
    # Data for each policy variant and storage type
    for variant in policy_variants:
        for storage_type in storage_types:
            for cache_size in cache_sizes:
                data = plot_data[
                    (plot_data['policy_variant'] == variant) & 
                    (plot_data['operation'] == workload) &
                    (plot_data['storage_type'] == storage_type) &
                    (plot_data['cache_size'] == cache_size)
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
                    
                    csv_rows.append([
                        variant, storage_type, cache_size, 
                        f"{mean_val:.2f}", f"{std_val:.2f}",
                        f"{p25:.2f}", f"{p50:.2f}", f"{p75:.2f}",
                        f"{min_val:.2f}", f"{max_val:.2f}", count
                    ])
    
    # Add empty rows before CV section
    csv_rows.append([])
    csv_rows.append([])
    csv_rows.append(['Coefficient of Variation (CV) - Storage Sensitivity'])
    csv_rows.append(['Policy Variant', 'Max CV Across Cache Sizes (%)'])
    
    # Add CV values
    for variant in policy_variants:
        if variant in max_cvs:
            csv_rows.append([variant, f"{max_cvs[variant]:.2f}"])
    
    # Write to CSV file
    csv_filename = output_dir / f"{workload}_data.csv"
    
    with open(csv_filename, 'w') as f:
        for row in csv_rows:
            f.write(','.join(str(cell) for cell in row) + '\n')
    
    print(f"  Exported data to: {csv_filename}")
    return csv_filename

def plot_workload_subplot(ax, plot_data, workload, policy_variants, storage_types, cache_sizes, max_cvs, global_min, global_max):
    """
    Create a box plot for a specific workload with separate boxes for each storage type.
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
    
    x_pos = 0
    bar_width = 0.8
    group_spacing = 5.0  # Space between policy groups
    
    for variant in policy_variants:
        group_start = x_pos
        
        # For each storage type in this policy variant
        for storage_type in storage_types:
            # Get data for this variant, workload, and storage type (thread_count is always 1)
            data = plot_data[
                (plot_data['policy_variant'] == variant) & 
                (plot_data['operation'] == workload) &
                (plot_data['storage_type'] == storage_type)
            ]['calculated_throughput'].values
            
            if len(data) > 0:
                box_data.append(data)
                box_positions.append(x_pos)
                box_colors.append(storage_color_map[storage_type])
                box_patterns.append(storage_pattern_map[storage_type])
                box_edge_colors.append(storage_edge_map[storage_type])
                
                # Store data for scatter plot overlay
                scatter_positions.append([x_pos] * len(data))
                scatter_data.append(data)
            
            x_pos += 1
        
        # Calculate center position for policy label
        group_center = group_start + (len(storage_types) - 1) / 2
        policy_x_positions.append(group_center)
        
        # Add spacing between policy groups
        x_pos += (group_spacing - len(storage_types))
    
    # Create box plot
    if len(box_data) > 0:
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
        
        # Add vertical reference lines for best-performing policy variant of each storage type
        # We need to track which box index corresponds to which storage type and position
        # Build a mapping from (storage_type, variant) to box index
        box_index_map = {}
        box_idx = 0
        
        x_pos = 0
        for variant in policy_variants:
            for storage_type in storage_types:
                data = plot_data[
                    (plot_data['policy_variant'] == variant) & 
                    (plot_data['operation'] == workload) &
                    (plot_data['storage_type'] == storage_type)
                ]['calculated_throughput'].values
                
                if len(data) > 0:
                    median_val = np.median(data)
                    box_index_map[(storage_type, variant)] = {
                        'box_idx': box_idx,
                        'x_pos': x_pos,
                        'median': median_val,
                        'data': data
                    }
                    box_idx += 1
                
                x_pos += 1
            
            # Add spacing between policy groups
            x_pos += (group_spacing - len(storage_types))
        
        # For each storage type, find the best-performing box and draw a vertical line
        for storage_type in storage_types:
            # Find all boxes for this storage type
            storage_boxes = {k: v for k, v in box_index_map.items() if k[0] == storage_type}
            
            if storage_boxes:
                # Find the box with the highest median
                best_key = max(storage_boxes.keys(), key=lambda k: storage_boxes[k]['median'])
                best_info = storage_boxes[best_key]
                best_box_idx = best_info['box_idx']
                best_x_pos = best_info['x_pos']
                
                # Draw vertical line spanning the entire plot height (from bottom to top of y-axis)
                print(f"  Drawing vertical line for {storage_type} at x={best_x_pos}")
                ax.axvline(x=best_x_pos, color='green', linestyle='--', linewidth=2.5, zorder=10, alpha=0.9)
    
    # Add Coefficient of Variation (CV) annotations using max CV across cache sizes
    # CV measures storage sensitivity: how much performance varies across storage devices
    # Alternate annotations above and below to prevent overlap
    for i, variant in enumerate(policy_variants):
        if variant in max_cvs:
            max_cv = max_cvs[variant]
            
            # Find the max throughput for this policy group for positioning
            max_y = 0
            for storage_type in storage_types:
                data = plot_data[
                    (plot_data['policy_variant'] == variant) & 
                    (plot_data['operation'] == workload) &
                    (plot_data['storage_type'] == storage_type)
                ]['calculated_throughput'].values
                
                if len(data) > 0:
                    max_y = max(max_y, data.max())
            
            # Create annotation text with max CV
            annotation_text = f"CV: {max_cv:.1f}%"
            
            # Calculate center position for this policy group
            group_start = i * (len(storage_types) + (5.0 - len(storage_types)))
            group_center = group_start + (len(storage_types) - 1) / 2
            
            # Hardcoded zigzag positions near the top of the plot
            # Alternating between two fixed y-positions for clear visibility
            if i % 2 == 0:
                # Even index: place at higher position
                y_pos = 2755000  # High position (reduced by 5% from 2900000)
                va = 'bottom'
            else:
                # Odd index: place at lower position
                y_pos = 2555000  # Lower position for zigzag effect
                va = 'bottom'
            
            ax.text(
                group_center, 
                y_pos,
                annotation_text,
                ha='center',
                va=va,
                fontsize=24,
                fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='gray', alpha=0.8)
            )
    
    # Set labels
    ax.set_ylabel('Throughput (ops/sec)', fontsize=30)
    
    # Set primary x-axis ticks for policy names
    ax.set_xticks(policy_x_positions)
    display_names = [get_display_name(variant) for variant in policy_variants]
    ax.set_xticklabels(display_names, fontsize=26, rotation=90, ha='right')
    
    # Format y-axis to show values in scientific notation
    ax.ticklabel_format(style='scientific', axis='y', scilimits=(0,0))
    ax.tick_params(axis='y', labelsize=26)
    
    # Increase the size of the exponent (offset text)
    ax.yaxis.get_offset_text().set_fontsize(24)
    
    # Add grid for better readability
    ax.grid(True, alpha=0.3, axis='y', color='grey')
    ax.set_axisbelow(True)
    
    # Set x-axis limits with padding to prevent boxes from touching borders
    if len(box_positions) > 0:
        x_min = min(box_positions) - bar_width
        x_max = max(box_positions) + bar_width
        ax.set_xlim(x_min, x_max)
    
    # Add workload title to subplot
    ax.set_title(get_workload_display_name(workload), fontsize=30, pad=10)
    
    return storage_color_map, storage_pattern_map, storage_edge_map

def main(csv_path=None):
    # Get data file from parameter or use default
    if csv_path is None:
        # Use default path relative to script location
        script_dir = Path(__file__).parent
        csv_path = script_dir / 'data' / 'combined_ycsb_benchmark_results_with_perf_20251004_051834.csv'
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
    
    # Get unique policy variants and storage types
    policy_variants = sorted(plot_data['policy_variant'].unique())
    # Define storage types in the desired order: NVDIMM, NVM, SSD NVMe
    storage_types = ['VolatileStorage', 'PMemStorage', 'FileStorage']
    
    # Get unique workloads and cache sizes
    workloads = sorted(plot_data['operation'].unique())
    cache_sizes = sorted(plot_data['cache_size'].unique())
    
    print(f"\nPolicy variants for plotting: {policy_variants}")
    print(f"Storage types for plotting: {storage_types}")
    print(f"Workloads for plotting: {workloads}")
    print(f"Cache sizes for plotting: {cache_sizes}")
    
    # Create 2x3 subplot grid (6 workloads) with custom spacing using gridspec
    fig = plt.figure(figsize=(36, 14))
    gs = gridspec.GridSpec(2, 3, figure=fig, 
                          left=0.02, right=0.98, top=0.98, bottom=0.02,
                          wspace=0.05, hspace=0.10)
    
    # Create subplots from gridspec
    axes_flat = []
    for i in range(2):
        for j in range(3):
            axes_flat.append(fig.add_subplot(gs[i, j]))
    
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
    
    # Create output directory for CSV files
    output_dir = Path(__file__).parent
    csv_output_dir = output_dir / "workload_data_csv"
    csv_output_dir.mkdir(exist_ok=True)
    print(f"\nCSV files will be saved to: {csv_output_dir}")
    
    # Plot each subplot for each workload
    for idx, workload in enumerate(workloads):
        print(f"\nPlotting subplot for workload: {workload}")
        
        # Calculate max CV across cache sizes for this workload
        max_cvs = calculate_max_cv_across_cache_sizes(
            plot_data, policy_variants, storage_types, workload, cache_sizes
        )
        
        # Print max CVs for this workload
        print(f"\nMax CV across cache sizes (Workload: {workload}):")
        print(f"{'─'*80}")
        print(f"{'Policy Variant':<45} {'Max CV':<15}")
        print(f"{'─'*80}")
        for variant in policy_variants:
            if variant in max_cvs:
                print(f"{variant:<45} {max_cvs[variant]:>10.1f}%")
        
        # Export data to CSV
        export_workload_data_to_csv(
            plot_data, workload, policy_variants, storage_types, 
            cache_sizes, max_cvs, csv_output_dir
        )
        
        storage_color_map, storage_pattern_map, storage_edge_map = plot_workload_subplot(
            axes_flat[idx], 
            plot_data, 
            workload, 
            policy_variants, 
            storage_types,
            cache_sizes,
            max_cvs,
            global_min,
            global_max
        )
        
        # Set the same y-axis limits for all subplots
        axes_flat[idx].set_ylim(global_min, global_max)
    
    # Remove x-axis labels and ticks from top row subplots
    for idx in range(3):  # First row (indices 0, 1, 2)
        axes_flat[idx].set_xlabel('')
        axes_flat[idx].set_xticklabels([])
        axes_flat[idx].tick_params(axis='x', which='both', bottom=False, top=False)
    
    # Add x-axis label to bottom row subplots
    #for idx in range(3, 6):  # Second row (indices 3, 4, 5)
    #    if idx == 4:
    #        axes_flat[idx].set_xlabel('Replacement Policy Variants', fontsize=30)
    #    else:
    #        axes_flat[idx].set_xlabel('', fontsize=30)
    # Remove y-axis labels and ticks from 2nd and 3rd columns
    for idx in [1, 2, 4, 5]:  # Columns 2 and 3 (indices 1, 2 in row 1; 4, 5 in row 2)
        axes_flat[idx].set_ylabel('')
        axes_flat[idx].set_yticklabels([])
        axes_flat[idx].tick_params(axis='y', which='both', left=False, right=False)
    
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
    
    # Add legend to the 6th subplot (bottom-right, index 5) in a single column
    # Position it lower using bbox_to_anchor and make background transparent with grey border
    legend = axes_flat[5].legend(handles=legend_elements, loc='upper right', fontsize=26, frameon=True, 
                                 ncol=1, framealpha=0.8, bbox_to_anchor=(1.0, 0.68))
    legend.get_frame().set_edgecolor('grey')
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the plot
    output_dir = Path(__file__).parent
    output_path = output_dir / "plot_figure10.png"
    
    print(f"\nSaving plot to: {output_path}")
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Plot saved successfully!")
    
    # Also save as PDF for better quality in papers
    output_path_pdf = output_dir / "plot_figure10.pdf"
    print(f"Saving PDF to: {output_path_pdf}")
    plt.savefig(output_path_pdf, bbox_inches='tight')
    print(f"PDF saved successfully!")
    
    plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate YCSB workload performance plots across different cache policies.'
    )
    parser.add_argument(
        '--csv',
        type=str,
        default=None,
        help='Path to the CSV file with YCSB benchmark results. Defaults to data/combined_ycsb_benchmark_results_with_perf_20251004_051834.csv'
    )
    args = parser.parse_args()
    main(csv_path=args.csv)