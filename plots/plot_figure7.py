#!/usr/bin/env python3
"""
Script to plot cache policy performance across different configurations, separated by storage type.
AGGREGATED VERSION: Shows all cache sizes together in one plot, but calculates max CV, max Range and max Ratio
across cache sizes separately for each policy variant.

Creates a 3x1 subplot grid showing throughput for each policy variant with separate boxes for each storage device.
Top subplot: Thread count = 1
Middle subplot: Thread count = 4
Bottom subplot: Thread count = 8
Uses different colors/patterns for different storage types.
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

def calculate_max_metrics_across_cache_sizes(plot_data, policy_variants, storage_types, thread_count, cache_sizes):
    """
    Calculate max CV, max Range and max Ratio across all cache sizes for each policy variant.
    Returns a dictionary: {policy_variant: {'max_cv': X, 'max_range': Y, 'max_ratio': Z}}
    """
    max_metrics = {}
    
    for variant in policy_variants:
        cvs = []
        ranges = []
        ratios = []
        
        # Calculate CV, Range and Ratio for each cache size
        for cache_size in cache_sizes:
            storage_means = []
            
            for storage_type in storage_types:
                data = plot_data[
                    (plot_data['policy_variant'] == variant) & 
                    (plot_data['thread_count'] == thread_count) &
                    (plot_data['cache_size'] == cache_size) &
                    (plot_data['storage_type'] == storage_type)
                ]['calculated_throughput'].values
                
                if len(data) > 0:
                    storage_mean = np.mean(data)
                    storage_means.append(storage_mean)
            
            # Calculate metrics for this cache size
            if len(storage_means) > 0:
                # Calculate CV (Coefficient of Variation)
                mean_of_means = np.mean(storage_means)
                std_of_means = np.std(storage_means, ddof=1) if len(storage_means) > 1 else 0
                if mean_of_means > 0:
                    cv = (std_of_means / mean_of_means) * 100
                    cvs.append(cv)
                
                # Calculate Range and Ratio
                max_storage = max(storage_means)
                min_storage = min(storage_means)
                
                if min_storage > 0:
                    perf_range = ((max_storage - min_storage) / min_storage) * 100
                    perf_ratio = max_storage / min_storage
                    
                    ranges.append(perf_range)
                    ratios.append(perf_ratio)
        
        # Store the maximum values across all cache sizes
        if len(cvs) > 0 and len(ranges) > 0:
            max_metrics[variant] = {
                'max_cv': max(cvs),
                'max_range': max(ranges),
                'max_ratio': max(ratios)
            }
    
    return max_metrics

def plot_thread_subplot(ax, plot_data, thread_count, policy_variants, storage_types, cache_sizes, max_metrics):
    """
    Create a box plot for a specific thread count with ALL cache sizes aggregated.
    Shows max Range and max Ratio calculated across cache sizes.
    """
    # Black/Grey/White color scheme with patterns for different storage types
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
            # Get data for this variant, thread count, and storage type (ALL cache sizes)
            data = plot_data[
                (plot_data['policy_variant'] == variant) & 
                (plot_data['thread_count'] == thread_count) &
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
    # Build a mapping from (storage_type, variant) to box index and median
    box_index_map = {}
    box_idx = 0
    
    x_pos = 0
    for variant in policy_variants:
        for storage_type in storage_types:
            data = plot_data[
                (plot_data['policy_variant'] == variant) & 
                (plot_data['thread_count'] == thread_count) &
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
            best_x_pos = best_info['x_pos']
            
            # Draw vertical line spanning the entire plot height
            ax.axvline(x=best_x_pos, color='green', linestyle='--', linewidth=2.5, zorder=10, alpha=0.9)
    
    # Add annotations with CV only
    for i, variant in enumerate(policy_variants):
        if variant in max_metrics:
            max_cv = max_metrics[variant]['max_cv']
            
            # Create annotation text with CV only
            annotation_text = f"CV: {max_cv:.1f}%"
            
            # Calculate center position for this policy group
            group_start = i * (len(storage_types) + (5.0 - len(storage_types)))
            group_center = group_start + (len(storage_types) - 1) / 2
            
            # Find the highest point in this policy group for positioning
            max_y = 0
            for storage_type in storage_types:
                data = plot_data[
                    (plot_data['policy_variant'] == variant) & 
                    (plot_data['thread_count'] == thread_count) &
                    (plot_data['storage_type'] == storage_type)
                ]['calculated_throughput'].values
                
                if len(data) > 0:
                    max_y = max(max_y, data.max())
            
            # Position annotation above the highest point
            y_offset = max_y * 0.05  # 5% above the max value
            ax.text(
                group_center, 
                max_y + y_offset,
                annotation_text,
                ha='left',
                va='bottom',
                fontsize=28,
                fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='gray', alpha=0.4)
            )
    
    # Set labels
    ax.set_ylabel('Throughput (ops/sec)', fontsize=36)
    
    # Set primary x-axis ticks for policy names
    ax.set_xticks(policy_x_positions)
    display_names = [get_display_name(variant) for variant in policy_variants]
    ax.set_xticklabels(display_names, fontsize=32, rotation=90, ha='right')
    
    # Format y-axis to show values in scientific notation
    ax.ticklabel_format(style='scientific', axis='y', scilimits=(0,0))
    ax.tick_params(axis='y', labelsize=32)
    
    # Increase the size of the exponent (offset text)
    ax.yaxis.get_offset_text().set_fontsize(32)
    
    # Add grid for better readability
    ax.grid(True, alpha=0.3, axis='y', color='grey')
    ax.set_axisbelow(True)
    
    return storage_color_map, storage_pattern_map, storage_edge_map

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
    
    # Prepare data for plotting - keep all columns including test_run_id
    plot_data = df[['policy_variant', 'thread_count', 'storage_type', 'cache_size', 'operation', 'test_run_id', 'calculated_throughput']].copy()
    
    print(f"\nData points for plotting: {len(plot_data)}")
    print(f"\nSample data:")
    print(plot_data.head(20))
    
    # Get unique policy variants, storage types, and cache sizes
    policy_variants = sorted(plot_data['policy_variant'].unique())
    # Define storage types in the desired order: NVDIMM, NVM, SSD NVMe
    storage_types = ['VolatileStorage', 'PMemStorage', 'FileStorage']
    # Get cache sizes
    cache_sizes = sorted(plot_data['cache_size'].unique())
    
    print(f"\nPolicy variants for plotting: {policy_variants}")
    print(f"Storage types for plotting: {storage_types}")
    print(f"Cache sizes for plotting: {cache_sizes}")
    
    # Thread counts to plot (1, 4, and 8)
    thread_counts_to_plot = [1, 4, 8]
    
    print(f"\n{'='*100}")
    print("CREATING AGGREGATED PLOT (All Cache Sizes Combined)")
    print("Max CV, Max Range and Max Ratio calculated separately across cache sizes")
    print(f"{'='*100}")
    
    # Create 3x1 subplot grid (one for each thread count)
    fig, axes = plt.subplots(3, 1, figsize=(26, 28))
    
    # Plot each subplot
    for idx, thread_count in enumerate(thread_counts_to_plot):
        print(f"\nPlotting subplot for thread count: {thread_count}")
        
        # Calculate max metrics across cache sizes for this thread count
        max_metrics = calculate_max_metrics_across_cache_sizes(
            plot_data, policy_variants, storage_types, thread_count, cache_sizes
        )
        
        # Print max metrics for this thread count
        print(f"\nMax CV, Range and Ratio across cache sizes (Thread count: {thread_count}):")
        print(f"{'─'*120}")
        print(f"{'Policy Variant':<45} {'Max CV':<15} {'Max Range':<15} {'Max Ratio':<15}")
        print(f"{'─'*120}")
        for variant in policy_variants:
            if variant in max_metrics:
                print(f"{variant:<45} {max_metrics[variant]['max_cv']:>10.1f}%     {max_metrics[variant]['max_range']:>10.1f}%     {max_metrics[variant]['max_ratio']:>10.2f}x")
        
        storage_color_map, storage_pattern_map, storage_edge_map = plot_thread_subplot(
            axes[idx], 
            plot_data, 
            thread_count,
            policy_variants, 
            storage_types,
            cache_sizes,
            max_metrics
        )
        
        # Remove x-axis ticks and labels from first and second subplots
        #if idx < 2:  # First two subplots (index 0 and 1)
        #    axes[idx].set_xticklabels([])
        #    axes[idx].set_xlabel('')
        #else:  # Last subplot (index 2)
            #axes[idx].set_xlabel('Replacement Policy Variants', fontsize=36)
        
        # Add thread count label on the right side of each subplot
        axes[idx].text(
            1.02, 0.5, f'{thread_count} Thread{"s" if thread_count > 1 else ""}',
            transform=axes[idx].transAxes,
            fontsize=36,
            rotation=270,
            va='center',
            ha='left'
        )
    
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
    
    # Add legend to the first subplot in the middle empty space
    axes[0].legend(handles=legend_elements, bbox_to_anchor=(0.5, 1.0), fontsize=32, frameon=True, 
                   ncol=1, framealpha=0.9)
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout(rect=[0, 0.01, 0.98, 1.0])
    
    # Save the plot
    script_dir = Path(__file__).parent
    output_path = script_dir / "plot_figure7.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {output_path}")
    
    # Also save as PDF for better quality
    output_path_pdf = output_path.with_suffix('.pdf')
    plt.savefig(output_path_pdf, bbox_inches='tight')
    print(f"Plot also saved as PDF: {output_path_pdf}")
    
    # Close the plot
    plt.close()
    
    print("\n" + "="*100)
    print("AGGREGATED PLOT COMPLETE")
    print("="*100)
    print("\nKey Features:")
    print("  - Box plots show data from ALL cache sizes (2%, 10%, 25%) combined")
    print("  - Max CV: Highest coefficient of variation across storage types (across all cache sizes)")
    print("  - Max Range: Highest % difference across storage types (across all cache sizes)")
    print("  - Max Ratio: Highest speedup ratio across storage types (across all cache sizes)")
    print("  - Yellow boxes highlight worst-case storage sensitivity")
    print("="*100)
    
    # ========================================
    # EXPORT DATA TO CSV FILES
    # ========================================
    print("\n" + "="*100)
    print("EXPORTING DATA TO CSV FILES")
    print("="*100)
    
    csv_output_dir = Path("/home/skarim/Code/haldendb_ex/haldendb_pvt_imp/benchmark/opt_plots")
    
    # 1. Export all aggregated throughput data (raw data used in box plots)
    print("\n1. Exporting aggregated throughput data...")
    aggregated_data_rows = []
    
    for thread_count in thread_counts_to_plot:
        for variant in policy_variants:
            for storage_type in storage_types:
                # Get all throughput values for this combination (all cache sizes)
                data = plot_data[
                    (plot_data['policy_variant'] == variant) & 
                    (plot_data['thread_count'] == thread_count) &
                    (plot_data['storage_type'] == storage_type)
                ]['calculated_throughput'].values
                
                # Add each data point as a row
                for throughput_value in data:
                    aggregated_data_rows.append({
                        'thread_count': thread_count,
                        'policy_variant': variant,
                        'storage_type': get_storage_display_name(storage_type),
                        'throughput_ops_per_sec': throughput_value
                    })
    
    aggregated_df = pd.DataFrame(aggregated_data_rows)
    aggregated_csv_path = csv_output_dir / "aggregated_throughput_data.csv"
    aggregated_df.to_csv(aggregated_csv_path, index=False)
    print(f"   Saved: {aggregated_csv_path}")
    print(f"   Total data points: {len(aggregated_df)}")
    
    # 2. Export CV metrics for each policy variant and thread count
    print("\n2. Exporting CV metrics...")
    cv_metrics_rows = []
    
    for thread_count in thread_counts_to_plot:
        # Calculate max metrics for this thread count
        max_metrics = calculate_max_metrics_across_cache_sizes(
            plot_data, policy_variants, storage_types, thread_count, cache_sizes
        )
        
        for variant in policy_variants:
            if variant in max_metrics:
                cv_metrics_rows.append({
                    'thread_count': thread_count,
                    'policy_variant': variant,
                    'policy_display_name': get_display_name(variant).replace('\n', ' '),
                    'max_cv_percent': round(max_metrics[variant]['max_cv'], 2)
                })
    
    cv_metrics_df = pd.DataFrame(cv_metrics_rows)
    cv_metrics_csv_path = csv_output_dir / "cv_metrics.csv"
    cv_metrics_df.to_csv(cv_metrics_csv_path, index=False)
    print(f"   Saved: {cv_metrics_csv_path}")
    print(f"   Total CV metrics: {len(cv_metrics_df)}")
    
    # 3. Export summary statistics (mean, median, std, min, max) for each box
    print("\n3. Exporting summary statistics...")
    summary_stats_rows = []
    
    for thread_count in thread_counts_to_plot:
        for variant in policy_variants:
            for storage_type in storage_types:
                # Get all throughput values for this combination
                data = plot_data[
                    (plot_data['policy_variant'] == variant) & 
                    (plot_data['thread_count'] == thread_count) &
                    (plot_data['storage_type'] == storage_type)
                ]['calculated_throughput'].values
                
                if len(data) > 0:
                    summary_stats_rows.append({
                        'thread_count': thread_count,
                        'policy_variant': variant,
                        'policy_display_name': get_display_name(variant).replace('\n', ' '),
                        'storage_type': get_storage_display_name(storage_type),
                        'count': len(data),
                        'mean_throughput': round(np.mean(data), 2),
                        'median_throughput': round(np.median(data), 2),
                        'std_throughput': round(np.std(data, ddof=1), 2) if len(data) > 1 else 0,
                        'min_throughput': round(np.min(data), 2),
                        'max_throughput': round(np.max(data), 2),
                        'q1_throughput': round(np.percentile(data, 25), 2),
                        'q3_throughput': round(np.percentile(data, 75), 2)
                    })
    
    summary_stats_df = pd.DataFrame(summary_stats_rows)
    summary_stats_csv_path = csv_output_dir / "summary_statistics.csv"
    summary_stats_df.to_csv(summary_stats_csv_path, index=False)
    print(f"   Saved: {summary_stats_csv_path}")
    print(f"   Total summary entries: {len(summary_stats_df)}")
    
    print("\n" + "="*100)
    print("CSV EXPORT COMPLETE")
    print("="*100)
    print("\nGenerated files:")
    print(f"  1. {aggregated_csv_path.name} - All throughput data points")
    print(f"  2. {cv_metrics_csv_path.name} - CV values for each policy variant")
    print(f"  3. {summary_stats_csv_path.name} - Statistical summary for each box")
    print("="*100)

if __name__ == '__main__':
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