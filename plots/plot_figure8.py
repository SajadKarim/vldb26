#!/usr/bin/env python3
"""
Script to generate heatmap visualizations showing cache policy performance RANKINGS
across different storage types, operations, and thread counts.

Rankings are row-wise: Rank 1 = highest throughput (best), Rank N = lowest throughput (worst)

This script follows the heatmap style from analyze_cache_benchmark_results_combined_rankings.py:
- Inverted color scheme: White background for best performance, dark grey for worst
- Text colors adjusted accordingly (black on light, white on dark)
- Uses engaging/short names for policy variants
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import argparse
import sys
warnings.filterwarnings('ignore')

# Set matplotlib and seaborn style
plt.style.use('default')
sns.set_palette("husl")

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

def format_operation_name(operation):
    """Format operation name for display in heatmap."""
    operation_map = {
        'delete': 'Delete',
        'insert': 'Insert',
        'search_random': 'Search\n(Random)',
        'search_sequential': 'Search\n(Sequential)',
        'search_uniform': 'Search\n(Uniform)',
        'search_zipfian': 'Search\n(Zipfian)'
    }
    return operation_map.get(operation, operation)

def get_policy_display_name(policy_variant):
    """Get display name for policy variant."""
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

def format_storage_name(storage_type):
    """Format storage type name for display."""
    storage_map = {
        'VolatileStorage': 'NVDIMM',
        'PMemStorage': 'NVM',
        'FileStorage': 'SSD NVMe'
    }
    return storage_map.get(storage_type, storage_type)

def rank_row_values(values):
    """
    Rank values in a row where higher values get better (lower) ranks.
    Rank 1 = highest value (best performance)
    Rank N = lowest value (worst performance)
    
    Args:
        values: list of numeric values (can contain None for missing data)
    
    Returns:
        list of ranks (or np.nan for missing data)
    """
    # Create a list of (index, value) tuples, filtering out None values
    valid_values = [(i, v) for i, v in enumerate(values) if v is not None]
    
    if not valid_values:
        return [np.nan] * len(values)
    
    # Sort by value in descending order (highest value = rank 1)
    sorted_values = sorted(valid_values, key=lambda x: x[1], reverse=True)
    
    # Assign ranks
    ranks = [None] * len(values)
    for rank, (idx, _) in enumerate(sorted_values, 1):
        ranks[idx] = rank
    
    # Convert None to np.nan
    return [r if r is not None else np.nan for r in ranks]

def get_text_color(rank_value):
    """Return appropriate text color (black/white) based on rank value."""
    if pd.isna(rank_value):
        return 'black'
    # Color scheme matching reference: best ranks (1, 2, 3) get dark background → white text
    # Worst ranks get light/white background → black text
    if rank_value <= 3:  # Top 3 ranks get white text on dark background
        return 'white'
    else:  # Lower ranks get black text on light background
        return 'black'

def create_detailed_ranking_data(grouped_df, policy_variants, operations, storage_types, thread_count):
    """
    Create detailed ranking data (operation-wise) for a specific thread count.
    Returns DataFrame with rankings.
    """
    # Filter data for this thread count
    thread_data = grouped_df[grouped_df['thread_count'] == thread_count]
    
    # Create row labels: operation-storage combinations
    row_labels = []
    ranking_data = []
    
    for operation in operations:
        for storage_type in storage_types:
            # Collect throughput values for ranking
            throughput_values = []
            
            # For each policy variant, get the average throughput
            for pv in policy_variants:
                data = thread_data[
                    (thread_data['policy_variant'] == pv) &
                    (thread_data['storage_type'] == storage_type) &
                    (thread_data['operation'] == operation)
                ]
                
                if len(data) > 0:
                    avg_throughput = data['throughput'].values[0]
                    throughput_values.append(avg_throughput)
                else:
                    throughput_values.append(None)
            
            # Rank the values (higher throughput = better rank)
            ranks = rank_row_values(throughput_values)
            
            # Create row label
            storage_display = format_storage_name(storage_type)
            row_label = storage_display
            
            row_labels.append(row_label)
            ranking_data.append(ranks)
    
    # Convert to DataFrame
    ranking_df = pd.DataFrame(ranking_data, columns=policy_variants, index=row_labels)
    
    # Rename columns to display names
    ranking_df.columns = [get_policy_display_name(col) for col in ranking_df.columns]
    
    return ranking_df, operations

def create_summary_ranking_data(grouped_df, policy_variants, storage_types, thread_count):
    """
    Create summary ranking data (averaged across operations) for a specific thread count.
    Returns DataFrame with rankings.
    """
    # Filter data for this thread count
    thread_data = grouped_df[grouped_df['thread_count'] == thread_count]
    
    row_labels = []
    ranking_data = []
    
    for storage_type in storage_types:
        # Collect throughput values for ranking
        throughput_values = []
        
        # For each policy variant, calculate mean across all operations
        for pv in policy_variants:
            data = thread_data[
                (thread_data['policy_variant'] == pv) &
                (thread_data['storage_type'] == storage_type)
            ]
            
            if len(data) > 0:
                mean_throughput = data['throughput'].mean()
                throughput_values.append(mean_throughput)
            else:
                throughput_values.append(None)
        
        # Rank the values
        ranks = rank_row_values(throughput_values)
        
        storage_display = format_storage_name(storage_type)
        row_label = storage_display
        
        row_labels.append(row_label)
        ranking_data.append(ranks)
    
    # Convert to DataFrame
    ranking_df = pd.DataFrame(ranking_data, columns=policy_variants, index=row_labels)
    
    # Rename columns to display names
    ranking_df.columns = [get_policy_display_name(col) for col in ranking_df.columns]
    
    return ranking_df

def plot_heatmap_on_axis(ax, ranking_df, title, show_colorbar=True, show_xlabel=True, 
                         show_title=True, show_xticklabels=True, show_ylabel=True,
                         operations=None, storage_types=None, annot_size=14, global_max_rank=None):
    """
    Plot a heatmap on a given axis.
    
    Args:
        global_max_rank: If provided, use this as the maximum rank for color scaling
                        instead of the local maximum. This ensures consistent color
                        scales across multiple heatmaps.
    """
    # Create formatted annotations (show rank numbers or "N/A" for missing values)
    ranking_formatted = ranking_df.applymap(lambda x: f'{int(x)}' if not pd.isna(x) else 'N/A')
    
    # For color mapping, we want lower ranks (1, 2, 3) to be darker (like reference script)
    # Use global_max_rank if provided, otherwise use local maximum
    max_rank = global_max_rank if global_max_rank is not None else ranking_df.max().max()
    
    if not pd.isna(max_rank) and max_rank > 0:
        # Normalize: rank 1 -> 1.0 (darkest), rank N -> 0.0 (lightest/white)
        # This matches the reference script's approach
        ranking_normalized = (max_rank + 1 - ranking_df) / max_rank
        
        # Fill NaN values with 0.0 (white background) for missing data
        ranking_normalized = ranking_normalized.fillna(0.0)
    else:
        ranking_normalized = ranking_df * 0  # All zeros if no data
    
    # Create the heatmap
    cbar_kws = {'label': 'Rank (1=Best)', 'shrink': 0.8} if show_colorbar else None
    heatmap = sns.heatmap(ranking_normalized, 
               annot=ranking_formatted, 
               fmt='',
               cmap='Greys',  # Grey for best (rank 1), white for worst (matching reference)
               ax=ax,
               annot_kws={'size': annot_size, 'weight': 'bold'},
               cbar=show_colorbar,
               cbar_kws=cbar_kws,
               linewidths=0,  # Increased from 0.5 to 1.0 for better visibility
               linecolor='white',  # Changed from lightgrey to black for stronger contrast
               square=False,
               vmin=0, vmax=1)
    
    # Manually set text colors based on rank values for better contrast
    # Top 3 ranks: white text on dark grey background
    # Lower ranks: black text on light/white background
    for text in heatmap.texts:
        text_val = text.get_text()
        if text_val == 'N/A':
            # Set N/A text to black for visibility on white background
            text.set_color('black')
            text.set_weight('bold')
        elif text_val and text_val.isdigit():  # If not empty and is a number
            rank_val = int(text_val)
            # Inverted from previous: top ranks get white text, lower ranks get black
            text_color = 'white' if rank_val <= 3 else 'black'
            text.set_color(text_color)
    
    # Set title
    if show_title:
        ax.set_title(title, fontsize=26, pad=10)  # Increased from 18 to 24
    
    # Set labels
    #if show_xlabel:
    #    ax.set_xlabel('Replacement Policy Variants', fontsize=30)  # Increased from 14 to 24
    #else:
    #    ax.set_xlabel('')
    
    if show_ylabel:
        ax.set_ylabel('Storage Type', fontsize=30)  # Increased from 14 to 24
    else:
        ax.set_ylabel('')
    
    # Rotate x-axis labels for better readability
    ax.tick_params(axis='x', rotation=90, labelsize=26)  # Increased from 11 to 22
    ax.tick_params(axis='y', rotation=0, labelsize=26)  # Increased from 12 to 20
    
    # Hide x-axis tick labels if requested
    if not show_xticklabels:
        ax.set_xticklabels([])
    
    # Add separator lines for detailed heatmap (with operations)
    if operations is not None and storage_types is not None:
        separator_positions = []
        for i, operation in enumerate(operations):
            if i < len(operations) - 1:
                line_position = (i + 1) * len(storage_types)
                separator_positions.append(line_position)
        
        for boundary in separator_positions:
            ax.axhline(y=boundary, color='red', linewidth=1.5, alpha=0.7)
        
        # Add operation group labels on the right
        operation_positions = []
        for i, operation in enumerate(operations):
            group_start = i * len(storage_types)
            group_center = group_start + (len(storage_types) - 1) / 2
            operation_positions.append((group_center, format_operation_name(operation)))
        
        # Create a second y-axis for operation group labels
        ax2 = ax.twinx()
        ax2.set_ylim(ax.get_ylim())
        ax2.set_yticks([pos[0] for pos in operation_positions])
        ax2.set_yticklabels([pos[1] for pos in operation_positions], fontsize=26)  # Increased from 11 to 20
        ax2.set_ylabel('Operation Type', fontsize=30)  # Increased from 14 to 24
        
        # Hide spines of the twin axis to remove borders
        ax2.spines['top'].set_visible(False)
        ax2.spines['right'].set_visible(False)
        ax2.spines['bottom'].set_visible(False)
        ax2.spines['left'].set_visible(False)
    
    return heatmap

def create_combined_ranking_heatmap(grouped_df, policy_variants, operations, storage_types, thread_counts, output_dir):
    """
    Create a single combined figure with 4 heatmaps in vertical layout (4x1):
    1. Thread 1 - Detailed (operation-wise) - TOP
    2. Thread 1 - Summary (averaged) - Below detailed
    3. Thread 4 - Summary (averaged) - Below Thread 1 summary
    4. Thread 8 - Summary (averaged) - BOTTOM
    
    All heatmaps use a unified color scale based on the global maximum rank.
    """
    print(f"\nGenerating combined heatmap with 4 subplots in vertical layout...")
    
    # Create all dataframes first to calculate global maximum rank
    detailed_df, ops = create_detailed_ranking_data(grouped_df, policy_variants, operations, storage_types, thread_counts[0])
    summary_df_1 = create_summary_ranking_data(grouped_df, policy_variants, storage_types, thread_counts[0])
    summary_df_4 = create_summary_ranking_data(grouped_df, policy_variants, storage_types, thread_counts[1]) if len(thread_counts) > 1 else None
    summary_df_8 = create_summary_ranking_data(grouped_df, policy_variants, storage_types, thread_counts[2]) if len(thread_counts) > 2 else None
    
    # Calculate global maximum rank across all heatmaps for consistent color scaling
    all_dfs = [detailed_df, summary_df_1]
    if summary_df_4 is not None:
        all_dfs.append(summary_df_4)
    if summary_df_8 is not None:
        all_dfs.append(summary_df_8)
    
    global_max_rank = max([df.max().max() for df in all_dfs if not df.empty])
    print(f"Using global maximum rank: {global_max_rank} for consistent color scaling across all heatmaps")
    
    # Create figure with 4 subplots in vertical layout (4 rows, 1 column)
    # Height ratios: detailed plot needs more space (18 rows) vs summary plots (3 rows each)
    # Adjusted figure size for more compact layout with larger fonts
    fig = plt.figure(figsize=(18, 28))  # Reduced from (20, 32) for compactness
    
    # Create grid spec for 4x1 layout
    # Height ratios: detailed (18 rows) vs 3 summaries (3 rows each) = 6:1:1:1
    # Reduced hspace for more compact layout
    gs = fig.add_gridspec(4, 1, hspace=0.12, 
                          left=0.08, right=0.88, top=0.97, bottom=0.03,
                          height_ratios=[6, 1, 1, 1])
    
    # 1. Thread 1 - Detailed (TOP)
    # Remove title, remove x-tick labels, keep y-label
    ax1 = fig.add_subplot(gs[0, 0])
    plot_heatmap_on_axis(ax1, detailed_df, f'Per-Operation Rankings ({thread_counts[0]} Thread)', 
                        show_colorbar=False, show_xlabel=False, 
                        show_title=True, show_xticklabels=False, show_ylabel=True,
                        operations=ops, storage_types=storage_types, annot_size=22,  # Matches other heatmaps
                        global_max_rank=global_max_rank)
    
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.spines['bottom'].set_visible(False)
    ax1.spines['left'].set_visible(False)

    # 2. Thread 1 - Summary (below detailed)
    # Remove title, remove x-tick labels, remove y-label
    ax2 = fig.add_subplot(gs[1, 0])
    plot_heatmap_on_axis(ax2, summary_df_1, f'Cross-Operation Rankings ({thread_counts[0]} Thread)', 
                        show_colorbar=False, show_xlabel=False, 
                        show_title=True, show_xticklabels=False, show_ylabel=False,
                        annot_size=22, global_max_rank=global_max_rank)  # Increased from 16 to 22
    
    # 3. Thread 4 - Summary (middle)
    # Remove title, remove x-tick labels, remove y-label
    ax3 = fig.add_subplot(gs[2, 0])
    if len(thread_counts) > 1:
        plot_heatmap_on_axis(ax3, summary_df_4, f'Cross-Operation Rankings ({thread_counts[1]} Threads)', 
                            show_colorbar=False, show_xlabel=False,
                            show_title=True, show_xticklabels=False, show_ylabel=False,
                            annot_size=22, global_max_rank=global_max_rank)  # Increased from 16 to 22
    
    # 4. Thread 8 - Summary (BOTTOM)
    # Remove title, keep x-tick labels, remove y-label
    ax4 = fig.add_subplot(gs[3, 0])
    if len(thread_counts) > 2:
        plot_heatmap_on_axis(ax4, summary_df_8, f'Cross-Operation Rankings ({thread_counts[2]} Threads)', 
                            show_colorbar=False, show_xlabel=True,
                            show_title=True, show_xticklabels=True, show_ylabel=False,
                            annot_size=22, global_max_rank=global_max_rank)  # Increased from 16 to 22
    
    # Save both PNG and PDF versions
    png_path = output_dir / 'plot_figure8.png'
    pdf_path = output_dir / 'plot_figure8.pdf'
    
    plt.savefig(png_path, dpi=300, bbox_inches='tight')
    plt.savefig(pdf_path, dpi=300, bbox_inches='tight', format='pdf')
    plt.close()
    
    print(f"\nCombined ranking heatmap saved:")
    print(f"   PNG: {png_path}")
    print(f"   PDF: {pdf_path}")

def main(csv_path=None):
    """Main function to generate performance ranking heatmap."""
    # Define paths
    script_dir = Path(__file__).parent
    results_dir = script_dir.parent / 'results'
    output_dir = script_dir
    
    # Load the CSV file
    if csv_path is None:
        csv_file = script_dir / 'data' / 'combined_benchmark_results_with_perf_20251002_072156.csv'
    else:
        csv_file = Path(csv_path)
    
    if not csv_file.exists():
        print(f"Error: CSV file not found at {csv_file}")
        sys.exit(1)
    
    print(f"Loading data from: {csv_file}")
    df = pd.read_csv(csv_file)
    
    # Calculate throughput
    df['throughput'] = df.apply(lambda row: calculate_throughput(row['record_count'], row['time_us']), axis=1)
    
    # Create policy variant column
    df['policy_variant'] = df.apply(create_policy_variant, axis=1)
    
    # Group by relevant columns and calculate mean throughput
    grouped_df = df.groupby(['policy_variant', 'storage_type', 'operation', 'thread_count'])['throughput'].mean().reset_index()
    
    # Get unique values
    policy_variants = sorted(grouped_df['policy_variant'].unique())
    operations = sorted(grouped_df['operation'].unique())
    storage_types = ['VolatileStorage', 'PMemStorage', 'FileStorage']  # Fixed order
    thread_counts = sorted(grouped_df['thread_count'].unique())
    
    print(f"\nGenerating combined ranking heatmap...")
    print(f"   Policy variants: {len(policy_variants)}")
    print(f"   Operations: {len(operations)}")
    print(f"   Storage types: {len(storage_types)}")
    print(f"   Thread counts: {thread_counts}")
    
    # Create combined heatmap
    create_combined_ranking_heatmap(grouped_df, policy_variants, operations, storage_types, thread_counts, output_dir)
    
    print("\nAll visualizations generated successfully!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Generate performance ranking heatmap visualizations.'
    )
    parser.add_argument(
        '--csv',
        type=str,
        default=None,
        help='Path to the CSV file with benchmark results. Defaults to data/combined_benchmark_results_with_perf_20251002_072156.csv'
    )
    args = parser.parse_args()
    main(csv_path=args.csv)