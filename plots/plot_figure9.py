#!/usr/bin/env python3
"""
Cache Policy Ranking Visualizations

Shows rankings of cache policies across different cache sizes, storage types, and thread counts.
Rankings are calculated column-wise (within each cache size) based on aggregated throughput.

Grid Layout: 3 rows (1 thread, 4 threads, 8 threads) × 3 columns (storage types)
Each cell shows: 3 columns (cache sizes: 2%, 10%, 25%) × N rows (policy variants)
Cell values: Rank (1=best, N=worst) based on throughput for that specific cache size
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
import sys
import argparse
warnings.filterwarnings('ignore')

class CachePolicyRankingVisualizer:
    def __init__(self, csv_file):
        """Initialize with data file."""
        self.csv_file = Path(csv_file)
        self.df = None
        self.aggregated_df = None
        
        # Storage display names
        self.storage_display_names = {
            'FileStorage': 'SSD NVMe',
            'PMemStorage': 'NVM', 
            'VolatileStorage': 'NVDIMM'
        }
        
        # Policy display names
        self.policy_display_names = {
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
        }
    
    def load_and_process_data(self):
        """Load and process data."""
        print(f"Loading data from {self.csv_file}")
        
        self.df = pd.read_csv(self.csv_file)
        print(f"Loaded {len(self.df)} records")
        
        # Clean column names
        self.df.columns = self.df.columns.str.strip()
        
        # Convert cache_size to percentage
        self.df['cache_size_pct'] = (self.df['cache_size'] * 100).round(1)
        
        # Create policy variant name
        self.df['clean_config_name'] = self.df['config_name'].str.replace('^(concurrent_|non_concurrent_)', '', regex=True)
        self.df['policy_variant'] = self.df['policy_name'] + '_' + self.df['clean_config_name']
        
        # Use throughput column
        self.df['throughput'] = self.df['throughput_ops_sec']
        
        print(f"Found {len(self.df['policy_variant'].unique())} policy variants")
        print(f"Cache sizes: {sorted(self.df['cache_size_pct'].unique())}%")
        print(f"Storage types: {sorted(self.df['storage_type'].unique())}")
        print(f"Thread counts: {sorted(self.df['thread_count'].unique())}")
        
    def aggregate_data(self):
        """Aggregate data across operations and test runs."""
        print("Aggregating data across operations and test runs...")
        
        # Group by configuration including test runs
        group_cols_with_runs = [
            'storage_type', 'cache_size_pct', 'thread_count', 'policy_variant', 
            'policy_name', 'clean_config_name', 'test_run_id'
        ]
        
        # Average throughput across operations for each test run
        run_averages = self.df.groupby(group_cols_with_runs)['throughput'].mean().reset_index()
        run_averages.rename(columns={'throughput': 'avg_throughput_per_run'}, inplace=True)
        
        # Average across test runs for each unique configuration
        group_cols_final = [
            'storage_type', 'cache_size_pct', 'thread_count', 'policy_variant',
            'policy_name', 'clean_config_name'
        ]
        
        self.aggregated_df = run_averages.groupby(group_cols_final).agg({
            'avg_throughput_per_run': ['mean', 'std', 'count']
        }).reset_index()
        
        # Flatten column names
        self.aggregated_df.columns = group_cols_final + ['throughput_mean', 'throughput_std', 'run_count']
        
        print(f"Aggregated to {len(self.aggregated_df)} unique configurations")
        
    def calculate_rankings(self, df_subset, cache_sizes, policy_variants):
        """
        Calculate rankings for each cache size (column-wise ranking).
        
        Args:
            df_subset: DataFrame filtered for specific storage_type and thread_count
            cache_sizes: List of cache sizes to rank
            policy_variants: List of policy variants
            
        Returns:
            DataFrame with rankings (rows=policies, columns=cache sizes)
        """
        ranking_data = []
        
        for policy in policy_variants:
            policy_ranks = []
            
            for cache_size in cache_sizes:
                # Get throughput for this policy at this cache size
                data = df_subset[
                    (df_subset['policy_variant'] == policy) &
                    (df_subset['cache_size_pct'] == cache_size)
                ]
                
                if len(data) > 0:
                    throughput = data['throughput_mean'].iloc[0]
                    policy_ranks.append(throughput)
                else:
                    policy_ranks.append(np.nan)
            
            ranking_data.append(policy_ranks)
        
        # Create DataFrame with throughput values
        throughput_df = pd.DataFrame(ranking_data, 
                                     index=policy_variants, 
                                     columns=cache_sizes)
        
        # Calculate rankings column-wise (within each cache size)
        # Higher throughput = better rank (rank 1)
        ranking_data_dict = {}
        
        for cache_size in cache_sizes:
            # Get throughput values for this cache size
            values = throughput_df[cache_size]
            
            # Rank: higher value gets lower rank number (1 is best)
            # Use method='min' so ties get the same rank
            ranks = values.rank(ascending=False, method='min', na_option='keep')
            ranking_data_dict[cache_size] = ranks
        
        ranking_df = pd.DataFrame(ranking_data_dict, index=policy_variants)
        
        return ranking_df, throughput_df
    
    def create_ranking_heatmaps(self):
        """Create ranking heatmaps for all thread counts and storage types."""
        
        df = self.aggregated_df
        storage_types = sorted(df['storage_type'].unique())
        thread_counts = sorted(df['thread_count'].unique())
        cache_sizes = sorted(df['cache_size_pct'].unique())
        policy_variants = sorted(df['policy_variant'].unique())
        
        print(f"\nThread counts found: {thread_counts}")
        print(f"Storage types: {storage_types}")
        print(f"Cache sizes: {cache_sizes}")
        print(f"Policy variants: {len(policy_variants)}")
        
        # Create figure: 3 rows (thread counts) × 3 columns (storage types)
        n_rows = len(thread_counts)
        n_cols = len(storage_types)
        
        # Compact size with larger fonts: width per column ~5.5, height per row ~10.5
        # Increased height per row from 9 to 10.5 for taller heatmap boxes
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(16, 12.5*n_rows))
        
        # Handle different array shapes
        if n_rows == 1 and n_cols == 1:
            axes = np.array([[axes]])
        elif n_rows == 1:
            axes = axes.reshape(1, -1)
        elif n_cols == 1:
            axes = axes.reshape(-1, 1)
        
        #fig.suptitle('Cache Policy Rankings by Cache Size\n(Rank 1=Best, Grey=Top Performers, White=Lower Performers)', 
        #             fontsize=20, fontweight='bold', y=0.995)
        
        # Calculate global max rank for consistent color scaling
        global_max_rank = len(policy_variants)
        
        for i, thread_count in enumerate(thread_counts):
            for j, storage_type in enumerate(storage_types):
                ax = axes[i, j]
                
                # Filter data for this thread count and storage type
                subset = df[
                    (df['thread_count'] == thread_count) &
                    (df['storage_type'] == storage_type)
                ]
                
                if len(subset) > 0:
                    # Calculate rankings
                    ranking_df, throughput_df = self.calculate_rankings(
                        subset, cache_sizes, policy_variants
                    )
                    
                    # Create formatted annotations showing only rank number
                    annot_data = []
                    for policy in policy_variants:
                        row_annots = []
                        for cache_size in cache_sizes:
                            rank = ranking_df.loc[policy, cache_size]
                            
                            if pd.notna(rank):
                                # Format: Just the rank number
                                annot_str = f"{int(rank)}"
                            else:
                                annot_str = "N/A"
                            row_annots.append(annot_str)
                        annot_data.append(row_annots)
                    
                    annot_df = pd.DataFrame(annot_data, 
                                           index=policy_variants, 
                                           columns=cache_sizes)
                    
                    # Use rankings directly for color mapping (rank 1 = darkest grey)
                    # Normalize: rank 1 -> 1.0 (darkest), rank N -> 0.0 (lightest/white)
                    ranking_normalized = (global_max_rank + 1 - ranking_df) / global_max_rank
                    ranking_normalized = ranking_normalized.fillna(0.0)  # N/A gets white
                    
                    # Create heatmap
                    sns.heatmap(ranking_normalized,
                               annot=annot_df,
                               fmt='',
                               cmap='Greys',  # Grey for best (rank 1), white for worst
                               ax=ax,
                               cbar=False,
                               linewidths=0,
                               linecolor='white',
                               vmin=0, vmax=1,
                               annot_kws={'size': 22, 'weight': 'bold'})  # Doubled from 11 to 22
                    
                    # Hide spines to remove borders
                    for spine in ax.spines.values():
                        spine.set_visible(False)
                    
                    # Manually set text colors based on rank values
                    for text_obj in ax.texts:
                        text_val = text_obj.get_text()
                        if text_val.isdigit():
                            rank = int(text_val)
                            # Top 3 ranks: white text on dark grey background
                            # Lower ranks: black text on light/white background
                            text_color = 'white' if rank <= 3 else 'black'
                            text_obj.set_color(text_color)
                        elif text_val == 'N/A':
                            text_obj.set_color('black')
                    
                    # Set title (only for first row)
                    if i == 0:
                        ax.set_title(f'{self.storage_display_names.get(storage_type, storage_type)}', 
                                     fontsize=30)  # Doubled from 18 to 36
                    
                    # Set y-label (only for first column)
                    if j == 0:
                        ylabel = 'Replacement Policy Variants'
                        ax.set_ylabel(ylabel, fontsize=30)  # Doubled from 16 to 32
                    else:
                        ax.set_ylabel('')
                    
                    # Set x-label (only for last row)
                    if i == n_rows - 1:
                        ax.set_xlabel('Cache Size (%)', fontsize=30)  # Doubled from 12 to 24
                    else:
                        ax.set_xlabel('')
                    
                    # Format y-axis labels (policy names)
                    y_labels = [self.policy_display_names.get(policy, policy.replace('_', ' ')) 
                               for policy in policy_variants]
                    ax.set_yticklabels(y_labels, rotation=0, fontsize=26)  # Doubled from 10 to 20
                    
                    # Format x-axis labels (cache sizes)
                    x_labels = [f'{int(size)}%' for size in cache_sizes]
                    # Only show x-tick labels for last row
                    if i == n_rows - 1:
                        ax.set_xticklabels(x_labels, rotation=0, fontsize=26)  # Doubled from 11 to 22
                    else:
                        ax.set_xticklabels([])
                    
                    # Remove y-tick labels for columns 2 and 3
                    if j > 0:
                        ax.set_yticklabels([])
                    
                    # Add thread count label to the right side (last column only)
                    if j == n_cols - 1:
                        thread_label = '1 Thread' if thread_count == 1 else f'{thread_count} Threads'
                        ax.text(1.02, 0.5, thread_label, 
                               transform=ax.transAxes,
                               rotation=270,
                               va='center',
                               ha='left',
                               fontsize=30)
                    
                else:
                    ax.text(0.5, 0.5, 'No Data', ha='center', va='center', 
                           transform=ax.transAxes, fontsize=26)
                    if i == 0:
                        ax.set_title(f'{self.storage_display_names.get(storage_type, storage_type)}')
                    if j == 0:
                        ylabel = '1 Thread' if thread_count == 1 else f'{thread_count} Threads'
                        ax.set_ylabel(ylabel, fontsize=26)
        
        plt.tight_layout()
        script_dir = Path(__file__).parent
        output_path = script_dir / 'plot_figure9.png'
        output_pdf = script_dir / 'plot_figure9.pdf'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.savefig(output_pdf, dpi=300, bbox_inches='tight')
        print(f"\nCreated ranking heatmaps: {output_path.name}/.pdf")
        return fig

def main(csv_path=None):
    """Main function to create ranking visualizations."""
    print("Creating Cache Policy Ranking Visualizations")
    print("=" * 70)
    
    # Get data file from parameter or use default
    if csv_path is None:
        # Use default path relative to script location
        script_dir = Path(__file__).parent
        csv_path = script_dir / 'data' / 'combined_benchmark_results_with_perf_20251002_072156.csv'
    else:
        csv_path = Path(csv_path)
    
    if not csv_path.exists():
        print(f"ERROR: CSV file not found at {csv_path}")
        sys.exit(1)
    
    data_file = str(csv_path)
    visualizer = CachePolicyRankingVisualizer(data_file)
    
    # Process data
    visualizer.load_and_process_data()
    visualizer.aggregate_data()
    
    print("\nCreating ranking visualizations...")
    
    # Create ranking heatmaps
    ranking_fig = visualizer.create_ranking_heatmaps()
    
    print("\nRanking Visualizations Complete!")
    print("=" * 70)
    print("Generated files:")
    print("  cache_policy_rankings_by_size.png/.pdf - Policy rankings by cache size")
    print("\nRanking Method:")
    print("  - Column-wise ranking (within each cache size)")
    print("  - Rank 1 = Best performance (highest throughput)")
    print("  - Rank N = Worst performance (lowest throughput)")
    print("  - Dark grey background = Top performers (ranks 1-3)")
    print("  - White background = Lower performers")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate cache policy ranking visualizations.'
    )
    parser.add_argument(
        '--csv',
        type=str,
        default=None,
        help='Path to the CSV file with benchmark results. Defaults to data/combined_benchmark_results_with_perf_20251002_072156.csv'
    )
    args = parser.parse_args()
    main(csv_path=args.csv)