#!/usr/bin/env python3
"""
Standalone script to generate throughput comparison plot with enhanced x-axis labels:
- 3 subplots (one for each storage type)
- X-axis: Enhanced labels showing Version-Threads | Policy Name
- Y-axis: Throughput
- 4 box plots per policy: V1-1T, V2-1T, V1-4T, V2-4T
- Black/grey/white color scheme with distinct patterns
- Individual data points overlaid on box plots
- Optional improvement indicators
- Enhanced readability with clear box identification
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

def load_and_process_data(v1_file, v2_file):
    """Load and process the data for box plot visualization."""
    print("[INFO] Loading datasets...")
    
    # Load data
    v1_data = pd.read_csv(v1_file)
    v2_data = pd.read_csv(v2_file)
    
    print(f"Version 1 (BPlusStore): {len(v1_data)} rows")
    print(f"Version 2 (BplusTreeSOA): {len(v2_data)} rows")
    
    # Process data to get individual run throughputs
    def aggregate_runs(data, tree_type):
        """Aggregate all operations within each run configuration."""
        print(f"[PROC] Aggregating runs for {tree_type}...")
        
        # Group by run configuration and test_run_id to get complete runs
        group_cols = ['policy_name', 'storage_type', 'cache_size', 'thread_count', 'test_run_id']
        
        # For each complete run (all operations in one test_run_id), aggregate
        run_aggregated = []
        
        for config, run_data in data.groupby(group_cols):
            policy, storage, cache_size, threads, test_run_id = config
            
            # Sum all operations in this complete run
            total_records = run_data['record_count'].sum() if 'record_count' in run_data.columns else len(run_data) * 100000
            total_time_us = run_data['time_us'].sum()
            
            # Calculate run-level throughput (records/second)
            run_throughput = (total_records / total_time_us) * 1_000_000 if total_time_us > 0 else 0
            
            run_aggregated.append({
                'policy_name': policy,
                'storage_type': storage,
                'cache_size': cache_size,
                'thread_count': threads,
                'test_run_id': test_run_id,
                'tree_type': tree_type,
                'run_throughput': run_throughput,
            })
        
        return pd.DataFrame(run_aggregated)
    
    # Get individual run data
    v1_runs = aggregate_runs(v1_data, "BPlusStore")
    v2_runs = aggregate_runs(v2_data, "BplusTreeSOA")
    
    return v1_runs, v2_runs

def create_throughput_boxplot_enhanced_labels(v1_runs, v2_runs, output_path, show_improvements=True):
    """Create the throughput comparison plot with enhanced x-axis labels."""
    print("[PLOT] Creating throughput box plot with enhanced x-axis labels...")
    
    fig, axes = plt.subplots(1, 3, figsize=(28, 12))
    
    colors = ['lightgrey', 'darkgrey', 'lightgrey', 'black']
    patterns = ['', '', '///', '///']
    edge_colors = ['black', 'black', 'black', 'white']
    labels = ['Version 1 (1 thread)', 'Version 2 (1 thread)', 'Version 1 (4 threads)', 'Version 2 (4 threads)']
    short_labels = ['V1-1T', 'V2-1T', 'V1-4T', 'V2-4T']
    
    storage_types = ['FileStorage', 'PMemStorage', 'VolatileStorage']
    storage_titles = {
        'FileStorage': 'SSD NVMe',
        'PMemStorage': 'NVM', 
        'VolatileStorage': 'NVDIMM'
    }
    
    print("[CALC] Calculating global y-scale across all subplots...")
    all_throughput_values = []
    
    for storage in storage_types:
        v1_storage = v1_runs[v1_runs['storage_type'] == storage]
        v2_storage = v2_runs[v2_runs['storage_type'] == storage]
        
        if len(v1_storage) > 0:
            all_throughput_values.extend(v1_storage['run_throughput'].values)
        if len(v2_storage) > 0:
            all_throughput_values.extend(v2_storage['run_throughput'].values)
    
    if all_throughput_values:
        global_y_min = 0
        global_y_max = max(all_throughput_values) * 1.1
        print(f"[STAT] Global y-scale: {global_y_min:.2e} to {global_y_max:.2e}")
    else:
        global_y_min, global_y_max = 0, 1
    
    for j, storage in enumerate(storage_types):
        ax = axes[j]
        
        v1_storage = v1_runs[v1_runs['storage_type'] == storage]
        v2_storage = v2_runs[v2_runs['storage_type'] == storage]
        
        v1_policies = set(v1_storage['policy_name'].unique())
        v2_policies = set(v2_storage['policy_name'].unique())
        policies = sorted(v1_policies.intersection(v2_policies))
        
        if len(policies) == 0:
            ax.text(0.5, 0.5, 'No Data', ha='center', va='center', transform=ax.transAxes)
            ax.set_title(storage_titles[storage])
            continue
        
        box_data = []
        box_positions = []
        box_colors = []
        box_patterns = []
        box_edge_colors = []
        improvement_data = []
        
        x_pos = 0
        x_ticks = []
        x_tick_labels = []
        policy_group_positions = []
        policy_group_labels = []
        
        for policy in policies:
            policy_improvements = {}
            
            policy_group_positions.append(x_pos + 1.2)
            policy_group_labels.append(policy)
            
            for i, (version_data, thread, color, pattern, edge_color, label, short_label) in enumerate(zip(
                [v1_storage, v2_storage, v1_storage, v2_storage], 
                [1, 1, 4, 4], 
                colors, patterns, edge_colors, labels, short_labels)):
                
                raw_data = version_data[
                    (version_data['policy_name'] == policy) & 
                    (version_data['thread_count'] == thread)
                ]
                
                if len(raw_data) > 0:
                    throughput_values = raw_data['run_throughput'].values
                    box_data.append(throughput_values)
                    box_positions.append(x_pos + i * 0.8)
                    box_colors.append(color)
                    box_patterns.append(pattern)
                    box_edge_colors.append(edge_color)
                    
                    avg_throughput = np.mean(throughput_values)
                    policy_improvements[label] = avg_throughput
                    
                    ax.scatter([x_pos + i * 0.8] * len(throughput_values), 
                             throughput_values, 
                             alpha=0.8, s=25, color='black', zorder=3, marker='o')
                    
                    x_ticks.append(x_pos + i * 0.8)
                    x_tick_labels.append(short_label)
                else:
                    box_data.append([0])
                    box_positions.append(x_pos + i * 0.8)
                    box_colors.append(color)
                    box_patterns.append(pattern)
                    box_edge_colors.append(edge_color)
                    policy_improvements[label] = 0
                    
                    x_ticks.append(x_pos + i * 0.8)
                    x_tick_labels.append(short_label)
            
            v1_1t_key = 'Version 1 (1 thread)'
            v2_1t_key = 'Version 2 (1 thread)'
            v1_4t_key = 'Version 1 (4 threads)'
            v2_4t_key = 'Version 2 (4 threads)'
            
            if show_improvements and policy_improvements.get(v1_1t_key, 0) > 0 and policy_improvements.get(v2_1t_key, 0) > 0:
                improvement_1t = policy_improvements[v2_1t_key] / policy_improvements[v1_1t_key]
                improvement_data.append((x_pos + 1 * 0.8, policy_improvements[v2_1t_key], improvement_1t, '1T'))
            
            if show_improvements and policy_improvements.get(v1_4t_key, 0) > 0 and policy_improvements.get(v2_4t_key, 0) > 0:
                improvement_4t = policy_improvements[v2_4t_key] / policy_improvements[v1_4t_key]
                improvement_data.append((x_pos + 3 * 0.8, policy_improvements[v2_4t_key], improvement_4t, '4T'))
            
            x_pos += 4.5
        
        if box_data:
            bp = ax.boxplot(box_data, positions=box_positions, widths=0.6, 
                           patch_artist=True, showfliers=False)
            
            for patch, color, pattern, edge_color in zip(bp['boxes'], box_colors, box_patterns, box_edge_colors):
                patch.set_facecolor(color)
                patch.set_hatch(pattern)
                patch.set_edgecolor(edge_color)
                patch.set_linewidth(1.5)
                patch.set_alpha(0.8)
            
            for whisker in bp['whiskers']:
                whisker.set_color('black')
                whisker.set_linewidth(1.5)
            for cap in bp['caps']:
                cap.set_color('black')
                cap.set_linewidth(1.5)
            for median in bp['medians']:
                median.set_color('red')
                median.set_linewidth(2)
        
        if show_improvements:
            for x_pos, y_pos, improvement, thread_type in improvement_data:
                if improvement != 0 and improvement != 1:
                    y_offset = global_y_max * 0.05
                    color = 'darkgreen' if improvement > 1 else 'darkred'
                    
                    ax.annotate(f'{improvement:.1f}x', 
                               xy=(x_pos, y_pos), 
                               xytext=(x_pos, y_pos + y_offset),
                               ha='center', va='bottom',
                               fontsize=27, fontweight='bold',
                               color=color,
                               bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8, edgecolor=color))
        
        ax.set_title(storage_titles[storage], fontweight='bold', fontsize=42)
        
        if j == 0:
            ax.set_ylabel('Throughput (ops/sec)', fontsize=36)
        
        ax.set_xticks(x_ticks)
        ax.set_xticklabels(x_tick_labels, fontsize=25, rotation=90, ha='center', va='top')
        
        for pos, policy_label in zip(policy_group_positions, policy_group_labels):
            ax.text(pos, global_y_min - global_y_max * 0.16, policy_label, 
                   ha='center', va='top', fontsize=36)
        
        ax.ticklabel_format(style='scientific', axis='y', scilimits=(0,0))
        ax.tick_params(axis='y', labelsize=30)
        ax.yaxis.get_offset_text().set_fontsize(24)
        ax.grid(True, alpha=0.3, color='grey')
        ax.set_ylim(global_y_min, global_y_max)
        
        if j == 0:
            legend_elements = []
            enhanced_labels = [
                f"{labels[0]} | {short_labels[0]}",
                f"{labels[1]} | {short_labels[1]}",
                f"{labels[2]} | {short_labels[2]}",
                f"{labels[3]} | {short_labels[3]}"
            ]
            
            for color, pattern, edge_color, enhanced_label in zip(colors, patterns, edge_colors, enhanced_labels):
                legend_elements.append(
                    plt.Rectangle((0,0), 1, 1, 
                                facecolor=color, 
                                hatch=pattern, 
                                edgecolor=edge_color,
                                linewidth=1.5,
                                alpha=0.8,
                                label=enhanced_label)
                )
            ax.legend(handles=legend_elements, loc='upper left', frameon=True, fancybox=True, shadow=True, fontsize=27)
    
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.15)
    
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    
    pdf_path = output_path.with_suffix('.pdf')
    plt.savefig(pdf_path, dpi=300, bbox_inches='tight', format='pdf')
    
    plt.close()
    
    print(f"[DONE] PNG plot saved to: {output_path}")
    print(f"[DONE] PDF plot saved to: {pdf_path}")

def main():
    """Main function to generate the throughput box plot with enhanced labels."""
    print("[MAIN] Starting Enhanced Labels Throughput Box Plot Generation...")
    
    if len(sys.argv) < 3:
        print("[ERROR] Usage: python script.py <v1_csv> <v2_csv> [output_dir]")
        print("[ERROR] Example: python script.py v1_data.csv v2_data.csv /output/path")
        print("[INFO] If output_dir not specified, script directory will be used")
        sys.exit(1)
    
    v1_file = sys.argv[1]
    v2_file = sys.argv[2]
    
    if len(sys.argv) >= 4:
        output_dir = Path(sys.argv[3])
    else:
        output_dir = Path(__file__).parent
    
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / 'plog_figure3.png'
    
    try:
        if not Path(v1_file).exists():
            print(f"[ERROR] V1 file not found: {v1_file}")
            sys.exit(1)
        if not Path(v2_file).exists():
            print(f"[ERROR] V2 file not found: {v2_file}")
            sys.exit(1)
        
        print(f"[INFO] Input V1: {v1_file}")
        print(f"[INFO] Input V2: {v2_file}")
        print(f"[INFO] Output dir: {output_dir}")
        
        v1_runs, v2_runs = load_and_process_data(v1_file, v2_file)
        create_throughput_boxplot_enhanced_labels(v1_runs, v2_runs, output_path, show_improvements=True)
        
        print("[SUCCESS] Enhanced labels throughput box plot generated!")
        print(f"[PATH] Location: {output_path}")
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
