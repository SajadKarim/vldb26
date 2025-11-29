#!/usr/bin/env python3
"""
Standalone script to generate instruction consumption comparison plot with enhanced x-axis labels:
- 3 subplots (one for each storage type)
- X-axis: Enhanced labels showing Version-Threads | Policy Name
- Y-axis: Total instructions consumed per test run
- 4 box plots per policy: V1-1T, V2-1T, V1-4T, V2-4T
- Black/grey/white color scheme with distinct patterns
- Individual data points overlaid on box plots
- Optional improvement indicators (lower is better for instructions)
- Enhanced readability with clear box identification
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

def load_and_process_data(v1_file, v2_file):
    """Load and process the data for instruction consumption box plot visualization."""
    print("[INFO] Loading datasets...")
    
    v1_data = pd.read_csv(v1_file)
    v2_data = pd.read_csv(v2_file)
    
    print(f"Version 1 (BPlusStore): {len(v1_data)} rows")
    print(f"Version 2 (BplusTreeSOA): {len(v2_data)} rows")
    
    def prepare_operation_data(data, tree_type):
        """Prepare individual operations as separate data points, calculating CPI."""
        print(f"[PROC] Preparing individual operation CPI data for {tree_type}...")
        
        operation_data = data.copy()
        operation_data['tree_type'] = tree_type
        
        operation_data['cpi'] = operation_data['perf_cycles'] / operation_data['perf_instructions'].replace(0, 1)
        
        operation_data = operation_data[
            (operation_data['cpi'].notna()) & 
            (operation_data['cpi'] != float('inf')) & 
            (operation_data['cpi'] > 0)
        ]
        
        result_cols = ['policy_name', 'storage_type', 'cache_size', 'thread_count', 
                      'test_run_id', 'tree_type', 'cpi']
        
        return operation_data[result_cols]
    
    v1_operations = prepare_operation_data(v1_data, "BPlusStore")
    v2_operations = prepare_operation_data(v2_data, "BplusTreeSOA")
    
    return v1_operations, v2_operations

def create_cpi_boxplot_enhanced_labels(v1_operations, v2_operations, output_path):
    """Create the CPI (Cycles Per Instruction) comparison plot with enhanced x-axis labels (individual operations)."""
    print("[PLOT] Creating CPI box plot with enhanced x-axis labels (individual operations)...")
    
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
    all_cpi_values = []
    
    for storage in storage_types:
        v1_storage = v1_operations[v1_operations['storage_type'] == storage]
        v2_storage = v2_operations[v2_operations['storage_type'] == storage]
        
        if len(v1_storage) > 0:
            all_cpi_values.extend(v1_storage['cpi'].values)
        if len(v2_storage) > 0:
            all_cpi_values.extend(v2_storage['cpi'].values)
    
    if all_cpi_values:
        global_y_min = 0
        global_y_max = max(all_cpi_values) * 1.1
        print(f"[STAT] Global y-scale: {global_y_min:.2e} to {global_y_max:.2e}")
    else:
        global_y_min, global_y_max = 0, 1
    
    for j, storage in enumerate(storage_types):
        ax = axes[j]
        
        v1_storage = v1_operations[v1_operations['storage_type'] == storage]
        v2_storage = v2_operations[v2_operations['storage_type'] == storage]
        
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
        
        x_pos = 0
        x_ticks = []
        x_tick_labels = []
        policy_group_positions = []
        policy_group_labels = []
        
        for policy in policies:
            
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
                    cpi_values = raw_data['cpi'].values
                    box_data.append(cpi_values)
                    box_positions.append(x_pos + i * 0.8)
                    box_colors.append(color)
                    box_patterns.append(pattern)
                    box_edge_colors.append(edge_color)
                    
                    ax.scatter([x_pos + i * 0.8] * len(cpi_values), 
                             cpi_values, 
                             alpha=0.8, s=25, color='black', zorder=3, marker='o')
                    
                    x_ticks.append(x_pos + i * 0.8)
                    x_tick_labels.append(short_label)
                else:
                    box_data.append([0])
                    box_positions.append(x_pos + i * 0.8)
                    box_colors.append(color)
                    box_patterns.append(pattern)
                    box_edge_colors.append(edge_color)
                    
                    x_ticks.append(x_pos + i * 0.8)
                    x_tick_labels.append(short_label)
            
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
        
        ax.set_title(storage_titles[storage], fontweight='bold', fontsize=42)
        
        if j == 0:
            ax.set_ylabel('Cycles Per Instruction', fontsize=36)
        
        ax.set_xticks(x_ticks)
        ax.set_xticklabels(x_tick_labels, fontsize=25, rotation=90, ha='center', va='top')
        
        if all_cpi_values:
            min_val = min(all_cpi_values)
            max_val = max(all_cpi_values)
            range_val = max_val - min_val
            label_y_pos = min_val - range_val * 0.2
        else:
            label_y_pos = -0.1
            
        for pos, policy_label in zip(policy_group_positions, policy_group_labels):
            ax.text(pos, label_y_pos, policy_label, 
                   ha='center', va='top', fontsize=36)
        
        ax.tick_params(axis='y', labelsize=30)
        ax.grid(True, alpha=0.3, color='grey')
        
        if all_cpi_values:
            min_val = min(all_cpi_values)
            max_val = max(all_cpi_values)
            range_val = max_val - min_val
            ax.set_ylim(min_val - range_val * 0.05, max_val + range_val * 0.05)
        
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
    """Main function to generate the CPI box plot with enhanced labels."""
    print("[MAIN] Starting Enhanced Labels CPI Box Plot Generation...")
    
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
    output_path = output_dir / 'plog_figure4.png'
    
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
        
        v1_operations, v2_operations = load_and_process_data(v1_file, v2_file)
        create_cpi_boxplot_enhanced_labels(v1_operations, v2_operations, output_path)
        
        print("[SUCCESS] Enhanced labels CPI box plot generated!")
        print(f"[PATH] Location: {output_path}")
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
