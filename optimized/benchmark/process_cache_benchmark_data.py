#!/usr/bin/env python3

import os
import pandas as pd
import re
import glob
from pathlib import Path
import numpy as np

def parse_folder_name(folder_name):
    """
    Parse the folder name to extract benchmark parameters.
    Example: concurrent_lru_update_in_order_concurrent_lru_update_in_order_BplusTreeSOA_LRU_PMemStorage_150000_4096_2147483648_uint64_t_uint64_t_delete_64_1000000_threads4
    """
    parts = folder_name.split('_')
    
    # Find the tree type (BplusTreeSOA)
    tree_idx = -1
    for i, part in enumerate(parts):
        if 'BplusTree' in part:
            tree_idx = i
            break
    
    if tree_idx == -1:
        return None
    
    # Extract cache variant (everything before tree type)
    cache_variant_parts = parts[:tree_idx]
    cache_variant = '_'.join(cache_variant_parts)
    
    # Extract parameters after tree type
    remaining_parts = parts[tree_idx+1:]  # Skip BplusTreeSOA
    
    try:
        # Expected pattern: CACHE_STORAGE_CACHESIZE_PAGESIZE_MAXSIZE_KEYTYPE_t_VALUETYPE_t_OPERATION_DEGREE_RECORDS_THREADS
        cache_policy = remaining_parts[0]  # LRU, A2Q, CLOCK
        storage_type = remaining_parts[1]  # PMemStorage, VolatileStorage
        cache_size = int(remaining_parts[2])
        page_size = int(remaining_parts[3])
        max_size = int(remaining_parts[4])
        
        # Handle uint64_t format - need to combine parts
        key_type = remaining_parts[5] + '_' + remaining_parts[6]  # uint64_t
        value_type = remaining_parts[7] + '_' + remaining_parts[8]  # uint64_t
        
        # Find operation - it could be compound like search_zipfian
        operation_start_idx = 9
        operation_parts = []
        degree_idx = -1
        
        # Look for the degree (numeric value after operation)
        for i in range(operation_start_idx, len(remaining_parts)):
            try:
                int(remaining_parts[i])
                degree_idx = i
                break
            except ValueError:
                operation_parts.append(remaining_parts[i])
        
        if degree_idx == -1:
            return None
            
        operation = '_'.join(operation_parts)
        degree = int(remaining_parts[degree_idx])
        records = int(remaining_parts[degree_idx + 1])
        threads_part = remaining_parts[degree_idx + 2]  # threads4 or threads1
        threads = int(threads_part.replace('threads', ''))
        
        return {
            'cache_variant': cache_variant,
            'cache_policy': cache_policy,
            'storage_type': storage_type,
            'cache_size': cache_size,
            'page_size': page_size,
            'max_size': max_size,
            'key_type': key_type,
            'value_type': value_type,
            'operation': operation,
            'degree': degree,
            'records': records,
            'threads': threads
        }
    except (IndexError, ValueError) as e:
        print(f"Error parsing folder name: {folder_name}")
        print(f"Parts: {parts}")
        print(f"Remaining parts: {remaining_parts}")
        print(f"Error: {e}")
        return None

def process_csv_file(csv_path, folder_info):
    """Process a single CSV file and extract benchmark data."""
    try:
        df = pd.read_csv(csv_path)
        
        if df.empty:
            return []
        
        results = []
        for _, row in df.iterrows():
            # Calculate actual throughput from record_count and time_us
            time_seconds = row['time_us'] / 1000000.0
            calculated_throughput = row['record_count'] / time_seconds
            
            # Determine concurrency type
            concurrency = 'concurrent' if folder_info['threads'] > 1 else 'non_concurrent'
            
            # For default implementations, include the cache policy in the variant name
            cache_variant = folder_info['cache_variant']
            if cache_variant in ['concurrent_concurrent', 'non_concurrent_non_concurrent']:
                cache_variant = f"{cache_variant}_{folder_info['cache_policy']}"
            
            # Map storage types to more descriptive names
            storage_type = folder_info['storage_type']
            if storage_type == 'VolatileStorage':
                storage_type = 'NVDIMM'
            elif storage_type == 'PMemStorage':
                storage_type = 'NVM'
            
            result = {
                'cache_policy': folder_info['cache_policy'],
                'cache_variant': cache_variant,
                'concurrency': concurrency,
                'storage_type': storage_type,
                'cache_size': folder_info['cache_size'],
                'operation': folder_info['operation'],
                'degree': folder_info['degree'],
                'threads': folder_info['threads'],
                'time_us': row['time_us'],
                'throughput_ops_sec': calculated_throughput,  # Use calculated throughput
                'record_count': row['record_count'],
                'cache_hits': row['cache_hits'],
                'cache_misses': row['cache_misses'],
                'cache_evictions': row['cache_evictions'],
                'cache_dirty_evictions': row['cache_dirty_evictions'],
                'cache_hit_rate': row['cache_hit_rate'],
                'test_run_id': row['test_run_id'],
                'tree_type': row['tree_type'],
                'key_type': row['key_type'],
                'value_type': row['value_type'],
                'workload_type': row['workload_type'],
                'timestamp': row['timestamp']
            }
            results.append(result)
        
        return results
    
    except Exception as e:
        print(f"Error processing CSV file {csv_path}: {e}")
        return []

def aggregate_test_runs(data):
    """Aggregate multiple test runs for the same configuration."""
    df = pd.DataFrame(data)
    
    # Group by all parameters except test_run_id, time_us, throughput_ops_sec, and timestamp
    group_cols = [
        'cache_policy', 'cache_variant', 'concurrency', 'storage_type', 
        'cache_size', 'operation', 'degree', 'threads', 'record_count',
        'tree_type', 'key_type', 'value_type', 'workload_type'
    ]
    
    # Calculate statistics for each group
    agg_funcs = {
        'time_us': ['mean', 'std'],
        'throughput_ops_sec': ['mean', 'std'],
        'cache_hits': 'mean',
        'cache_misses': 'mean',
        'cache_evictions': 'mean',
        'cache_dirty_evictions': 'mean',
        'cache_hit_rate': 'mean',
        'test_run_id': 'count'  # Number of test runs
    }
    
    aggregated = df.groupby(group_cols).agg(agg_funcs).reset_index()
    
    # Flatten column names
    aggregated.columns = [
        col[0] if col[1] == '' else f"{col[0]}_{col[1]}" 
        for col in aggregated.columns
    ]
    
    # Rename count column
    aggregated = aggregated.rename(columns={'test_run_id_count': 'num_test_runs'})
    
    # Fill NaN std values with 0 (for cases with only 1 test run)
    aggregated['time_us_std'] = aggregated['time_us_std'].fillna(0)
    aggregated['throughput_ops_sec_std'] = aggregated['throughput_ops_sec_std'].fillna(0)
    
    return aggregated

def main():
    results_dir = "/home/skarim/Code/haldendb_ex/haldendb_pvt/benchmark/build/cache_profiling_results_20250917_153838"
    
    print("Processing cache benchmark data...")
    
    # Find all subdirectories
    subdirs = [d for d in os.listdir(results_dir) 
               if os.path.isdir(os.path.join(results_dir, d)) and d != 'analysis_results']
    
    print(f"Found {len(subdirs)} benchmark run directories")
    
    all_data = []
    processed_count = 0
    error_count = 0
    
    for subdir in subdirs:
        folder_path = os.path.join(results_dir, subdir)
        
        # Parse folder name to extract parameters
        folder_info = parse_folder_name(subdir)
        if folder_info is None:
            print(f"Skipping directory with unparseable name: {subdir}")
            error_count += 1
            continue
        
        # Find CSV file in the directory
        csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
        if not csv_files:
            print(f"No CSV file found in {subdir}")
            error_count += 1
            continue
        
        if len(csv_files) > 1:
            print(f"Multiple CSV files found in {subdir}, using first one")
        
        csv_path = csv_files[0]
        
        # Process the CSV file
        csv_data = process_csv_file(csv_path, folder_info)
        all_data.extend(csv_data)
        
        processed_count += 1
        if processed_count % 50 == 0:
            print(f"Processed {processed_count} directories...")
    
    print(f"Successfully processed {processed_count} directories")
    print(f"Encountered errors in {error_count} directories")
    print(f"Total data points: {len(all_data)}")
    
    if not all_data:
        print("No data to process!")
        return
    
    # Create analysis_results directory
    analysis_dir = os.path.join(results_dir, "analysis_results")
    os.makedirs(analysis_dir, exist_ok=True)
    
    # Save raw data
    raw_df = pd.DataFrame(all_data)
    raw_output_path = os.path.join(analysis_dir, "raw_cache_performance_data.csv")
    raw_df.to_csv(raw_output_path, index=False)
    print(f"Raw data saved to: {raw_output_path}")
    
    # Aggregate test runs
    print("Aggregating test runs...")
    aggregated_df = aggregate_test_runs(all_data)
    
    # Save aggregated data
    processed_output_path = os.path.join(analysis_dir, "processed_cache_performance_data.csv")
    aggregated_df.to_csv(processed_output_path, index=False)
    print(f"Processed data saved to: {processed_output_path}")
    
    # Print summary statistics
    print("\n=== DATA SUMMARY ===")
    print(f"Total configurations: {len(aggregated_df)}")
    print(f"Cache policies: {sorted(aggregated_df['cache_policy'].unique())}")
    print(f"Cache variants: {sorted(aggregated_df['cache_variant'].unique())}")
    print(f"Storage types: {sorted(aggregated_df['storage_type'].unique())}")
    print(f"Cache sizes: {sorted(aggregated_df['cache_size'].unique())}")
    print(f"Operations: {sorted(aggregated_df['operation'].unique())}")
    print(f"Thread counts: {sorted(aggregated_df['threads'].unique())}")
    
    # Show sample of the data
    print("\n=== SAMPLE DATA ===")
    print(aggregated_df.head(10).to_string())
    
    print(f"\nData processing complete! Files saved in: {analysis_dir}")

if __name__ == "__main__":
    main()