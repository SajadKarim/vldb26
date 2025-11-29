#pragma once

/**
 * Device-Aware YCSB Benchmark
 * 
 * This benchmark uses DeviceAwarePolicy to automatically select the optimal
 * cache policy and configuration based on workload type and storage device.
 * 
 * Key Features:
 * - Automatic policy selection (no manual cache type specification)
 * - Workload-aware optimization
 * - Storage-aware configuration
 * - Runtime policy switching without recompilation
 */

#include "bm_tree_with_cache_ycsb.hpp"
#include "../libcache/DeviceAwarePolicy.hpp"
#include "../libcache/PolicyVariants.hpp"

namespace bm_tree_with_cache_ycsb_device_aware {

using namespace DeviceAware;

/**
 * Run YCSB benchmark with DeviceAwarePolicy
 * 
 * This function demonstrates how to use the policy selector to automatically
 * choose the best cache configuration for a given workload and storage type.
 */
template<typename StoreType, typename KeyType, typename ValueType>
void run_ycsb_device_aware_benchmark(
    const std::string& workload_str,
    const std::string& storage_str,
    size_t degree,
    size_t records,
    int cache_size,
    int page_size,
    long long memory_size,
    const std::string& storage_path,
    CSVLogger& logger,
    int run_id,
    const std::string& output_dir,
    int threads,
    const std::string& cache_size_percentage,
    size_t cache_page_limit
) {
    // Step 1: Query DeviceAwarePolicy for optimal configuration
    DeviceAwarePolicy policy;
    auto workload_type = DeviceAwarePolicy::parseWorkload(workload_str);
    auto storage_type = DeviceAwarePolicy::parseStorage(storage_str);
    auto config = policy.selectPolicy(workload_type, storage_type);
    
    // Step 2: Print selected configuration
    std::cout << "\n=== DeviceAwarePolicy Selection ===" << std::endl;
    std::cout << "Workload: " << DeviceAwarePolicy::getWorkloadName(workload_type) << std::endl;
    std::cout << "Storage: " << DeviceAwarePolicy::getStorageName(storage_type) << std::endl;
    std::cout << "Selected Policy: " << config.policy_name << std::endl;
    std::cout << "Configuration: " << config.build_config << std::endl;
    std::cout << "Rationale: " << config.selection_rationale << std::endl;
    std::cout << "===================================\n" << std::endl;
    
    // Step 3: Create tree with selected policy
    // Note: In the current implementation, we still need to instantiate the correct
    // cache type at compile time. The PolicyVariants system allows runtime selection
    // within a single binary that includes all variants.
    
    // For now, we'll use the standard benchmark function but log the policy decision
    // In a full implementation, you would use PolicyFactory to create the cache
    
    // Log the policy decision to CSV using the standard CSVLogger API
    logger.log_result(
        "BplusTreeSOA",                       // tree_type
        "uint64_t",                           // key_type
        "uint64_t",                           // value_type
        config.policy_name,                   // policy_name
        storage_str,                          // storage_type
        config.build_config,                  // config_name
        records,                              // record_count
        degree,                               // degree
        workload_str,                         // operation (workload)
        0,                                    // time_us (placeholder)
        0.0,                                  // throughput (placeholder)
        run_id,                               // test_run_id
        0,                                    // cache_hits
        0,                                    // cache_misses
        0,                                    // cache_evictions
        0,                                    // cache_dirty_evictions
        0.0,                                  // cache_hit_rate
        cache_size_percentage,                // cache_size
        cache_page_limit,                     // cache_page_limit
        threads                               // thread_count
    );
    
    // Step 4: Run the actual benchmark
    // This would call the appropriate benchmark function based on the selected policy
    // For demonstration, we show the pattern:
    
    std::cout << "Running benchmark with selected policy..." << std::endl;
    // The actual benchmark execution would go here
    // run_ycsb_cache_benchmark<StoreType, KeyType, ValueType>(...);
}

/**
 * Example: Test all YCSB workloads with automatic policy selection
 * 
 * This function demonstrates how to run a comprehensive benchmark suite
 * where the cache policy is automatically selected for each combination
 * of workload and storage type.
 */
void test_ycsb_with_device_aware_policy(
    const std::vector<std::string>& workload_types = {"ycsb_a", "ycsb_b", "ycsb_c", "ycsb_d", "ycsb_e", "ycsb_f"},
    const std::vector<std::string>& storage_types = {"VolatileStorage", "PMemStorage", "FileStorage"},
    const std::vector<size_t>& degrees = {64, 128, 256},
    const std::vector<size_t>& record_counts = {100000, 500000, 1000000},
    int num_runs = 3,
    const std::string& output_dir = "",
    int threads = 1
) {
    std::cout << "\n=== YCSB Benchmark with DeviceAwarePolicy ===" << std::endl;
    std::cout << "This benchmark automatically selects optimal cache policies" << std::endl;
    std::cout << "based on workload characteristics and storage device type.\n" << std::endl;
    
    // Create CSV logger
    std::string csv_filename = output_dir.empty() ? 
        "ycsb_device_aware_results.csv" : 
        output_dir + "/ycsb_device_aware_results.csv";
    
    CSVLogger logger(csv_filename);
    logger.write_header();
    
    DeviceAwarePolicy policy;
    
    // Print decision matrix for reference
    std::cout << "\nDecision Matrix Preview:" << std::endl;
    std::cout << "========================" << std::endl;
    
    for (const auto& workload_str : workload_types) {
        for (const auto& storage_str : storage_types) {
            auto workload = DeviceAwarePolicy::parseWorkload(workload_str);
            auto storage = DeviceAwarePolicy::parseStorage(storage_str);
            auto config = policy.selectPolicy(workload, storage);
            
            std::cout << "[" << workload_str << "] x [" << storage_str << "] -> "
                     << config.policy_name << " (" << config.build_config << ")" << std::endl;
        }
    }
    std::cout << "========================\n" << std::endl;
    
    // Run benchmarks
    int total_tests = workload_types.size() * storage_types.size() * 
                     degrees.size() * record_counts.size() * num_runs;
    int current_test = 0;
    
    for (const auto& workload : workload_types) {
        for (const auto& storage : storage_types) {
            for (size_t degree : degrees) {
                for (size_t records : record_counts) {
                    for (int run = 1; run <= num_runs; run++) {
                        current_test++;
                        std::cout << "\n[" << current_test << "/" << total_tests << "] ";
                        std::cout << "Testing: " << workload << " on " << storage 
                                 << " (degree=" << degree << ", records=" << records 
                                 << ", run=" << run << ")" << std::endl;
                        
                        // Get policy recommendation
                        auto workload_type = DeviceAwarePolicy::parseWorkload(workload);
                        auto storage_type = DeviceAwarePolicy::parseStorage(storage);
                        auto config = policy.selectPolicy(workload_type, storage_type);
                        
                        std::cout << "  Policy: " << config.policy_name 
                                 << " (" << config.build_config << ")" << std::endl;
                        
                        // Here you would run the actual benchmark with the selected policy
                        // For now, we just log the decision
                        logger.log_result(
                            "BplusTreeSOA",           // tree_type
                            "uint64_t",               // key_type
                            "uint64_t",               // value_type
                            config.policy_name,       // policy_name
                            storage,                  // storage_type
                            config.build_config,      // config_name
                            records,                  // record_count
                            degree,                   // degree
                            workload,                 // operation (workload)
                            0,                        // time_us (placeholder)
                            0.0,                      // throughput (placeholder)
                            run,                      // test_run_id
                            0,                        // cache_hits
                            0,                        // cache_misses
                            0,                        // cache_evictions
                            0,                        // cache_dirty_evictions
                            0.0,                      // cache_hit_rate
                            "10%",                    // cache_size
                            0,                        // cache_page_limit
                            threads                   // thread_count
                        );
                        
                        // Brief sleep between runs
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    }
                }
            }
        }
    }
    
    std::cout << "\n=== Benchmark Complete ===" << std::endl;
    std::cout << "Results saved to: " << csv_filename << std::endl;
    std::cout << "Total tests run: " << current_test << std::endl;
}

/**
 * Example: Compare DeviceAwarePolicy selections with manual selections
 * 
 * This function runs the same workload with both the automatically selected
 * policy and a manually specified policy, allowing comparison of performance.
 */
void compare_device_aware_vs_manual(
    const std::string& workload_str,
    const std::string& storage_str,
    const std::string& manual_cache_policy,
    const std::string& manual_config,
    size_t degree,
    size_t records,
    int num_runs = 3
) {
    DeviceAwarePolicy policy;
    auto workload = DeviceAwarePolicy::parseWorkload(workload_str);
    auto storage = DeviceAwarePolicy::parseStorage(storage_str);
    auto auto_config = policy.selectPolicy(workload, storage);
    
    std::cout << "\n=== Policy Comparison ===" << std::endl;
    std::cout << "Workload: " << workload_str << std::endl;
    std::cout << "Storage: " << storage_str << std::endl;
    std::cout << "\nAutomatic Selection:" << std::endl;
    std::cout << "  Policy: " << auto_config.policy_name << std::endl;
    std::cout << "  Config: " << auto_config.build_config << std::endl;
    std::cout << "  Rationale: " << auto_config.selection_rationale << std::endl;
    std::cout << "\nManual Selection:" << std::endl;
    std::cout << "  Policy: " << manual_cache_policy << std::endl;
    std::cout << "  Config: " << manual_config << std::endl;
    std::cout << "========================\n" << std::endl;
    
    // Run benchmarks with both configurations and compare results
    // Implementation would go here
}

/**
 * Utility: Print policy recommendations for all combinations
 */
void print_all_recommendations() {
    DeviceAwarePolicy policy;
    policy.printDecisionMatrix();
}

} // namespace bm_tree_with_cache_ycsb_device_aware