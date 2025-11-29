#ifndef BM_TREE_WITH_CACHE_HPP
#define BM_TREE_WITH_CACHE_HPP

#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <memory>
#include <algorithm>
#include <random>
#include <cstdlib>
#include <cstring>

// Include tree-related headers
#include "BPlusStoreTraits.hpp"
#include "BplusTreeSOA.hpp"
#include "BplusTreeAOS.hpp"
#include "common.h"
#include "workloadgenerator.hpp"
#include "csv_logger.hpp"

// Include cache-related headers
#include "LRUCache.hpp"
#include "LRUCacheObject.hpp"
#include "CLOCKCache.hpp"
#include "CLOCKCacheObject.hpp"
#include "A2QCache.hpp"
#include "A2QCacheObject.hpp"
#include "VolatileStorage.hpp"
#include "FileStorage.hpp"
#include "ObjectFatUID.h"

// Include node types for cache operations
#include "DataNode.hpp"
#include "IndexNode.hpp"

namespace bm_tree_with_cache {

// Force memory cleanup function
void force_memory_cleanup() {
    std::cout << "    Forcing memory cleanup..." << std::endl;
    
    // Force garbage collection and memory cleanup
    std::system("sync");
    
    // Small delay to allow system to settle
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    std::cout << "    Memory cleanup completed." << std::endl;
}

// Template function to test a single operation with specific tree traits
template<typename TreeTraits, typename KeyType, typename ValueType>
void test_single_operation(
    const std::vector<KeyType>& data,
    const std::string& tree_name,
    const std::string& cache_name,
    const std::string& storage_name,
    size_t cache_size,
    const std::string& operation,
    size_t degree,
    CSVLogger& logger,
    size_t test_run_id
) {
    using TreeType = BplusTreeSOA<TreeTraits>;
    
    std::cout << "\n--- Testing " << tree_name << " with " << cache_name << "/" << storage_name 
              << " (Cache Size: " << cache_size << ") ---" << std::endl;
    std::cout << "Operation: " << operation << ", Degree: " << degree << ", Records: " << data.size() << std::endl;
    
    // Create tree instance
    auto tree = std::make_shared<TreeType>(degree);
    
    // Measure operation time
    auto start_time = std::chrono::high_resolution_clock::now();
    
    if (operation == "insert") {
        for (const auto& key : data) {
            ValueType value;
            if constexpr (std::is_same_v<ValueType, uint64_t>) {
                value = key;
            } else if constexpr (std::is_same_v<ValueType, CHAR16>) {
                std::memcpy(value.data, &key, std::min(sizeof(key), sizeof(value.data)));
            }
            tree->insert(key, value);
        }
    } else if (operation == "search_random" || operation == "search_sequential" || 
               operation == "search_uniform" || operation == "search_zipfian") {
        // First insert all data
        for (const auto& key : data) {
            ValueType value;
            if constexpr (std::is_same_v<ValueType, uint64_t>) {
                value = key;
            } else if constexpr (std::is_same_v<ValueType, CHAR16>) {
                std::memcpy(value.data, &key, std::min(sizeof(key), sizeof(value.data)));
            }
            tree->insert(key, value);
        }
        
        // Reset timer for search operation
        start_time = std::chrono::high_resolution_clock::now();
        
        // Perform searches
        for (const auto& key : data) {
            ValueType result;
            tree->search(key, result);
        }
    } else if (operation == "delete") {
        // First insert all data
        for (const auto& key : data) {
            ValueType value;
            if constexpr (std::is_same_v<ValueType, uint64_t>) {
                value = key;
            } else if constexpr (std::is_same_v<ValueType, CHAR16>) {
                std::memcpy(value.data, &key, std::min(sizeof(key), sizeof(value.data)));
            }
            tree->insert(key, value);
        }
        
        // Reset timer for delete operation
        start_time = std::chrono::high_resolution_clock::now();
        
        // Perform deletions
        for (const auto& key : data) {
            tree->remove(key);
        }
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto operation_time = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    
    // Calculate throughput
    double throughput = (static_cast<double>(data.size()) / operation_time) * 1000000.0;
    
    std::cout << "    " << operation << " [" << data.size() << " records]: " << operation_time << " us" << std::endl;
    
    // Get type names for logging
    std::string key_type_name = typeid(KeyType).name();
    std::string value_type_name = typeid(ValueType).name();
    if (key_type_name.find("uint64") != std::string::npos) key_type_name = "uint64_t";
    if (key_type_name.find("CHAR16") != std::string::npos) key_type_name = "char16";
    if (value_type_name.find("uint64") != std::string::npos) value_type_name = "uint64_t";
    if (value_type_name.find("CHAR16") != std::string::npos) value_type_name = "char16";
    
    // Log results
    logger.log_result(tree_name, key_type_name, value_type_name, "cache_benchmark", data.size(), degree, 
                     operation, operation_time, throughput, test_run_id);
    
    std::cout << "    Throughput: " << (int)throughput << " ops/sec" << std::endl;
    
    // Force memory cleanup after test
    force_memory_cleanup();
}

// Template helper for single configuration with specific cache type
template<template<typename> class CacheType, template<typename> class CacheObjectType>
void test_single_config_with_cache_type(
    const std::string& tree_type,
    const std::string& key_type,
    const std::string& value_type,
    const std::string& operation,
    size_t degree,
    size_t records,
    size_t runs,
    const std::string& output_dir,
    const std::string& cache_type_name
) {
    std::cout << "\n=== Cache Benchmark Single Configuration ===" << std::endl;
    std::cout << "Tree: " << tree_type << std::endl;
    std::cout << "Cache Type: " << cache_type_name << std::endl;
    std::cout << "Key Type: " << key_type << std::endl;
    std::cout << "Value Type: " << value_type << std::endl;
    std::cout << "Operation: " << operation << std::endl;
    std::cout << "Degree: " << degree << std::endl;
    std::cout << "Records: " << records << std::endl;
    std::cout << "Runs: " << runs << std::endl;
    
    // Create CSV logger
    std::string csv_filename;
    if (!output_dir.empty()) {
        csv_filename = CSVLogger::generate_filename("benchmark_single");
        // Replace the default path with output_dir
        size_t pos = csv_filename.find_last_of('/');
        if (pos != std::string::npos) {
            csv_filename = output_dir + "/" + csv_filename.substr(pos + 1);
        } else {
            csv_filename = output_dir + "/" + csv_filename;
        }
    } else {
        csv_filename = CSVLogger::generate_filename("benchmark_cache_single");
    }
    
    CSVLogger logger(csv_filename);
    logger.write_header();
    std::cout << "Logging results to: " << csv_filename << std::endl;
    
    // Generate test data based on operation type
    std::vector<uint64_t> uint64_data;
    std::vector<CHAR16> char16_data;
    
    if (key_type == "uint64_t") {
        if (operation == "search_random") {
            workloadgenerator::generate_data(records, workloadgenerator::DistributionType::Random, uint64_data);
        } else if (operation == "search_sequential") {
            workloadgenerator::generate_data(records, workloadgenerator::DistributionType::Sequential, uint64_data);
        } else if (operation == "search_uniform") {
            workloadgenerator::generate_data(records, workloadgenerator::DistributionType::Uniform, uint64_data);
        } else if (operation == "search_zipfian") {
            workloadgenerator::generate_data(records, workloadgenerator::DistributionType::Zipfian, uint64_data);
        } else {
            // For insert and delete operations, use random data
            workloadgenerator::generate_data(records, workloadgenerator::DistributionType::Random, uint64_data);
        }
    } else if (key_type == "char16") {
        if (operation == "search_random") {
            workloadgenerator::generate_data(records, workloadgenerator::DistributionType::Random, char16_data);
        } else if (operation == "search_sequential") {
            workloadgenerator::generate_data(records, workloadgenerator::DistributionType::Sequential, char16_data);
        } else if (operation == "search_uniform") {
            workloadgenerator::generate_data(records, workloadgenerator::DistributionType::Uniform, char16_data);
        } else if (operation == "search_zipfian") {
            workloadgenerator::generate_data(records, workloadgenerator::DistributionType::Zipfian, char16_data);
        } else {
            // For insert and delete operations, use random data
            workloadgenerator::generate_data(records, workloadgenerator::DistributionType::Random, char16_data);
        }
    }
    
    for (size_t run = 1; run <= runs; run++) {
        std::cout << "\n--- Run " << run << "/" << runs << " ---" << std::endl;
        
        // Test configurations
        std::vector<std::pair<std::string, size_t>> cache_configs = {
            {cache_type_name + "_1000", 1000}
        };
        
        for (const auto& cache_config : cache_configs) {
            const std::string& cache_name = cache_config.first;
            size_t cache_size = cache_config.second;
            
            // Test different tree types and key-value combinations
            if (tree_type == "BplusTreeSOA") {
                if (key_type == "uint64_t" && value_type == "uint64_t") {
                    using TreeTraits = BPlusStoreTraits<uint64_t, uint64_t, TYPE_UID::DATA_NODE_INT_INT, 
                                                         TYPE_UID::INDEX_NODE_INT_INT, ObjectFatUID, 
                                                         CacheObjectType, CacheType, VolatileStorage>;
                    test_single_operation<TreeTraits, uint64_t, uint64_t>(
                        uint64_data, tree_type, cache_name, "VolatileStorage", cache_size, 
                        operation, degree, logger, run);
                } else if (key_type == "uint64_t" && value_type == "char16") {
                    using TreeTraits = BPlusStoreTraits<uint64_t, CHAR16, TYPE_UID::DATA_NODE_INT_INT, 
                                                         TYPE_UID::INDEX_NODE_INT_INT, ObjectFatUID, 
                                                         CacheObjectType, CacheType, VolatileStorage>;
                    test_single_operation<TreeTraits, uint64_t, CHAR16>(
                        uint64_data, tree_type, cache_name, "VolatileStorage", cache_size, 
                        operation, degree, logger, run);
                } else if (key_type == "char16" && value_type == "char16") {
                    using TreeTraits = BPlusStoreTraits<CHAR16, CHAR16, TYPE_UID::DATA_NODE_INT_INT, 
                                                         TYPE_UID::INDEX_NODE_INT_INT, ObjectFatUID, 
                                                         CacheObjectType, CacheType, VolatileStorage>;
                    test_single_operation<TreeTraits, CHAR16, CHAR16>(
                        char16_data, tree_type, cache_name, "VolatileStorage", cache_size, 
                        operation, degree, logger, run);
                }
            }
            // Add more tree types as needed...
        }
    }
    
    std::cout << "\nCache benchmark single configuration completed!" << std::endl;
}

// Test function for single configuration
void test_single_config(
    const std::string& tree_type,
    const std::string& key_type,
    const std::string& value_type,
    const std::string& operation,
    size_t degree,
    size_t records,
    size_t runs,
    const std::string& output_dir,
    const std::string& cache_type = "LRU"
) {
    // Dispatch to the appropriate cache type
    if (cache_type == "LRU") {
        test_single_config_with_cache_type<LRUCache, LRUCacheObject>(
            tree_type, key_type, value_type, operation, degree, records, runs, output_dir, cache_type);
    } else if (cache_type == "CLOCK") {
        test_single_config_with_cache_type<CLOCKCache, CLOCKCacheObject>(
            tree_type, key_type, value_type, operation, degree, records, runs, output_dir, cache_type);
    } else if (cache_type == "A2Q") {
        test_single_config_with_cache_type<A2QCache, A2QCacheObject>(
            tree_type, key_type, value_type, operation, degree, records, runs, output_dir, cache_type);
    } else {
        std::cerr << "Error: Unknown cache type: " << cache_type << std::endl;
        std::cerr << "Available cache types: LRU, CLOCK, A2Q" << std::endl;
    }
}

// Template function to run tests with specific cache type
template<template<typename> class CacheType, template<typename> class CacheObjectType>
void test_with_cache_type(const std::string& cache_name, int num_runs = 1) {
    std::cout << "\n=== Testing B+ Tree with " << cache_name << " Cache ===" << std::endl;
    std::cout << "Number of runs per configuration: " << num_runs << std::endl;

    // Create CSV logger with timestamp
    std::string csv_filename = CSVLogger::generate_filename("benchmark_" + cache_name + "_cache");
    CSVLogger logger(csv_filename);
    logger.write_header();
    std::cout << "Logging results to: " << csv_filename << std::endl;

    // Test configurations
    std::vector<size_t> record_counts = {100000, 500000, 1000000};
    std::vector<size_t> degrees = {64, 128, 256};
    std::vector<std::string> operations = {"insert", "search_random", "search_sequential", "delete"};
    std::vector<std::pair<std::string, size_t>> cache_configs = {
        {cache_name + "_100", 100},
        {cache_name + "_500", 500},
        {cache_name + "_1000", 1000},
        {cache_name + "_2000", 2000}
    };

    // Generate test data
    std::vector<uint64_t> uint64_data_100k, uint64_data_500k, uint64_data_1m;
    workloadgenerator::generate_data(100000, workloadgenerator::DistributionType::Random, uint64_data_100k);
    workloadgenerator::generate_data(500000, workloadgenerator::DistributionType::Random, uint64_data_500k);
    workloadgenerator::generate_data(1000000, workloadgenerator::DistributionType::Random, uint64_data_1m);

    for (int run = 0; run < num_runs; run++) {
        std::cout << "\n=== Run " << (run + 1) << "/" << num_runs << " ===" << std::endl;
        
        for (const auto& cache_config : cache_configs) {
            const std::string& cache_config_name = cache_config.first;
            size_t cache_size = cache_config.second;
            
            for (size_t degree : degrees) {
                for (const std::string& operation : operations) {
                    // Test with different record counts
                    for (size_t records : record_counts) {
                        std::vector<uint64_t>* data_ptr = nullptr;
                        if (records == 100000) data_ptr = &uint64_data_100k;
                        else if (records == 500000) data_ptr = &uint64_data_500k;
                        else if (records == 1000000) data_ptr = &uint64_data_1m;
                        
                        if (data_ptr) {
                            // Test BPlusTreeSOA with uint64_t key and value using the specified cache type
                            using TreeTraits = BPlusStoreTraits<uint64_t, uint64_t, TYPE_UID::DATA_NODE_INT_INT, 
                                                                 TYPE_UID::INDEX_NODE_INT_INT, ObjectFatUID, 
                                                                 CacheObjectType, CacheType, VolatileStorage>;
                            test_single_operation<TreeTraits, uint64_t, uint64_t>(
                                *data_ptr, "BplusTreeSOA", cache_config_name, "VolatileStorage", cache_size, 
                                operation, degree, logger, run + 1);
                        }
                    }
                }
            }
        }
    }

    std::cout << "\n" << cache_name << " cache benchmark completed!" << std::endl;
}

// Function to run tests with cache type specified by string
void test_with_cache_type_string(const std::string& cache_type, int num_runs = 1) {
    if (cache_type == "LRU") {
        test_with_cache_type<LRUCache, LRUCacheObject>("LRU", num_runs);
    } else if (cache_type == "CLOCK") {
        test_with_cache_type<CLOCKCache, CLOCKCacheObject>("CLOCK", num_runs);
    } else if (cache_type == "A2Q") {
        test_with_cache_type<A2QCache, A2QCacheObject>("A2Q", num_runs);
    } else {
        std::cerr << "Error: Unknown cache type: " << cache_type << std::endl;
        std::cerr << "Available cache types: LRU, CLOCK, A2Q" << std::endl;
    }
}

// Main test function for cache benchmarks - defaults to LRU for backward compatibility
void test(int num_runs = 1) {
    test_with_cache_type_string("LRU", num_runs);
}

} // namespace bm_tree_with_cache

#endif // BM_TREE_WITH_CACHE_HPP