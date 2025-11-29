#pragma once

#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <algorithm>
#include <numeric>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#ifdef __GLIBC__
#include <malloc.h>
#endif

// Must include common.h first to define WAL_FILE_PATH and other constants
#include "common.h"
#include "csv_logger.hpp"

#ifdef __TREE_WITH_CACHE__

// Include all necessary headers for cache operations
#include "LRUCache.hpp"
#include "LRUCacheObject.hpp"
#include "VolatileStorage.hpp"
#include "FileStorage.hpp"
#include "ObjectFatUID.h"
#include "DataNodeCOpt.hpp"
#include "IndexNodeCOpt.hpp"
#include "PMemWAL.hpp"

namespace bm_tree_with_cache_working {

// Define GenericCOptTraits exactly like in the working tests
template <
    typename TKeyType,
    typename TValueType,
    uint8_t TDataNodeUID,
    uint8_t TIndexNodeUID,
    typename TObjectUIDType,
    template<typename> class TCacheObjectType,
    template<typename> class TCacheType,
    template<typename> class TStorageType = NoStorage
>
struct GenericCOptTraits
{
    using KeyType = TKeyType;
    using ValueType = TValueType;
    using ObjectUIDType = TObjectUIDType;

    static constexpr uint8_t DataNodeUID = TDataNodeUID;
    static constexpr uint8_t IndexNodeUID = TIndexNodeUID;

    // Core Objects
    using DataNodeType = DataNodeCOpt<GenericCOptTraits>;
    using IndexNodeType = IndexNodeCOpt<GenericCOptTraits>;
    using ObjectType = TCacheObjectType<GenericCOptTraits>;

    // Cache and Storage
    using CacheType = TCacheType<GenericCOptTraits>;
    using StorageType = TStorageType<GenericCOptTraits>;

    // Store
    using StoreType = BPlusStore<GenericCOptTraits>;

    using WALType = PMemWAL<GenericCOptTraits>;
};

// Template function to run benchmark with specific cache and storage types
template<
    template<typename> class CacheType,
    template<typename> class CacheObjectType,
    template<typename> class StorageType
>
void run_cache_benchmark(
    const std::string& cache_name,
    const std::string& storage_name,
    int cache_size,
    int page_size,
    size_t memory_size,
    size_t degree,
    size_t records,
    const std::string& operation,
    CSVLogger& logger,
    int run_id
) {
    std::cout << "Testing " << cache_name << "/" << storage_name 
              << " - Records: " << records << ", Degree: " << degree 
              << ", Operation: " << operation << std::endl;

    // Use the exact same trait configuration as the working test
    using T = GenericCOptTraits<
        int,                   // Key type - same as working test
        int,                   // Value type - same as working test
        TYPE_UID::DATA_NODE_INT_INT,
        TYPE_UID::INDEX_NODE_INT_INT,
        ObjectFatUID,          // Object UID type
        CacheObjectType,
        CacheType,
        StorageType
    >;

    using MyStore = typename T::StoreType;
    
    std::cout << "Creating tree with " << cache_name << " cache..." << std::endl;
    
    // Create tree exactly like the working test
    MyStore* ptrTree = new MyStore(degree, cache_size, page_size, memory_size);
    ptrTree->template init<typename T::DataNodeType>();
    
    // Generate test data
    std::vector<int> vtNumberData(records);
    std::iota(vtNumberData.begin(), vtNumberData.end(), 1);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(vtNumberData.begin(), vtNumberData.end(), g);
    
    auto begin = std::chrono::steady_clock::now();
    
    if (operation == "insert") {
        // Insert operations
        for (size_t nCntr = 0; nCntr < records; nCntr++) {
            ErrorCode ec = ptrTree->insert(vtNumberData[nCntr], vtNumberData[nCntr]);
            if (ec != ErrorCode::Success) {
                std::cout << "Insert failed at record " << nCntr << std::endl;
                break;
            }
        }
    } else if (operation.find("search") == 0) {
        // First insert all data
        for (size_t nCntr = 0; nCntr < records; nCntr++) {
            ErrorCode ec = ptrTree->insert(vtNumberData[nCntr], vtNumberData[nCntr]);
            if (ec != ErrorCode::Success) {
                std::cout << "Insert failed at record " << nCntr << std::endl;
                break;
            }
        }
        
        // Then perform search operations
        std::vector<int> searchData = vtNumberData;
        if (operation == "search_sequential") {
            std::sort(searchData.begin(), searchData.end());
        } else if (operation == "search_random") {
            std::shuffle(searchData.begin(), searchData.end(), g);
        }
        
        // Reset timer for search operations
        begin = std::chrono::steady_clock::now();
        
        int found_count = 0;
        for (size_t nCntr = 0; nCntr < records; nCntr++) {
            int value;
            ErrorCode ec = ptrTree->search(searchData[nCntr], value);
            if (ec == ErrorCode::Success) {
                found_count++;
            }
        }
        std::cout << "Found " << found_count << " out of " << records << " records" << std::endl;
    } else if (operation == "delete") {
        // First insert all data
        for (size_t nCntr = 0; nCntr < records; nCntr++) {
            ErrorCode ec = ptrTree->insert(vtNumberData[nCntr], vtNumberData[nCntr]);
            if (ec != ErrorCode::Success) {
                std::cout << "Insert failed at record " << nCntr << std::endl;
                break;
            }
        }
        
        // Reset timer for delete operations
        begin = std::chrono::steady_clock::now();
        
        // Then perform delete operations
        for (size_t nCntr = 0; nCntr < records; nCntr++) {
            ErrorCode ec = ptrTree->remove(vtNumberData[nCntr]);
            if (ec != ErrorCode::Success) {
                std::cout << "Delete failed at record " << nCntr << std::endl;
                break;
            }
        }
    }
    
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    
    double throughput = (records * 1000000.0) / duration.count();
    
    std::cout << "Time taken: " << duration.count() << " microseconds" << std::endl;
    std::cout << "Throughput: " << throughput << " ops/sec" << std::endl;
    
    // Log results to CSV
    logger.log_result(
        "BplusTreeSOA",     // tree_type
        "int",              // key_type
        "int",              // value_type
        cache_name + "_" + storage_name + "_cache_benchmark", // workload_type
        records,            // record_count
        degree,             // degree
        operation,          // operation
        duration.count(),   // time_us
        throughput,         // throughput_ops_sec
        run_id              // test_run_id
    );
    
    // Cleanup
    delete ptrTree;
}

// Main test function that tests different cache and storage combinations
void test_cache_combinations(
    const std::vector<std::string>& cache_types,
    const std::vector<std::string>& storage_types,
    const std::vector<int>& cache_sizes,
    const std::vector<size_t>& degrees,
    const std::vector<size_t>& record_counts,
    const std::vector<std::string>& operations,
    int page_size = 2048,
    size_t memory_size = 1073741824LL,
    int num_runs = 1
) {
    // Create CSV logger with timestamp
    std::string csv_filename = CSVLogger::generate_filename("benchmark_cache_combinations");
    CSVLogger logger(csv_filename);
    logger.write_header();
    std::cout << "Logging results to: " << csv_filename << std::endl;
    
    for (int run = 1; run <= num_runs; run++) {
        std::cout << "\n=== Run " << run << "/" << num_runs << " ===" << std::endl;
        
        for (const std::string& cache_type : cache_types) {
            for (const std::string& storage_type : storage_types) {
                for (int cache_size : cache_sizes) {
                    for (size_t degree : degrees) {
                        for (size_t records : record_counts) {
                            for (const std::string& operation : operations) {
                                std::cout << "\n--- Testing " << cache_type << "/" << storage_type 
                                         << " (cache_size=" << cache_size << ") ---" << std::endl;
                                
                                // For now, only support LRU cache to avoid compatibility issues
                                if (cache_type == "LRU" && storage_type == "VolatileStorage") {
                                    run_cache_benchmark<LRUCache, LRUCacheObject, VolatileStorage>(
                                        cache_type, storage_type, cache_size, page_size, memory_size,
                                        degree, records, operation, logger, run);
                                } else if (cache_type == "LRU" && storage_type == "FileStorage") {
                                    run_cache_benchmark<LRUCache, LRUCacheObject, FileStorage>(
                                        cache_type, storage_type, cache_size, page_size, memory_size,
                                        degree, records, operation, logger, run);
                                } else {
                                    std::cout << "Currently only LRU cache with VolatileStorage/FileStorage is supported." << std::endl;
                                    std::cout << "Requested: " << cache_type << "/" << storage_type << std::endl;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    std::cout << "\nCache benchmark completed!" << std::endl;
    std::cout << "Results saved to: " << csv_filename << std::endl;
}

// Simple test function for quick verification
void test_simple_cache() {
    std::cout << "\n=== Simple Cache Test ===" << std::endl;
    
    std::vector<std::string> cache_types = {"LRU"};
    std::vector<std::string> storage_types = {"VolatileStorage"};
    std::vector<int> cache_sizes = {100};
    std::vector<size_t> degrees = {64};
    std::vector<size_t> record_counts = {10000};
    std::vector<std::string> operations = {"insert"};
    
    test_cache_combinations(
        cache_types, storage_types, cache_sizes, degrees, record_counts, operations,
        2048, 1073741824LL, 1
    );
}

// Function for backward compatibility with existing benchmark.cpp
void test_with_cache_type_string(const std::string& cache_type, int num_runs = 1) {
    std::cout << "\n=== Testing B+ Tree with " << cache_type << " Cache ===" << std::endl;
    
    std::vector<std::string> cache_types = {cache_type};
    std::vector<std::string> storage_types = {"VolatileStorage", "FileStorage"};
    std::vector<int> cache_sizes = {100, 500};
    std::vector<size_t> degrees = {64, 128};
    std::vector<size_t> record_counts = {100000, 500000};
    std::vector<std::string> operations = {"insert", "search_random", "search_sequential", "delete"};
    
    test_cache_combinations(
        cache_types, storage_types, cache_sizes, degrees, record_counts, operations,
        2048, 1073741824LL, num_runs
    );
}

// Single configuration test function for compatibility with benchmark.cpp
void test_single_config(
    const std::string& tree_type,
    const std::string& key_type,
    const std::string& value_type,
    const std::string& operation,
    size_t degree,
    size_t records,
    size_t runs,
    const std::string& output_dir,
    const std::string& cache_type = "LRU",
    const std::string& storage_type = "VolatileStorage",
    int cache_size = 100,
    int page_size = 2048,
    long long memory_size = 1073741824LL
) {
    std::cout << "\n=== Single Cache Configuration Test ===" << std::endl;
    std::cout << "Cache: " << cache_type << "/" << storage_type << std::endl;
    std::cout << "Tree: " << tree_type << ", Operation: " << operation << std::endl;
    std::cout << "Records: " << records << ", Degree: " << degree << ", Runs: " << runs << std::endl;
    
    // Create CSV logger
    std::string csv_filename = CSVLogger::generate_filename("benchmark_cache_single");
    CSVLogger logger(csv_filename);
    logger.write_header();
    std::cout << "Logging results to: " << csv_filename << std::endl;
    
    // Run the single configuration test
    std::vector<std::string> cache_types = {cache_type};
    std::vector<std::string> storage_types = {storage_type};
    std::vector<int> cache_sizes = {cache_size};
    std::vector<size_t> degrees = {degree};
    std::vector<size_t> record_counts = {records};
    std::vector<std::string> operations = {operation};
    
    test_cache_combinations(
        cache_types, storage_types, cache_sizes, degrees, record_counts, operations,
        page_size, memory_size, runs
    );
}

} // namespace bm_tree_with_cache_working

#endif // __TREE_WITH_CACHE__