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
// We don't need to include workloadgenerator.hpp here since it's already included in benchmark.cpp

#ifndef __TREE_WITH_CACHE__
namespace bm_tree_with_no_cache {

// Load data from binary file
template<typename T>
std::vector<T> load_data_from_file(const std::string& filepath) {
    std::ifstream file(filepath, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }
    
    size_t count;
    file.read(reinterpret_cast<char*>(&count), sizeof(count));
    
    std::vector<T> data(count);
    file.read(reinterpret_cast<char*>(data.data()), count * sizeof(T));
    file.close();
    
    return data;
}

// Force memory cleanup to reduce memory pressure between tests
void force_memory_cleanup() {
    std::cout << "    Forcing memory cleanup..." << std::endl;
    
    // Force garbage collection and memory cleanup
    #ifdef __GLIBC__
    // Return unused memory to the OS
    malloc_trim(0);
    #endif
    
    // Give OS time to reclaim memory
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    std::cout << "    Memory cleanup completed." << std::endl;
}

// New multi-workload test function - handles insert/delete with unique data and search with multiple distributions
template <typename TreeTraits, typename KeyType, typename ValueType>
void test_internal_multi_workload(
    const std::vector<KeyType>& insert_delete_data,  // Random, no duplicates
    const std::vector<KeyType>& search_random_data,
    const std::vector<KeyType>& search_sequential_data, 
    const std::vector<KeyType>& search_uniform_data,
    const std::vector<KeyType>& search_zipfian_data,
    const std::string& tree_name,
    CSVLogger& logger,
    int test_run_id
) {
    std::cout << "\n--- Testing " << tree_name << " ---" << std::endl;
    
    size_t nMaxNumber = insert_delete_data.size();
    std::vector<size_t> degrees = { 16, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256 }; // Removed 110 to avoid crash
    
    // Determine type names for logging
    std::string key_type_name, value_type_name;
    if constexpr (std::is_same_v<KeyType, uint64_t>) {
        key_type_name = "uint64_t";
    } else if constexpr (std::is_same_v<KeyType, CHAR16>) {
        key_type_name = "char16";
    }
    
    if constexpr (std::is_same_v<ValueType, uint64_t>) {
        value_type_name = "uint64_t";
    } else if constexpr (std::is_same_v<ValueType, CHAR16>) {
        value_type_name = "char16";
    }
    
    for (size_t nDegree : degrees) {
        std::cout << "\n  Degree: " << nDegree << std::endl;
        
        // Create tree instance
        typename TreeTraits::StoreType ptrTree(nDegree);
        ptrTree.template init<typename TreeTraits::DataNodeType>();
        
        auto begin = std::chrono::steady_clock::now();
        auto end = std::chrono::steady_clock::now();
        
        // Phase 1: Insert using unique random data (no duplicates)
        begin = std::chrono::steady_clock::now();
        for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr++) {
            ErrorCode ec = ErrorCode::InsertFailed;
            if constexpr (std::is_same_v<ValueType, CHAR16> && std::is_same_v<KeyType, uint64_t>) {
                ec = ptrTree.insert(insert_delete_data[nCntr], CHAR16("constant_value"));
            } else if constexpr (std::is_same_v<ValueType, CHAR16> && std::is_same_v<KeyType, CHAR16>) {
                ec = ptrTree.insert(insert_delete_data[nCntr], CHAR16("constant_value"));
            } else {
                ec = ptrTree.insert(insert_delete_data[nCntr], static_cast<ValueType>(insert_delete_data[nCntr]));
            }
            
            ASSERT (ec == ErrorCode::Success);
        }
        end = std::chrono::steady_clock::now();
        
        auto insert_time = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        std::cout << "    Insert [" << nMaxNumber << " records]: " << insert_time << " us" << std::endl;
        
        // Log insert results
        double insert_throughput = (double)nMaxNumber / insert_time * 1000000;
        std::string tree_type = tree_name;
        logger.log_result(tree_type, key_type_name, value_type_name, "multi_workload", nMaxNumber, nDegree, 
                         "insert", insert_time, insert_throughput, test_run_id);
        
        // Sleep after insert phase
        std::cout << "    Sleeping for 10 seconds after insert..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(4));
        
        // Phase 2: Search using different distributions
        std::vector<std::pair<std::string, std::vector<KeyType>>> search_workloads = {
            {"search_random", search_random_data},
            {"search_sequential", search_sequential_data},
            {"search_uniform", search_uniform_data},
            {"search_zipfian", search_zipfian_data}
        };
        
        for (const auto& workload_pair : search_workloads) {
            const std::string& workload_name = workload_pair.first;
            const std::vector<KeyType>& search_data = workload_pair.second;
            begin = std::chrono::steady_clock::now();
            for (size_t nCntr = 0; nCntr < search_data.size(); nCntr++) {
                ValueType value;
                ErrorCode ec = ptrTree.search(search_data[nCntr], value);
                ASSERT( ec == ErrorCode::Success);
            }
            end = std::chrono::steady_clock::now();
            
            auto search_time = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
            std::cout << "    " << workload_name << " [" << search_data.size() << " records]: " << search_time << " us" << std::endl;
            
            // Log search results
            double search_throughput = (double)search_data.size() / search_time * 1000000;
            logger.log_result(tree_type, key_type_name, value_type_name, "multi_workload", search_data.size(), nDegree, 
                             workload_name, search_time, search_throughput, test_run_id);

            // Sleep after search phase
            std::cout << "    Sleeping for 10 seconds after search..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(4));
        }
                
        // Phase 3: Delete using same unique data as insert
        begin = std::chrono::steady_clock::now();
        for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr++) {
            ErrorCode ec = ptrTree.remove(insert_delete_data[nCntr]);
            ASSERT (ec == ErrorCode::Success);
        }
        end = std::chrono::steady_clock::now();
        
        auto delete_time = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        std::cout << "    Delete [" << nMaxNumber << " records]: " << delete_time << " us" << std::endl;
        
        // Log delete results
        double delete_throughput = (double)nMaxNumber / delete_time * 1000000;
        logger.log_result(tree_type, key_type_name, value_type_name, "multi_workload", nMaxNumber, nDegree, 
                         "delete", delete_time, delete_throughput, test_run_id);
        
        // Sleep after delete phase
        std::cout << "    Sleeping for 10 seconds after delete..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(8));
        
        std::cout << "    Summary - Insert: " << insert_time << "us, Delete: " << delete_time << "us" << std::endl;
        std::cout << "    Throughput - Insert: " << (int)insert_throughput << " ops/sec, "
                  << "Delete: " << (int)delete_throughput << " ops/sec" << std::endl;
        
        // Force memory cleanup after each degree to prevent memory accumulation
        force_memory_cleanup();
    }
}

// Original test internal function template - handles tree construction and testing
template <typename TreeTraits, typename KeyType, typename ValueType>
void test_internal(const std::vector<KeyType>& vtData, const std::string& tree_name, 
                  CSVLogger& logger, const std::string& workload_type, int test_run_id) {
    std::cout << "\n--- Testing " << tree_name << " ---" << std::endl;
    
    size_t nMaxNumber = vtData.size();
    std::vector<size_t> degrees = { 64, 92, 110 };// , 128, 208, 224, 240, 256, 272, 288};
    
    // Determine type names for logging
    std::string key_type_name, value_type_name;
    if constexpr (std::is_same_v<KeyType, uint64_t>) {
        key_type_name = "uint64_t";
    } else if constexpr (std::is_same_v<KeyType, CHAR16>) {
        key_type_name = "CHAR16";
    }
    
    if constexpr (std::is_same_v<ValueType, uint64_t>) {
        value_type_name = "uint64_t";
    } else if constexpr (std::is_same_v<ValueType, CHAR16>) {
        value_type_name = "CHAR16";
    }
    
    for (size_t nDegree : degrees) {
        std::cout << "\n  Degree: " << nDegree << std::endl;

        // Construct the tree (no cache version)
        typename TreeTraits::StoreType ptrTree(nDegree);
        ptrTree.template init<typename TreeTraits::DataNodeType>();
        
        std::chrono::steady_clock::time_point begin, end;
        
        // Insert phase
        begin = std::chrono::steady_clock::now();
        for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr++) {
            ErrorCode ec = ErrorCode::InsertFailed;
            if constexpr (std::is_same_v<ValueType, CHAR16> && std::is_same_v<KeyType, uint64_t>) {
                // uint64_t key, CHAR16 value
                ec = ptrTree.insert(vtData[nCntr], CHAR16("constant_value"));
            } else if constexpr (std::is_same_v<ValueType, CHAR16> && std::is_same_v<KeyType, CHAR16>) {
                // CHAR16 key, CHAR16 value
                ec = ptrTree.insert(vtData[nCntr], CHAR16("constant_value"));
            } else {
                // uint64_t key, uint64_t value - use key as value
                ec = ptrTree.insert(vtData[nCntr], static_cast<ValueType>(vtData[nCntr]));
            }
            //ASSERT(ec == ErrorCode::Success); // for the timebeing.. disabling it due to duplicates
        }
        end = std::chrono::steady_clock::now();
        
        auto insert_time = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        std::cout << "    Insert [" << nMaxNumber << " records]: " << insert_time << " us" << std::endl;
        
        // Log insert results
        double insert_throughput = (double)nMaxNumber / insert_time * 1000000; // ops/sec
        std::string tree_type = tree_name;// .find("Bplus") != std::string::npos ? "BPlus" : "BEpsilon";
        logger.log_result(tree_type, key_type_name, value_type_name, workload_type, nMaxNumber, nDegree, 
                         "insert", insert_time, insert_throughput, test_run_id);
        
        // Search phase
        begin = std::chrono::steady_clock::now();
        for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr++) {
            ValueType value;
            ErrorCode ec = ptrTree.search(vtData[nCntr], value);
            ASSERT(ec == ErrorCode::Success);
        }
        end = std::chrono::steady_clock::now();
        
        auto search_time = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        std::cout << "    Search [" << nMaxNumber << " records]: " << search_time << " us" << std::endl;
        
        // Log search results
        double search_throughput = (double)nMaxNumber / search_time * 1000000; // ops/sec
        logger.log_result(tree_type, key_type_name, value_type_name, workload_type, nMaxNumber, nDegree, 
                         "search", search_time, search_throughput, test_run_id);
        
        // Delete phase
        begin = std::chrono::steady_clock::now();
        for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr++) {
            ErrorCode ec = ptrTree.remove(vtData[nCntr]);
            //ASSERT(ec == ErrorCode::Success); // for the timebeing.. disabling it due to duplicates
        }
        end = std::chrono::steady_clock::now();
        
        auto delete_time = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        std::cout << "    Delete [" << nMaxNumber << " records]: " << delete_time << " us" << std::endl;
        
        // Log delete results
        double delete_throughput = (double)nMaxNumber / delete_time * 1000000; // ops/sec
        logger.log_result(tree_type, key_type_name, value_type_name, workload_type, nMaxNumber, nDegree, 
                         "delete", delete_time, delete_throughput, test_run_id);
        
        std::cout << "    Summary - Insert: " << insert_time << "us, Search: " << search_time 
                  << "us, Delete: " << delete_time << "us" << std::endl;
        
        std::cout << "    Throughput - Insert: " << (int)insert_throughput << " ops/sec, "
                  << "Search: " << (int)search_throughput << " ops/sec, "
                  << "Delete: " << (int)delete_throughput << " ops/sec" << std::endl;
        
        // Force memory cleanup after each degree to prevent memory accumulation
        force_memory_cleanup();
    }
}

// Test with uint64_t key and uint64_t value for all workloads and record counts
void test_tree_with_key_and_value_as_uint64_t(int num_runs = 1) {
    std::cout << "\n=== Testing B+ Tree with No Cache (uint64_t key and uint64_t value) ===" << std::endl;
    std::cout << "Number of runs per configuration: " << num_runs << std::endl;

    // Create CSV logger with timestamp
    std::string csv_filename = CSVLogger::generate_filename("benchmark_no_cache_uint64_uint64");
    CSVLogger logger(csv_filename);
    logger.write_header();
    std::cout << "Logging results to: " << csv_filename << std::endl;

    using BPlusStoreSOA = BPlusStoreSOATrait <
        uint64_t,                           // Key type
        uint64_t,                           // Value type
        TYPE_UID::DATA_NODE_INT_INT,        // Data node type UID
        TYPE_UID::INDEX_NODE_INT_INT,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache                             // Cache type
    >;

    using BPlusStoreAOS = BPlusStoreAOSTrait <
        uint64_t,                           // Key type
        uint64_t,                           // Value type
        TYPE_UID::DATA_NODE_INT_INT,        // Data node type UID
        TYPE_UID::INDEX_NODE_INT_INT,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache                             // Cache type
    >;

    using BEpsilonStoreSOA = BEpsilonStoreSOATrait <
        uint64_t,                           // Key type
        uint64_t,                           // Value type
        TYPE_UID::DATA_NODE_INT_INT,        // Data node type UID
        TYPE_UID::INDEX_NODE_INT_INT,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache                             // Cache type
    >;

    using BEpsilonStoreSOALazyNodes = BEpsilonStoreSOATrait <
        uint64_t,                           // Key type
        uint64_t,                           // Value type
        TYPE_UID::DATA_NODE_INT_INT,        // Data node type UID
        TYPE_UID::INDEX_NODE_INT_INT,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache,                             // Cache type
        NoStorage,
        true,
        true
    >;

    using BEpsilonStoreSOALazyIndex = BEpsilonStoreSOATrait <
        uint64_t,                           // Key type
        uint64_t,                           // Value type
        TYPE_UID::DATA_NODE_INT_INT,        // Data node type UID
        TYPE_UID::INDEX_NODE_INT_INT,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache,                             // Cache type
        NoStorage,
        true,
        false
    >;

    using BEpsilonStoreAOS = BEpsilonStoreAOSTrait <
        uint64_t,                           // Key type
        uint64_t,                           // Value type
        TYPE_UID::DATA_NODE_INT_INT,        // Data node type UID
        TYPE_UID::INDEX_NODE_INT_INT,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache                             // Cache type
    >;

    using BEpsilonStoreSOAII = BEpsilonStoreSOAIITrait <
        uint64_t,                           // Key type
        uint64_t,                           // Value type
        TYPE_UID::DATA_NODE_INT_INT,        // Data node type UID
        TYPE_UID::INDEX_NODE_INT_INT,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache,                            // Cache type
        NoStorage,
        false,
        false
    >;


    // Test different record counts using multi-workload approach
    std::vector<size_t> record_counts = { 100000, 500000, 1000000, 5000000};
    
    for (size_t record_count : record_counts) {
        std::cout << "\n--- Testing with " << record_count << " records using multi-workload approach ---" << std::endl;
        
        try {
            // Load all workload data files
            std::string random_file = "data/uint64_random_" + std::to_string(record_count) + ".dat";
            std::string sequential_file = "data/uint64_sequential_" + std::to_string(record_count) + ".dat";
            std::string uniform_file = "data/uint64_uniform_" + std::to_string(record_count) + ".dat";
            std::string zipfian_file = "data/uint64_zipfian_" + std::to_string(record_count) + ".dat";
            
            std::cout << "Loading workload files:" << std::endl;
            std::cout << "  Random (insert/delete): " << random_file << std::endl;
            std::cout << "  Sequential (search): " << sequential_file << std::endl;
            std::cout << "  Uniform (search): " << uniform_file << std::endl;
            std::cout << "  Zipfian (search): " << zipfian_file << std::endl;
            
            // Load all datasets
            std::cout << "Loading random data..." << std::endl;
            std::vector<uint64_t> random_data = load_data_from_file<uint64_t>(random_file);
            std::cout << "Loading sequential data..." << std::endl;
            std::vector<uint64_t> sequential_data = load_data_from_file<uint64_t>(sequential_file);
            std::cout << "Loading uniform data..." << std::endl;
            std::vector<uint64_t> uniform_data = load_data_from_file<uint64_t>(uniform_file);
            std::cout << "Loading zipfian data..." << std::endl;
            std::vector<uint64_t> zipfian_data = load_data_from_file<uint64_t>(zipfian_file);
            
            // Resize to requested number of records if needed
            if (random_data.size() > record_count) random_data.resize(record_count);
            if (sequential_data.size() > record_count) sequential_data.resize(record_count);
            if (uniform_data.size() > record_count) uniform_data.resize(record_count);
            if (zipfian_data.size() > record_count) zipfian_data.resize(record_count);
            
            // Run multiple iterations for averaging
            for (int run_id = 1; run_id <= num_runs; run_id++) {
                std::cout << "\n  === Run " << run_id << " of " << num_runs << " ===" << std::endl;
                
                // Test B+ Tree
                test_internal_multi_workload<BPlusStoreSOA, uint64_t, uint64_t>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BplusTreeSOA", logger, run_id);

                test_internal_multi_workload<BPlusStoreAOS, uint64_t, uint64_t>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BplusTreeAOS", logger, run_id);

                // Test B-Epsilon Tree with multi-workload approach
                test_internal_multi_workload<BEpsilonStoreSOA, uint64_t, uint64_t>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BepsilonTreeSOA", logger, run_id);

                test_internal_multi_workload<BEpsilonStoreSOALazyNodes, uint64_t, uint64_t>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BepsilonTreeSOALazyNodes", logger, run_id);

                test_internal_multi_workload<BEpsilonStoreSOALazyIndex, uint64_t, uint64_t>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BepsilonTreeSOALazyIndex", logger, run_id);

                test_internal_multi_workload<BEpsilonStoreAOS, uint64_t, uint64_t>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BepsilonTreeAOS", logger, run_id);

                test_internal_multi_workload<BEpsilonStoreSOAII, uint64_t, uint64_t>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BepsilonTreeSOAII", logger, run_id);
                
                // Sleep between runs (but not after the last run)
                if (run_id < num_runs) {
                    std::cout << "\n    Sleeping for 10 seconds between runs..." << std::endl;
                    std::this_thread::sleep_for(std::chrono::seconds(10));
                }
            }
        }
        catch (const std::exception& e) {
            std::cout << "Error loading workload files: " << e.what() << std::endl;
            std::cout << "Skipping this record count..." << std::endl;
        }
    }
}

// Test with uint64_t key and CHAR16 value for all workloads and record counts
void test_tree_with_key_as_uint64_and_value_as_string(int num_runs = 1) {
    std::cout << "\n=== Testing B+ Tree with No Cache (uint64_t key and CHAR16 value) ===" << std::endl;
    std::cout << "Number of runs per configuration: " << num_runs << std::endl;

    // Create CSV logger with timestamp
    std::string csv_filename = CSVLogger::generate_filename("benchmark_no_cache_uint64_char16");
    CSVLogger logger(csv_filename);
    logger.write_header();
    std::cout << "Logging results to: " << csv_filename << std::endl;

    using BPlusStoreSOA = BPlusStoreSOATrait <
        uint64_t,                           // Key type
        CHAR16,                           // Value type
        TYPE_UID::DATA_NODE_INT_INT,        // Data node type UID
        TYPE_UID::INDEX_NODE_STRING_STRING,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache                             // Cache type
    >;

    using BPlusStoreAOS = BPlusStoreAOSTrait <
        uint64_t,                           // Key type
        CHAR16,                           // Value type
        TYPE_UID::DATA_NODE_INT_INT,        // Data node type UID
        TYPE_UID::INDEX_NODE_STRING_STRING,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache                             // Cache type
    >;

    using BEpsilonStoreSOA = BEpsilonStoreSOATrait <
        uint64_t,                           // Key type
        CHAR16,                           // Value type
        TYPE_UID::DATA_NODE_INT_INT,        // Data node type UID
        TYPE_UID::INDEX_NODE_STRING_STRING,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache                             // Cache type
    >;

    using BEpsilonStoreSOALazyNodes = BEpsilonStoreSOATrait <
        uint64_t,                           // Key type
        CHAR16,                           // Value type
        TYPE_UID::DATA_NODE_INT_INT,        // Data node type UID
        TYPE_UID::INDEX_NODE_STRING_STRING,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache,                             // Cache type
        NoStorage,
        true,
        true
    >;

    using BEpsilonStoreSOALazyIndex = BEpsilonStoreSOATrait <
        uint64_t,                           // Key type
        CHAR16,                           // Value type
        TYPE_UID::DATA_NODE_INT_INT,        // Data node type UID
        TYPE_UID::INDEX_NODE_STRING_STRING,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache,                             // Cache type
        NoStorage,
        true,
        false
    >;

    using BEpsilonStoreAOS = BEpsilonStoreAOSTrait <
        uint64_t,                           // Key type
        CHAR16,                           // Value type
        TYPE_UID::DATA_NODE_INT_INT,        // Data node type UID
        TYPE_UID::INDEX_NODE_STRING_STRING,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache                             // Cache type
    >;

    // Test different record counts using multi-workload approach
    std::vector<size_t> record_counts = { 100000, 500000, 1000000, 5000000};
    
    for (size_t record_count : record_counts) {
        std::cout << "\n--- Testing with " << record_count << " records using multi-workload approach ---" << std::endl;
        
        // Define workload file paths (using uint64_t data files since keys are uint64_t)
        std::string random_file = "data/uint64_random_" + std::to_string(record_count) + ".dat";
        std::string sequential_file = "data/uint64_sequential_" + std::to_string(record_count) + ".dat";
        std::string uniform_file = "data/uint64_uniform_" + std::to_string(record_count) + ".dat";
        std::string zipfian_file = "data/uint64_zipfian_" + std::to_string(record_count) + ".dat";
        
        std::cout << "Loading workload files:" << std::endl;
        std::cout << "  Random (insert/delete): " << random_file << std::endl;
        std::cout << "  Sequential (search): " << sequential_file << std::endl;
        std::cout << "  Uniform (search): " << uniform_file << std::endl;
        std::cout << "  Zipfian (search): " << zipfian_file << std::endl;
        
        try {
            // Load all workload data
            std::cout << "Loading random data..." << std::endl;
            std::vector<uint64_t> random_data = load_data_from_file<uint64_t>(random_file);
            std::cout << "Loading sequential data..." << std::endl;
            std::vector<uint64_t> sequential_data = load_data_from_file<uint64_t>(sequential_file);
            std::cout << "Loading uniform data..." << std::endl;
            std::vector<uint64_t> uniform_data = load_data_from_file<uint64_t>(uniform_file);
            std::cout << "Loading zipfian data..." << std::endl;
            std::vector<uint64_t> zipfian_data = load_data_from_file<uint64_t>(zipfian_file);
            
            // Resize all datasets to requested number of records if needed
            if (random_data.size() > record_count) random_data.resize(record_count);
            if (sequential_data.size() > record_count) sequential_data.resize(record_count);
            if (uniform_data.size() > record_count) uniform_data.resize(record_count);
            if (zipfian_data.size() > record_count) zipfian_data.resize(record_count);
            
            // Run multiple iterations for averaging
            for (int run_id = 1; run_id <= num_runs; run_id++) {
                std::cout << "\n  === Run " << run_id << " of " << num_runs << " ===" << std::endl;
                
                // Test B+ Tree
                test_internal_multi_workload<BPlusStoreSOA, uint64_t, CHAR16>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BplusTreeSOA", logger, run_id);

                test_internal_multi_workload<BPlusStoreAOS, uint64_t, CHAR16>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BplusTreeAOS", logger, run_id);

                // Test B-Epsilon Tree with multi-workload approach
                test_internal_multi_workload<BEpsilonStoreSOA, uint64_t, CHAR16>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BepsilonTreeSOA", logger, run_id);

                test_internal_multi_workload<BEpsilonStoreSOALazyNodes, uint64_t, CHAR16>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BepsilonTreeSOALazyNodes", logger, run_id);

                test_internal_multi_workload<BEpsilonStoreSOALazyIndex, uint64_t, CHAR16>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BepsilonTreeSOALazyIndex", logger, run_id);

                test_internal_multi_workload<BEpsilonStoreAOS, uint64_t, CHAR16>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BepsilonTreeAOS", logger, run_id);
                
                // Sleep between runs (but not after the last run)
                if (run_id < num_runs) {
                    std::cout << "\n    Sleeping for 10 seconds between runs..." << std::endl;
                    std::this_thread::sleep_for(std::chrono::seconds(10));
                }
            }
        }
        catch (const std::exception& e) {
            std::cout << "Error loading workload files: " << e.what() << std::endl;
            std::cout << "Skipping this record count..." << std::endl;
        }
    }
}

// Test with CHAR16 key and CHAR16 value for all workloads and record counts
void test_tree_with_key_and_value_as_string(int num_runs = 1) {
    std::cout << "\n=== Testing B+ Tree with No Cache (CHAR16 key and CHAR16 value) ===" << std::endl;
    std::cout << "Number of runs per configuration: " << num_runs << std::endl;

    // Create CSV logger with timestamp
    std::string csv_filename = CSVLogger::generate_filename("benchmark_no_cache_char16_char16");
    CSVLogger logger(csv_filename);
    logger.write_header();
    std::cout << "Logging results to: " << csv_filename << std::endl;

    using BPlusStoreSOA = BPlusStoreSOATrait <
        CHAR16,                           // Key type
        CHAR16,                           // Value type
        TYPE_UID::DATA_NODE_STRING_STRING,        // Data node type UID
        TYPE_UID::INDEX_NODE_STRING_STRING,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache                             // Cache type
    >;

    using BPlusStoreAOS = BPlusStoreAOSTrait <
        CHAR16,                           // Key type
        CHAR16,                           // Value type
        TYPE_UID::DATA_NODE_STRING_STRING,        // Data node type UID
        TYPE_UID::INDEX_NODE_STRING_STRING,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache                             // Cache type
    >;

    using BEpsilonStoreSOA = BEpsilonStoreSOATrait <
        CHAR16,                           // Key type
        CHAR16,                           // Value type
        TYPE_UID::DATA_NODE_STRING_STRING,        // Data node type UID
        TYPE_UID::INDEX_NODE_STRING_STRING,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache                             // Cache type
    >;

    using BEpsilonStoreSOALazyNodes = BEpsilonStoreSOATrait <
        CHAR16,                           // Key type
        CHAR16,                           // Value type
        TYPE_UID::DATA_NODE_STRING_STRING,        // Data node type UID
        TYPE_UID::INDEX_NODE_STRING_STRING,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache,                             // Cache type
        NoStorage,
        true,
        true
    >;

    using BEpsilonStoreSOALazyIndex = BEpsilonStoreSOATrait <
        CHAR16,                           // Key type
        CHAR16,                           // Value type
        TYPE_UID::DATA_NODE_STRING_STRING,        // Data node type UID
        TYPE_UID::INDEX_NODE_STRING_STRING,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache,                             // Cache type
        NoStorage,
        true,
        false
    >;

    using BEpsilonStoreAOS = BEpsilonStoreAOSTrait <
        CHAR16,                           // Key type
        CHAR16,                           // Value type
        TYPE_UID::DATA_NODE_STRING_STRING,        // Data node type UID
        TYPE_UID::INDEX_NODE_STRING_STRING,       // Index node type UID
        uintptr_t,                          // Object UID type
        NoCacheObject,                      // Object type
        NoCache                             // Cache type
    >;


    // Test different record counts using multi-workload approach
    std::vector<size_t> record_counts = { 100000 , 500000, 1000000, 5000000};
    
    for (size_t record_count : record_counts) {
        std::cout << "\n--- Testing with " << record_count << " records using multi-workload approach ---" << std::endl;
        
        // Define workload file paths (using char16 data files since keys are CHAR16)
        std::string random_file = "data/char16_random_" + std::to_string(record_count) + ".dat";
        std::string sequential_file = "data/char16_sequential_" + std::to_string(record_count) + ".dat";
        std::string uniform_file = "data/char16_uniform_" + std::to_string(record_count) + ".dat";
        std::string zipfian_file = "data/char16_zipfian_" + std::to_string(record_count) + ".dat";
        
        std::cout << "Loading workload files:" << std::endl;
        std::cout << "  Random (insert/delete): " << random_file << std::endl;
        std::cout << "  Sequential (search): " << sequential_file << std::endl;
        std::cout << "  Uniform (search): " << uniform_file << std::endl;
        std::cout << "  Zipfian (search): " << zipfian_file << std::endl;
        
        try {
            // Load all workload data
            std::cout << "Loading random data..." << std::endl;
            std::vector<CHAR16> random_data = load_data_from_file<CHAR16>(random_file);
            std::cout << "Loading sequential data..." << std::endl;
            std::vector<CHAR16> sequential_data = load_data_from_file<CHAR16>(sequential_file);
            std::cout << "Loading uniform data..." << std::endl;
            std::vector<CHAR16> uniform_data = load_data_from_file<CHAR16>(uniform_file);
            std::cout << "Loading zipfian data..." << std::endl;
            std::vector<CHAR16> zipfian_data = load_data_from_file<CHAR16>(zipfian_file);
            
            // Resize all datasets to requested number of records if needed
            if (random_data.size() > record_count) random_data.resize(record_count);
            if (sequential_data.size() > record_count) sequential_data.resize(record_count);
            if (uniform_data.size() > record_count) uniform_data.resize(record_count);
            if (zipfian_data.size() > record_count) zipfian_data.resize(record_count);
            
            // Run multiple iterations for averaging
            for (int run_id = 1; run_id <= num_runs; run_id++) {
                std::cout << "\n  === Run " << run_id << " of " << num_runs << " ===" << std::endl;
                
                // Test B+ Tree
                test_internal_multi_workload<BPlusStoreSOA, CHAR16, CHAR16>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BplusTreeSOA", logger, run_id);

                test_internal_multi_workload<BPlusStoreAOS, CHAR16, CHAR16>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BplusTreeAOS", logger, run_id);

                // Test B-Epsilon Tree with multi-workload approach
                test_internal_multi_workload<BEpsilonStoreSOA, CHAR16, CHAR16>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BepsilonTreeSOA", logger, run_id);

                test_internal_multi_workload<BEpsilonStoreSOALazyNodes, CHAR16, CHAR16>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BepsilonTreeSOALazyNodes", logger, run_id);

                test_internal_multi_workload<BEpsilonStoreSOALazyIndex, CHAR16, CHAR16>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BepsilonTreeSOALazyIndex", logger, run_id);

                test_internal_multi_workload<BEpsilonStoreAOS, CHAR16, CHAR16>(
                    random_data, random_data, sequential_data, uniform_data, zipfian_data,
                    "BepsilonTreeAOS", logger, run_id);
                
                // Sleep between runs (but not after the last run)
                if (run_id < num_runs) {
                    std::cout << "\n    Sleeping for 10 seconds between runs..." << std::endl;
                    std::this_thread::sleep_for(std::chrono::seconds(10));
                }
            }
        }
        catch (const std::exception& e) {
            std::cout << "Error loading workload files: " << e.what() << std::endl;
            std::cout << "Skipping this record count..." << std::endl;
        }
    }
}

// Main test function - runs all benchmark tests
void test(int num_runs = 1) {
    std::cout << "\n=== Running All Benchmark Tests ===" << std::endl;
    std::cout << "Number of runs per configuration: " << num_runs << std::endl;
    
    // Test different key-value type combinations
    test_tree_with_key_and_value_as_uint64_t(num_runs);
    test_tree_with_key_as_uint64_and_value_as_string(num_runs);
    test_tree_with_key_and_value_as_string(num_runs);
}

// Helper function to run a single test configuration
void run_single_test(
    const std::vector<uint64_t>& data,
    const std::string& tree_type,
    const std::string& operation,
    int degree,
    CSVLogger& logger,
    int runs
) {
    std::cout << "\n--- Testing " << tree_type << " - " << operation << " - Degree " << degree << " ---" << std::endl;
    
    for (int run_id = 1; run_id <= runs; run_id++) {
        std::cout << "\n  === Run " << run_id << " of " << runs << " ===" << std::endl;
        
        auto begin = std::chrono::steady_clock::now();
        auto end = std::chrono::steady_clock::now();
        
        if (tree_type == "BplusTreeSOA") {
            using BPlusStoreSOA = BPlusStoreSOATrait<uint64_t, uint64_t, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, uintptr_t, NoCacheObject, NoCache>;
            BPlusStoreSOA::StoreType tree(degree);
            tree.template init<BPlusStoreSOA::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                // Setup: insert all data first
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Measure: delete all data
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.substr(0, 6) == "search") {
                // Setup: insert all data first
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Measure: search all data
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    uint64_t value;
                    ErrorCode ec = tree.search(data[i], value);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BplusTreeAOS") {
            using BPlusStoreAOS = BPlusStoreAOSTrait<uint64_t, uint64_t, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, uintptr_t, NoCacheObject, NoCache>;
            BPlusStoreAOS::StoreType tree(degree);
            tree.template init<BPlusStoreAOS::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.substr(0, 6) == "search") {
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    uint64_t value;
                    ErrorCode ec = tree.search(data[i], value);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BepsilonTreeSOA") {
            using BEpsilonStoreSOA = BEpsilonStoreSOATrait<uint64_t, uint64_t, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, uintptr_t, NoCacheObject, NoCache>;
            BEpsilonStoreSOA::StoreType tree(degree);
            tree.template init<BEpsilonStoreSOA::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.substr(0, 6) == "search") {
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    uint64_t value;
                    ErrorCode ec = tree.search(data[i], value);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BepsilonTreeAOS") {
            using BEpsilonStoreAOS = BEpsilonStoreAOSTrait<uint64_t, uint64_t, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, uintptr_t, NoCacheObject, NoCache>;
            BEpsilonStoreAOS::StoreType tree(degree);
            tree.template init<BEpsilonStoreAOS::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.substr(0, 6) == "search") {
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    uint64_t value;
                    ErrorCode ec = tree.search(data[i], value);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BepsilonTreeSOALazyNodes") {
            using BEpsilonStoreSOALazyNodes = BEpsilonStoreSOATrait<uint64_t, uint64_t, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, uintptr_t, NoCacheObject, NoCache, NoStorage, true, true>;
            BEpsilonStoreSOALazyNodes::StoreType tree(degree);
            tree.template init<BEpsilonStoreSOALazyNodes::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.substr(0, 6) == "search") {
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    uint64_t value;
                    ErrorCode ec = tree.search(data[i], value);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BepsilonTreeSOALazyIndex") {
            using BEpsilonStoreSOALazyIndex = BEpsilonStoreSOATrait<uint64_t, uint64_t, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, uintptr_t, NoCacheObject, NoCache, NoStorage, true, false>;
            BEpsilonStoreSOALazyIndex::StoreType tree(degree);
            tree.template init<BEpsilonStoreSOALazyIndex::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.substr(0, 6) == "search") {
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    uint64_t value;
                    ErrorCode ec = tree.search(data[i], value);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BepsilonTreeSOAII") {
            using BEpsilonStoreSOAII = BEpsilonStoreSOAIITrait<uint64_t, uint64_t, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, uintptr_t, NoCacheObject, NoCache, NoStorage, false, false>;
            BEpsilonStoreSOAII::StoreType tree(degree);
            tree.template init<BEpsilonStoreSOAII::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.substr(0, 6) == "search") {
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.insert(data[i], data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    uint64_t value;
                    ErrorCode ec = tree.search(data[i], value);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else {
            std::cout << "Error: Unknown tree type: " << tree_type << std::endl;
            return;
        }
        
        auto time_us = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        double throughput = (double)data.size() / time_us * 1000000;
        
        std::cout << "    " << operation << " [" << data.size() << " records]: " << time_us << " us" << std::endl;
        std::cout << "    Throughput: " << (int)throughput << " ops/sec" << std::endl;
        
        // Log results
        logger.log_result(tree_type, "uint64_t", "uint64_t", "single_workload", 
                         data.size(), degree, operation, time_us, throughput, run_id);
        
        // Sleep between runs (but not after the last run)
        if (run_id < runs) {
            std::cout << "\n    Sleeping for 10 seconds between runs..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }
}

// Helper function to run a single test configuration for uint64_t/CHAR16
void run_single_test_uint64_char16(
    const std::vector<uint64_t>& data,
    const std::string& tree_type,
    const std::string& operation,
    int degree,
    CSVLogger& logger,
    int runs
) {
    std::cout << "\n--- Testing " << tree_type << " - " << operation << " - Degree " << degree << " ---" << std::endl;
    
    for (int run_id = 1; run_id <= runs; run_id++) {
        std::cout << "\n  === Run " << run_id << " of " << runs << " ===" << std::endl;
        
        auto begin = std::chrono::steady_clock::now();
        auto end = std::chrono::steady_clock::now();
        
        if (tree_type == "BplusTreeSOA") {
            using BPlusStoreSOA = BPlusStoreSOATrait<uint64_t, CHAR16, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_STRING_STRING, uintptr_t, NoCacheObject, NoCache>;
            BPlusStoreSOA::StoreType tree(degree);
            tree.template init<BPlusStoreSOA::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the deletion
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.find("search") == 0) {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the search
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 result;
                    ErrorCode ec = tree.search(data[i], result);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BplusTreeAOS") {
            using BPlusStoreAOS = BPlusStoreAOSTrait<uint64_t, CHAR16, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_STRING_STRING, uintptr_t, NoCacheObject, NoCache>;
            BPlusStoreAOS::StoreType tree(degree);
            tree.template init<BPlusStoreAOS::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the deletion
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.find("search") == 0) {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the search
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 result;
                    ErrorCode ec = tree.search(data[i], result);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BepsilonTreeSOA") {
            using BEpsilonStoreSOA = BEpsilonStoreSOATrait<uint64_t, CHAR16, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_STRING_STRING, uintptr_t, NoCacheObject, NoCache>;
            BEpsilonStoreSOA::StoreType tree(degree);
            tree.template init<BEpsilonStoreSOA::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the deletion
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.find("search") == 0) {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the search
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 result;
                    ErrorCode ec = tree.search(data[i], result);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BepsilonTreeAOS") {
            using BEpsilonStoreAOS = BEpsilonStoreAOSTrait<uint64_t, CHAR16, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_STRING_STRING, uintptr_t, NoCacheObject, NoCache>;
            BEpsilonStoreAOS::StoreType tree(degree);
            tree.template init<BEpsilonStoreAOS::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the deletion
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.find("search") == 0) {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the search
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 result;
                    ErrorCode ec = tree.search(data[i], result);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BepsilonTreeSOALazyNodes") {
            using BEpsilonStoreSOALazyNodes = BEpsilonStoreSOATrait<uint64_t, CHAR16, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_STRING_STRING, uintptr_t, NoCacheObject, NoCache, NoStorage, true, true>;
            BEpsilonStoreSOALazyNodes::StoreType tree(degree);
            tree.template init<BEpsilonStoreSOALazyNodes::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the deletion
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.find("search") == 0) {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the search
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 result;
                    ErrorCode ec = tree.search(data[i], result);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BepsilonTreeSOALazyIndex") {
            using BEpsilonStoreSOALazyIndex = BEpsilonStoreSOATrait<uint64_t, CHAR16, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_STRING_STRING, uintptr_t, NoCacheObject, NoCache, NoStorage, true, false>;
            BEpsilonStoreSOALazyIndex::StoreType tree(degree);
            tree.template init<BEpsilonStoreSOALazyIndex::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the deletion
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.find("search") == 0) {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the search
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 result;
                    ErrorCode ec = tree.search(data[i], result);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BepsilonTreeSOAII") {
            using BEpsilonStoreSOAII = BEpsilonStoreSOAIITrait<uint64_t, CHAR16, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_STRING_STRING, uintptr_t, NoCacheObject, NoCache, NoStorage, false, false>;
            BEpsilonStoreSOAII::StoreType tree(degree);
            tree.template init<BEpsilonStoreSOAII::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the deletion
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.find("search") == 0) {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::to_string(data[i])).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the search
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 result;
                    ErrorCode ec = tree.search(data[i], result);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else {
            std::cout << "Error: Tree type " << tree_type << " not supported for uint64_t/char16 combination" << std::endl;
            return;
        }
        
        auto time_us = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        double throughput = (double)data.size() / time_us * 1000000;
        
        std::cout << "    " << operation << " [" << data.size() << " records]: " << time_us << " us" << std::endl;
        std::cout << "    Throughput: " << (int)throughput << " ops/sec" << std::endl;
        
        // Log results
        logger.log_result(tree_type, "uint64_t", "char16", "single_workload", 
                         data.size(), degree, operation, time_us, throughput, run_id);
        
        // Sleep between runs (but not after the last run)
        if (run_id < runs) {
            std::cout << "\n    Sleeping for 10 seconds between runs..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }
}

// Helper function to run a single test configuration for CHAR16/CHAR16
void run_single_test_char16_char16(
    const std::vector<CHAR16>& data,
    const std::string& tree_type,
    const std::string& operation,
    int degree,
    CSVLogger& logger,
    int runs
) {
    std::cout << "\n--- Testing " << tree_type << " - " << operation << " - Degree " << degree << " ---" << std::endl;
    
    for (int run_id = 1; run_id <= runs; run_id++) {
        std::cout << "\n  === Run " << run_id << " of " << runs << " ===" << std::endl;
        
        auto begin = std::chrono::steady_clock::now();
        auto end = std::chrono::steady_clock::now();
        
        if (tree_type == "BplusTreeSOA") {
            using BPlusStoreSOA = BPlusStoreSOATrait<CHAR16, CHAR16, TYPE_UID::DATA_NODE_STRING_STRING, TYPE_UID::INDEX_NODE_STRING_STRING, uintptr_t, NoCacheObject, NoCache>;
            BPlusStoreSOA::StoreType tree(degree);
            tree.template init<BPlusStoreSOA::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the deletion
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.find("search") == 0) {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the search
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 result;
                    ErrorCode ec = tree.search(data[i], result);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BplusTreeAOS") {
            using BPlusStoreAOS = BPlusStoreAOSTrait<CHAR16, CHAR16, TYPE_UID::DATA_NODE_STRING_STRING, TYPE_UID::INDEX_NODE_STRING_STRING, uintptr_t, NoCacheObject, NoCache>;
            BPlusStoreAOS::StoreType tree(degree);
            tree.template init<BPlusStoreAOS::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the deletion
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.find("search") == 0) {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the search
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 result;
                    ErrorCode ec = tree.search(data[i], result);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BepsilonTreeSOA") {
            using BEpsilonStoreSOA = BEpsilonStoreSOATrait<CHAR16, CHAR16, TYPE_UID::DATA_NODE_STRING_STRING, TYPE_UID::INDEX_NODE_STRING_STRING, uintptr_t, NoCacheObject, NoCache>;
            BEpsilonStoreSOA::StoreType tree(degree);
            tree.template init<BEpsilonStoreSOA::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the deletion
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.find("search") == 0) {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the search
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 result;
                    ErrorCode ec = tree.search(data[i], result);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BepsilonTreeAOS") {
            using BEpsilonStoreAOS = BEpsilonStoreAOSTrait<CHAR16, CHAR16, TYPE_UID::DATA_NODE_STRING_STRING, TYPE_UID::INDEX_NODE_STRING_STRING, uintptr_t, NoCacheObject, NoCache>;
            BEpsilonStoreAOS::StoreType tree(degree);
            tree.template init<BEpsilonStoreAOS::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the deletion
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.find("search") == 0) {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the search
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 result;
                    ErrorCode ec = tree.search(data[i], result);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BepsilonTreeSOALazyNodes") {
            using BEpsilonStoreSOALazyNodes = BEpsilonStoreSOATrait<CHAR16, CHAR16, TYPE_UID::DATA_NODE_STRING_STRING, TYPE_UID::INDEX_NODE_STRING_STRING, uintptr_t, NoCacheObject, NoCache, NoStorage, true, true>;
            BEpsilonStoreSOALazyNodes::StoreType tree(degree);
            tree.template init<BEpsilonStoreSOALazyNodes::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the deletion
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.find("search") == 0) {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the search
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 result;
                    ErrorCode ec = tree.search(data[i], result);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BepsilonTreeSOALazyIndex") {
            using BEpsilonStoreSOALazyIndex = BEpsilonStoreSOATrait<CHAR16, CHAR16, TYPE_UID::DATA_NODE_STRING_STRING, TYPE_UID::INDEX_NODE_STRING_STRING, uintptr_t, NoCacheObject, NoCache, NoStorage, true, false>;
            BEpsilonStoreSOALazyIndex::StoreType tree(degree);
            tree.template init<BEpsilonStoreSOALazyIndex::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the deletion
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.find("search") == 0) {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the search
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 result;
                    ErrorCode ec = tree.search(data[i], result);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else if (tree_type == "BepsilonTreeSOAII") {
            using BEpsilonStoreSOAII = BEpsilonStoreSOAIITrait<CHAR16, CHAR16, TYPE_UID::DATA_NODE_STRING_STRING, TYPE_UID::INDEX_NODE_STRING_STRING, uintptr_t, NoCacheObject, NoCache, NoStorage, false, false>;
            BEpsilonStoreSOAII::StoreType tree(degree);
            tree.template init<BEpsilonStoreSOAII::DataNodeType>();
            
            if (operation == "insert") {
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation == "delete") {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the deletion
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    ErrorCode ec = tree.remove(data[i]);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            } else if (operation.find("search") == 0) {
                // First insert all data
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 value_str(("val" + std::string(data[i].data)).c_str());
                    ErrorCode ec = tree.insert(data[i], value_str);
                    ASSERT(ec == ErrorCode::Success);
                }
                // Then time the search
                begin = std::chrono::steady_clock::now();
                for (size_t i = 0; i < data.size(); i++) {
                    CHAR16 result;
                    ErrorCode ec = tree.search(data[i], result);
                    ASSERT(ec == ErrorCode::Success);
                }
                end = std::chrono::steady_clock::now();
            }
        } else {
            std::cout << "Error: Tree type " << tree_type << " not supported for char16/char16 combination" << std::endl;
            return;
        }
        
        auto time_us = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        double throughput = (double)data.size() / time_us * 1000000;
        
        std::cout << "    " << operation << " [" << data.size() << " records]: " << time_us << " us" << std::endl;
        std::cout << "    Throughput: " << (int)throughput << " ops/sec" << std::endl;
        
        // Log results
        logger.log_result(tree_type, "char16", "char16", "single_workload", 
                         data.size(), degree, operation, time_us, throughput, run_id);
        
        // Sleep between runs (but not after the last run)
        if (run_id < runs) {
            std::cout << "\n    Sleeping for 10 seconds between runs..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }
}

// Simple single configuration test function for profiling
void test_single_configuration(
    const std::string& tree_type,
    const std::string& key_type, 
    const std::string& value_type,
    const std::string& operation,
    int degree,
    int records,
    int runs,
    const std::string& output_dir = ""
) {
    std::cout << "\n=== Single Configuration Benchmark ===" << std::endl;
    
    // Create CSV logger with specific filename for this configuration
    std::string csv_filename = CSVLogger::generate_filename(
        tree_type + "_" + key_type + "_" + value_type + "_" + 
        operation + "_" + std::to_string(degree) + "_" + std::to_string(records));
    CSVLogger logger(csv_filename, output_dir);
    logger.write_header();
    std::string full_path = output_dir.empty() ? csv_filename : output_dir + "/" + csv_filename;
    std::cout << "Logging results to: " << full_path << std::endl;
    
    // Load appropriate data files based on key type and operation
    std::string data_file;
    if (key_type == "uint64_t") {
        if (operation == "insert" || operation == "delete") {
            data_file = "data/uint64_random_" + std::to_string(records) + ".dat";
        } else if (operation == "search_random") {
            data_file = "data/uint64_random_" + std::to_string(records) + ".dat";
        } else if (operation == "search_sequential") {
            data_file = "data/uint64_sequential_" + std::to_string(records) + ".dat";
        } else if (operation == "search_uniform") {
            data_file = "data/uint64_uniform_" + std::to_string(records) + ".dat";
        } else if (operation == "search_zipfian") {
            data_file = "data/uint64_zipfian_" + std::to_string(records) + ".dat";
        }
    } else if (key_type == "char16") {
        if (operation == "insert" || operation == "delete") {
            data_file = "data/char16_random_" + std::to_string(records) + ".dat";
        } else if (operation == "search_random") {
            data_file = "data/char16_random_" + std::to_string(records) + ".dat";
        } else if (operation == "search_sequential") {
            data_file = "data/char16_sequential_" + std::to_string(records) + ".dat";
        } else if (operation == "search_uniform") {
            data_file = "data/char16_uniform_" + std::to_string(records) + ".dat";
        } else if (operation == "search_zipfian") {
            data_file = "data/char16_zipfian_" + std::to_string(records) + ".dat";
        }
    }
    
    std::cout << "Loading data from: " << data_file << std::endl;
    
    // Support all key-value type combinations for single configuration tests
    if (key_type == "uint64_t" && value_type == "uint64_t") {
        try {
            // Use the proper load_data_from_file function defined in this namespace
            std::vector<uint64_t> data = load_data_from_file<uint64_t>(data_file);
            
            if (data.size() != static_cast<size_t>(records)) {
                std::cout << "Warning: Expected " << records << " records, got " << data.size() << std::endl;
            }
            
            // Run the test using uint64_t/uint64_t
            run_single_test(data, tree_type, operation, degree, logger, runs);
        } catch (const std::exception& e) {
            std::cout << "Error loading data: " << e.what() << std::endl;
            return;
        }
        
    } else if (key_type == "uint64_t" && value_type == "char16") {
        try {
            // Use the proper load_data_from_file function defined in this namespace
            std::vector<uint64_t> data = load_data_from_file<uint64_t>(data_file);
            
            if (data.size() != static_cast<size_t>(records)) {
                std::cout << "Warning: Expected " << records << " records, got " << data.size() << std::endl;
            }
            
            // Run the test using uint64_t/CHAR16
            run_single_test_uint64_char16(data, tree_type, operation, degree, logger, runs);
        } catch (const std::exception& e) {
            std::cout << "Error loading data: " << e.what() << std::endl;
            return;
        }
        
    } else if (key_type == "char16" && value_type == "char16") {
        try {
            // Use the proper load_data_from_file function defined in this namespace
            std::vector<CHAR16> char16_data = load_data_from_file<CHAR16>(data_file);
            
            if (char16_data.size() != static_cast<size_t>(records)) {
                std::cout << "Warning: Expected " << records << " records, got " << char16_data.size() << std::endl;
            }
            
            // Run the test using CHAR16/CHAR16
            run_single_test_char16_char16(char16_data, tree_type, operation, degree, logger, runs);
        } catch (const std::exception& e) {
            std::cout << "Error loading data: " << e.what() << std::endl;
            return;
        }
        
    } else {
        std::cout << "Error: Unsupported key-value combination: " << key_type << "/" << value_type << std::endl;
        std::cout << "Supported combinations: uint64_t/uint64_t, uint64_t/char16, char16/char16" << std::endl;
    }
}

} // namespace bm_tree_with_no_cache
#endif // __TREE_WITH_CACHE__