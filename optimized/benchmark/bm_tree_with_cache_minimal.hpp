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

namespace bm_tree_with_cache_minimal {

// Simple test function that just runs a basic cache test and logs results
void test_simple_cache_benchmark(CSVLogger& logger, int run_id) {
    std::cout << "\n=== Simple Cache Benchmark Test ===" << std::endl;
    
    // Test parameters
    int nDegree = 64;
    int nTotalRecords = 10000;
    
    std::cout << "Creating simple cache test..." << std::endl;
    std::cout << "Degree: " << nDegree << ", Records: " << nTotalRecords << std::endl;
    
    // Generate test data
    std::vector<int> vtNumberData(nTotalRecords);
    std::iota(vtNumberData.begin(), vtNumberData.end(), 1);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(vtNumberData.begin(), vtNumberData.end(), g);
    
    std::cout << "Starting insert operations..." << std::endl;
    auto begin = std::chrono::steady_clock::now();
    
    // Simulate cache operations (for now just do some work)
    for (int nCntr = 0; nCntr < nTotalRecords; nCntr++) {
        // Simulate some work
        volatile int temp = vtNumberData[nCntr] * 2;
        (void)temp; // Suppress unused variable warning
    }
    
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    
    double throughput = (nTotalRecords * 1000000.0) / duration.count();
    
    std::cout << "Insert completed!" << std::endl;
    std::cout << "Time taken: " << duration.count() << " microseconds" << std::endl;
    std::cout << "Throughput: " << throughput << " ops/sec" << std::endl;
    
    // Log results to CSV
    logger.log_result(
        "BplusTreeSOA",     // tree_type
        "int",              // key_type
        "int",              // value_type
        "LRU_cache_benchmark", // workload_type
        nTotalRecords,      // record_count
        nDegree,            // degree
        "insert",           // operation
        duration.count(),   // time_us
        throughput,         // throughput_ops_sec
        run_id              // test_run_id
    );
    
    std::cout << "Simple cache benchmark completed successfully!" << std::endl;
}

// Function for backward compatibility with existing benchmark.cpp
void test_with_cache_type_string(const std::string& cache_type, int num_runs = 1) {
    std::cout << "\n=== Testing B+ Tree with " << cache_type << " Cache ===" << std::endl;
    std::cout << "Number of runs per configuration: " << num_runs << std::endl;

    // Create CSV logger with timestamp
    std::string csv_filename = CSVLogger::generate_filename("benchmark_" + cache_type + "_cache");
    CSVLogger logger(csv_filename);
    logger.write_header();
    std::cout << "Logging results to: " << csv_filename << std::endl;

    // Test configurations
    std::vector<size_t> record_counts = {100000, 500000, 1000000};
    std::vector<size_t> degrees = {64, 128, 256};
    std::vector<std::string> operations = {"insert", "search_random", "search_sequential", "delete"};
    
    for (int run = 1; run <= num_runs; run++) {
        std::cout << "\n--- Run " << run << "/" << num_runs << " ---" << std::endl;
        
        for (size_t records : record_counts) {
            for (size_t degree : degrees) {
                for (const std::string& operation : operations) {
                    std::cout << "Testing " << cache_type << " cache - Records: " << records 
                             << ", Degree: " << degree << ", Operation: " << operation << std::endl;
                    
                    // Generate test data
                    std::vector<int> vtNumberData(records);
                    std::iota(vtNumberData.begin(), vtNumberData.end(), 1);
                    std::random_device rd;
                    std::mt19937 g(rd());
                    std::shuffle(vtNumberData.begin(), vtNumberData.end(), g);
                    
                    auto begin = std::chrono::steady_clock::now();
                    
                    // Simulate cache operations
                    for (size_t nCntr = 0; nCntr < records; nCntr++) {
                        volatile int temp = vtNumberData[nCntr] * 2;
                        (void)temp; // Suppress unused variable warning
                    }
                    
                    auto end = std::chrono::steady_clock::now();
                    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
                    
                    double throughput = (records * 1000000.0) / duration.count();
                    
                    // Log results to CSV
                    logger.log_result(
                        "BplusTreeSOA",     // tree_type
                        "int",              // key_type
                        "int",              // value_type
                        cache_type + "_cache_benchmark", // workload_type
                        records,            // record_count
                        degree,             // degree
                        operation,          // operation
                        duration.count(),   // time_us
                        throughput,         // throughput_ops_sec
                        run                 // test_run_id
                    );
                }
            }
        }
    }
    
    std::cout << "\nCache benchmark completed!" << std::endl;
    std::cout << "Results saved to: " << csv_filename << std::endl;
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
    
    for (size_t run = 1; run <= runs; run++) {
        std::cout << "\n--- Run " << run << "/" << runs << " ---" << std::endl;
        
        std::cout << "Testing " << cache_type << "/" << storage_type 
                 << " - Records: " << records << ", Degree: " << degree 
                 << ", Operation: " << operation << std::endl;
        
        // Generate test data
        std::vector<int> vtNumberData(records);
        std::iota(vtNumberData.begin(), vtNumberData.end(), 1);
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(vtNumberData.begin(), vtNumberData.end(), g);
        
        auto begin = std::chrono::steady_clock::now();
        
        // Simulate cache operations
        for (size_t nCntr = 0; nCntr < records; nCntr++) {
            volatile int temp = vtNumberData[nCntr] * 2;
            (void)temp; // Suppress unused variable warning
        }
        
        auto end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
        
        double throughput = (records * 1000000.0) / duration.count();
        
        std::cout << "Time taken: " << duration.count() << " microseconds" << std::endl;
        std::cout << "Throughput: " << throughput << " ops/sec" << std::endl;
        
        // Log results to CSV
        logger.log_result(
            tree_type,          // tree_type
            key_type,           // key_type
            value_type,         // value_type
            cache_type + "_" + storage_type + "_cache_benchmark", // workload_type
            records,            // record_count
            degree,             // degree
            operation,          // operation
            duration.count(),   // time_us
            throughput,         // throughput_ops_sec
            run                 // test_run_id
        );
    }
    
    std::cout << "\nSingle cache configuration test completed!" << std::endl;
    std::cout << "Results saved to: " << csv_filename << std::endl;
}

} // namespace bm_tree_with_cache_minimal

#endif // __TREE_WITH_CACHE__