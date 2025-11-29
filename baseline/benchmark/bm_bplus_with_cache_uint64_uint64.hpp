#ifndef BM_BPLUS_WITH_CACHE_UINT64_UINT64_HPP
#define BM_BPLUS_WITH_CACHE_UINT64_UINT64_HPP

#include <vector>
#include <cassert>
#include <chrono>
#include <iostream>
#include <fstream>
#include <string>
#include <memory>
#include <algorithm>
#include <random>
#include <unordered_set>

#include "BPlusStore.hpp"
#include "DataNode.hpp"
#include "IndexNode.hpp"
#include "LRUCache.hpp"
#include "A2QCache.hpp"
#include "CLOCKCache.hpp"
#include "LRUCacheObject.hpp"
#include "A2QCacheObject.hpp"
#include "CLOCKCacheObject.hpp"
#include "VolatileStorage.hpp"
#include "FileStorage.hpp"
#include "PMemStorage.hpp"
#include "TypeMarshaller.hpp"
#include "ObjectFatUID.h"
#include "common.h"
#include "csv_logger.hpp"
#include "workloadgenerator.hpp"
#include "IFlushCallback.h"
#include "TypeUID.h"

namespace bm_bplus_with_cache_uint64_uint64 {

// Type definitions for BPlusStore with different cache types
typedef uint64_t KeyType;
typedef uint64_t ValueType;
typedef ObjectFatUID ObjectUIDType;

typedef DataNode<KeyType, ValueType, ObjectUIDType, TYPE_UID::DATA_NODE_UINT64_UINT64> DataNodeType;
typedef IndexNode<KeyType, ValueType, ObjectUIDType, DataNodeType, TYPE_UID::INDEX_NODE_UINT64_UINT64> IndexNodeType;

// Cache object types
typedef LRUCacheObject<TypeMarshaller, DataNodeType, IndexNodeType> LRUObjectType;
typedef A2QCacheObject<TypeMarshaller, DataNodeType, IndexNodeType> A2QObjectType;
typedef CLOCKCacheObject<TypeMarshaller, DataNodeType, IndexNodeType> CLOCKObjectType;

// Callback interface
typedef IFlushCallback<ObjectUIDType, LRUObjectType> LRUCallback;
typedef IFlushCallback<ObjectUIDType, A2QObjectType> A2QCallback;
typedef IFlushCallback<ObjectUIDType, CLOCKObjectType> CLOCKCallback;

// BPlusStore type definitions with different cache and storage combinations
typedef BPlusStore<LRUCallback, KeyType, ValueType, LRUCache<LRUCallback, VolatileStorage<LRUCallback, ObjectUIDType, LRUCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreLRUVolatile;
typedef BPlusStore<LRUCallback, KeyType, ValueType, LRUCache<LRUCallback, FileStorage<LRUCallback, ObjectUIDType, LRUCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreLRUFile;

typedef BPlusStore<A2QCallback, KeyType, ValueType, A2QCache<A2QCallback, VolatileStorage<A2QCallback, ObjectUIDType, A2QCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreA2QVolatile;
typedef BPlusStore<A2QCallback, KeyType, ValueType, A2QCache<A2QCallback, FileStorage<A2QCallback, ObjectUIDType, A2QCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreA2QFile;

typedef BPlusStore<CLOCKCallback, KeyType, ValueType, CLOCKCache<CLOCKCallback, VolatileStorage<CLOCKCallback, ObjectUIDType, CLOCKCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreCLOCKVolatile;
typedef BPlusStore<CLOCKCallback, KeyType, ValueType, CLOCKCache<CLOCKCallback, FileStorage<CLOCKCallback, ObjectUIDType, CLOCKCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreCLOCKFile;

// PMemStorage type definitions
typedef BPlusStore<LRUCallback, KeyType, ValueType, LRUCache<LRUCallback, PMemStorage<LRUCallback, ObjectUIDType, LRUCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreLRUPMem;
typedef BPlusStore<A2QCallback, KeyType, ValueType, A2QCache<A2QCallback, PMemStorage<A2QCallback, ObjectUIDType, A2QCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreA2QPMem;
typedef BPlusStore<CLOCKCallback, KeyType, ValueType, CLOCKCache<CLOCKCallback, PMemStorage<CLOCKCallback, ObjectUIDType, CLOCKCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreCLOCKPMem;

// Benchmark operation functions
template<typename BPlusStoreType>
Duration benchmark_insert(BPlusStoreType& store, const std::vector<KeyType>& keys, const std::vector<ValueType>& values) {
    assert(keys.size() == values.size());
    
    auto start = get_time();

    for (size_t i = 0; i < keys.size(); ++i) {
        ErrorCode result = store.insert(keys[i], values[i]);
        if (result != ErrorCode::Success) {
            std::cerr << "Insert failed for key " << keys[i] << " with error code " << static_cast<int>(result) << std::endl;
        }
    }
    store.flush();

    auto end = get_time();
    return get_duration(start, end);
}

template<typename BPlusStoreType>
Duration benchmark_search(BPlusStoreType& store, const std::vector<KeyType>& keys) {
    auto start = get_time();
    
    for (const auto& key : keys) {
        ValueType value;
        ErrorCode result = store.search(key, value);
        if (result != ErrorCode::Success) {
            std::cerr << "Search failed for key " << key << " with error code " << static_cast<int>(result) << std::endl;
        }
    }
    
    store.flush();
    auto end = get_time();
    return get_duration(start, end);
}

template<typename BPlusStoreType>
Duration benchmark_delete(BPlusStoreType& store, const std::vector<KeyType>& keys) {
    auto start = get_time();
    
    for (const auto& key : keys) {
        ErrorCode result = store.remove(key);
        if (result != ErrorCode::Success) {
            std::cerr << "Delete failed for key " << key << " with error code " << static_cast<int>(result) << std::endl;
        }
    }
    
    store.flush();
    auto end = get_time();
    return get_duration(start, end);
}

// Concurrent operation functions
template<typename BPlusStoreType>
void concurrent_insert(BPlusStoreType& store, const std::vector<KeyType>& keys, 
                      const std::vector<ValueType>& values, size_t start_idx, size_t end_idx) {
    for (size_t i = start_idx; i < end_idx && i < keys.size(); ++i) {
        ErrorCode result = store.insert(keys[i], values[i]);
        if (result != ErrorCode::Success) {
            std::cerr << "Concurrent insert failed for key " << keys[i] << std::endl;
        }
    }
}

template<typename BPlusStoreType>
void concurrent_search(BPlusStoreType& store, const std::vector<KeyType>& keys, 
                      size_t start_idx, size_t end_idx) {
    for (size_t i = start_idx; i < end_idx && i < keys.size(); ++i) {
        ValueType value;
        ErrorCode result = store.search(keys[i], value);
        if (result != ErrorCode::Success) {
            std::cerr << "Concurrent search failed for key " << keys[i] << std::endl;
        }
    }
}

template<typename BPlusStoreType>
void concurrent_delete(BPlusStoreType& store, const std::vector<KeyType>& keys, 
                      size_t start_idx, size_t end_idx) {
    for (size_t i = start_idx; i < end_idx && i < keys.size(); ++i) {
        ErrorCode result = store.remove(keys[i]);
        if (result != ErrorCode::Success) {
            std::cerr << "Concurrent delete failed for key " << keys[i] << std::endl;
        }
    }
}

template<typename BPlusStoreType>
Duration benchmark_concurrent_operation(BPlusStoreType& store, const std::string& operation,
                                       const std::vector<KeyType>& keys, 
                                       const std::vector<ValueType>& values,
                                       int thread_count) {
    size_t keys_per_thread = keys.size() / thread_count;
    std::vector<std::thread> threads;
    
    auto start = get_time();
    
    for (int i = 0; i < thread_count; ++i) {
        size_t start_idx = i * keys_per_thread;
        size_t end_idx = (i == thread_count - 1) ? keys.size() : (i + 1) * keys_per_thread;
        
        if (operation == "insert") {
            threads.emplace_back(concurrent_insert<BPlusStoreType>, std::ref(store), 
                               std::cref(keys), std::cref(values), start_idx, end_idx);
        } else if (operation.find("search_") == 0) {
            threads.emplace_back(concurrent_search<BPlusStoreType>, std::ref(store), 
                               std::cref(keys), start_idx, end_idx);
        } else if (operation == "delete") {
            threads.emplace_back(concurrent_delete<BPlusStoreType>, std::ref(store), 
                               std::cref(keys), start_idx, end_idx);
        }
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    auto end = get_time();
    return get_duration(start, end);
}

// Template function to create BPlusStore instances with correct constructor parameters
template<typename T>
std::unique_ptr<T> create_bplus_store(size_t degree, size_t cache_size, size_t page_size, size_t memory_size);

// Specializations for VolatileStorage (2 parameters)
template<>
std::unique_ptr<BPlusStoreLRUVolatile> create_bplus_store<BPlusStoreLRUVolatile>(size_t degree, size_t cache_size, size_t page_size, size_t memory_size) {
    return std::make_unique<BPlusStoreLRUVolatile>(degree, cache_size, page_size, memory_size);
}

template<>
std::unique_ptr<BPlusStoreA2QVolatile> create_bplus_store<BPlusStoreA2QVolatile>(size_t degree, size_t cache_size, size_t page_size, size_t memory_size) {
    return std::make_unique<BPlusStoreA2QVolatile>(degree, cache_size, page_size, memory_size);
}

template<>
std::unique_ptr<BPlusStoreCLOCKVolatile> create_bplus_store<BPlusStoreCLOCKVolatile>(size_t degree, size_t cache_size, size_t page_size, size_t memory_size) {
    return std::make_unique<BPlusStoreCLOCKVolatile>(degree, cache_size, page_size, memory_size);
}

// Specializations for FileStorage (3 parameters)
template<>
std::unique_ptr<BPlusStoreLRUFile> create_bplus_store<BPlusStoreLRUFile>(size_t degree, size_t cache_size, size_t page_size, size_t memory_size) {
    return std::make_unique<BPlusStoreLRUFile>(degree, cache_size, page_size, memory_size, FILE_STORAGE_PATH);
}

template<>
std::unique_ptr<BPlusStoreA2QFile> create_bplus_store<BPlusStoreA2QFile>(size_t degree, size_t cache_size, size_t page_size, size_t memory_size) {
    return std::make_unique<BPlusStoreA2QFile>(degree, cache_size, page_size, memory_size, FILE_STORAGE_PATH);
}

template<>
std::unique_ptr<BPlusStoreCLOCKFile> create_bplus_store<BPlusStoreCLOCKFile>(size_t degree, size_t cache_size, size_t page_size, size_t memory_size) {
    return std::make_unique<BPlusStoreCLOCKFile>(degree, cache_size, page_size, memory_size, FILE_STORAGE_PATH);
}

// Specializations for PMemStorage (3 parameters)
template<>
std::unique_ptr<BPlusStoreLRUPMem> create_bplus_store<BPlusStoreLRUPMem>(size_t degree, size_t cache_size, size_t page_size, size_t memory_size) {
    return std::make_unique<BPlusStoreLRUPMem>(degree, cache_size, page_size, memory_size, PMEM_STORAGE_PATH);
}

template<>
std::unique_ptr<BPlusStoreA2QPMem> create_bplus_store<BPlusStoreA2QPMem>(size_t degree, size_t cache_size, size_t page_size, size_t memory_size) {
    return std::make_unique<BPlusStoreA2QPMem>(degree, cache_size, page_size, memory_size, PMEM_STORAGE_PATH);
}

template<>
std::unique_ptr<BPlusStoreCLOCKPMem> create_bplus_store<BPlusStoreCLOCKPMem>(size_t degree, size_t cache_size, size_t page_size, size_t memory_size) {
    return std::make_unique<BPlusStoreCLOCKPMem>(degree, cache_size, page_size, memory_size, PMEM_STORAGE_PATH);
}

// Main benchmark function for a specific configuration
template<typename BPlusStoreType>
std::vector<BenchmarkResult> run_benchmark_configuration(
    const std::string& cache_type_name,
    const std::string& storage_type_name,
    const std::string& operation,
    size_t degree,
    size_t records,
    size_t cache_size,
    double cache_percentage,
    size_t page_size,
    size_t memory_size,
    int runs,
    int thread_count,
    const std::string& config_name,
    const std::string& data_path) {
    
    std::vector<BenchmarkResult> results;
    RandomGenerator rng;
    
    std::cout << "Running " << cache_type_name << "/" << storage_type_name 
              << " - " << operation << " - Degree " << degree 
              << " - Records " << records << " - Threads " << thread_count << std::endl;
    
    for (int run = 0; run < runs; ++run) {
        // Create fresh data for each run using workload generator
        // Load unique random data for insert operations (keys and values)
        std::vector<KeyType> keys = workloadgenerator::load_insert_workload<KeyType>(records, data_path);
        //std::vector<ValueType> values = workloadgenerator::load_insert_workload<ValueType>(records, data_path);
        
        // Generate search keys based on operation type using workload generator
        std::vector<KeyType> search_keys;
        if (operation == "search_random") {
            search_keys = workloadgenerator::load_search_workload<KeyType>(records, workloadgenerator::DistributionType::Random, data_path);
        } else if (operation == "search_sequential") {
            search_keys = workloadgenerator::load_search_workload<KeyType>(records, workloadgenerator::DistributionType::Sequential, data_path);
        } else if (operation == "search_uniform") {
            search_keys = workloadgenerator::load_search_workload<KeyType>(records, workloadgenerator::DistributionType::Uniform, data_path);
        } else if (operation == "search_zipfian") {
            search_keys = workloadgenerator::load_search_workload<KeyType>(records, workloadgenerator::DistributionType::Zipfian, data_path);
        } else if (operation == "delete") {
            // For delete, use the same keys as insert (already randomized)
            // No need to shuffle since keys are already unique and random
        }
        
        // Create BPlusStore instance using specialized template function
        auto store = create_bplus_store<BPlusStoreType>(degree, cache_size, page_size, memory_size);
        store->template init<DataNodeType>();
        
        Duration duration;
        
        if (operation == "insert") {
            if (thread_count == 1) {
                duration = benchmark_insert(*store, keys, keys);
            } else {
                duration = benchmark_concurrent_operation(*store, operation, keys, keys, thread_count);
            }
        } else if (operation.find("search_") == 0) {
            // First insert all data
            benchmark_insert(*store, keys, keys);
            
            // Then measure search performance using the appropriate search keys
            if (thread_count == 1) {
                duration = benchmark_search(*store, search_keys);
            } else {
                duration = benchmark_concurrent_operation(*store, operation, search_keys, keys, thread_count);
            }
        } else if (operation == "delete") {
            // First insert all data
            benchmark_insert(*store, keys, keys);
            
            // Then measure delete performance
            if (thread_count == 1) {
                duration = benchmark_delete(*store, keys);
            } else {
                duration = benchmark_concurrent_operation(*store, operation, keys, keys, thread_count);
            }
        }
        
#ifdef __CACHE_COUNTERS__
        // Collect cache performance counters
        uint64_t cache_hits = store->getCacheHits();
        uint64_t cache_misses = store->getCacheMisses();
        uint64_t evictions = store->getEvictions();
        uint64_t dirty_evictions = store->getDirtyEvictions();
        
        BenchmarkResult result("BPlusStore", cache_type_name, storage_type_name, cache_percentage, cache_size,
                              "uint64_t", "uint64_t", operation, degree, records, run + 1, 
                              thread_count, duration, config_name,
                              cache_hits, cache_misses, evictions, dirty_evictions);
#else
        BenchmarkResult result("BPlusStore", cache_type_name, storage_type_name, cache_percentage, cache_size,
                              "uint64_t", "uint64_t", operation, degree, records, run + 1, 
                              thread_count, duration, config_name);
#endif //__CACHE_COUNTERS__
        
        results.push_back(result);
        
        std::cout << "  Run " << (run + 1) << "/" << runs 
                  << ": " << duration_to_microseconds(duration) << " μs"
                  << " (" << std::fixed << std::setprecision(2) << result.throughput_ops_sec << " ops/sec)"
                  << std::endl;
    }
    
    return results;
}

// Function to run benchmarks for all cache/storage combinations
std::vector<BenchmarkResult> run_all_configurations(
    const std::string& cache_type,
    const std::string& storage_type,
    const std::string& operation,
    size_t degree,
    size_t records,
    size_t cache_size,
    double cache_percentage,
    size_t page_size,
    size_t memory_size,
    int runs,
    int thread_count,
    const std::string& config_name,
    const std::string& data_path) {
    
    std::vector<BenchmarkResult> all_results;
    
    if (cache_type == "LRU") {
        if (storage_type == "VolatileStorage") {
            auto results = run_benchmark_configuration<BPlusStoreLRUVolatile>(
                cache_type, storage_type, operation, degree, records, cache_size, cache_percentage,
                page_size, memory_size, runs, thread_count, config_name, data_path);
            all_results.insert(all_results.end(), results.begin(), results.end());
        } else if (storage_type == "FileStorage") {
            auto results = run_benchmark_configuration<BPlusStoreLRUFile>(
                cache_type, storage_type, operation, degree, records, cache_size, cache_percentage,
                page_size, memory_size, runs, thread_count, config_name, data_path);
            all_results.insert(all_results.end(), results.begin(), results.end());
        } else if (storage_type == "PMemStorage") {
            auto results = run_benchmark_configuration<BPlusStoreLRUPMem>(
                cache_type, storage_type, operation, degree, records, cache_size, cache_percentage,
                page_size, memory_size, runs, thread_count, config_name, data_path);
            all_results.insert(all_results.end(), results.begin(), results.end());
        }
    } else if (cache_type == "A2Q") {
        if (storage_type == "VolatileStorage") {
            auto results = run_benchmark_configuration<BPlusStoreA2QVolatile>(
                "A2Q", storage_type, operation, degree, records, cache_size, cache_percentage,
                page_size, memory_size, runs, thread_count, config_name, data_path);
            all_results.insert(all_results.end(), results.begin(), results.end());
        } else if (storage_type == "FileStorage") {
            auto results = run_benchmark_configuration<BPlusStoreA2QFile>(
                cache_type, storage_type, operation, degree, records, cache_size, cache_percentage,
                page_size, memory_size, runs, thread_count, config_name, data_path);
            all_results.insert(all_results.end(), results.begin(), results.end());
        } else if (storage_type == "PMemStorage") {
            auto results = run_benchmark_configuration<BPlusStoreA2QPMem>(
                cache_type, storage_type, operation, degree, records, cache_size, cache_percentage,
                page_size, memory_size, runs, thread_count, config_name, data_path);
            all_results.insert(all_results.end(), results.begin(), results.end());
        }
    } else if (cache_type == "CLOCK") {
        if (storage_type == "VolatileStorage") {
            auto results = run_benchmark_configuration<BPlusStoreCLOCKVolatile>(
                cache_type, storage_type, operation, degree, records, cache_size, cache_percentage,
                page_size, memory_size, runs, thread_count, config_name, data_path);
            all_results.insert(all_results.end(), results.begin(), results.end());
        } else if (storage_type == "FileStorage") {
            auto results = run_benchmark_configuration<BPlusStoreCLOCKFile>(
                cache_type, storage_type, operation, degree, records, cache_size, cache_percentage,
                page_size, memory_size, runs, thread_count, config_name, data_path);
            all_results.insert(all_results.end(), results.begin(), results.end());
        } else if (storage_type == "PMemStorage") {
            auto results = run_benchmark_configuration<BPlusStoreCLOCKPMem>(
                cache_type, storage_type, operation, degree, records, cache_size, cache_percentage,
                page_size, memory_size, runs, thread_count, config_name, data_path);
            all_results.insert(all_results.end(), results.begin(), results.end());
        }
    }
    
    return all_results;
}

// Main test function called from shell script
void test_with_shell_parameters(
    const std::string& cache_type,
    int runs,
    const std::string& output_dir,
    const std::string& storage_type,
    int cache_size,
    double cache_percentage,
    int page_size,
    long long memory_size,
    const std::vector<std::string>& operations,
    const std::vector<size_t>& degrees,
    const std::vector<size_t>& record_counts,
    int threads,
    const std::string& config_name,
    const std::string& data_path) {
    
    std::cout << "=== BPlusStore Cache Benchmark Suite (uint64_t->uint64_t) ===" << std::endl;
    std::cout << "Cache Type: " << cache_type << std::endl;
    std::cout << "Storage Type: " << storage_type << std::endl;
    std::cout << "Cache Size: " << cache_size << std::endl;
    std::cout << "Runs per configuration: " << runs << std::endl;
    std::cout << "Threads: " << threads << std::endl;
    std::cout << "Output Directory: " << (output_dir.empty() ? "current" : output_dir) << std::endl;
    std::cout << "=========================================" << std::endl;
    
    BatchCSVLogger logger(output_dir, "benchmark");
    
    for (const auto& operation : operations) {
        for (size_t degree : degrees) {
            for (size_t records : record_counts) {
                std::cout << "\n--- Testing " << operation << " with degree " << degree 
                          << " and " << records << " records ---" << std::endl;
                
                auto results = run_all_configurations(
                    cache_type, storage_type, operation, degree, records,
                    cache_size, cache_percentage, page_size, memory_size, runs, threads, config_name, data_path);
                
                logger.add_results(results);
            }
        }
    }
    
    logger.flush_to_file(config_name);
    
    std::cout << "\n=== BPlusStore Cache Benchmark Complete ===" << std::endl;
}

// Function for single configuration testing
void test_single_configuration(
    const std::string& cache_type,
    const std::string& storage_type,
    int cache_size,
    double cache_percentage,
    int page_size,
    long long memory_size,
    const std::string& key_type,
    const std::string& value_type,
    const std::string& operation,
    int degree,
    int records,
    int runs,
    int threads,
    const std::string& output_dir,
    const std::string& config_name,
    const std::string& data_path) {
    
    std::cout << "=== Single BPlusStore Configuration Test (uint64_t->uint64_t) ===" << std::endl;
    std::cout << "Cache: " << cache_type << "/" << storage_type << std::endl;
    std::cout << "Operation: " << operation << std::endl;
    std::cout << "Degree: " << degree << ", Records: " << records << std::endl;
    std::cout << "Runs: " << runs << ", Threads: " << threads << std::endl;
    std::cout << "Data Path: " << data_path << std::endl;
    std::cout << "=============================================" << std::endl;
    
    auto results = run_all_configurations(
        cache_type, storage_type, operation, degree, records,
        cache_size, cache_percentage, page_size, memory_size, runs, threads, config_name, data_path);
    
    // Save results
    BatchCSVLogger logger(output_dir, "benchmark_single");
    logger.add_results(results);
    logger.flush_to_file(config_name);
    
    // Print summary
    if (!results.empty()) {
        double avg_throughput = 0.0;
        for (const auto& result : results) {
            avg_throughput += result.throughput_ops_sec;
        }
        avg_throughput /= results.size();
        
        std::cout << "\nSummary:" << std::endl;
        std::cout << "Average throughput: " << std::fixed << std::setprecision(2) 
                  << avg_throughput << " ops/sec" << std::endl;
    }
    
    std::cout << "=== Single Configuration Test Complete ===" << std::endl;
}

} // namespace bm_bplus_with_cache_uint64_uint64

#endif // BM_BPLUS_WITH_CACHE_UINT64_UINT64_HPP