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
#include <sstream>
#include <thread>
#include <memory>
#ifdef __GLIBC__
#include <malloc.h>
#endif

// Must include common.h first to define WAL_FILE_PATH and other constants
#include "common.h"
#include "csv_logger.hpp"

#ifdef __RECORD_LATENCY__
// Buffered latency logger for high-performance latency recording
class BufferedLatencyLogger {
private:
    std::ofstream file_;
    std::string buffer_;
    static constexpr size_t BUFFER_SIZE = 4096; // 4KB buffer
    
public:
    BufferedLatencyLogger(const std::string& filename) : file_(filename) {
        if (!file_.is_open()) {
            throw std::runtime_error("Cannot open latency log file: " + filename);
        }
        buffer_.reserve(BUFFER_SIZE);
        // Write CSV header
        buffer_ += "operation_index,latency_ns\n";
    }
    
    void log_latency(size_t index, std::chrono::nanoseconds latency) {
        std::string entry = std::to_string(index) + "," + std::to_string(latency.count()) + "\n";
        
        if (buffer_.size() + entry.size() >= BUFFER_SIZE) {
            flush();
        }
        buffer_ += entry;
    }
    
    void flush() {
        if (!buffer_.empty()) {
            file_ << buffer_;
            file_.flush();
            buffer_.clear();
        }
    }
    
    ~BufferedLatencyLogger() {
        flush();
        if (file_.is_open()) {
            file_.close();
        }
    }
};

// Generate latency log filenames based on benchmark parameters
inline std::string generate_latency_filename(const std::string& operation, 
                                           const std::string& cache_type_name,
                                           const std::string& storage_type_name,
                                           const std::string& key_type_str,
                                           const std::string& value_type_str,
                                           size_t degree, size_t records, int run_id,
                                           const std::string& output_dir = "",
                                           int thread_id = -1) {
    std::string filename = "latency_" + operation + "_" + cache_type_name + "_" + storage_type_name + 
                          "_" + key_type_str + "_" + value_type_str + "_deg" + std::to_string(degree) + 
                          "_rec" + std::to_string(records) + "_run" + std::to_string(run_id);
    
    if (thread_id >= 0) {
        filename += "_thread" + std::to_string(thread_id);
    }
    
    filename += ".csv";
    
    if (output_dir.empty()) {
        return filename;
    } else {
        return output_dir + "/" + filename;
    }
}

// Merge latency files from multiple threads
inline void merge_latency_files(const std::vector<std::string>& thread_filenames,
                               const std::string& output_filename) {
    std::ofstream output_file(output_filename);
    if (!output_file.is_open()) {
        throw std::runtime_error("Cannot open output latency file: " + output_filename);
    }
    
    // Write CSV header
    output_file << "operation_index,latency_ns\n";
    
    size_t global_index = 0;
    for (const auto& filename : thread_filenames) {
        std::ifstream input_file(filename);
        if (!input_file.is_open()) {
            std::cerr << "Warning: Cannot open thread latency file: " << filename << std::endl;
            continue;
        }
        
        std::string line;
        // Skip header line
        std::getline(input_file, line);
        
        // Copy data with updated indices
        while (std::getline(input_file, line)) {
            size_t comma_pos = line.find(',');
            if (comma_pos != std::string::npos) {
                std::string latency_part = line.substr(comma_pos + 1);
                output_file << global_index << "," << latency_part << "\n";
                global_index++;
            }
        }
        
        input_file.close();
        // Clean up thread file
        std::filesystem::remove(filename);
    }
    
    output_file.close();
}
#endif

#ifdef __TREE_WITH_CACHE__

// Include all necessary headers for cache operations
#include "LRUCache.hpp"
#include "LRUCacheObject.hpp"
#include "A2QCache.hpp"
#include "A2QCacheObject.hpp"
#include "CLOCKCache.hpp"
#include "CLOCKCacheObject.hpp"
#include "CacheStatsProvider.hpp"
#include "VolatileStorage.hpp"
#include "FileStorage.hpp"
#include "PMemStorage.hpp"
#include "ObjectFatUID.h"
#include "DataNode.hpp"
#include "IndexNode.hpp"
#include "BPlusStore.hpp"
#include "PMemWAL.hpp"

namespace bm_tree_with_cache_real {

#ifdef __CACHE_COUNTERS__
// Callback type for collecting cache stats from threads
template<typename BPlusStoreType>
using StatsCollectorCallback = std::function<void(const CacheStatsProvider<typename BPlusStoreType::CacheType>*)>;
#endif

// BPlusStoreTraits template definition (copied from sandbox.cpp)
template <
    typename TKeyType,
    typename TValueType,
    uint8_t TDataNodeUID,
    uint8_t TIndexNodeUID,
    typename TObjectUIDType,
    template<typename> class TCacheObjectType,
    template<typename> class TCacheType,
    template<typename> class TStorageType
>
struct BPlusStoreTraits
{
    using KeyType = TKeyType;
    using ValueType = TValueType;
    using ObjectUIDType = TObjectUIDType;

    static constexpr uint8_t DataNodeUID = TDataNodeUID;
    static constexpr uint8_t IndexNodeUID = TIndexNodeUID;
    static constexpr uint16_t BufferRatioToFanout = 5;

    // Core Objects
    using DataNodeType = DataNode<BPlusStoreTraits>;
    using IndexNodeType = IndexNode<BPlusStoreTraits>;
    using ObjectType = TCacheObjectType<BPlusStoreTraits>;

    // Cache and Storage
    using CacheType = TCacheType<BPlusStoreTraits>;
    using StorageType = TStorageType<BPlusStoreTraits>;

    // Store
    using StoreType = BPlusStore<BPlusStoreTraits>;

#ifndef _MSC_VER
    using WALType = PMemWAL<BPlusStoreTraits>;
#else //_MSC_VER
    using WALType = FileWAL<BPlusStoreTraits>;
#endif //_MSC_VER
};

// Helper function to map C++ type strings to workload generator type strings
inline std::string get_workload_type_string(const std::string& cpp_type) {
    if (cpp_type == "uint64_t") return "uint64";
    if (cpp_type == "char16") return "char16";
    if (cpp_type == "int") return "uint64"; // Map int to uint64 for compatibility
    return "uint64"; // default fallback
}

#ifdef __CONCURRENT__
// Worker functions for multi-threaded operations
template<typename StoreType, typename KeyType, typename ValueType>
void insert_worker(
    StoreType& ptrTree,
    const std::vector<KeyType>& data,
    size_t start_idx,
    size_t end_idx,
    int thread_id
#ifdef __RECORD_LATENCY__
    ,BufferedLatencyLogger* thread_logger
#endif
#ifdef __CACHE_COUNTERS__
    ,StatsCollectorCallback<StoreType> stats_callback
#endif
) {
    for (size_t i = start_idx; i < end_idx; i++) {
#ifdef __RECORD_LATENCY__
        auto start = std::chrono::steady_clock::now();
#endif
        
        if constexpr (std::is_same_v<ValueType, KeyType>) {
            ptrTree.insert(data[i], data[i]);
        } else {
            ValueType value = static_cast<ValueType>(data[i]);
            ptrTree.insert(data[i], value);
        }
        
#ifdef __RECORD_LATENCY__
        auto end = std::chrono::steady_clock::now();
        if (thread_logger) {
            thread_logger->log_latency(i, std::chrono::duration_cast<std::chrono::nanoseconds>(end - start));
        }
#endif
    }
    
#ifdef __CACHE_COUNTERS__
    // Collect stats for this thread
    if (stats_callback) {
        auto cache = ptrTree.getCache();
        auto cache_provider = cache->getCacheStatsProvider();
        if (cache_provider) {
            stats_callback(cache_provider);
        }
    }
#endif
}

template<typename StoreType, typename KeyType, typename ValueType>
void search_worker(
    StoreType& ptrTree,
    const std::vector<KeyType>& data,
    size_t start_idx,
    size_t end_idx,
    int thread_id
#ifdef __RECORD_LATENCY__
    ,BufferedLatencyLogger* thread_logger
#endif
#ifdef __CACHE_COUNTERS__
    ,StatsCollectorCallback<StoreType> stats_callback
#endif
) {
    ValueType value;
    for (size_t i = start_idx; i < end_idx; i++) {
#ifdef __RECORD_LATENCY__
        auto start = std::chrono::steady_clock::now();
#endif
        
        ptrTree.search(data[i], value);
        
#ifdef __RECORD_LATENCY__
        auto end = std::chrono::steady_clock::now();
        if (thread_logger) {
            thread_logger->log_latency(i, std::chrono::duration_cast<std::chrono::nanoseconds>(end - start));
        }
#endif
    }
    
#ifdef __CACHE_COUNTERS__
    // Collect stats for this thread
    if (stats_callback) {
        auto cache = ptrTree.getCache();
        auto cache_provider = cache->getCacheStatsProvider();
        if (cache_provider) {
            stats_callback(cache_provider);
        }
    }
#endif
}

template<typename StoreType, typename KeyType, typename ValueType>
void delete_worker(
    StoreType& ptrTree,
    const std::vector<KeyType>& data,
    size_t start_idx,
    size_t end_idx,
    int thread_id
#ifdef __RECORD_LATENCY__
    ,BufferedLatencyLogger* thread_logger
#endif
#ifdef __CACHE_COUNTERS__
    ,StatsCollectorCallback<StoreType> stats_callback
#endif
) {
    for (size_t i = start_idx; i < end_idx; i++) {
#ifdef __RECORD_LATENCY__
        auto start = std::chrono::steady_clock::now();
#endif
        
        ptrTree.remove(data[i]);
        
#ifdef __RECORD_LATENCY__
        auto end = std::chrono::steady_clock::now();
        if (thread_logger) {
            thread_logger->log_latency(i, std::chrono::duration_cast<std::chrono::nanoseconds>(end - start));
        }
#endif
    }
    
#ifdef __CACHE_COUNTERS__
    // Collect stats for this thread
    if (stats_callback) {
        auto cache = ptrTree.getCache();
        auto cache_provider = cache->getCacheStatsProvider();
        if (cache_provider) {
            stats_callback(cache_provider);
        }
    }
#endif
}
#endif // __CONCURRENT__

// Operation functions that handle both single-threaded and multi-threaded execution
template<typename StoreType, typename KeyType, typename ValueType>
void perform_insert_operations(
    StoreType& ptrTree,
    const std::vector<KeyType>& vtNumberData,
    size_t records,
    int threads,
    std::chrono::nanoseconds& duration,
    const std::string& cache_type_name,
    const std::string& storage_type_name,
    const std::string& key_type_str,
    const std::string& value_type_str,
    size_t degree,
    int run_id,
    const std::string& output_dir
#ifdef __CACHE_COUNTERS__
    , auto& aggregated_stats
#endif
#ifdef __RECORD_LATENCY__
    , std::unique_ptr<BufferedLatencyLogger>& insert_logger
#endif
) {
#ifndef __CONCURRENT__
    // Single-threaded implementation (existing code)
    std::cout << "Performing insert operations (single-threaded)..." << std::endl;
    auto begin = std::chrono::steady_clock::now();
    
    for (size_t i = 0; i < records; i++) {
#ifdef __RECORD_LATENCY__
        auto start = std::chrono::steady_clock::now();
#endif
        if constexpr (std::is_same_v<ValueType, KeyType>) {
            ptrTree.insert(vtNumberData[i], vtNumberData[i]);
        } else {
            ValueType value = static_cast<ValueType>(vtNumberData[i]);
            ptrTree.insert(vtNumberData[i], value);
        }
#ifdef __RECORD_LATENCY__
        auto end = std::chrono::steady_clock::now();
        if (insert_logger) {
            insert_logger->log_latency(i, std::chrono::duration_cast<std::chrono::nanoseconds>(end - start));
        }
#endif
    }
    
    auto end = std::chrono::steady_clock::now();
    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
    
#else // __CONCURRENT__
    if (threads == 1) {
        // Use single-threaded path even in concurrent build
        std::cout << "Performing insert operations (single-threaded)..." << std::endl;
        auto begin = std::chrono::steady_clock::now();
        
        for (size_t i = 0; i < records; i++) {
#ifdef __RECORD_LATENCY__
            auto start = std::chrono::steady_clock::now();
#endif
            if constexpr (std::is_same_v<ValueType, KeyType>) {
                ptrTree.insert(vtNumberData[i], vtNumberData[i]);
            } else {
                ValueType value = static_cast<ValueType>(vtNumberData[i]);
                ptrTree.insert(vtNumberData[i], value);
            }
#ifdef __RECORD_LATENCY__
            auto end = std::chrono::steady_clock::now();
            if (insert_logger) {
                insert_logger->log_latency(i, std::chrono::duration_cast<std::chrono::nanoseconds>(end - start));
            }
#endif
        }
        
        auto end = std::chrono::steady_clock::now();
        duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
    } else {
        // Multi-threaded implementation
        std::cout << "Performing insert operations with " << threads << " threads..." << std::endl;
        
#ifdef __RECORD_LATENCY__
        // Create per-thread loggers
        std::vector<std::unique_ptr<BufferedLatencyLogger>> thread_loggers;
        std::vector<std::string> thread_filenames;
        for (int i = 0; i < threads; i++) {
            std::string thread_filename = generate_latency_filename(
                "insert", cache_type_name, storage_type_name, 
                key_type_str, value_type_str, degree, records, 
                run_id, output_dir, i
            );
            thread_filenames.push_back(thread_filename);
            thread_loggers.push_back(std::make_unique<BufferedLatencyLogger>(thread_filename));
        }
#endif

#ifdef __CACHE_COUNTERS__
        // Stats collector callback using the passed aggregated_stats
        auto stats_collector = [&aggregated_stats](const auto* cache_provider) {
            std::lock_guard<std::mutex> lock(aggregated_stats.stats_mutex);
            StoreType::CacheType::aggregateThreadStats(
                cache_provider,
                aggregated_stats.hits,
                aggregated_stats.misses,
                aggregated_stats.evictions,
                aggregated_stats.dirty_evictions
            );
        };
#endif

        std::vector<std::thread> vtThreads;
        auto begin = std::chrono::steady_clock::now();
        
        for (int thread_id = 0; thread_id < threads; thread_id++) {
            size_t start_idx = thread_id * (records / threads);
            size_t end_idx = (thread_id == threads - 1) ? records : (thread_id + 1) * (records / threads);
            
            vtThreads.emplace_back(insert_worker<StoreType, KeyType, ValueType>,
                std::ref(ptrTree),
                std::cref(vtNumberData),
                start_idx,
                end_idx,
                thread_id
#ifdef __RECORD_LATENCY__
                ,thread_loggers[thread_id].get()
#endif
#ifdef __CACHE_COUNTERS__
                ,stats_collector
#endif
            );
        }
        
        // Wait for all threads to complete
        for (auto& thread : vtThreads) {
            thread.join();
        }
        
        auto end = std::chrono::steady_clock::now();
        duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);

#ifdef __RECORD_LATENCY__
        // Merge latency files
        if (insert_logger) {
            std::string main_filename = generate_latency_filename(
                "insert", cache_type_name, storage_type_name, 
                key_type_str, value_type_str, degree, records, 
                run_id, output_dir
            );
            
            // Close thread loggers first
            thread_loggers.clear();
            
            merge_latency_files(thread_filenames, main_filename);
        }
#endif
    }
#endif // __CONCURRENT__
}

template<typename StoreType, typename KeyType, typename ValueType>
void perform_search_operations(
    StoreType& ptrTree,
    const std::vector<KeyType>& vtNumberData,
    size_t records,
    int threads,
    std::chrono::nanoseconds& duration,
    const std::string& cache_type_name,
    const std::string& storage_type_name,
    const std::string& key_type_str,
    const std::string& value_type_str,
    size_t degree,
    int run_id,
    const std::string& output_dir
#ifdef __CACHE_COUNTERS__
    , auto& aggregated_stats
#endif
#ifdef __RECORD_LATENCY__
    , std::unique_ptr<BufferedLatencyLogger>& search_logger
#endif
) {
#ifndef __CONCURRENT__
    // Single-threaded implementation
    std::cout << "Performing search operations (single-threaded)..." << std::endl;
    auto begin = std::chrono::steady_clock::now();
    
    ValueType value;
    for (size_t i = 0; i < records; i++) {
#ifdef __RECORD_LATENCY__
        auto start = std::chrono::steady_clock::now();
#endif
        ptrTree.search(vtNumberData[i], value);
#ifdef __RECORD_LATENCY__
        auto end = std::chrono::steady_clock::now();
        if (search_logger) {
            search_logger->log_latency(i, std::chrono::duration_cast<std::chrono::nanoseconds>(end - start));
        }
#endif
    }
    
    auto end = std::chrono::steady_clock::now();
    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
    
#else // __CONCURRENT__
    if (threads == 1) {
        // Use single-threaded path even in concurrent build
        std::cout << "Performing search operations (single-threaded)..." << std::endl;
        auto begin = std::chrono::steady_clock::now();
        
        ValueType value;
        for (size_t i = 0; i < records; i++) {
#ifdef __RECORD_LATENCY__
            auto start = std::chrono::steady_clock::now();
#endif
            ptrTree.search(vtNumberData[i], value);
#ifdef __RECORD_LATENCY__
            auto end = std::chrono::steady_clock::now();
            if (search_logger) {
                search_logger->log_latency(i, std::chrono::duration_cast<std::chrono::nanoseconds>(end - start));
            }
#endif
        }
        
        auto end = std::chrono::steady_clock::now();
        duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
    } else {
        // Multi-threaded implementation
        std::cout << "Performing search operations with " << threads << " threads..." << std::endl;
        
#ifdef __RECORD_LATENCY__
        // Create per-thread loggers
        std::vector<std::unique_ptr<BufferedLatencyLogger>> thread_loggers;
        std::vector<std::string> thread_filenames;
        for (int i = 0; i < threads; i++) {
            std::string thread_filename = generate_latency_filename(
                "search", cache_type_name, storage_type_name, 
                key_type_str, value_type_str, degree, records, 
                run_id, output_dir, i
            );
            thread_filenames.push_back(thread_filename);
            thread_loggers.push_back(std::make_unique<BufferedLatencyLogger>(thread_filename));
        }
#endif

#ifdef __CACHE_COUNTERS__
        // Stats collector callback using the passed aggregated_stats
        auto stats_collector = [&aggregated_stats](const auto* cache_provider) {
            std::lock_guard<std::mutex> lock(aggregated_stats.stats_mutex);
            StoreType::CacheType::aggregateThreadStats(
                cache_provider,
                aggregated_stats.hits,
                aggregated_stats.misses,
                aggregated_stats.evictions,
                aggregated_stats.dirty_evictions
            );
        };
#endif

        std::vector<std::thread> vtThreads;
        auto begin = std::chrono::steady_clock::now();
        
        for (int thread_id = 0; thread_id < threads; thread_id++) {
            size_t start_idx = thread_id * (records / threads);
            size_t end_idx = (thread_id == threads - 1) ? records : (thread_id + 1) * (records / threads);
            
            vtThreads.emplace_back(search_worker<StoreType, KeyType, ValueType>,
                std::ref(ptrTree),
                std::cref(vtNumberData),
                start_idx,
                end_idx,
                thread_id
#ifdef __RECORD_LATENCY__
                ,thread_loggers[thread_id].get()
#endif
#ifdef __CACHE_COUNTERS__
                ,stats_collector
#endif
            );
        }
        
        // Wait for all threads to complete
        for (auto& thread : vtThreads) {
            thread.join();
        }
        
        auto end = std::chrono::steady_clock::now();
        duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);

#ifdef __RECORD_LATENCY__
        // Merge latency files
        if (search_logger) {
            std::string main_filename = generate_latency_filename(
                "search", cache_type_name, storage_type_name, 
                key_type_str, value_type_str, degree, records, 
                run_id, output_dir
            );
            
            // Close thread loggers first
            thread_loggers.clear();
            
            merge_latency_files(thread_filenames, main_filename);
        }
#endif
    }
#endif // __CONCURRENT__
}

template<typename StoreType, typename KeyType, typename ValueType>
void perform_delete_operations(
    StoreType& ptrTree,
    const std::vector<KeyType>& vtNumberData,
    size_t records,
    int threads,
    std::chrono::nanoseconds& duration,
    const std::string& cache_type_name,
    const std::string& storage_type_name,
    const std::string& key_type_str,
    const std::string& value_type_str,
    size_t degree,
    int run_id,
    const std::string& output_dir
#ifdef __CACHE_COUNTERS__
    , auto& aggregated_stats
#endif
#ifdef __RECORD_LATENCY__
    , std::unique_ptr<BufferedLatencyLogger>& delete_logger
#endif
) {
#ifndef __CONCURRENT__
    // Single-threaded implementation
    std::cout << "Performing delete operations (single-threaded)..." << std::endl;
    auto begin = std::chrono::steady_clock::now();
    
    for (size_t i = 0; i < records; i++) {
#ifdef __RECORD_LATENCY__
        auto start = std::chrono::steady_clock::now();
#endif
        ptrTree.remove(vtNumberData[i]);
#ifdef __RECORD_LATENCY__
        auto end = std::chrono::steady_clock::now();
        if (delete_logger) {
            delete_logger->log_latency(i, std::chrono::duration_cast<std::chrono::nanoseconds>(end - start));
        }
#endif
    }
    
    auto end = std::chrono::steady_clock::now();
    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
    
#else // __CONCURRENT__
    if (threads == 1) {
        // Use single-threaded path even in concurrent build
        std::cout << "Performing delete operations (single-threaded)..." << std::endl;
        auto begin = std::chrono::steady_clock::now();
        
        for (size_t i = 0; i < records; i++) {
#ifdef __RECORD_LATENCY__
            auto start = std::chrono::steady_clock::now();
#endif
            ptrTree.remove(vtNumberData[i]);
#ifdef __RECORD_LATENCY__
            auto end = std::chrono::steady_clock::now();
            if (delete_logger) {
                delete_logger->log_latency(i, std::chrono::duration_cast<std::chrono::nanoseconds>(end - start));
            }
#endif
        }
        
        auto end = std::chrono::steady_clock::now();
        duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
    } else {
        // Multi-threaded implementation
        std::cout << "Performing delete operations with " << threads << " threads..." << std::endl;
        
#ifdef __RECORD_LATENCY__
        // Create per-thread loggers
        std::vector<std::unique_ptr<BufferedLatencyLogger>> thread_loggers;
        std::vector<std::string> thread_filenames;
        for (int i = 0; i < threads; i++) {
            std::string thread_filename = generate_latency_filename(
                "delete", cache_type_name, storage_type_name, 
                key_type_str, value_type_str, degree, records, 
                run_id, output_dir, i
            );
            thread_filenames.push_back(thread_filename);
            thread_loggers.push_back(std::make_unique<BufferedLatencyLogger>(thread_filename));
        }
#endif

#ifdef __CACHE_COUNTERS__
        // Stats collector callback using the passed aggregated_stats
        auto stats_collector = [&aggregated_stats](const auto* cache_provider) {
            std::lock_guard<std::mutex> lock(aggregated_stats.stats_mutex);
            StoreType::CacheType::aggregateThreadStats(
                cache_provider,
                aggregated_stats.hits,
                aggregated_stats.misses,
                aggregated_stats.evictions,
                aggregated_stats.dirty_evictions
            );
        };
#endif

        std::vector<std::thread> vtThreads;
        auto begin = std::chrono::steady_clock::now();
        
        for (int thread_id = 0; thread_id < threads; thread_id++) {
            size_t start_idx = thread_id * (records / threads);
            size_t end_idx = (thread_id == threads - 1) ? records : (thread_id + 1) * (records / threads);
            
            vtThreads.emplace_back(delete_worker<StoreType, KeyType, ValueType>,
                std::ref(ptrTree),
                std::cref(vtNumberData),
                start_idx,
                end_idx,
                thread_id
#ifdef __RECORD_LATENCY__
                ,thread_loggers[thread_id].get()
#endif
#ifdef __CACHE_COUNTERS__
                ,stats_collector
#endif
            );
        }
        
        // Wait for all threads to complete
        for (auto& thread : vtThreads) {
            thread.join();
        }
        
        auto end = std::chrono::steady_clock::now();
        duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);

#ifdef __RECORD_LATENCY__
        // Merge latency files
        if (delete_logger) {
            std::string main_filename = generate_latency_filename(
                "delete", cache_type_name, storage_type_name, 
                key_type_str, value_type_str, degree, records, 
                run_id, output_dir
            );
            
            // Close thread loggers first
            thread_loggers.clear();
            
            merge_latency_files(thread_filenames, main_filename);
        }
#endif
    }
#endif // __CONCURRENT__
}

// Template function to run cache benchmark with real cache implementation
template<typename StoreType, typename KeyType, typename ValueType>
void run_real_cache_benchmark(
    StoreType& ptrTree,
    const std::string& cache_type_name,
    const std::string& storage_type_name,
    const std::string& key_type_str,
    const std::string& value_type_str,
    size_t degree,
    size_t records,
    const std::string& operation,
    CSVLogger& logger,
    int run_id,
    const std::string& output_dir = "",
    int threads = 1,
    const std::string& config_name = "",
    const std::string& cache_size_percentage = "",
    size_t cache_page_limit = 0
) {
    std::cout << "Running real cache benchmark: " << cache_type_name << "/" << storage_type_name << std::endl;
    std::cout << "Degree: " << degree << ", Records: " << records << ", Operation: " << operation << std::endl;
    std::cout << "Threads: " << threads << std::endl;


#ifdef __CACHE_COUNTERS__
    // Aggregated stats container for the entire benchmark run
    struct AggregatedStats {
        std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> hits;
        std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> misses;
        std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> evictions;
        std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> dirty_evictions;
        std::mutex stats_mutex;
    };
    
    AggregatedStats aggregated_stats;
#endif

#ifdef __RECORD_LATENCY__
    // Create latency loggers with consistent naming
    std::unique_ptr<BufferedLatencyLogger> insert_logger, search_logger, delete_logger;
    
    if (operation == "insert") {
        insert_logger = std::make_unique<BufferedLatencyLogger>(
            generate_latency_filename("insert", cache_type_name, storage_type_name, 
                                    key_type_str, value_type_str, degree, records, run_id, output_dir));
    } else if (operation.find("search") != std::string::npos) {
        search_logger = std::make_unique<BufferedLatencyLogger>(
            generate_latency_filename("search", cache_type_name, storage_type_name, 
                                    key_type_str, value_type_str, degree, records, run_id, output_dir));
    } else if (operation == "delete") {
        delete_logger = std::make_unique<BufferedLatencyLogger>(
            generate_latency_filename("delete", cache_type_name, storage_type_name, 
                                    key_type_str, value_type_str, degree, records, run_id, output_dir));
    }
#endif

    try {
        std::cout << "Tree initialized successfully" << std::endl;

        // Load test data from pre-generated workload files
        std::vector<KeyType> vtNumberData;
        
        // Declare duration variable to be passed to operation functions
        std::chrono::nanoseconds duration{0};
        
        try {
            // Determine the distribution type based on operation
            workloadgenerator::DistributionType dist_type;
#ifndef __CONCURRENT__
            if (operation.find("random") != std::string::npos || operation == "insert" || operation == "delete") {
                dist_type = workloadgenerator::DistributionType::Random;
            } else if (operation.find("sequential") != std::string::npos) {
                dist_type = workloadgenerator::DistributionType::Sequential;
            } else if (operation.find("uniform") != std::string::npos) {
                dist_type = workloadgenerator::DistributionType::Uniform;
            } else if (operation.find("zipfian") != std::string::npos) {
                dist_type = workloadgenerator::DistributionType::Zipfian;
            } else {
                // Default to random for insert/delete operations
                dist_type = workloadgenerator::DistributionType::Random;
            }
#else
                dist_type = workloadgenerator::DistributionType::Sequential;
#endif
            
            // Generate filename using the correct key type
            std::string key_workload_type = get_workload_type_string(key_type_str);
            std::string filename = workloadgenerator::generate_filename(key_workload_type, dist_type, records);
            std::cout << "Loading workload data from: " << filename << std::endl;
            
            // Load data based on the actual key type
            if constexpr (std::is_same_v<KeyType, uint64_t>) {
                vtNumberData = workloadgenerator::load_data_from_file<uint64_t>(filename);
            } else if constexpr (std::is_same_v<KeyType, CHAR16>) {
                std::vector<CHAR16> char16_data = workloadgenerator::load_data_from_file<CHAR16>(filename);
                vtNumberData.assign(char16_data.begin(), char16_data.end());
            } else {
                // For int or other types, load as uint64_t and convert
                std::vector<uint64_t> uint64_data = workloadgenerator::load_data_from_file<uint64_t>(filename);
                vtNumberData.reserve(uint64_data.size());
                for (uint64_t val : uint64_data) {
                    vtNumberData.push_back(static_cast<KeyType>(val));
                }
            }
            
            std::cout << "Loaded " << vtNumberData.size() << " records from workload file" << std::endl;
            
        } catch (const std::exception& e) {
            std::cerr << "Warning: Could not load workload file (" << e.what() << "), generating data in-memory" << std::endl;
            
            // Fallback to in-memory generation
            vtNumberData.resize(records);
            if constexpr (std::is_same_v<KeyType, uint64_t> || std::is_same_v<KeyType, int>) {
                std::iota(vtNumberData.begin(), vtNumberData.end(), static_cast<KeyType>(1));
            } else if constexpr (std::is_same_v<KeyType, CHAR16>) {
                // For CHAR16, generate simple sequential data
                for (size_t i = 0; i < records; ++i) {
                    vtNumberData[i] = static_cast<CHAR16>(i + 1);
                }
            }
            
            // Shuffle for random operations
            if (operation.find("random") != std::string::npos || operation == "insert" || operation == "delete") {
                std::random_device rd;
                std::mt19937 g(rd());
                std::shuffle(vtNumberData.begin(), vtNumberData.end(), g);
            }
        }

        if (operation == "insert") {
            perform_insert_operations<StoreType, KeyType, ValueType>(
                ptrTree, vtNumberData, records, threads, duration,
                cache_type_name, storage_type_name, key_type_str, value_type_str,
                degree, run_id, output_dir
#ifdef __CACHE_COUNTERS__
                , aggregated_stats
#endif
#ifdef __RECORD_LATENCY__
                , insert_logger
#endif
            );
        } else if (operation.find("search") != std::string::npos) {
            std::cout << "Performing insert operations first..." << std::endl;
            
            // Load separate data for insert operations (always random)
            std::vector<KeyType> vtInsertData;
            try {
                std::string key_workload_type = get_workload_type_string(key_type_str);
                std::string insert_filename = workloadgenerator::generate_filename(key_workload_type, workloadgenerator::DistributionType::Random, records);
                std::cout << "Loading insert data from: " << insert_filename << std::endl;
                
                // Load data based on the actual key type
                if constexpr (std::is_same_v<KeyType, uint64_t>) {
                    vtInsertData = workloadgenerator::load_data_from_file<uint64_t>(insert_filename);
                } else if constexpr (std::is_same_v<KeyType, CHAR16>) {
                    std::vector<CHAR16> char16_data = workloadgenerator::load_data_from_file<CHAR16>(insert_filename);
                    vtInsertData.assign(char16_data.begin(), char16_data.end());
                } else {
                    // For int or other types, load as uint64_t and convert
                    std::vector<uint64_t> uint64_insert_data = workloadgenerator::load_data_from_file<uint64_t>(insert_filename);
                    vtInsertData.reserve(uint64_insert_data.size());
                    for (uint64_t val : uint64_insert_data) {
                        vtInsertData.push_back(static_cast<KeyType>(val));
                    }
                }
                
                std::cout << "Loaded " << vtInsertData.size() << " records for insert operations" << std::endl;
                
            } catch (const std::exception& e) {
                std::cerr << "Warning: Could not load insert workload file (" << e.what() << "), generating random data in-memory" << std::endl;
                
                // Fallback to in-memory generation for insert data
                vtInsertData.resize(records);
                if constexpr (std::is_same_v<KeyType, uint64_t> || std::is_same_v<KeyType, int>) {
                    std::iota(vtInsertData.begin(), vtInsertData.end(), static_cast<KeyType>(1));
                } else if constexpr (std::is_same_v<KeyType, CHAR16>) {
                    for (size_t i = 0; i < records; ++i) {
                        vtInsertData[i] = static_cast<CHAR16>(i + 1);
                    }
                }
                std::random_device rd;
                std::mt19937 g(rd());
                std::shuffle(vtInsertData.begin(), vtInsertData.end(), g);
            }
            
            // First insert all data using random distribution (single-threaded for setup)
            for (size_t i = 0; i < records; i++) {
                if constexpr (std::is_same_v<ValueType, KeyType>) {
                    ptrTree.insert(vtInsertData[i], vtInsertData[i]);
                } else {
                    ValueType value = static_cast<ValueType>(vtInsertData[i]);
                    ptrTree.insert(vtInsertData[i], value);
                }
            }
            //ptrTree.flush();
            // Then perform search operations with threading support
            perform_search_operations<StoreType, KeyType, ValueType>(
                ptrTree, vtNumberData, records, threads, duration,
                cache_type_name, storage_type_name, key_type_str, value_type_str,
                degree, run_id, output_dir
#ifdef __CACHE_COUNTERS__
                , aggregated_stats
#endif
#ifdef __RECORD_LATENCY__
                , search_logger
#endif
            );
        } else if (operation == "delete") {
            std::cout << "Performing insert operations first..." << std::endl;
            
            // Load separate data for insert operations (always random)
            std::vector<KeyType> vtInsertData;
            try {
                std::string key_workload_type = get_workload_type_string(key_type_str);
                std::string insert_filename = workloadgenerator::generate_filename(key_workload_type, workloadgenerator::DistributionType::Random, records);
                std::cout << "Loading insert data from: " << insert_filename << std::endl;
                
                // Load data based on the actual key type
                if constexpr (std::is_same_v<KeyType, uint64_t>) {
                    vtInsertData = workloadgenerator::load_data_from_file<uint64_t>(insert_filename);
                } else if constexpr (std::is_same_v<KeyType, CHAR16>) {
                    std::vector<CHAR16> char16_data = workloadgenerator::load_data_from_file<CHAR16>(insert_filename);
                    vtInsertData.assign(char16_data.begin(), char16_data.end());
                } else {
                    // For int or other types, load as uint64_t and convert
                    std::vector<uint64_t> uint64_insert_data = workloadgenerator::load_data_from_file<uint64_t>(insert_filename);
                    vtInsertData.reserve(uint64_insert_data.size());
                    for (uint64_t val : uint64_insert_data) {
                        vtInsertData.push_back(static_cast<KeyType>(val));
                    }
                }
                
                std::cout << "Loaded " << vtInsertData.size() << " records for insert operations" << std::endl;
                
            } catch (const std::exception& e) {
                std::cerr << "Warning: Could not load insert workload file (" << e.what() << "), generating random data in-memory" << std::endl;
                
                // Fallback to in-memory generation for insert data
                vtInsertData.resize(records);
                if constexpr (std::is_same_v<KeyType, uint64_t> || std::is_same_v<KeyType, int>) {
                    std::iota(vtInsertData.begin(), vtInsertData.end(), static_cast<KeyType>(1));
                } else if constexpr (std::is_same_v<KeyType, CHAR16>) {
                    for (size_t i = 0; i < records; ++i) {
                        vtInsertData[i] = static_cast<CHAR16>(i + 1);
                    }
                }
                std::random_device rd;
                std::mt19937 g(rd());
                std::shuffle(vtInsertData.begin(), vtInsertData.end(), g);
            }
            
            // First insert all data using random distribution (single-threaded for setup)
            for (size_t i = 0; i < records; i++) {
                if constexpr (std::is_same_v<ValueType, KeyType>) {
                    ptrTree.insert(vtInsertData[i], vtInsertData[i]);
                } else {
                    ValueType value = static_cast<ValueType>(vtInsertData[i]);
                    ptrTree.insert(vtInsertData[i], value);
                }
            }
            
            // Then perform delete operations with threading support
            perform_delete_operations<StoreType, KeyType, ValueType>(
                ptrTree, vtNumberData, records, threads, duration,
                cache_type_name, storage_type_name, key_type_str, value_type_str,
                degree, run_id, output_dir
#ifdef __CACHE_COUNTERS__
                , aggregated_stats
#endif
#ifdef __RECORD_LATENCY__
                , delete_logger
#endif
            );
        }

        // Convert duration from nanoseconds to microseconds for display and throughput calculation
        auto duration_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration);
        
        double throughput = (records * 1000000.0) / duration_microseconds.count();
        
        std::cout << "Operation completed!" << std::endl;
        std::cout << "Time taken: " << duration_microseconds.count() << " microseconds" << std::endl;
        std::cout << "Throughput: " << throughput << " ops/sec" << std::endl;
        
#ifdef __CACHE_COUNTERS__
        // Collect cache statistics
        uint64_t total_hits = 0, total_misses = 0, total_evictions = 0, total_dirty_evictions = 0;
        double cache_hit_rate = 0.0;
        
        // Flush any pending operations
        //ptrTree.flush();
        
        // Collect stats from main thread and add to the shared aggregated_stats
        auto cache = ptrTree.getCache();
        auto cache_provider = cache->getCacheStatsProvider();
        if (cache_provider) {
            // Stats collector callback using the shared aggregated_stats
            auto stats_collector = [&aggregated_stats](const auto* cache_provider) {
                std::lock_guard<std::mutex> lock(aggregated_stats.stats_mutex);
                StoreType::CacheType::aggregateThreadStats(
                    cache_provider,
                    aggregated_stats.hits,
                    aggregated_stats.misses,
                    aggregated_stats.evictions,
                    aggregated_stats.dirty_evictions
                );
            };
            
            stats_collector(cache_provider);
            
            // Calculate total counts
            for (const auto& entry : aggregated_stats.hits) total_hits += entry.second;
            for (const auto& entry : aggregated_stats.misses) total_misses += entry.second;
            for (const auto& entry : aggregated_stats.evictions) total_evictions += entry.second;
            for (const auto& entry : aggregated_stats.dirty_evictions) total_dirty_evictions += entry.second;
            
            if (total_hits + total_misses > 0) {
                cache_hit_rate = (double)total_hits / (total_hits + total_misses) * 100.0;
            }
            
            std::cout << "=== Cache Statistics ===" << std::endl;
            std::cout << "Total hits: " << total_hits << std::endl;
            std::cout << "Total misses: " << total_misses << std::endl;
            std::cout << "Total evictions: " << total_evictions << std::endl;
            std::cout << "Total dirty evictions: " << total_dirty_evictions << std::endl;
            std::cout << "Cache hit rate: " << std::fixed << std::setprecision(2) << cache_hit_rate << "%" << std::endl;
            std::cout << "========================" << std::endl;
        }
        
        // Convert cache_size_percentage to decimal format (e.g., "10" -> "0.10")
        std::string cache_size_decimal = "";
        if (!cache_size_percentage.empty()) {
            try {
                double percentage_value = std::stod(cache_size_percentage);
                std::ostringstream oss;
                oss << std::fixed << std::setprecision(4) << (percentage_value / 100.0);
                cache_size_decimal = oss.str();
            } catch (const std::exception& e) {
                cache_size_decimal = cache_size_percentage; // fallback to original if conversion fails
            }
        }
        
        // Log results to CSV with cache statistics
        logger.log_result(
            "BplusTreeSOA",         // tree_type
            key_type_str,           // key_type
            value_type_str,         // value_type
            cache_type_name,        // policy_name
            storage_type_name,      // storage_type
            config_name,            // config_name
            records,                // record_count
            degree,                 // degree
            operation,              // operation
            duration_microseconds.count(),       // time_us
            throughput,             // throughput_ops_sec
            run_id,                 // test_run_id
            total_hits,             // cache_hits
            total_misses,           // cache_misses
            total_evictions,        // cache_evictions
            total_dirty_evictions,  // cache_dirty_evictions
            cache_hit_rate,         // cache_hit_rate
            cache_size_decimal,     // cache_size (converted to decimal)
            cache_page_limit,       // cache_page_limit
            threads                 // thread_count
        );
#else
        // Convert cache_size_percentage to decimal format (e.g., "10" -> "0.10")
        std::string cache_size_decimal = "";
        if (!cache_size_percentage.empty()) {
            try {
                double percentage_value = std::stod(cache_size_percentage);
                std::ostringstream oss;
                oss << std::fixed << std::setprecision(4) << (percentage_value / 100.0);
                cache_size_decimal = oss.str();
            } catch (const std::exception& e) {
                cache_size_decimal = cache_size_percentage; // fallback to original if conversion fails
            }
        }
        

        // Log results to CSV without cache statistics
        logger.log_result(
            "BplusTreeSOA",         // tree_type
            key_type_str,           // key_type
            value_type_str,         // value_type
            cache_type_name,        // policy_name
            storage_type_name,      // storage_type
            config_name,            // config_name
            records,                // record_count
            degree,                 // degree
            operation,              // operation
            duration_microseconds.count(),       // time_us
            throughput,             // throughput_ops_sec
            run_id,                 // test_run_id
            0, 0, 0, 0, 0.0,        // cache stats (default values)
            cache_size_decimal,     // cache_size (converted to decimal)
            cache_page_limit,       // cache_page_limit
            threads                 // thread_count
        );
#endif

#ifdef __RECORD_LATENCY__
        // Force flush all latency loggers before function ends
        if (insert_logger) insert_logger->flush();
        if (search_logger) search_logger->flush();
        if (delete_logger) delete_logger->flush();
#endif
        
    } catch (const std::exception& e) {
        std::cerr << "Error in cache benchmark: " << e.what() << std::endl;
        
        // Log error result (use the version without cache stats for errors)
        logger.log_result(
            "BplusTreeSOA",         // tree_type
            key_type_str,           // key_type
            value_type_str,         // value_type
            cache_type_name + "_ERROR", // policy_name
            storage_type_name,      // storage_type
            config_name,            // config_name
            records,                // record_count
            degree,                 // degree
            operation,              // operation
            -1,                     // time_us (error indicator)
            0.0,                    // throughput_ops_sec
            run_id,                 // test_run_id
            0, 0, 0, 0, 0.0,        // cache stats (default values)
            cache_size_percentage,  // cache_size
            cache_page_limit,       // cache_page_limit
            threads                 // thread_count
        );
    }
}

// Main test function with cache type dispatch
void test_cache_combinations(
    const std::vector<std::string>& cache_types,
    const std::vector<std::string>& storage_types,
    const std::vector<int>& cache_sizes,
    const std::vector<size_t>& degrees,
    const std::vector<size_t>& record_counts,
    const std::vector<std::string>& operations,
    int page_size = 2048,
    long long memory_size = 1073741824LL,
    int num_runs = 1,
    const std::string& output_dir = "",
    int threads = 1,
    const std::string& config_name = "",
    const std::string& cache_size_percentage = "",
    size_t cache_page_limit = 0
) {
    // Create CSV logger with timestamp and output directory
    std::string csv_filename = CSVLogger::generate_filename("benchmark_real_cache");
    CSVLogger logger(csv_filename, output_dir);
    logger.write_header();
    std::string full_path = output_dir.empty() ? csv_filename : output_dir + "/" + csv_filename;
    std::cout << "Logging results to: " << full_path << std::endl;
    std::cout << "DEBUG: test_cache_combinations - cache_size_percentage = '" << cache_size_percentage << "'" << std::endl;
    std::cout << "DEBUG: test_cache_combinations - cache_page_limit = " << cache_page_limit << std::endl;

    for (const std::string& cache_type : cache_types) {
        for (const std::string& storage_type : storage_types) {
            for (int cache_size : cache_sizes) {
                for (size_t degree : degrees) {
                    for (size_t records : record_counts) {
                        for (const std::string& operation : operations) {
                            std::cout << "\n--- Testing " << cache_type << "/" << storage_type 
                                     << " (cache_size=" << cache_size << ", degree=" << degree 
                                     << ", records=" << records << ", operation=" << operation << ") ---" << std::endl;
                            
                            for (int run = 1; run <= num_runs; run++) {
                                std::cout << "Run " << run << "/" << num_runs << "... ";
                                
                                // Support all cache and storage combinations (hardcoded to int/int for compatibility)
                                if (cache_type == "LRU" && storage_type == "VolatileStorage") {
                                    using T = BPlusStoreTraits<int, int, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, ObjectFatUID, LRUCacheObject, LRUCache, VolatileStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_real_cache_benchmark<typename T::StoreType, int, int>(ptrTree, cache_type, storage_type, "int", "int", degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "LRU" && storage_type == "FileStorage") {
                                    using T = BPlusStoreTraits<int, int, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, ObjectFatUID, LRUCacheObject, LRUCache, FileStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, FILE_STORAGE_PATH);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_real_cache_benchmark<typename T::StoreType, int, int>(ptrTree, cache_type, storage_type, "int", "int", degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "LRU" && storage_type == "PMemStorage") {
                                    using T = BPlusStoreTraits<int, int, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, ObjectFatUID, LRUCacheObject, LRUCache, PMemStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, PMEM_STORAGE_PATH);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_real_cache_benchmark<typename T::StoreType, int, int>(ptrTree, cache_type, storage_type, "int", "int", degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "A2Q" && storage_type == "VolatileStorage") {
                                    using T = BPlusStoreTraits<int, int, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, ObjectFatUID, A2QCacheObject, A2QCache, VolatileStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_real_cache_benchmark<typename T::StoreType, int, int>(ptrTree, cache_type, storage_type, "int", "int", degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "A2Q" && storage_type == "FileStorage") {
                                    using T = BPlusStoreTraits<int, int, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, ObjectFatUID, A2QCacheObject, A2QCache, FileStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, FILE_STORAGE_PATH);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_real_cache_benchmark<typename T::StoreType, int, int>(ptrTree, cache_type, storage_type, "int", "int", degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "A2Q" && storage_type == "PMemStorage") {
                                    using T = BPlusStoreTraits<int, int, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, ObjectFatUID, A2QCacheObject, A2QCache, PMemStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, PMEM_STORAGE_PATH);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_real_cache_benchmark<typename T::StoreType, int, int>(ptrTree, cache_type, storage_type, "int", "int", degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "CLOCK" && storage_type == "VolatileStorage") {
                                    using T = BPlusStoreTraits<int, int, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, ObjectFatUID, CLOCKCacheObject, CLOCKCache, VolatileStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_real_cache_benchmark<typename T::StoreType, int, int>(ptrTree, cache_type, storage_type, "int", "int", degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "CLOCK" && storage_type == "FileStorage") {
                                    using T = BPlusStoreTraits<int, int, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, ObjectFatUID, CLOCKCacheObject, CLOCKCache, FileStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, FILE_STORAGE_PATH);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_real_cache_benchmark<typename T::StoreType, int, int>(ptrTree, cache_type, storage_type, "int", "int", degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "CLOCK" && storage_type == "PMemStorage") {
                                    using T = BPlusStoreTraits<int, int, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT, ObjectFatUID, CLOCKCacheObject, CLOCKCache, PMemStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, PMEM_STORAGE_PATH);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_real_cache_benchmark<typename T::StoreType, int, int>(ptrTree, cache_type, storage_type, "int", "int", degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else {
                                    std::cout << "Unsupported cache/storage combination: " << cache_type << "/" << storage_type << std::endl;
                                }

                                // Brief sleep to let system settle between runs
                                //if (run < num_runs) {
                                std::cout << "sleep for 2 mins.................................." << std::endl;
                                    std::this_thread::sleep_for(std::chrono::seconds(2));
                                //}
                            }
                            std::cout << "Completed all runs for this configuration." << std::endl;
                        }
                    }
                }
            }
        }
    }
    
    std::cout << "\nReal cache benchmark completed!" << std::endl;
    std::cout << "Results saved to: " << csv_filename << std::endl;
}

// Function for backward compatibility with existing benchmark.cpp
void test_with_cache_type_string(const std::string& cache_type, int num_runs = 1, const std::string& output_dir = "") {
    std::cout << "\n=== Testing B+ Tree with " << cache_type << " Real Cache ===" << std::endl;
    std::cout << "Number of runs per configuration: " << num_runs << std::endl;

    // Test configurations
    std::vector<std::string> cache_types = {cache_type};
    std::vector<std::string> storage_types = {"VolatileStorage", "FileStorage", "PMemStorage"};
    std::vector<int> cache_sizes = {100, 500};
    std::vector<size_t> degrees = {64, 128};
    std::vector<size_t> record_counts = {10000, 50000};
    std::vector<std::string> operations = {"insert", "search_random", "delete"};
    
    test_cache_combinations(
        cache_types, storage_types, cache_sizes, degrees, record_counts, operations,
        2048, 1073741824LL, num_runs, output_dir, 1
    );
}

// Function that uses shell script parameters instead of hardcoded defaults
void test_with_shell_parameters(
    const std::string& cache_type, 
    int num_runs = 1, 
    const std::string& output_dir = "",
    const std::string& storage_type = "VolatileStorage",
    int cache_size = 100,
    int page_size = 2048,
    long long memory_size = 1073741824LL,
    const std::vector<std::string>& operations = {"insert", "search_random", "search_sequential", "search_uniform", "search_zipfian", "delete"},
    const std::vector<size_t>& degrees = {64, 128, 256},
    const std::vector<size_t>& record_counts = {100000, 500000, 1000000},
    int threads = 1,
    const std::string& config_name = "",
    const std::string& cache_size_percentage = "",
    size_t cache_page_limit = 0
) {
    std::cout << "\n=== Testing B+ Tree with Shell Script Parameters ===" << std::endl;
    std::cout << "Cache Type: " << cache_type << std::endl;
    std::cout << "Storage Type: " << storage_type << std::endl;
    std::cout << "Cache Size: " << cache_size << std::endl;
    std::cout << "Page Size: " << page_size << std::endl;
    std::cout << "Memory Size: " << memory_size << std::endl;
    std::cout << "Number of runs per configuration: " << num_runs << std::endl;
    std::cout << "Number of threads: " << threads << std::endl;
    std::cout << "DEBUG: cache_size_percentage = '" << cache_size_percentage << "'" << std::endl;
    std::cout << "DEBUG: cache_page_limit = " << cache_page_limit << std::endl;

    // Use the provided parameters instead of hardcoded defaults
    std::vector<std::string> cache_types = {cache_type};
    std::vector<std::string> storage_types = {storage_type};
    std::vector<int> cache_sizes = {cache_size};
    
    test_cache_combinations(
        cache_types, storage_types, cache_sizes, degrees, record_counts, operations,
        page_size, memory_size, num_runs, output_dir, threads, config_name, cache_size_percentage, cache_page_limit
    );
}

// Template helper function to dispatch based on key/value types
template<typename KeyType, typename ValueType, uint8_t DataNodeUID, uint8_t IndexNodeUID>
void run_typed_cache_benchmark(
    const std::string& cache_type,
    const std::string& storage_type,
    const std::string& key_type_str,
    const std::string& value_type_str,
    size_t degree,
    size_t records,
    const std::string& operation,
    CSVLogger& logger,
    int run,
    int cache_size,
    int page_size,
    long long memory_size,
    const std::string& output_dir = "",
    int threads = 1,
    const std::string& config_name = "",
    const std::string& cache_size_percentage = "",
    size_t cache_page_limit = 0
) {
    if (cache_type == "LRU" && storage_type == "VolatileStorage") {
        using T = BPlusStoreTraits<KeyType, ValueType, DataNodeUID, IndexNodeUID, ObjectFatUID, LRUCacheObject, LRUCache, VolatileStorage>;
        typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size);
        ptrTree.template init<typename T::DataNodeType>();
        run_real_cache_benchmark<typename T::StoreType, KeyType, ValueType>(ptrTree, cache_type, storage_type, key_type_str, value_type_str, degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
    } else if (cache_type == "LRU" && storage_type == "FileStorage") {
        using T = BPlusStoreTraits<KeyType, ValueType, DataNodeUID, IndexNodeUID, ObjectFatUID, LRUCacheObject, LRUCache, FileStorage>;
        typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, FILE_STORAGE_PATH);
        ptrTree.template init<typename T::DataNodeType>();
        run_real_cache_benchmark<typename T::StoreType, KeyType, ValueType>(ptrTree, cache_type, storage_type, key_type_str, value_type_str, degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
    } else if (cache_type == "LRU" && storage_type == "PMemStorage") {
        using T = BPlusStoreTraits<KeyType, ValueType, DataNodeUID, IndexNodeUID, ObjectFatUID, LRUCacheObject, LRUCache, PMemStorage>;
        typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, PMEM_STORAGE_PATH);
        ptrTree.template init<typename T::DataNodeType>();
        run_real_cache_benchmark<typename T::StoreType, KeyType, ValueType>(ptrTree, cache_type, storage_type, key_type_str, value_type_str, degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
    } else if (cache_type == "A2Q" && storage_type == "VolatileStorage") {
        using T = BPlusStoreTraits<KeyType, ValueType, DataNodeUID, IndexNodeUID, ObjectFatUID, A2QCacheObject, A2QCache, VolatileStorage>;
        typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size);
        ptrTree.template init<typename T::DataNodeType>();
        run_real_cache_benchmark<typename T::StoreType, KeyType, ValueType>(ptrTree, cache_type, storage_type, key_type_str, value_type_str, degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
    } else if (cache_type == "A2Q" && storage_type == "FileStorage") {
        using T = BPlusStoreTraits<KeyType, ValueType, DataNodeUID, IndexNodeUID, ObjectFatUID, A2QCacheObject, A2QCache, FileStorage>;
        typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, FILE_STORAGE_PATH);
        ptrTree.template init<typename T::DataNodeType>();
        run_real_cache_benchmark<typename T::StoreType, KeyType, ValueType>(ptrTree, cache_type, storage_type, key_type_str, value_type_str, degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
    } else if (cache_type == "A2Q" && storage_type == "PMemStorage") {
        using T = BPlusStoreTraits<KeyType, ValueType, DataNodeUID, IndexNodeUID, ObjectFatUID, A2QCacheObject, A2QCache, PMemStorage>;
        typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, PMEM_STORAGE_PATH);
        ptrTree.template init<typename T::DataNodeType>();
        run_real_cache_benchmark<typename T::StoreType, KeyType, ValueType>(ptrTree, cache_type, storage_type, key_type_str, value_type_str, degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
    } else if (cache_type == "CLOCK" && storage_type == "VolatileStorage") {
        using T = BPlusStoreTraits<KeyType, ValueType, DataNodeUID, IndexNodeUID, ObjectFatUID, CLOCKCacheObject, CLOCKCache, VolatileStorage>;
        typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size);
        ptrTree.template init<typename T::DataNodeType>();
        run_real_cache_benchmark<typename T::StoreType, KeyType, ValueType>(ptrTree, cache_type, storage_type, key_type_str, value_type_str, degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
    } else if (cache_type == "CLOCK" && storage_type == "FileStorage") {
        using T = BPlusStoreTraits<KeyType, ValueType, DataNodeUID, IndexNodeUID, ObjectFatUID, CLOCKCacheObject, CLOCKCache, FileStorage>;
        typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, FILE_STORAGE_PATH);
        ptrTree.template init<typename T::DataNodeType>();
        run_real_cache_benchmark<typename T::StoreType, KeyType, ValueType>(ptrTree, cache_type, storage_type, key_type_str, value_type_str, degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
    } else if (cache_type == "CLOCK" && storage_type == "PMemStorage") {
        using T = BPlusStoreTraits<KeyType, ValueType, DataNodeUID, IndexNodeUID, ObjectFatUID, CLOCKCacheObject, CLOCKCache, PMemStorage>;
        typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, PMEM_STORAGE_PATH);
        ptrTree.template init<typename T::DataNodeType>();
        run_real_cache_benchmark<typename T::StoreType, KeyType, ValueType>(ptrTree, cache_type, storage_type, key_type_str, value_type_str, degree, records, operation, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
    } else {
        std::cout << "Unsupported cache/storage combination: " << cache_type << "/" << storage_type << std::endl;
    }
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
    long long memory_size = 1073741824LL,
    int threads = 1,
    const std::string& config_name = "",
    const std::string& cache_size_percentage = "",
    size_t cache_page_limit = 0
) {
    std::cout << "\n=== Single Real Cache Configuration Test ===" << std::endl;
    std::cout << "Cache: " << cache_type << "/" << storage_type << std::endl;
    std::cout << "Tree: " << tree_type << ", Operation: " << operation << std::endl;
    std::cout << "Records: " << records << ", Degree: " << degree << ", Runs: " << runs << std::endl;
    std::cout << "Threads: " << threads << std::endl;
    
    // Create CSV logger with output directory
    std::string csv_filename = CSVLogger::generate_filename("benchmark_real_cache_single");
    CSVLogger logger(csv_filename, output_dir);
    logger.write_header();
    std::string full_path = output_dir.empty() ? csv_filename : output_dir + "/" + csv_filename;
    std::cout << "Logging results to: " << full_path << std::endl;
    
    for (size_t run = 1; run <= runs; run++) {
        std::cout << "\n--- Run " << run << "/" << runs << " ---" << std::endl;
        
        // Dispatch based on key/value type combinations
        if (key_type == "int" && value_type == "int") {
            run_typed_cache_benchmark<int, int, TYPE_UID::DATA_NODE_INT_INT, TYPE_UID::INDEX_NODE_INT_INT>(
                cache_type, storage_type, key_type, value_type, degree, records, operation, logger, run, cache_size, page_size, memory_size, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
        } else if (key_type == "uint64_t" && value_type == "uint64_t") {
            run_typed_cache_benchmark<uint64_t, uint64_t, TYPE_UID::DATA_NODE_UINT64_UINT64, TYPE_UID::INDEX_NODE_UINT64_UINT64>(
                cache_type, storage_type, key_type, value_type, degree, records, operation, logger, run, cache_size, page_size, memory_size, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
        } else if (key_type == "uint64_t" && value_type == "char16") {
            run_typed_cache_benchmark<uint64_t, CHAR16, TYPE_UID::DATA_NODE_UINT64_CHAR16, TYPE_UID::INDEX_NODE_UINT64_CHAR16>(
                cache_type, storage_type, key_type, value_type, degree, records, operation, logger, run, cache_size, page_size, memory_size, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
        } else if (key_type == "char16" && value_type == "char16") {
            run_typed_cache_benchmark<CHAR16, CHAR16, TYPE_UID::DATA_NODE_CHAR16_CHAR16, TYPE_UID::INDEX_NODE_CHAR16_CHAR16>(
                cache_type, storage_type, key_type, value_type, degree, records, operation, logger, run, cache_size, page_size, memory_size, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
        } else {
            std::cout << "Unsupported key/value type combination: " << key_type << "/" << value_type << std::endl;
            std::cout << "Supported combinations:" << std::endl;
            std::cout << "  - int/int" << std::endl;
            std::cout << "  - uint64_t/uint64_t" << std::endl;
            std::cout << "  - uint64_t/char16" << std::endl;
            std::cout << "  - char16/char16" << std::endl;
        }
        
        // Add sleep between runs to let system settle
        //if (run < runs) {
            std::cout << "sleep for 2 mins.................................." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(2));
        //}
    }
    
    std::cout << "\nSingle real cache configuration test completed!" << std::endl;
    std::cout << "Results saved to: " << csv_filename << std::endl;
}

} // namespace bm_tree_with_cache_real

#endif // __TREE_WITH_CACHE__