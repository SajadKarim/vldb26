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
#include <barrier>
#ifdef __GLIBC__
#include <malloc.h>
#endif

// Must include common.h first to define WAL_FILE_PATH and other constants
#include "common.h"
#include "csv_logger.hpp"
#include "ycsbworkloadgenerator.hpp"

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
inline std::string generate_ycsb_latency_filename(const std::string& workload_type, 
                                           const std::string& cache_type_name,
                                           const std::string& storage_type_name,
                                           const std::string& key_type_str,
                                           const std::string& value_type_str,
                                           size_t degree, size_t records, int run_id,
                                           const std::string& output_dir = "",
                                           int thread_id = -1) {
    std::string filename = "latency_ycsb_" + workload_type + "_" + cache_type_name + "_" + storage_type_name + 
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
inline void merge_ycsb_latency_files(const std::vector<std::string>& thread_filenames,
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

namespace bm_tree_with_cache_ycsb {

#ifdef __CACHE_COUNTERS__
// Callback type for collecting cache stats from threads
template<typename BPlusStoreType>
using StatsCollectorCallback = std::function<void(const CacheStatsProvider<typename BPlusStoreType::CacheType>*)>;
#endif

// BPlusStoreTraits template definition
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

// Helper function to get workload type enum from string
inline ycsbworkloadgenerator::WorkloadType get_ycsb_workload_type(const std::string& workload_str) {
    if (workload_str == "ycsb_a" || workload_str == "a") return ycsbworkloadgenerator::WorkloadType::WORKLOAD_A;
    if (workload_str == "ycsb_b" || workload_str == "b") return ycsbworkloadgenerator::WorkloadType::WORKLOAD_B;
    if (workload_str == "ycsb_c" || workload_str == "c") return ycsbworkloadgenerator::WorkloadType::WORKLOAD_C;
    if (workload_str == "ycsb_d" || workload_str == "d") return ycsbworkloadgenerator::WorkloadType::WORKLOAD_D;
    if (workload_str == "ycsb_e" || workload_str == "e") return ycsbworkloadgenerator::WorkloadType::WORKLOAD_E;
    if (workload_str == "ycsb_f" || workload_str == "f") return ycsbworkloadgenerator::WorkloadType::WORKLOAD_F;
    return ycsbworkloadgenerator::WorkloadType::WORKLOAD_A; // default
}

// Helper function to get workload name for display
inline std::string get_ycsb_workload_name(const std::string& workload_str) {
    if (workload_str == "ycsb_a" || workload_str == "a") return "workload_a";
    if (workload_str == "ycsb_b" || workload_str == "b") return "workload_b";
    if (workload_str == "ycsb_c" || workload_str == "c") return "workload_c";
    if (workload_str == "ycsb_d" || workload_str == "d") return "workload_d";
    if (workload_str == "ycsb_e" || workload_str == "e") return "workload_e";
    if (workload_str == "ycsb_f" || workload_str == "f") return "workload_f";
    return "workload_a"; // default
}

#ifdef __CONCURRENT__
// Worker function for multi-threaded YCSB operations
// Each thread processes a contiguous chunk of the pre-generated operations array
template<typename StoreType, typename KeyType, typename ValueType>
void ycsb_worker(
    StoreType& ptrTree,
    const std::vector<ycsbworkloadgenerator::YCSBOperation<KeyType>>& operations,
    size_t start_idx,
    size_t end_idx,
    int thread_id,
    std::barrier<>& sync_barrier
#ifdef __RECORD_LATENCY__
    ,BufferedLatencyLogger* thread_logger
#endif
#ifdef __CACHE_COUNTERS__
    ,StatsCollectorCallback<StoreType> stats_callback
#endif
) {
    // Wait for all threads to be ready before starting workload
    sync_barrier.arrive_and_wait();
    
    // Execute operations from the pre-generated array
    ValueType value;
    for (size_t i = start_idx; i < end_idx; i++) {
#ifdef __RECORD_LATENCY__
        auto start = std::chrono::steady_clock::now();
#endif
        
        const auto& op = operations[i];
        switch (op.operation) {
            case ycsbworkloadgenerator::OperationType::READ:
                ptrTree.search(op.key, value);
                break;
            case ycsbworkloadgenerator::OperationType::UPDATE:
                if constexpr (std::is_same_v<ValueType, KeyType>) {
                    ptrTree.insert(op.key, op.key);
                } else {
                    ValueType update_value = static_cast<ValueType>(op.key);
                    ptrTree.insert(op.key, update_value);
                }
                break;
            case ycsbworkloadgenerator::OperationType::INSERT:
                if constexpr (std::is_same_v<ValueType, KeyType>) {
                    ptrTree.insert(op.key, op.key);
                } else {
                    ValueType insert_value = static_cast<ValueType>(op.key);
                    ptrTree.insert(op.key, insert_value);
                }
                break;
            case ycsbworkloadgenerator::OperationType::DELETE:
                ptrTree.remove(op.key);
                break;
            case ycsbworkloadgenerator::OperationType::SCAN:
                ptrTree.search(op.key, value);
                break;
            case ycsbworkloadgenerator::OperationType::READ_MODIFY_WRITE:
                ptrTree.search(op.key, value);
                if constexpr (std::is_same_v<ValueType, KeyType>) {
                    ptrTree.insert(op.key, op.key);
                } else {
                    ValueType rmw_value = static_cast<ValueType>(op.key);
                    ptrTree.insert(op.key, rmw_value);
                }
                break;
        }
        
#ifdef __RECORD_LATENCY__
        auto end = std::chrono::steady_clock::now();
        if (thread_logger) {
            thread_logger->log_latency(i - start_idx, std::chrono::duration_cast<std::chrono::nanoseconds>(end - start));
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

// Main function to perform YCSB workload operations
template<typename StoreType, typename KeyType, typename ValueType>
void perform_ycsb_operations(
    StoreType& ptrTree,
    const std::vector<ycsbworkloadgenerator::YCSBOperation<KeyType>>& operations,
    size_t operation_count,
    int threads,
    std::chrono::nanoseconds& duration,
    const std::string& cache_type_name,
    const std::string& storage_type_name,
    const std::string& key_type_str,
    const std::string& value_type_str,
    size_t degree,
    int run_id,
    const std::string& workload_type,
    const std::string& output_dir
#ifdef __CACHE_COUNTERS__
    , auto& aggregated_stats
#endif
#ifdef __RECORD_LATENCY__
    , std::unique_ptr<BufferedLatencyLogger>& ycsb_logger
#endif
) {
#ifndef __CONCURRENT__
    // Single-threaded implementation
    std::cout << "Performing YCSB operations (single-threaded)..." << std::endl;
    auto begin = std::chrono::steady_clock::now();
    
    ValueType value;
    for (size_t i = 0; i < operation_count; i++) {
#ifdef __RECORD_LATENCY__
        auto start = std::chrono::steady_clock::now();
#endif
        
        const auto& op = operations[i];
        switch (op.operation) {
            case ycsbworkloadgenerator::OperationType::READ:
                ptrTree.search(op.key, value);
                break;
            case ycsbworkloadgenerator::OperationType::UPDATE:
                if constexpr (std::is_same_v<ValueType, KeyType>) {
                    ptrTree.insert(op.key, op.key);
                } else {
                    ValueType update_value = static_cast<ValueType>(op.key);
                    ptrTree.insert(op.key, update_value);
                }
                break;
            case ycsbworkloadgenerator::OperationType::INSERT:
                if constexpr (std::is_same_v<ValueType, KeyType>) {
                    ptrTree.insert(op.key, op.key);
                } else {
                    ValueType insert_value = static_cast<ValueType>(op.key);
                    ptrTree.insert(op.key, insert_value);
                }
                break;
            case ycsbworkloadgenerator::OperationType::DELETE:
                ptrTree.remove(op.key);
                break;
            case ycsbworkloadgenerator::OperationType::SCAN:
                ptrTree.search(op.key, value);
                break;
            case ycsbworkloadgenerator::OperationType::READ_MODIFY_WRITE:
                ptrTree.search(op.key, value);
                if constexpr (std::is_same_v<ValueType, KeyType>) {
                    ptrTree.insert(op.key, op.key);
                } else {
                    ValueType rmw_value = static_cast<ValueType>(op.key);
                    ptrTree.insert(op.key, rmw_value);
                }
                break;
        }
        
#ifdef __RECORD_LATENCY__
        auto end = std::chrono::steady_clock::now();
        if (ycsb_logger) {
            ycsb_logger->log_latency(i, std::chrono::duration_cast<std::chrono::nanoseconds>(end - start));
        }
#endif
    }
    
    auto end = std::chrono::steady_clock::now();
    duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
    
#else // __CONCURRENT__
    //if (threads == 1) {
        // never occur .. as already addressed above
    //} else 
    {
        // Multi-threaded implementation - split pre-generated operations array
        std::cout << "Performing YCSB operations with " << threads << " threads..." << std::endl;
        
#ifdef __CACHE_COUNTERS__
        // Stats collector callback
        std::vector<const CacheStatsProvider<typename StoreType::CacheType>*> thread_stats;
        std::mutex stats_mutex;
        
        auto stats_collector = [&thread_stats, &stats_mutex](const CacheStatsProvider<typename StoreType::CacheType>* provider) {
            std::lock_guard<std::mutex> lock(stats_mutex);
            thread_stats.push_back(provider);
        };
#endif

#ifdef __RECORD_LATENCY__
        // Create per-thread latency loggers
        std::vector<std::unique_ptr<BufferedLatencyLogger>> thread_loggers;
        std::vector<std::string> thread_filenames;
        
        if (ycsb_logger) {
            for (int t = 0; t < threads; t++) {
                std::string thread_filename = generate_ycsb_latency_filename(
                    workload_type, cache_type_name, storage_type_name,
                    key_type_str, value_type_str, degree, operation_count,
                    run_id, output_dir, t
                );
                thread_filenames.push_back(thread_filename);
                thread_loggers.push_back(std::make_unique<BufferedLatencyLogger>(thread_filename));
            }
        }
#endif
        
        // Create barrier for synchronizing thread start (threads + 1 for main thread)
        std::barrier sync_barrier(threads + 1);
        
        std::vector<std::thread> vtThreads;
        
        // Calculate operations per thread
        size_t ops_per_thread = operation_count / threads;
        
        // Launch worker threads - each processes a chunk of the operations array
        // Threads will block on the barrier before starting work
        for (int thread_id = 0; thread_id < threads; thread_id++) {
            size_t start_idx = thread_id * ops_per_thread;
            size_t end_idx = (thread_id == threads - 1) ? 
                operation_count : 
                (thread_id + 1) * ops_per_thread;
            
            vtThreads.emplace_back(ycsb_worker<StoreType, KeyType, ValueType>,
                std::ref(ptrTree),
                std::cref(operations),
                start_idx,
                end_idx,
                thread_id,
                std::ref(sync_barrier)
#ifdef __RECORD_LATENCY__
                ,thread_loggers[thread_id].get()
#endif
#ifdef __CACHE_COUNTERS__
                ,stats_collector
#endif
            );
        }
        
        // All threads are created and waiting at the barrier
        // Main thread arrives at barrier to release all threads simultaneously
        sync_barrier.arrive_and_wait();
        
        // Start timing immediately after releasing threads
        auto begin = std::chrono::steady_clock::now();
        
        // Wait for all threads to complete
        for (auto& thread : vtThreads) {
            thread.join();
        }
        
        // Stop timing after all threads complete
        auto end = std::chrono::steady_clock::now();
        duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);

#ifdef __RECORD_LATENCY__
        // Merge latency files
        if (ycsb_logger) {
            std::string main_filename = generate_ycsb_latency_filename(
                workload_type, cache_type_name, storage_type_name,
                key_type_str, value_type_str, degree, operation_count,
                run_id, output_dir
            );
            
            // Close thread loggers first
            thread_loggers.clear();
            
            merge_ycsb_latency_files(thread_filenames, main_filename);
        }
#endif

#ifdef __CACHE_COUNTERS__
        // Aggregate stats from all threads - stats are already collected via stats_collector callback
        // No additional aggregation needed here as the callback already added to aggregated_stats
#endif
    }
#endif // __CONCURRENT__
}

// Main benchmark function for YCSB workloads
template<typename StoreType, typename KeyType, typename ValueType>
void run_ycsb_cache_benchmark(
    StoreType& ptrTree,
    const std::string& cache_type_name,
    const std::string& storage_type_name,
    const std::string& key_type_str,
    const std::string& value_type_str,
    size_t degree,
    size_t records,
    const std::string& workload_type,
    CSVLogger& logger,
    int run_id,
    const std::string& output_dir = "",
    int threads = 1,
    const std::string& config_name = "",
    const std::string& cache_size_percentage = "",
    size_t cache_page_limit = 0
) {
    std::cout << "Running YCSB cache benchmark: " << cache_type_name << "/" << storage_type_name << std::endl;
    std::cout << "Degree: " << degree << ", Records: " << records << ", Workload: " << workload_type << std::endl;
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
    // Create latency logger
    std::unique_ptr<BufferedLatencyLogger> ycsb_logger;
    ycsb_logger = std::make_unique<BufferedLatencyLogger>(
        generate_ycsb_latency_filename(workload_type, cache_type_name, storage_type_name,
                                key_type_str, value_type_str, degree, records, run_id, output_dir));
#endif

    try {
        std::cout << "Tree initialized successfully" << std::endl;

        // Load YCSB workload operations
        std::vector<ycsbworkloadgenerator::YCSBOperation<KeyType>> operations;
        
        std::chrono::nanoseconds duration{0};
        
        try {
            // Generate YCSB workload filename
            std::string key_workload_type = get_workload_type_string(key_type_str);
            std::string workload_name = get_ycsb_workload_name(workload_type);
            std::string filename = "ycsb/" + key_workload_type + "_" + workload_name + "_" + 
                                 std::to_string(records) + "_ops_" + std::to_string(records) + ".dat";
            
            std::cout << "Loading YCSB workload from: " << filename << std::endl;
            
            // Load YCSB operations
            operations = ycsbworkloadgenerator::load_ycsb_operations<KeyType>(filename);
            
            std::cout << "Loaded " << operations.size() << " operations from YCSB workload file" << std::endl;
            
        } catch (const std::exception& e) {
            std::cerr << "Error: Could not load YCSB workload file (" << e.what() << ")" << std::endl;
            std::cerr << "Please ensure YCSB workload files are generated." << std::endl;
            return;
        }

        // First, populate the tree with initial data (for workloads that need it)
        // YCSB workloads typically start with a populated database
        std::cout << "Populating tree with initial data..." << std::endl;
        
        // Load initial data for population
        std::vector<KeyType> initial_data;
        try {
            std::string key_workload_type = get_workload_type_string(key_type_str);
            std::string init_filename = "data/" + key_workload_type + "_sequential_" + std::to_string(records) + ".dat";
            
            if constexpr (std::is_same_v<KeyType, uint64_t>) {
                initial_data = workloadgenerator::load_data_from_file<uint64_t>(init_filename);
            } else if constexpr (std::is_same_v<KeyType, CHAR16>) {
                std::vector<CHAR16> char16_data = workloadgenerator::load_data_from_file<CHAR16>(init_filename);
                initial_data.assign(char16_data.begin(), char16_data.end());
            } else {
                std::vector<uint64_t> uint64_data = workloadgenerator::load_data_from_file<uint64_t>(init_filename);
                initial_data.reserve(uint64_data.size());
                for (uint64_t val : uint64_data) {
                    initial_data.push_back(static_cast<KeyType>(val));
                }
            }
            
            std::cout << "Loaded " << initial_data.size() << " records for initial population" << std::endl;
            
        } catch (const std::exception& e) {
            std::cerr << "Warning: Could not load initial data file (" << e.what() << "), generating in-memory" << std::endl;
            
            initial_data.resize(records);
            if constexpr (std::is_same_v<KeyType, uint64_t> || std::is_same_v<KeyType, int>) {
                std::iota(initial_data.begin(), initial_data.end(), static_cast<KeyType>(1));
            } else if constexpr (std::is_same_v<KeyType, CHAR16>) {
                for (size_t i = 0; i < records; ++i) {
                    initial_data[i] = static_cast<CHAR16>(i + 1);
                }
            }
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(initial_data.begin(), initial_data.end(), g);
        }
        
        // Insert initial data (single-threaded for setup)
        for (size_t i = 0; i < initial_data.size(); i++) {
            if constexpr (std::is_same_v<ValueType, KeyType>) {
                ptrTree.insert(initial_data[i], initial_data[i]);
            } else {
                ValueType value = static_cast<ValueType>(initial_data[i]);
                ptrTree.insert(initial_data[i], value);
            }
        }
        
        std::cout << "Initial population complete. Starting YCSB workload execution..." << std::endl;

        // Execute YCSB workload operations
        perform_ycsb_operations<StoreType, KeyType, ValueType>(
            ptrTree, operations, operations.size(), threads, duration,
            cache_type_name, storage_type_name, key_type_str, value_type_str,
            degree, run_id, workload_type, output_dir
#ifdef __CACHE_COUNTERS__
            , aggregated_stats
#endif
#ifdef __RECORD_LATENCY__
            , ycsb_logger
#endif
        );

        // Calculate throughput
        double seconds = duration.count() / 1e9;
        double throughput = operations.size() / seconds;
        
        std::cout << "YCSB workload completed in " << seconds << " seconds" << std::endl;
        std::cout << "Throughput: " << throughput << " ops/sec" << std::endl;

#ifdef __CACHE_COUNTERS__
        // Get final cache statistics
        auto cache = ptrTree.getCache();
        auto cache_provider = cache->getCacheStatsProvider();
        
        uint64_t total_hits = 0;
        uint64_t total_misses = 0;
        uint64_t total_evictions = 0;
        uint64_t total_dirty_evictions = 0;
        
        if (cache_provider) {
            // Collect stats from main thread and add to the shared aggregated_stats
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
            
            // Calculate totals
            for (const auto& entry : aggregated_stats.hits) total_hits += entry.second;
            for (const auto& entry : aggregated_stats.misses) total_misses += entry.second;
            for (const auto& entry : aggregated_stats.evictions) total_evictions += entry.second;
            for (const auto& entry : aggregated_stats.dirty_evictions) total_dirty_evictions += entry.second;
        }
        
        double hit_rate = (total_hits + total_misses > 0) ? 
                         (static_cast<double>(total_hits) / (total_hits + total_misses)) * 100.0 : 0.0;
        
        std::cout << "Cache Statistics:" << std::endl;
        std::cout << "  Hits: " << total_hits << std::endl;
        std::cout << "  Misses: " << total_misses << std::endl;
        std::cout << "  Hit Rate: " << hit_rate << "%" << std::endl;
        std::cout << "  Evictions: " << total_evictions << std::endl;
        std::cout << "  Dirty Evictions: " << total_dirty_evictions << std::endl;
        
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
        
        // Log to CSV with correct parameter order
        logger.log_result(
            "BplusTreeSOA",         // tree_type
            key_type_str,           // key_type
            value_type_str,         // value_type
            cache_type_name,        // policy_name
            storage_type_name,      // storage_type
            config_name,            // config_name
            operations.size(),      // record_count
            degree,                 // degree
            workload_type,          // operation
            duration.count()/1000,       // time_us (nanoseconds)
            throughput,             // throughput_ops_sec
            run_id,                 // test_run_id
            total_hits,             // cache_hits
            total_misses,           // cache_misses
            total_evictions,        // cache_evictions
            total_dirty_evictions,  // cache_dirty_evictions
            hit_rate,               // cache_hit_rate
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
        
        // Log without cache stats with correct parameter order
        logger.log_result(
            "BplusTreeSOA",         // tree_type
            key_type_str,           // key_type
            value_type_str,         // value_type
            cache_type_name,        // policy_name
            storage_type_name,      // storage_type
            config_name,            // config_name
            operations.size(),      // record_count
            degree,                 // degree
            workload_type,          // operation
            duration.count()/1000,       // time_us (nanoseconds)
            throughput,             // throughput_ops_sec
            run_id,                 // test_run_id
            0,                      // cache_hits
            0,                      // cache_misses
            0,                      // cache_evictions
            0,                      // cache_dirty_evictions
            0.0,                    // cache_hit_rate
            cache_size_decimal,     // cache_size (converted to decimal)
            cache_page_limit,       // cache_page_limit
            threads                 // thread_count
        );
#endif

    } catch (const std::exception& e) {
        std::cerr << "Exception during YCSB benchmark: " << e.what() << std::endl;
    }
}

// Test function for YCSB workloads with cache combinations
void test_ycsb_cache_combinations(
    const std::vector<std::string>& cache_types,
    const std::vector<std::string>& storage_types,
    const std::vector<int>& cache_sizes,
    const std::vector<size_t>& degrees,
    const std::vector<size_t>& record_counts,
    const std::vector<std::string>& workload_types,
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
    std::string csv_filename = CSVLogger::generate_filename("benchmark_ycsb_cache");
    CSVLogger logger(csv_filename, output_dir);
    logger.write_header();
    std::string full_path = output_dir.empty() ? csv_filename : output_dir + "/" + csv_filename;
    std::cout << "Logging YCSB results to: " << full_path << std::endl;

    for (const std::string& cache_type : cache_types) {
        for (const std::string& storage_type : storage_types) {
            for (int cache_size : cache_sizes) {
                for (size_t degree : degrees) {
                    for (size_t records : record_counts) {
                        for (const std::string& workload_type : workload_types) {
                            std::cout << "\n--- Testing YCSB " << cache_type << "/" << storage_type 
                                     << " (cache_size=" << cache_size << ", degree=" << degree 
                                     << ", records=" << records << ", workload=" << workload_type << ") ---" << std::endl;
                            
                            for (int run = 1; run <= num_runs; run++) {
                                std::cout << "Run " << run << "/" << num_runs << "... ";
                                
                                // Support all cache and storage combinations (using uint64_t/uint64_t)
                                if (cache_type == "LRU" && storage_type == "VolatileStorage") {
                                    using T = BPlusStoreTraits<uint64_t, uint64_t, TYPE_UID::DATA_NODE_UINT64_UINT64, TYPE_UID::INDEX_NODE_UINT64_UINT64, ObjectFatUID, LRUCacheObject, LRUCache, VolatileStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_ycsb_cache_benchmark<typename T::StoreType, uint64_t, uint64_t>(ptrTree, cache_type, storage_type, "uint64_t", "uint64_t", degree, records, workload_type, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "LRU" && storage_type == "FileStorage") {
                                    using T = BPlusStoreTraits<uint64_t, uint64_t, TYPE_UID::DATA_NODE_UINT64_UINT64, TYPE_UID::INDEX_NODE_UINT64_UINT64, ObjectFatUID, LRUCacheObject, LRUCache, FileStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, FILE_STORAGE_PATH);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_ycsb_cache_benchmark<typename T::StoreType, uint64_t, uint64_t>(ptrTree, cache_type, storage_type, "uint64_t", "uint64_t", degree, records, workload_type, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "LRU" && storage_type == "PMemStorage") {
                                    using T = BPlusStoreTraits<uint64_t, uint64_t, TYPE_UID::DATA_NODE_UINT64_UINT64, TYPE_UID::INDEX_NODE_UINT64_UINT64, ObjectFatUID, LRUCacheObject, LRUCache, PMemStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, PMEM_STORAGE_PATH);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_ycsb_cache_benchmark<typename T::StoreType, uint64_t, uint64_t>(ptrTree, cache_type, storage_type, "uint64_t", "uint64_t", degree, records, workload_type, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "A2Q" && storage_type == "VolatileStorage") {
                                    using T = BPlusStoreTraits<uint64_t, uint64_t, TYPE_UID::DATA_NODE_UINT64_UINT64, TYPE_UID::INDEX_NODE_UINT64_UINT64, ObjectFatUID, A2QCacheObject, A2QCache, VolatileStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_ycsb_cache_benchmark<typename T::StoreType, uint64_t, uint64_t>(ptrTree, cache_type, storage_type, "uint64_t", "uint64_t", degree, records, workload_type, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "A2Q" && storage_type == "FileStorage") {
                                    using T = BPlusStoreTraits<uint64_t, uint64_t, TYPE_UID::DATA_NODE_UINT64_UINT64, TYPE_UID::INDEX_NODE_UINT64_UINT64, ObjectFatUID, A2QCacheObject, A2QCache, FileStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, FILE_STORAGE_PATH);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_ycsb_cache_benchmark<typename T::StoreType, uint64_t, uint64_t>(ptrTree, cache_type, storage_type, "uint64_t", "uint64_t", degree, records, workload_type, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "A2Q" && storage_type == "PMemStorage") {
                                    using T = BPlusStoreTraits<uint64_t, uint64_t, TYPE_UID::DATA_NODE_UINT64_UINT64, TYPE_UID::INDEX_NODE_UINT64_UINT64, ObjectFatUID, A2QCacheObject, A2QCache, PMemStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, PMEM_STORAGE_PATH);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_ycsb_cache_benchmark<typename T::StoreType, uint64_t, uint64_t>(ptrTree, cache_type, storage_type, "uint64_t", "uint64_t", degree, records, workload_type, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "CLOCK" && storage_type == "VolatileStorage") {
                                    using T = BPlusStoreTraits<uint64_t, uint64_t, TYPE_UID::DATA_NODE_UINT64_UINT64, TYPE_UID::INDEX_NODE_UINT64_UINT64, ObjectFatUID, CLOCKCacheObject, CLOCKCache, VolatileStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_ycsb_cache_benchmark<typename T::StoreType, uint64_t, uint64_t>(ptrTree, cache_type, storage_type, "uint64_t", "uint64_t", degree, records, workload_type, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "CLOCK" && storage_type == "FileStorage") {
                                    using T = BPlusStoreTraits<uint64_t, uint64_t, TYPE_UID::DATA_NODE_UINT64_UINT64, TYPE_UID::INDEX_NODE_UINT64_UINT64, ObjectFatUID, CLOCKCacheObject, CLOCKCache, FileStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, FILE_STORAGE_PATH);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_ycsb_cache_benchmark<typename T::StoreType, uint64_t, uint64_t>(ptrTree, cache_type, storage_type, "uint64_t", "uint64_t", degree, records, workload_type, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else if (cache_type == "CLOCK" && storage_type == "PMemStorage") {
                                    using T = BPlusStoreTraits<uint64_t, uint64_t, TYPE_UID::DATA_NODE_UINT64_UINT64, TYPE_UID::INDEX_NODE_UINT64_UINT64, ObjectFatUID, CLOCKCacheObject, CLOCKCache, PMemStorage>;
                                    typename T::StoreType ptrTree(degree, cache_size, page_size, memory_size, PMEM_STORAGE_PATH);
                                    ptrTree.template init<typename T::DataNodeType>();
                                    run_ycsb_cache_benchmark<typename T::StoreType, uint64_t, uint64_t>(ptrTree, cache_type, storage_type, "uint64_t", "uint64_t", degree, records, workload_type, logger, run, output_dir, threads, config_name, cache_size_percentage, cache_page_limit);
                                } else {
                                    std::cout << "Unsupported cache/storage combination: " << cache_type << "/" << storage_type << std::endl;
                                }

                                // Brief sleep to let system settle between runs
                                std::cout << "sleep for 2 seconds.................................." << std::endl;
                                std::this_thread::sleep_for(std::chrono::seconds(2));
                            }
                            std::cout << "Completed all runs for this configuration." << std::endl;
                        }
                    }
                }
            }
        }
    }
    
    std::cout << "\nYCSB cache benchmark completed!" << std::endl;
    std::cout << "Results saved to: " << csv_filename << std::endl;
}

// Function that uses shell script parameters for YCSB workloads
void test_ycsb_with_shell_parameters(
    const std::string& cache_type, 
    int num_runs = 1, 
    const std::string& output_dir = "",
    const std::string& storage_type = "VolatileStorage",
    int cache_size = 100,
    int page_size = 2048,
    long long memory_size = 1073741824LL,
    const std::vector<std::string>& workload_types = {"ycsb_a", "ycsb_b", "ycsb_c", "ycsb_d", "ycsb_e", "ycsb_f"},
    const std::vector<size_t>& degrees = {64, 128, 256},
    const std::vector<size_t>& record_counts = {100000, 500000, 1000000},
    int threads = 1,
    const std::string& config_name = "",
    const std::string& cache_size_percentage = "",
    size_t cache_page_limit = 0
) {
    std::cout << "\n=== Testing B+ Tree with YCSB Workloads ===" << std::endl;
    std::cout << "Cache Type: " << cache_type << std::endl;
    std::cout << "Storage Type: " << storage_type << std::endl;
    std::cout << "Cache Size: " << cache_size << std::endl;
    std::cout << "Page Size: " << page_size << std::endl;
    std::cout << "Memory Size: " << memory_size << std::endl;
    std::cout << "Number of runs per configuration: " << num_runs << std::endl;
    std::cout << "Number of threads: " << threads << std::endl;

    // Use the provided parameters
    std::vector<std::string> cache_types = {cache_type};
    std::vector<std::string> storage_types = {storage_type};
    std::vector<int> cache_sizes = {cache_size};
    
    test_ycsb_cache_combinations(
        cache_types, storage_types, cache_sizes, degrees, record_counts, workload_types,
        page_size, memory_size, num_runs, output_dir, threads, config_name, cache_size_percentage, cache_page_limit
    );
}

} // namespace bm_tree_with_cache_ycsb

#endif // __TREE_WITH_CACHE__