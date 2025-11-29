#pragma once

#ifdef __TREE_WITH_CACHE__

#include "bm_tree_with_cache_ycsb.hpp"
#include "BiStorage.hpp"
#include <algorithm>
#include <cctype>
#include <thread>
#include <chrono>

namespace bm_tree_with_cache_bistorage_ycsb {

// Define all 9 BiStorage template aliases at namespace scope
template<typename Traits> using BiStorage_Volatile_Volatile = BiStorage<Traits, VolatileStorage, VolatileStorage>;
template<typename Traits> using BiStorage_Volatile_PMem = BiStorage<Traits, VolatileStorage, PMemStorage>;
template<typename Traits> using BiStorage_Volatile_File = BiStorage<Traits, VolatileStorage, FileStorage>;
template<typename Traits> using BiStorage_PMem_Volatile = BiStorage<Traits, PMemStorage, VolatileStorage>;
template<typename Traits> using BiStorage_PMem_PMem = BiStorage<Traits, PMemStorage, PMemStorage>;
template<typename Traits> using BiStorage_PMem_File = BiStorage<Traits, PMemStorage, FileStorage>;
template<typename Traits> using BiStorage_File_Volatile = BiStorage<Traits, FileStorage, VolatileStorage>;
template<typename Traits> using BiStorage_File_PMem = BiStorage<Traits, FileStorage, PMemStorage>;
template<typename Traits> using BiStorage_File_File = BiStorage<Traits, FileStorage, FileStorage>;

/**
 * @brief Macro to instantiate and run BiStorage YCSB benchmark
 * 
 * This eliminates code duplication across different cache and storage combinations.
 * Storage paths are determined from common.h based on primary and secondary storage types.
 * 
 * Note: This macro runs the benchmark for a single run iteration. The caller should
 * wrap this in a loop to execute multiple runs.
 */
#define INSTANTIATE_AND_RUN_BISTORAGE_YCSB(CACHE_OBJ, CACHE_TYPE, STORAGE_TYPE, PRIMARY_PATH, SECONDARY_PATH) \
    do { \
        using T = bm_tree_with_cache_ycsb::BPlusStoreTraits<uint64_t, uint64_t, TYPE_UID::DATA_NODE_INT_INT, \
                                   TYPE_UID::INDEX_NODE_INT_INT, ObjectFatUID, \
                                   CACHE_OBJ, CACHE_TYPE, STORAGE_TYPE>; \
        \
        typename T::StoreType ptrTree( \
            degree, \
            cache_size, \
            primary_read_cost, \
            primary_write_cost, \
            secondary_read_cost, \
            secondary_write_cost, \
            page_size, \
            memory_size, \
            page_size, \
            memory_size, \
            PRIMARY_PATH, \
            SECONDARY_PATH \
        ); \
        \
        ptrTree.template init<typename T::DataNodeType>(); \
        \
        bm_tree_with_cache_ycsb::run_ycsb_cache_benchmark<typename T::StoreType, uint64_t, uint64_t>( \
            ptrTree, cache_type, storage_name, "uint64_t", "uint64_t", degree, records, \
            workload_type, logger, run, output_dir, threads, config_name, \
            cache_size_percentage, cache_page_limit); \
    } while(0)

/**
 * @brief Main function to test BiStorage with YCSB workloads
 * 
 * This function handles all 9 combinations of primary/secondary storage:
 * - Primary: VolatileStorage, PMemStorage, FileStorage
 * - Secondary: VolatileStorage, PMemStorage, FileStorage
 * 
 * And all 3 cache types:
 * - LRU, A2Q, CLOCK
 * 
 * Uses YCSB workload operations instead of synthetic operations.
 */
void test_bistorage_ycsb_with_shell_parameters(
    const std::string& cache_type,
    const std::string& primary_storage_type,
    const std::string& secondary_storage_type,
    uint64_t primary_read_cost,
    uint64_t primary_write_cost,
    uint64_t secondary_read_cost,
    uint64_t secondary_write_cost,
    int cache_size,
    int page_size,
    long long memory_size,
    const std::string& tree_type,
    const std::string& workload_type,
    size_t degree,
    size_t records,
    int runs,
    const std::string& output_dir,
    int threads,
    const std::string& config_name,
    const std::string& cache_size_percentage,
    int cache_page_limit)
{
    std::cout << "\n=== Testing B+ Tree with " << cache_type << " Cache and BiStorage (YCSB) ===" << std::endl;
    std::cout << "Primary Storage: " << primary_storage_type << std::endl;
    std::cout << "Secondary Storage: " << secondary_storage_type << std::endl;
    std::cout << "Primary Costs (R/W): " << primary_read_cost << "/" << primary_write_cost << " ns" << std::endl;
    std::cout << "Secondary Costs (R/W): " << secondary_read_cost << "/" << secondary_write_cost << " ns" << std::endl;
    std::cout << "Cache Size: " << cache_size << " (" << cache_size_percentage << ")" << std::endl;
    std::cout << "Workload: " << workload_type << ", Degree: " << degree << ", Records: " << records << std::endl;
    std::cout << "Threads: " << threads << ", Runs: " << runs << std::endl;
    
    // Create storage name for logging - format: bistorage_primary_secondary (all lowercase)
    std::string primary_lower = primary_storage_type;
    std::string secondary_lower = secondary_storage_type;
    std::transform(primary_lower.begin(), primary_lower.end(), primary_lower.begin(), ::tolower);
    std::transform(secondary_lower.begin(), secondary_lower.end(), secondary_lower.begin(), ::tolower);
    std::string storage_name = "bistorage_" + primary_lower + "_" + secondary_lower;
    
    // Create CSV logger with output directory (same pattern as regular cache benchmarks)
    std::string csv_filename = CSVLogger::generate_filename("benchmark_bistorage_ycsb");
    CSVLogger logger(csv_filename, output_dir);
    logger.write_header();
    std::string full_path = output_dir.empty() ? csv_filename : output_dir + "/" + csv_filename;
    std::cout << "Logging BiStorage YCSB results to: " << full_path << std::endl;
    
    // Execute benchmark for the specified number of runs
    // Determine BiStorage type and cache type, then instantiate
    // Paths are selected from common.h based on primary and secondary storage types
    for (int run = 1; run <= runs; run++) {
        std::cout << "\nRun " << run << "/" << runs << "... ";
        
        if (primary_storage_type == "VolatileStorage" && secondary_storage_type == "VolatileStorage") {
            if (cache_type == "LRU") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(LRUCacheObject, LRUCache, BiStorage_Volatile_Volatile, "", "");
            } else if (cache_type == "A2Q") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(A2QCacheObject, A2QCache, BiStorage_Volatile_Volatile, "", "");
            } else if (cache_type == "CLOCK") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(CLOCKCacheObject, CLOCKCache, BiStorage_Volatile_Volatile, "", "");
            } else {
                std::cerr << "Unsupported cache type: " << cache_type << std::endl;
            }
        } else if (primary_storage_type == "VolatileStorage" && secondary_storage_type == "PMemStorage") {
            if (cache_type == "LRU") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(LRUCacheObject, LRUCache, BiStorage_Volatile_PMem, "", PMEM_STORAGE_PATH);
            } else if (cache_type == "A2Q") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(A2QCacheObject, A2QCache, BiStorage_Volatile_PMem, "", PMEM_STORAGE_PATH);
            } else if (cache_type == "CLOCK") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(CLOCKCacheObject, CLOCKCache, BiStorage_Volatile_PMem, "", PMEM_STORAGE_PATH);
            } else {
                std::cerr << "Unsupported cache type: " << cache_type << std::endl;
            }
        } else if (primary_storage_type == "VolatileStorage" && secondary_storage_type == "FileStorage") {
            if (cache_type == "LRU") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(LRUCacheObject, LRUCache, BiStorage_Volatile_File, "", FILE_STORAGE_PATH);
            } else if (cache_type == "A2Q") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(A2QCacheObject, A2QCache, BiStorage_Volatile_File, "", FILE_STORAGE_PATH);
            } else if (cache_type == "CLOCK") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(CLOCKCacheObject, CLOCKCache, BiStorage_Volatile_File, "", FILE_STORAGE_PATH);
            } else {
                std::cerr << "Unsupported cache type: " << cache_type << std::endl;
            }
        } else if (primary_storage_type == "PMemStorage" && secondary_storage_type == "VolatileStorage") {
            if (cache_type == "LRU") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(LRUCacheObject, LRUCache, BiStorage_PMem_Volatile, PMEM_STORAGE_PATH, "");
            } else if (cache_type == "A2Q") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(A2QCacheObject, A2QCache, BiStorage_PMem_Volatile, PMEM_STORAGE_PATH, "");
            } else if (cache_type == "CLOCK") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(CLOCKCacheObject, CLOCKCache, BiStorage_PMem_Volatile, PMEM_STORAGE_PATH, "");
            } else {
                std::cerr << "Unsupported cache type: " << cache_type << std::endl;
            }
        } else if (primary_storage_type == "PMemStorage" && secondary_storage_type == "PMemStorage") {
            if (cache_type == "LRU") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(LRUCacheObject, LRUCache, BiStorage_PMem_PMem, PMEM_STORAGE_PATH, PMEM_STORAGE_PATH);
            } else if (cache_type == "A2Q") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(A2QCacheObject, A2QCache, BiStorage_PMem_PMem, PMEM_STORAGE_PATH, PMEM_STORAGE_PATH);
            } else if (cache_type == "CLOCK") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(CLOCKCacheObject, CLOCKCache, BiStorage_PMem_PMem, PMEM_STORAGE_PATH, PMEM_STORAGE_PATH);
            } else {
                std::cerr << "Unsupported cache type: " << cache_type << std::endl;
            }
        } else if (primary_storage_type == "PMemStorage" && secondary_storage_type == "FileStorage") {
            if (cache_type == "LRU") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(LRUCacheObject, LRUCache, BiStorage_PMem_File, PMEM_STORAGE_PATH, FILE_STORAGE_PATH);
            } else if (cache_type == "A2Q") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(A2QCacheObject, A2QCache, BiStorage_PMem_File, PMEM_STORAGE_PATH, FILE_STORAGE_PATH);
            } else if (cache_type == "CLOCK") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(CLOCKCacheObject, CLOCKCache, BiStorage_PMem_File, PMEM_STORAGE_PATH, FILE_STORAGE_PATH);
            } else {
                std::cerr << "Unsupported cache type: " << cache_type << std::endl;
            }
        } else if (primary_storage_type == "FileStorage" && secondary_storage_type == "VolatileStorage") {
            if (cache_type == "LRU") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(LRUCacheObject, LRUCache, BiStorage_File_Volatile, FILE_STORAGE_PATH, "");
            } else if (cache_type == "A2Q") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(A2QCacheObject, A2QCache, BiStorage_File_Volatile, FILE_STORAGE_PATH, "");
            } else if (cache_type == "CLOCK") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(CLOCKCacheObject, CLOCKCache, BiStorage_File_Volatile, FILE_STORAGE_PATH, "");
            } else {
                std::cerr << "Unsupported cache type: " << cache_type << std::endl;
            }
        } else if (primary_storage_type == "FileStorage" && secondary_storage_type == "PMemStorage") {
            if (cache_type == "LRU") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(LRUCacheObject, LRUCache, BiStorage_File_PMem, FILE_STORAGE_PATH, PMEM_STORAGE_PATH);
            } else if (cache_type == "A2Q") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(A2QCacheObject, A2QCache, BiStorage_File_PMem, FILE_STORAGE_PATH, PMEM_STORAGE_PATH);
            } else if (cache_type == "CLOCK") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(CLOCKCacheObject, CLOCKCache, BiStorage_File_PMem, FILE_STORAGE_PATH, PMEM_STORAGE_PATH);
            } else {
                std::cerr << "Unsupported cache type: " << cache_type << std::endl;
            }
        } else if (primary_storage_type == "FileStorage" && secondary_storage_type == "FileStorage") {
            if (cache_type == "LRU") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(LRUCacheObject, LRUCache, BiStorage_File_File, FILE_STORAGE_PATH, FILE_STORAGE_PATH);
            } else if (cache_type == "A2Q") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(A2QCacheObject, A2QCache, BiStorage_File_File, FILE_STORAGE_PATH, FILE_STORAGE_PATH);
            } else if (cache_type == "CLOCK") {
                INSTANTIATE_AND_RUN_BISTORAGE_YCSB(CLOCKCacheObject, CLOCKCache, BiStorage_File_File, FILE_STORAGE_PATH, FILE_STORAGE_PATH);
            } else {
                std::cerr << "Unsupported cache type: " << cache_type << std::endl;
            }
        } else {
            std::cerr << "Unsupported BiStorage combination: " 
                      << primary_storage_type << " + " << secondary_storage_type << std::endl;
        }
        
        // Brief sleep to let system settle between runs
        if (run < runs) {
            std::cout << "sleep for 2 seconds.................................." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }
    
    std::cout << "\nCompleted all " << runs << " runs for this configuration." << std::endl;
    
    std::cout << "\nBiStorage YCSB benchmark completed!" << std::endl;
}

} // namespace bm_tree_with_cache_bistorage_ycsb

#endif // __TREE_WITH_CACHE__