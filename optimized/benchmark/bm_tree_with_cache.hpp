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

// Include cache headers directly (following sandbox.cpp pattern)
#include "LRUCache.hpp"
#include "LRUCacheObject.hpp"
#include "CLOCKCache.hpp"
#include "CLOCKCacheObject.hpp"
#include "A2QCache.hpp"
#include "A2QCacheObject.hpp"

// Include storage headers
#include "VolatileStorage.hpp"
#include "FileStorage.hpp"
#include "PMemStorage.hpp"

// Include core tree components
#include "ObjectFatUID.h"
#include "TypeUID.h"
#include "BPlusStore.hpp"
#include "DataNode.hpp"
#include "IndexNode.hpp"

// Include WAL headers
#include "FileWAL.hpp"
#include "PMemWAL.hpp"

// Include common headers
#include "common.h"
#include "csv_logger.hpp"

namespace bm_tree_with_cache {

// Workload generation functions (to avoid multiple definition issues)
enum class DistributionType {
    Random,
    Sequential,
    Uniform,
    Zipfian
};

template<typename T>
void generate_data(size_t count, DistributionType distribution, std::vector<T>& data) {
    data.clear();
    data.reserve(count);
    
    if constexpr (std::is_same_v<T, uint64_t>) {
        for (size_t i = 0; i < count; ++i) {
            data.push_back(static_cast<uint64_t>(i + 1));
        }
        if (distribution == DistributionType::Random) {
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(data.begin(), data.end(), g);
        }
    } else if constexpr (std::is_same_v<T, CHAR16>) {
        for (size_t i = 0; i < count; ++i) {
            CHAR16 item;
            std::memset(item.data, 0, sizeof(item.data));
            uint64_t value = i + 1;
            std::memcpy(item.data, &value, std::min(sizeof(value), sizeof(item.data)));
            data.push_back(item);
        }
        if (distribution == DistributionType::Random) {
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(data.begin(), data.end(), g);
        }
    }
}

// Copy BPlusStoreTraits template definition from sandbox.cpp (lines 215-260)
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

// Template function to perform benchmark operations
template<typename TreeTraits, typename KeyType, typename ValueType>
void perform_benchmark_operation(
    const std::vector<KeyType>& data,
    const std::string& operation,
    size_t degree,
    int cache_size,
    int page_size,
    long long memory_size,
    CSVLogger& logger,
    const std::string& tree_type,
    const std::string& cache_type,
    const std::string& storage_type,
    size_t test_run_id
) {
    using MyStore = typename TreeTraits::StoreType;
    
    // Create tree instance with parameterized constructor (degree, cache_size, page_size, memory_size)
    MyStore ptrTree(degree, cache_size, page_size, memory_size);
    
    // Initialize tree using pattern from sandbox.cpp
    ptrTree.template init<typename TreeTraits::DataNodeType>();
    
    std::cout << "    Testing " << tree_type << " with " << cache_type << "/" << storage_type 
              << " (Cache Size: " << cache_size << ", Page Size: " << page_size 
              << ", Memory Size: " << memory_size << ")" << std::endl;
    std::cout << "    Operation: " << operation << ", Degree: " << degree 
              << ", Records: " << data.size() << std::endl;
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    if (operation == "insert") {
        // Insert all data and measure time
        for (const auto& key : data) {
            ValueType value;
            if constexpr (std::is_same_v<ValueType, uint64_t>) {
                value = key;
            } else if constexpr (std::is_same_v<ValueType, CHAR16>) {
                std::memcpy(value.data, &key, std::min(sizeof(key), sizeof(value.data)));
            }
            ptrTree.insert(key, value);
        }
    } else if (operation == "search_random" || operation == "search_sequential" || 
               operation == "search_uniform" || operation == "search_zipfian") {
        // Insert data first, then search and measure search time
        for (const auto& key : data) {
            ValueType value;
            if constexpr (std::is_same_v<ValueType, uint64_t>) {
                value = key;
            } else if constexpr (std::is_same_v<ValueType, CHAR16>) {
                std::memcpy(value.data, &key, std::min(sizeof(key), sizeof(value.data)));
            }
            ptrTree.insert(key, value);
        }
        
        // Reset timer for search operation
        start_time = std::chrono::high_resolution_clock::now();
        
        // Perform searches
        for (const auto& key : data) {
            ValueType result;
            ptrTree.search(key, result);
        }
    } else if (operation == "delete") {
        // Insert data first, then delete and measure delete time
        for (const auto& key : data) {
            ValueType value;
            if constexpr (std::is_same_v<ValueType, uint64_t>) {
                value = key;
            } else if constexpr (std::is_same_v<ValueType, CHAR16>) {
                std::memcpy(value.data, &key, std::min(sizeof(key), sizeof(value.data)));
            }
            ptrTree.insert(key, value);
        }
        
        // Reset timer for delete operation
        start_time = std::chrono::high_resolution_clock::now();
        
        // Perform deletions
        for (const auto& key : data) {
            ptrTree.remove(key);
        }
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto operation_time = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    
    // Calculate throughput
    double throughput = (static_cast<double>(data.size()) / operation_time) * 1000000.0;
    
    std::cout << "    " << operation << " [" << data.size() << " records]: " << operation_time << " us" << std::endl;
    std::cout << "    Throughput: " << (int)throughput << " ops/sec" << std::endl;
    
    // Get type names for logging
    std::string key_type_name = typeid(KeyType).name();
    std::string value_type_name = typeid(ValueType).name();
    if (key_type_name.find("uint64") != std::string::npos) key_type_name = "uint64_t";
    if (key_type_name.find("CHAR16") != std::string::npos) key_type_name = "char16";
    if (value_type_name.find("uint64") != std::string::npos) value_type_name = "uint64_t";
    if (value_type_name.find("CHAR16") != std::string::npos) value_type_name = "char16";
    
    // Log results
    logger.log_result(tree_type, key_type_name, value_type_name, "cache_benchmark", data.size(), degree, 
                     operation, operation_time, throughput, test_run_id);
}

// Single function that handles all cache and storage type dispatch
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
    long long memory_size = 1ULL * 1024 * 1024 * 1024
) {
    std::cout << "\n=== Cache Benchmark Single Configuration ===" << std::endl;
    std::cout << "Tree: " << tree_type << std::endl;
    std::cout << "Cache Type: " << cache_type << std::endl;
    std::cout << "Storage Type: " << storage_type << std::endl;
    std::cout << "Cache Size: " << cache_size << std::endl;
    std::cout << "Page Size: " << page_size << std::endl;
    std::cout << "Memory Size: " << memory_size << std::endl;
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
            generate_data(records, DistributionType::Random, uint64_data);
        } else if (operation == "search_sequential") {
            generate_data(records, DistributionType::Sequential, uint64_data);
        } else if (operation == "search_uniform") {
            generate_data(records, DistributionType::Uniform, uint64_data);
        } else if (operation == "search_zipfian") {
            generate_data(records, DistributionType::Zipfian, uint64_data);
        } else {
            // For insert and delete operations, use random data
            generate_data(records, DistributionType::Random, uint64_data);
        }
    } else if (key_type == "char16") {
        if (operation == "search_random") {
            generate_data(records, DistributionType::Random, char16_data);
        } else if (operation == "search_sequential") {
            generate_data(records, DistributionType::Sequential, char16_data);
        } else if (operation == "search_uniform") {
            generate_data(records, DistributionType::Uniform, char16_data);
        } else if (operation == "search_zipfian") {
            generate_data(records, DistributionType::Zipfian, char16_data);
        } else {
            // For insert and delete operations, use random data
            generate_data(records, DistributionType::Random, char16_data);
        }
    }
    
    // Dispatch based on cache type and storage type
    for (size_t run = 1; run <= runs; run++) {
        std::cout << "\n--- Run " << run << "/" << runs << " ---" << std::endl;
        
        if (cache_type == "LRU" && storage_type == "VolatileStorage") {
            // Use LRUCache, LRUCacheObject, VolatileStorage templates
            if (key_type == "uint64_t" && value_type == "uint64_t") {
                using T = BPlusStoreTraits<
                    uint64_t,             // Key type
                    uint64_t,             // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    LRUCacheObject,       // Cache object type
                    LRUCache,             // Cache type
                    VolatileStorage       // Storage type
                >;
                perform_benchmark_operation<T, uint64_t, uint64_t>(
                    uint64_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            } else if (key_type == "uint64_t" && value_type == "char16") {
                using T = BPlusStoreTraits<
                    uint64_t,             // Key type
                    CHAR16,               // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    LRUCacheObject,       // Cache object type
                    LRUCache,             // Cache type
                    VolatileStorage       // Storage type
                >;
                perform_benchmark_operation<T, uint64_t, CHAR16>(
                    uint64_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            } else if (key_type == "char16" && value_type == "char16") {
                using T = BPlusStoreTraits<
                    CHAR16,               // Key type
                    CHAR16,               // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    LRUCacheObject,       // Cache object type
                    LRUCache,             // Cache type
                    VolatileStorage       // Storage type
                >;
                perform_benchmark_operation<T, CHAR16, CHAR16>(
                    char16_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            }
        } else if (cache_type == "LRU" && storage_type == "FileStorage") {
            // Use LRUCache, LRUCacheObject, FileStorage templates
            if (key_type == "uint64_t" && value_type == "uint64_t") {
                using T = BPlusStoreTraits<
                    uint64_t,             // Key type
                    uint64_t,             // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    LRUCacheObject,       // Cache object type
                    LRUCache,             // Cache type
                    FileStorage           // Storage type
                >;
                perform_benchmark_operation<T, uint64_t, uint64_t>(
                    uint64_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            } else if (key_type == "char16" && value_type == "char16") {
                using T = BPlusStoreTraits<
                    CHAR16,               // Key type
                    CHAR16,               // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    LRUCacheObject,       // Cache object type
                    LRUCache,             // Cache type
                    FileStorage           // Storage type
                >;
                perform_benchmark_operation<T, CHAR16, CHAR16>(
                    char16_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            }
        } else if (cache_type == "LRU" && storage_type == "PMemStorage") {
            // Use LRUCache, LRUCacheObject, PMemStorage templates
            if (key_type == "uint64_t" && value_type == "uint64_t") {
                using T = BPlusStoreTraits<
                    uint64_t,             // Key type
                    uint64_t,             // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    LRUCacheObject,       // Cache object type
                    LRUCache,             // Cache type
                    PMemStorage           // Storage type
                >;
                perform_benchmark_operation<T, uint64_t, uint64_t>(
                    uint64_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            } else if (key_type == "char16" && value_type == "char16") {
                using T = BPlusStoreTraits<
                    CHAR16,               // Key type
                    CHAR16,               // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    LRUCacheObject,       // Cache object type
                    LRUCache,             // Cache type
                    PMemStorage           // Storage type
                >;
                perform_benchmark_operation<T, CHAR16, CHAR16>(
                    char16_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            }
        } else if (cache_type == "CLOCK" && storage_type == "VolatileStorage") {
            // Use CLOCKCache, CLOCKCacheObject, VolatileStorage templates
            if (key_type == "uint64_t" && value_type == "uint64_t") {
                using T = BPlusStoreTraits<
                    uint64_t,             // Key type
                    uint64_t,             // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    CLOCKCacheObject,     // Cache object type
                    CLOCKCache,           // Cache type
                    VolatileStorage       // Storage type
                >;
                perform_benchmark_operation<T, uint64_t, uint64_t>(
                    uint64_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            } else if (key_type == "char16" && value_type == "char16") {
                using T = BPlusStoreTraits<
                    CHAR16,               // Key type
                    CHAR16,               // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    CLOCKCacheObject,     // Cache object type
                    CLOCKCache,           // Cache type
                    VolatileStorage       // Storage type
                >;
                perform_benchmark_operation<T, CHAR16, CHAR16>(
                    char16_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            }
        } else if (cache_type == "CLOCK" && storage_type == "FileStorage") {
            // Use CLOCKCache, CLOCKCacheObject, FileStorage templates
            if (key_type == "uint64_t" && value_type == "uint64_t") {
                using T = BPlusStoreTraits<
                    uint64_t,             // Key type
                    uint64_t,             // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    CLOCKCacheObject,     // Cache object type
                    CLOCKCache,           // Cache type
                    FileStorage           // Storage type
                >;
                perform_benchmark_operation<T, uint64_t, uint64_t>(
                    uint64_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            } else if (key_type == "char16" && value_type == "char16") {
                using T = BPlusStoreTraits<
                    CHAR16,               // Key type
                    CHAR16,               // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    CLOCKCacheObject,     // Cache object type
                    CLOCKCache,           // Cache type
                    FileStorage           // Storage type
                >;
                perform_benchmark_operation<T, CHAR16, CHAR16>(
                    char16_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            }
        } else if (cache_type == "CLOCK" && storage_type == "PMemStorage") {
            // Use CLOCKCache, CLOCKCacheObject, PMemStorage templates
            if (key_type == "uint64_t" && value_type == "uint64_t") {
                using T = BPlusStoreTraits<
                    uint64_t,             // Key type
                    uint64_t,             // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    CLOCKCacheObject,     // Cache object type
                    CLOCKCache,           // Cache type
                    PMemStorage           // Storage type
                >;
                perform_benchmark_operation<T, uint64_t, uint64_t>(
                    uint64_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            } else if (key_type == "char16" && value_type == "char16") {
                using T = BPlusStoreTraits<
                    CHAR16,               // Key type
                    CHAR16,               // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    CLOCKCacheObject,     // Cache object type
                    CLOCKCache,           // Cache type
                    PMemStorage           // Storage type
                >;
                perform_benchmark_operation<T, CHAR16, CHAR16>(
                    char16_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            }
        } else if (cache_type == "A2Q" && storage_type == "VolatileStorage") {
            // Use A2QCache, A2QCacheObject, VolatileStorage templates
            if (key_type == "uint64_t" && value_type == "uint64_t") {
                using T = BPlusStoreTraits<
                    uint64_t,             // Key type
                    uint64_t,             // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    A2QCacheObject,       // Cache object type
                    A2QCache,             // Cache type
                    VolatileStorage       // Storage type
                >;
                perform_benchmark_operation<T, uint64_t, uint64_t>(
                    uint64_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            } else if (key_type == "char16" && value_type == "char16") {
                using T = BPlusStoreTraits<
                    CHAR16,               // Key type
                    CHAR16,               // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    A2QCacheObject,       // Cache object type
                    A2QCache,             // Cache type
                    VolatileStorage       // Storage type
                >;
                perform_benchmark_operation<T, CHAR16, CHAR16>(
                    char16_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            }
        } else if (cache_type == "A2Q" && storage_type == "FileStorage") {
            // Use A2QCache, A2QCacheObject, FileStorage templates
            if (key_type == "uint64_t" && value_type == "uint64_t") {
                using T = BPlusStoreTraits<
                    uint64_t,             // Key type
                    uint64_t,             // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    A2QCacheObject,       // Cache object type
                    A2QCache,             // Cache type
                    FileStorage           // Storage type
                >;
                perform_benchmark_operation<T, uint64_t, uint64_t>(
                    uint64_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            } else if (key_type == "char16" && value_type == "char16") {
                using T = BPlusStoreTraits<
                    CHAR16,               // Key type
                    CHAR16,               // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    A2QCacheObject,       // Cache object type
                    A2QCache,             // Cache type
                    FileStorage           // Storage type
                >;
                perform_benchmark_operation<T, CHAR16, CHAR16>(
                    char16_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            }
        } else if (cache_type == "A2Q" && storage_type == "PMemStorage") {
            // Use A2QCache, A2QCacheObject, PMemStorage templates
            if (key_type == "uint64_t" && value_type == "uint64_t") {
                using T = BPlusStoreTraits<
                    uint64_t,             // Key type
                    uint64_t,             // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    A2QCacheObject,       // Cache object type
                    A2QCache,             // Cache type
                    PMemStorage           // Storage type
                >;
                perform_benchmark_operation<T, uint64_t, uint64_t>(
                    uint64_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            } else if (key_type == "char16" && value_type == "char16") {
                using T = BPlusStoreTraits<
                    CHAR16,               // Key type
                    CHAR16,               // Value type
                    TYPE_UID::DATA_NODE_INT_INT,
                    TYPE_UID::INDEX_NODE_INT_INT,
                    ObjectFatUID,         // Object UID type
                    A2QCacheObject,       // Cache object type
                    A2QCache,             // Cache type
                    PMemStorage           // Storage type
                >;
                perform_benchmark_operation<T, CHAR16, CHAR16>(
                    char16_data, operation, degree, cache_size, page_size, memory_size,
                    logger, tree_type, cache_type, storage_type, run);
            }
        } else {
            std::cerr << "Error: Unknown cache/storage combination: " << cache_type << "/" << storage_type << std::endl;
            std::cerr << "Available cache types: LRU, CLOCK, A2Q" << std::endl;
            std::cerr << "Available storage types: VolatileStorage, FileStorage, PMemStorage" << std::endl;
            return;
        }
    }
    
    std::cout << "\nCache benchmark single configuration completed!" << std::endl;
}

// Function to test with cache type string (for backward compatibility)
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
    
    // Test different cache and storage combinations
    std::vector<std::pair<std::string, std::string>> cache_storage_combos = {
        {cache_type, "VolatileStorage"},
        {cache_type, "FileStorage"}
    };
    
    for (size_t run = 1; run <= num_runs; run++) {
        std::cout << "\n--- Run " << run << "/" << num_runs << " ---" << std::endl;
        
        for (const auto& combo : cache_storage_combos) {
            for (size_t records : record_counts) {
                for (size_t degree : degrees) {
                    for (const std::string& operation : operations) {
                        test_single_config(
                            "BplusTreeSOA", "uint64_t", "uint64_t", operation, 
                            degree, records, 1, "", combo.first, combo.second, 100, 2048, 1073741824LL);
                    }
                }
            }
        }
    }
    
    std::cout << "\nCache benchmark completed!" << std::endl;
}

} // namespace bm_tree_with_cache

#endif // BM_TREE_WITH_CACHE_HPP