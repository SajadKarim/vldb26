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
#include "ObjectFatUID.h"
#include "DataNodeCOpt.hpp"
#include "IndexNodeCOpt.hpp"
#include "PMemWAL.hpp"

namespace bm_tree_with_cache_simple {

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

// Simple test function that exactly mirrors the working test
void test_simple_cache_benchmark() {
    std::cout << "\n=== Simple Cache Benchmark Test ===" << std::endl;
    
    // Use the exact same trait configuration as the working test
    using T = GenericCOptTraits<
        int,                   // Key type - same as working test
        int,                   // Value type - same as working test
        TYPE_UID::DATA_NODE_INT_INT,
        TYPE_UID::INDEX_NODE_INT_INT,
        ObjectFatUID,          // Object UID type
        LRUCacheObject,
        LRUCache,
        VolatileStorage
    >;

    using MyStore = typename T::StoreType;
    
    // Test parameters (same as working test)
    int nDegree = 64;
    int nTotalRecords = 10000;
    int nCacheSize = 100;
    int nBlockSize = 4096;
    size_t nStorageSize = 1024 * 1024 * 1024;  // 1GB
    
    std::cout << "Creating tree with cache..." << std::endl;
    std::cout << "Degree: " << nDegree << ", Records: " << nTotalRecords << std::endl;
    std::cout << "Cache size: " << nCacheSize << ", Block size: " << nBlockSize << std::endl;
    
    // Create tree exactly like the working test
    MyStore* ptrTree = new MyStore(nDegree, nCacheSize, nBlockSize, nStorageSize);
    ptrTree->template init<T::DataNodeType>();
    
    // Generate test data
    std::vector<int> vtNumberData(nTotalRecords);
    std::iota(vtNumberData.begin(), vtNumberData.end(), 1);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(vtNumberData.begin(), vtNumberData.end(), g);
    
    std::cout << "Starting insert operations..." << std::endl;
    auto begin = std::chrono::steady_clock::now();
    
    // Insert operations exactly like the working test
    for (int nCntr = 0; nCntr < nTotalRecords; nCntr++) {
        ErrorCode ec = ptrTree->insert(vtNumberData[nCntr], vtNumberData[nCntr]);
        if (ec != ErrorCode::Success) {
            std::cout << "Insert failed at record " << nCntr << std::endl;
            break;
        }
    }
    
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    
    std::cout << "Insert completed!" << std::endl;
    std::cout << "Time taken: " << duration.count() << " microseconds" << std::endl;
    std::cout << "Throughput: " << (nTotalRecords * 1000000.0 / duration.count()) << " ops/sec" << std::endl;
    
    // Cleanup
    delete ptrTree;
    
    std::cout << "Simple cache benchmark completed successfully!" << std::endl;
}

} // namespace bm_tree_with_cache_simple

#endif // __TREE_WITH_CACHE__