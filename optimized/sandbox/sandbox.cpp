#include <iostream>
#include <iomanip>
#include <functional>

#define __MAX_MSG__ 10
#define __CACHE_COUNTERS__
//#define __PROD__
//#define __FANOUT_PROD_ENV__ 24

#ifdef _MSC_VER
#define WAL_FILE_PATH "c:\\wal.bin"
#define FILE_STORAGE_PATH "c:\\filestore.hdb"
#define PMEM_STORAGE_PATH "c:\\filestore.hdb"
#define PMEM_STORAGE_PATH_II "c:\\filestore.hdb"
#else //_MSC_VER
#define WAL_FILE_PATH "/mnt/tmpfs/wal.bin"
#define FILE_STORAGE_PATH "/mnt/tmpfs/filestore.hdb"
#define PMEM_STORAGE_PATH "/mnt/tmpfs/datafile1"
#define PMEM_STORAGE_PATH_II "/mnt/tmpfs/datafile2"
#endif //_MSC_VER

#include "LRUCache.hpp"
#include "BPlusStore.hpp"
#include "NoCache.hpp"
//#include "glog/logging.h"
#include <type_traits>
#include <variant>
#include <typeinfo>
#include "DataNode.hpp"
#include "DataNodeCOpt.hpp"
#include "DataNodeCROpt.hpp"
#include "IndexNode.hpp"
#include "IndexNodeCOpt.hpp"
#include "DataNodeROpt.hpp"
#include "IndexNodeROpt.hpp"
#include "IndexNodeCROpt.hpp"

#include "BEpsilonStore.hpp"
#include "DataNodeWithBuffer.hpp"
#include "DataNodeWithBufferROpt.hpp"
#include "DataNodeWithBufferRWOpt.hpp"
#include "IndexNodeWithBuffer.hpp"
#include "IndexNodeWithBufferROpt.hpp"
#include "IndexNodeWithBufferRWOpt.hpp"

#include "DataNodeWithBufferCOpt.hpp"
#include "IndexNodeWithBufferCOpt.hpp"

#include "BEpsilonStoreEx.hpp"
#include "DataNodeWithBufferEx.hpp"
#include "IndexNodeWithBufferEx.hpp"

#include "BEpsilonStoreExII.hpp"
#include "IndexNodeWithBufferExII.hpp"


#include "BEpsilonStoreInMemory.hpp"
#include "DataNodeWithBufferCOptInMemory.hpp"
#include "IndexNodeWithBufferCOptInMemory_Simple.hpp"

#include <chrono>
#include <cassert>
#include "VolatileStorage.hpp"
#include "NoCacheObject.hpp"
#include "LRUCacheObject.hpp"
#include "FileStorage.hpp"
#include "PMemStorage.hpp"
#include "TypeUID.h"
#include <iostream>
#include "ObjectFatUID.h"
#include "ObjectUID.h"
#include <set>
#include <random>
#include <numeric>
#include "A2QCache.hpp"
#include "A2QCacheObject.hpp"
#include <string>
#include "validityasserts.h"

#include "FileWAL.hpp"
#include "PMemWAL.hpp"
#include "HybridStorage.hpp"

#include "CLOCKCache.hpp"
#include "CLOCKCacheObject.hpp"

#ifdef __CACHE_COUNTERS__
// Callback type for collecting cache stats from threads
template<typename BPlusStoreType>
using StatsCollectorCallback = std::function<void(const CacheStatsProvider<typename BPlusStoreType::CacheType>*)>;
#endif

#ifndef _MSC_VER
//#include "MMapStorage.hpp"
//#include "IOUringStorage.hpp"
#include "../../../tlx/tlx/tlx/container/btree_map.hpp"
#endif //_MSC_VER

template <typename Traits> class DataNode;
template <typename Traits> class DataNodeCOpt;
template <typename Traits> class DataNodeROpt;
template <typename Traits> class DataNodeCROpt;
template <typename Traits> class IndexNode;
template <typename Traits> class IndexNodeCOpt;
template <typename Traits> class IndexNodeROpt;
template <typename Traits> class IndexNodeCROpt;
template <typename Traits> class LRUCacheObject;
template <typename Traits> class NoCacheObject;
template <typename Traits> class VolatileStorage;
template <typename Traits> class FileStorage;
template <typename Traits> class PMemStorage;
template <typename Traits> class LRUCache;
template <typename Traits> class CLOCKCache;
template <typename Traits> class NoCache;
template <typename Traits> class BPlusStore;
template <typename Traits> class HybridStorage;

#ifndef _MSC_VER
template <typename Traits> class MMapStorage;
template <typename Traits> class IOUringStorage;
#endif //_MSC_VER

template <typename Traits> class FileWAL;
template <typename Traits> class PMemWAL;

template <typename Traits> class BEpsilonStore;
template <typename Traits> class DataNodeWithBuffer;
template <typename Traits> class DataNodeWithBufferROpt;
template <typename Traits> class DataNodeWithBufferRWOpt;
template <typename Traits> class IndexNodeWithBuffer;
template <typename Traits> class IndexNodeWithBufferROpt;
template <typename Traits> class IndexNodeWithBufferRWOpt;

template <typename Traits> class DataNodeWithBufferCOpt;
template <typename Traits> class IndexNodeWithBufferCOpt;

template <typename Traits> class BEpsilonStoreEx;
template <typename Traits> class DataNodeWithBufferEx;
template <typename Traits> class IndexNodeWithBufferEx;

template <typename Traits> class BEpsilonStoreInMemory;
template <typename Traits> class DataNodeWithBufferCOptInMemory;
template <typename Traits> class IndexNodeWithBufferCOptInMemory;

template<typename T>
struct NoStorage {};

//// Traits for InMemory B-Epsilon Tree
//struct InMemoryTraits
//{
//    using KeyType = int;
//    using ValueType = int;
//    using DataNodeType = DataNodeWithBufferCOptInMemory<InMemoryTraits>;
//    using IndexNodeType = IndexNodeWithBufferCOptInMemory<InMemoryTraits>;
//    
//    enum MessageType
//    {
//        Insert = 1,
//        Update = 2,
//        Delete = 3
//    };
//    
//    struct BufferedMessage
//    {
//        MessageType op;
//        KeyType key;
//        ValueType value;
//    };
//    
//    static const uint8_t DataNodeUID = 1;
//    static const uint8_t IndexNodeUID = 2;
//};
//
//
//template <
//#ifdef __PROD__
//    uint16_t TFanout,
//#endif //__PROD__
//    typename TKeyType,
//    typename TValueType,
//    uint8_t TDataNodeUID,
//    uint8_t TIndexNodeUID,
//    typename TObjectUIDType,
//    template<typename> class TCacheObjectType,
//    template<typename> class TCacheType,
//    template<typename> class TStorageType = NoStorage
//>
//struct HBPlusStoreTraits
//{
//#ifdef __PROD__
//    static constexpr uint8_t Fanout = TFanout;
//#endif //__PROD__
//
//    using KeyType = TKeyType;
//    using ValueType = TValueType;
//    using ObjectUIDType = TObjectUIDType;
//
//    static constexpr uint8_t DataNodeUID = TDataNodeUID;
//    static constexpr uint8_t IndexNodeUID = TIndexNodeUID;
//
//    // Core Objects
//    using DataNodeType = DataNode<HBPlusStoreTraits>;
//    using IndexNodeType = IndexNode<HBPlusStoreTraits>;
//    using ObjectType = TCacheObjectType<HBPlusStoreTraits>;
//
//    // Cache and Storage
//    using CacheType = TCacheType<HBPlusStoreTraits>;
//    using StorageType = TStorageType<HBPlusStoreTraits>;
//
//    // Store
//    using StoreType = BPlusStore<HBPlusStoreTraits>;
//
//#ifndef _MSC_VER
//    using WALType = PMemWAL<HBPlusStoreTraits>;
//#else //_MSC_VER
//    using WALType = FileWAL<HBPlusStoreTraits>;
//#endif //_MSC_VER
//};


template <
#ifdef __PROD__
    uint16_t TFanout,
#endif //__PROD__
    typename TKeyType,
    typename TValueType,
    uint8_t TDataNodeUID,
    uint8_t TIndexNodeUID,
    typename TObjectUIDType,
    template<typename> class TCacheObjectType,
    template<typename> class TCacheType,
    template<typename> class TStorageType = NoStorage
>
struct BPlusStoreTraits
{
#ifdef __PROD__
    static constexpr uint8_t Fanout = TFanout;
#endif //__PROD__

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

//template <
//#ifdef __PROD__
//    uint16_t TFanout,
//#endif //__PROD__
//    typename TKeyType,
//    typename TValueType,
//    uint8_t TDataNodeUID,
//    uint8_t TIndexNodeUID,
//    typename TObjectUIDType,
//    template<typename> class TCacheObjectType,
//    template<typename> class TCacheType,
//    template<typename> class TStorageType = NoStorage
//>
//struct BPlusStoreROptTraits
//{
//#ifdef __PROD__
//    static constexpr uint8_t Fanout = TFanout;
//#endif //__PROD__
//
//    using KeyType = TKeyType;
//    using ValueType = TValueType;
//    using ObjectUIDType = TObjectUIDType;
//
//    static constexpr uint8_t DataNodeUID = TDataNodeUID;
//    static constexpr uint8_t IndexNodeUID = TIndexNodeUID;
//    static constexpr uint16_t BufferRatioToFanout = 5;
//
//    // Core Objects
//    using DataNodeType = DataNodeCROpt<BPlusStoreROptTraits>;
//    using IndexNodeType = IndexNodeCROpt<BPlusStoreROptTraits>;
//    using ObjectType = TCacheObjectType<BPlusStoreROptTraits>;
//
//    // Cache and Storage
//    using CacheType = TCacheType<BPlusStoreROptTraits>;
//    using StorageType = TStorageType<BPlusStoreROptTraits>;
//
//    // Store
//    using StoreType = BPlusStore<BPlusStoreROptTraits>;
//
//#ifndef _MSC_VER
//    using WALType = PMemWAL<BPlusStoreROptTraits>;
//#else //_MSC_VER
//    using WALType = FileWAL<BPlusStoreROptTraits>;
//#endif //_MSC_VER
//
//};
//
//////////////////////////////////////////
//
//template <
//#ifdef __PROD__
//    uint16_t TFanout,
//#endif //__PROD__
//    typename TKeyType,
//    typename TValueType,
//    uint8_t TDataNodeUID,
//    uint8_t TIndexNodeUID,
//    typename TObjectUIDType,
//    template<typename> class TCacheObjectType,
//    template<typename> class TCacheType,
//    template<typename> class TStorageType = NoStorage
//>
//struct BPlusStoreCOptTraits
//{
//#ifdef __PROD__
//    static constexpr uint8_t Fanout = TFanout;
//#endif //__PROD__
//
//    using KeyType = TKeyType;
//    using ValueType = TValueType;
//    using ObjectUIDType = TObjectUIDType;
//
//    static constexpr uint8_t DataNodeUID = TDataNodeUID;
//    static constexpr uint8_t IndexNodeUID = TIndexNodeUID;
//    static constexpr uint16_t BufferRatioToFanout = 5;
//
//    // Core Objects
//    using DataNodeType = DataNodeCOpt<BPlusStoreCOptTraits>;
//    using IndexNodeType = IndexNodeCOpt<BPlusStoreCOptTraits>;
//    using ObjectType = TCacheObjectType<BPlusStoreCOptTraits>;
//
//    // Cache and Storage
//    using CacheType = TCacheType<BPlusStoreCOptTraits>;
//    using StorageType = TStorageType<BPlusStoreCOptTraits>;
//
//    // Store
//    using StoreType = BPlusStore<BPlusStoreCOptTraits>;
//
//#ifndef _MSC_VER
//    using WALType = PMemWAL<BPlusStoreCOptTraits>;
//#else //_MSC_VER
//    using WALType = FileWAL<BPlusStoreCOptTraits>;
//#endif //_MSC_VER
//
//};
//
//template <
//#ifdef __PROD__
//    uint16_t TFanout,
//#endif //__PROD__
//    typename TKeyType,
//    typename TValueType,
//    uint8_t TDataNodeUID,
//    uint8_t TIndexNodeUID,
//    typename TObjectUIDType,
//    template<typename> class TCacheObjectType,
//    template<typename> class TCacheType,
//    template<typename> class TStorageType = NoStorage
//>
//struct BPlusStoreCROptTraits
//{
//#ifdef __PROD__
//    static constexpr uint8_t Fanout = TFanout;
//#endif //__PROD__
//
//    using KeyType = TKeyType;
//    using ValueType = TValueType;
//    using ObjectUIDType = TObjectUIDType;
//
//    static constexpr uint8_t DataNodeUID = TDataNodeUID;
//    static constexpr uint8_t IndexNodeUID = TIndexNodeUID;
//    static constexpr uint16_t BufferRatioToFanout = 5;
//
//    // Core Objects
//    using DataNodeType = DataNodeCROpt<BPlusStoreCROptTraits>;
//    using IndexNodeType = IndexNodeCROpt<BPlusStoreCROptTraits>;
//    using ObjectType = TCacheObjectType<BPlusStoreCROptTraits>;
//
//    // Cache and Storage
//    using CacheType = TCacheType<BPlusStoreCROptTraits>;
//    using StorageType = TStorageType<BPlusStoreCROptTraits>;
//
//    // Store
//    using StoreType = BPlusStore<BPlusStoreCROptTraits>;
//
//#ifndef _MSC_VER
//    using WALType = PMemWAL<BPlusStoreCROptTraits>;
//#else //_MSC_VER
//    using WALType = FileWAL<BPlusStoreCROptTraits>;
//#endif //_MSC_VER
//
//};
//
//
//////////////////////////////////////////
//
//template <
//    typename TKeyType,
//    typename TValueType,
//    uint8_t TDataNodeUID,
//    uint8_t TIndexNodeUID,
//    typename TObjectUIDType,
//    template<typename> class TCacheObjectType,
//    template<typename> class TCacheType,
//    template<typename> class TStorageType = NoStorage
//>
//struct BEpsilonStoreTraits
//{
//    using KeyType = TKeyType;
//    using ValueType = TValueType;
//    using ObjectUIDType = TObjectUIDType;
//
//    static constexpr uint8_t DataNodeUID = TDataNodeUID;
//    static constexpr uint8_t IndexNodeUID = TIndexNodeUID;
//    static constexpr uint16_t BufferRatioToFanout = 5;
//
//    enum MessageType
//    {
//        Insert,
//        Delete,
//        Update
//    };
//
//    struct BufferedMessage
//    {
//        enum MessageType op;
//        KeyType key;
//        ValueType value; // optional depending on op
//    };
//
//    struct MessageData
//    {
//        ValueType value;
//        MessageType operation;
//
//        MessageData() : operation(MessageType::Delete) {} // Default constructor creates Delete operation
//        MessageData(const ValueType& val, MessageType op) : value(val), operation(op) {}
//    };
//
//    // Core Objects
//    using DataNodeType = DataNodeWithBuffer<BEpsilonStoreTraits>;
//    using IndexNodeType = IndexNodeWithBuffer<BEpsilonStoreTraits>;
//    using ObjectType = TCacheObjectType<BEpsilonStoreTraits>;
//
//    // Cache and Storage
//    using CacheType = TCacheType<BEpsilonStoreTraits>;
//    using StorageType = TStorageType<BEpsilonStoreTraits>;
//
//    // Store
//    using StoreType = BEpsilonStore<BEpsilonStoreTraits>;
//
//#ifndef _MSC_VER
//    using WALType = PMemWAL<BEpsilonStoreTraits>;
//#else //_MSC_VER
//    using WALType = FileWAL<BEpsilonStoreTraits>;
//#endif //_MSC_VER
//
//};
//
//template <
//    typename TKeyType,
//    typename TValueType,
//    uint8_t TDataNodeUID,
//    uint8_t TIndexNodeUID,
//    typename TObjectUIDType,
//    template<typename> class TCacheObjectType,
//    template<typename> class TCacheType,
//    template<typename> class TStorageType = NoStorage,
//    bool TEnableLazyIndex = false,          // Enable lazy index insertion
//    bool TEnableLazyData = false,          // Enable lazy data insertion
//    uint16_t TBufferRatioToFanout = 3       // Buffer ratio to fanout
//
//>
//struct BEpsilonStoreExTraits
//{
//    using KeyType = TKeyType;
//    using ValueType = TValueType;
//    using ObjectUIDType = TObjectUIDType;
//
//    static constexpr uint8_t DataNodeUID = TDataNodeUID;
//    static constexpr uint8_t IndexNodeUID = TIndexNodeUID;
//
//    // Configuration options
//    static constexpr bool EnableLazyIndex = TEnableLazyIndex;
//    static constexpr bool EnableLazyData = TEnableLazyData;
//    static constexpr uint16_t BufferRatioToFanout = TBufferRatioToFanout;
//
//    enum class MessageType
//    {
//        Insert = 0,
//        Update = 1,
//        Delete = 2
//    };
//
//    struct MessageData
//    {
//        ValueType value;
//        MessageType operation;
//
//        MessageData() : operation(MessageType::Delete) {} // Default constructor creates Delete operation
//        MessageData(const ValueType& val, MessageType op) : value(val), operation(op) {}
//    };
//
//    // Core Objects
//    using DataNodeType = DataNodeWithBufferEx<BEpsilonStoreExTraits>;
//    using IndexNodeType = IndexNodeWithBufferExII<BEpsilonStoreExTraits>;
//    using ObjectType = TCacheObjectType<BEpsilonStoreExTraits>;
//
//    // Cache and Storage
//    using CacheType = TCacheType<BEpsilonStoreExTraits>;
//    using StorageType = TStorageType<BEpsilonStoreExTraits>;
//
//    // Store
//    using StoreType = BEpsilonStoreExII<BEpsilonStoreExTraits>;
//
//#ifndef _MSC_VER
//    using WALType = PMemWAL<BEpsilonStoreExTraits>;
//#else //_MSC_VER
//    using WALType = FileWAL<BEpsilonStoreExTraits>;
//#endif //_MSC_VER
//
//};
//
//template <
//    typename TKeyType,
//    typename TValueType,
//    uint8_t TDataNodeUID,
//    uint8_t TIndexNodeUID,
//    typename TObjectUIDType,
//    template<typename> class TCacheObjectType,
//    template<typename> class TCacheType,
//    template<typename> class TStorageType = NoStorage
//>
//struct BEpsilonStoreROptTraits
//{
//    using KeyType = TKeyType;
//    using ValueType = TValueType;
//    using ObjectUIDType = TObjectUIDType;
//
//    static constexpr uint8_t DataNodeUID = TDataNodeUID;
//    static constexpr uint8_t IndexNodeUID = TIndexNodeUID;
//    static constexpr uint16_t BufferRatioToFanout = 5;
//
//    enum MessageType
//    {
//        Insert,
//        Delete,
//        Update
//    };
//
//    struct BufferedMessage
//    {
//        enum MessageType op;
//        KeyType key;
//        ValueType value; // optional depending on op
//    };
//
//    // Core Objects
//    using DataNodeType = DataNodeWithBufferROpt<BEpsilonStoreROptTraits>;
//    using IndexNodeType = IndexNodeWithBufferROpt<BEpsilonStoreROptTraits>;
//    using ObjectType = TCacheObjectType<BEpsilonStoreROptTraits>;
//
//    // Cache and Storage
//    using CacheType = TCacheType<BEpsilonStoreROptTraits>;
//    using StorageType = TStorageType<BEpsilonStoreROptTraits>;
//
//    // Store
//    using StoreType = BEpsilonStore<BEpsilonStoreROptTraits>;
//
//#ifndef _MSC_VER
//    using WALType = PMemWAL<BEpsilonStoreROptTraits>;
//#else //_MSC_VER
//    using WALType = FileWAL<BEpsilonStoreROptTraits>;
//#endif //_MSC_VER
//
//};
//
//template <
//    typename TKeyType,
//    typename TValueType,
//    uint8_t TDataNodeUID,
//    uint8_t TIndexNodeUID,
//    typename TObjectUIDType,
//    template<typename> class TCacheObjectType,
//    template<typename> class TCacheType,
//    template<typename> class TStorageType = NoStorage
//>
//struct BEpsilonStoreRWOptTraits
//{
//    using KeyType = TKeyType;
//    using ValueType = TValueType;
//    using ObjectUIDType = TObjectUIDType;
//
//    static constexpr uint8_t DataNodeUID = TDataNodeUID;
//    static constexpr uint8_t IndexNodeUID = TIndexNodeUID;
//    static constexpr uint16_t BufferRatioToFanout = 5;
//
//    enum MessageType
//    {
//        Insert,
//        Delete,
//        Update
//    };
//
//    struct BufferedMessage
//    {
//        enum MessageType op;
//        KeyType key;
//        ValueType value; // optional depending on op
//    };
//
//    // Core Objects
//    using DataNodeType = DataNodeWithBufferRWOpt<BEpsilonStoreRWOptTraits>;
//    using IndexNodeType = IndexNodeWithBufferRWOpt<BEpsilonStoreRWOptTraits>;
//    using ObjectType = TCacheObjectType<BEpsilonStoreRWOptTraits>;
//
//    // Cache and Storage
//    using CacheType = TCacheType<BEpsilonStoreRWOptTraits>;
//    using StorageType = TStorageType<BEpsilonStoreRWOptTraits>;
//
//    // Store
//    using StoreType = BEpsilonStore<BEpsilonStoreRWOptTraits>;
//
//#ifndef _MSC_VER
//    using WALType = PMemWAL<BEpsilonStoreRWOptTraits>;
//#else //_MSC_VER
//    using WALType = FileWAL<BEpsilonStoreRWOptTraits>;
//#endif //_MSC_VER
//
//};



struct CHAR16 {
    char data[16];

    // Default constructor (trivial)
    CHAR16() = default;

    // Parameterized constructor
    CHAR16(const char* str) {
        std::memset(data, 0, sizeof(data));
#ifndef _MSC_VER
        strncpy(data, str, sizeof(data) - 1);
#else //_MSC_VER
        strncpy_s(data, sizeof(data), str, sizeof(data) - 1);
#endif //_MSC_VER
    }

    // Define the < operator for comparison
    bool operator<(const CHAR16& other) const {
        return std::strncmp(data, other.data, sizeof(data)) < 0;
    }

    // Define the == operator for comparison
    bool operator==(const CHAR16& other) const {
        return std::strncmp(data, other.data, sizeof(data)) == 0;
    }
};

#define GENERATE_RANDOM_NUMBER_ARRAY(__START__, __END__, __VECTOR__) { \
    __VECTOR__.resize(__END__ - __START__); \
    std::iota(__VECTOR__.begin(), __VECTOR__.end(), __START__); \
    ::random_device _rd; \
    std::mt19937 _eng(_rd()); \
    std::shuffle(__VECTOR__.begin(), __VECTOR__.end(), _eng); \
}

#define GENERATE_RANDOM_CHAR_ARRAY(__STR_LENGTH__, __SAMPLE_SIZE__, __VECTOR__) { \
    __VECTOR__.resize(__SAMPLE_SIZE__); \
    const char _charset[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"; \
    const size_t _charsetSize = sizeof(_charset) - 1; \
    for (size_t _idx = 0; _idx < __SAMPLE_SIZE__; _idx++) { \
        std::string _string; \
        for (size_t _idy = 0; _idy < __STR_LENGTH__; _idy++) { \
            _string += _charset[std::rand() % _charsetSize]; \
        } \
        __VECTOR__[_idx] = _string.c_str(); \
    } \
}

#ifdef __CONCURRENT__
template <typename BPlusStoreType>
void insert_concurent(BPlusStoreType* ptrTree, int nRangeStart, int nRangeEnd
#ifdef __CACHE_COUNTERS__
    , StatsCollectorCallback<BPlusStoreType> stats_callback = nullptr
#endif
)
{
    std::vector<int> vtNumberData;
    GENERATE_RANDOM_NUMBER_ARRAY(nRangeStart, nRangeEnd, vtNumberData);

    for (int nCntr = 0; nCntr < nRangeEnd - nRangeStart; nCntr++)
    {
        ErrorCode ec = ptrTree->insert(vtNumberData[nCntr], vtNumberData[nCntr]);
        ASSERT(ec == ErrorCode::Success);
    }

#ifdef __CACHE_COUNTERS__
    // Collect stats before thread terminates
    if (stats_callback) {
        auto cache = ptrTree->getCache();
        if (cache) {
            auto cache_provider = cache->getCacheStatsProvider();
            if (cache_provider) {
                stats_callback(cache_provider);
            }
        }
    }
#endif
}

template <typename BPlusStoreType>
void reverse_insert_concurent(BPlusStoreType* ptrTree, int nRangeStart, int nRangeEnd)
{
    for (int nCntr = nRangeEnd - 1; nCntr >= nRangeStart; nCntr--)
    {
        ErrorCode ec = ptrTree->insert(nCntr, nCntr);
        ASSERT(ec == ErrorCode::Success);
    }
}

template <typename BPlusStoreType>
void search_concurent(BPlusStoreType* ptrTree, int nRangeStart, int nRangeEnd
#ifdef __CACHE_COUNTERS__
    , StatsCollectorCallback<BPlusStoreType> stats_callback = nullptr
#endif
)
{
    std::vector<int> vtNumberData;
    GENERATE_RANDOM_NUMBER_ARRAY(nRangeStart, nRangeEnd, vtNumberData);

    for (int nCntr = 0; nCntr < nRangeEnd - nRangeStart; nCntr++)
    {
        int nValue = 0;
        ErrorCode ec = ptrTree->search(vtNumberData[nCntr], nValue);

        if (vtNumberData[nCntr] != nValue || ec != ErrorCode::Success)
        {
            std::cout << "vtNumberData[nCntr] (" << vtNumberData[nCntr] << ") nValue" << nValue << std::endl;
        }

        ASSERT(vtNumberData[nCntr] == nValue && ec == ErrorCode::Success);
    }

#ifdef __CACHE_COUNTERS__
    // Collect stats before thread terminates
    if (stats_callback) {
        auto cache = ptrTree->getCache();
        if (cache) {
            auto cache_provider = cache->getCacheStatsProvider();
            if (cache_provider) {
                stats_callback(cache_provider);
            }
        }
    }
#endif
}

template <typename BPlusStoreType>
void search_not_found_concurent(BPlusStoreType* ptrTree, int nRangeStart, int nRangeEnd
#ifdef __CACHE_COUNTERS__
    , StatsCollectorCallback<BPlusStoreType> stats_callback = nullptr
#endif
) {
    for (int nCntr = nRangeStart; nCntr < nRangeEnd; nCntr++)
    {
        int nValue = 0;
        ErrorCode ec = ptrTree->search(nCntr, nValue);

        ASSERT(ec == ErrorCode::KeyDoesNotExist);
    }

#ifdef __CACHE_COUNTERS__
    // Collect stats before thread terminates
    if (stats_callback) {
        auto cache = ptrTree->getCache();
        if (cache) {
            auto cache_provider = cache->getCacheStatsProvider();
            if (cache_provider) {
                stats_callback(cache_provider);
            }
        }
    }
#endif
}

template <typename BPlusStoreType>
void delete_concurent(BPlusStoreType* ptrTree, int nRangeStart, int nRangeEnd
#ifdef __CACHE_COUNTERS__
    , StatsCollectorCallback<BPlusStoreType> stats_callback = nullptr
#endif
)
{
    std::vector<int> vtNumberData;
    GENERATE_RANDOM_NUMBER_ARRAY(nRangeStart, nRangeEnd, vtNumberData);

    for (int nCntr = 0; nCntr < nRangeEnd - nRangeStart; nCntr++)
    {
        ErrorCode ec = ptrTree->remove(vtNumberData[nCntr]);

        ASSERT(ec == ErrorCode::Success);
    }

#ifdef __CACHE_COUNTERS__
    // Collect stats before thread terminates
    if (stats_callback) {
        auto cache = ptrTree->getCache();
        if (cache) {
            auto cache_provider = cache->getCacheStatsProvider();
            if (cache_provider) {
                stats_callback(cache_provider);
            }
        }
    }
#endif
}

template <typename BPlusStoreType>
void reverse_delete_concurent(BPlusStoreType* ptrTree, int nRangeStart, int nRangeEnd) {
    for (int nCntr = nRangeEnd - 1; nCntr >= nRangeStart; nCntr--)
    {
        ErrorCode ec = ptrTree->remove(nCntr);
        ASSERT(ec == ErrorCode::KeyDoesNotExist);
    }
}

template <typename BPlusStoreType>
void threaded_test(BPlusStoreType* ptrTree, int degree, int total_entries, int thread_count)
{
    vector<std::thread> vtThreads;

#ifdef __CACHE_COUNTERS__
    // Aggregated stats container
    struct AggregatedStats {
        std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> hits;
        std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> misses;
        std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> evictions;
        std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> dirty_evictions;
        std::mutex stats_mutex;
    };
    
    AggregatedStats aggregated_stats;
    
    // Stats collector callback
    auto stats_collector = [&aggregated_stats](const auto* cache_provider) {
        std::lock_guard<std::mutex> lock(aggregated_stats.stats_mutex);
        BPlusStoreType::CacheType::aggregateThreadStats(
            cache_provider,
            aggregated_stats.hits,
            aggregated_stats.misses,
            aggregated_stats.evictions,
            aggregated_stats.dirty_evictions
        );
    };
#endif

    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    for (size_t nTestCntr = 0; nTestCntr < 1; nTestCntr++) {

        for (int nIdx = 0; nIdx < thread_count; nIdx++)
        {
            int nTotal = total_entries / thread_count;
            vtThreads.push_back(std::thread(insert_concurent<BPlusStoreType>, ptrTree, nIdx * nTotal, nIdx * nTotal + nTotal
#ifdef __CACHE_COUNTERS__
                , stats_collector
#endif
            ));
        }

        auto it = vtThreads.begin();
        while (it != vtThreads.end())
        {
            (*it).join(); it++;
        }

        vtThreads.clear();


    
        for (int nIdx = 0; nIdx < thread_count; nIdx++)
        {
            int nTotal = total_entries / thread_count;
            vtThreads.push_back(std::thread(search_concurent<BPlusStoreType>, ptrTree, nIdx * nTotal, nIdx * nTotal + nTotal
#ifdef __CACHE_COUNTERS__
                , stats_collector
#endif
            ));
        }

        it = vtThreads.begin();
        while (it != vtThreads.end())
        {
            (*it).join(); it++;
        }

        vtThreads.clear();
        
        for (int nIdx = 0; nIdx < thread_count; nIdx++)
        {
            int nTotal = total_entries / thread_count;
            vtThreads.push_back(std::thread(delete_concurent<BPlusStoreType>, ptrTree, nIdx * nTotal, nIdx * nTotal + nTotal
#ifdef __CACHE_COUNTERS__
                , stats_collector
#endif
            ));
        }

        it = vtThreads.begin();
        while (it != vtThreads.end())
        {
            (*it).join(); it++;
        }

        vtThreads.clear();

       /* std::ofstream out_1("d:\\after_delete.txt");
      ptrTree->print(out_1);
      out_1.flush();
      out_1.close();*/


        for (int nIdx = 0; nIdx < thread_count; nIdx++)
        {
            int nTotal = total_entries / thread_count;
            vtThreads.push_back(std::thread(search_not_found_concurent<BPlusStoreType>, ptrTree, nIdx * nTotal, nIdx * nTotal + nTotal
#ifdef __CACHE_COUNTERS__
                , stats_collector
#endif
            ));
        }

        it = vtThreads.begin();
        while (it != vtThreads.end())
        {
            (*it).join(); it++;
        }

        vtThreads.clear();

#ifdef __TREE_WITH_CACHE__
      /*  size_t nLRU, nMap;
        ptrTree->getObjectsCountInCache(nLRU);
        ASSERT(nLRU == 1);*/
#endif //__TREE_WITH_CACHE__

    }
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

    std::cout
        << ">> threaded_test [Time: "
        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
        << std::endl;

#ifdef __CACHE_COUNTERS__
    ptrTree->flush();

    // Collect stats from background thread (if cache supports it)
    auto cache = ptrTree->getCache();
    auto cache_provider = cache->getCacheStatsProvider();
    if (cache_provider) {
        stats_collector(cache_provider);
    }

    // Output aggregated cache statistics
    std::cout << "=== Aggregated Cache Statistics ===" << std::endl;
    std::cout << "Total cache hits timeline entries: " << aggregated_stats.hits.size() << std::endl;
    std::cout << "Total cache misses timeline entries: " << aggregated_stats.misses.size() << std::endl;
    std::cout << "Total cache evictions timeline entries: " << aggregated_stats.evictions.size() << std::endl;
    std::cout << "Total dirty evictions timeline entries: " << aggregated_stats.dirty_evictions.size() << std::endl;
    
    // Calculate total counts
    uint64_t total_hits = 0, total_misses = 0, total_evictions = 0, total_dirty_evictions = 0;
    for (const auto& entry : aggregated_stats.hits) total_hits += entry.second;
    for (const auto& entry : aggregated_stats.misses) total_misses += entry.second;
    for (const auto& entry : aggregated_stats.evictions) total_evictions += entry.second;
    for (const auto& entry : aggregated_stats.dirty_evictions) total_dirty_evictions += entry.second;
    
    std::cout << "Total hits: " << total_hits << std::endl;
    std::cout << "Total misses: " << total_misses << std::endl;
    std::cout << "Total evictions: " << total_evictions << std::endl;
    std::cout << "Total dirty evictions: " << total_dirty_evictions << std::endl;
    
    if (total_hits + total_misses > 0) {
        double hit_rate = (double)total_hits / (total_hits + total_misses) * 100.0;
        std::cout << "Cache hit rate: " << std::fixed << std::setprecision(2) << hit_rate << "%" << std::endl;
    }
    std::cout << "===================================" << std::endl;
#endif
}

template <typename BPlusStoreType>
void fptree_threaded_test(BPlusStoreType* ptrTree, int total_entries, int thread_count)
{
    vector<std::thread> vtThreads;

    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    for (int nIdx = 0; nIdx < thread_count; nIdx++)
    {
        int nTotal = total_entries / thread_count;
        vtThreads.push_back(std::thread(insert_concurent<BPlusStoreType>, ptrTree, nIdx * nTotal, nIdx * nTotal + nTotal));
    }

    auto it = vtThreads.begin();
    while (it != vtThreads.end())
    {
        (*it).join();
        it++;
    }

    vtThreads.clear();

    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout
        << ">> insert (threaded) [Time: "
        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
        << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(10));

    ptrTree->flush();

    begin = std::chrono::steady_clock::now();

    for (int nIdx = 0; nIdx < thread_count; nIdx++)
    {
        int nTotal = total_entries / thread_count;
        vtThreads.push_back(std::thread(search_concurent<BPlusStoreType>, ptrTree, nIdx * nTotal, nIdx * nTotal + nTotal));
    }

    it = vtThreads.begin();
    while (it != vtThreads.end())
    {
        (*it).join();
        it++;
    }

    vtThreads.clear();

    end = std::chrono::steady_clock::now();
    std::cout
        << ">> search (threaded) [Time: "
        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
        << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(10));

    begin = std::chrono::steady_clock::now();

    for (int nIdx = 0; nIdx < thread_count; nIdx++)
    {
        int nTotal = total_entries / thread_count;
        vtThreads.push_back(std::thread(delete_concurent<BPlusStoreType>, ptrTree, nIdx * nTotal, nIdx * nTotal + nTotal));
    }

    it = vtThreads.begin();
    while (it != vtThreads.end())
    {
        (*it).join();
        it++;
    }

    vtThreads.clear();

    end = std::chrono::steady_clock::now();
    std::cout
        << ">> delete (threaded) [Time: "
        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
        << std::endl;

#ifdef __TREE_WITH_CACHE__
  /*  size_t nLRU, nMap;
    ptrTree->getObjectsCountInCache(nLRU);

    ASSERT(nLRU == 1 && nMap == 1);*/
#endif //__TREE_WITH_CACHE__
}
#endif //__CONCURRENT__

template <typename BPlusStoreType>
void int_test(BPlusStoreType* ptrTree, size_t nMaxNumber)
{
    std::vector<int> vtNumberData;
    GENERATE_RANDOM_NUMBER_ARRAY(0, nMaxNumber, vtNumberData);

    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    for (size_t nTestCntr = 0; nTestCntr < 2; nTestCntr++)
    {
        for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr = nCntr + 1)
        {
            ErrorCode code = ptrTree->insert(vtNumberData[nCntr], vtNumberData[nCntr]);
            ASSERT(code == ErrorCode::Success);
        }

        //std::ofstream out_1("d:\\tree_post_insert_0.txt");
        //ptrTree->print(out_1);
        //out_1.flush();
        //out_1.close();

        for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr++)
        {
            int nValue = 0;
            ErrorCode code = ptrTree->search(vtNumberData[nCntr], nValue);

            ASSERT(nValue == vtNumberData[nCntr]);
        }

        for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr = nCntr + 2)
        {
            ErrorCode code = ptrTree->remove(vtNumberData[nCntr]);

            ASSERT(code == ErrorCode::Success);
        }
        for (size_t nCntr = 1; nCntr < nMaxNumber; nCntr = nCntr + 2)
        {
            ErrorCode code = ptrTree->remove(vtNumberData[nCntr]);

            ASSERT(code == ErrorCode::Success);
        }

        for (int nCntr = 0; nCntr < nMaxNumber; nCntr++)
        {
            int nValue = 0;
            ErrorCode code = ptrTree->search(vtNumberData[nCntr], nValue);

            ASSERT(code == ErrorCode::KeyDoesNotExist);
        }

#ifdef __TREE_WITH_CACHE__
       /* size_t nLRU, nMap;
        ptrTree->getObjectsCountInCache(nLRU);

        ASSERT(nLRU == 1);*/
#endif //__TREE_WITH_CACHE__
    }

    for (size_t nTestCntr = 0; nTestCntr < 2; nTestCntr++)
    {
        for (int nCntr = nMaxNumber; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode ec = ptrTree->insert(nCntr, nCntr);
            ASSERT(ec == ErrorCode::Success);

        }
        for (int nCntr = nMaxNumber - 1; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode ec = ptrTree->insert(nCntr, nCntr);
            ASSERT(ec == ErrorCode::Success);
        }

        for (int nCntr = 0; nCntr < nMaxNumber; nCntr++)
        {
            int nValue = 0;
            ErrorCode ec = ptrTree->search(nCntr, nValue);

            ASSERT(nValue == nCntr && ec == ErrorCode::Success);
        }

        for (int nCntr = nMaxNumber; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode ec = ptrTree->remove(nCntr);
            ASSERT(ec == ErrorCode::Success);
        }

        for (int nCntr = nMaxNumber - 1; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode ec = ptrTree->remove(nCntr);
            ASSERT(ec == ErrorCode::Success);
        }

        for (int nCntr = 0; nCntr < nMaxNumber; nCntr++)
        {
            int nValue = 0;
            ErrorCode ec = ptrTree->search(nCntr, nValue);

            ASSERT(ec == ErrorCode::KeyDoesNotExist);
        }

#ifdef __TREE_WITH_CACHE__
       /* size_t nLRU, nMap;
        ptrTree->getObjectsCountInCache(nLRU);

        ASSERT(nLRU == 1);*/
#endif //__TREE_WITH_CACHE__
    }

    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout
        << ">> int_test [Time: "
        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
        << std::endl;
}

template <typename BPlusStoreType>
void string_test(BPlusStoreType* ptrTree, int degree, int total_entries)
{
    std::vector<CHAR16> vtCharData;
    GENERATE_RANDOM_CHAR_ARRAY(sizeof(CHAR16), total_entries, vtCharData);

    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    for (size_t nTestCntr = 0; nTestCntr < 10; nTestCntr++)
    {
        for (size_t nCntr = 0; nCntr < total_entries; nCntr = nCntr + 2)
        {
            ptrTree->insert(vtCharData.at(nCntr), vtCharData.at(nCntr));
        }
        for (size_t nCntr = 1; nCntr < total_entries; nCntr = nCntr + 2)
        {
            ptrTree->insert(vtCharData.at(nCntr), vtCharData.at(nCntr));
        }

        for (size_t nCntr = 0; nCntr < total_entries; nCntr++)
        {
            CHAR16 nValue;
            ErrorCode code = ptrTree->search(vtCharData.at(nCntr), nValue);

            ASSERT(nValue == vtCharData.at(nCntr));
        }

        for (size_t nCntr = 0; nCntr < total_entries; nCntr = nCntr + 2)
        {
            ErrorCode code = ptrTree->remove(vtCharData.at(nCntr));
        }
        for (size_t nCntr = 1; nCntr < total_entries; nCntr = nCntr + 2)
        {
            ErrorCode code = ptrTree->remove(vtCharData.at(nCntr));
        }

        for (size_t nCntr = 0; nCntr < total_entries; nCntr++)
        {
            CHAR16 nValue;
            ErrorCode code = ptrTree->search(vtCharData.at(nCntr), nValue);

            ASSERT(code == ErrorCode::KeyDoesNotExist);
        }

#ifdef __TREE_WITH_CACHE__
        size_t nLRU, nMap;
        ptrTree->getObjectsCountInCache(nLRU);

        ASSERT(nLRU == 1);
#endif //__TREE_WITH_CACHE__
    }

    for (size_t nTestCntr = 0; nTestCntr < 10; nTestCntr++)
    {
        for (int nCntr = total_entries - 1; nCntr >= 0; nCntr = nCntr - 2)
        {
            ptrTree->insert(vtCharData.at(nCntr), vtCharData.at(nCntr));
        }
        for (int nCntr = total_entries - 2; nCntr >= 0; nCntr = nCntr - 2)
        {
            ptrTree->insert(vtCharData.at(nCntr), vtCharData.at(nCntr));
        }

        for (int nCntr = 0; nCntr < total_entries; nCntr++)
        {
            CHAR16 nValue;
            ErrorCode code = ptrTree->search(vtCharData.at(nCntr), nValue);

            ASSERT(nValue == vtCharData.at(nCntr));
        }

        for (int nCntr = total_entries - 1; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode code = ptrTree->remove(vtCharData.at(nCntr));
        }
        for (int nCntr = total_entries - 2; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode code = ptrTree->remove(vtCharData.at(nCntr));
        }

        for (int nCntr = 0; nCntr < total_entries; nCntr++)
        {
            CHAR16 nValue;
            ErrorCode code = ptrTree->search(vtCharData.at(nCntr), nValue);

            ASSERT(code == ErrorCode::KeyDoesNotExist);
        }

#ifdef __TREE_WITH_CACHE__
        size_t nLRU, nMap;
        ptrTree->getObjectsCountInCache(nLRU);

        ASSERT(nLRU == 1);
#endif //__TREE_WITH_CACHE__
    }

    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout
        << ">> int_test [Time: "
        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
        << std::endl;
}

void test_for_ints()
{
    for (size_t nDegree = 32; nDegree < 256; nDegree = nDegree + 32)
    {
        std::cout << ">>>>> Running 'test_for_ints' for nDegree:" << nDegree << std::endl;

#ifndef __TREE_WITH_CACHE__
        {
            using T = BPlusStoreTraits<
                int,              // Key type
                int,              // Value type
                TYPE_UID::DATA_NODE_INT_INT,
                TYPE_UID::INDEX_NODE_INT_INT,
                uintptr_t,     // Object UID type
                NoCacheObject,
                NoCache
            >;

            using MyStore = typename T::StoreType;
            MyStore ptrTree(nDegree);

            ptrTree.template init<T::DataNodeType>();

            int_test<MyStore>(&ptrTree, 5000000);
        }
#else //__TREE_WITH_CACHE__
        {
            using T = BPlusStoreTraits <
                int,              // Key type
                int,              // Value type
                TYPE_UID::DATA_NODE_INT_INT,
                TYPE_UID::INDEX_NODE_INT_INT,
                ObjectFatUID,     // Object UID type
                LRUCacheObject,
                LRUCache,
#ifdef _MSC_VER
                VolatileStorage
#else //_MSC_VER
                VolatileStorage
#endif //_MSC_VER
            >;

            using MyStore = typename T::StoreType;
            MyStore ptrTree(nDegree, 100, 2048, 1ULL * 1024 * 1024 * 1024);// , FILE_STORAGE_PATH);

            ptrTree.init<T::DataNodeType>();


            int_test<MyStore>(&ptrTree, 50000);
        }
        //return;
//        {
//            using T = BEpsilonStoreTraits <
//#ifdef __PROD__
//                __FANOUT_PROD_ENV__,
//#endif //__PROD__
//                int,              // Key type
//                int,              // Value type
//                TYPE_UID::DATA_NODE_INT_INT,
//                TYPE_UID::INDEX_NODE_INT_INT,
//                ObjectFatUID,     // Object UID type
//                LRUCacheObject,
//                LRUCache,
//                FileStorage
//            >;
//
//            using MyStore = typename T::StoreType;
//            MyStore ptrTree(nDegree, 100, 32, 4ULL * 1024 * 1024 * 1024, FILE_STORAGE_PATH);
//
//            ptrTree.template init<T::DataNodeType>();
//
//            int_test<MyStore>(&ptrTree, 50000);
//        }
//        {
//            using T = BEpsilonStoreTraits <
//#ifdef __PROD__
//                __FANOUT_PROD_ENV__,
//#endif //__PROD__
//                int,              // Key type
//                int,              // Value type
//                TYPE_UID::DATA_NODE_INT_INT,
//                TYPE_UID::INDEX_NODE_INT_INT,
//                ObjectFatUID,     // Object UID type
//                LRUCacheObject,
//                LRUCache,
//                PMemStorage
//            >;
//
//#ifndef _MSC_VER
//            using MyStore = typename T::StoreType;
//            MyStore ptrTree(nDegree, 100, 32, 25ULL * 1024 * 1024 * 1024, PMEM_STORAGE_PATH);
//            ptrTree.template init<T::DataNodeType>();
//
//            int_test<MyStore>(&ptrTree, 500000);
//#endif //_MSC_VER
//        }
        {
            //typedef int KeyType;
            //typedef int ValueType;
            //typedef ObjectFatUID ObjectUIDType;

            //typedef DataNodeROpt<KeyType, ValueType, ObjectUIDType, TYPE_UID::DATA_NODE_INT_INT> DataNodeType;
            //typedef IndexNodeROpt<KeyType, ValueType, ObjectUIDType, DataNodeType, TYPE_UID::INDEX_NODE_INT_INT> IndexNodeType;

            //typedef LRUCacheObject<TypeMarshaller, DataNodeType, IndexNodeType> ObjectType;
            //typedef IFlushCallback<ObjectUIDType, ObjectType> ICallback;

            //typedef BPlusStore<ICallback, KeyType, ValueType, LRUCache<ICallback, VolatileStorage<ICallback, ObjectUIDType, LRUCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreType;
            //BPlusStoreType ptrTree(nDegree, 100, 32, 2ULL * 1024 * 1024 * 1024);
            //ptrTree.template init<DataNodeType>();

            //int_test<BPlusStoreType>(&ptrTree, 100000);
        }
        {
            //typedef int KeyType;
            //typedef int ValueType;
            //typedef ObjectFatUID ObjectUIDType;

            //typedef DataNodeROpt<KeyType, ValueType, ObjectUIDType, TYPE_UID::DATA_NODE_INT_INT> DataNodeType;
            //typedef IndexNodeROpt<KeyType, ValueType, ObjectUIDType, DataNodeType, TYPE_UID::INDEX_NODE_INT_INT> IndexNodeType;

            //typedef LRUCacheObject<TypeMarshaller, DataNodeType, IndexNodeType> ObjectType;
            //typedef IFlushCallback<ObjectUIDType, ObjectType> ICallback;

            //typedef BPlusStore<ICallback, KeyType, ValueType, LRUCache<ICallback, FileStorage<ICallback, ObjectUIDType, LRUCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreType;
            //BPlusStoreType ptrTree(nDegree, 100, 32, 2ULL * 1024 * 1024 * 1024, FILE_STORAGE_PATH);
            //ptrTree.init<DataNodeType>();

            //int_test<BPlusStoreType>(&ptrTree, 100000);
        }
        {
            //typedef int KeyType;
            //typedef int ValueType;
            //typedef ObjectFatUID ObjectUIDType;

            //typedef DataNodeROpt<KeyType, ValueType, ObjectUIDType, TYPE_UID::DATA_NODE_INT_INT> DataNodeType;
            //typedef IndexNodeROpt<KeyType, ValueType, ObjectUIDType, DataNodeType, TYPE_UID::INDEX_NODE_INT_INT> IndexNodeType;

            //typedef LRUCacheObject<TypeMarshaller, DataNodeType, IndexNodeType> ObjectType;
            //typedef IFlushCallback<ObjectUIDType, ObjectType> ICallback;

            //typedef BPlusStore<ICallback, KeyType, ValueType, LRUCache<ICallback, PMemStorage<ICallback, ObjectUIDType, LRUCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreType;
#ifndef _MSC_VER
            //BPlusStoreType ptrTree(nDegree, 100, 32, 2ULL * 1024 * 1024 * 1024, PMEM_STORAGE_PATH);
            //ptrTree.init<DataNodeType>();
            //int_test<BPlusStoreType>(&ptrTree, 1000000);
#endif //_MSC_VER
        }
#endif //__TREE_WITH_CACHE__

        std::cout << std::endl;
    }
}

void test_for_string()
{
    for (size_t nDegree = 3; nDegree < 40; nDegree++)
    {
        std::cout << ">>>>> Running 'test_for_string' for nDegree:" << nDegree << std::endl;

#ifndef __TREE_WITH_CACHE__
        {
            using T = BPlusStoreTraits<
                CHAR16,              // Key type
                CHAR16,              // Value type
                TYPE_UID::DATA_NODE_INT_INT,
                TYPE_UID::INDEX_NODE_INT_INT,
                uintptr_t,     // Object UID type
                NoCacheObject,
                NoCache            >;

            using MyStore = typename T::StoreType;
            MyStore ptrTree(nDegree);

            ptrTree.template init<T::DataNodeType>();

            string_test<MyStore>(&ptrTree, nDegree, 100000);
        }
#else //__TREE_WITH_CACHE__
        {
            using T = BPlusStoreTraits <
#ifdef __PROD__
                __FANOUT_PROD_ENV__,
#endif //__PROD__
                CHAR16,              // Key type
                CHAR16,              // Value type
                TYPE_UID::DATA_NODE_INT_INT,
                TYPE_UID::INDEX_NODE_INT_INT,
                ObjectFatUID,     // Object UID type
                LRUCacheObject,
                LRUCache,
                VolatileStorage
            >;

            using MyStore = typename T::StoreType;
            MyStore ptrTree(nDegree, 100, 32, 4ULL * 1024 * 1024 * 1024);

            ptrTree.template init<T::DataNodeType>();

            string_test<MyStore>(&ptrTree, nDegree, 50000);
        }
        return;
//        {
//            using T = BPlusStoreTraits <
//#ifdef __PROD__
//                __FANOUT_PROD_ENV__,
//#endif //__PROD__
//                CHAR16,              // Key type
//                CHAR16,              // Value type
//                TYPE_UID::DATA_NODE_INT_INT,
//                TYPE_UID::INDEX_NODE_INT_INT,
//                ObjectFatUID,     // Object UID type
//                LRUCacheObject,
//                LRUCache,
//                FileStorage
//            >;
//
//            using MyStore = typename T::StoreType;
//            MyStore ptrTree(nDegree, 100, 32, 4ULL * 1024 * 1024 * 1024, FILE_STORAGE_PATH);
//
//            ptrTree.template init<T::DataNodeType>();
//
//            string_test<MyStore>(&ptrTree, nDegree, 50000);
//        }
//        {
//            using T = BPlusStoreTraits <
//#ifdef __PROD__
//                __FANOUT_PROD_ENV__,
//#endif //__PROD__
//                CHAR16,              // Key type
//                CHAR16,              // Value type
//                TYPE_UID::DATA_NODE_INT_INT,
//                TYPE_UID::INDEX_NODE_INT_INT,
//                ObjectFatUID,     // Object UID type
//                LRUCacheObject,
//                LRUCache,
//                PMemStorage
//            >;
//
//#ifndef _MSC_VER
//            using MyStore = typename T::StoreType;
//            MyStore ptrTree(nDegree, 1000, 32, 25ULL * 1024 * 1024 * 1024, PMEM_STORAGE_PATH);
//
//            ptrTree.template init<T::DataNodeType>();
//
//            string_test<MyStore>(&ptrTree, nDegree, 50000);
//#endif //_MSC_VER
//        }
        {
            //typedef CHAR16 KeyType;
            //typedef CHAR16 ValueType;
            //typedef ObjectFatUID ObjectUIDType;

            //typedef DataNodeROpt<KeyType, ValueType, ObjectUIDType, TYPE_UID::DATA_NODE_STRING_STRING> DataNodeType;
            //typedef IndexNodeROpt<KeyType, ValueType, ObjectUIDType, DataNodeType, TYPE_UID::INDEX_NODE_INT_INT> IndexNodeType;

            //typedef LRUCacheObject<TypeMarshaller, DataNodeType, IndexNodeType> ObjectType;
            //typedef IFlushCallback<ObjectUIDType, ObjectType> ICallback;

            //typedef BPlusStore<ICallback, KeyType, ValueType, LRUCache<ICallback, VolatileStorage<ICallback, ObjectUIDType, LRUCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreType;
            //BPlusStoreType ptrTree(nDegree, 100, 32, 2ULL * 1024 * 1024 * 1024);
            //ptrTree.template init<DataNodeType>();

            //int_test<BPlusStoreType>(&ptrTree, 100000);
        }
        {
            //typedef CHAR16 KeyType;
            //typedef CHAR16 ValueType;
            //typedef ObjectFatUID ObjectUIDType;

            //typedef DataNodeROpt<KeyType, ValueType, ObjectUIDType, TYPE_UID::DATA_NODE_STRING_STRING> DataNodeType;
            //typedef IndexNodeROpt<KeyType, ValueType, ObjectUIDType, DataNodeType, TYPE_UID::INDEX_NODE_INT_INT> IndexNodeType;

            //typedef LRUCacheObject<TypeMarshaller, DataNodeType, IndexNodeType> ObjectType;
            //typedef IFlushCallback<ObjectUIDType, ObjectType> ICallback;

            //typedef BPlusStore<ICallback, KeyType, ValueType, LRUCache<ICallback, FileStorage<ICallback, ObjectUIDType, LRUCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreType;
            //BPlusStoreType ptrTree(nDegree, 100, 32, 2ULL * 1024 * 1024 * 1024, FILE_STORAGE_PATH);
            //ptrTree.init<DataNodeType>();

            //int_test<BPlusStoreType>(&ptrTree, 100000);
        }
        {
            //typedef CHAR16 KeyType;
            //typedef CHAR16 ValueType;
            //typedef ObjectFatUID ObjectUIDType;

            //typedef DataNodeROpt<KeyType, ValueType, ObjectUIDType, TYPE_UID::DATA_NODE_STRING_STRING> DataNodeType;
            //typedef IndexNodeROpt<KeyType, ValueType, ObjectUIDType, DataNodeType, TYPE_UID::INDEX_NODE_INT_INT> IndexNodeType;

            //typedef LRUCacheObject<TypeMarshaller, DataNodeType, IndexNodeType> ObjectType;
            //typedef IFlushCallback<ObjectUIDType, ObjectType> ICallback;

            //typedef BPlusStore<ICallback, KeyType, ValueType, LRUCache<ICallback, PMemStorage<ICallback, ObjectUIDType, LRUCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreType;
#ifndef _MSC_VER
            //BPlusStoreType ptrTree(nDegree, 100, 32, 2ULL * 1024 * 1024 * 1024, PMEM_STORAGE_PATH);
            //ptrTree.init<DataNodeType>();
            //int_test<BPlusStoreType>(&ptrTree, 1000000);
#endif //_MSC_VER
        }
#endif //__TREE_WITH_CACHE__
        std::cout << std::endl;
    }
}

void test_for_threaded()
{
#ifdef __CONCURRENT__
    for (size_t nDegree = 3; nDegree < 100; nDegree = nDegree + 1)
    {
        std::cout << ">>>>> Running 'test_for_threaded' for nDegree:" << nDegree << std::endl;
        //continue;
#ifndef __TREE_WITH_CACHE__
        {
            using T = BPlusStoreTraits<
                int,              // Key type
                int,              // Value type
                TYPE_UID::DATA_NODE_INT_INT,
                TYPE_UID::INDEX_NODE_INT_INT,
                uintptr_t,     // Object UID type
                NoCacheObject,
                NoCache            >;

            using MyStore = typename T::StoreType;
            MyStore ptrTree(nDegree);

            ptrTree.template init<T::DataNodeType>();

            threaded_test<MyStore>(&ptrTree, nDegree, 1000000, 12);
        }
#else //__TREE_WITH_CACHE__
        {
        using T = BPlusStoreTraits <
#ifdef __PROD__
            __FANOUT_PROD_ENV__,
#endif //__PROD__
            int,              // Key type
            int,              // Value type
            TYPE_UID::DATA_NODE_INT_INT,
            TYPE_UID::INDEX_NODE_INT_INT,
            ObjectFatUID,     // Object UID type
            CLOCKCacheObject,
            CLOCKCache,
#ifdef _MSC_VER
            VolatileStorage
#else //_MSC_VER
            VolatileStorage
#endif //_MSC_VER
        >;

            using MyStore = typename T::StoreType;
            MyStore ptrTree(nDegree, 5000, 2048, 1ULL * 1024 * 1024 * 1024);// , FILE_STORAGE_PATH);

            ptrTree.init<T::DataNodeType>();

            threaded_test<MyStore>(&ptrTree, nDegree, 500000, 10);

            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
        //return;
//        {
//            using T = BEpsilonStoreTraits <
//#ifdef __PROD__
//                __FANOUT_PROD_ENV__,
//#endif //__PROD__
//                int,              // Key type
//                int,              // Value type
//                TYPE_UID::DATA_NODE_INT_INT,
//                TYPE_UID::INDEX_NODE_INT_INT,
//                ObjectFatUID,     // Object UID type
//                A2QCacheObject,
//                A2QCache,
//                FileStorage
//            >;
//
//            using MyStore = typename T::StoreType;
//            MyStore ptrTree(nDegree, 100, 32, 4ULL * 1024 * 1024 * 1024, FILE_STORAGE_PATH);
//
//            ptrTree.template init<T::DataNodeType>();
//
//            threaded_test<MyStore>(&ptrTree, nDegree, 10000, 12);
//        }
//        {
//            using T = BEpsilonStoreTraits <
//#ifdef __PROD__
//                __FANOUT_PROD_ENV__,
//#endif //__PROD__
//                int,              // Key type
//                int,              // Value type
//                TYPE_UID::DATA_NODE_INT_INT,
//                TYPE_UID::INDEX_NODE_INT_INT,
//                ObjectFatUID,     // Object UID type
//                LRUCacheObject,
//                LRUCache,
//                PMemStorage
//            >;
//
//#ifndef _MSC_VER
//            using MyStore = typename T::StoreType;
//            MyStore ptrTree(nDegree, 100, 32, 25ULL * 1024 * 1024 * 1024, PMEM_STORAGE_PATH);
//
//            ptrTree.template init<T::DataNodeType>();
//
//            threaded_test<MyStore>(&ptrTree, nDegree, 1000000, 12);
//#endif //_MSC_VER
//        }
        {
            /*typedef int KeyType;
            typedef int ValueType;
            typedef ObjectFatUID ObjectUIDType;

            typedef DataNodeROpt<KeyType, ValueType, ObjectUIDType, TYPE_UID::DATA_NODE_INT_INT> DataNodeType;
            typedef IndexNodeROpt<KeyType, ValueType, ObjectUIDType, DataNodeType, TYPE_UID::INDEX_NODE_INT_INT> IndexNodeType;

            typedef LRUCacheObject<TypeMarshaller, DataNodeType, IndexNodeType> ObjectType;
            typedef IFlushCallback<ObjectUIDType, ObjectType> ICallback;


            typedef BPlusStore<ICallback, KeyType, ValueType, LRUCache<ICallback, VolatileStorage<ICallback, ObjectUIDType, LRUCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreType;
            BPlusStoreType ptrTree(nDegree, 100, 32, 2ULL * 1024 * 1024 * 1024);
            ptrTree.template init<DataNodeType>();

            threaded_test<BPlusStoreType>(&ptrTree, nDegree, 1000000, 12);*/
        }
        {
            /*typedef int KeyType;
            typedef int ValueType;
            typedef ObjectFatUID ObjectUIDType;

            typedef DataNodeROpt<KeyType, ValueType, ObjectUIDType, TYPE_UID::DATA_NODE_INT_INT> DataNodeType;
            typedef IndexNodeROpt<KeyType, ValueType, ObjectUIDType, DataNodeType, TYPE_UID::INDEX_NODE_INT_INT> IndexNodeType;

            typedef LRUCacheObject<TypeMarshaller, DataNodeType, IndexNodeType> ObjectType;
            typedef IFlushCallback<ObjectUIDType, ObjectType> ICallback;

            typedef BPlusStore<ICallback, KeyType, ValueType, LRUCache<ICallback, FileStorage<ICallback, ObjectUIDType, LRUCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreType;
            BPlusStoreType ptrTree(nDegree, 100, 32, 2ULL * 1024 * 1024 * 1024, FILE_STORAGE_PATH);
            ptrTree.template init<DataNodeType>();

            threaded_test<BPlusStoreType>(&ptrTree, nDegree, 1000000, 12);*/
        }
        {
            /* typedef int KeyType;
             typedef int ValueType;
             typedef ObjectFatUID ObjectUIDType;

             typedef DataNodeROpt<KeyType, ValueType, ObjectUIDType, TYPE_UID::DATA_NODE_INT_INT> DataNodeType;
             typedef IndexNodeROpt<KeyType, ValueType, ObjectUIDType, DataNodeType, TYPE_UID::INDEX_NODE_INT_INT> IndexNodeType;

             typedef LRUCacheObject<TypeMarshaller, DataNodeType, IndexNodeType> ObjectType;
             typedef IFlushCallback<ObjectUIDType, ObjectType> ICallback;

             typedef BPlusStore<ICallback, KeyType, ValueType, LRUCache<ICallback, PMemStorage<ICallback, ObjectUIDType, LRUCacheObject, TypeMarshaller, DataNodeType, IndexNodeType>>> BPlusStoreType;*/
#ifndef _MSC_VER
             //BPlusStoreType ptrTree(nDegree, 100, 32, 2ULL * 1024 * 1024 * 1024, PMEM_STORAGE_PATH);
             //ptrTree.template init<DataNodeType>();
             //threaded_test<BPlusStoreType>(&ptrTree, nDegree, 1000000, 6);
#endif //_MSC_VER
        }
#endif //__TREE_WITH_CACHE__

        std::cout << std::endl;
    }
#endif //__CONCURRENT__
}

void quick_test()
{
    for (size_t idx = 0; idx < 1; idx++) 
    {
        //test_for_ints();
        //test_for_string();
        test_for_threaded();
    }
}
//
//template <typename BPlusStoreType, typename KeyType, typename ValueType>
//void fptree_test(BPlusStoreType* ptrTree, size_t nMaxNumber, vector<KeyType>& vtData)
//{
//    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
//
//    for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr++)
//    {
//        ErrorCode ec = ptrTree->insert(vtData[nCntr], 0);
//        ASSERT(ec == ErrorCode::Success);
//    }
//
//    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
//    std::cout
//        << ">> insert [Time: "
//        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
//        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
//        << std::endl;
//
//    std::this_thread::sleep_for(std::chrono::seconds(2));
//    /*
//    #ifdef __TREE_WITH_CACHE__
//        begin = std::chrono::steady_clock::now();
//
//        ptrTree->flush();
//
//        end = std::chrono::steady_clock::now();
//        std::cout
//            << ">> flush [Time: "
//            << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
//            << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
//            << std::endl;
//
//        std::this_thread::sleep_for(std::chrono::seconds(2));
//    #endif //__TREE_WITH_CACHE__
//    */
//    begin = std::chrono::steady_clock::now();
//
//    for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr++)
//    {
//        ValueType value;
//        ErrorCode ec = ptrTree->search(vtData[nCntr], value);
//
//        ASSERT(ec == ErrorCode::Success);
//    }
//
//    end = std::chrono::steady_clock::now();
//    std::cout
//        << ">> search [Time: "
//        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
//        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
//        << std::endl;
//
//    std::this_thread::sleep_for(std::chrono::seconds(2));
//
//    begin = std::chrono::steady_clock::now();
//
//    for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr++)
//    {
//        ErrorCode ec = ptrTree->remove(vtData[nCntr]);
//
//        ASSERT(ec == ErrorCode::Success);
//    }
//
//    end = std::chrono::steady_clock::now();
//    std::cout
//        << ">> delete [Time: "
//        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
//        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
//        << std::endl;
//}
//
//template <typename A, typename k, typename v, typename dnt, typename uidt>
//void fptree_bm_int(size_t nMaxNumber = 5000)
//{
//
//#ifdef _MSC_VER
//    std::vector<int64_t> vtData(nMaxNumber);
//    GENERATE_RANDOM_NUMBER_ARRAY(0, nMaxNumber, vtData);
//#else //_MSC_VER
//    std::vector<int64_t> vtData(nMaxNumber);
//    GENERATE_RANDOM_NUMBER_ARRAY(0, nMaxNumber, vtData);
//
//    //std::ifstream file("/home/skarim/Reproducibility/benchmarks/microbenchmarks/values_int.dat");
//
//    //std::vector<int64_t> vtData;
//    //std::string line;
//
//    //while (std::getline(file, line))
//    //{
//    //    vtData.push_back(std::stoi(line));
//    //}
//#endif //_MSC_VER
//
//#ifndef __TREE_WITH_CACHE__
//    std::vector<size_t> degrees = { 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64, 68, 72, 76, 80 };
//    for (size_t nDegree : degrees)
//    {
//        std::cout << "volatile storage.." << std::endl;
//        {
//            std::cout << "Order = " << nDegree << std::endl;
//
//            using MyStore = A;
//
//            for (size_t nCntr = 0; nCntr < 1; nCntr++)
//            {
//                MyStore ptrTree(nDegree);
//
//                ptrTree.template init<dnt>();
//
//                std::cout << "Iteration = " << nCntr + 1 << std::endl;
//                fptree_test<MyStore, k, v>(&ptrTree, nMaxNumber, vtData);
//                std::this_thread::sleep_for(std::chrono::seconds(10));
//            }
//        }
//    }
//#else //__TREE_WITH_CACHE__
//#ifndef __CONCURRENT__
//
//    std::vector<size_t> degrees = { 64 , 92, 110, 128, 256-48, 256-32, 256-16, 256, 256+16, 256+32 };
//    for (size_t nDegree : degrees)
//    {
//        std::cout << "volatile storage.." << std::endl;
//        {
//            using MyStore = A;
//
//            size_t nInternalNodeSize = (nDegree - 1) * sizeof(v) + nDegree * sizeof(uidt) + sizeof(uint64_t);
//            size_t nTotalInternalNodes = nMaxNumber / nDegree;
//            //size_t nMemoryOfNodes = nTotalNodes * nNodeSize;
//            //size_t nMemoryOfData = nMaxNumber * sizeof(KeyType);
//            size_t nTotalMemory = nTotalInternalNodes * nInternalNodeSize;
//            //double nTotalMemoryInMB = nTotalMemory / (1024.0 * 1024.0);
//
//            size_t nBlockSize = nInternalNodeSize > 2048 ? 8192 : 2048;
//
//            std::cout
//                << "Order = " << nDegree
//                << ", Total IN (n) = " << nTotalInternalNodes
//                << ", Total Memory (Bytes) = " << nTotalMemory
//                << ", InternalNode Size = " << nInternalNodeSize
//                << ", Block Size = " << nBlockSize
//                << std::endl;
//
//            for (size_t nCntr = 0; nCntr < 2; nCntr++)
//            {
//                MyStore ptrTree(nDegree, nTotalInternalNodes, nBlockSize, 16ULL * 1024 * 1024 * 1024);
//                ptrTree.template init<dnt>();
//
//                std::cout << "Iteration = " << nCntr + 1 << std::endl;
//                fptree_test<MyStore, k, v>(&ptrTree, nMaxNumber, vtData);
//                std::this_thread::sleep_for(std::chrono::seconds(10));
//            }
//        }
//
//        //#ifndef _MSC_VER
//        //            std::cout << "pmem storage.." << std::endl;
//        //            {
//        //                using T = BPlusStoreTraits<
//        //                    int64_t,              // Key type
//        //                    int64_t,              // Value type
//        //                    TYPE_UID::DATA_NODE_INT_INT,
//        //                    TYPE_UID::INDEX_NODE_INT_INT,
//        //                    ObjectFatUID,     // Object UID type
//        //                    LRUCacheObject,
//        //                    LRUCache,
//        //                    PMemStorage
//        //                >;
//        //
//        //                using MyStore = typename T::StoreType;
//        //
//        //                size_t nInternalNodeSize = (nDegree - 1) * sizeof(T::ValueType) + nDegree * sizeof(T::ObjectUIDType) + sizeof(int*);
//        //                size_t nTotalInternalNodes = nMaxNumber / nDegree;
//        //                //size_t nMemoryOfNodes = nTotalNodes * nNodeSize;
//        //                //size_t nMemoryOfData = nMaxNumber * sizeof(KeyType);
//        //                size_t nTotalMemory = nTotalInternalNodes * nInternalNodeSize;
//        //                size_t nTotalMemoryInMB = nTotalMemory / (1024 * 1024);
//        //
//        //                size_t nBlockSize = nInternalNodeSize > 256 ? 256 : 128;
//        //
//        //                std::cout
//        //                    << "Order = " << nDegree
//        //                    << ", Total IN (n) = " << nTotalInternalNodes
//        //                    << ", Total Memory (MB) = " << nTotalMemoryInMB
//        //                    << ", Block Size = " << nBlockSize
//        //                    << std::endl;
//        //
//        //                for (size_t nCntr = 0; nCntr < 2; nCntr++)
//        //                {
//        //                    MyStore ptrTree(nDegree, nTotalInternalNodes, nBlockSize, 120ULL * 1024 * 1024 * 1024, PMEM_STORAGE_PATH_II);
//        //                    ptrTree.init<T::DataNodeType>();
//        //
//        //                    std::cout << "Iteration = " << nCntr + 1 << std::endl;
//        //                    fptree_test<MyStore, T::KeyType, T::ValueType>(&ptrTree, nMaxNumber, vtData);
//        //                    std::this_thread::sleep_for(std::chrono::seconds(2));
//        //                }
//        //            }
//        //#endif _MSC_VER
//        //
//        //            std::cout << "file storage.." << std::endl;
//        //            {
//        //                using T = BPlusStoreTraits<
//        //#ifdef __PROD__
//        //                    __FANOUT_PROD_ENV__,
//        //#endif //__PROD__
//        //                    int64_t,              // Key type
//        //                    int64_t,              // Value type
//        //                    TYPE_UID::DATA_NODE_INT_INT,
//        //                    TYPE_UID::INDEX_NODE_INT_INT,
//        //                    ObjectFatUID,     // Object UID type
//        //                    LRUCacheObject,
//        //                    LRUCache,
//        //                    FileStorage
//        //                >;
//        //
//        //                using MyStore = typename T::StoreType;
//        //
//        //                size_t nInternalNodeSize = (nDegree - 1) * sizeof(T::ValueType) + nDegree * sizeof(T::ObjectUIDType) + sizeof(int*);
//        //                size_t nTotalInternalNodes = nMaxNumber / nDegree;
//        //                //size_t nMemoryOfNodes = nTotalNodes * nNodeSize;
//        //                //size_t nMemoryOfData = nMaxNumber * sizeof(KeyType);
//        //                size_t nTotalMemory = nTotalInternalNodes * nInternalNodeSize;
//        //                size_t nTotalMemoryInMB = nTotalMemory / (1024 * 1024);
//        //
//        //                size_t nBlockSize = nInternalNodeSize > 256 ? 256 : 128;
//        //
//        //                std::cout
//        //                    << "Order = " << nDegree
//        //                    << ", Total IN (n) = " << nTotalInternalNodes
//        //                    << ", Total Memory (MB) = " << nTotalMemoryInMB
//        //                    << ", Block Size = " << nBlockSize
//        //                    << std::endl;
//        //
//        //                for (size_t nCntr = 0; nCntr < 2; nCntr++)
//        //                {
//        //                    MyStore ptrTree(nDegree, nTotalInternalNodes, nBlockSize, 120ULL * 1024 * 1024 * 1024, PMEM_STORAGE_PATH_II);
//        //                    ptrTree.init<T::DataNodeType>();
//        //
//        //                    std::cout << "Iteration = " << nCntr + 1 << std::endl;
//        //                    fptree_test<MyStore, T::KeyType, T::ValueType>(&ptrTree, nMaxNumber, vtData);
//        //                    std::this_thread::sleep_for(std::chrono::seconds(2));
//        //                }
//        //            }
//        //
//        //            std::cout << std::endl;
//    }
//#else //__CONCURRENT__
//        // TODO.
//#endif //__CONCURRENT__
//#endif //__TREE_WITH_CACHE__
//}
//
//void fptree_bm_string(size_t nMaxNumber = 5000)
//{
//#ifdef _MSC_VER
//    std::vector<CHAR16> vtData;
//    GENERATE_RANDOM_CHAR_ARRAY(sizeof(CHAR16), nMaxNumber, vtData);
//#else //_MSC_VER
//    std::ifstream file("/home/skarim/Reproducibility/benchmarks/microbenchmarks/values_string.dat");
//
//    std::vector<CHAR16> vtData;
//    std::string line;
//
//    while (std::getline(file, line))
//    {
//        CHAR16 itm;
//        std::memcpy(&itm.data, line.c_str(), 15);
//        itm.data[15] = '\0';
//        vtData.push_back(itm);
//    }
//#endif _MSC_VER
//
//#ifndef __TREE_WITH_CACHE__
//
//#else //__TREE_WITH_CACHE__
//#ifndef __CONCURRENT__
//
//    for (size_t nDegree = 16; nDegree < 4000; nDegree = nDegree + 10)
//    {
//        std::cout << "volatile storage.." << std::endl;
//        {
//            using T = BPlusStoreTraits<
//#ifdef __PROD__
//                __FANOUT_PROD_ENV__,
//#endif //__PROD__
//                CHAR16,              // Key type
//                int,              // Value type
//                TYPE_UID::DATA_NODE_INT_INT,
//                TYPE_UID::INDEX_NODE_INT_INT,
//                ObjectFatUID,     // Object UID type
//                LRUCacheObject,
//                LRUCache,
//                VolatileStorage
//            >;
//
//            using MyStore = typename T::StoreType;
//
//            size_t nInternalNodeSize = (nDegree - 1) * sizeof(T::ValueType) + nDegree * sizeof(T::ObjectUIDType) + sizeof(int*);
//            size_t nTotalInternalNodes = nMaxNumber / nDegree;
//            //size_t nMemoryOfNodes = nTotalNodes * nNodeSize;
//            //size_t nMemoryOfData = nMaxNumber * sizeof(KeyType);
//            size_t nTotalMemory = nTotalInternalNodes * nInternalNodeSize;
//            size_t nTotalMemoryInMB = nTotalMemory / (1024 * 1024);
//
//            size_t nBlockSize = nInternalNodeSize > 256 ? 256 : 128;
//
//            std::cout
//                << "Order = " << nDegree
//                << ", Total IN (n) = " << nTotalInternalNodes
//                << ", Total Memory (MB) = " << nTotalMemoryInMB
//                << ", Block Size = " << nBlockSize
//                << std::endl;
//
//            for (size_t nCntr = 0; nCntr < 2; nCntr++)
//            {
//                MyStore ptrTree(nDegree, nTotalInternalNodes, nBlockSize, 10ULL * 1024 * 1024 * 1024);
//                ptrTree.init<T::DataNodeType>();
//
//                std::cout << "Iteration = " << nCntr + 1 << std::endl;
//                fptree_test<MyStore, T::KeyType, T::ValueType>(&ptrTree, nMaxNumber, vtData);
//                std::this_thread::sleep_for(std::chrono::seconds(10));
//            }
//        }
//
//#ifndef _MSC_VER
//        std::cout << "pmem storage.." << std::endl;
//        {
//            using T = BPlusStoreTraits<
//                CHAR16,              // Key type
//                int,              // Value type
//                TYPE_UID::DATA_NODE_INT_INT,
//                TYPE_UID::INDEX_NODE_INT_INT,
//                ObjectFatUID,     // Object UID type
//                LRUCacheObject,
//                LRUCache,
//                PMemStorage
//            >;
//
//            using MyStore = typename T::StoreType;
//
//            size_t nInternalNodeSize = (nDegree - 1) * sizeof(T::ValueType) + nDegree * sizeof(T::ObjectUIDType) + sizeof(int*);
//            size_t nTotalInternalNodes = nMaxNumber / nDegree;
//            //size_t nMemoryOfNodes = nTotalNodes * nNodeSize;
//            //size_t nMemoryOfData = nMaxNumber * sizeof(KeyType);
//            size_t nTotalMemory = nTotalInternalNodes * nInternalNodeSize;
//            size_t nTotalMemoryInMB = nTotalMemory / (1024 * 1024);
//
//            size_t nBlockSize = nInternalNodeSize > 256 ? 256 : 128;
//
//            std::cout
//                << "Order = " << nDegree
//                << ", Total IN (n) = " << nTotalInternalNodes
//                << ", Total Memory (MB) = " << nTotalMemoryInMB
//                << ", Block Size = " << nBlockSize
//                << std::endl;
//
//            for (size_t nCntr = 0; nCntr < 2; nCntr++)
//            {
//                MyStore ptrTree(nDegree, nTotalInternalNodes, nBlockSize, 120ULL * 1024 * 1024 * 1024, PMEM_STORAGE_PATH_II);
//                ptrTree.init<T::DataNodeType>();
//
//                std::cout << "Iteration = " << nCntr + 1 << std::endl;
//                fptree_test<MyStore, T::KeyType, T::ValueType>(&ptrTree, nMaxNumber, vtData);
//                std::this_thread::sleep_for(std::chrono::seconds(10));
//            }
//        }
//#endif _MSC_VER
//
//        std::cout << "file storage.." << std::endl;
//        {
//            using T = BPlusStoreTraits<
//#ifdef __PROD__
//                __FANOUT_PROD_ENV__,
//#endif //__PROD__
//                CHAR16,              // Key type
//                int,              // Value type
//                TYPE_UID::DATA_NODE_INT_INT,
//                TYPE_UID::INDEX_NODE_INT_INT,
//                ObjectFatUID,     // Object UID type
//                LRUCacheObject,
//                LRUCache,
//                FileStorage
//            >;
//
//            using MyStore = typename T::StoreType;
//
//            size_t nInternalNodeSize = (nDegree - 1) * sizeof(T::ValueType) + nDegree * sizeof(T::ObjectUIDType) + sizeof(int*);
//            size_t nTotalInternalNodes = nMaxNumber / nDegree;
//            //size_t nMemoryOfNodes = nTotalNodes * nNodeSize;
//            //size_t nMemoryOfData = nMaxNumber * sizeof(KeyType);
//            size_t nTotalMemory = nTotalInternalNodes * nInternalNodeSize;
//            size_t nTotalMemoryInMB = nTotalMemory / (1024 * 1024);
//
//            size_t nBlockSize = nInternalNodeSize > 256 ? 256 : 128;
//
//            std::cout
//                << "Order = " << nDegree
//                << ", Total IN (n) = " << nTotalInternalNodes
//                << ", Total Memory (MB) = " << nTotalMemoryInMB
//                << ", Block Size = " << nBlockSize
//                << std::endl;
//
//            for (size_t nCntr = 0; nCntr < 2; nCntr++)
//            {
//                MyStore ptrTree(nDegree, nTotalInternalNodes, nBlockSize, 120ULL * 1024 * 1024 * 1024, PMEM_STORAGE_PATH_II);
//                ptrTree.init<T::DataNodeType>();
//
//                std::cout << "Iteration = " << nCntr + 1 << std::endl;
//                fptree_test<MyStore, T::KeyType, T::ValueType>(&ptrTree, nMaxNumber, vtData);
//                std::this_thread::sleep_for(std::chrono::seconds(10));
//            }
//        }
//
//        std::cout << std::endl;
//    }
//#else //__CONCURRENT__
//    // TODO.
//#endif //__CONCURRENT__
//#endif //__TREE_WITH_CACHE__
//}
//
//
//void _stxtree(size_t nMaxNumber = 5000)
//{
//    std::vector<int64_t> vtData(nMaxNumber);
//    GENERATE_RANDOM_NUMBER_ARRAY(0, nMaxNumber, vtData);
//
//#ifndef _MSC_VER
//    tlx::btree_map<uint64_t, uint64_t> my_map;
//
//    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
//
//    for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr++)
//    {
//        my_map[vtData[nCntr]] = 0;
//    }
//
//    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
//    std::cout
//        << ">> insert [Time: "
//        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
//        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
//        << std::endl;
//
//    std::this_thread::sleep_for(std::chrono::seconds(2));
//
//    begin = std::chrono::steady_clock::now();
//
//    for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr++)
//    {
//        uint64_t value = my_map[vtData[nCntr]];
//    }
//
//    end = std::chrono::steady_clock::now();
//    std::cout
//        << ">> search [Time: "
//        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
//        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
//        << std::endl;
//
//    std::this_thread::sleep_for(std::chrono::seconds(2));
//
//    begin = std::chrono::steady_clock::now();
//
//    for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr++)
//    {
//        my_map.erase(vtData[nCntr]);
//    }
//
//    end = std::chrono::steady_clock::now();
//    std::cout
//        << ">> delete [Time: "
//        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
//        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
//        << std::endl;
//#endif //_MSC_VER
//}
//
//
//void fptree_bm_test(size_t nMaxNumber = 5000)
//{
//#ifdef __TREE_WITH_CACHE__
//    using DefaultBPlusTree = BPlusStoreTraits <
//        int64_t,              // Key type
//        int64_t,              // Value type
//        TYPE_UID::DATA_NODE_INT_INT,
//        TYPE_UID::INDEX_NODE_INT_INT,
//        ObjectFatUID,     // Object UID type
//        LRUCacheObject,
//        LRUCache,
//#ifdef _MSC_VER
//        VolatileStorage
//#else //_MSC_VER
//        VolatileStorage
//#endif //_MSC_VER
//    >;
//
//    using ROptBPlusTree = BPlusStoreROptTraits <
//        int64_t,              // Key type
//        int64_t,              // Value type
//        TYPE_UID::DATA_NODE_INT_INT,
//        TYPE_UID::INDEX_NODE_INT_INT,
//        ObjectFatUID,     // Object UID type
//        LRUCacheObject,
//        LRUCache,
//#ifdef _MSC_VER
//        VolatileStorage
//#else //_MSC_VER
//        VolatileStorage
//#endif //_MSC_VER
//    >;
//
//    using COptBPlusTree = BPlusStoreCOptTraits <
//        int64_t,              // Key type
//        int64_t,              // Value type
//        TYPE_UID::DATA_NODE_INT_INT,
//        TYPE_UID::INDEX_NODE_INT_INT,
//        ObjectFatUID,     // Object UID type
//        LRUCacheObject,
//        LRUCache,
//#ifdef _MSC_VER
//        VolatileStorage
//#else //_MSC_VER
//        VolatileStorage
//#endif //_MSC_VER
//    >;
//
//    using CROptBPlusTree = BPlusStoreCROptTraits <
//        int64_t,              // Key type
//        int64_t,              // Value type
//        TYPE_UID::DATA_NODE_INT_INT,
//        TYPE_UID::INDEX_NODE_INT_INT,
//        ObjectFatUID,     // Object UID type
//        LRUCacheObject,
//        LRUCache,
//#ifdef _MSC_VER
//        VolatileStorage
//#else //_MSC_VER
//        VolatileStorage
//#endif //_MSC_VER
//    >;
//
//
//
//    using BEpsilonTree = BEpsilonStoreTraits <
//        int64_t,              // Key type
//        int64_t,              // Value type
//        TYPE_UID::DATA_NODE_INT_INT,
//        TYPE_UID::INDEX_NODE_INT_INT,
//        ObjectFatUID,     // Object UID type
//        LRUCacheObject,
//        LRUCache,
//#ifdef _MSC_VER
//        VolatileStorage
//#else //_MSC_VER
//        VolatileStorage
//#endif //_MSC_VER
//    >;
//
//
//
//#else //__TREE_WITH_CACHE__
//    using DefaultBPlusTree = BPlusStoreTraits <
//        int64_t,              // Key type
//        int64_t,              // Value type
//        TYPE_UID::DATA_NODE_INT_INT,
//        TYPE_UID::INDEX_NODE_INT_INT,
//        uintptr_t,     // Object UID type
//        NoCacheObject,
//        NoCache
//    >;
//
//    using COptBPlusTree = BPlusStoreCOptTraits <
//        int64_t,              // Key type
//        int64_t,              // Value type
//        TYPE_UID::DATA_NODE_INT_INT,
//        TYPE_UID::INDEX_NODE_INT_INT,
//        uintptr_t,     // Object UID type
//        NoCacheObject,
//        NoCache
//    >;
//
//#endif //__TREE_WITH_CACHE__
//
//    /*    std::cout << "_stxtree start" << std::endl;
//    _stxtree(nMaxNumber);
//        std::cout << "_strtree end" << std::endl;
//    */
//#ifdef __TREE_WITH_CACHE__
//    //fptree_bm_int<BEpsilonTree::StoreType, BEpsilonTree::KeyType, BEpsilonTree::ValueType, BEpsilonTree::DataNodeType, BEpsilonTree::ObjectUIDType>(nMaxNumber);
//
//    fptree_bm_int<DefaultBPlusTree::StoreType, DefaultBPlusTree::KeyType, DefaultBPlusTree::ValueType, DefaultBPlusTree::DataNodeType, DefaultBPlusTree::ObjectUIDType>(nMaxNumber);
//    //fptree_bm_int<ROptBPlusTree::StoreType, ROptBPlusTree::KeyType, ROptBPlusTree::ValueType, ROptBPlusTree::DataNodeType, ROptBPlusTree::ObjectUIDType>(nMaxNumber);
//    //fptree_bm_int<COptBPlusTree::StoreType, COptBPlusTree::KeyType, COptBPlusTree::ValueType, COptBPlusTree::DataNodeType, COptBPlusTree::ObjectUIDType>(nMaxNumber);
//    //fptree_bm_int<CROptBPlusTree::StoreType, CROptBPlusTree::KeyType, CROptBPlusTree::ValueType, CROptBPlusTree::DataNodeType, CROptBPlusTree::ObjectUIDType>(nMaxNumber);
//#else //__TREE_WITH_CACHE__
//    fptree_bm_int<DefaultBPlusTree::StoreType, DefaultBPlusTree::KeyType, DefaultBPlusTree::ValueType, DefaultBPlusTree::DataNodeType, DefaultBPlusTree::ObjectUIDType>(nMaxNumber);
//    fptree_bm_int<COptBPlusTree::StoreType, COptBPlusTree::KeyType, COptBPlusTree::ValueType, COptBPlusTree::DataNodeType, COptBPlusTree::ObjectUIDType>(nMaxNumber);
//#endif //__TREE_WITH_CACHE__
//}

template <typename T>
void dumpVectorToFile(const std::vector<T>& vec, const std::string& filename)
{
    std::ofstream outFile(filename);
    if (!outFile)
    {
        std::cerr << "Error: Cannot open file " << filename << "\n";
        return;
    }

    for (const auto& item : vec)
    {
        outFile << item << '\n';
    }
}

template <typename T>
std::vector<T> readVectorFromFile(const std::string& filename)
{
    std::vector<T> vec;
    std::ifstream inFile(filename);

    if (!inFile)
    {
        std::cerr << "Error: Cannot open file " << filename << "\n";
        return vec;
    }

    T value;
    while (inFile >> value)
    {
        vec.push_back(value);
    }

    inFile.close();
    return vec;
}

void test(int i)
{
#ifdef __TREE_WITH_CACHE__
    using T = BPlusStoreTraits <
#ifdef __PROD__
        __FANOUT_PROD_ENV__,
#endif //__PROD__
        int,              // Key type
        int,              // Value type
        TYPE_UID::DATA_NODE_INT_INT,
        TYPE_UID::INDEX_NODE_INT_INT,
        ObjectFatUID,     // Object UID type
        CLOCKCacheObject,
        CLOCKCache,
#ifdef _MSC_VER
        VolatileStorage
#else //_MSC_VER
        VolatileStorage
#endif //_MSC_VER

    >;

    using MyStore = typename T::StoreType;
    MyStore ptrTree(i, 1000, 4096, 1ULL * 1024 * 1024 * 1024);// , FILE_STORAGE_PATH);

    ptrTree.init<T::DataNodeType>();
#else //__TREE_WITH_CACHE__
    using T = BPlusStoreTraits<
        int,              // Key type
        int,              // Value type
        TYPE_UID::DATA_NODE_INT_INT,
        TYPE_UID::INDEX_NODE_INT_INT,
        uintptr_t,     // Object UID type
        NoCacheObject,
        NoCache
    >;

    using MyStore = typename T::StoreType;
    MyStore ptrTree(3);

    ptrTree.init<T::DataNodeType>();
#endif //__TREE_WITH_CACHE__

    size_t nTotalEntries = 100000;
    std::vector<int> vtNumberData;
    GENERATE_RANDOM_NUMBER_ARRAY(0, nTotalEntries, vtNumberData);

#ifdef _MSC_VER
    dumpVectorToFile(vtNumberData, "C:\\vals");
    //vtNumberData = readVectorFromFile<int>("C:\\vals");
#else //_MSC_VER
    //dumpVectorToFile(vtNumberData, "/mnt/tmpfs/vals");
#endif //_MSC_VER

    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    //for (int nCntr = 0; nCntr < nTotalEntries; nCntr++)
    for (int nCntr = nTotalEntries - 1; nCntr >= 500; nCntr--)
    {
        ErrorCode ec = ptrTree.insert(vtNumberData[nCntr], vtNumberData[nCntr]);
        ASSERT(ec == ErrorCode::Success);
    }

    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout
        << ">> insert [Time: "
        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
        << std::endl;

    
    //std::ofstream out1("d:\\t1.txt");
    //ptrTree.print(out1);
    //out1.flush();
    //out1.close();
    

#ifdef __TREE_WITH_CACHE__
    begin = std::chrono::steady_clock::now();

    //ptrTree.flush();

    end = std::chrono::steady_clock::now();
    std::cout
        << ">> flush [Time: "
        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
        << std::endl;
#endif //__TREE_WITH_CACHE__

    begin = std::chrono::steady_clock::now();

    //for (int nCntr = 0; nCntr < nTotalEntries; nCntr++)
    for (int nCntr = nTotalEntries - 1; nCntr >= 500; nCntr--)
    {
        T::ValueType nValue = 0;
        ErrorCode ec = ptrTree.search(vtNumberData[nCntr], nValue);

        ASSERT(nValue == vtNumberData[nCntr]);
    }

    //for (int nCntr = 0; nCntr < nTotalEntries; nCntr++)
    for (int nCntr = 500 - 1; nCntr >= 0; nCntr--)
    {
        ErrorCode ec = ptrTree.insert(vtNumberData[nCntr], vtNumberData[nCntr]);
        ASSERT(ec == ErrorCode::Success);
    }

    begin = std::chrono::steady_clock::now();

    for (int nCntr = nTotalEntries - 1; nCntr >= 0; nCntr--)
        //for (size_t nCntr = 0; nCntr < nTotalEntries; nCntr++)
    {
        T::ValueType nValue = 0;
        ErrorCode ec = ptrTree.search(vtNumberData[nCntr], nValue);

        ASSERT(ec == ErrorCode::Success);
        ASSERT(nValue == vtNumberData[nCntr]);
    }

    end = std::chrono::steady_clock::now();
    std::cout
        << ">> search [Time: "
        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
        << std::endl;

#ifdef __TREE_WITH_CACHE__
    //ptrTree.flush();
#endif //__TREE_WITH_CACHE__

    for (int nCntr = nTotalEntries - 1; nCntr >= 0; nCntr--)
        //for (size_t nCntr = 0; nCntr < nTotalEntries; nCntr++)
    {
        T::ValueType nValue = 0;
        ErrorCode ec = ptrTree.search(vtNumberData[nCntr], nValue);

        ASSERT(ec == ErrorCode::Success);
        ASSERT(nValue == vtNumberData[nCntr]);
    }


    begin = std::chrono::steady_clock::now();
    for (int nCntr = nTotalEntries - 1; nCntr >= 0; nCntr--)
    {
        ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);

        ASSERT(ec == ErrorCode::Success);
    }

    end = std::chrono::steady_clock::now();
    std::cout
        << ">> delete [Time: "
        << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
        << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
        << std::endl;

    size_t nObjectsLinkedList = 0;
    size_t nObjectsInMap = 0;

#ifdef __TREE_WITH_CACHE__
    //ptrTree.getObjectsCountInCache(nObjectsLinkedList);
    //ASSERT(nObjectsLinkedList == 1);
#endif //__TREE_WITH_CACHE__

    for (int nCntr = nTotalEntries - 1; nCntr >= 0; nCntr--)
        //for (size_t nCntr = 0; nCntr < nTotalEntries; nCntr++)
    {
        T::ValueType nValue = 0;
        ErrorCode ec = ptrTree.search(vtNumberData[nCntr], nValue);

        ASSERT(ec == ErrorCode::KeyDoesNotExist);
    }

    std::cout << "End." << std::endl;
}


void test_ex(int degree)
{
    size_t nTotalRecords = 100000;
    //std::vector<int> vtNumberData;
    //GENERATE_RANDOM_NUMBER_ARRAY(0, nTotalRecords, vtNumberData);
#ifdef __TREE_WITH_CACHE__
    using T = BPlusStoreTraits <
#ifdef __PROD__
        __FANOUT_PROD_ENV__,
#endif //__PROD__
        int,              // Key type
        int,              // Value type
        TYPE_UID::DATA_NODE_INT_INT,
        TYPE_UID::INDEX_NODE_INT_INT,
        ObjectFatUID,     // Object UID type
        CLOCKCacheObject,
        CLOCKCache,
#ifdef _MSC_VER
        VolatileStorage
#else //_MSC_VER
        VolatileStorage
#endif //_MSC_VER
    >;

    using MyStore = typename T::StoreType;
    MyStore ptrTree(degree, 1000, 4096, 2ULL * 1024 * 1024 * 1024);// , FILE_STORAGE_PATH);

    ptrTree.init<T::DataNodeType>();
#else //__TREE_WITH_CACHE__
    using T = BPlusStoreTraits<
        int,              // Key type
        int,              // Value type
        TYPE_UID::DATA_NODE_INT_INT,
        TYPE_UID::INDEX_NODE_INT_INT,
        uintptr_t,     // Object UID type
        NoCacheObject,
        NoCache
    >;

    using MyStore = typename T::StoreType;
    MyStore ptrTree(24);

    ptrTree.init<T::DataNodeType>();
#endif //__TREE_WITH_CACHE__

    for (int nTestCntr = 0; nTestCntr < 1; nTestCntr++)
    {
        std::vector<int> vtNumberData(nTotalRecords);
        GENERATE_RANDOM_NUMBER_ARRAY(0, nTotalRecords, vtNumberData);

        dumpVectorToFile(vtNumberData, "C:\\vals");
        //vtNumberData = readVectorFromFile<int>("C:\\vals");

        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        //for (int nCntr = nTotalRecords - 1; nCntr >= 0; nCntr--)
        {
            ErrorCode ec = ptrTree.insert(vtNumberData[nCntr], vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }

        //ptrTree.flush();
        size_t nObjectsLinkedList = 0;
        size_t nObjectsInMap = 0;

//#ifdef __TREE_WITH_CACHE__
//        ptrTree.getObjectsCountInCache(nObjectsLinkedList);
//        ASSERT(nObjectsLinkedList == 1);
//#endif //__TREE_WITH_CACHE__

        //std::ofstream out1("d:\\t1.txt");
        //ptrTree.print(out1);
        //out1.flush();
        //out1.close();

        //for (int nCntr = nTotalRecords - 1; nCntr >= 0; nCntr--)
        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        {
            T::ValueType oValue = 0;
            ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
            ASSERT(oValue == vtNumberData[nCntr]);
        }
        //std::ofstream out1("d:\\t0.txt");
        //ptrTree.print(out1);
        //out1.flush();
        //out1.close();

        //for (int nCntr = nTotalRecords - 1; nCntr >= 0; nCntr--)
        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        {
            ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);
            if (ec != ErrorCode::Success)
            {
                std::cout << nCntr << std::endl;
            }
            ASSERT(ec == ErrorCode::Success);
        }
        /*
        for (int nCntr = 1; nCntr < nTotalRecords; nCntr = nCntr + 2)
        {
            ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }
        */

    /*   std::ofstream out2("d:\\t1.txt");
       ptrTree.print(out2);
       out2.flush();
       out2.close();*/

       for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        {
            T::ValueType oValue = 0;
            ErrorCode ec =ptrTree.search(vtNumberData[nCntr], oValue);
            ASSERT(ec == ErrorCode::KeyDoesNotExist);
        }
    }
    //return;
    for (int nTestCntr = 0; nTestCntr < 2; nTestCntr++)
    {
        std::vector<int> vtNumberData(nTotalRecords);
        GENERATE_RANDOM_NUMBER_ARRAY(0, nTotalRecords, vtNumberData);
        dumpVectorToFile(vtNumberData, "C:\\vals");
        //vtNumberData = readVectorFromFile<int>("C:\\vals5");

        for (int nCntr = nTotalRecords - 1; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode ec =ptrTree.insert(vtNumberData[nCntr], vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);

        }
        for (int nCntr = nTotalRecords - 2; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode ec =ptrTree.insert(vtNumberData[nCntr], vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }

        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        {
            T::ValueType oValue = 0;
            ErrorCode ec =ptrTree.search(vtNumberData[nCntr], oValue);
            ASSERT(ec == ErrorCode::Success);
        }

        for (int nCntr = nTotalRecords - 1; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode ec =ptrTree.remove(vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }

        for (int nCntr = nTotalRecords - 2; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode ec =ptrTree.remove(vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }

        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        {
            T::ValueType oValue = 0;
            ErrorCode ec =ptrTree.search(vtNumberData[nCntr], oValue);
            ASSERT(ec == ErrorCode::KeyDoesNotExist);
        }
    }
}

void test2(int degree)
{
    //ASSERT(1 == 2);
    size_t nTotalRecords = 100000;

#ifdef __TREE_WITH_CACHE__
    using T = BPlusStoreTraits <
#ifdef __PROD__
        __FANOUT_PROD_ENV__,
#endif //__PROD__
        int,              // Key type
        int,              // Value type
        TYPE_UID::DATA_NODE_INT_INT,
        TYPE_UID::INDEX_NODE_INT_INT,
        ObjectFatUID,     // Object UID type
        CLOCKCacheObject,
        CLOCKCache,
#ifdef _MSC_VER
        VolatileStorage
#else //_MSC_VER
        VolatileStorage
#endif //_MSC_VER
    >;

    using MyStore = typename T::StoreType;
    MyStore ptrTree(degree, 1000, 4096, 2ULL * 1024 * 1024 * 1024);// , FILE_STORAGE_PATH);

    ptrTree.init<T::DataNodeType>();
#else //__TREE_WITH_CACHE__
    using T = BPlusStoreTraits<
        int,              // Key type
        int,              // Value type
        TYPE_UID::DATA_NODE_INT_INT,
        TYPE_UID::INDEX_NODE_INT_INT,
        uintptr_t,     // Object UID type
        NoCacheObject,
        NoCache
    >;

    using MyStore = typename T::StoreType;
    MyStore ptrTree(24);

    ptrTree.init<T::DataNodeType>();
#endif //__TREE_WITH_CACHE__

  /*  std::vector<int> vtNumberData(nTotalRecords);
    GENERATE_RANDOM_NUMBER_ARRAY(0, nTotalRecords, vtNumberData);

    dumpVectorToFile(vtNumberData, "C:\\vals");*/
    //vtNumberData = readVectorFromFile<int>("C:\\vals5");


    for (int nTestCntr = 0; nTestCntr < 2; nTestCntr++)
    {
        std::vector<int> vtNumberData(nTotalRecords);
        GENERATE_RANDOM_NUMBER_ARRAY(0, nTotalRecords, vtNumberData);
        dumpVectorToFile(vtNumberData, "C:\\vals_crash");
        //vtNumberData = readVectorFromFile<int>("C:\\vals_crash");

        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        {
            ErrorCode ec = ptrTree.insert(vtNumberData[nCntr], vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }

        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        {
            T::ValueType oValue = 0;
            ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
            ASSERT(oValue == vtNumberData[nCntr]);
        }

        for (int nCntr = 0; nCntr < nTotalRecords; nCntr = nCntr + 2)
        {
            ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }

        /*std::string filename = "c:\\fix\\___.txt";
        std::ofstream out(filename);
        ptrTree.print(out);
        out.flush();
        out.close();*/

        for (int nCntr = 1; nCntr < nTotalRecords; nCntr = nCntr + 2)
        {
            //if (nCntr >= 52475 && nCntr <= 51500 && (nCntr - 52475) % 1000 == 0)
          /*  if (nCntr == 52475 + 6)
            {
                std::string filename = "c:\\fix\\" + std::to_string(nCntr) + ".txt";
                std::ofstream out(filename);
                ptrTree.print(out);
                out.flush();
                out.close();
            }*/

            ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }

        //if (nTestCntr == 1) {
            
        //}
        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        {
            T::ValueType oValue = 0;
            ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
            if (ec != ErrorCode::KeyDoesNotExist)
            {
                std::cout << vtNumberData[nCntr] << std::endl;
                std::cout << nCntr << std::endl;
                ASSERT(ec == ErrorCode::KeyDoesNotExist);
            }
        }
    }

    for (int nTestCntr = 0; nTestCntr < 2; nTestCntr++)
    {
        std::vector<int> vtNumberData(nTotalRecords);
        GENERATE_RANDOM_NUMBER_ARRAY(0, nTotalRecords, vtNumberData);
        dumpVectorToFile(vtNumberData, "C:\\vals");
        //vtNumberData = readVectorFromFile<int>("C:\\vals");

        for (int nCntr = nTotalRecords - 1; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode ec = ptrTree.insert(vtNumberData[nCntr], vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);

            //if (nCntr >= 63481 - 5 && nCntr <= 63481 + 5)
            /*if (nCntr == 63481)
            {
                std::string filename = "c:\\fix\\" + std::to_string(nCntr) + ".txt";
                std::ofstream out(filename);
                ptrTree.print(out);
                out.flush();
                out.close();
            }*/


        }
      /*  std::string filename = "c:\\pre123.txt";
        std::ofstream out(filename);
        ptrTree.print(out);
        out.flush();
        out.close();*/
        for (int nCntr = nTotalRecords - 2; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode ec = ptrTree.insert(vtNumberData[nCntr], vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }

       /* std::string filename = "c:\\123.txt";
        std::ofstream out(filename);
        ptrTree.print(out);
        out.flush();
        out.close();*/

        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        {
            T::ValueType oValue = 0;
            ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);           
            ASSERT(oValue == vtNumberData[nCntr] && ec == ErrorCode::Success);
        }

        for (int nCntr = nTotalRecords - 1; nCntr >= 0; nCntr = nCntr - 2)
        {
            /*if (nCntr == 20386)
            {
                std::string filename = "c:\\fix\\r" + std::to_string(nCntr) + ".txt";
                std::ofstream out(filename);
                ptrTree.print(out);
                out.flush();
                out.close();
            }*/

            ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }

        for (int nCntr = nTotalRecords - 2; nCntr >= 0; nCntr = nCntr - 2)
        {
            //if (nCntr >= 800 && nCntr <= 946)//if (nCntr == 20386)
            //{
            //    std::string filename = "c:\\fix\\r" + std::to_string(nCntr) + ".txt";
            //    std::ofstream out(filename);
            //    ptrTree.print(out);
            //    out.flush();
            //    out.close();
            //}


            ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }

        /*{
            std::string filename = "c:\\fix\\r___.txt";
            std::ofstream out(filename);
            ptrTree.print(out);
            out.flush();
            out.close();
        }*/

        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        {
          /* if (nCntr >= 20386 - 5 && nCntr <= 20386 + 5)
           {
               std::string filename = "c:\\fix\\s" + std::to_string(nCntr) + ".txt";
               std::ofstream out(filename);
               ptrTree.print(out);
               out.flush();
               out.close();
           }*/

            T::ValueType oValue = 0;
            ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
            ASSERT(ec == ErrorCode::KeyDoesNotExist);
        }
    }
}

//void test_inmemory(int degree)
//{
//    std::cout << "Testing In-Memory B-Epsilon Store with degree " << degree << std::endl;
//    
//    using T = InMemoryTraits;
//
//    BEpsilonStoreInMemory<T> ptrTree(degree);
//    ptrTree.template init<typename T::DataNodeType>();
//
//    size_t nTotalRecords = 1000;
//    std::vector<int> vtNumberData(nTotalRecords);
//    GENERATE_RANDOM_NUMBER_ARRAY(0, nTotalRecords, vtNumberData);
//
//    // Insert test
//    std::cout << "Inserting " << nTotalRecords << " records..." << std::endl;
//    for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
//    {
//        ErrorCode ec = ptrTree.insert(vtNumberData[nCntr], nCntr);
//        ASSERT(ec == ErrorCode::Success);
//    }
//
//    // Search test
//    std::cout << "Searching for all records..." << std::endl;
//    for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
//    {
//        T::ValueType oValue = 0;
//        ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
//        ASSERT(oValue == nCntr && ec == ErrorCode::Success);
//    }
//
//    // Update test
//    std::cout << "Updating records..." << std::endl;
//    for (int nCntr = 0; nCntr < nTotalRecords / 2; nCntr++)
//    {
//        ErrorCode ec = ptrTree.update(vtNumberData[nCntr], nCntr + 1000);
//        ASSERT(ec == ErrorCode::Success);
//    }
//
//    // Verify updates
//    std::cout << "Verifying updates..." << std::endl;
//    for (int nCntr = 0; nCntr < nTotalRecords / 2; nCntr++)
//    {
//        T::ValueType oValue = 0;
//        ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
//        ASSERT(oValue == nCntr + 1000 && ec == ErrorCode::Success);
//    }
//
//    // Delete test
//    std::cout << "Deleting half of the records..." << std::endl;
//    for (int nCntr = 0; nCntr < nTotalRecords / 2; nCntr++)
//    {
//        ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);
//        ASSERT(ec == ErrorCode::Success);
//    }
//
//    // Verify deletions
//    std::cout << "Verifying deletions..." << std::endl;
//    for (int nCntr = 0; nCntr < nTotalRecords / 2; nCntr++)
//    {
//        T::ValueType oValue = 0;
//        ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
//        ASSERT(ec == ErrorCode::KeyDoesNotExist);
//    }
//
//    // Verify remaining records
//    std::cout << "Verifying remaining records..." << std::endl;
//    for (int nCntr = nTotalRecords / 2; nCntr < nTotalRecords; nCntr++)
//    {
//        T::ValueType oValue = 0;
//        ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
//        ASSERT(oValue == nCntr && ec == ErrorCode::Success);
//    }
//
//    std::cout << "In-Memory B-Epsilon Store test completed successfully!" << std::endl;
//}

void test3(int i)
{
#ifdef __TREE_WITH_CACHE__
    using T = BPlusStoreTraits <
#ifdef __PROD__
        __FANOUT_PROD_ENV__,
#endif //__PROD__
        int,              // Key type
        int,              // Value type
        TYPE_UID::DATA_NODE_INT_INT,
        TYPE_UID::INDEX_NODE_INT_INT,
        ObjectFatUID,     // Object UID type
        CLOCKCacheObject,
        CLOCKCache,
#ifdef _MSC_VER
        VolatileStorage
#else //_MSC_VER
        VolatileStorage
#endif //_MSC_VER
    >;

    using MyStore = typename T::StoreType;
    MyStore ptrTree(i, 1000, 4096, 2ULL * 1024 * 1024 * 1024);//, FILE_STORAGE_PATH);

    ptrTree.init<T::DataNodeType>();
#else //__TREE_WITH_CACHE__
    using T = BPlusStoreTraits<
        int,              // Key type
        int,              // Value type
        TYPE_UID::DATA_NODE_INT_INT,
        TYPE_UID::INDEX_NODE_INT_INT,
        uintptr_t,     // Object UID type
        NoCacheObject,
        NoCache
    >;

    using MyStore = typename T::StoreType;
    MyStore ptrTree(3);

    ptrTree.init<T::DataNodeType>();
#endif //__TREE_WITH_CACHE__

    size_t nTotalRecords = 100000;
    //std::vector<int> vtNumberData;
    //GENERATE_RANDOM_NUMBER_ARRAY(0, nTotalRecords, vtNumberData);

#ifdef _MSC_VER
    //dumpVectorToFile(vtNumberData, "C:\\vals");
    //vtNumberData = readVectorFromFile<int>("C:\\vals");
#else //_MSC_VER
    //dumpVectorToFile(vtNumberData, "/mnt/tmpfs/vals");
#endif //_MSC_VER
    std::string filename;

    for (int nTestCntr = 0; nTestCntr < 2; nTestCntr++)
    {
        std::string filename = "c:\\vals_0_it" + std::to_string(nTestCntr) + ".txt";
        std::vector<int> vtNumberData(nTotalRecords);
        GENERATE_RANDOM_NUMBER_ARRAY(0, nTotalRecords, vtNumberData);
        dumpVectorToFile(vtNumberData, "C:\\vals");
        //vtNumberData = readVectorFromFile<int>(filename);

        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        {
            ErrorCode ec = ptrTree.insert(vtNumberData[nCntr], vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }
        
      /*  filename = "c:\\tree_state_after_first_insert_round.txt";
        std::ofstream out(filename);
        ptrTree.print(out);
        out.flush();
        out.close();*/

        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        {
            T::ValueType oValue = 0;
            ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
            ASSERT(oValue == vtNumberData[nCntr]);
        }

        for (int nCntr = 0; nCntr < nTotalRecords; nCntr = nCntr + 2)
        {
            ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }

        for (int nCntr = 1; nCntr < nTotalRecords; nCntr = nCntr + 2)
        {
            ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }

       /* filename = "c:\\delete.txt";
        std::ofstream out2(filename);
        ptrTree.print(out2);
        out2.flush();
        out2.close();*/

        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        {
            T::ValueType oValue = 0;
            ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
            ASSERT(ec == ErrorCode::KeyDoesNotExist);
        }
        
    }
    //return;
    for (int nTestCntr = 0; nTestCntr < 2; nTestCntr++)
    {
        filename = "c:\\vals_1_it" + std::to_string(nTestCntr) + ".txt";
        std::vector<int> vtNumberData(nTotalRecords);
        GENERATE_RANDOM_NUMBER_ARRAY(0, nTotalRecords, vtNumberData);
        //dumpVectorToFile(vtNumberData, "C:\\vals");
        //vtNumberData = readVectorFromFile<int>(filename);

        for (int nCntr = nTotalRecords - 1; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode ec = ptrTree.insert(vtNumberData[nCntr], vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);

        }
        for (int nCntr = nTotalRecords - 2; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode ec = ptrTree.insert(vtNumberData[nCntr], vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }

       /* filename = "c:\\tree_state_after_first_insert_round2.txt";
        std::ofstream out(filename);
        ptrTree.print(out);
        out.flush();
        out.close();*/

        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        {
            T::ValueType oValue = 0;
            ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
            ASSERT(oValue == vtNumberData[nCntr] && ec == ErrorCode::Success);
        }

        for (int nCntr = nTotalRecords - 1; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }

        for (int nCntr = nTotalRecords - 2; nCntr >= 0; nCntr = nCntr - 2)
        {
            ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }

        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
        {
            T::ValueType oValue = 0;
            ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
            ASSERT(ec == ErrorCode::KeyDoesNotExist);
        }
    }
}

//void test3InMemory(int i)
//{
//    using T = InMemoryTraits;
//    
//    BEpsilonStoreInMemory<T> ptrTree(i);
//    ptrTree.template init<typename T::DataNodeType>();
//
//    size_t nTotalRecords = 500000;
//
//    for (int nTestCntr = 0; nTestCntr < 2; nTestCntr++)
//    {
//        std::vector<int> vtNumberData(nTotalRecords);
//        GENERATE_RANDOM_NUMBER_ARRAY(0, nTotalRecords, vtNumberData);
//
//        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
//        {
//            ErrorCode ec = ptrTree.insert(vtNumberData[nCntr], vtNumberData[nCntr]);
//            ASSERT(ec == ErrorCode::Success);
//        }
//        
//        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
//        {
//            T::ValueType oValue = 0;
//            ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
//            ASSERT(oValue == vtNumberData[nCntr]);
//        }
//       
//        for (int nCntr = 0; nCntr < nTotalRecords; nCntr = nCntr + 2)
//        {
//            ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);
//            ASSERT(ec == ErrorCode::Success);
//        }
//
//        for (int nCntr = 1; nCntr < nTotalRecords; nCntr = nCntr + 2)
//        {
//            ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);
//            ASSERT(ec == ErrorCode::Success);
//        }
//
//        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
//        {
//            T::ValueType oValue = 0;
//            ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
//            ASSERT(ec == ErrorCode::KeyDoesNotExist);
//        }
//    }
//
//    for (int nTestCntr = 0; nTestCntr < 2; nTestCntr++)
//    {
//        std::vector<int> vtNumberData(nTotalRecords);
//        GENERATE_RANDOM_NUMBER_ARRAY(0, nTotalRecords, vtNumberData);
//
//        for (int nCntr = nTotalRecords - 1; nCntr >= 0; nCntr = nCntr - 2)
//        {
//            ErrorCode ec = ptrTree.insert(vtNumberData[nCntr], nCntr);
//            ASSERT(ec == ErrorCode::Success);
//        }
//        for (int nCntr = nTotalRecords - 2; nCntr >= 0; nCntr = nCntr - 2)
//        {
//            ErrorCode ec = ptrTree.insert(vtNumberData[nCntr], nCntr);
//            ASSERT(ec == ErrorCode::Success);
//        }
//
//        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
//        {
//            T::ValueType oValue = 0;
//            ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
//            ASSERT(oValue == nCntr && ec == ErrorCode::Success);
//        }
//
//        for (int nCntr = nTotalRecords - 1; nCntr >= 0; nCntr = nCntr - 2)
//        {
//            ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);
//            ASSERT(ec == ErrorCode::Success);
//        }
//
//        for (int nCntr = nTotalRecords - 2; nCntr >= 0; nCntr = nCntr - 2)
//        {
//            ErrorCode ec = ptrTree.remove(vtNumberData[nCntr]);
//            ASSERT(ec == ErrorCode::Success);
//        }
//
//        for (int nCntr = 0; nCntr < nTotalRecords; nCntr++)
//        {
//            T::ValueType oValue = 0;
//            ErrorCode ec = ptrTree.search(vtNumberData[nCntr], oValue);
//            ASSERT(ec == ErrorCode::KeyDoesNotExist);
//        }
//    }
//}

// Include the B+ Tree and B-Epsilon Tree variants testing
int main(int argc, char* argv[])
{
    // std::cout << "Testing InMemory B-Epsilon Tree..." << std::endl;
    // test3InMemory(3);
    // std::cout << "InMemory B-Epsilon Tree test completed successfully!" << std::endl;
    // return 0;

    quick_test();
    for (int i = 64; i < 200; i++)
        for (int j = i; j < i + 5; j++)
        {
            std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

            test(3);
            test(i);
            //continue;
            std::cout << j << "..." << std::endl;
            //test3InMemory(i);
            //test3InMemory(j);
            test3(3);
            test3(i);
            //return 0;
            test3(j);
            test2(j);
            test2(3);
            test_ex(j);
            test_ex(3);

            std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
            std::cout
                << ">> [Time: "
                << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() << "us"
                << ", " << std::chrono::duration_cast<std::chrono::nanoseconds> (end - begin).count() << "ns]"
                << std::endl;

            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

    //fptree_bm_test(10000000);
    //fptree_bm_string();

    return 0;

    //for (int i = 3; i < 400; i++)
    //    for (int j = 3; j < 12; i++)
    // {
    //     test(3);
    //     test(4);
    //     test(j);
    //     std::cout << i << ": Main thread sleeping for 10 seconds..." << std::endl;
    //     std::this_thread::sleep_for(std::chrono::seconds(1)); // Sleep for 3 seconds
    //     std::cout << "Main thread woke up!" << std::endl;
    // }

    char ch = getchar();
}