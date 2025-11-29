#pragma once

#define __MAX_MSG__ 100

#include <iostream>
#include <type_traits>
#include <variant>
#include <typeinfo>
#include <cstring>
#include <memory>
#include <vector>
#include <optional>
//#include "glog/logging.h"
#ifndef __TREE_WITH_CACHE__
#include "NoCache.hpp"
#endif
#include "validityasserts.h"

#include "DataNode.hpp"
#include "DataNodeCOpt.hpp"
#include "IndexNode.hpp"
#include "IndexNodeCOpt.hpp"
#include "BPlusStore.hpp"


#include "IndexNodeWithBuffer.hpp"
#include "DataNodeWithBuffer.hpp"
#include "BEpsilonStore.hpp"


#include "IndexNodeWithBufferEx.hpp"
#include "DataNodeWithBufferEx.hpp"
#include "BEpsilonStoreEx.hpp"

#include "IndexNodeWithBufferExII.hpp"
#include "BEpsilonStoreExII.hpp"


#include "NoCacheObject.hpp"
#include "TypeUID.h"

#include "FileWAL.hpp"
#include "PMemWAL.hpp"

//template <typename Traits> class DataNode;
//template <typename Traits> class DataNodeCOpt;
//template <typename Traits> class IndexNode;
//template <typename Traits> class IndexNodeCOpt;
//template <typename Traits> class DataNodeWithBufferEx;
//template <typename Traits> class IndexNodeWithBufferEx;
//
//
//template <typename Traits> class NoCacheObject;
//template <typename Traits> class NoCache;
//template <typename Traits> class BPlusStore;
//template <typename Traits> class BEpsilonStoreEx;
//
//
//template <typename Traits> class FileWAL;
//template <typename Traits> class PMemWAL;

#define __TEST_INTERNAL_REPEAT_COUNT__ 1
//#define __EXECUTE_ALL_TESTS__


#ifdef _MSC_VER
#define WAL_FILE_PATH "c:\\wal.bin"
#define FILE_STORAGE_PATH "c:\\file_storage.bin"
#define PMEM_STORAGE_PATH "c:\\pmem_storage.bin"
#define __CACHE_SIZE__ 100
#define __NO_OF_RECORDS__ 500000
#define __STORAGE_SIZE__ 2ULL * 1024 * 1024 * 1024
#else //_MSC_VER
#define WAL_FILE_PATH "/mnt/tmpfs/wal.bin"
#define FILE_STORAGE_PATH "/home/skarim/file_storage.bin"
#define PMEM_STORAGE_PATH "/mnt/tmpfs/pmem_storage.bin"
#define __CACHE_SIZE__ 1000
#define __NO_OF_RECORDS__ 100000
#define __STORAGE_SIZE__ 2ULL * 1024 * 1024 * 1024
#endif //_MSC_VER

struct CHAR16 {
    char data[16];

    // Default constructor (trivial)
    CHAR16() = default;

    // Parameterized constructor from string
    CHAR16(const char* str) {
        std::memset(data, 0, sizeof(data));
#ifndef _MSC_VER
        strncpy(data, str, sizeof(data) - 1);
#else //_MSC_VER
        strncpy_s(data, sizeof(data), str, sizeof(data) - 1);
#endif //_MSC_VER
    }

    // Constructor from numeric value (only for integral types)
    template<typename T, typename = typename std::enable_if<std::is_integral<T>::value>::type>
    CHAR16(T value) {
        std::memset(data, 0, sizeof(data));
        char str[16];
        snprintf(str, sizeof(str), "str_%08llu", static_cast<unsigned long long>(value));
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

    // Define the >= operator for comparison
    bool operator>=(const CHAR16& other) const {
        return std::strncmp(data, other.data, sizeof(data)) >= 0;
    }

    // Define the > operator for comparison
    bool operator>(const CHAR16& other) const {
        return std::strncmp(data, other.data, sizeof(data)) > 0;
    }

    // Define the <= operator for comparison
    bool operator<=(const CHAR16& other) const {
        return std::strncmp(data, other.data, sizeof(data)) <= 0;
    }

    // Define the != operator for comparison
    bool operator!=(const CHAR16& other) const {
        return std::strncmp(data, other.data, sizeof(data)) != 0;
    }

    // Stream output operator for debugging/logging
    friend std::ostream& operator<<(std::ostream& os, const CHAR16& obj) {
        os << obj.data;
        return os;
    }
};

// Specialize std::hash for CHAR16
namespace std {
    template<>
    struct hash<CHAR16> {
        std::size_t operator()(const CHAR16& obj) const noexcept {
            // Use std::hash<std::string_view> for efficient hashing
            return std::hash<std::string_view>{}(std::string_view(obj.data, strnlen(obj.data, sizeof(obj.data))));
        }
    };
}

template<typename T>
struct NoStorage {};

struct NoTypeMarshaller {};

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
struct BPlusStoreSOATrait
{
    using KeyType = TKeyType;
    using ValueType = TValueType;
    using ObjectUIDType = TObjectUIDType;

    static constexpr uint8_t DataNodeUID = TDataNodeUID;
    static constexpr uint8_t IndexNodeUID = TIndexNodeUID;

    // Core Objects
    using DataNodeType = DataNode<BPlusStoreSOATrait>;
    using IndexNodeType = IndexNode<BPlusStoreSOATrait>;
    using ObjectType = TCacheObjectType<BPlusStoreSOATrait>;

    // Cache and Storage
    using CacheType = TCacheType<BPlusStoreSOATrait>;
    using StorageType = TStorageType<BPlusStoreSOATrait>;

    // Store
    using StoreType = BPlusStore<BPlusStoreSOATrait>;

#ifndef _MSC_VER
    using WALType = PMemWAL<BPlusStoreSOATrait>;
#else //_MSC_VER
    using WALType = FileWAL<BPlusStoreSOATrait>;
#endif //_MSC_VER
};

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
struct BPlusStoreAOSTrait
{
    using KeyType = TKeyType;
    using ValueType = TValueType;
    using ObjectUIDType = TObjectUIDType;

    static constexpr uint8_t DataNodeUID = TDataNodeUID;
    static constexpr uint8_t IndexNodeUID = TIndexNodeUID;

    // Core Objects
    using DataNodeType = DataNodeCOpt<BPlusStoreAOSTrait>;
    using IndexNodeType = IndexNodeCOpt<BPlusStoreAOSTrait>;
    using ObjectType = TCacheObjectType<BPlusStoreAOSTrait>;

    // Cache and Storage
    using CacheType = TCacheType<BPlusStoreAOSTrait>;
    using StorageType = TStorageType<BPlusStoreAOSTrait>;

    // Store
    using StoreType = BPlusStore<BPlusStoreAOSTrait>;

#ifndef _MSC_VER
    using WALType = PMemWAL<BPlusStoreAOSTrait>;
#else //_MSC_VER
    using WALType = FileWAL<BPlusStoreAOSTrait>;
#endif //_MSC_VER
};
