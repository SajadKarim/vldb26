# DualNodeStorage - Dual-Tier Storage for B+-Tree Nodes

## Overview

`DualNodeStorage` is a storage class that implements a **dual-tier storage architecture** for B+-tree nodes. It routes different node types to different storage backends based on their access patterns and cost characteristics.

### Key Features

- **Node-Type-Aware Routing**: Automatically routes IndexNodes and DataNodes to different storage tiers
- **Cost Tracking**: Tracks access costs and statistics per storage tier
- **Transparent Integration**: Works seamlessly with existing cache policies (LRU, CLOCK, A2Q)
- **Flexible Configuration**: Supports any combination of storage backends (Volatile, PMem, File)
- **Thread-Safe**: Atomic counters for statistics in concurrent environments

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Cache Layer                          │
│              (LRU, CLOCK, A2Q, etc.)                    │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                 DualNodeStorage                         │
│                                                         │
│  ┌──────────────────────┐  ┌──────────────────────┐   │
│  │  Primary Storage     │  │  Secondary Storage   │   │
│  │  (IndexNodes)        │  │  (DataNodes)         │   │
│  │                      │  │                      │   │
│  │  e.g., Volatile      │  │  e.g., FileStorage   │   │
│  │  (Fast DRAM)         │  │  (Slower, Persistent)│   │
│  └──────────────────────┘  └──────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Routing Logic

The routing is based on the **node type** encoded in the `ObjectUID`:

- **IndexNodes** (internal nodes, `IndexNodeUID = 2`) → **Primary Storage**
  - Typically fast, volatile storage (e.g., VolatileStorage)
  - Lower access cost
  - Frequently accessed during tree traversal

- **DataNodes** (leaf nodes, `DataNodeUID = 1`) → **Secondary Storage**
  - Typically slower, persistent storage (e.g., FileStorage)
  - Higher access cost
  - Contains actual key-value data

## Usage

### 1. Basic Setup

```cpp
#include "DualNodeStorage.hpp"
#include "VolatileStorage.hpp"
#include "FileStorage.hpp"

// Define a type alias for your specific dual storage configuration
template<typename Traits>
using DualVolatileFileStorage = DualNodeStorage<
    Traits,
    VolatileStorage,  // Primary storage for IndexNodes
    FileStorage       // Secondary storage for DataNodes
>;
```

### 2. Configure BPlusStoreTraits

```cpp
using MyTraits = BPlusStoreTraits<
    uint64_t,                    // KeyType
    uint64_t,                    // ValueType
    1,                           // DataNodeUID
    2,                           // IndexNodeUID
    ObjectFatUID,                // ObjectUIDType
    LRUCacheObject,              // CacheObjectType
    LRUCache,                    // CacheType
    DualVolatileFileStorage      // StorageType - Use dual storage!
>;
```

### 3. Initialize with Configuration

```cpp
// Create configuration
typename DualVolatileFileStorage<MyTraits>::Config config(
    // Primary storage (VolatileStorage for IndexNodes)
    4096,                        // primaryBlockSize (4KB)
    1024 * 1024 * 1024,         // primaryStorageSize (1GB)
    
    // Secondary storage (FileStorage for DataNodes)
    4096,                        // secondaryBlockSize (4KB)
    10ULL * 1024 * 1024 * 1024, // secondaryStorageSize (10GB)
    "/tmp/bptree_data.bin",     // secondaryPath
    
    // Cost configuration (in nanoseconds)
    10,                          // primaryAccessCost (10ns for DRAM)
    1000                         // secondaryAccessCost (1000ns for File I/O)
);

// Create cache with dual storage
auto cache = new LRUCache<MyTraits>(
    cacheCapacity,
    config  // Pass config to storage constructor
);
```

### 4. Alternative: Direct Constructor

```cpp
// Or use direct constructor without config struct
auto cache = new LRUCache<MyTraits>(
    cacheCapacity,
    4096,                        // primaryBlockSize
    1024 * 1024 * 1024,         // primaryStorageSize
    4096,                        // secondaryBlockSize
    10ULL * 1024 * 1024 * 1024, // secondaryStorageSize
    "/tmp/bptree_data.bin",     // secondaryPath
    10,                          // primaryAccessCost
    1000                         // secondaryAccessCost
);
```

## Storage Combinations

### Recommended Configurations

#### 1. **Volatile + File** (Most Common)
```cpp
template<typename Traits>
using DualVolatileFileStorage = DualNodeStorage<
    Traits,
    VolatileStorage,  // Fast DRAM for IndexNodes
    FileStorage       // Persistent file for DataNodes
>;
```
- **Use Case**: General-purpose B+-tree with persistence
- **Benefits**: Fast index traversal, persistent data storage

#### 2. **Volatile + PMem**
```cpp
template<typename Traits>
using DualVolatilePMemStorage = DualNodeStorage<
    Traits,
    VolatileStorage,  // Fast DRAM for IndexNodes
    PMemStorage       // Persistent memory for DataNodes
>;
```
- **Use Case**: High-performance persistent storage with PMem hardware
- **Benefits**: Fast index traversal, low-latency persistent data

#### 3. **PMem + File**
```cpp
template<typename Traits>
using DualPMemFileStorage = DualNodeStorage<
    Traits,
    PMemStorage,      // PMem for IndexNodes
    FileStorage       // File for DataNodes
>;
```
- **Use Case**: Large datasets with PMem for hot index data
- **Benefits**: Persistent index, cost-effective data storage

## Cost Tracking and Statistics

### Accessing Cost Information

```cpp
// Get storage instance from cache
auto storage = cache->getStorage();

// Get cost for a specific node type
uint64_t indexCost = storage->getAccessCost(MyTraits::IndexNodeUID);
uint64_t dataCost = storage->getAccessCost(MyTraits::DataNodeUID);

// Get cost from a UID
uint64_t cost = storage->getAccessCostFromUID(someUID);

// Get full cost info
const auto& costInfo = storage->getCostInfo();
std::cout << "Primary reads: " << costInfo.primaryReadCount.load() << std::endl;
std::cout << "Secondary reads: " << costInfo.secondaryReadCount.load() << std::endl;
```

### Printing Statistics

```cpp
// Print detailed statistics
storage->printStatistics();

// Output:
// === DualNodeStorage Statistics ===
// Primary Storage (IndexNodes):
//   Reads:  12345
//   Writes: 678
//   Total Cost: 130230 units
// Secondary Storage (DataNodes):
//   Reads:  98765
//   Writes: 4321
//   Total Cost: 103086000 units
// Total Operations: 116109
// Total Cost: 103216230 units
// Average Cost per Operation: 889.12 units
// ===================================
```

### Resetting Statistics

```cpp
storage->resetStatistics();
```

## Integration with Existing Caches

### No Cache Modifications Required!

The beauty of `DualNodeStorage` is that it works **transparently** with existing cache implementations:

- ✅ **LRUCache** - Works as-is
- ✅ **CLOCKCache** - Works as-is
- ✅ **A2QCache** - Works as-is

The cache calls the storage interface methods (`getObject`, `addObject`, `remove`), and `DualNodeStorage` handles the routing internally.

### How It Works

1. **Cache eviction**: Cache calls `storage->addObject(ptrObject, uidUpdated)`
2. **DualNodeStorage routing**: Checks `ptrObject->m_nCoreObjectType`
3. **Storage selection**: Routes to primary or secondary storage
4. **UID update**: Storage updates `uidUpdated` with correct media type
5. **Cache tracking**: Cache stores the updated UID for future retrieval

## Cost-Aware Caching (Future Enhancement)

While the current implementation works with existing caches, you can extend cache policies to be **cost-aware**:

### Example: Cost-Aware LRU Eviction

```cpp
// In a future CostAwareLRUCache implementation
double calculateEvictionScore(ObjectTypePtr obj) {
    // Get storage cost for this node type
    uint64_t storageCost = m_ptrStorage->getAccessCost(obj->m_nCoreObjectType);
    
    // Base LRU score (recency)
    double recencyScore = getRecencyScore(obj);
    
    // Combined score: higher cost = higher priority to keep in cache
    // Lower score = more likely to evict
    return recencyScore / storageCost;
}
```

This would make the cache **prefer to evict low-cost items** (e.g., IndexNodes in fast DRAM) over high-cost items (e.g., DataNodes in slow File storage).

## Performance Considerations

### Access Cost Configuration

Choose cost values that reflect **actual access latencies**:

```cpp
// Example latencies (approximate)
DRAM (VolatileStorage):     10-100 ns
PMem (PMemStorage):         100-500 ns
SSD (FileStorage):          10,000-100,000 ns (10-100 μs)
HDD (FileStorage):          5,000,000-10,000,000 ns (5-10 ms)
```

### Memory Overhead

- **Minimal**: Only adds one pointer per storage tier
- **Statistics**: Atomic counters (8 bytes each × 6 = 48 bytes)
- **Total overhead**: ~64 bytes per DualNodeStorage instance

### Thread Safety

- All statistics use **atomic operations** for thread-safe updates
- No locks in the hot path (read/write operations)
- Storage-specific thread safety handled by underlying storage classes

## Troubleshooting

### Issue: Objects not routing correctly

**Check:**
1. Verify `m_nCoreObjectType` is set correctly in DataNode and IndexNode constructors
2. Ensure `DataNodeUID` and `IndexNodeUID` constants match your traits definition
3. Add debug logging in `addObject()` to verify routing

### Issue: UID media type mismatch

**Cause:** The storage's `addObject()` method updates the UID with the correct media type.

**Solution:** Always use the `uidUpdated` returned by `addObject()`, not the original UID.

### Issue: Constructor parameter mismatch

**Cause:** Cache constructor expects different parameters than DualNodeStorage.

**Solution:** Use the `Config` struct or ensure parameter order matches:
```cpp
DualNodeStorage(
    primaryBlockSize, primaryStorageSize,
    secondaryBlockSize, secondaryStorageSize, secondaryPath,
    primaryCost, secondaryCost
)
```

## Testing

### Unit Test Example

```cpp
void testDualNodeStorage() {
    using TestTraits = BPlusStoreTraits<...>;
    using DualStorage = DualVolatileFileStorage<TestTraits>;
    
    // Create storage
    typename DualStorage::Config config(...);
    DualStorage storage(config);
    
    // Test IndexNode routing
    auto indexNode = new IndexNode<TestTraits>(...);
    ObjectFatUID uidIndex;
    storage.addObject(indexNode, uidIndex);
    assert(uidIndex.getMediaType() == ObjectFatUID::DRAM);  // Should be in primary
    
    // Test DataNode routing
    auto dataNode = new DataNode<TestTraits>(...);
    ObjectFatUID uidData;
    storage.addObject(dataNode, uidData);
    assert(uidData.getMediaType() == ObjectFatUID::File);   // Should be in secondary
    
    // Verify retrieval
    auto retrievedIndex = storage.getObject(degree, uidIndex);
    auto retrievedData = storage.getObject(degree, uidData);
    
    // Check statistics
    assert(storage.getCostInfo().primaryWriteCount.load() == 1);
    assert(storage.getCostInfo().secondaryWriteCount.load() == 1);
    
    std::cout << "All tests passed!" << std::endl;
}
```

## Future Enhancements

1. **Dynamic Cost Adjustment**: Update costs based on runtime measurements
2. **Migration Support**: Move objects between storage tiers based on access patterns
3. **Multi-Tier Support**: Extend to 3+ storage tiers (L1/L2/L3 cache hierarchy)
4. **Cost-Aware Eviction**: Integrate cost information into cache eviction policies
5. **Compression**: Different compression strategies per storage tier

## References

- `HybridStorage.hpp` - Similar multi-tier storage implementation
- `ObjectFatUID.h` - UID system with media type tracking
- `VolatileStorage.hpp` - DRAM-based storage implementation
- `FileStorage.hpp` - File-based persistent storage implementation