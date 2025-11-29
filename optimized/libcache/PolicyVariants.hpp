#pragma once

#include "LRUCache.hpp"
#include "A2QCache.hpp"
#include "CLOCKCache.hpp"
#include "DeviceAwarePolicy.hpp"
#include <memory>
#include <string>
#include <unordered_set>
#include <deque>

namespace PolicyVariants {

// ============================================================================
// A2Q Cache Variants
// ============================================================================

/**
 * A2QCacheRelaxed: A2Q with selective/relaxed metadata updates
 * Simulates: __SELECTIVE_UPDATE__ flag
 * 
 * Behavior: Skips metadata updates when no new nodes are added to the tree.
 * This reduces overhead for read-heavy workloads where the cache is stable.
 */
template<typename Traits>
class A2QCacheRelaxed : public A2QCache<Traits> {
public:
    using Base = A2QCache<Traits>;
    using KeyType = typename Traits::KeyType;
    using ValueType = typename Traits::ValueType;
    using ObjectType = typename Traits::ObjectType;
    using ObjectTypePtr = typename Traits::ObjectType*;
    using StorageType = typename Traits::StorageType;
    
    template <typename... StorageArgs>
    A2QCacheRelaxed(size_t nCapacity, StorageArgs... args)
        : Base(nCapacity, args...)
    {}
    
    // Override updateObjectsAccessMetadata to implement relaxed behavior
    CacheErrorCode updateObjectsAccessMetadata(int nDepth, 
                                               std::vector<ObjectTypePtr>& vtObjects, 
                                               bool hasNewNodes = false) {
        // Relaxed mode: Skip metadata update if no new nodes were added
        // This is the key optimization for read-heavy workloads
        if (!hasNewNodes) {
            return CacheErrorCode::Success;
        }
        
        // If new nodes were added, perform normal metadata update
        return Base::updateObjectsAccessMetadata(nDepth, vtObjects, hasNewNodes);
    }
};

/**
 * A2QCacheWithGhostQueue: A2Q with ghost queue management
 * Simulates: __MANAGE_GHOST_Q__ flag
 * 
 * Behavior: Maintains a ghost queue to track recently evicted items,
 * improving hit rate for workloads with working set slightly larger than cache.
 */
template<typename Traits>
class A2QCacheWithGhostQueue : public A2QCache<Traits> {
public:
    using Base = A2QCache<Traits>;
    using KeyType = typename Traits::KeyType;
    using ValueType = typename Traits::ValueType;
    using ObjectType = typename Traits::ObjectType;
    using ObjectTypePtr = typename Traits::ObjectType*;
    using StorageType = typename Traits::StorageType;
    using ObjectUIDType = typename Traits::ObjectUIDType;
    
private:
    // Ghost queue: tracks UIDs of recently evicted items
    std::deque<ObjectUIDType> m_ghostQueue;
    std::unordered_set<ObjectUIDType> m_ghostSet;
    static constexpr size_t GHOST_QUEUE_SIZE_MULTIPLIER = 2;
    size_t m_maxGhostQueueSize;
    
public:
    template <typename... StorageArgs>
    A2QCacheWithGhostQueue(size_t nCapacity, StorageArgs... args)
        : Base(nCapacity, args...)
        , m_maxGhostQueueSize(nCapacity * GHOST_QUEUE_SIZE_MULTIPLIER)
    {}
    
    // Check if an item is in the ghost queue (was recently evicted)
    bool isInGhostQueue(const ObjectUIDType& uid) const {
        return m_ghostSet.find(uid) != m_ghostSet.end();
    }
    
    // Add evicted item to ghost queue
    void addToGhostQueue(const ObjectUIDType& uid) {
        if (m_ghostSet.find(uid) != m_ghostSet.end()) {
            return; // Already in ghost queue
        }
        
        // Add to ghost queue
        m_ghostQueue.push_back(uid);
        m_ghostSet.insert(uid);
        
        // Maintain size limit
        while (m_ghostQueue.size() > m_maxGhostQueueSize) {
            ObjectUIDType old_uid = m_ghostQueue.front();
            m_ghostQueue.pop_front();
            m_ghostSet.erase(old_uid);
        }
    }
    
    // Override eviction to track in ghost queue
    CacheErrorCode evict(ObjectTypePtr* pptrObject) {
        if (pptrObject && *pptrObject) {
            ObjectUIDType uid = (*pptrObject)->getObjectUID();
            addToGhostQueue(uid);
        }
        return Base::evict(pptrObject);
    }
};

// ============================================================================
// LRU Cache Variants
// ============================================================================

/**
 * LRUCacheInOrder: LRU with ordered metadata updates
 * Simulates: __UPDATE_IN_ORDER__ flag
 * 
 * Behavior: Ensures metadata updates happen in a specific order,
 * important for consistency in persistent memory scenarios.
 */
template<typename Traits>
class LRUCacheInOrder : public LRUCache<Traits> {
public:
    using Base = LRUCache<Traits>;
    using KeyType = typename Traits::KeyType;
    using ValueType = typename Traits::ValueType;
    using ObjectType = typename Traits::ObjectType;
    using ObjectTypePtr = typename Traits::ObjectType*;
    using StorageType = typename Traits::StorageType;
    
    template <typename... StorageArgs>
    LRUCacheInOrder(size_t nCapacity, StorageArgs... args)
        : Base(nCapacity, args...)
    {}
    
    // Override updateObjectsAccessMetadata to enforce ordering
    CacheErrorCode updateObjectsAccessMetadata(int nDepth, 
                                               std::vector<ObjectTypePtr>& vtObjects, 
                                               bool hasNewNodes = false) {
        // Sort objects by their position in the tree (depth-first order)
        // This ensures consistent ordering for persistent memory
        std::sort(vtObjects.begin(), vtObjects.end(), 
                 [](ObjectTypePtr a, ObjectTypePtr b) {
                     return a->getObjectUID() < b->getObjectUID();
                 });
        
        // Now perform metadata updates in order
        return Base::updateObjectsAccessMetadata(nDepth, vtObjects, hasNewNodes);
    }
};

/**
 * LRUCacheRelaxed: LRU with selective/relaxed metadata updates
 * Simulates: __SELECTIVE_UPDATE__ flag applied to LRU
 * 
 * Behavior: Similar to A2Q relaxed, skips updates when no new nodes.
 */
template<typename Traits>
class LRUCacheRelaxed : public LRUCache<Traits> {
public:
    using Base = LRUCache<Traits>;
    using KeyType = typename Traits::KeyType;
    using ValueType = typename Traits::ValueType;
    using ObjectType = typename Traits::ObjectType;
    using ObjectTypePtr = typename Traits::ObjectType*;
    using StorageType = typename Traits::StorageType;
    
    template <typename... StorageArgs>
    LRUCacheRelaxed(size_t nCapacity, StorageArgs... args)
        : Base(nCapacity, args...)
    {}
    
    CacheErrorCode updateObjectsAccessMetadata(int nDepth, 
                                               std::vector<ObjectTypePtr>& vtObjects, 
                                               bool hasNewNodes = false) {
        if (!hasNewNodes) {
            return CacheErrorCode::Success;
        }
        return Base::updateObjectsAccessMetadata(nDepth, vtObjects, hasNewNodes);
    }
};

// ============================================================================
// CLOCK Cache Variants
// ============================================================================

/**
 * CLOCKCacheWithBuffer: CLOCK with buffered eviction
 * Simulates: __CLOCK_WITH_BUFFER__ flag
 * 
 * Behavior: Buffers evicted items before writing to storage,
 * enabling batch I/O for better performance with disk/SSD.
 */
template<typename Traits>
class CLOCKCacheWithBuffer : public CLOCKCache<Traits> {
public:
    using Base = CLOCKCache<Traits>;
    using KeyType = typename Traits::KeyType;
    using ValueType = typename Traits::ValueType;
    using ObjectType = typename Traits::ObjectType;
    using ObjectTypePtr = typename Traits::ObjectType*;
    using StorageType = typename Traits::StorageType;
    
private:
    static constexpr size_t EVICTION_BUFFER_SIZE = 256;
    std::vector<ObjectTypePtr> m_evictionBuffer;
    
public:
    template <typename... StorageArgs>
    CLOCKCacheWithBuffer(size_t nCapacity, StorageArgs... args)
        : Base(nCapacity, args...)
    {
        m_evictionBuffer.reserve(EVICTION_BUFFER_SIZE);
    }
    
    // Buffer evicted items
    CacheErrorCode evict(ObjectTypePtr* pptrObject) {
        if (pptrObject && *pptrObject) {
            m_evictionBuffer.push_back(*pptrObject);
            
            // Flush buffer when full
            if (m_evictionBuffer.size() >= EVICTION_BUFFER_SIZE) {
                flushEvictionBuffer();
            }
        }
        return Base::evict(pptrObject);
    }
    
    // Flush buffered evictions to storage
    void flushEvictionBuffer() {
        if (m_evictionBuffer.empty()) {
            return;
        }
        
        // Batch write all buffered items
        for (auto ptr : m_evictionBuffer) {
            if (ptr && ptr->isDirty()) {
                Base::m_ptrStorage->write(ptr);
            }
        }
        
        m_evictionBuffer.clear();
    }
    
    ~CLOCKCacheWithBuffer() {
        flushEvictionBuffer();
    }
};

// ============================================================================
// Policy Factory: Creates the right cache variant based on configuration
// ============================================================================

template<typename Traits>
class PolicyFactory {
public:
    using CacheType = typename Traits::CacheType;
    using StorageType = typename Traits::StorageType;
    
    /**
     * Create a cache instance based on DeviceAwarePolicy configuration
     * 
     * @param config The policy configuration from DeviceAwarePolicy
     * @param capacity Cache capacity
     * @param args Storage constructor arguments
     * @return Unique pointer to the created cache instance
     */
    template<typename... StorageArgs>
    static std::unique_ptr<CacheType> createCache(
        const DeviceAware::PolicyConfig& config,
        size_t capacity,
        StorageArgs... args)
    {
        using namespace DeviceAware;
        
        // Select based on policy type and configuration
        if (config.policy_type == CachePolicyType::A2Q) {
            if (config.enable_selective_update) {
                // A2Q with relaxed updates
                return std::make_unique<A2QCacheRelaxed<Traits>>(capacity, args...);
            } else if (config.enable_manage_ghost_q) {
                // A2Q with ghost queue
                return std::make_unique<A2QCacheWithGhostQueue<Traits>>(capacity, args...);
            } else {
                // Standard A2Q
                return std::make_unique<A2QCache<Traits>>(capacity, args...);
            }
        }
        else if (config.policy_type == CachePolicyType::LRU) {
            if (config.enable_update_in_order) {
                // LRU with ordered updates
                return std::make_unique<LRUCacheInOrder<Traits>>(capacity, args...);
            } else if (config.enable_selective_update) {
                // LRU with relaxed updates
                return std::make_unique<LRUCacheRelaxed<Traits>>(capacity, args...);
            } else {
                // Standard LRU
                return std::make_unique<LRUCache<Traits>>(capacity, args...);
            }
        }
        else if (config.policy_type == CachePolicyType::CLOCK) {
            if (config.enable_clock_with_buffer) {
                // CLOCK with buffered eviction
                return std::make_unique<CLOCKCacheWithBuffer<Traits>>(capacity, args...);
            } else {
                // Standard CLOCK
                return std::make_unique<CLOCKCache<Traits>>(capacity, args...);
            }
        }
        
        // Default fallback: LRU
        return std::make_unique<LRUCache<Traits>>(capacity, args...);
    }
    
    /**
     * Convenience method: Create cache directly from workload and storage type
     */
    template<typename... StorageArgs>
    static std::unique_ptr<CacheType> createCacheForWorkload(
        const std::string& workload_str,
        const std::string& storage_str,
        size_t capacity,
        StorageArgs... args)
    {
        DeviceAware::DeviceAwarePolicy policy;
        auto workload = DeviceAware::DeviceAwarePolicy::parseWorkload(workload_str);
        auto storage = DeviceAware::DeviceAwarePolicy::parseStorage(storage_str);
        auto config = policy.selectPolicy(workload, storage);
        
        return createCache(config, capacity, args...);
    }
};

} // namespace PolicyVariants