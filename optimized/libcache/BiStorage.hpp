#pragma once

#include <memory>
#include <atomic>
#include <iostream>
#include <string>
#include "CacheErrorCodes.h"

/**
 * @file BiStorage.hpp
 * @brief Bi-tier storage implementation that routes IndexNodes and DataNodes to different storage backends
 * 
 * BiStorage provides a dual-tier storage architecture for B+-tree nodes:
 * - IndexNodes (internal nodes) → Primary Storage (typically fast DRAM via VolatileStorage)
 * - DataNodes (leaf nodes) → Secondary Storage (typically persistent storage via PMemStorage or FileStorage)
 * 
 * This separation enables:
 * 1. Faster index traversal (IndexNodes in fast DRAM)
 * 2. Persistent data storage (DataNodes in PMem/File)
 * 3. Cost-aware caching (different access costs per tier)
 * 4. Better cache utilization (cache can focus on high-cost DataNodes)
 * 
 * The routing is transparent to cache policies (LRU, CLOCK, A2Q) and uses the existing
 * UID system to track which storage tier holds each object.
 * 
 * @tparam Traits The traits type defining ObjectType, ObjectUIDType, etc.
 * @tparam TPrimaryStorage Storage type for IndexNodes (e.g., VolatileStorage)
 * @tparam TSecondaryStorage Storage type for DataNodes (e.g., PMemStorage, FileStorage)
 */
template <typename Traits, template<typename> class TPrimaryStorage, template<typename> class TSecondaryStorage>
class BiStorage
{
public:
    using ObjectType = typename Traits::ObjectType;
    using ObjectTypePtr = ObjectType*;
    using ObjectUIDType = typename Traits::ObjectUIDType;
    using PrimaryStorageType = TPrimaryStorage<Traits>;
    using SecondaryStorageType = TSecondaryStorage<Traits>;

    private:
    PrimaryStorageType* m_ptrPrimaryStorage;      // Storage for IndexNodes (fast)
    SecondaryStorageType* m_ptrSecondaryStorage;  // Storage for DataNodes (persistent)
    
    // Cost per operation (in nanoseconds)
    uint64_t m_nPrimaryReadCost;
    uint64_t m_nPrimaryWriteCost;
    uint64_t m_nSecondaryReadCost;
    uint64_t m_nSecondaryWriteCost;

#ifdef __CONCURRENT__
    mutable std::shared_mutex m_mtxStorage;
#endif

public:
    /**
     * @brief Destructor - cleans up both storage tiers
     */
    ~BiStorage()
    {
        delete m_ptrPrimaryStorage;
        delete m_ptrSecondaryStorage;
    }

    BiStorage(uint64_t nPrimaryReadCost
        , uint64_t nPrimaryWriteCost
        , uint64_t nSecondaryReadCost
        , uint64_t nSecondaryWriteCost
        , uint32_t nPrimaryBlockSize
        , uint64_t nPrimaryStorageSize
        , uint32_t nSecondaryBlockSize
        , uint64_t nSecondaryStorageSize
        , std::string szPrimaryFilePath
        , std::string szSecondaryFilePath)
        : m_nPrimaryReadCost(nPrimaryReadCost)
        , m_nPrimaryWriteCost(nPrimaryWriteCost)
        , m_nSecondaryReadCost(nSecondaryReadCost)
        , m_nSecondaryWriteCost(nSecondaryWriteCost)
    {
        // Create primary storage
        // VolatileStorage takes 2 params, PMemStorage/FileStorage take 3 params (with path)
        if constexpr (std::is_same_v<PrimaryStorageType, VolatileStorage<Traits>>) {
            m_ptrPrimaryStorage = new PrimaryStorageType(
                nPrimaryBlockSize,
                nPrimaryStorageSize
            );
        } else {
            // PMemStorage or FileStorage - needs path
            m_ptrPrimaryStorage = new PrimaryStorageType(
                nPrimaryBlockSize,
                nPrimaryStorageSize,
                szPrimaryFilePath
            );
        }
        
        // Create secondary storage
        // VolatileStorage takes 2 params, PMemStorage/FileStorage take 3 params (with path)
        if constexpr (std::is_same_v<SecondaryStorageType, VolatileStorage<Traits>>) {
            m_ptrSecondaryStorage = new SecondaryStorageType(
                nSecondaryBlockSize,
                nSecondaryStorageSize
            );
        } else {
            // PMemStorage or FileStorage - needs path
            m_ptrSecondaryStorage = new SecondaryStorageType(
                nSecondaryBlockSize,
                nSecondaryStorageSize,
                szSecondaryFilePath
            );
        }
    }

    /**
     * @brief Initialize storage (called by cache after construction)
     */
    template <typename... InitArgs>
    CacheErrorCode init(InitArgs... args)
    {
        CacheErrorCode result = m_ptrPrimaryStorage->init(args...);
        if (result != CacheErrorCode::Success) {
            return result;
        }
        return m_ptrSecondaryStorage->init(args...);
    }

    /**
     * @brief Remove an object from storage
     * 
     * Routes to appropriate storage based on object's node type in UID.
     * 
     * @param uidObject UID of the object to remove
     * @return CacheErrorCode indicating success or failure
     */
    CacheErrorCode remove(const ObjectUIDType& uidObject)
    {
        uint8_t nodeType = uidObject.getObjectType();
        
        if (nodeType == Traits::IndexNodeUID) {
            // IndexNode → Primary Storage
            return m_ptrPrimaryStorage->remove(uidObject);
        } else if (nodeType == Traits::DataNodeUID) {
            // DataNode → Secondary Storage
            return m_ptrSecondaryStorage->remove(uidObject);
        }
        
        return CacheErrorCode::Error;
    }

    /**
     * @brief Get an object from storage (in-place update variant)
     * 
     * Routes to appropriate storage based on object's node type in UID.
     * Tracks read statistics for cost analysis.
     * 
     * @param nDegree Tree degree
     * @param uidObject UID of the object to retrieve
     * @param ptrObject Pointer to object to update in-place
     * @return CacheErrorCode indicating success or failure
     */
    CacheErrorCode getObject(uint16_t nDegree, const ObjectUIDType& uidObject, ObjectTypePtr& ptrObject)
    {
        uint8_t nodeType = uidObject.getObjectType();
        
        if (nodeType == Traits::IndexNodeUID) {
            // IndexNode → Primary Storage
            return m_ptrPrimaryStorage->getObject(nDegree, uidObject, ptrObject);
        } else if (nodeType == Traits::DataNodeUID) {
            // DataNode → Secondary Storage
            return m_ptrSecondaryStorage->getObject(nDegree, uidObject, ptrObject);
        }
        
        return CacheErrorCode::Error;
    }

    /**
     * @brief Get an object from storage (new object variant)
     * 
     * Routes to appropriate storage based on object's node type in UID.
     * Tracks read statistics for cost analysis.
     * 
     * @param nDegree Tree degree
     * @param uidObject UID of the object to retrieve
     * @return Pointer to newly created object
     */
    ObjectTypePtr getObject(uint16_t nDegree, const ObjectUIDType& uidObject)
    {
        uint8_t nodeType = uidObject.getObjectType();
        
        if (nodeType == Traits::IndexNodeUID) {
            // IndexNode → Primary Storage
            return m_ptrPrimaryStorage->getObject(nDegree, uidObject);
        } else if (nodeType == Traits::DataNodeUID) {
            // DataNode → Secondary Storage
            return m_ptrSecondaryStorage->getObject(nDegree, uidObject);
        }
        
        return nullptr;
    }

    /**
     * @brief Add an object to storage
     * 
     * Routes to appropriate storage based on object's node type.
     * The underlying storage will update uidUpdated with the correct media type.
     * Tracks write statistics for cost analysis.
     * 
     * @param ptrObject Pointer to the object to add
     * @param uidUpdated UID that will be updated with storage location
     * @return CacheErrorCode indicating success or failure
     */
    CacheErrorCode addObject(ObjectTypePtr& ptrObject, ObjectUIDType& uidUpdated)
    {
        // Get node type from the object itself
        uint8_t nodeType = ptrObject->m_nCoreObjectType;
        
        if (nodeType == Traits::IndexNodeUID) {
            // IndexNode → Primary Storage
            return m_ptrPrimaryStorage->addObject(ptrObject, uidUpdated);
        } else if (nodeType == Traits::DataNodeUID) {
            // DataNode → Secondary Storage
            return m_ptrSecondaryStorage->addObject(ptrObject, uidUpdated);
        }
        
        return CacheErrorCode::Error;
    }

    // ========================================
    // Cost Tracking and Statistics
    // ========================================

    /**
     * @brief Get total accumulated cost for primary storage
     * @return Total cost in nanoseconds
     */
    uint64_t getPrimaryCost() const
    {
        return (m_nPrimaryReadCost + m_nPrimaryWriteCost)/2;
    }

    /**
     * @brief Get total accumulated cost for secondary storage
     * @return Total cost in nanoseconds
     */
    uint64_t getSecondaryCost() const
    {
        return (m_nSecondaryReadCost + m_nSecondaryWriteCost) / 2;
    }

    /**
     * @brief Get access cost for a specific node type
     * @param nodeType The node type (IndexNodeUID or DataNodeUID)
     * @return Read cost in nanoseconds
     */
    uint64_t getAccessCost(uint8_t nodeType) const
    {
        if (nodeType == Traits::IndexNodeUID) {
            return m_nPrimaryReadCost;
        } else if (nodeType == Traits::DataNodeUID) {
            return m_nSecondaryReadCost;
        }
        return 0;
    }
};