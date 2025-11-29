#pragma once
#include <memory>
#include <iostream>
#include <fcntl.h>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <variant>
#include <cmath>
#include <vector>
#include <optional>
#include <chrono>
#include "CacheErrorCodes.h"

/**
 * DualNodeStorage - A storage class that routes B+-tree nodes to different storage tiers
 * 
 * This class implements a dual-tier storage architecture where:
 * - IndexNodes (internal nodes) are stored in Primary Storage (e.g., VolatileStorage - fast DRAM)
 * - DataNodes (leaf nodes) are stored in Secondary Storage (e.g., FileStorage - slower but persistent)
 * 
 * The routing is based on the node type encoded in the ObjectUID.
 * This enables cost-aware caching policies by tracking access costs per storage tier.
 * 
 * Template Parameters:
 * - Traits: BPlusStoreTraits containing node type definitions
 * - TPrimaryStorage: Storage class for IndexNodes (e.g., VolatileStorage)
 * - TSecondaryStorage: Storage class for DataNodes (e.g., FileStorage)
 */
template <
    typename Traits,
    template<typename> class TPrimaryStorage,
    template<typename> class TSecondaryStorage
>
class DualNodeStorage
{
public:
    using ObjectType = typename Traits::ObjectType;
    using ObjectTypePtr = ObjectType*;
    using ObjectUIDType = typename Traits::ObjectUIDType;
    
    using PrimaryStorageType = TPrimaryStorage<Traits>;
    using SecondaryStorageType = TSecondaryStorage<Traits>;
    
    /**
     * Configuration structure for dual storage initialization
     */
    struct Config {
        // Primary storage configuration (for IndexNodes)
        uint32_t primaryBlockSize;
        uint64_t primaryStorageSize;
        
        // Secondary storage configuration (for DataNodes)
        uint32_t secondaryBlockSize;
        uint64_t secondaryStorageSize;
        std::string secondaryPath;  // File path or PMem path
        
        // Cost configuration (in nanoseconds or arbitrary units)
        uint64_t primaryAccessCost;   // e.g., 10 ns for DRAM
        uint64_t secondaryAccessCost; // e.g., 1000 ns for File I/O
        
        Config(
            uint32_t primBlockSize, uint64_t primStorageSize,
            uint32_t secBlockSize, uint64_t secStorageSize, const std::string& secPath,
            uint64_t primCost = 10, uint64_t secCost = 1000
        ) : primaryBlockSize(primBlockSize), primaryStorageSize(primStorageSize),
            secondaryBlockSize(secBlockSize), secondaryStorageSize(secStorageSize),
            secondaryPath(secPath),
            primaryAccessCost(primCost), secondaryAccessCost(secCost)
        {}
    };
    
    /**
     * Storage cost and statistics tracking
     */
    struct StorageCostInfo {
        // Configured access costs
        uint64_t primaryAccessCost;
        uint64_t secondaryAccessCost;
        
        // Access statistics
        std::atomic<uint64_t> primaryReadCount{0};
        std::atomic<uint64_t> secondaryReadCount{0};
        std::atomic<uint64_t> primaryWriteCount{0};
        std::atomic<uint64_t> secondaryWriteCount{0};
        
        // Total cost accumulation
        std::atomic<uint64_t> totalPrimaryCost{0};
        std::atomic<uint64_t> totalSecondaryCost{0};
        
        StorageCostInfo(uint64_t primCost, uint64_t secCost)
            : primaryAccessCost(primCost), secondaryAccessCost(secCost)
        {}
    };

private:
    PrimaryStorageType* m_ptrPrimaryStorage;
    SecondaryStorageType* m_ptrSecondaryStorage;
    
    StorageCostInfo m_costInfo;
    
#ifdef __CONCURRENT__
    mutable std::shared_mutex m_mtxStorage;
#endif

public:
    /**
     * Destructor - Clean up both storage instances
     */
    ~DualNodeStorage()
    {
        delete m_ptrPrimaryStorage;
        delete m_ptrSecondaryStorage;
    }
    
    /**
     * Constructor - Initialize both storage tiers
     * 
     * @param config Configuration struct containing parameters for both storage tiers
     */
    DualNodeStorage(const Config& config)
        : m_costInfo(config.primaryAccessCost, config.secondaryAccessCost)
    {
        // Initialize primary storage (for IndexNodes)
        m_ptrPrimaryStorage = new PrimaryStorageType(
            config.primaryBlockSize,
            config.primaryStorageSize
        );
        
        // Initialize secondary storage (for DataNodes)
        m_ptrSecondaryStorage = new SecondaryStorageType(
            config.secondaryBlockSize,
            config.secondaryStorageSize,
            config.secondaryPath
        );
    }
    
    /**
     * Alternative constructor for backward compatibility
     * Accepts individual parameters instead of config struct
     */
    DualNodeStorage(
        uint32_t primaryBlockSize,
        uint64_t primaryStorageSize,
        uint32_t secondaryBlockSize,
        uint64_t secondaryStorageSize,
        const std::string& secondaryPath,
        uint64_t primaryCost = 10,
        uint64_t secondaryCost = 1000
    ) : m_costInfo(primaryCost, secondaryCost)
    {
        m_ptrPrimaryStorage = new PrimaryStorageType(
            primaryBlockSize,
            primaryStorageSize
        );
        
        m_ptrSecondaryStorage = new SecondaryStorageType(
            secondaryBlockSize,
            secondaryStorageSize,
            secondaryPath
        );
    }

public:
    /**
     * Initialize storage (for interface compatibility)
     */
    template <typename... InitArgs>
    CacheErrorCode init(InitArgs... args)
    {
        return CacheErrorCode::Success;
    }
    
    /**
     * Remove an object from storage
     * Routes to appropriate storage based on node type in UID
     * 
     * @param uidObject UID of the object to remove
     * @return CacheErrorCode indicating success or failure
     */
    CacheErrorCode remove(const ObjectUIDType& uidObject)
    {
        uint8_t nodeType = uidObject.getObjectType();
        
        if (nodeType == Traits::IndexNodeUID) {
            return m_ptrPrimaryStorage->remove(uidObject);
        }
        else if (nodeType == Traits::DataNodeUID) {
            return m_ptrSecondaryStorage->remove(uidObject);
        }
        
        return CacheErrorCode::Error;
    }
    
    /**
     * Retrieve an object from storage (in-place update)
     * Routes to appropriate storage based on node type in UID
     * 
     * @param nDegree B+-tree degree
     * @param uidObject UID of the object to retrieve
     * @param ptrObject Pointer to object to update with retrieved data
     * @return CacheErrorCode indicating success or failure
     */
    CacheErrorCode getObject(
        uint16_t nDegree,
        const ObjectUIDType& uidObject,
        ObjectTypePtr& ptrObject
    )
    {
        uint8_t nodeType = uidObject.getObjectType();
        
        if (nodeType == Traits::IndexNodeUID) {
            // Route to primary storage (IndexNodes)
            m_costInfo.primaryReadCount.fetch_add(1, std::memory_order_relaxed);
            m_costInfo.totalPrimaryCost.fetch_add(
                m_costInfo.primaryAccessCost,
                std::memory_order_relaxed
            );
            return m_ptrPrimaryStorage->getObject(nDegree, uidObject, ptrObject);
        }
        else if (nodeType == Traits::DataNodeUID) {
            // Route to secondary storage (DataNodes)
            m_costInfo.secondaryReadCount.fetch_add(1, std::memory_order_relaxed);
            m_costInfo.totalSecondaryCost.fetch_add(
                m_costInfo.secondaryAccessCost,
                std::memory_order_relaxed
            );
            return m_ptrSecondaryStorage->getObject(nDegree, uidObject, ptrObject);
        }
        
        return CacheErrorCode::Error;
    }
    
    /**
     * Retrieve an object from storage (returns new pointer)
     * Routes to appropriate storage based on node type in UID
     * 
     * @param nDegree B+-tree degree
     * @param uidObject UID of the object to retrieve
     * @return Pointer to newly created object, or nullptr on error
     */
    ObjectTypePtr getObject(uint16_t nDegree, const ObjectUIDType& uidObject)
    {
        uint8_t nodeType = uidObject.getObjectType();
        
        if (nodeType == Traits::IndexNodeUID) {
            // Route to primary storage (IndexNodes)
            m_costInfo.primaryReadCount.fetch_add(1, std::memory_order_relaxed);
            m_costInfo.totalPrimaryCost.fetch_add(
                m_costInfo.primaryAccessCost,
                std::memory_order_relaxed
            );
            return m_ptrPrimaryStorage->getObject(nDegree, uidObject);
        }
        else if (nodeType == Traits::DataNodeUID) {
            // Route to secondary storage (DataNodes)
            m_costInfo.secondaryReadCount.fetch_add(1, std::memory_order_relaxed);
            m_costInfo.totalSecondaryCost.fetch_add(
                m_costInfo.secondaryAccessCost,
                std::memory_order_relaxed
            );
            return m_ptrSecondaryStorage->getObject(nDegree, uidObject);
        }
        
        return nullptr;
    }
    
    /**
     * Add/persist an object to storage
     * Routes to appropriate storage based on node type in the object
     * 
     * @param ptrObject Pointer to object to persist
     * @param uidUpdated Output parameter - updated UID with storage location
     * @return CacheErrorCode indicating success or failure
     */
    CacheErrorCode addObject(ObjectTypePtr& ptrObject, ObjectUIDType& uidUpdated)
    {
        // Get node type from the object itself (not from UID, as it may not be set yet)
        uint8_t nodeType = ptrObject->m_nCoreObjectType;
        
        if (nodeType == Traits::IndexNodeUID) {
            // Route to primary storage (IndexNodes)
            m_costInfo.primaryWriteCount.fetch_add(1, std::memory_order_relaxed);
            m_costInfo.totalPrimaryCost.fetch_add(
                m_costInfo.primaryAccessCost,
                std::memory_order_relaxed
            );
            return m_ptrPrimaryStorage->addObject(ptrObject, uidUpdated);
        }
        else if (nodeType == Traits::DataNodeUID) {
            // Route to secondary storage (DataNodes)
            m_costInfo.secondaryWriteCount.fetch_add(1, std::memory_order_relaxed);
            m_costInfo.totalSecondaryCost.fetch_add(
                m_costInfo.secondaryAccessCost,
                std::memory_order_relaxed
            );
            return m_ptrSecondaryStorage->addObject(ptrObject, uidUpdated);
        }
        
        return CacheErrorCode::Error;
    }
    
    /**
     * Get access cost for a specific node type
     * 
     * @param nodeType Node type identifier (IndexNodeUID or DataNodeUID)
     * @return Access cost in configured units (e.g., nanoseconds)
     */
    uint64_t getAccessCost(uint8_t nodeType) const
    {
        if (nodeType == Traits::IndexNodeUID) {
            return m_costInfo.primaryAccessCost;
        }
        else if (nodeType == Traits::DataNodeUID) {
            return m_costInfo.secondaryAccessCost;
        }
        return 0;
    }
    
    /**
     * Get access cost from a UID
     * 
     * @param uidObject UID containing node type
     * @return Access cost in configured units
     */
    uint64_t getAccessCostFromUID(const ObjectUIDType& uidObject) const
    {
        return getAccessCost(uidObject.getObjectType());
    }
    
    /**
     * Get reference to cost information structure
     * Useful for collecting statistics
     */
    const StorageCostInfo& getCostInfo() const
    {
        return m_costInfo;
    }
    
    /**
     * Print storage statistics
     */
    void printStatistics(std::ostream& os = std::cout) const
    {
        os << "=== DualNodeStorage Statistics ===" << std::endl;
        os << "Primary Storage (IndexNodes):" << std::endl;
        os << "  Reads:  " << m_costInfo.primaryReadCount.load() << std::endl;
        os << "  Writes: " << m_costInfo.primaryWriteCount.load() << std::endl;
        os << "  Total Cost: " << m_costInfo.totalPrimaryCost.load() << " units" << std::endl;
        
        os << "Secondary Storage (DataNodes):" << std::endl;
        os << "  Reads:  " << m_costInfo.secondaryReadCount.load() << std::endl;
        os << "  Writes: " << m_costInfo.secondaryWriteCount.load() << std::endl;
        os << "  Total Cost: " << m_costInfo.totalSecondaryCost.load() << " units" << std::endl;
        
        uint64_t totalReads = m_costInfo.primaryReadCount.load() + 
                              m_costInfo.secondaryReadCount.load();
        uint64_t totalWrites = m_costInfo.primaryWriteCount.load() + 
                               m_costInfo.secondaryWriteCount.load();
        uint64_t totalCost = m_costInfo.totalPrimaryCost.load() + 
                             m_costInfo.totalSecondaryCost.load();
        
        os << "Total Operations: " << (totalReads + totalWrites) << std::endl;
        os << "Total Cost: " << totalCost << " units" << std::endl;
        
        if (totalReads + totalWrites > 0) {
            double avgCost = static_cast<double>(totalCost) / (totalReads + totalWrites);
            os << "Average Cost per Operation: " << avgCost << " units" << std::endl;
        }
        os << "===================================" << std::endl;
    }
    
    /**
     * Reset statistics counters
     */
    void resetStatistics()
    {
        m_costInfo.primaryReadCount.store(0);
        m_costInfo.secondaryReadCount.store(0);
        m_costInfo.primaryWriteCount.store(0);
        m_costInfo.secondaryWriteCount.store(0);
        m_costInfo.totalPrimaryCost.store(0);
        m_costInfo.totalSecondaryCost.store(0);
    }
};