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
#include "CacheErrorCodes.h"
#include "SIMDBitmapAllocator.hpp"

template <typename Traits>
class VolatileStorage 
{
public:
    using ObjectType = typename Traits::ObjectType;
    using ObjectTypePtr = ObjectType*;
    using ObjectUIDType = typename Traits::ObjectUIDType;

private:
    void* m_ptrStorage;
    SIMDBitmapAllocator* m_ptrAllocator;

#ifdef __CONCURRENT__
    mutable std::shared_mutex m_mtxStorage;
#endif

public:
    ~VolatileStorage() 
    {
        delete m_ptrAllocator;

#ifdef _WIN32
        _aligned_free(m_ptrStorage);
#else
        free(m_ptrStorage);
#endif
    }

    VolatileStorage(uint32_t nBlockSize, uint64_t nStorageSize)
    {
#ifndef _MSC_VER
        if (posix_memalign(&m_ptrStorage, 64, nStorageSize) != 0 || m_ptrStorage == nullptr)
#else
        m_ptrStorage = _aligned_malloc(nStorageSize, 64);
        if (!m_ptrStorage)
#endif
        {
            throw std::bad_alloc();
        }

        m_ptrAllocator = new SIMDBitmapAllocator(nBlockSize, nStorageSize);
    }

public:
    template <typename... InitArgs>
    CacheErrorCode init(InitArgs... args) 
    {
        return CacheErrorCode::Success;
    }

    CacheErrorCode remove(const ObjectUIDType& uidObject) 
    {
#ifdef __CONCURRENT__
        std::unique_lock<std::shared_mutex> lock_storage(m_mtxStorage);
#endif

        m_ptrAllocator->free(uidObject.getPersistentPointerValue(), uidObject.getPersistentObjectSize());

        return CacheErrorCode::Success;
    }

    inline char* GetStoragePtr(uint64_t nOffset) 
    {
        return static_cast<char*>(m_ptrStorage) + nOffset;
    }

    CacheErrorCode getObject(uint16_t nDegree, const ObjectUIDType& uidObject, ObjectTypePtr& ptrObject) 
    {
        return ptrObject->updateCoreObject(nDegree, GetStoragePtr(uidObject.getPersistentPointerValue()), uidObject, uidObject.getPersistentObjectSize(), m_ptrAllocator->m_nBlockSize);
    }

    ObjectTypePtr getObject(uint16_t nDegree, const ObjectUIDType& uidObject) 
    {
        return new ObjectType(nDegree, uidObject, GetStoragePtr(uidObject.getPersistentPointerValue()), uidObject.getPersistentObjectSize(), m_ptrAllocator->m_nBlockSize);
    }

    CacheErrorCode addObject(ObjectTypePtr& ptrObject, ObjectUIDType& uidUpdated) 
    {
        uint32_t nBufferSize = 0;
        bool bAlignedAllocation = false;

        char* szBuffer = nullptr;
        void* ptrOffset = nullptr;

        const ObjectUIDType& uidObject = ptrObject->m_uid;

        ptrObject->serialize(szBuffer, nBufferSize, m_ptrAllocator->m_nBlockSize, ptrOffset, bAlignedAllocation);

#ifdef __CONCURRENT__
        std::unique_lock<std::shared_mutex> lock_storage(m_mtxStorage);
#endif
        if (ptrOffset == nullptr) 
        {
            auto oResult = m_ptrAllocator->allocate(nBufferSize);
            if (!oResult) 
            {
#ifdef __CONCURRENT__
                lock_storage.unlock();
#endif
                if (!bAlignedAllocation)
                {
                    delete[] szBuffer;
                }
                else 
                {
#ifdef _MSC_VER
                    _aligned_free(szBuffer);
#else
                    free(szBuffer);
#endif
                }
                return CacheErrorCode::OutOfStorage;
            }

            uint64_t nOffset = *oResult;

            //memset(GetStoragePtr(nOffset), 0, nBlocksRequired*m_nBlockSize);
            memcpy(GetStoragePtr(nOffset), szBuffer, nBufferSize);

            if (!bAlignedAllocation)
            {
                delete[] szBuffer;
            }
            else 
            {
#ifdef _MSC_VER
                _aligned_free(szBuffer);
#else
                free(szBuffer);
#endif
            }

            ObjectUIDType::createAddressFromDRAMCacheCounter(uidUpdated, uidObject.getObjectType(), nOffset, nBufferSize);

            if (uidObject.isPersistedObject())
            {
                if (!m_ptrAllocator->free(uidObject.getPersistentPointerValue(), uidObject.getPersistentObjectSize()))
                {
#ifdef __CONCURRENT__
                    lock_storage.unlock();
#endif
                    return CacheErrorCode::Error;
                }
            }
#ifdef __CONCURRENT__
            lock_storage.unlock();
#endif
        }
        else 
        {
            uidUpdated = uidObject;
            memcpy(ptrOffset, szBuffer, nBufferSize);
#ifdef __ENABLE_ASSERTS__
            auto ptrTmp = std::make_shared<ObjectType>(3, uidObject, GetStoragePtr(uidObject.getPersistentPointerValue()), uidObject.getPersistentObjectSize() + nBufferSize, m_ptrAllocator->m_nBlockSize);
            ptrObject->validate(ptrTmp);
#endif
#ifdef __CONCURRENT__
            lock_storage.unlock();
#endif
            if (!bAlignedAllocation)
            {
                delete[] szBuffer;
            }
            else 
            {
#ifdef _MSC_VER
                _aligned_free(szBuffer);
#else
                free(szBuffer);
#endif
            }
        }
        return CacheErrorCode::Success;
    }
};
