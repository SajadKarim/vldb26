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

#include "VolatileStorage.hpp"
#include "PMemStorage.hpp"
#include "FileStorage.hpp"

template <typename Traits>
class HybridStorage
{
public:
    using ObjectType = typename Traits::ObjectType;
    using ObjectTypePtr = std::shared_ptr<ObjectType>;
    using ObjectUIDType = typename Traits::ObjectUIDType;

    typedef VolatileStorage<Traits> L1Storage;
#ifndef _MSC_VER
    typedef PMemStorage<Traits> L2Storage;
#endif //_MSC_VER
    typedef FileStorage<Traits> L3Storage;

private:
    void* m_ptrStorage;
    size_t m_nStorageSize;

    size_t m_nBlockSize;

    L1Storage* m_ptrL1Storage;
#ifndef _MSC_VER
    L2Storage* m_ptrL2Storage;
#endif //_MSC_VER
    L3Storage* m_ptrL3Storage;

#ifdef __CONCURRENT__
    mutable std::shared_mutex m_mtxStorage;
#endif

public:
    ~HybridStorage()
    {
        delete m_ptrL1Storage;
#ifndef _MSC_VER
        delete m_ptrL2Storage;
#endif //_MSC_VER
        delete m_ptrL3Storage;

#ifdef _WIN32
        _aligned_free(m_ptrStorage);
#else
        free(m_ptrStorage);
#endif
    }

    HybridStorage(size_t nBlockSize, size_t nStorageSize, const std::string& stPMemPath, const std::string& stBlockDevicePath)
        : m_nStorageSize(nStorageSize)
        , m_nBlockSize(nBlockSize)
    {
        m_ptrL1Storage = new L1Storage(nBlockSize, nStorageSize);
#ifndef _MSC_VER
        m_ptrL2Storage = new L2Storage(nBlockSize, nStorageSize, stPMemPath);
#endif //_MSC_VER
        m_ptrL3Storage = new L3Storage(nBlockSize, nStorageSize, stBlockDevicePath);
    }

public:
    template <typename... InitArgs>
    CacheErrorCode init(InitArgs... args)
    {
        return CacheErrorCode::Success;
    }

    CacheErrorCode remove(const ObjectUIDType& uidObject)
    {
        switch (uidObject.getMediaType())
        {
            case ObjectUIDType::StorageMedia::DRAM:
                return m_ptrL1Storage->remove(uidObject);
#ifndef _MSC_VER
            case ObjectUIDType::StorageMedia::PMem:
                return m_ptrL2Storage->remove(uidObject);
#endif //_MSC_VER
            case ObjectUIDType::StorageMedia::File:
                return m_ptrL3Storage->remove(uidObject);
            default:
                break;
        }

        return CacheErrorCode::Error;
    }

    CacheErrorCode getObject(uint16_t nDegree, const ObjectUIDType& uidObject, ObjectTypePtr& ptrObject)
    {
        switch (uidObject.getMediaType())
        {
            case ObjectUIDType::StorageMedia::DRAM:
                return m_ptrL1Storage->getObject(nDegree, uidObject, ptrObject);
#ifndef _MSC_VER
            case ObjectUIDType::StorageMedia::PMem:
                return m_ptrL2Storage->getObject(nDegree, uidObject, ptrObject);
#endif //_MSC_VER
            case ObjectUIDType::StorageMedia::File:
                return m_ptrL3Storage->getObject(nDegree, uidObject, ptrObject);
            default:
                break;
        }

        //ptrObject->updateCoreObject(nDegree, GetStoragePtr(uidObject.getPersistentPointerValue()), uidObject, uidObject.getPersistentObjectSize(), (uint16_t)m_nBlockSize);
        return CacheErrorCode::Error;
    }

    ObjectTypePtr getObject(uint16_t nDegree, const ObjectUIDType& uidObject)
    {
        switch (uidObject.getMediaType())
        {
        case ObjectUIDType::StorageMedia::DRAM:
            return m_ptrL1Storage->getObject(nDegree, uidObject);
#ifndef _MSC_VER
        case ObjectUIDType::StorageMedia::PMem:
            return m_ptrL2Storage->getObject(nDegree, uidObject);
#endif //_MSC_VER
        case ObjectUIDType::StorageMedia::File:
            return m_ptrL3Storage->getObject(nDegree, uidObject);
        default:
            break;
        }

        //return std::make_shared<ObjectType>(nDegree, uidObject, GetStoragePtr(uidObject.getPersistentPointerValue()), uidObject.getPersistentObjectSize(), (uint16_t)m_nBlockSize);
        return nullptr;
    }

    CacheErrorCode addObject(const ObjectUIDType& uidObject, ObjectTypePtr ptrObject, ObjectUIDType& uidUpdated)
    {
        CacheErrorCode nResult = m_ptrL1Storage->addObject(uidObject, ptrObject, uidUpdated);
        if (nResult != CacheErrorCode::Success)
        {
#ifndef _MSC_VER
            nResult = m_ptrL2Storage->addObject(uidObject, ptrObject, uidUpdated);
            if (nResult != CacheErrorCode::Success)
#endif //_MSC_VER
            {
                return m_ptrL3Storage->addObject(uidObject, ptrObject, uidUpdated);
            }
        }

        return CacheErrorCode::Success;
    }
};
