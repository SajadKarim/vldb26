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
class PMemStorage
{
public:
	bool createMMapFile(void*& hMemory, const char* szPath, size_t nFileSize, size_t& nMappedLen, int& bIsPMem)
	{
#ifndef _MSC_VER
		if ((hMemory = pmem_map_file(szPath,
			nFileSize,
			PMEM_FILE_CREATE | PMEM_FILE_EXCL,
			0666, &nMappedLen, &bIsPMem)) == NULL)
		{
			return false;
		}
#endif //_MSC_VER

		return true;
	}

	bool openMMapFile(void*& hMemory, const char* szPath, size_t& nMappedLen, int& bIsPMem)
	{
#ifndef _MSC_VER
		if ((hMemory = pmem_map_file(szPath,
			0,
			0,
			0666, &nMappedLen, &bIsPMem)) == NULL)
		{
			return false;
		}
#endif //_MSC_VER

		return true;
	}

	bool writeMMapFile(void* hMemory, const char* szBuf, size_t nLen)
	{
#ifndef _MSC_VER
		void* hDestBuf = pmem_memcpy_persist(hMemory, szBuf, nLen);

		if (hDestBuf == NULL)
		{
			return false;
		}
		//pmem_drain();
#endif //_MSC_VER

		return true;
	}

	bool readMMapFile(const void* hMemory, char* szBuf, size_t nLen)
	{
#ifndef _MSC_VER
		void* hDestBuf = pmem_memcpy(szBuf, hMemory, nLen, PMEM_F_MEM_NOFLUSH);

		if (hDestBuf == NULL)
		{
			return false;
		}
#endif //_MSC_VER

		return true;
	}

	void closeMMapFile(void* hMemory, size_t nMappedLen)
	{
#ifndef _MSC_VER
		pmem_unmap(hMemory, nMappedLen);
#endif //_MSC_VER
	}

public:
	using ObjectType = typename Traits::ObjectType;
	using ObjectTypePtr = ObjectType*;
	using ObjectUIDType = typename Traits::ObjectUIDType;

private:
	int nIsPMem;
	size_t m_nMappedLen;

	void* m_hMemory = NULL;
	std::string m_stFilename;

	SIMDBitmapAllocator* m_ptrAllocator;

#ifdef __CONCURRENT__
	mutable std::shared_mutex m_mtxStorage;
#endif //__CONCURRENT__

public:
	~PMemStorage()
	{
		delete m_ptrAllocator;

		closeMMapFile(m_hMemory, m_nMappedLen);
	}

	PMemStorage(uint32_t nBlockSize, uint64_t nStorageSize, const std::string& stFilename)
		: m_nMappedLen(0)
		, m_hMemory(nullptr)
		, m_stFilename(stFilename)
	{
		if( !openMMapFile(m_hMemory, stFilename.c_str(), m_nMappedLen, nIsPMem))
		{
			if( !createMMapFile(m_hMemory, stFilename.c_str(), nStorageSize, m_nMappedLen, nIsPMem))
			{
				throw new std::logic_error("Critical State: Failed to create mmap file for PMemStorage.");
			}
		}

		ASSERT(m_hMemory != nullptr);

		ASSERT(m_nMappedLen == nStorageSize);

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
#endif //__CONCURRENT__

		m_ptrAllocator->free(uidObject.getPersistentPointerValue(), uidObject.getPersistentObjectSize());

		return CacheErrorCode::Success;
	}

	inline char* GetStoragePtr(size_t nOffset)
	{
		return (char*)m_hMemory + nOffset;
	}

	CacheErrorCode getObject(uint16_t nDegree, const ObjectUIDType& uidObject,ObjectTypePtr& ptrObject)
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
#endif //__CONCURRENT__
		
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

#ifndef _MSC_VER
			if (!writeMMapFile(m_hMemory + nOffset, szBuffer, nBufferSize))
#endif //_MSC_VER
			{
				throw new std::logic_error("Critical State: Failed to write object to PMemStorage.");   // TODO: critical log.
			}

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

			ObjectUIDType::createAddressFromPMemOffset(uidUpdated, uidObject.getObjectType(), nOffset, nBufferSize);

			if (uidObject.isPersistedObject())
			{
				m_ptrAllocator->free(uidObject.getPersistentPointerValue(), uidObject.getPersistentObjectSize());
			}
		}
		else
		{
// 			uidUpdated = uidObject;

// 			memcpy(ptrOffset, szBuffer, nBufferSize);

// #ifdef __ENABLE_ASSERTS__
// 			ObjectTypePtr ptrTmp = std::make_shared<ObjectType>(3, uidObject, GetStoragePtr(uidObject.getPersistentPointerValue()), uidObject.getPersistentObjectSize() + nBufferSize, m_ptrAllocator->m_nBlockSize);
// 			ptrObject->validate(ptrTmp);
// #endif //__ENABLE_ASSERTS__

// #ifdef __CONCURRENT__
// 			lock_storage.unlock();
// #endif //__CONCURRENT__

// 			if (!bAlignedAllocation)
// 			{
// 				delete[] szBuffer;
// 			}
// 			else
// 			{
// #ifdef _MSC_VER
// 				_aligned_free(szBuffer);
// #else
// 				free(szBuffer);
// #endif
// 			}
		}

		return CacheErrorCode::Success;
	}
};

