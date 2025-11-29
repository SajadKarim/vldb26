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
class FileStorageIOURing
{
public:
	using ObjectType = typename Traits::ObjectType;
	using ObjectTypePtr = std::shared_ptr<ObjectType>;
	using ObjectUIDType = typename Traits::ObjectUIDType;

private:
	size_t m_nStorageSize;
	std::string m_stFilename;
	std::fstream m_fsStorage;

	size_t m_nBlockSize;
	size_t m_nTotalBlocks;
	SIMDBitmapAllocator* m_ptrAllocator;

#ifdef __CONCURRENT__
	mutable std::shared_mutex m_mtxStorage;
#endif //__CONCURRENT__

public:
	~FileStorageIOURing()
	{
		delete m_ptrAllocator;

		m_fsStorage.close();
	}

	FileStorageIOURing(size_t nBlockSize, size_t nStorageSize, const std::string& stFilename)
		: m_nBlockSize(nBlockSize)
		, m_stFilename(stFilename)
		, m_nStorageSize(nStorageSize)
	{
		//m_fsStorage.rdbuf()->pubsetbuf(0, 0);
		m_fsStorage.open(stFilename.c_str(), std::ios::out | std::ios::binary);
		m_fsStorage.close();

		m_fsStorage.open(stFilename.c_str(), std::ios::out | std::ios::binary | std::ios::in);
		m_fsStorage.seekp(0);
		m_fsStorage.seekg(0);

		if (!m_fsStorage.is_open())
		{
			throw new std::logic_error("Failed to open file as a storage.");
		}

		m_nTotalBlocks = m_nStorageSize / m_nBlockSize;
		m_ptrAllocator = new SIMDBitmapAllocator(m_nTotalBlocks);
	}

public:
	template <typename... InitArgs>
	CacheErrorCode init(InitArgs... args)
	{
		return CacheErrorCode::Success;
	}

	CacheErrorCode remove(const ObjectUIDType& uidObject)
	{
		size_t nOffset = uidObject.getPersistentPointerValue();
		size_t nSize = uidObject.getPersistentObjectSize();
		size_t nBlockStart = nOffset / m_nBlockSize;
		size_t nBlockCount = (nSize + m_nBlockSize - 1) / m_nBlockSize;

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_storage(m_mtxStorage);
#endif //__CONCURRENT__

		m_ptrAllocator->free(nBlockStart, nBlockCount);

		return CacheErrorCode::Success;
	}

	CacheErrorCode getObject(uint16_t nDegree, const ObjectUIDType& uidObject, std::shared_ptr<ObjectType>& ptrObject)
	{
		char* szBuffer = new char[uidObject.getPersistentObjectSize() + 1];
		memset(szBuffer, 0, uidObject.getPersistentObjectSize() + 1);

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_storage(m_mtxStorage);
#endif //__CONCURRENT__

		m_fsStorage.seekg(uidObject.getPersistentPointerValue());
		m_fsStorage.read(szBuffer, uidObject.getPersistentObjectSize());

#ifdef __CONCURRENT__
		lock_storage.unlock();
#endif //__CONCURRENT__

		ptrObject->updateCoreObject(nDegree, szBuffer, uidObject, uidObject.getPersistentObjectSize(), (uint16_t)m_nBlockSize);

		delete[] szBuffer;

		return CacheErrorCode::Success;
	}

	std::shared_ptr<ObjectType> getObject(uint16_t nDegree, const ObjectUIDType& uidObject)
	{
		char* szBuffer = new char[uidObject.getPersistentObjectSize() + 1];
		memset(szBuffer, 0, uidObject.getPersistentObjectSize() + 1);

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_storage(m_mtxStorage);
#endif //__CONCURRENT__

		m_fsStorage.seekg(uidObject.getPersistentPointerValue());
		m_fsStorage.read(szBuffer, uidObject.getPersistentObjectSize());

#ifdef __CONCURRENT__
		lock_storage.unlock();
#endif //__CONCURRENT__

		std::shared_ptr<ObjectType> ptrObject = std::make_shared<ObjectType>(nDegree, uidObject, szBuffer, uidObject.getPersistentObjectSize(), (uint16_t)m_nBlockSize);

		delete[] szBuffer;

		return ptrObject;
	}

	inline size_t nextPowerOf2(size_t nNumber)
	{
		if (nNumber <= 1)
		{
			return 1;
		}

#if defined(_MSC_VER)
		unsigned long index;
		_BitScanReverse64(&index, nNumber - 1);
		return 1ULL << (index + 1);
#elif defined(__GNUC__) || defined(__clang__)
		return 1ULL << (64 - __builtin_clzl(nNumber - 1));
#else
		// Fallback to bit-smearing
		--nNumber;
		nNumber |= nNumber >> 1;
		nNumber |= nNumber >> 2;
		nNumber |= nNumber >> 4;
		nNumber |= nNumber >> 8;
		nNumber |= nNumber >> 16;
		nNumber |= nNumber >> 32;
		return nNumber + 1;
#endif
	}

	CacheErrorCode addObject(ObjectUIDType uidObject, std::shared_ptr<ObjectType> ptrObject, ObjectUIDType& uidUpdated)
	{
		uint32_t nBufferSize = 0;
		bool bAlignedAllocation = false;

		char* szBuffer = nullptr;
		void* ptrOffset = nullptr;
		ptrObject->serialize(szBuffer, nBufferSize, (uint16_t)m_nBlockSize, ptrOffset, bAlignedAllocation);

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_storage(m_mtxStorage);
#endif //__CONCURRENT__

		if (ptrOffset == nullptr)
		{
			size_t nBlocks = (nBufferSize + m_nBlockSize - 1) / m_nBlockSize;
			size_t nBlocksRequired = nextPowerOf2(nBlocks);

			auto oResult = m_ptrAllocator->allocate(nBlocksRequired);
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

			size_t nOffset = *oResult * m_nBlockSize;

			ASSERT(nOffset + nBufferSize <= m_nStorageSize);

			m_fsStorage.seekp(nOffset);
			m_fsStorage.write(szBuffer, nBufferSize);
			m_fsStorage.flush();

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

			ObjectUIDType::createAddressFromFileOffset(uidUpdated, uidObject.getObjectType(), nOffset, nBufferSize);
		}
		else
		{
			uidUpdated = uidObject;

			throw new std::logic_error("unimplemented!.");

			//memcpy(ptrOffset, szBuffer, nBufferSize);

#ifdef __ENABLE_ASSERTS__
			//ObjectTypePtr ptrTmp = std::make_shared<ObjectType>(3, uidObject, GetStoragePtr(uidObject.getPersistentPointerValue()), uidObject.getPersistentObjectSize() + nBufferSize, m_nBlockSize);
			//ptrObject->validate(ptrTmp);
#endif //__ENABLE_ASSERTS__

#ifdef __CONCURRENT__
			lock_storage.unlock();
#endif //__CONCURRENT__

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
