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
class FileStorage
{
public:
	using ObjectType = typename Traits::ObjectType;
	using ObjectTypePtr = ObjectType*;
	using ObjectUIDType = typename Traits::ObjectUIDType;

private:
	std::string m_stFilename;
	uint64_t m_nStorageSize;
#ifdef _MSC_VER
	std::fstream m_fsStorage;
	std::mutex m_mtxFile;
#else //_MSC_VER
	int m_fdStorage;
#endif //_MSC_VER

	SIMDBitmapAllocator* m_ptrAllocator;

	struct WriteRequest 
	{
		size_t offset;
		size_t size;
		char* buffer;
		bool bAlignedAllocation;
	};

	// Write buffers

	std::thread m_tBackgroundFlush;
	std::atomic<bool> m_bStopBackground{ false };

	std::mutex m_mtxWritesInFlight;

	std::vector<WriteRequest> m_vtWrites;

	std::condition_variable cv;
	std::unordered_map<size_t, bool> m_mpWritesInFlight;

#ifdef __CONCURRENT__
	mutable std::shared_mutex m_mtxAllocator;
#endif //__CONCURRENT__

public:
	~FileStorage()
	{
		m_bStopBackground.store(true, std::memory_order_release);
		if (m_tBackgroundFlush.joinable()) {
			m_tBackgroundFlush.join();
		}

		delete m_ptrAllocator;

#ifdef _MSC_VER
		m_fsStorage.close();
#else //_MSC_VER
        close(m_fdStorage);
#endif //_MSC_VER
	}

	FileStorage(uint32_t nBlockSize, uint64_t nStorageSize, const std::string& stFilename)
		: m_stFilename(stFilename)
		, m_nStorageSize(nStorageSize)
	{
#ifdef _MSC_VER
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
#else //_MSC_VER
		m_fdStorage = open(m_stFilename.c_str(), O_RDWR | O_CREAT, 0644);
		if (m_fdStorage < 0)
		{
			throw std::logic_error("Failed to open file as storage");
		}

		if (ftruncate(m_fdStorage, m_nStorageSize) < 0) 
		{ 
			close(m_fdStorage);
			throw std::logic_error("Failed to set storage size"); 
		}
#endif //_MSC_VER

		//m_mpWritesInFlight.reserve(9024); // Adjust size based on expected entries

		m_ptrAllocator = new SIMDBitmapAllocator(nBlockSize, nStorageSize);
		m_tBackgroundFlush = std::thread(&FileStorage::backgroundFlushLoop, this);
	}

public:
	template <typename... InitArgs>
	CacheErrorCode init(InitArgs... args)
	{
		return CacheErrorCode::Success;
	}

	CacheErrorCode remove(const ObjectUIDType& uidObject)
	{
		{
#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock(m_mtxAllocator);
#endif //__CONCURRENT__

			m_ptrAllocator->free(uidObject.getPersistentPointerValue(), uidObject.getPersistentObjectSize());
		}

		return CacheErrorCode::Success;
	}

	CacheErrorCode getObject(uint16_t nDegree, const ObjectUIDType& uidObject, ObjectTypePtr& ptrObject)
	{
		uint32_t nSize = uidObject.getPersistentObjectSize();
		size_t nOffset = uidObject.getPersistentPointerValue();

		char* szBuffer = new char[nSize + 1];
		memset(szBuffer, 0, nSize + 1);		

		{
			std::unique_lock<std::mutex> lock(m_mtxWritesInFlight);

			// TODO: Locate the in-fligh bytes and return?
			cv.wait(lock, [&] {
				auto it = m_mpWritesInFlight.find(nOffset);
				return (it == m_mpWritesInFlight.end()) || it->second;
				});
		}

#ifdef _MSC_VER
		{
			std::unique_lock<std::mutex> lock(m_mtxFile);

			m_fsStorage.seekg(nOffset);
			m_fsStorage.read(szBuffer, nSize);
		}
#else //_MSC_VER
		if (pread(m_fdStorage, szBuffer, nSize, nOffset) < 0)
		{
			throw std::logic_error("Failed to read bytes");
		}
#endif //_MSC_VER

		ptrObject->updateCoreObject(nDegree, szBuffer, uidObject, uidObject.getPersistentObjectSize(), m_ptrAllocator->m_nBlockSize);

		delete[] szBuffer;

		return CacheErrorCode::Success;
	}

	ObjectTypePtr getObject(uint16_t nDegree, const ObjectUIDType& uidObject)
	{
		uint32_t nSize = uidObject.getPersistentObjectSize();
		size_t nOffset = uidObject.getPersistentPointerValue();

		char* szBuffer = new char[nSize + 1];
		memset(szBuffer, 0, nSize + 1);

		{
			std::unique_lock<std::mutex> lock(m_mtxWritesInFlight);

			// TODO: Locate the in-fligh bytes and return?
			cv.wait(lock, [&] {
				auto it = m_mpWritesInFlight.find(nOffset);
				return (it == m_mpWritesInFlight.end()) || it->second;
				});
		}

#ifdef _MSC_VER
		{
			std::unique_lock<std::mutex> lock(m_mtxFile);

			m_fsStorage.seekg(nOffset);
			m_fsStorage.read(szBuffer, nSize);
		}
#else //_MSC_VER
		if (pread(m_fdStorage, szBuffer, nSize, nOffset) < 0)
		{
			throw std::logic_error("Failed to read bytes");
		}
#endif //_MSC_VER

		ObjectTypePtr ptrObject = new ObjectType(nDegree, uidObject, szBuffer, uidObject.getPersistentObjectSize(), m_ptrAllocator->m_nBlockSize);

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

	CacheErrorCode addObject(ObjectTypePtr& ptrObject, ObjectUIDType& uidUpdated)
	{
		uint32_t nBufferSize = 0;
		bool bAlignedAllocation = false;

		char* szBuffer = nullptr;
		void* ptrOffset = nullptr;

		const ObjectUIDType& uidObject = ptrObject->m_uid;

		ptrObject->serialize(szBuffer, nBufferSize, m_ptrAllocator->m_nBlockSize, ptrOffset, bAlignedAllocation);

		if (ptrOffset == nullptr)
		{
			std::optional<size_t> oResult = std::nullopt;

			{
#ifdef __CONCURRENT__
				std::unique_lock<std::shared_mutex> lock(m_mtxAllocator);
#endif //__CONCURRENT__

				oResult = m_ptrAllocator->allocate(nBufferSize);
			}

			if (!oResult)
			{
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

			size_t nOffset = *oResult;

			{
				std::unique_lock<std::mutex> lock(m_mtxWritesInFlight);

				auto it = m_mpWritesInFlight.find(nOffset);
				if (it != m_mpWritesInFlight.end())
				{
					//m_mpWritesInFlight[nOffset] = false;
					throw std::runtime_error("Write request for same offset!");
				}

				m_mpWritesInFlight[nOffset] = false;

				WriteRequest oRequest{ nOffset, nBufferSize, szBuffer, bAlignedAllocation };
				m_vtWrites.emplace_back(std::move(oRequest));

			}

			ObjectUIDType::createAddressFromFileOffset(uidUpdated, uidObject.getObjectType(), nOffset, nBufferSize);

			if (uidObject.isPersistedObject())
			{
				m_ptrAllocator->free(uidObject.getPersistentPointerValue(), uidObject.getPersistentObjectSize());
			}
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

	void backgroundFlushLoop()
	{
		while (!m_bStopBackground.load(std::memory_order_acquire)) 
		{
			{
				std::unique_lock<std::mutex> lock(m_mtxWritesInFlight);

				if (m_vtWrites.size() == 0)
				{
					lock.unlock();
						std::this_thread::sleep_for(std::chrono::microseconds(1000));
					continue;
				}

#ifdef _MSC_VER
				std::unique_lock<std::mutex> _lock(m_mtxFile);

				for (auto& b : m_vtWrites)
				{
					m_fsStorage.seekp(b.offset);
					m_fsStorage.write(b.buffer, b.size);

					if (!b.bAlignedAllocation)
					{
						delete[] b.buffer;
					}
					else
					{
//#ifdef _MSC_VER
						_aligned_free(b.buffer);
//#else
//						free(b.buffer);
//#endif
					}

					m_mpWritesInFlight[b.offset] = true;
				}
#else //_MSC_VER
				//std::unique_lock<std::mutex> _lock(m_mtxFile);

				for (auto& b : m_vtWrites)
				{
					if (pwrite(m_fdStorage, b.buffer, b.size, b.offset) < 0)
					{
						throw std::logic_error("Failed to write bytes");
					}

					if (!b.bAlignedAllocation)
					{
						delete[] b.buffer;
					}
					else
					{
						free(b.buffer);
					}

					m_mpWritesInFlight[b.offset] = true;
				}
#endif //_MSC_VER


				//m_fsStorage.flush();
				cv.notify_all();
				//}
			//{
				//std::unique_lock<std::mutex> lock(m_mtxWritesInFlight);

				m_mpWritesInFlight.clear();
				/*for (auto& b : m_vtWrites)
				{
					m_mpWritesInFlight.erase(b.offset);
				}*/
				m_vtWrites.clear();
			}
		}
	}

};
