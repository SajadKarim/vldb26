#pragma once
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <syncstream>
#include <thread>
#include <variant>
#include <typeinfo>
#include <unordered_map>
#include <queue>
#include <algorithm>
#include <tuple>
#include <condition_variable>
#include <unordered_map>
#include <atomic>
#include "CacheErrorCodes.h"
#include <optional>
#include <chrono>
#include "PMemWAL.hpp"
#include "validityasserts.h"
#include <queue>
#ifdef __CACHE_COUNTERS__
#include "CacheStatsProvider.hpp"
#endif

#define FLUSH_COUNT 100
#define MIN_CACHE_FOOTPRINT 1024 * 1024	// Safe check!
#define MAX_EVICTED_BUFFER_SIZE 256
//#define __CLOCK_WITH_BUFFER__
using namespace std::chrono_literals;

template <typename Traits>
class CLOCKCache
#ifdef __CACHE_COUNTERS__
	: public CacheStatsProvider<CLOCKCache<Traits>>
#endif
{
	typedef CLOCKCache<Traits> SelfType;

public:
	using KeyType = typename Traits::KeyType;
	using ValueType = typename Traits::ValueType;
	using CacheType = typename Traits::CacheType;

	using ObjectType = typename Traits::ObjectType;
	using ObjectTypePtr = typename Traits::ObjectType*;
	using ObjectUIDType = typename Traits::ObjectUIDType;

	using WALType = typename Traits::WALType;
	using StorageType = typename Traits::StorageType;

	struct OpDeleteInfo
	{
		ObjectTypePtr m_ptrPrimary;
		ObjectTypePtr m_ptrAffectedSibling;
		ObjectTypePtr m_ptrToDiscard;

		OpDeleteInfo(ObjectTypePtr ptrPrimary, ObjectTypePtr ptrAffectedSibling, ObjectTypePtr ptrToDiscard)
			: m_ptrPrimary(ptrPrimary)
			, m_ptrAffectedSibling(ptrAffectedSibling)
			, m_ptrToDiscard(ptrToDiscard)
		{}
	};

#ifndef __CONCURRENT__
	static constexpr bool MARK_INUSE_FLAG = true;
#endif //__CONCURRENT__

protected:
	ObjectTypePtr m_ptrHead;
	ObjectTypePtr m_ptrTail;

	size_t m_nClockHand;
	std::vector<int> m_vtClockBufferWeight;
	std::vector<ObjectTypePtr> m_vtClockBuffer;

	// Circular eviction buffer with uint8_t counters
	ObjectTypePtr m_arrEvictedItems[MAX_EVICTED_BUFFER_SIZE] = {nullptr};
	std::atomic<uint8_t> m_nWriteCounter{ 255 };
	std::atomic<uint8_t> m_nCommitCounter{ 255 };
	std::atomic<uint8_t> m_nFlushCounter{ 0 };
	std::vector<std::pair<ObjectTypePtr, int>> m_vtClockQ;

	StorageType* m_ptrStorage;

	uint64_t m_nCacheCapacity;

#ifdef __CONCURRENT__
	std::atomic<uint64_t> m_nUsedCacheCapacity;
#else //__CONCURRENT__
	uint64_t m_nUsedCacheCapacity;
#endif //__CONCURRENT__

	int m_pendingclock;

	WALType* m_ptrWAL;

	std::condition_variable cv;

#ifdef __CONCURRENT__
	bool m_bStop;

	std::thread m_threadCacheFlush;

	mutable std::mutex m_mtxCache;

#ifdef __CACHE_COUNTERS__
	// Storage for background thread's cache statistics
	std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> m_backgroundThreadHits;
	std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> m_backgroundThreadMisses;
	std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> m_backgroundThreadEvictions;
	std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> m_backgroundThreadDirtyEvictions;
#endif //__CACHE_COUNTERS__
#endif //__CONCURRENT__

public:
	~CLOCKCache()
	{
#ifdef __CONCURRENT__
#ifdef __CLOCK_WITH_BUFFER__
		{
			std::unique_lock<std::mutex> lock_cache(m_mtxCache);
			cv.wait(lock_cache, [&] { return m_vtClockQ.size() == 0; });
		}
#endif //__CLOCK_WITH_BUFFER__

	#ifdef __CLOCK_WITH_BUFFER__
		// Stop background thread first
		m_bStop = true;
		m_threadCacheFlush.join();
	#endif //__CLOCK_WITH_BUFFER__
#endif //__CONCURRENT__

		flush(false);

		m_ptrHead == nullptr;
		m_ptrTail == nullptr;

		delete m_ptrStorage;
		m_ptrStorage = nullptr;
	}

	template <typename... StorageArgs>
	CLOCKCache(size_t nCapacity, StorageArgs... args)
		: m_ptrHead(nullptr)
		, m_ptrTail(nullptr)
		, m_nUsedCacheCapacity(0)
		, m_nCacheCapacity(nCapacity)
	{
#ifdef __CACHE_COUNTERS__
		// Reset thread-local stats when creating a new cache instance
		CacheStatsProvider<CLOCKCache<Traits>>::resetThreadLocalStats();
#endif
		
		m_ptrStorage = new StorageType(args...);

		//m_ptrWAL = new WALType(this, WAL_FILE_PATH);

		m_vtClockQ.reserve(m_nCacheCapacity);
		m_pendingclock = 0;
		m_nClockHand = 0;
		m_vtClockBuffer.resize(nCapacity);
		m_vtClockBufferWeight.resize(nCapacity);
		for (int i = 0; i < nCapacity; ++i)
		{
			m_vtClockBuffer[i] = nullptr;
			m_vtClockBufferWeight[i] = -1;
		}

		// Initialize circular evicted items buffer to nullptr
		for (int i = 0; i < MAX_EVICTED_BUFFER_SIZE; ++i)
		{
			m_arrEvictedItems[i] = nullptr;
		}

#ifdef __CONCURRENT__
	#ifdef __CLOCK_WITH_BUFFER__
		// Start background thread AFTER all initialization is complete
		// This prevents race condition where background thread accesses
		// uninitialized data structures (detected by ThreadSanitizer)
		m_bStop = false;
		m_threadCacheFlush = std::thread(handlerCacheFlush, this);
	#endif //__CLOCK_WITH_BUFFER__
#endif //__CONCURRENT__
	}

	template <typename... InitArgs>
	CacheErrorCode init(InitArgs... args)
	{
		return m_ptrStorage->init(this);
	}

	void log(uint8_t op, const KeyType& key, const ValueType& value)
	{
		//m_ptrWAL->append(op, key, value);
	}

#ifdef __CACHE_COUNTERS__
	const CacheStatsProvider<CLOCKCache<Traits>>* getCacheStatsProvider() const
	{
		return static_cast<const CacheStatsProvider<CLOCKCache<Traits>>*>(this);
	}

#ifdef __CONCURRENT__
	// Getters for background thread's copied cache statistics
	const std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>& getBackgroundThreadHits() const
	{
		return m_backgroundThreadHits;
	}

	const std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>& getBackgroundThreadMisses() const
	{
		return m_backgroundThreadMisses;
	}

	const std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>& getBackgroundThreadEvictions() const
	{
		return m_backgroundThreadEvictions;
	}

	const std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>& getBackgroundThreadDirtyEvictions() const
	{
		return m_backgroundThreadDirtyEvictions;
	}
#endif //__CONCURRENT__
#endif

#ifdef __SELECTIVE_UPDATE__
	CacheErrorCode updateObjectsAccessMetadata(int nDepth, std::vector<ObjectTypePtr>& vtObjects, bool hasNewNodes)
#else //__SELECTIVE_UPDATE__
	CacheErrorCode updateObjectsAccessMetadata(int nDepth, std::vector<ObjectTypePtr>& vtObjects)
#endif //__SELECTIVE_UPDATE__
	{
#ifdef __SELECTIVE_UPDATE__
		if (!hasNewNodes)
		{
			for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
			{
				auto obj = (*it);

				if (obj == nullptr || obj->m_ptrCoreObject == nullptr)
				{
					ASSERT(false);
				}

				if (obj == nullptr || obj->m_ptrCoreObject == nullptr) continue;

	#ifdef __CONCURRENT__
				ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
				obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
	#else //__CONCURRENT__
				if constexpr (CacheType::MARK_INUSE_FLAG)
				{
					ASSERT(obj->m_bInUse);
					obj->m_bInUse = false;
				}
	#endif //__CONCURRENT__
			}
			return CacheErrorCode::Success;
		}
#endif //__SELECTIVE_UPDATE__

#if defined(__CONCURRENT__) && defined(__CLOCK_WITH_BUFFER__)

		std::vector<std::pair<ObjectTypePtr, int>> vtBuffer;
		vtBuffer.reserve(vtObjects.size());

		int nLevel = 1;
		for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
		{
			auto obj = (*it);

			if (obj == nullptr || obj->m_ptrCoreObject == nullptr) continue;

			ASSERT(obj->m_ptrCoreObject); //failed.. might be object has been moved while it has bene in the queue.. but how.. I do have check for ref_count!!

			// ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
			// obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);

			vtBuffer.push_back({ obj, nLevel++ });
		}

		std::unique_lock<std::mutex> lock_cache(m_mtxCache);

		cv.wait(lock_cache, [&] { return m_vtClockQ.size() + vtBuffer.size() < (m_nCacheCapacity / 2 - 10); });

		m_vtClockQ.insert(m_vtClockQ.end(), vtBuffer.begin(), vtBuffer.end());

		vtObjects.clear();
#else //defined(__CONCURRENT__) && defined(__CLOCK_WITH_BUFFER__)

#ifdef __CONCURRENT__
		std::unique_lock<std::mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		uint16_t nLevel = 1;
		for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
		{
			auto obj = (*it);

			if (obj == nullptr || obj->m_ptrCoreObject == nullptr)
			{
				ASSERT(false);
			}

			if (obj == nullptr || obj->m_ptrCoreObject == nullptr) continue;

			if (obj->m_nPositionInCLOCK == -1)
			{
				auto nIdx = evictItemFromCache();
				ASSERT(nIdx == m_nClockHand);

				ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);
				ASSERT(!m_vtClockBuffer[m_nClockHand]);

				m_vtClockBuffer[m_nClockHand] = obj;
#ifdef __COST_WEIGHTED_EVICTION__
				m_vtClockBufferWeight[m_nClockHand] = nLevel + obj->getObjectCost();
#else
				m_vtClockBufferWeight[m_nClockHand] = nLevel;
#endif //__COST_WEIGHTED_EVICTION__

				obj->m_nPositionInCLOCK = m_nClockHand;
			}

#ifdef __CONCURRENT__
			ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
			obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
#else //__CONCURRENT__
			if constexpr (CacheType::MARK_INUSE_FLAG)
			{
				ASSERT(obj->m_bInUse);
				obj->m_bInUse = false;
			}
#endif //__CONCURRENT__

#ifdef __COST_WEIGHTED_EVICTION__
			m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = nLevel + obj->getObjectCost();
#else
			m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = nLevel;
#endif //__COST_WEIGHTED_EVICTION__
			nLevel++;
		}

		vtObjects.clear();

#endif //defined(__CONCURRENT__) && defined(__CLOCK_WITH_BUFFER__)

		return CacheErrorCode::Success;
	}

#ifdef __SELECTIVE_UPDATE__
	CacheErrorCode updateObjectsAccessMetadata(int nDepth, std::vector<std::pair<ObjectTypePtr, ObjectTypePtr>>& vtObjects, bool hasNewNodes)
#else //__SELECTIVE_UPDATE__
	CacheErrorCode updateObjectsAccessMetadata(int nDepth, std::vector<std::pair<ObjectTypePtr, ObjectTypePtr>>& vtObjects)
#endif //__SELECTIVE_UPDATE__
	{
#ifdef __SELECTIVE_UPDATE__
		if (!hasNewNodes)
		{
			for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
			{
				for (int j = 1; j >= 0; j--)
				{
					auto obj = j == 0 ? (*it).first : (*it).second;

					if (obj == nullptr || obj->m_ptrCoreObject == nullptr) continue;

	#ifdef __CONCURRENT__
					ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
					obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
	#else //__CONCURRENT__
					if constexpr (CacheType::MARK_INUSE_FLAG)
					{
						ASSERT(obj->m_bInUse);
						obj->m_bInUse = false;
					}
	#endif //__CONCURRENT__
				}
			}
			return CacheErrorCode::Success;
		}
#endif //__SELECTIVE_UPDATE__

#if defined(__CONCURRENT__) && defined(__CLOCK_WITH_BUFFER__)
		std::vector<std::pair<ObjectTypePtr, int>> vtBuffer;
		vtBuffer.reserve(vtObjects.size());

		uint16_t nLevel = 1;
		for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
		{
			for (int j = 1; j >= 0; j--)
			{
				auto obj = j == 0 ? (*it).first : (*it).second;

				if (obj == nullptr || obj->m_ptrCoreObject == nullptr) continue;

				ASSERT(obj->m_ptrCoreObject); //failed.. might be object has been moved while it has bene in the queue.. but how.. I do have check for ref_count!!

				// ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
				// obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
				
				vtBuffer.push_back({ obj, nLevel });
			}

			if ((*it).first != nullptr || (*it).second != nullptr)
				nLevel++;
		}

		std::unique_lock<std::mutex> lock_cache(m_mtxCache);

		cv.wait(lock_cache, [&] { return m_vtClockQ.size() + vtBuffer.size() < (m_nCacheCapacity / 2 - 10); });

		m_vtClockQ.insert(m_vtClockQ.end(), vtBuffer.begin(), vtBuffer.end());

		vtObjects.clear();
#else //defined(__CONCURRENT__) && defined(__CLOCK_WITH_BUFFER__)
#ifdef __CONCURRENT__
		std::unique_lock<std::mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		uint16_t nLevel = 1;
		for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
		{
			for (int j = 1; j >= 0; j--)
			{
				auto obj = j == 0 ? (*it).first : (*it).second;

				if (obj == nullptr || obj->m_ptrCoreObject == nullptr) continue;

				if (obj->m_nPositionInCLOCK == -1)
				{
					auto nIdx = evictItemFromCache();
					ASSERT(nIdx == m_nClockHand);

					ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);
					ASSERT(!m_vtClockBuffer[m_nClockHand]);

					m_vtClockBuffer[m_nClockHand] = obj;
#ifdef __COST_WEIGHTED_EVICTION__
					m_vtClockBufferWeight[m_nClockHand] = nLevel + obj->getObjectCost();
#else
					m_vtClockBufferWeight[m_nClockHand] = nLevel;
#endif //__COST_WEIGHTED_EVICTION__

					obj->m_nPositionInCLOCK = m_nClockHand;
				}

#ifdef __COST_WEIGHTED_EVICTION__
				m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = nLevel + obj->getObjectCost();
#else
				m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = nLevel;
#endif //__COST_WEIGHTED_EVICTION__

#ifdef __CONCURRENT__
				ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
				obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
#else //__CONCURRENT__
				if constexpr (CacheType::MARK_INUSE_FLAG)
				{
					ASSERT(obj->m_bInUse);
					obj->m_bInUse = false;
				}
#endif //__CONCURRENT__
			}

			if ((*it).first != nullptr || (*it).second != nullptr) nLevel++;
		}

		vtObjects.clear();

#endif //defined(__CONCURRENT__) && defined(__CLOCK_WITH_BUFFER__)

		return CacheErrorCode::Success;
	}

#ifdef __SELECTIVE_UPDATE__
	CacheErrorCode updateObjectsAccessMetadata(int nDepth, std::vector<OpDeleteInfo>& vtObjects, bool hasNewNodes)
#else //__SELECTIVE_UPDATE__
	CacheErrorCode updateObjectsAccessMetadata(int nDepth, std::vector<OpDeleteInfo>& vtObjects)
#endif //__SELECTIVE_UPDATE__
	{
#ifdef __SELECTIVE_UPDATE__
		if (!hasNewNodes)
		{
			for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
			{
				for (int j = 2; j >= 0; j--)
				{
					ObjectTypePtr obj = nullptr;
					switch (j)
					{
						case 0:
							obj = (*it).m_ptrPrimary;
							break;
						case 1:
							obj = (*it).m_ptrAffectedSibling;
							break;
						case 2:
							obj = (*it).m_ptrToDiscard;
							break;
						default:
							ASSERT(false && "Invalid state.");
					}
					//auto& obj = j == 0 ? it.m_ptrfirst : it.second;
					if (obj == nullptr || obj->m_ptrCoreObject == nullptr) continue;

	#ifdef __CONCURRENT__
					ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
					obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
	#else //__CONCURRENT__
					if constexpr (CacheType::MARK_INUSE_FLAG)
					{
						ASSERT(obj->m_bInUse);
						obj->m_bInUse = false;
					}
	#endif //__CONCURRENT__
				}
			}
			return CacheErrorCode::Success;
		}
#endif //__SELECTIVE_UPDATE__

#if defined(__CONCURRENT__) && defined(__CLOCK_WITH_BUFFER__)
		std::vector<std::pair<ObjectTypePtr, int>> vtBuffer;
		vtBuffer.reserve(vtObjects.size());

		uint16_t nLevel = 1;
		bool bDecreaseDepth = false;
		for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
		{
			bDecreaseDepth = false;
			for (int j = 2; j >= 0; j--)
			{
				ObjectTypePtr obj = nullptr;
				switch (j)
				{
				case 0:
					obj = (*it).m_ptrPrimary;
					break;
				case 1:
					obj = (*it).m_ptrAffectedSibling;
					break;
				case 2:
					obj = (*it).m_ptrToDiscard;
					break;
				default:
					ASSERT(false && "Invalid state.");
				}
				//auto& obj = j == 0 ? it.m_ptrfirst : it.second;
				if (obj == nullptr || obj->m_ptrCoreObject == nullptr) continue;

				bDecreaseDepth = true;

				ASSERT(obj->m_ptrCoreObject); //failed.. might be object has been moved while it has bene in the queue.. but how.. I do have check for ref_count!!

				// ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
				// obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);

				vtBuffer.push_back({ obj, nLevel });
			}

			if (bDecreaseDepth) nLevel++;
		}

		std::unique_lock<std::mutex> lock_cache(m_mtxCache);

		cv.wait(lock_cache, [&] { return m_vtClockQ.size() + vtBuffer.size() < (m_nCacheCapacity / 2 - 10); });

		m_vtClockQ.insert(m_vtClockQ.end(), vtBuffer.begin(), vtBuffer.end());

		vtObjects.clear();
#else //defined(__CONCURRENT__) && defined(__CLOCK_WITH_BUFFER__)
#ifdef __CONCURRENT__
		std::unique_lock<std::mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		uint16_t nLevel = 1;
		bool bDecreaseDepth = false;
		for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
		{
			bDecreaseDepth = false;
			for (int j = 2; j >= 0; j--)
			{
				ObjectTypePtr obj = nullptr;
				switch (j)
				{
					case 0:
						obj = (*it).m_ptrPrimary;
						break;
					case 1:
						obj = (*it).m_ptrAffectedSibling;
						break;
					case 2:
						obj = (*it).m_ptrToDiscard;
						break;
					default:
						ASSERT(false && "Invalid state.");
				}
				//auto& obj = j == 0 ? it.m_ptrfirst : it.second;
				if (obj == nullptr || obj->m_ptrCoreObject == nullptr) continue;

				bDecreaseDepth = true;

				if (obj->m_nPositionInCLOCK == -1)
				{
					auto nIdx = evictItemFromCache();
					ASSERT(nIdx == m_nClockHand);

					ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);
					ASSERT(!m_vtClockBuffer[m_nClockHand]);

					m_vtClockBuffer[m_nClockHand] = obj;
#ifdef __COST_WEIGHTED_EVICTION__
					m_vtClockBufferWeight[m_nClockHand] = nLevel + obj->getObjectCost();
#else
					m_vtClockBufferWeight[m_nClockHand] = nLevel;
#endif //__COST_WEIGHTED_EVICTION__

					obj->m_nPositionInCLOCK = m_nClockHand;
				}

#ifdef __COST_WEIGHTED_EVICTION__
				m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = nLevel + obj->getObjectCost();
#else
				m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = nLevel;
#endif //__COST_WEIGHTED_EVICTION__

#ifdef __CONCURRENT__
				ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
				obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
#else //__CONCURRENT__
				if constexpr (CacheType::MARK_INUSE_FLAG)
				{
					ASSERT(obj->m_bInUse);
					obj->m_bInUse = false;
				}
#endif //__CONCURRENT__
			}

			if (bDecreaseDepth) nLevel++;
		}

		vtObjects.clear();

#endif //defined(__CONCURRENT__) && defined(__CLOCK_WITH_BUFFER__)

		return CacheErrorCode::Success;
	}

	CacheErrorCode remove(ObjectTypePtr& ptrObject)
	{
		ASSERT(false);

		return CacheErrorCode::Success;
	}

	CacheErrorCode getCoreObject(uint16_t nDegree, const ObjectUIDType& uidObject, ObjectTypePtr& ptrObject)
	{
		m_ptrStorage->getObject(nDegree, uidObject, ptrObject);

		ASSERT(ptrObject->m_ptrCoreObject != nullptr && "The requested object does not exist.");

#ifdef __COST_WEIGHTED_EVICTION__
		// Capture the cost of accessing this object from storage
		if constexpr (requires { m_ptrStorage->getAccessCost(ptrObject->m_nCoreObjectType); })
		{
			uint64_t accessCost = m_ptrStorage->getAccessCost(ptrObject->m_nCoreObjectType);
			ptrObject->setObjectCost(accessCost);
		}
		else
		{
			ptrObject->setObjectCost(1);  // Fallback
		}
#endif //__COST_WEIGHTED_EVICTION__

#ifdef __CONCURRENT__
		m_nUsedCacheCapacity.fetch_add(1, std::memory_order_relaxed);
#else //__CONCURRENT__
		m_nUsedCacheCapacity++;
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}

	CacheErrorCode getObject(uint16_t nDegree, const ObjectUIDType& uidObject, ObjectTypePtr& ptrObject)
	{
		ptrObject = m_ptrStorage->getObject(nDegree, uidObject);

		ASSERT(ptrObject == nullptr || "The requested object does not exist.");

#ifdef __CONCURRENT__
		m_nUsedCacheCapacity.fetch_add(1, std::memory_order_relaxed);
#else //__CONCURRENT__
		m_nUsedCacheCapacity++;
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}

	template<class Type, typename... ArgsType>
	CacheErrorCode createObjectOfType(ObjectUIDType& uidObject, ObjectTypePtr& ptrObject, const ArgsType... args)
	{
		ptrObject = new ObjectType((uint8_t)Type::UID, new Type(args...));

		uidObject.createUIDFromVolatilePointer((uint8_t)Type::UID, reinterpret_cast<uintptr_t>(ptrObject));

		ptrObject->m_uid = uidObject;

#ifdef __CONCURRENT__
		m_nUsedCacheCapacity.fetch_add(1, std::memory_order_relaxed);
#else //__CONCURRENT__
		m_nUsedCacheCapacity++;
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}

	void getObjectsCountInCache(size_t& nObjectsLinkedList)
	{
		//m_nCacheCapacity - m_qClockEmptySlots.size();
		/*nObjectsLinkedList = 0;
		ObjectTypePtr ptrItem = m_ptrHead;

		while (ptrItem != nullptr)
		{
			nObjectsLinkedList++;
			ptrItem = ptrItem->m_ptrNext;
		} */
	}

	CacheErrorCode flush(bool bPauseFlushThread = true)
	{
		if (bPauseFlushThread)
		{
#ifdef __CONCURRENT__
#ifdef __CLOCK_WITH_BUFFER__
			m_bStop = true;
			m_threadCacheFlush.join();
#endif //__CLOCK_WITH_BUFFER__

#ifdef __CACHE_COUNTERS__
			// Move background thread's copied data to main thread's thread-local variables
			// This is efficient since we're transferring ownership using move semantics
			auto* statsProvider = static_cast<CacheStatsProvider<CLOCKCache<Traits>>*>(this);

			// Access main thread's thread-local data and merge with background thread data
			// Note: We need to merge rather than replace to preserve main thread's existing data
			auto& mainThreadHits = const_cast<std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>&>(statsProvider->getHitsTimeline());
			auto& mainThreadMisses = const_cast<std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>&>(statsProvider->getMissesTimeline());
			auto& mainThreadEvictions = const_cast<std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>&>(statsProvider->getEvictionsTimeline());
			auto& mainThreadDirtyEvictions = const_cast<std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>&>(statsProvider->getDirtyEvictionsTimeline());

			// Move background thread data to main thread (efficient transfer of ownership)
			mainThreadHits.insert(mainThreadHits.end(),
				std::make_move_iterator(m_backgroundThreadHits.begin()),
				std::make_move_iterator(m_backgroundThreadHits.end()));
			mainThreadMisses.insert(mainThreadMisses.end(),
				std::make_move_iterator(m_backgroundThreadMisses.begin()),
				std::make_move_iterator(m_backgroundThreadMisses.end()));
			mainThreadEvictions.insert(mainThreadEvictions.end(),
				std::make_move_iterator(m_backgroundThreadEvictions.begin()),
				std::make_move_iterator(m_backgroundThreadEvictions.end()));
			mainThreadDirtyEvictions.insert(mainThreadDirtyEvictions.end(),
				std::make_move_iterator(m_backgroundThreadDirtyEvictions.begin()),
				std::make_move_iterator(m_backgroundThreadDirtyEvictions.end()));

			// Clear the background thread storage (data has been moved)
			m_backgroundThreadHits.clear();
			m_backgroundThreadMisses.clear();
			m_backgroundThreadEvictions.clear();
			m_backgroundThreadDirtyEvictions.clear();
#endif //__CACHE_COUNTERS__
#endif //__CONCURRENT__
		}

		flushAllItemsToStorage();

		if (bPauseFlushThread)
		{
#ifdef __CONCURRENT__
#ifdef __CLOCK_WITH_BUFFER__
			m_bStop = false;
			m_threadCacheFlush = std::thread(handlerCacheFlush, this);
#endif //__CLOCK_WITH_BUFFER__
#endif //__CONCURRENT__
		}

		return CacheErrorCode::Success;
	}

	inline size_t evictItemFromCache()
	{
		uint64_t i=0;
		while (true)
		{
			if (m_vtClockBufferWeight[m_nClockHand] == -1)
			{
				ASSERT(!m_vtClockBuffer[m_nClockHand]);
				return m_nClockHand;
			}

#ifdef __CONCURRENT__
			if (m_vtClockBuffer[m_nClockHand]->m_nUseCounter != 0)
			{
				//std::cout << "++ :" << i++ << std::endl;
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}
#else //__CONCURRENT__
			if constexpr (CacheType::MARK_INUSE_FLAG)
			{
				if (m_vtClockBuffer[m_nClockHand]->m_bInUse)
				{
					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
					continue;
				}
			}
#endif //__CONCURRENT__

			if (m_vtClockBufferWeight[m_nClockHand] > 0)
			{
				//std::cout << "-- :" << i++ << std::endl;
				m_vtClockBufferWeight[m_nClockHand]--;
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

			auto obj = m_vtClockBuffer[m_nClockHand];

#ifdef __CONCURRENT__
			if (!obj->m_mtx.try_lock())
			{
				//std::cout << ".. :" << i++ << std::endl;
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

			if (m_vtClockBuffer[m_nClockHand]->m_nUseCounter != 0)
			{
				//std::cout << "xx :" << i++ << std::endl;
				obj->m_mtx.unlock();
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}
#endif //__CONCURRENT__

			ASSERT(m_vtClockBuffer[m_nClockHand]);
			ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);

			if (obj->m_bMarkDelete)
			{
				ASSERT(m_nClockHand == obj->m_nPositionInCLOCK);

				if (obj->m_uid.getMediaType() > 1)
				{
					m_ptrStorage->remove(obj->m_uid);
				}

				if (obj->m_nPositionInCLOCK != -1)
				{
					ASSERT(m_vtClockBuffer[obj->m_nPositionInCLOCK]);

					m_vtClockBuffer[obj->m_nPositionInCLOCK] = nullptr;
					m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = -1;
				}

#ifdef __CONCURRENT__
				obj->m_mtx.unlock();
#endif //__CONCURRENT__

				delete obj;
				obj = nullptr;

#ifdef __CONCURRENT__
				ASSERT(m_nUsedCacheCapacity.load(std::memory_order_relaxed) != 0);
				m_nUsedCacheCapacity.fetch_sub(1, std::memory_order_relaxed);
#else //__CONCURRENT__
				ASSERT(m_nUsedCacheCapacity != 0);
				m_nUsedCacheCapacity--;
#endif //__CONCURRENT__
				return m_nClockHand;
			}

			if (obj->_havedependentsincache())
			{
				//std::cout << "** :" << i++ << std::endl;
#ifdef __CONCURRENT__
				obj->m_mtx.unlock();
#endif //__CONCURRENT__
				m_vtClockBufferWeight[m_nClockHand] = 0;
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

#ifdef __CONCURRENT__
		#ifdef __CLOCK_WITH_BUFFER__
			// Circular buffer eviction with uint8_t counters
			// 1. Reserve slot (WriteCounter update)
			uint8_t mySlot = m_nWriteCounter.fetch_add(1, std::memory_order_acq_rel);

			// 3. Wait for my turn (FIFO ordering)
			uint8_t expectedPrev = mySlot;
			uint8_t currentCommit = m_nCommitCounter.load(std::memory_order_acquire);

			while (currentCommit != expectedPrev || m_arrEvictedItems[mySlot] != nullptr) {
				std::this_thread::yield();
				currentCommit = m_nCommitCounter.load(std::memory_order_acquire);
			}

			// 2. Store CLOCK position for cleanup (before any waiting)
			int clockPosition = obj->m_nPositionInCLOCK;
			ASSERT(m_nClockHand == clockPosition);
			// 4. Clean up CLOCK buffer (before commit to prevent race condition)
			obj->m_nPositionInCLOCK = -1;
			m_vtClockBufferWeight[clockPosition] = -1;
			m_vtClockBuffer[clockPosition] = nullptr;

			// 5. Write object and commit (CommitCounter update)
			m_arrEvictedItems[mySlot] = obj;  // No mod needed! uint8_t naturally 0-255
			//std::atomic_thread_fence(std::memory_order_release);  // Ensure write is visible
			m_nCommitCounter.store(mySlot + 1, std::memory_order_release);

			ASSERT(m_nUsedCacheCapacity.load(std::memory_order_relaxed) != 0);
			m_nUsedCacheCapacity.fetch_sub(1, std::memory_order_relaxed);
		#else //__CLOCK_WITH_BUFFER__
			bool is_dirty = obj->hasUpdatesToBeFlushed();
			if (is_dirty)
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(obj, uidUpdated) != CacheErrorCode::Success)
					throw std::logic_error("Critical: failed to add object to storage during eviction.");

				obj->m_uidUpdated = uidUpdated;
			}

		#ifdef __CACHE_COUNTERS__
			// Record eviction with dirty flag
			this->recordEviction(is_dirty);
		#endif

			ASSERT(m_nClockHand == obj->m_nPositionInCLOCK);

			m_vtClockBuffer[m_nClockHand] = nullptr;
			m_vtClockBufferWeight[m_nClockHand] = -1;

			obj->m_bDirty = false;
			obj->deleteCoreObject();
			obj->m_nPositionInCLOCK = -1;

			obj->m_mtx.unlock();
			obj = nullptr;

			ASSERT(m_nUsedCacheCapacity.load(std::memory_order_relaxed) != 0);
			m_nUsedCacheCapacity.fetch_sub(1, std::memory_order_relaxed);
		#endif //__CLOCK_WITH_BUFFER__
#else //__CONCURRENT__
		bool is_dirty = obj->hasUpdatesToBeFlushed();
		if (is_dirty)
		{
			ObjectUIDType uidUpdated;
			if (m_ptrStorage->addObject(obj, uidUpdated) != CacheErrorCode::Success)
				throw std::logic_error("Critical: failed to add object to storage during eviction.");

			obj->m_uidUpdated = uidUpdated;
		}

	#ifdef __CACHE_COUNTERS__
		// Record eviction with dirty flag
		this->recordEviction(is_dirty);
	#endif

		ASSERT(m_nClockHand == obj->m_nPositionInCLOCK);

		m_vtClockBuffer[m_nClockHand] = nullptr;
		m_vtClockBufferWeight[m_nClockHand] = -1;

		obj->m_bDirty = false;
		obj->deleteCoreObject();
		obj->m_nPositionInCLOCK = -1;

		obj = nullptr;

		m_nUsedCacheCapacity--;
#endif //__CONCURRENT__

		return m_nClockHand;
		}
	}

	inline void evictAllItemsFromCache()
	{
		//return 0;// buggy code
#ifdef __CONCURRENT__
		auto nUsedCacheCapacity = m_nUsedCacheCapacity.load(std::memory_order_relaxed);
#else //__CONCURRENT__
		auto nUsedCacheCapacity = m_nUsedCacheCapacity;
#endif //__CONCURRENT__

		while (false)//nUsedCacheCapacity > 0)
		{
// #ifdef __CONCURRENT__
// 			std::cout << m_nUsedCacheCapacity.load(std::memory_order_relaxed) << "," << nUsedCacheCapacity << std::endl;
// #else //__CONCURRENT__
// 			std::cout << m_nUsedCacheCapacity << "," << nUsedCacheCapacity << std::endl;
// #endif //__CONCURRENT__
			if (m_vtClockBufferWeight[m_nClockHand] == -1)
			{
				ASSERT(!m_vtClockBuffer[m_nClockHand]);

				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

#ifdef __CONCURRENT__
			if (m_vtClockBuffer[m_nClockHand]->m_nUseCounter != 0)
			{
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}
#else //__CONCURRENT__
			if constexpr (CacheType::MARK_INUSE_FLAG)
			{
				if (m_vtClockBuffer[m_nClockHand]->m_bInUse)
				{
					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
					continue;
				}
			}
#endif //__CONCURRENT__

			if (m_vtClockBufferWeight[m_nClockHand] > 0)
			{
				m_vtClockBufferWeight[m_nClockHand]--;
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

			auto obj = m_vtClockBuffer[m_nClockHand];
#ifdef __CONCURRENT__
			if (!obj->m_mtx.try_lock())
			{
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

			if (m_vtClockBuffer[m_nClockHand]->m_nUseCounter != 0)
			{
				obj->m_mtx.unlock();
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}
#endif //__CONCURRENT__

			ASSERT(m_vtClockBuffer[m_nClockHand]);
			ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);
			
			if (obj->m_bMarkDelete)
			{
				ASSERT(m_nClockHand == obj->m_nPositionInCLOCK);

				if (obj->m_uid.getMediaType() > 1)
				{
					m_ptrStorage->remove(obj->m_uid);
				}

				if (obj->m_nPositionInCLOCK != -1)
				{
					ASSERT(m_vtClockBuffer[obj->m_nPositionInCLOCK]);

					m_vtClockBuffer[obj->m_nPositionInCLOCK] = nullptr;
					m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = -1;

					ASSERT(nUsedCacheCapacity != 0);
					nUsedCacheCapacity--;
				}

#ifdef __CONCURRENT__
				obj->m_mtx.unlock();
#endif //__CONCURRENT__

				delete obj;
				obj = nullptr;

				ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);
				ASSERT(m_vtClockBuffer[m_nClockHand] == nullptr);

				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

			if (obj->_havedependentsincache())
			{
#ifdef __CONCURRENT__
				obj->m_mtx.unlock();
#endif //__CONCURRENT__
				m_vtClockBufferWeight[m_nClockHand] = 0;
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

			//#ifdef __CONCURRENT__
			//
			//			// Circular buffer eviction with uint8_t counters
			//			// 1. Reserve slot (WriteCounter update)
			//			uint8_t mySlot = m_nWriteCounter.fetch_add(1, std::memory_order_acq_rel);
			//
			//			// 3. Wait for my turn (FIFO ordering)
			//			uint8_t expectedPrev = mySlot;
			//			uint8_t currentCommit = m_nCommitCounter.load(std::memory_order_acquire);
			//
			//			while (currentCommit != expectedPrev || m_arrEvictedItems[mySlot] != nullptr) {
			//				std::this_thread::yield();
			//				currentCommit = m_nCommitCounter.load(std::memory_order_acquire);
			//			}
			//
			//			// 2. Store CLOCK position for cleanup (before any waiting)
			//			int clockPosition = obj->m_nPositionInCLOCK;
			//			ASSERT(m_nClockHand == clockPosition);
			//			// 4. Clean up CLOCK buffer (before commit to prevent race condition)
			//			obj->m_nPositionInCLOCK = -1;
			//			m_vtClockBufferWeight[clockPosition] = -1;
			//			m_vtClockBuffer[clockPosition] = nullptr;
			//
			//			// 5. Write object and commit (CommitCounter update)
			//			m_arrEvictedItems[mySlot] = obj;  // No mod needed! uint8_t naturally 0-255
			//			//std::atomic_thread_fence(std::memory_order_release);  // Ensure write is visible
			//			m_nCommitCounter.store(mySlot + 1, std::memory_order_release);
			//
			//			m_nUsedCacheCapacity.fetch_add(1, std::memory_order_relaxed);
			//#else //__CONCURRENT__

			bool is_dirty = obj->hasUpdatesToBeFlushed();
			if (is_dirty)
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(obj, uidUpdated) != CacheErrorCode::Success)
					throw std::logic_error("Critical: failed to add object to storage during eviction.");

				obj->m_uidUpdated = uidUpdated;
			}

#ifdef __CACHE_COUNTERS__
			// Record eviction with dirty flag
			this->recordEviction(is_dirty);
#endif

			ASSERT(m_nClockHand == obj->m_nPositionInCLOCK);

			m_vtClockBuffer[m_nClockHand] = nullptr;
			m_vtClockBufferWeight[m_nClockHand] = -1;

			obj->m_bDirty = false;
			obj->deleteCoreObject();
			obj->m_nPositionInCLOCK = -1;

			obj = nullptr;

			ASSERT(nUsedCacheCapacity != 0);
			nUsedCacheCapacity--;
			m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
		}

#ifdef __CONCURRENT__
			m_nUsedCacheCapacity.store(0, std::memory_order_relaxed);
#else //__CONCURRENT__
			m_nUsedCacheCapacity = 0;
#endif //__CONCURRENT__
	}

private:

	inline void flushItemsToStorage()
	{
	#if defined(__CONCURRENT__) && defined(__CLOCK_WITH_BUFFER__)
	#ifdef __CONCURRENT__
		std::vector<ObjectTypePtr> vtObjects;

		int nQSiz = 0;
		{
			std::unique_lock<std::mutex> lock_cache(m_mtxCache);
			if ((nQSiz = m_vtClockQ.size()) == 0) return;
		}

		int nIdx = 0;
		for (; nIdx < nQSiz; nIdx++)
		{
			auto& ptrObjToUpdate = m_vtClockQ[nIdx].first;

			if (ptrObjToUpdate == nullptr || ptrObjToUpdate->m_ptrCoreObject == nullptr) continue;

			if (ptrObjToUpdate->m_nPositionInCLOCK == -1)
			{
				while (true)
				{
					if (m_vtClockBufferWeight[m_nClockHand] == -1)
					{
						ASSERT(!m_vtClockBuffer[m_nClockHand]);
						break;
					}

					if (m_vtClockBufferWeight[m_nClockHand] > 0)
					{
						m_vtClockBufferWeight[m_nClockHand]--;
						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					if (m_vtClockBuffer[m_nClockHand]->m_nUseCounter != 0)
					{
						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					auto& obj = m_vtClockBuffer[m_nClockHand];

					if (!obj->m_mtx.try_lock())
					{
						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					if (obj->m_nUseCounter != 0)
					{
						obj->m_mtx.unlock();
						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}
					
					if (obj->m_bMarkDelete == true)
					{
						ASSERT(obj->m_ptrCoreObject);

						if (obj->m_uid.getMediaType() > 1)
						{
							m_ptrStorage->remove(obj->m_uid);
						}

						obj->deleteCoreObject();
						obj->m_uidUpdated = std::nullopt;
						obj->m_uid = ObjectUIDType(INT64_MAX);

						ASSERT(obj->m_nPositionInCLOCK != -1);

						ASSERT(m_vtClockBuffer[obj->m_nPositionInCLOCK]);
						ASSERT(m_vtClockBufferWeight[obj->m_nPositionInCLOCK] != -1);

						m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = -1;
						obj->m_nPositionInCLOCK = -1;

						obj->m_mtx.unlock();
						obj = nullptr;

						ASSERT(m_nUsedCacheCapacity.load(std::memory_order_relaxed) != 0);
						m_nUsedCacheCapacity.fetch_sub(1, std::memory_order_relaxed);
						ASSERT(!m_vtClockBuffer[m_nClockHand]);
						ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);


						break;
					}

					ASSERT(m_vtClockBuffer[m_nClockHand]);
					if (obj->havedependentsincache())
					{
						m_vtClockBufferWeight[m_nClockHand] = 0;
						obj->m_mtx.unlock();
						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					ASSERT(obj->m_ptrCoreObject);

					vtObjects.push_back(obj);

					ASSERT(obj->m_nPositionInCLOCK == m_nClockHand);

					m_vtClockBufferWeight[m_nClockHand] = -1;

					obj->m_nPositionInCLOCK = -1;
					//obj->reftofirstiteminpair = nullptr;

					ASSERT(ptrObjToUpdate->m_uid != obj->m_uid);

					m_vtClockBuffer[m_nClockHand] = nullptr;

					ASSERT(m_nUsedCacheCapacity.load(std::memory_order_relaxed) != 0);
					m_nUsedCacheCapacity.fetch_sub(1, std::memory_order_relaxed);
					ASSERT(!m_vtClockBuffer[m_nClockHand]);
					ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);

					break;
				}

				ASSERT(ptrObjToUpdate->m_ptrCoreObject);

				int nSlotIdx = m_nClockHand;

				ASSERT(m_vtClockBufferWeight[nSlotIdx] == -1);
				ASSERT(!m_vtClockBuffer[nSlotIdx]);

				m_vtClockBuffer[nSlotIdx] = ptrObjToUpdate;	// use atomics!!!!
				ptrObjToUpdate->m_nPositionInCLOCK = nSlotIdx;
			}

			auto _ = ptrObjToUpdate->m_nUseCounter.load(std::memory_order_relaxed);
			ASSERT(_ > 0);
			ptrObjToUpdate->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);

			m_vtClockBufferWeight[ptrObjToUpdate->m_nPositionInCLOCK] = m_vtClockQ[nIdx].second;
		}

		{
			ASSERT(nIdx == nQSiz);

			std::unique_lock<std::mutex> lock_cache(m_mtxCache);
			m_vtClockQ.erase(m_vtClockQ.begin(), m_vtClockQ.begin() + nIdx);
		}

		//cv.notify_all();

		int nLeftOver = 0;
		bool bProgress = false;
		bool bLeftOver = false;
		do
		{
			bProgress = false;
			bLeftOver = false;

			for (auto& obj : vtObjects)
			{
				if (!obj) continue;

				ASSERT(obj->m_nPositionInCLOCK == -1);

				if (obj->_havedependentsincache())
				{
					nLeftOver++;

					bLeftOver = true;
					continue;
				}

				bool is_dirty = obj->hasUpdatesToBeFlushed();
				if (is_dirty)
				{
					ObjectUIDType uidUpdated;
					if (m_ptrStorage->addObject(obj, uidUpdated) != CacheErrorCode::Success)
						throw std::logic_error("Critical: failed to add object to storage during eviction.");

					obj->m_uidUpdated = uidUpdated;
				}

	#ifdef __CACHE_COUNTERS__
				// Record eviction with dirty flag
				this->recordEviction(is_dirty);
	#endif

				obj->m_bDirty = false;

				ASSERT(obj->m_ptrCoreObject);
				obj->deleteCoreObject();

				obj->m_nPositionInCLOCK = -1;

				obj->m_mtx.unlock();
				obj = nullptr;
				bProgress = true;
			}
		} while (bLeftOver && bProgress);

		for (auto& obj : vtObjects)
		{
			if (!obj) continue;

			if (obj->_havedependentsincache())
			{
				ASSERT(nIdx == nQSiz);
				std::unique_lock<std::mutex> lock_cache(m_mtxCache);
				m_vtClockQ.push_back({ obj , 1 });
				ASSERT(m_vtClockBuffer.size() < m_nCacheCapacity * 2);

				obj->m_mtx.unlock();
			}
		}

		vtObjects.clear();

		{
			//std::unique_lock<std::mutex> lock_cache(m_mtxCache);
			//m_nUsedCacheCapacity -= nLeftOver;

			//ASSERT(m_nUsedCacheCapacity.load(std::memory_order_relaxed) != 0);
			m_nUsedCacheCapacity.fetch_add(nLeftOver, std::memory_order_relaxed);
		}

		cv.notify_all();

	#else //__CONCURRENT__
		while (true)
		{
			if (m_vtClockBufferWeight[m_nClockHand] == -1)
			{
				ASSERT(!m_vtClockBuffer[m_nClockHand]);
				return;
			}

			if (m_vtClockBufferWeight[m_nClockHand] > 0)
			{
				m_vtClockBufferWeight[m_nClockHand]--;
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

			// but use counter is part of concurrent setting!!
			//if (m_vtClockBuffer[m_nClockHand].use_count() > 2)
			//{
			//	m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
			//	continue;
			//}

			auto& obj = m_vtClockBuffer[m_nClockHand];
			ASSERT(m_vtClockBuffer[m_nClockHand]);
			ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);

			if (obj->m_ptrCoreObject == nullptr)
			{
				//ASSERT(false);

				obj->m_nPositionInCLOCK = -1;

				//m_vtClockBuffer[m_nClockHand] = nullptr;
				m_vtClockBuffer[m_nClockHand] = nullptr;
				m_vtClockBufferWeight[m_nClockHand] = -1;

				return;

			}

			//if (obj.use_count() > 2)
			//{
			//	m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
			//	continue;
			//}

			if (obj->_havedependentsincache())
			{
				m_vtClockBufferWeight[m_nClockHand] = 0;
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

			bool is_dirty = obj->hasUpdatesToBeFlushed();
			if (is_dirty)
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(obj, uidUpdated) != CacheErrorCode::Success)
					throw std::logic_error("Critical: failed to add object to storage during eviction.");
				obj->m_uidUpdated = uidUpdated;
			}

	#ifdef __CACHE_COUNTERS__
			// Record eviction with dirty flag
			this->recordEviction(is_dirty);
	#endif

			int idx = obj->m_nPositionInCLOCK;


			//obj->deleteCoreObject();
			obj->m_nFlushPriority = 0;
			obj->m_nPositionInCLOCK = -1;
			//*obj->reftofirstiteminpair = -1;
			//obj->reftofirstiteminpair = nullptr;
			//obj.reset();

			obj->m_bDirty = false;
			obj->deleteCoreObject();
			obj = nullptr;

			//m_vtClockBuffer[idx] = nullptr;
			m_vtClockBuffer[idx] = nullptr;
			m_vtClockBufferWeight[idx] = -1;

#ifdef __CONCURRENT__
			ASSERT(m_nUsedCacheCapacity.load(std::memory_order_relaxed) != 0);
			m_nUsedCacheCapacity.fetch_sub(1, std::memory_order_relaxed);
#else //__CONCURRENT__
			ASSERT(m_nUsedCacheCapacity != 0);
			m_nUsedCacheCapacity--;
#endif //__CONCURRENT__
			return;
		}
	#endif //__CONCURRENT__
	#else //defined(__CONCURRENT__) && defined(__CLOCK_WITH_BUFFER__)

	#ifdef __CONCURRENT__

		// Circular buffer flush pattern
		uint8_t currentFlush = m_nFlushCounter.load(std::memory_order_acquire);
		uint8_t currentCommit = m_nCommitCounter.load(std::memory_order_acquire);

		// Check if there are items to flush (items between FlushCounter and CommitCounter)
		//if (currentFlush != currentCommit) {
			// Process all committed items in the circular range
		uint8_t i = currentFlush;
		uint8_t endSlot = currentCommit - 1;

		do
		{
			auto obj = m_arrEvictedItems[i];
			if (obj != nullptr)
			{
				if (obj->_havedependentsincache())
				{
					std::cout << "...." << std::endl;
					throw std::logic_error("Critical: failed to add object to storage during eviction.");
				}

				bool is_dirty = obj->hasUpdatesToBeFlushed();
				if (is_dirty) {
					ObjectUIDType uidUpdated;
					if (m_ptrStorage->addObject(obj, uidUpdated) != CacheErrorCode::Success)
						throw std::logic_error("Critical: failed to add object to storage during eviction.");

					obj->m_uidUpdated = uidUpdated;
				}

	#ifdef __CACHE_COUNTERS__
				// Record eviction with dirty flag
				this->recordEviction(is_dirty);
	#endif

				obj->m_bDirty = false;
				obj->deleteCoreObject();
				obj->m_mtx.unlock();

				// Clear the array slot
				m_arrEvictedItems[i] = nullptr;
			}
		} while (i++ != endSlot);

		// Update flush progress (FlushCounter update)
		m_nFlushCounter.store(i, std::memory_order_release);
		//}
		//else {
		//	// No items to flush, sleep briefly
		//	std::this_thread::sleep_for(std::chrono::microseconds(10));
		//}
	#endif //__CONCURRENT__
	#endif //defined(__CONCURRENT__) && defined(__CLOCK_WITH_BUFFER__)
	}

	inline void flushAllItemsToStorage()
	{
	#if defined(__CONCURRENT__) && defined(__CLOCK_WITH_BUFFER__)
	#ifdef __CONCURRENT__
		std::vector<ObjectTypePtr> vtObjects;

		int nQSiz = 0;
		{
			std::unique_lock<std::mutex> lock_cache(m_mtxCache);
			if ((nQSiz = m_vtClockQ.size()) == 0) return;
		}

		int nIdx = 0;
		for (; nIdx < nQSiz; nIdx++)
		{
			auto& ptrObjToUpdate = m_vtClockQ[nIdx].first;

			if (ptrObjToUpdate == nullptr || ptrObjToUpdate->m_ptrCoreObject == nullptr) continue;

			if (ptrObjToUpdate->m_nPositionInCLOCK == -1)
			{
				while (true)
				{
					if (m_vtClockBufferWeight[m_nClockHand] == -1)
					{
						ASSERT(!m_vtClockBuffer[m_nClockHand]);
						break;
					}

					if (m_vtClockBufferWeight[m_nClockHand] > 0)
					{
						m_vtClockBufferWeight[m_nClockHand]--;
						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					if (m_vtClockBuffer[m_nClockHand]->m_nUseCounter != 0)
					{
						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					auto& obj = m_vtClockBuffer[m_nClockHand];

					if (!obj->m_mtx.try_lock())
					{
						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					if (obj->m_nUseCounter != 0)
					{
						obj->m_mtx.unlock();
						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					ASSERT(m_vtClockBuffer[m_nClockHand]);
					if (obj->havedependentsincache())
					{
						m_vtClockBufferWeight[m_nClockHand] = 0;
						obj->m_mtx.unlock();
						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					if (obj->m_bMarkDelete == true)
					{
						ASSERT(obj->m_ptrCoreObject);

						if (obj->m_uid.getMediaType() > 1)
						{
							m_ptrStorage->remove(obj->m_uid);
						}

						obj->deleteCoreObject();
						obj->m_uidUpdated = std::nullopt;
						obj->m_uid = ObjectUIDType(INT64_MAX);

						ASSERT(obj->m_nPositionInCLOCK != -1);

						ASSERT(m_vtClockBuffer[obj->m_nPositionInCLOCK]);
						ASSERT(m_vtClockBufferWeight[obj->m_nPositionInCLOCK] != -1);

						m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = -1;
						obj->m_nPositionInCLOCK = -1;

						obj->m_mtx.unlock();
						obj = nullptr;

						ASSERT(m_nUsedCacheCapacity.load(std::memory_order_relaxed) != 0);
						m_nUsedCacheCapacity.fetch_sub(1, std::memory_order_relaxed);
						ASSERT(!m_vtClockBuffer[m_nClockHand]);
						ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);

						break;
					}

					ASSERT(obj->m_ptrCoreObject);

					vtObjects.push_back(obj);

					ASSERT(obj->m_nPositionInCLOCK == m_nClockHand);

					m_vtClockBufferWeight[m_nClockHand] = -1;

					obj->m_nPositionInCLOCK = -1;
					//obj->reftofirstiteminpair = nullptr;

					ASSERT(ptrObjToUpdate->m_uid != obj->m_uid);

					m_vtClockBuffer[m_nClockHand] = nullptr;

					ASSERT(m_nUsedCacheCapacity.load(std::memory_order_relaxed) != 0);
					m_nUsedCacheCapacity.fetch_sub(1, std::memory_order_relaxed);
					ASSERT(!m_vtClockBuffer[m_nClockHand]);
					ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);

					break;
				}

				ASSERT(ptrObjToUpdate->m_ptrCoreObject);

				int nSlotIdx = m_nClockHand;

				ASSERT(m_vtClockBufferWeight[nSlotIdx] == -1);
				ASSERT(!m_vtClockBuffer[nSlotIdx]);

				m_vtClockBuffer[nSlotIdx] = ptrObjToUpdate;	// use atomics!!!!
				ptrObjToUpdate->m_nPositionInCLOCK = nSlotIdx;
			}

			auto _ = ptrObjToUpdate->m_nUseCounter.load(std::memory_order_relaxed);
			ASSERT(_ > 0);
			ptrObjToUpdate->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);

			m_vtClockBufferWeight[ptrObjToUpdate->m_nPositionInCLOCK] = m_vtClockQ[nIdx].second;
		}

		{
			ASSERT(nIdx == nQSiz);

			std::unique_lock<std::mutex> lock_cache(m_mtxCache);
			m_vtClockQ.erase(m_vtClockQ.begin(), m_vtClockQ.begin() + nIdx);
		}

		//cv.notify_all();

		int nLeftOver = 0;
		bool bProgress = false;
		bool bLeftOver = false;
		do
		{
			bProgress = false;
			bLeftOver = false;

			for (auto& obj : vtObjects)
			{
				if (!obj) continue;

				ASSERT(obj->m_nPositionInCLOCK == -1);

				if (obj->_havedependentsincache())
				{
					nLeftOver++;
					bLeftOver = true;
					continue;
				}

				bool is_dirty = obj->hasUpdatesToBeFlushed();
				if (is_dirty)
				{
					ObjectUIDType uidUpdated;
					if (m_ptrStorage->addObject(obj, uidUpdated) != CacheErrorCode::Success)
						throw std::logic_error("Critical: failed to add object to storage during eviction.");

					obj->m_uidUpdated = uidUpdated;
				}

	#ifdef __CACHE_COUNTERS__
				// Record eviction with dirty flag
				this->recordEviction(is_dirty);
	#endif

				obj->m_bDirty = false;

				ASSERT(obj->m_ptrCoreObject);
				obj->deleteCoreObject();

				obj->m_nPositionInCLOCK = -1;

				obj->m_mtx.unlock();
				obj = nullptr;
				bProgress = true;
			}
		} while (bLeftOver && bProgress);

		for (auto& obj : vtObjects)
		{
			if (!obj) continue;

			if (obj->_havedependentsincache())
			{
				ASSERT(nIdx == nQSiz);
				std::unique_lock<std::mutex> lock_cache(m_mtxCache);
				m_vtClockQ.push_back({ obj , 1 });
				ASSERT(m_vtClockBuffer.size() < m_nCacheCapacity * 2);

				obj->m_mtx.unlock();
			}
		}

		vtObjects.clear();

		{
			//std::unique_lock<std::mutex> lock_cache(m_mtxCache);
			//m_nUsedCacheCapacity -= nLeftOver;
			m_nUsedCacheCapacity.fetch_add(nLeftOver, std::memory_order_relaxed);
		}

		cv.notify_all();

	#else //__CONCURRENT__
		while (true)
		{
			if (m_vtClockBufferWeight[m_nClockHand] == -1)
			{
				ASSERT(!m_vtClockBuffer[m_nClockHand]);
				return;
			}

			if (m_vtClockBufferWeight[m_nClockHand] > 0)
			{
				m_vtClockBufferWeight[m_nClockHand]--;
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

			// but use counter is part of concurrent setting!!
			//if (m_vtClockBuffer[m_nClockHand].use_count() > 2)
			//{
			//	m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
			//	continue;
			//}

			auto& obj = m_vtClockBuffer[m_nClockHand];
			ASSERT(m_vtClockBuffer[m_nClockHand]);
			ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);

			if (obj->m_ptrCoreObject == nullptr)
			{
				//ASSERT(false);

				obj->m_nPositionInCLOCK = -1;

				//m_vtClockBuffer[m_nClockHand] = nullptr;
				m_vtClockBuffer[m_nClockHand] = nullptr;
				m_vtClockBufferWeight[m_nClockHand] = -1;

				return;

			}

			//if (obj.use_count() > 2)
			//{
			//	m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
			//	continue;
			//}

			if (obj->_havedependentsincache())
			{
				m_vtClockBufferWeight[m_nClockHand] = 0;
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

			bool is_dirty = obj->hasUpdatesToBeFlushed();
			if (is_dirty)
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(obj, uidUpdated) != CacheErrorCode::Success)
					throw std::logic_error("Critical: failed to add object to storage during eviction.");
				obj->m_uidUpdated = uidUpdated;
			}

	#ifdef __CACHE_COUNTERS__
			// Record eviction with dirty flag
			this->recordEviction(is_dirty);
	#endif

			int idx = obj->m_nPositionInCLOCK;


			//obj->deleteCoreObject();
			obj->m_nFlushPriority = 0;
			obj->m_nPositionInCLOCK = -1;
			//*obj->reftofirstiteminpair = -1;
			//obj->reftofirstiteminpair = nullptr;
			//obj.reset();

			obj->m_bDirty = false;
			obj->deleteCoreObject();
			obj = nullptr;

			//m_vtClockBuffer[idx] = nullptr;
			m_vtClockBuffer[idx] = nullptr;
			m_vtClockBufferWeight[idx] = -1;

#ifdef __CONCURRENT__
			ASSERT(m_nUsedCacheCapacity.load(std::memory_order_relaxed) != 0);

			m_nUsedCacheCapacity.fetch_sub(1, std::memory_order_relaxed);
#else //__CONCURRENT__
			ASSERT(m_nUsedCacheCapacity != 0);
			m_nUsedCacheCapacity--;
#endif //__CONCURRENT__
			return;
		}
	#endif //__CONCURRENT__
	#else //defined(__CONCURRENT__) && defined(__CLOCK_WITH_BUFFER__)

	#ifdef __CONCURRENT__

		// Circular buffer flush pattern
		uint8_t currentFlush = m_nFlushCounter.load(std::memory_order_acquire);
		uint8_t currentCommit = m_nCommitCounter.load(std::memory_order_acquire);

		// Check if there are items to flush (items between FlushCounter and CommitCounter)
		//if (currentFlush != currentCommit) {
			// Process all committed items in the circular range
		uint8_t i = currentFlush;
		uint8_t endSlot = currentCommit - 1;

		do
		{
			auto obj = m_arrEvictedItems[i];
			if (obj != nullptr)
			{
				if (obj->_havedependentsincache())
				{
					std::cout << "...." << std::endl;
					throw std::logic_error("Critical: failed to add object to storage during eviction.");
				}

				bool is_dirty = obj->hasUpdatesToBeFlushed();
				if (is_dirty) {
					ObjectUIDType uidUpdated;
					if (m_ptrStorage->addObject(obj, uidUpdated) != CacheErrorCode::Success)
						throw std::logic_error("Critical: failed to add object to storage during eviction.");

					obj->m_uidUpdated = uidUpdated;
				}

	#ifdef __CACHE_COUNTERS__
				// Record eviction with dirty flag
				this->recordEviction(is_dirty);
	#endif

				obj->m_bDirty = false;
				obj->deleteCoreObject();
				obj->m_mtx.unlock();

				// Clear the array slot
				m_arrEvictedItems[i] = nullptr;
			}
		} while (i++ != endSlot);

		// Update flush progress (FlushCounter update)
		m_nFlushCounter.store(i, std::memory_order_release);
		//}
		//else {
		//	// No items to flush, sleep briefly
		//	std::this_thread::sleep_for(std::chrono::microseconds(10));
		//}
	#endif //__CONCURRENT__
	#endif //defined(__CONCURRENT__) && defined(__CLOCK_WITH_BUFFER__)

		evictAllItemsFromCache();
	}
	//
	//public:
	//	inline void persistAllItems()
	//	{
	//#ifdef __CONCURRENT__
	//		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
	//#endif //__CONCURRENT__
	//
	//		ObjectTypePtr ptrItemToFlush = m_ptrTail;
	//
	//		while (ptrItemToFlush != nullptr)
	//		{
	//			//if (m_ptrTail.use_count() > 3)
	//			//{
	//			//	/* Info:
	//			//	 * Should proceed with the preceeding one?
	//			//	 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
	//			//	 */
	//			//	std::cout << "Critical State: Failed to add object to Storage." << std::endl;
	//			//	throw new std::logic_error(".....");   // TODO: critical log.
	//			//}
	//
	//#ifdef __CONCURRENT__
	//			// Check if the object is in use
	//			std::shared_lock<std::shared_mutex> lock_item(ptrItemToFlush->m_mtx);
	//			//if (!ptrItemToFlush->m_mtx.lock())
	//			//{
	//			//	throw new std::logic_error("The object is still in use");
	//			//}
	//#endif //__CONCURRENT__
	//
	//			ptrItemToFlush->m_uidUpdated = std::nullopt;
	//			if (ptrItemToFlush->hasUpdatesToBeFlushed())
	//			{
	//				ObjectUIDType uidUpdated;
	//				if (m_ptrStorage->addObject(ptrItemToFlush->m_uid, ptrItemToFlush, uidUpdated, ptrItemToFlush->pendingFlag) != CacheErrorCode::Success)
	//				{
	//					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
	//					throw new std::logic_error(".....");   // TODO: critical log.
	//				}
	//
	//				ptrItemToFlush->m_uidUpdated = uidUpdated;
	//			}
	//
	//			ptrItemToFlush->m_bDirty = false;
	//
	//			//ObjectTypePtr ptrItemToFlush = m_ptrTail;
	//
	//			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
	//			ptrItemToFlush = ptrItemToFlush->m_ptrPrev.lock();
	//
	//			/*
	//			// If the list still has elements, update the new tail's next pointer
	//			if (m_ptrTail)
	//			{
	//				m_ptrTail->m_ptrNext.reset();  // Optional: Explicitly reset next pointer
	//			}
	//			else
	//			{
	//				// The list had only one element, so update the head as well
	//				m_ptrHead.reset();
	//			}
	//
	//			// Clear the removed node's pointers
	//			ptrItemToFlush->m_ptrPrev.reset();
	//			ptrItemToFlush->m_ptrNext.reset();
	//
	//			ptrItemToFlush->deleteCoreObject();
	//			*/
	//
	//#ifdef __CONCURRENT__
	//			ptrItemToFlush->m_mtx.unlock();
	//#endif //__CONCURRENT__
	//
	//			/*
	//			ptrItemToFlush.reset();
	//
	//			m_nUsedCacheCapacity--;
	//			*/
	//		}
	//
	//		//ASSERT(m_nUsedCacheCapacity == 0);
	//	}

	#ifdef __CONCURRENT__
	static void handlerCacheFlush(SelfType* ptrSelf)
	{
		std::vector<int> vtEmptySlots;
		vtEmptySlots.reserve(ptrSelf->m_nCacheCapacity);

		do
		{
			ptrSelf->flushItemsToStorage();

			std::this_thread::sleep_for(1ms);

		} while (!ptrSelf->m_bStop);

	#ifdef __CACHE_COUNTERS__
		// Append thread-local stats from background thread before thread exits
		// Access the background thread's thread-local data through the CacheStatsProvider interface
		const auto* statsProvider = static_cast<const CacheStatsProvider<CLOCKCache<Traits>>*>(ptrSelf);

		// Append the background thread's thread-local data to the main cache instance
		auto bgHits = statsProvider->getHitsTimeline();
		auto bgMisses = statsProvider->getMissesTimeline();
		auto bgEvictions = statsProvider->getEvictionsTimeline();
		auto bgDirtyEvictions = statsProvider->getDirtyEvictionsTimeline();

		ptrSelf->m_backgroundThreadHits.insert(ptrSelf->m_backgroundThreadHits.end(), bgHits.begin(), bgHits.end());
		ptrSelf->m_backgroundThreadMisses.insert(ptrSelf->m_backgroundThreadMisses.end(), bgMisses.begin(), bgMisses.end());
		ptrSelf->m_backgroundThreadEvictions.insert(ptrSelf->m_backgroundThreadEvictions.end(), bgEvictions.begin(), bgEvictions.end());
		ptrSelf->m_backgroundThreadDirtyEvictions.insert(ptrSelf->m_backgroundThreadDirtyEvictions.end(), bgDirtyEvictions.begin(), bgDirtyEvictions.end());
	#endif
	}
	#endif //__CONCURRENT__
};