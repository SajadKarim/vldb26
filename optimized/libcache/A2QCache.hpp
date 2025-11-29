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
#include  <algorithm>
#include <tuple>
#include <condition_variable>
#include <unordered_map>
#include "CacheErrorCodes.h"
#include <optional>
#include <algorithm>
#include "validityasserts.h"
#ifdef __CACHE_COUNTERS__
#include "CacheStatsProvider.hpp"
#endif

//#define __MANAGE_GHOST_Q__

template <typename Traits>
class A2QCache 
#ifdef __CACHE_COUNTERS__
    : public CacheStatsProvider<A2QCache<Traits>>
#endif
{
	typedef A2QCache<Traits> SelfType;

public:
	using KeyType = typename Traits::KeyType;
	using ValueType = typename Traits::ValueType;

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
	static constexpr bool MARK_INUSE_FLAG = false;
#endif //__CONCURRENT__

private:
	enum QType
	{
		NONE = 0,
		HYBRID = 1,
		FREQUENT = 2
	};

	struct GhostNode
	{
		ObjectUIDType m_uid;
		GhostNode* m_ptrPrev;
		GhostNode* m_ptrNext;

		// Default constructor
		GhostNode()
			: m_uid(0), m_ptrPrev(nullptr), m_ptrNext(nullptr)
		{}

		// Default constructor
		GhostNode(const ObjectUIDType& uid)
			: m_uid(uid), m_ptrPrev(nullptr), m_ptrNext(nullptr)
		{}
	};

#ifdef __MANAGE_GHOST_Q__
#ifdef __CONCURRENT__
	std::atomic<uint64_t> m_nUsedCapacityGhostQ;
	mutable std::shared_mutex m_mtxGhostQ;
#else //__CONCURRENT__
	uint64_t m_nUsedCapacityGhostQ = 0;
#endif //__CONCURRENT__
	GhostNode* m_ptrHeadGhostQ;
	GhostNode* m_ptrTailGhostQ;
	std::unordered_map<ObjectUIDType, GhostNode*> m_ghostMap;
#endif //__MANAGE_GHOST_Q__

#ifdef __CONCURRENT__
	std::atomic<double> m_nFrequentQRatio;
	std::atomic<uint64_t> m_nUsedCapacityHybridQ;
	std::atomic<uint64_t> m_nUsedCapacityFrequentQ;
#else //__CONCURRENT__
	double m_nFrequentQRatio;
	uint64_t m_nUsedCapacityHybridQ, m_nUsedCapacityFrequentQ;
#endif //__CONCURRENT__

	uint64_t m_nCacheCapacity;

	WALType* m_ptrWAL;

	ObjectTypePtr m_ptrHeadHybridQ, m_ptrHeadFrequentQ;
	ObjectTypePtr m_ptrTailHybridQ, m_ptrTailFrequentQ;

#ifdef __CONCURRENT__
	bool m_bStop;
	std::thread m_threadCacheFlush;
	mutable std::shared_mutex m_mtxCache;
	mutable std::shared_mutex m_mtxStorage;

#ifdef __CACHE_COUNTERS__
	// Storage for background thread's cache statistics
	std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> m_backgroundThreadHits;
	std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> m_backgroundThreadMisses;
	std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> m_backgroundThreadEvictions;
	std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> m_backgroundThreadDirtyEvictions;
#endif //__CACHE_COUNTERS__
#endif //__CONCURRENT__

	StorageType* m_ptrStorage;

	// Direct capacity calculation using ratio
	inline uint64_t getCapacityFrequentQ() const {
#ifdef __CONCURRENT__
		double ratio = m_nFrequentQRatio.load(std::memory_order_relaxed);
#else
		double ratio = m_nFrequentQRatio;
#endif
		return static_cast<uint64_t>(m_nCacheCapacity * ratio);
	}

	inline uint64_t getCapacityHybridQ() const {
#ifdef __CONCURRENT__
		double ratio = m_nFrequentQRatio.load(std::memory_order_relaxed);
#else
		double ratio = m_nFrequentQRatio;
#endif
		return m_nCacheCapacity - static_cast<uint64_t>(m_nCacheCapacity * ratio);
	}

public:
	~A2QCache()
	{
#ifdef __CONCURRENT__
		m_bStop = true;
		m_threadCacheFlush.join();
#endif //__CONCURRENT__
		//m_ptrWAL.reset();

		flush(false);

		m_ptrHeadHybridQ = nullptr;
		m_ptrTailHybridQ = nullptr;

		m_ptrHeadFrequentQ = nullptr;
		m_ptrTailFrequentQ = nullptr;

#ifdef __MANAGE_GHOST_Q__
		GhostNode* ptrItem = m_ptrHeadGhostQ;
		while (ptrItem != nullptr)
		{
			GhostNode* ptrCurrent = ptrItem;
			ptrItem = ptrCurrent->m_ptrNext;

			ptrCurrent->m_ptrNext = nullptr;
			ptrCurrent->m_ptrPrev = nullptr;

			delete ptrCurrent;
			ptrCurrent = nullptr;
		}


		m_ghostMap.clear();
		m_ptrHeadGhostQ = nullptr;
		m_ptrTailGhostQ = nullptr;
#endif //__MANAGE_GHOST_Q__

		//delete m_ptrWAL;
		//m_ptrWAL = nullptr;

		delete m_ptrStorage;
		m_ptrStorage = nullptr;
	}

	template <typename... StorageArgs>
	A2QCache(size_t nCapacity, StorageArgs... args)
		: m_nCacheCapacity(nCapacity)
		, m_nFrequentQRatio(1.0 / 3.0) // Initial ratio 1/3
		, m_nUsedCapacityHybridQ(0)
		, m_nUsedCapacityFrequentQ(0)
		, m_ptrHeadHybridQ(nullptr)
		, m_ptrTailHybridQ(nullptr)
		, m_ptrHeadFrequentQ(nullptr)
		, m_ptrTailFrequentQ(nullptr)
#ifdef __MANAGE_GHOST_Q__
		, m_nUsedCapacityGhostQ(0)
		, m_ptrHeadGhostQ(nullptr)
		, m_ptrTailGhostQ(nullptr)
#endif //__MANAGE_GHOST_Q__	
	{
#ifdef __CACHE_COUNTERS__
		// Reset thread-local stats when creating a new cache instance
		CacheStatsProvider<A2QCache<Traits>>::resetThreadLocalStats();
#endif
		
		// Verify initial capacities sum to total capacity
		ASSERT(getCapacityFrequentQ() + getCapacityHybridQ() == nCapacity);

		m_ptrStorage = new StorageType(args...);

#ifdef __CONCURRENT__
		m_bStop = false;
		m_threadCacheFlush = std::thread(handlerCacheFlush, this);
#endif //__CONCURRENT__

		//m_ptrWAL = new WALType(this, WAL_FILE_PATH);
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
	const CacheStatsProvider<A2QCache<Traits>>* getCacheStatsProvider() const
	{
		return static_cast<const CacheStatsProvider<A2QCache<Traits>>*>(this);
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
#ifdef __CONCURRENT__
			for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
			{
				auto& obj = *it;

				if (obj != nullptr && obj->m_ptrCoreObject != nullptr)
				{
					ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
					obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
				}
			}
#endif //__CONCURRENT__
			return CacheErrorCode::Success;
		}
#endif //__SELECTIVE_UPDATE__

		uint64_t nUsedCapacityFrequentQ = 0;
		uint64_t nUsedCapacityHybridQ = 0;

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		bool bChildInFrequentQ = false;
		for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
		{
			auto& obj = *it;

			if (obj != nullptr && obj->m_ptrCoreObject != nullptr)
			{
				if (obj->m_nQueueType == QType::NONE && bChildInFrequentQ)
				{
					obj->m_nQueueType = QType::FREQUENT;

					nUsedCapacityFrequentQ++;
					nUsedCapacityHybridQ++;
				}

				moveToTheFrontOfCacheQ(obj, nUsedCapacityFrequentQ, nUsedCapacityHybridQ);

				bChildInFrequentQ |= (obj->m_nQueueType > QType::NONE);

#if defined(__CONCURRENT__) && defined(__TREE_WITH_CACHE__)
				ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);		// call this once?? at the end???
				obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
#endif //__CONCURRENT__ && __TREE_WITH_CACHE__
			}
		}

#ifdef __CONCURRENT__
		lock_cache.unlock();
#endif //__CONCURRENT__

		vtObjects.clear();

#ifdef __CONCURRENT__
		m_nUsedCapacityFrequentQ.fetch_add(nUsedCapacityFrequentQ, std::memory_order_relaxed);
		m_nUsedCapacityHybridQ.fetch_sub(nUsedCapacityHybridQ, std::memory_order_relaxed);
#else //__CONCURRENT__
		m_nUsedCapacityFrequentQ += nUsedCapacityFrequentQ;
		m_nUsedCapacityHybridQ -= nUsedCapacityHybridQ;
#endif //__CONCURRENT__

#ifndef __CONCURRENT__
		flushItemsToStorage();
#endif //__CONCURRENT__

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
#ifdef __CONCURRENT__
			for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
			{
				auto& lhs = (*it).first;
				auto& rhs = (*it).second;

				if (lhs != nullptr && lhs->m_ptrCoreObject != nullptr)
				{
					ASSERT(lhs->m_nUseCounter.load(std::memory_order_relaxed) > 0);
					lhs->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
				}

				if (rhs != nullptr && rhs->m_ptrCoreObject != nullptr)
				{
					ASSERT(rhs->m_nUseCounter.load(std::memory_order_relaxed) > 0);
					rhs->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
				}
			}
#endif //__CONCURRENT__
			return CacheErrorCode::Success;
		}
#endif //__SELECTIVE_UPDATE__


		uint64_t nUsedCapacityFrequentQ = 0;
		uint64_t nUsedCapacityHybridQ = 0;

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		bool bChildInFrequentQ = false;
		for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
		{
			auto& lhs = (*it).first;
			auto& rhs = (*it).second;

			if (lhs != nullptr && lhs->m_ptrCoreObject != nullptr)
			{
				if (lhs->m_nQueueType == QType::NONE && bChildInFrequentQ)
				{
					lhs->m_nQueueType = QType::FREQUENT;

					nUsedCapacityFrequentQ++;
					nUsedCapacityHybridQ++;
				}

				moveToTheFrontOfCacheQ(lhs, nUsedCapacityFrequentQ, nUsedCapacityHybridQ);

				bChildInFrequentQ |= (lhs->m_nQueueType > QType::NONE);

#if defined(__CONCURRENT__) && defined(__TREE_WITH_CACHE__)
				ASSERT(lhs->m_nUseCounter.load(std::memory_order_relaxed) > 0);
				lhs->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
#endif //__CONCURRENT__ && __TREE_WITH_CACHE__

			}

			if (rhs != nullptr && rhs->m_ptrCoreObject != nullptr)
			{
				if (rhs->m_nQueueType == QType::NONE && bChildInFrequentQ)
				{
					rhs->m_nQueueType = QType::FREQUENT;

					nUsedCapacityFrequentQ++;
					nUsedCapacityHybridQ++;
				}

				moveToTheFrontOfCacheQ(rhs, nUsedCapacityFrequentQ, nUsedCapacityHybridQ);

				bChildInFrequentQ |= (rhs->m_nQueueType > QType::NONE);

#if defined(__CONCURRENT__) && defined(__TREE_WITH_CACHE__)
				ASSERT(rhs->m_nUseCounter.load(std::memory_order_relaxed) > 0);
				rhs->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
#endif //__CONCURRENT__ && __TREE_WITH_CACHE__
			}
		}

#ifdef __CONCURRENT__
		lock_cache.unlock();
#endif //__CONCURRENT__

		vtObjects.clear();

#ifdef __CONCURRENT__
		m_nUsedCapacityFrequentQ.fetch_add(nUsedCapacityFrequentQ, std::memory_order_relaxed);
		m_nUsedCapacityHybridQ.fetch_sub(nUsedCapacityHybridQ, std::memory_order_relaxed);
#else //__CONCURRENT__
		m_nUsedCapacityFrequentQ += nUsedCapacityFrequentQ;
		m_nUsedCapacityHybridQ -= nUsedCapacityHybridQ;
#endif //__CONCURRENT__

#ifndef __CONCURRENT__
		flushItemsToStorage();
#endif //__CONCURRENT__

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
#ifdef __CONCURRENT__
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
						if ((*it).m_ptrToDiscard != nullptr) remove((*it).m_ptrToDiscard);
						continue;
					default:
						ASSERT(false && "Invalid state.");
					}

					if (obj == nullptr || obj->m_ptrCoreObject == nullptr) continue;

					ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
					obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
				}
			}
#endif //__CONCURRENT__
			return CacheErrorCode::Success;
		}
#endif //__SELECTIVE_UPDATE__

		uint64_t nUsedCapacityFrequentQ = 0;
		uint64_t nUsedCapacityHybridQ = 0;

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		bool bChildInFrequentQ = false;

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
					if ((*it).m_ptrToDiscard != nullptr) remove((*it).m_ptrToDiscard);
					continue;
				default:
					ASSERT(false && "Invalid state.");
				}

				if (obj == nullptr || obj->m_ptrCoreObject == nullptr) continue;

				if (obj->m_nQueueType == QType::NONE && bChildInFrequentQ)
				{
					obj->m_nQueueType = QType::FREQUENT;

					nUsedCapacityFrequentQ++;
					nUsedCapacityHybridQ++;
				}

				moveToTheFrontOfCacheQ(obj, nUsedCapacityFrequentQ, nUsedCapacityHybridQ);

				bChildInFrequentQ |= (obj->m_nQueueType > QType::NONE);

#if defined(__CONCURRENT__) && defined(__TREE_WITH_CACHE__)
				ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
				obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
#endif //__CONCURRENT__ && __TREE_WITH_CACHE__
			}
		}

#ifdef __CONCURRENT__
		lock_cache.unlock();
#endif //__CONCURRENT__

		vtObjects.clear();

#ifdef __CONCURRENT__
		m_nUsedCapacityFrequentQ.fetch_add(nUsedCapacityFrequentQ, std::memory_order_relaxed);
		m_nUsedCapacityHybridQ.fetch_sub(nUsedCapacityHybridQ, std::memory_order_relaxed);
#else //__CONCURRENT__
		m_nUsedCapacityFrequentQ += nUsedCapacityFrequentQ;
		m_nUsedCapacityHybridQ -= nUsedCapacityHybridQ;
#endif //__CONCURRENT__

#ifndef __CONCURRENT__
		flushItemsToStorage();
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}

	CacheErrorCode remove(ObjectTypePtr& ptrObject)
	{
		if (ptrObject->m_uid.getMediaType() > 1)
		{
			m_ptrStorage->remove(ptrObject->m_uid);
		}

#ifdef __CONCURRENT__
		//std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		removeFromCacheQ(ptrObject);

		delete ptrObject;
		ptrObject = nullptr;

		return CacheErrorCode::Success;
	}

	CacheErrorCode getCoreObject(uint16_t nDegree, const ObjectUIDType& uidObject, ObjectTypePtr& ptrObject)
	{
		m_ptrStorage->getObject(nDegree, uidObject, ptrObject);

		ASSERT(ptrObject->m_ptrCoreObject != nullptr && "The requested object does not exist.");

		ptrObject->m_nQueueType = QType::NONE;
		ptrObject->m_bIsDowgraded = false;

#ifdef __COST_WEIGHTED_EVICTION__
		// Capture the cost of accessing this object from storage
		if constexpr (requires { m_ptrStorage->getAccessCost(ptrObject->m_nCoreObjectType); })
		{
			uint64_t accessCost = m_ptrStorage->getAccessCost(ptrObject->m_nCoreObjectType);
			ptrObject->setObjectCost(accessCost);
		}
		else
		{
			ptrObject->setObjectCost(1);
		}
#endif //__COST_WEIGHTED_EVICTION__

#ifdef __CONCURRENT__
		m_nUsedCapacityHybridQ.fetch_add(1, std::memory_order_relaxed);
#else //__CONCURRENT__
		m_nUsedCapacityHybridQ++;
#endif //__CONCURRENT__

#ifdef __MANAGE_GHOST_Q__
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> GhostQLock(m_mtxGhostQ);
#endif //__CONCURRENT__

		auto it = m_ghostMap.find(ptrObject->m_uid);
		if (it != m_ghostMap.end())
		{
			// Ghost queue hit: increase HybridQ capacity (decrease FrequentQ ratio)
			// Ensure FrequentQ ratio doesn't go below 1/capacity (to keep at least 1 item)
			double minRatio = 1.0 / m_nCacheCapacity;
			double adjustment = 1.0 / m_nCacheCapacity; // Adjust by 1 item worth
#ifdef __CONCURRENT__
			double currentRatio = m_nFrequentQRatio.load(std::memory_order_relaxed);
			if (currentRatio > minRatio)
			{
				m_nFrequentQRatio.store(std::max(minRatio, currentRatio - adjustment), std::memory_order_relaxed);
			}
#else
			if (m_nFrequentQRatio > minRatio)
			{
				m_nFrequentQRatio = std::max(minRatio, m_nFrequentQRatio - adjustment);
			}
#endif

			removeFromGhostQ(it->second);
			m_ghostMap.erase(ptrObject->m_uid);
		}
#endif //__MANAGE_GHOST_Q__

		return CacheErrorCode::Success;
	}

	CacheErrorCode getObject(uint16_t nDegree, const ObjectUIDType& uidObject, ObjectTypePtr& ptrObject)
	{
		ptrObject = m_ptrStorage->getObject(nDegree, uidObject);
		ptrObject->m_nQueueType = QType::NONE;

		ASSERT(ptrObject != nullptr && "The requested object does not exist.");

#ifdef __CONCURRENT__
		m_nUsedCapacityHybridQ.fetch_add(1, std::memory_order_relaxed);
#else //__CONCURRENT__
		m_nUsedCapacityHybridQ++;
#endif //__CONCURRENT__

#ifdef __MANAGE_GHOST_Q__
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> GhostQLock(m_mtxGhostQ);
#endif //__CONCURRENT__

		auto it = m_ghostMap.find(ptrObject->m_uid);
		if (it != m_ghostMap.end())
		{
			// Ghost queue hit: increase HybridQ capacity (decrease FrequentQ ratio)
			// Ensure FrequentQ ratio doesn't go below 1/capacity (to keep at least 1 item)
			double minRatio = 1.0 / m_nCacheCapacity;
			double adjustment = 1.0 / m_nCacheCapacity; // Adjust by 1 item worth
#ifdef __CONCURRENT__
			double currentRatio = m_nFrequentQRatio.load(std::memory_order_relaxed);
			if (currentRatio > minRatio)
			{
				m_nFrequentQRatio.store(std::max(minRatio, currentRatio - adjustment), std::memory_order_relaxed);
			}
#else
			if (m_nFrequentQRatio > minRatio)
			{
				m_nFrequentQRatio = std::max(minRatio, m_nFrequentQRatio - adjustment);
			}
#endif

			removeFromGhostQ(it->second);
			m_ghostMap.erase(ptrObject->m_uid);
		}
#endif //__MANAGE_GHOST_Q__

		return CacheErrorCode::Success;
	}

	template<class Type, typename... ArgsType>
	CacheErrorCode createObjectOfType(ObjectUIDType& uidObject, ObjectTypePtr& ptrObject, const ArgsType... args)
	{
		ptrObject = new ObjectType((uint8_t)Type::UID, new Type(args...), QType::NONE);

		uidObject.createUIDFromVolatilePointer((uint8_t)Type::UID, reinterpret_cast<uintptr_t>(ptrObject));

		ptrObject->m_uid = uidObject;

#ifdef __COST_WEIGHTED_EVICTION__
		// Newly created objects get default cost
		ptrObject->setObjectCost(1);
#endif //__COST_WEIGHTED_EVICTION__

#ifdef __CONCURRENT__
		m_nUsedCapacityHybridQ.fetch_add(1, std::memory_order_relaxed);
#else //__CONCURRENT__
		m_nUsedCapacityHybridQ++;
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}

	void getObjectsCountInCache(size_t& nObjects)
	{
		nObjects = 0;
		ObjectTypePtr ptrItem = m_ptrHeadHybridQ;

		while (ptrItem != nullptr)
		{
			nObjects++;
			ptrItem = ptrItem->m_ptrNext;
		} 

		ptrItem = m_ptrHeadFrequentQ;

		while (ptrItem != nullptr)
		{
			nObjects++;
			ptrItem = ptrItem->m_ptrNext;
		}

#ifdef __ENABLE_ASSERTS__
		size_t nValidate = 0;
		ptrItem = m_ptrTailHybridQ;

		while (ptrItem != nullptr)
		{
			nValidate++;
			ptrItem = ptrItem->m_ptrPrev.lock();
		}

		ptrItem = m_ptrTailFrequentQ;

		while (ptrItem != nullptr)
		{
			nValidate++;
			ptrItem = ptrItem->m_ptrPrev.lock();
		}

		ASSERT(nValidate == nObjects);
#endif //__ENABLE_ASSERTS__

	}

	CacheErrorCode flush(bool bStopFlushThread = true)
	{
		if (bStopFlushThread)
		{
#ifdef __CONCURRENT__
			m_bStop = true;
			m_threadCacheFlush.join();

#ifdef __CACHE_COUNTERS__
			// Move background thread's copied data to main thread's thread-local variables
			// This is efficient since we're transferring ownership using move semantics
			auto* statsProvider = static_cast<CacheStatsProvider<A2QCache<Traits>>*>(this);
			
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

		if (bStopFlushThread)
		{
#ifdef __CONCURRENT__
			m_bStop = false;
			m_threadCacheFlush = std::thread(handlerCacheFlush, this);
#endif //__CONCURRENT__
		}

		return CacheErrorCode::Success;
	}

private:
#ifdef __MANAGE_GHOST_Q__
	void addItemToGhostQ(const ObjectUIDType& uid)
	{
		GhostNode* ptrItem = nullptr;

		auto it = m_ghostMap.find(uid);
		if (it != m_ghostMap.end())
		{
			ptrItem = it->second;
		}
		else
		{
			ptrItem = new GhostNode(uid);
			m_ghostMap[uid] = ptrItem;

#ifdef __CONCURRENT__
			m_nUsedCapacityGhostQ.fetch_add(1, std::memory_order_relaxed);
#else //__CONCURRENT__
			++m_nUsedCapacityGhostQ;
#endif //__CONCURRENT__
		}

		//std::cout << "---";
		//std::cout << m_nUsedCapacityGhostQ.load(std::memory_order_relaxed);
		//std::cout << ",";
		//std::cout << m_nCapacityFrequentQ.load(std::memory_order_relaxed);
		//std::cout << '\n';

		
#ifndef __CONCURRENT__
		while (ptrItem != m_ptrTailGhostQ && m_nUsedCapacityGhostQ > getCapacityFrequentQ())
		{
			ASSERT (m_ptrTailGhostQ != nullptr);
			ASSERT(m_ptrHeadGhostQ != nullptr);

			--m_nUsedCapacityGhostQ;

			GhostNode* ptrTemp = m_ptrTailGhostQ;

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTailGhostQ = m_ptrTailGhostQ->m_ptrPrev;

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTailGhostQ)
			{
				m_ptrTailGhostQ->m_ptrNext = nullptr;  // Optional: Explicitly reset next pointer
			}
			else
			{
				ASSERT(m_ptrHeadGhostQ == ptrTemp);
				// The list had only one element, so update the head as well
				m_ptrHeadGhostQ = nullptr;
			}
			//std::cout << ",";

			m_ghostMap.erase(ptrTemp->m_uid);
			// Clear the removed node's pointers
			//std::cout << ",";
			ptrTemp->m_ptrPrev = nullptr;
			//std::cout << ",";
			ptrTemp->m_ptrNext = nullptr;
			delete ptrTemp;
			//std::cout << ",";
			ptrTemp = nullptr;
			//std::cout << ",";


		}
#endif //__CONCURRENT__

		//std::cout << ",";
		// Check for an empty list.
		if (m_ptrHeadGhostQ == nullptr)
		{
			m_ptrHeadGhostQ = ptrItem;
			m_ptrTailGhostQ = ptrItem;
			return;
		}

		// If the node is already at the front, do nothing.
		if (ptrItem == m_ptrHeadGhostQ)
		{
			return;
		}

		//// Detach ptrItem from its current position.
		if (ptrItem->m_ptrPrev != nullptr)
		{
			ptrItem->m_ptrPrev->m_ptrNext = ptrItem->m_ptrNext;
		}
		if (ptrItem->m_ptrNext != nullptr)
		{
			ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
		}
		if (ptrItem == m_ptrTailGhostQ)
		{
			m_ptrTailGhostQ = ptrItem->m_ptrPrev;
		}

		// Insert ptrItem at the front.
		ptrItem->m_ptrPrev = nullptr;  // Clear previous pointer.
		ptrItem->m_ptrNext = m_ptrHeadGhostQ;
		m_ptrHeadGhostQ->m_ptrPrev = ptrItem;  // weak_ptr can be directly assigned from a shared_ptr.
		m_ptrHeadGhostQ = ptrItem;
	}

	void addItemToGhostQ_()
	{
#ifdef __CONCURRENT__
		while (m_nUsedCapacityGhostQ.load(std::memory_order_relaxed) > getCapacityFrequentQ())
#else //__CONCURRENT__
		while (m_nUsedCapacityGhostQ > getCapacityFrequentQ())
#endif //__CONCURRENT__
		{
			ASSERT(m_ptrTailGhostQ != nullptr);
			ASSERT(m_ptrHeadGhostQ != nullptr);

#ifdef __CONCURRENT__
			m_nUsedCapacityGhostQ.fetch_sub(1, std::memory_order_relaxed);
#else //__CONCURRENT__
			--m_nUsedCapacityGhostQ;
#endif //__CONCURRENT__

			GhostNode* ptrTemp = m_ptrTailGhostQ;

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTailGhostQ = m_ptrTailGhostQ->m_ptrPrev;

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTailGhostQ)
			{
				m_ptrTailGhostQ->m_ptrNext = nullptr;  // Optional: Explicitly reset next pointer
			}
			else
			{
				//ASSERT(m_ptrHeadGhostQ == ptrTemp);
				// The list had only one element, so update the head as well
				m_ptrHeadGhostQ = nullptr;
			}
			//std::cout << ",";

			m_ghostMap.erase(ptrTemp->m_uid);
			// Clear the removed node's pointers
			//std::cout << ",";
			ptrTemp->m_ptrPrev = nullptr;
			//std::cout << ",";
			ptrTemp->m_ptrNext = nullptr;
			/*std::cout << m_nUsedCapacityGhostQ.load(std::memory_order_relaxed);
			std::cout << ",";
			std::cout << m_nCapacityFrequentQ.load(std::memory_order_relaxed);
			std::cout << '\n';*/
			delete ptrTemp;
			//std::cout << ",";
			ptrTemp = nullptr;
			//std::cout << ",";

		}
	}

	void removeFromGhostQ(GhostNode*& ptrItem)
	{
		// Lock the weak pointer for previous node
		auto& prev = ptrItem->m_ptrPrev;
		auto& next = ptrItem->m_ptrNext;

		// Case 1: Node is both head and tail (only element)
		if (ptrItem == m_ptrHeadGhostQ && ptrItem == m_ptrTailGhostQ)
		{
			m_ptrHeadGhostQ = nullptr;
			m_ptrTailGhostQ = nullptr;
		}
		// Case 2: Node is head
		else if (ptrItem == m_ptrHeadGhostQ)
		{
			m_ptrHeadGhostQ = next;
			if (next != nullptr)
			{
				next->m_ptrPrev = nullptr;
			}
		}
		// Case 3: Node is tail
		else if (ptrItem == m_ptrTailGhostQ)
		{
			m_ptrTailGhostQ = prev;
			if (prev != nullptr)
			{
				prev->m_ptrNext = nullptr;
			}
		}
		// Case 4: Node is in the middle
		else
		{
			if (prev != nullptr)
			{
				prev->m_ptrNext = next;
			}
			if (next != nullptr)
			{
				next->m_ptrPrev = ptrItem->m_ptrPrev;
			}
		}

		// Optionally clear the removed node's pointers.
		ptrItem->m_ptrNext = nullptr;
		ptrItem->m_ptrPrev = nullptr;

		delete ptrItem;
		ptrItem = nullptr;

#ifdef __CONCURRENT__
		m_nUsedCapacityGhostQ.fetch_sub(1, std::memory_order_relaxed);
#else //__CONCURRENT__
		m_nUsedCapacityGhostQ--;
#endif //__CONCURRENT__
	}
#endif //__MANAGE_GHOST_Q__

	inline void moveToTheFrontOfCacheQ(ObjectTypePtr& ptrItem, uint64_t& nUsedCapacityFrequentQ, uint64_t& nUsedCapacityHybridQ)
	{
		switch (ptrItem->m_nQueueType)
		{
			case QType::NONE:
			{
				ptrItem->m_nQueueType = QType::HYBRID;

				// Check for an empty list.
				if (!m_ptrHeadHybridQ)
				{
					m_ptrHeadHybridQ = ptrItem;
					m_ptrTailHybridQ = ptrItem;
					return;
				}

				// If the node is already at the front, do nothing.
				if (ptrItem == m_ptrHeadHybridQ)
				{
					return;
				}

				//// Detach ptrItem from its current position.
				//if (auto prev = ptrItem->m_ptrPrev.lock())
				//{
				//	prev->m_ptrNext = ptrItem->m_ptrNext;
				//}
				//if (ptrItem->m_ptrNext)
				//{
				//	ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
				//}
				//if (ptrItem == m_ptrTailHybridQ)
				//{
				//	m_ptrTailHybridQ = ptrItem->m_ptrPrev.lock();
				//}

				// Insert ptrItem at the front.
				ptrItem->m_ptrPrev = nullptr;  // Clear previous pointer.
				ptrItem->m_ptrNext = m_ptrHeadHybridQ;
				m_ptrHeadHybridQ->m_ptrPrev = ptrItem;  // weak_ptr can be directly assigned from a shared_ptr.
				m_ptrHeadHybridQ = ptrItem;

				break;
			}
			case QType::HYBRID:
			{
				// Lock the weak pointer for previous node
				auto prev = ptrItem->m_ptrPrev;
				auto next = ptrItem->m_ptrNext;

				// Case 1: Node is both head and tail (only element)
				if (ptrItem == m_ptrHeadHybridQ && ptrItem == m_ptrTailHybridQ)
				{
					m_ptrHeadHybridQ = nullptr;
					m_ptrTailHybridQ = nullptr;
				}
				// Case 2: Node is head
				else if (ptrItem == m_ptrHeadHybridQ)
				{
					m_ptrHeadHybridQ = next;
					if (next)
					{
						next->m_ptrPrev = nullptr;
					}
				}
				// Case 3: Node is tail
				else if (ptrItem == m_ptrTailHybridQ)
				{
					m_ptrTailHybridQ = prev;
					if (prev)
					{
						prev->m_ptrNext = nullptr;
					}
				}
				// Case 4: Node is in the middle
				else
				{
					if (prev)
					{
						prev->m_ptrNext = next;
					}
					if (next)
					{
						next->m_ptrPrev = ptrItem->m_ptrPrev;
					}
				}

				// Optionally clear the removed node's pointers.
				ptrItem->m_ptrNext = nullptr;
				ptrItem->m_ptrPrev = nullptr;

#ifdef __MANAGE_GHOST_Q__
				if (ptrItem->m_bIsDowgraded)
				{
					// Item was downgraded from FrequentQ: increase FrequentQ ratio (decrease HybridQ)
					// Ensure FrequentQ ratio doesn't exceed (capacity-1)/capacity
					double maxRatio = static_cast<double>(m_nCacheCapacity - 1) / m_nCacheCapacity;
					double adjustment = 1.0 / m_nCacheCapacity; // Adjust by 1 item worth
#ifdef __CONCURRENT__
					double currentRatio = m_nFrequentQRatio.load(std::memory_order_relaxed);
					if (currentRatio < maxRatio)
					{
						m_nFrequentQRatio.store(std::min(maxRatio, currentRatio + adjustment), std::memory_order_relaxed);
					}
#else
					if (m_nFrequentQRatio < maxRatio)
					{
						m_nFrequentQRatio = std::min(maxRatio, m_nFrequentQRatio + adjustment);
					}
#endif
					
					ptrItem->m_bIsDowgraded = false;
				}
#endif //__MANAGE_GHOST_Q__

//#ifdef __CONCURRENT__
//				m_nUsedCapacityHybridQ.fetch_sub(1, std::memory_order_relaxed);
//				m_nUsedCapacityFrequentQ.fetch_add(1, std::memory_order_relaxed);
//#else //__CONCURRENT__
//				ASSERT(m_nUsedCapacityHybridQ != 0);
//				--m_nUsedCapacityHybridQ;
//				++m_nUsedCapacityFrequentQ;
//#endif //__CONCURRENT__
				nUsedCapacityHybridQ++;
				nUsedCapacityFrequentQ++;

				ptrItem->m_nQueueType = QType::FREQUENT;
			}
			case QType::FREQUENT:
			{
				// Check for an empty list.
				if (!m_ptrHeadFrequentQ)
				{
					m_ptrHeadFrequentQ = ptrItem;
					m_ptrTailFrequentQ = ptrItem;
					return;
				}

				// If the node is already at the front, do nothing.
				if (ptrItem == m_ptrHeadFrequentQ)
				{
					return;
				}

				// Detach ptrItem from its current position.
				if (ptrItem->m_ptrPrev != nullptr)
				{
					ptrItem->m_ptrPrev->m_ptrNext = ptrItem->m_ptrNext;
				}
				if (ptrItem->m_ptrNext)
				{
					ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
				}
				if (ptrItem == m_ptrTailFrequentQ)
				{
					m_ptrTailFrequentQ = ptrItem->m_ptrPrev;
				}

				// Insert ptrItem at the front.
				ptrItem->m_ptrPrev = nullptr;  // Clear previous pointer.
				ptrItem->m_ptrNext = m_ptrHeadFrequentQ;
				m_ptrHeadFrequentQ->m_ptrPrev = ptrItem;  // weak_ptr can be directly assigned from a shared_ptr.
				m_ptrHeadFrequentQ = ptrItem;

				break;
			}
			default:
			{
				throw new std::logic_error("Critical State: ....");   // TODO: critical log.
			}
		}
	}

	inline void removeFromCacheQ(ObjectTypePtr& ptrItem)
	{
		switch (ptrItem->m_nQueueType)
		{
			case QType::FREQUENT:
			{
				// Lock the weak pointer for previous node
				auto& prev = ptrItem->m_ptrPrev;
				auto& next = ptrItem->m_ptrNext;

				// Case 1: Node is both head and tail (only element)
				if (ptrItem == m_ptrHeadFrequentQ && ptrItem == m_ptrTailFrequentQ)
				{
					m_ptrHeadFrequentQ = nullptr;
					m_ptrTailFrequentQ = nullptr;
				}
				// Case 2: Node is head
				else if (ptrItem == m_ptrHeadFrequentQ)
				{
					m_ptrHeadFrequentQ = next;
					if (next)
					{
						next->m_ptrPrev = nullptr;
					}
				}
				// Case 3: Node is tail
				else if (ptrItem == m_ptrTailFrequentQ)
				{
					m_ptrTailFrequentQ = prev;
					if (prev)
					{
						prev->m_ptrNext = nullptr;
					}
				}
				// Case 4: Node is in the middle
				else
				{
					if (prev)
					{
						prev->m_ptrNext = next;
					}
					if (next)
					{
						next->m_ptrPrev = ptrItem->m_ptrPrev;
					}
				}

				// Optionally clear the removed node's pointers.
				ptrItem->m_ptrNext = nullptr;
				ptrItem->m_ptrPrev = nullptr;

				//ptrItem->deleteCoreObject();
				//ptrItem->m_uidUpdated = std::nullopt;
				//ptrItem->m_uid = ObjectUIDType(INT64_MAX);

				//ptrItem.reset();

#ifdef __CONCURRENT__
				m_nUsedCapacityFrequentQ.fetch_sub(1, std::memory_order_relaxed);
#else //__CONCURRENT__
				--m_nUsedCapacityFrequentQ;
#endif //__CONCURRENT__

				break;
			}
			case QType::HYBRID:
			{
				// Lock the weak pointer for previous node
				auto& prev = ptrItem->m_ptrPrev;
				auto& next = ptrItem->m_ptrNext;

				// Case 1: Node is both head and tail (only element)
				if (ptrItem == m_ptrHeadHybridQ && ptrItem == m_ptrTailHybridQ)
				{
					m_ptrHeadHybridQ = nullptr;
					m_ptrTailHybridQ = nullptr;
				}
				// Case 2: Node is head
				else if (ptrItem == m_ptrHeadHybridQ)
				{
					m_ptrHeadHybridQ = next;
					if (next)
					{
						next->m_ptrPrev = nullptr;
					}
				}
				// Case 3: Node is tail
				else if (ptrItem == m_ptrTailHybridQ)
				{
					m_ptrTailHybridQ = prev;
					if (prev)
					{
						prev->m_ptrNext = nullptr;
					}
				}
				// Case 4: Node is in the middle
				else
				{
					if (prev)
					{
						prev->m_ptrNext = next;
					}
					if (next)
					{
						next->m_ptrPrev = ptrItem->m_ptrPrev;
					}
				}

				// Optionally clear the removed node's pointers.
				ptrItem->m_ptrNext = nullptr;
				ptrItem->m_ptrPrev = nullptr;

				//ptrItem->deleteCoreObject();
				//ptrItem->m_uidUpdated = std::nullopt;
				//ptrItem->m_uid = ObjectUIDType(INT64_MAX);

				//ptrItem.reset();

#ifdef __CONCURRENT__
				m_nUsedCapacityHybridQ.fetch_sub(1, std::memory_order_relaxed);
#else //__CONCURRENT__
				ASSERT(m_nUsedCapacityHybridQ != 0);

				--m_nUsedCapacityHybridQ;
#endif //__CONCURRENT__

				break;
			}
			default:
			{
				auto& prev = ptrItem->m_ptrPrev;
				auto& next = ptrItem->m_ptrNext;

				// Case 1: Node is both head and tail (only element)
				if (ptrItem == m_ptrHeadHybridQ && ptrItem == m_ptrTailHybridQ)
				{
					m_ptrHeadHybridQ = nullptr;
					m_ptrTailHybridQ = nullptr;
				}
				// Case 2: Node is head
				else if (ptrItem == m_ptrHeadHybridQ)
				{
					m_ptrHeadHybridQ = next;
					if (next)
					{
						next->m_ptrPrev = nullptr;
					}
				}
				// Case 3: Node is tail
				else if (ptrItem == m_ptrTailHybridQ)
				{
					m_ptrTailHybridQ = prev;
					if (prev)
					{
						prev->m_ptrNext = nullptr;
					}
				}
				// Case 4: Node is in the middle
				else
				{
					if (prev)
					{
						prev->m_ptrNext = next;
					}
					if (next)
					{
						next->m_ptrPrev = ptrItem->m_ptrPrev;
					}
				}

				// Optionally clear the removed node's pointers.
				ptrItem->m_ptrNext = nullptr;
				ptrItem->m_ptrPrev = nullptr;

				//ptrItem->deleteCoreObject();
				//ptrItem->m_uidUpdated = std::nullopt;
				//ptrItem->m_uid = ObjectUIDType(INT64_MAX);

				//ptrItem.reset();

#ifdef __CONCURRENT__
				m_nUsedCapacityHybridQ.fetch_sub(1, std::memory_order_relaxed);
#else //__CONCURRENT__
				ASSERT(m_nUsedCapacityHybridQ != 0);

				--m_nUsedCapacityHybridQ;
#endif //__CONCURRENT__

				break;
			}
		}
	}

#ifdef __COST_WEIGHTED_EVICTION__
	/**
	 * @brief Unlinks a node from the Hybrid Queue without deleting it
	 * @param ptrNode The node to unlink
	 */
	inline void unlinkNodeFromHybridQ(ObjectTypePtr ptrNode)
	{
		if (!ptrNode) return;

		ObjectTypePtr prev = ptrNode->m_ptrPrev;
		ObjectTypePtr next = ptrNode->m_ptrNext;

		// Case 1: Single node (both head and tail)
		if (!prev && !next)
		{
			m_ptrHeadHybridQ = nullptr;
			m_ptrTailHybridQ = nullptr;
		}
		// Case 2: Node is head
		else if (ptrNode == m_ptrHeadHybridQ)
		{
			m_ptrHeadHybridQ = next;
			if (next)
			{
				next->m_ptrPrev = nullptr;
			}
		}
		// Case 3: Node is tail
		else if (ptrNode == m_ptrTailHybridQ)
		{
			m_ptrTailHybridQ = prev;
			if (prev)
			{
				prev->m_ptrNext = nullptr;
			}
		}
		// Case 4: Node is in the middle
		else
		{
			if (prev)
			{
				prev->m_ptrNext = next;
			}
			if (next)
			{
				next->m_ptrPrev = ptrNode->m_ptrPrev;
			}
		}

		// Clear the removed node's pointers
		ptrNode->m_ptrNext = nullptr;
		ptrNode->m_ptrPrev = nullptr;
	}

	/**
	 * @brief Finds the best victim for eviction from Hybrid Queue based on cost ratio
	 * Compares the tail (LRU position) with its predecessor and evicts whichever has lower cost
	 * @return Pointer to the victim node, or nullptr if no victim can be found
	 */
	inline ObjectTypePtr findVictimByCostRatioHybridQ()
	{
		if (!m_ptrTailHybridQ) return nullptr;

		ObjectTypePtr tail = m_ptrTailHybridQ;
		ObjectTypePtr predecessor = m_ptrTailHybridQ->m_ptrPrev;

#ifdef __CONCURRENT__
		// If tail is in use, can't evict it
		if (tail->m_nUseCounter.load(std::memory_order_relaxed) > 0)
		{
			return nullptr;
		}
#endif //__CONCURRENT__

		// If no predecessor, must evict tail
		if (!predecessor)
		{
			return tail;
		}

#ifdef __CONCURRENT__
		// If predecessor is in use, evict tail
		if (predecessor->m_nUseCounter.load(std::memory_order_relaxed) > 0)
		{
			return tail;
		}
#endif //__CONCURRENT__

		uint64_t tailCost = tail->getObjectCost();
		uint64_t predCost = predecessor->getObjectCost();

		// Evict whichever has lower cost (cheaper to re-fetch)
		// If costs are equal, prefer tail (LRU default)
		if (tailCost <= predCost)
		{
			return tail;
		}
		else
		{
			return predecessor;
		}
	}
#endif //__COST_WEIGHTED_EVICTION__

	inline void flushItemsToStorage()
	{
#ifdef __CONCURRENT__
		std::vector<ObjectTypePtr> vtObjects;

		auto nCapacityHybridQ = getCapacityHybridQ();
		auto nCapacityFrequentQ = getCapacityFrequentQ();
		auto nUsedCapacityHybridQ = m_nUsedCapacityHybridQ.load(std::memory_order_relaxed);
		auto nUsedCapacityFrequentQ = m_nUsedCapacityFrequentQ.load(std::memory_order_relaxed);

		if (nUsedCapacityFrequentQ + nUsedCapacityHybridQ <= nCapacityHybridQ) return;

		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);

		//int nFlushCount = nUsedCapacityFrequentQ - nCapacityFrequentQ;
		//for (; nFlushCount > 0; nFlushCount--)
		while (m_nUsedCapacityFrequentQ.load(std::memory_order_relaxed) > getCapacityFrequentQ())
		{
			if (m_ptrTailFrequentQ->m_nUseCounter.load(std::memory_order_relaxed) != 0) break;

			ObjectTypePtr ptrItem = m_ptrTailFrequentQ;

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTailFrequentQ = m_ptrTailFrequentQ->m_ptrPrev;

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTailFrequentQ)
			{
				ASSERT(m_ptrHeadFrequentQ != nullptr);

				m_ptrTailFrequentQ->m_ptrNext = nullptr;  // Optional: Explicitly reset next pointer
			}
			else
			{
				ASSERT(m_ptrHeadFrequentQ == ptrItem);

				// The list had only one element, so update the head as well
				m_ptrHeadFrequentQ = nullptr;
			}

			// Clear the removed node's pointers
			ptrItem->m_ptrPrev = nullptr;
			ptrItem->m_ptrNext = nullptr;

			m_nUsedCapacityFrequentQ.fetch_sub(1, std::memory_order_relaxed);

			m_nUsedCapacityHybridQ.fetch_add(1, std::memory_order_relaxed);

			ptrItem->m_bIsDowgraded = true;
			ptrItem->m_nQueueType = QType::HYBRID;

			// Check for an empty list.
			if (!m_ptrHeadHybridQ)
			{
				m_ptrHeadHybridQ = ptrItem;
				m_ptrTailHybridQ = ptrItem;
				continue;
			}

			// If the node is already at the front, do nothing.
			if (ptrItem == m_ptrHeadHybridQ)
			{
				continue;
			}

	//		// Detach ptrItem from its current position.
	//		if (auto prev = ptrItem->m_ptrPrev.lock())
	//		{
	//			prev->m_ptrNext = ptrItem->m_ptrNext;
	//		}
	//		if (ptrItem->m_ptrNext)
	//		{
	//			ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
	//		}
	//		if (ptrItem == m_ptrTailHybridQ)
	//		{
	//			m_ptrTailHybridQ = ptrItem->m_ptrPrev.lock();
	//}

			// Insert ptrItem at the front.
			ptrItem->m_ptrPrev = nullptr;  // Clear previous pointer.
			ptrItem->m_ptrNext = m_ptrHeadHybridQ;
			m_ptrHeadHybridQ->m_ptrPrev = ptrItem;  // weak_ptr can be directly assigned from a shared_ptr.
			m_ptrHeadHybridQ = ptrItem;
		}

		nCapacityHybridQ = getCapacityHybridQ();
		nUsedCapacityHybridQ = m_nUsedCapacityHybridQ.load(std::memory_order_relaxed);

		int nFlushCount = nUsedCapacityHybridQ - nCapacityHybridQ;
		for (; nFlushCount > 0; nFlushCount--)
		//while (m_nUsedCapacityHybridQ.load(std::memory_order_relaxed) > getCapacityHybridQ())
		{
#ifdef __COST_WEIGHTED_EVICTION__
			// Use cost-weighted victim selection for Hybrid Queue
			ObjectTypePtr ptrItemToFlush = findVictimByCostRatioHybridQ();

			if (!ptrItemToFlush) break;  // No victim found

			if (!ptrItemToFlush->m_mtx.try_lock()) break;

			if (ptrItemToFlush->m_nUseCounter.load(std::memory_order_relaxed) != 0)
			{
				ptrItemToFlush->m_mtx.unlock();
				break;
			}

			vtObjects.push_back(ptrItemToFlush);

			// Unlink the victim (could be tail or predecessor)
			unlinkNodeFromHybridQ(ptrItemToFlush);

			m_nUsedCapacityHybridQ.fetch_sub(1, std::memory_order_relaxed);
#else
			// Traditional LRU: always evict tail
			if (m_ptrTailHybridQ == nullptr) break;

			if (m_ptrTailHybridQ->m_nUseCounter.load(std::memory_order_relaxed) != 0) break;

			if (!m_ptrTailHybridQ->m_mtx.try_lock()) break;

			if (m_ptrTailHybridQ->m_nUseCounter.load(std::memory_order_relaxed) != 0)
			{
				m_ptrTailHybridQ->m_mtx.unlock();
				break;
			}

			ObjectTypePtr ptrItemToFlush = m_ptrTailHybridQ;

			vtObjects.push_back(ptrItemToFlush);

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTailHybridQ = m_ptrTailHybridQ->m_ptrPrev;

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTailHybridQ)
			{
				ASSERT(m_ptrHeadHybridQ != nullptr);

				m_ptrTailHybridQ->m_ptrNext = nullptr;  // Optional: Explicitly reset next pointer
			}
			else
			{
				ASSERT(m_ptrHeadHybridQ == ptrItemToFlush);

				// The list had only one element, so update the head as well
				m_ptrHeadHybridQ = nullptr;
			}

			// Clear the removed node's pointers
			ptrItemToFlush->m_ptrPrev = nullptr;
			ptrItemToFlush->m_ptrNext = nullptr;

			m_nUsedCapacityHybridQ.fetch_sub(1, std::memory_order_relaxed);
#endif //__COST_WEIGHTED_EVICTION__
		}

		lock_cache.unlock();

		for (auto& obj : vtObjects)
		{
			ASSERT(obj->m_ptrCoreObject != nullptr);

			obj->m_uidUpdated = std::nullopt;
			bool is_dirty = obj->hasUpdatesToBeFlushed();
			if (is_dirty) //check for uidupdated and uid in the cache object and reset uidupdated once used!!!
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(obj, uidUpdated) != CacheErrorCode::Success)
				{
					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				obj->m_uidUpdated = uidUpdated;
			}

#ifdef __CACHE_COUNTERS__
			// Record eviction with dirty flag
			this->recordEviction(is_dirty);
#endif

			obj->m_bDirty = false;

#ifndef __MANAGE_GHOST_Q__
			obj->deleteCoreObject();

			obj->m_mtx.unlock();
			obj = nullptr;
#endif //__MANAGE_GHOST_Q__

		}

#ifdef __MANAGE_GHOST_Q__
		std::unique_lock<std::shared_mutex> lock_ghost(m_mtxGhostQ);

		// GhostQ lock already acquired at the beginning of function
		for (auto& obj : vtObjects)
		{
			if (obj->m_uidUpdated != std::nullopt) addItemToGhostQ(*obj->m_uidUpdated);
			else addItemToGhostQ(obj->m_uid);

			//addItemToGhostQ(obj->m_uid);
			obj->m_bDirty = false;
			obj->deleteCoreObject();

			obj->m_mtx.unlock();
			obj = nullptr;
		}
		addItemToGhostQ_();
#endif //__MANAGE_GHOST_Q__

		vtObjects.clear();
#else //__CONCURRENT__
		while (m_nUsedCapacityFrequentQ > getCapacityFrequentQ())
		{
			ASSERT(m_ptrTailFrequentQ != nullptr);

			ObjectTypePtr ptrItem = m_ptrTailFrequentQ;

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTailFrequentQ = m_ptrTailFrequentQ->m_ptrPrev;

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTailFrequentQ)
			{
				m_ptrTailFrequentQ->m_ptrNext = nullptr;  // Optional: Explicitly reset next pointer
			}
			else
			{
				ASSERT(m_ptrHeadFrequentQ == ptrItem);

				// The list had only one element, so update the head as well
				m_ptrHeadFrequentQ = nullptr;
			}

			// Clear the removed node's pointers
			ptrItem->m_ptrPrev = nullptr;
			ptrItem->m_ptrNext = nullptr;

			--m_nUsedCapacityFrequentQ;

			m_nUsedCapacityHybridQ++;
			ptrItem->m_bIsDowgraded = true;
			ptrItem->m_nQueueType = QType::HYBRID;

			// Check for an empty list.
			if (!m_ptrHeadHybridQ)
			{
				m_ptrHeadHybridQ = ptrItem;
				m_ptrTailHybridQ = ptrItem;
				continue;
			}

			// If the node is already at the front, do nothing.
			if (ptrItem == m_ptrHeadHybridQ)
			{
				continue;
			}

			//// Detach ptrItem from its current position.
			//if (auto prev = ptrItem->m_ptrPrev.lock())
			//{
			//	prev->m_ptrNext = ptrItem->m_ptrNext;
			//}
			//if (ptrItem->m_ptrNext)
			//{
			//	ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
			//}
			//if (ptrItem == m_ptrTailHybridQ)
			//{
			//	m_ptrTailHybridQ = ptrItem->m_ptrPrev.lock();
			//}

			// Insert ptrItem at the front.
			ptrItem->m_ptrPrev = nullptr;  // Clear previous pointer.
			ptrItem->m_ptrNext = m_ptrHeadHybridQ;
			m_ptrHeadHybridQ->m_ptrPrev = ptrItem;  // weak_ptr can be directly assigned from a shared_ptr.
			m_ptrHeadHybridQ = ptrItem;

		}

		while (m_nUsedCapacityHybridQ > getCapacityHybridQ())
		{
#ifdef __COST_WEIGHTED_EVICTION__
			// Use cost-weighted victim selection for Hybrid Queue
			ObjectTypePtr ptrTemp = findVictimByCostRatioHybridQ();

			if (!ptrTemp) break;  // No victim found

			bool is_dirty = ptrTemp->hasUpdatesToBeFlushed();
			if (is_dirty) //check for uidupdated and uid in the cache object and reset uidupdated once used!!!
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(ptrTemp, uidUpdated) != CacheErrorCode::Success)
				{
					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				ptrTemp->m_uidUpdated = uidUpdated;
			}

#ifdef __CACHE_COUNTERS__
			// Record eviction with dirty flag
			this->recordEviction(is_dirty);
#endif

			ptrTemp->m_bDirty = false;

			// Unlink the victim (could be tail or predecessor)
			unlinkNodeFromHybridQ(ptrTemp);

#ifdef __MANAGE_GHOST_Q__
			if (ptrTemp->m_uidUpdated != std::nullopt) addItemToGhostQ(*ptrTemp->m_uidUpdated);
			else addItemToGhostQ(ptrTemp->m_uid);
#endif //__MANAGE_GHOST_Q__

			// Call custom deletion logic and release the pointer
			ptrTemp->m_bDirty = false;
			ptrTemp->deleteCoreObject();
			ptrTemp->m_nQueueType = QType::NONE;
			ptrTemp = nullptr;

			ASSERT(m_nUsedCapacityHybridQ != 0);
			m_nUsedCapacityHybridQ--;
#else
			// Traditional LRU: always evict tail
			ASSERT(m_ptrTailHybridQ != nullptr);

			bool is_dirty = m_ptrTailHybridQ->hasUpdatesToBeFlushed();
			if (is_dirty) //check for uidupdated and uid in the cache object and reset uidupdated once used!!!
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(m_ptrTailHybridQ, uidUpdated) != CacheErrorCode::Success)
				{
					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				m_ptrTailHybridQ->m_uidUpdated = uidUpdated;
			}

#ifdef __CACHE_COUNTERS__
			// Record eviction with dirty flag
			this->recordEviction(is_dirty);
#endif

			m_ptrTailHybridQ->m_bDirty = false;
			ObjectTypePtr ptrTemp = m_ptrTailHybridQ;

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTailHybridQ = m_ptrTailHybridQ->m_ptrPrev;

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTailHybridQ)
			{
				m_ptrTailHybridQ->m_ptrNext = nullptr;  // Optional: Explicitly reset next pointer
			}
			else
			{
				ASSERT(m_ptrHeadHybridQ == ptrTemp);

				// The list had only one element, so update the head as well
				m_ptrHeadHybridQ = nullptr;
			}

			// Clear the removed node's pointers
			ptrTemp->m_ptrPrev = nullptr;
			ptrTemp->m_ptrNext = nullptr;

#ifdef __MANAGE_GHOST_Q__
			if (ptrTemp->m_uidUpdated != std::nullopt) addItemToGhostQ(*ptrTemp->m_uidUpdated);
			else addItemToGhostQ(ptrTemp->m_uid);
#endif //__MANAGE_GHOST_Q__

			// Call custom deletion logic and release the pointer
			ptrTemp->m_bDirty = false;
			ptrTemp->deleteCoreObject();
			ptrTemp->m_nQueueType = QType::NONE;
			ptrTemp = nullptr;

			ASSERT(m_nUsedCapacityHybridQ != 0);
			m_nUsedCapacityHybridQ--;
#endif //__COST_WEIGHTED_EVICTION__
		}
#endif //__CONCURRENT__
	}

	inline void flushAllItemsToStorage()
	{
#ifdef __CONCURRENT__
		std::vector<ObjectTypePtr> vtObjects;

		auto nUsedCapacityFrequentQ = m_nUsedCapacityFrequentQ.load(std::memory_order_relaxed);

		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);

#ifdef __MANAGE_GHOST_Q__
		std::unique_lock<std::shared_mutex> lock_ghost(m_mtxGhostQ);
#endif //__MANAGE_GHOST_Q__

		while (nUsedCapacityFrequentQ > 0)
		{
			if (m_ptrTailFrequentQ->m_nUseCounter.load(std::memory_order_relaxed) != 0) break;

			ObjectTypePtr ptrItem = m_ptrTailFrequentQ;

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTailFrequentQ = m_ptrTailFrequentQ->m_ptrPrev;

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTailFrequentQ)
			{
				ASSERT(m_ptrHeadFrequentQ != nullptr);

				m_ptrTailFrequentQ->m_ptrNext = nullptr;  // Optional: Explicitly reset next pointer
			}
			else
			{
				ASSERT(m_ptrHeadFrequentQ == ptrItem);

				// The list had only one element, so update the head as well
				m_ptrHeadFrequentQ = nullptr;
			}

			// Clear the removed node's pointers
			ptrItem->m_ptrPrev = nullptr;
			ptrItem->m_ptrNext = nullptr;

			nUsedCapacityFrequentQ--;
			m_nUsedCapacityFrequentQ.fetch_sub(1, std::memory_order_relaxed);

			m_nUsedCapacityHybridQ.fetch_add(1, std::memory_order_relaxed);

			ptrItem->m_bIsDowgraded = true;
			ptrItem->m_nQueueType = QType::HYBRID;

			// Check for an empty list.
			if (!m_ptrHeadHybridQ)
			{
				m_ptrHeadHybridQ = ptrItem;
				m_ptrTailHybridQ = ptrItem;
				continue;
			}

			// If the node is already at the front, do nothing.
			if (ptrItem == m_ptrHeadHybridQ)
			{
				continue;
			}

			//		// Detach ptrItem from its current position.
			//		if (auto prev = ptrItem->m_ptrPrev.lock())
			//		{
			//			prev->m_ptrNext = ptrItem->m_ptrNext;
			//		}
			//		if (ptrItem->m_ptrNext)
			//		{
			//			ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
			//		}
			//		if (ptrItem == m_ptrTailHybridQ)
			//		{
			//			m_ptrTailHybridQ = ptrItem->m_ptrPrev.lock();
			//}

					// Insert ptrItem at the front.
			ptrItem->m_ptrPrev = nullptr;  // Clear previous pointer.
			ptrItem->m_ptrNext = m_ptrHeadHybridQ;
			m_ptrHeadHybridQ->m_ptrPrev = ptrItem;  // weak_ptr can be directly assigned from a shared_ptr.
			m_ptrHeadHybridQ = ptrItem;
		}

		ASSERT(nUsedCapacityFrequentQ == 0);
		ASSERT(m_ptrTailFrequentQ == nullptr);

		auto nUsedCapacityHybridQ = m_nUsedCapacityHybridQ.load(std::memory_order_relaxed);

		while (nUsedCapacityHybridQ > 0)
		{
			if (m_ptrTailHybridQ == nullptr) break;

			if (m_ptrTailHybridQ->m_nUseCounter.load(std::memory_order_relaxed) != 0) break;

			if (!m_ptrTailHybridQ->m_mtx.try_lock()) break;

			if (m_ptrTailHybridQ->m_nUseCounter.load(std::memory_order_relaxed) != 0)
			{
				m_ptrTailHybridQ->m_mtx.unlock();
				break;
			}

			bool is_dirty = m_ptrTailHybridQ->hasUpdatesToBeFlushed();
			if (is_dirty) //check for uidupdated and uid in the cache object and reset uidupdated once used!!!
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(m_ptrTailHybridQ, uidUpdated) != CacheErrorCode::Success)
				{
					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				m_ptrTailHybridQ->m_uidUpdated = uidUpdated;
			}

#ifdef __CACHE_COUNTERS__
			// Record eviction with dirty flag
			this->recordEviction(is_dirty);
#endif

			m_ptrTailHybridQ->m_bDirty = false;
			ObjectTypePtr ptrTemp = m_ptrTailHybridQ;

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTailHybridQ = m_ptrTailHybridQ->m_ptrPrev;

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTailHybridQ)
			{
				m_ptrTailHybridQ->m_ptrNext = nullptr;  // Optional: Explicitly reset next pointer
			}
			else
			{
				ASSERT(m_ptrHeadHybridQ == ptrTemp);

				// The list had only one element, so update the head as well
				m_ptrHeadHybridQ = nullptr;
			}

			// Clear the removed node's pointers
			ptrTemp->m_ptrPrev = nullptr;
			ptrTemp->m_ptrNext = nullptr;

#ifdef __MANAGE_GHOST_Q__
			if (ptrTemp->m_uidUpdated != std::nullopt) addItemToGhostQ(*ptrTemp->m_uidUpdated);
			else addItemToGhostQ(ptrTemp->m_uid);
#endif //__MANAGE_GHOST_Q__

			// Call custom deletion logic and release the pointer
			ptrTemp->m_bDirty = false;
			ptrTemp->deleteCoreObject();
			ptrTemp->m_nQueueType = QType::NONE;
			ptrTemp = nullptr;

			ASSERT(m_nUsedCapacityHybridQ != 0);

			nUsedCapacityHybridQ--;
			m_nUsedCapacityHybridQ.fetch_sub(1, std::memory_order_relaxed);
		}

		ASSERT(nUsedCapacityHybridQ == 0);
		ASSERT(m_ptrTailHybridQ == nullptr);

#else //__CONCURRENT__
		while (m_nUsedCapacityFrequentQ > 0)
		{
			ASSERT(m_ptrTailFrequentQ != nullptr);

			ObjectTypePtr ptrItem = m_ptrTailFrequentQ;

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTailFrequentQ = m_ptrTailFrequentQ->m_ptrPrev;

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTailFrequentQ)
			{
				m_ptrTailFrequentQ->m_ptrNext = nullptr;  // Optional: Explicitly reset next pointer
			}
			else
			{
				ASSERT(m_ptrHeadFrequentQ == ptrItem);

				// The list had only one element, so update the head as well
				m_ptrHeadFrequentQ = nullptr;
			}

			// Clear the removed node's pointers
			ptrItem->m_ptrPrev = nullptr;
			ptrItem->m_ptrNext = nullptr;

			--m_nUsedCapacityFrequentQ;

			m_nUsedCapacityHybridQ++;
			ptrItem->m_bIsDowgraded = true;
			ptrItem->m_nQueueType = QType::HYBRID;

			// Check for an empty list.
			if (!m_ptrHeadHybridQ)
			{
				m_ptrHeadHybridQ = ptrItem;
				m_ptrTailHybridQ = ptrItem;
				continue;
			}

			// If the node is already at the front, do nothing.
			if (ptrItem == m_ptrHeadHybridQ)
			{
				continue;
			}

			//// Detach ptrItem from its current position.
			//if (auto prev = ptrItem->m_ptrPrev.lock())
			//{
			//	prev->m_ptrNext = ptrItem->m_ptrNext;
			//}
			//if (ptrItem->m_ptrNext)
			//{
			//	ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
			//}
			//if (ptrItem == m_ptrTailHybridQ)
			//{
			//	m_ptrTailHybridQ = ptrItem->m_ptrPrev.lock();
			//}

			// Insert ptrItem at the front.
			ptrItem->m_ptrPrev = nullptr;  // Clear previous pointer.
			ptrItem->m_ptrNext = m_ptrHeadHybridQ;
			m_ptrHeadHybridQ->m_ptrPrev = ptrItem;  // weak_ptr can be directly assigned from a shared_ptr.
			m_ptrHeadHybridQ = ptrItem;

		}

		ASSERT(m_nUsedCapacityFrequentQ == 0);
		ASSERT(m_ptrTailFrequentQ == nullptr);

		while (m_nUsedCapacityHybridQ > 0)
		{
			ASSERT(m_ptrTailHybridQ != nullptr);

			bool is_dirty = m_ptrTailHybridQ->hasUpdatesToBeFlushed();
			if (is_dirty) //check for uidupdated and uid in the cache object and reset uidupdated once used!!!
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(m_ptrTailHybridQ, uidUpdated) != CacheErrorCode::Success)
				{
					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				m_ptrTailHybridQ->m_uidUpdated = uidUpdated;
			}

#ifdef __CACHE_COUNTERS__
			// Record eviction with dirty flag
			this->recordEviction(is_dirty);
#endif

			m_ptrTailHybridQ->m_bDirty = false;
			ObjectTypePtr ptrTemp = m_ptrTailHybridQ;

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTailHybridQ = m_ptrTailHybridQ->m_ptrPrev;

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTailHybridQ)
			{
				m_ptrTailHybridQ->m_ptrNext = nullptr;  // Optional: Explicitly reset next pointer
			}
			else
			{
				ASSERT(m_ptrHeadHybridQ == ptrTemp);

				// The list had only one element, so update the head as well
				m_ptrHeadHybridQ = nullptr;
			}

			// Clear the removed node's pointers
			ptrTemp->m_ptrPrev = nullptr;
			ptrTemp->m_ptrNext = nullptr;

#ifdef __MANAGE_GHOST_Q__
			if (ptrTemp->m_uidUpdated != std::nullopt) addItemToGhostQ(*ptrTemp->m_uidUpdated);
			else addItemToGhostQ(ptrTemp->m_uid);
#endif //__MANAGE_GHOST_Q__

			// Call custom deletion logic and release the pointer
			ptrTemp->m_bDirty = false;
			ptrTemp->deleteCoreObject();
			ptrTemp->m_nQueueType = QType::NONE;
			ptrTemp = nullptr;

			ASSERT(m_nUsedCapacityHybridQ != 0);
			m_nUsedCapacityHybridQ--;
		}

		ASSERT(m_nUsedCapacityHybridQ == 0);
		ASSERT(m_ptrTailHybridQ == nullptr);
#endif //__CONCURRENT__
	}

public:
	inline void persistAllItems()
	{
//#ifdef __CONCURRENT__
//		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
//#endif //__CONCURRENT__
//
//		ObjectTypePtr ptrItemToFlush = m_ptrTailHybridQ;
//
//		while (ptrItemToFlush != nullptr)
//		{
//			//if (m_ptrTailHybridQ.use_count() > 3)
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
//			if (!ptrItemToFlush->m_mtx.try_lock())
//			{
//				throw new std::logic_error("The object is still in use");
//			}
//#endif //__CONCURRENT__
//
//			ptrItemToFlush->m_uidUpdated = std::nullopt;
//			if (ptrItemToFlush->hasUpdatesToBeFlushed())
//			{
//				ObjectUIDType uidUpdated;
//				if (m_ptrStorage->addObject(ptrItemToFlush->m_uid, ptrItemToFlush, uidUpdated) != CacheErrorCode::Success)
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
//			ptrItemToFlush = ptrItemToFlush->m_ptrPrev.lock();
//			/*
//			ObjectTypePtr ptrItemToFlush = m_ptrTailHybridQ;
//
//			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
//			m_ptrTailHybridQ = m_ptrTailHybridQ->m_ptrPrev.lock();
//
//			// If the list still has elements, update the new tail's next pointer
//			if (m_ptrTailHybridQ)
//			{
//				m_ptrTailHybridQ->m_ptrNext.reset();  // Optional: Explicitly reset next pointer
//			}
//			else
//			{
//				// The list had only one element, so update the head as well
//				m_ptrHeadHybridQ.reset();
//			}
//
//			// Clear the removed node's pointers
//			ptrItemToFlush->m_ptrPrev.reset();
//			ptrItemToFlush->m_ptrNext.reset();			
//
//#ifdef __MANAGE_GHOST_Q__
//			addItemToGhostQ(ptrItemToFlush->m_uid);
//#endif //__MANAGE_GHOST_Q__
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
//			m_nUsedCapacityHybridQ--;
//			*/
//		}
//
//		ptrItemToFlush = m_ptrTailFrequentQ;
//
//		while (ptrItemToFlush != nullptr)
//		{
//#ifdef __CONCURRENT__
//			// Check if the object is in use
//			if (!ptrItemToFlush->m_mtx.try_lock())
//			{
//				throw new std::logic_error("The object is still in use");
//			}
//#endif //__CONCURRENT__
//
//			ptrItemToFlush->m_uidUpdated = std::nullopt;
//			if (ptrItemToFlush->hasUpdatesToBeFlushed())
//			{
//				ObjectUIDType uidUpdated;
//				if (m_ptrStorage->addObject(ptrItemToFlush->m_uid, ptrItemToFlush, uidUpdated) != CacheErrorCode::Success)
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
//
//		}	
	}

#ifdef __CONCURRENT__
	static void handlerCacheFlush(SelfType* ptrSelf)
	{
		do
		{
			ptrSelf->flushItemsToStorage();

			std::this_thread::sleep_for(1ms);

		} while (!ptrSelf->m_bStop);

#ifdef __CACHE_COUNTERS__
		// Append thread-local stats from background thread before thread exits
		// Access the background thread's thread-local data through the CacheStatsProvider interface
		const auto* statsProvider = static_cast<const CacheStatsProvider<A2QCache<Traits>>*>(ptrSelf);
		
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