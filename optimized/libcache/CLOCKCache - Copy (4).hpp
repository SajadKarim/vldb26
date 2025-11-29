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
#include "CacheErrorCodes.h"
#include <optional>
#include "PMemWAL.hpp"
#include "validityasserts.h"
#include <queue>

#define FLUSH_COUNT 100
#define MIN_CACHE_FOOTPRINT 1024 * 1024	// Safe check!

using namespace std::chrono_literals;

template <typename Traits>
class CLOCKCache
{
	typedef CLOCKCache<Traits> SelfType;

public:
	using KeyType = typename Traits::KeyType;
	using ValueType = typename Traits::ValueType;

	using ObjectType = typename Traits::ObjectType;
	using ObjectTypePtr = typename Traits::ObjectType*;
	using ObjectUIDType = typename Traits::ObjectUIDType;

	using WALType = typename Traits::WALType;
	using StorageType = typename Traits::StorageType;

protected:
	ObjectTypePtr m_ptrHead;
	ObjectTypePtr m_ptrTail;

	size_t m_nClockHand;
	std::vector<int> m_vtClockBufferWeight;
	std::vector<ObjectTypePtr> m_vtClockBuffer;
	std::vector<std::pair<ObjectTypePtr, int>> m_vtClockQ;

	StorageType* m_ptrStorage;

	uint64_t m_nCacheCapacity;

#ifdef __CONCURRENT__
	std::atomic<uint64_t> m_nCacheMiss;
	std::atomic<uint64_t> m_nUsedCacheCapacity;
#else //__CONCURRENT__
	uint64_t m_nCacheMiss;
	uint64_t m_nUsedCacheCapacity;
#endif //__CONCURRENT__

	int m_pendingclock;

	WALType* m_ptrWAL;

	std::condition_variable cv;

#ifdef __CONCURRENT__
	bool m_bStop;

	std::thread m_threadCacheFlush;

	mutable std::mutex m_mtxCache;
#endif //__CONCURRENT__

public:
	~CLOCKCache()
	{
#ifdef __ENABLE_ASSERTS__
		std::cout << "Total Cache Misses: " << m_nCacheMiss << std::endl;
#endif //__ENABLE_ASSERTS__

#ifdef __CONCURRENT__
		{
			std::unique_lock<std::mutex> lock_cache(m_mtxCache);
			cv.wait(lock_cache, [&] { return m_vtClockQ.size() == 0; });
		}

		m_bStop = true;
		m_threadCacheFlush.join();
#endif //__CONCURRENT__

		flush(false);

		m_ptrHead == nullptr;
		m_ptrTail == nullptr;

		delete m_ptrStorage;
		m_ptrStorage = nullptr;
	}

	template <typename... StorageArgs>
	CLOCKCache(size_t nCapacity, StorageArgs... args)
		: m_nCacheMiss(0)
		, m_ptrHead(nullptr)
		, m_ptrTail(nullptr)
		, m_nUsedCacheCapacity(0)
		, m_nCacheCapacity(nCapacity)
	{
		m_ptrStorage = new StorageType(args...);

		//m_ptrWAL = new WALType(this, WAL_FILE_PATH);

#ifdef __CONCURRENT__
		m_bStop = false;
		m_threadCacheFlush = std::thread(handlerCacheFlush, this);
#endif //__CONCURRENT__

		m_vtClockQ.reserve(m_nCacheCapacity);
		m_pendingclock = 0;
		m_nClockHand = 0;
		m_vtClockBuffer.resize(nCapacity);
		m_vtClockBufferWeight.resize(nCapacity);
		for (int i = 0 ; i < nCapacity ; ++i)
		{
			m_vtClockBuffer[i] = nullptr;
			m_vtClockBufferWeight[i] = -1;
		}
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

	CacheErrorCode updateObjectsAccessMetadata(int nDepth, std::vector<ObjectTypePtr>& vtObjects)
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		for (auto& obj : vtObjects)
		{
			if (obj == nullptr && obj->m_ptrCoreObject == nullptr) continue;

			if (obj->m_nPositionInCLOCK == -1)
			{
				auto nIdx = evitItemFromCache();
				ASSERT(nIdx == m_nClockHand);

				ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);
				ASSERT(!m_vtClockBuffer[m_nClockHand]);

				m_vtClockBuffer[m_nClockHand] = obj;
				m_vtClockBufferWeight[m_nClockHand] = nDepth;

				obj->m_nPositionInCLOCK = m_nClockHand;
			}

			ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
			obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);

			m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = nDepth--;
		}

		vtObjects.clear();

		return CacheErrorCode::Success;

		
		//#ifdef __CONCURRENT__
//		bool bUpdatesOnly = true;
//		std::vector<std::pair<ObjectTypePtr, int>> vtBuffer; 
//		vtBuffer.reserve(vtObjects.size());
//
//		int y = -1;
//		for (auto obj : vtObjects)
//		//int lvl = 1;
//		//for(auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
//		{
//			y++;
//			//auto& obj = *it;
//			if (obj == nullptr)
//			{
//				continue;
//			}
//
//			if (obj->del)
//			{
//				//ASSERT(vt[y] == 0);
//				if (obj->m_nPositionInCLOCK == -1)
//				{
//					//ASSERT(obj.use_count() == 1);
//				}
//				else
//				{
//					//ASSERT(obj.use_count() == 2);
//				}
//
//				//ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
//				//obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
//
//
//				bUpdatesOnly = false;
//				vtBuffer.push_back({obj, 0 });
//				continue;
//			}
//
//			ASSERT(obj->m_ptrCoreObject); //failed.. might be object has been moved while it has bene in the queue.. but how.. I do have check for ref_count!!
//
//
//			if (obj->m_nPositionInCLOCK == -1)
//			{
//				ASSERT(obj->m_ptrCoreObject);
//
//				//obj->m_nPositionInCLOCK = INT_MAX;
//				bUpdatesOnly = false;
//			}
//			else
//			{
//				ASSERT(y <= vtObjects.size());
//			}
//
//			ASSERT(y <= vtObjects.size());
//			//ASSERT((level - vt[y]) > 0);
//			vtBuffer.push_back({ obj, nDepth});
//
//			if (obj != nullptr)
//				nDepth--;
//
//		}
//
//		if (bUpdatesOnly)
//		{
//			for (auto obj : vtObjects) 
//			{
//				if (obj == nullptr || obj->del) continue;
//				ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
//				obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
//			}
//			return CacheErrorCode::Success;
//		}
//
//		{
//			std::unique_lock<std::mutex> lock_cache(m_mtxCache);
//
//			cv.wait(lock_cache, [&] { return m_vtClockQ.size() + vtBuffer.size() < (m_nCacheCapacity/2 - 10); });
//
//			m_vtClockQ.insert(m_vtClockQ.end(), vtBuffer.begin(), vtBuffer.end());
//		}
//
//		vtObjects.clear();
//#else //__CONCURRENT__
//		for (auto it = vtObjects.begin(); it != vtObjects.end(); it++)
//		{
//			auto& obj = *it;
//
//			if (obj == nullptr)
//			{
//				continue;
//			}
//
//			if (obj->del)
//			{
//				auto idx = obj->m_nPositionInCLOCK;
//				if (idx != -1/* && idx != INT_MAX*/) //might be case where item had to bee added tot the queue but got deleted!!
//				{
//					ASSERT(m_vtClockBuffer[idx]);
//					ASSERT(m_vtClockBufferWeight[idx] != -1);
//
//
//					m_vtClockBufferWeight[idx] = -1;
//					obj->m_nPositionInCLOCK = -1;
//					//___obj->reftofirstiteminpair = nullptr;
//
//					//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//
//					obj = nullptr;
//					//m_vtClockBuffer[idx] = nullptr;
//					m_vtClockBuffer[idx] = nullptr;
//
//					//break;
//				}
//
//				continue;
//			}
//
//
//			if (obj->m_ptrCoreObject == nullptr)
//			{
//				ASSERT(false);
//				//	continue;
//			}
//
//			if (obj->m_nPositionInCLOCK == -1)
//			{
//				continue;
//				//int idx = m_vtClockBufferWeight[m_nClockHand];
//				//if (idx != -1)
//				//{
//				//	ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);
//				//	ASSERT(m_vtClockBuffer[m_nClockHand]);
//				//	flushItemsToStorage();
//				//}
//
//				//idx = m_nClockHand;
//
//				//ASSERT(m_vtClockBufferWeight[idx] == -1);
//				//ASSERT(!m_vtClockBuffer[idx]);
//
//				//m_vtClockBuffer[idx] = *it;	// use atomics!!!!
//				//m_vtClockBufferWeight[idx] = nDepth - obj->m_nFlushPriority;	// use atomics!!!!
//				//obj->m_nPositionInCLOCK = idx;
//				////obj->reftofirstiteminpair = &m_vtClockBufferWeight[idx];
//				//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//			}
//
//			m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = nDepth - obj->m_nFlushPriority;
//		}
//
//		for (auto it = vtObjects.begin(); it != vtObjects.end(); it++)
//		{
//			auto& obj = *it;
//
//			if (obj == nullptr || obj->m_ptrCoreObject == nullptr)
//			{
//				continue;
//			}
//
//			if (obj->m_nPositionInCLOCK != -1)
//				continue;
//
//			//if (obj->del)
//			//{
//			//	auto idx = obj->m_nPositionInCLOCK;
//			//	if (idx != -1/* && idx != INT_MAX*/) //might be case where item had to bee added tot the queue but got deleted!!
//			//	{
//			//		ASSERT(m_vtClockBuffer[idx]);
//			//		ASSERT(m_vtClockBufferWeight[idx] != -1);
//
//
//			//		m_vtClockBufferWeight[idx] = -1;
//			//		obj->m_nPositionInCLOCK = -1;
//			//		//___obj->reftofirstiteminpair = nullptr;
//
//			//		//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//
//			//		obj = nullptr;
//			//		//m_vtClockBuffer[idx] = nullptr;
//			//		m_vtClockBuffer[idx] = nullptr;
//
//			//		//break;
//			//	}
//
//			//	continue;
//			//}
//
//
//			//if (obj->m_ptrCoreObject == nullptr)
//			//{
//			//	ASSERT(false);
//			////	continue;
//			//}
//
//			//if (obj->m_nPositionInCLOCK == -1)
//			{
//				int idx = m_vtClockBufferWeight[m_nClockHand];
//				if (idx != -1)
//				{
//					ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);
//					ASSERT(m_vtClockBuffer[m_nClockHand]);
//					flushItemsToStorage();
//				}
//
//				idx = m_nClockHand;
//
//				ASSERT(m_vtClockBufferWeight[idx] == -1);
//				ASSERT(!m_vtClockBuffer[idx]);
//
//				m_vtClockBuffer[idx] = *it;	// use atomics!!!!
//				m_vtClockBufferWeight[idx] = nDepth - obj->m_nFlushPriority;	// use atomics!!!!
//				obj->m_nPositionInCLOCK = idx;
//				//obj->reftofirstiteminpair = &m_vtClockBufferWeight[idx];
//				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//			}
//
//			m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = nDepth - obj->m_nFlushPriority;
//		}
//
//		//for (auto it = vtObjects.begin(); it != vtObjects.end(); it++)
//		//{
//		//	auto obj = *it;
//		//	if (obj == nullptr || obj->m_nPositionInCLOCK == -1 || obj->m_ptrCoreObject == nullptr)
//		//	{
//		//		continue;
//		//	}
//
//		//	m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = level - obj->m_nFlushPriority;
//		//	//*obj->reftofirstiteminpair = level - obj->m_nFlushPriority;	// use atomics!!!!	// why are you updating it everytime?
//		//}
//
//		vtObjects.clear();
//#endif //__CONCURRENT__
//
//		return CacheErrorCode::Success;
	}

	CacheErrorCode updateObjectsAccessMetadata(int nDepth, std::vector<std::pair<ObjectTypePtr, ObjectTypePtr>>& vtObjects)		
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		for (auto& it : vtObjects)
		{
			for (int j = 0; j < 2; ++j)
			{
				auto& obj = j == 0 ? it.first : it.second;

				if (obj == nullptr || obj->m_ptrCoreObject == nullptr) continue;

				if (obj->m_nPositionInCLOCK == -1)
				{
					auto nIdx = evitItemFromCache();
					ASSERT(nIdx == m_nClockHand);

					ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);
					ASSERT(!m_vtClockBuffer[m_nClockHand]);

					m_vtClockBuffer[m_nClockHand] = obj;
					m_vtClockBufferWeight[m_nClockHand] = nDepth;

					obj->m_nPositionInCLOCK = m_nClockHand;
				}

				m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = nDepth;

				ASSERT(obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
				obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
			}

			if (it.first != nullptr || it.second != nullptr) nDepth--;
		}

		vtObjects.clear();

		return CacheErrorCode::Success;
//#else //__CONCURRENT__
//		for (auto it = vtObjects.begin(); it != vtObjects.end(); it++)
//		{
//			for (int j = 0; j < 2; ++j)
//			{
//				auto& obj = j == 0 ? (*it).first : (*it).second;
//
//				//if (obj== nullptr || obj->m_ptrCoreObject == nullptr)
//				//{
//				//	continue;
//				//}
//
//				//auto& obj = *it;
//
//				if (obj == nullptr)
//				{
//					continue;
//				}
//
//				if (obj->del)
//				{
//					auto idx = obj->m_nPositionInCLOCK;
//					if (idx != -1/* && idx != INT_MAX*/) //might be case where item had to bee added tot the queue but got deleted!!
//					{
//						ASSERT(m_vtClockBuffer[idx]);
//						ASSERT(m_vtClockBufferWeight[idx] != -1);
//
//
//						m_vtClockBufferWeight[idx] = -1;
//						obj->m_nPositionInCLOCK = -1;
//						//___obj->reftofirstiteminpair = nullptr;
//
//						//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//
//						obj = nullptr;
//						//m_vtClockBuffer[idx] = nullptr;
//						m_vtClockBuffer[idx] = nullptr;
//
//						//break;
//					}
//
//					continue;
//				}
//
//
//				if (obj->m_ptrCoreObject == nullptr)
//				{
//					ASSERT(false);
//					//	continue;
//				}
//
//				if (obj->m_nPositionInCLOCK == -1)
//				{
//					continue;
//				//	int idx = m_vtClockBufferWeight[m_nClockHand];
//				//	if (idx != -1)
//				//	{
//				//		ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);
//				//		ASSERT(m_vtClockBuffer[m_nClockHand]);
//				//		flushItemsToStorage();
//				//	}
//
//				//	idx = m_nClockHand;
//
//				//	ASSERT(m_vtClockBufferWeight[idx] == -1);
//				//	ASSERT(!m_vtClockBuffer[idx]);
//
//				//	m_vtClockBuffer[idx] = obj;	// use atomics!!!!
//				//	m_vtClockBufferWeight[idx] = nDepth - obj->m_nFlushPriority;	// use atomics!!!!
//				//	obj->m_nPositionInCLOCK = idx;
//				//	//obj->reftofirstiteminpair = &m_vtClockBufferWeight[idx];
//				//	m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//				}
//
//				m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = nDepth - obj->m_nFlushPriority;
//			}
//		}
//
//		for (auto it = vtObjects.begin(); it != vtObjects.end(); it++)
//		{
//			for (int j = 0; j < 2; ++j)
//			{
//				auto& obj = j == 0 ? (*it).first : (*it).second;
//
//				if (obj == nullptr || obj->m_ptrCoreObject == nullptr)
//				{
//					continue;
//				}
//
//				if (obj->m_nPositionInCLOCK != -1)
//					continue;
//
//				int idx = m_vtClockBufferWeight[m_nClockHand];
//				if (idx != -1)
//				{
//					ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);
//					ASSERT(m_vtClockBuffer[m_nClockHand]);
//					flushItemsToStorage();
//				}
//
//				idx = m_nClockHand;
//
//				ASSERT(m_vtClockBufferWeight[idx] == -1);
//				ASSERT(!m_vtClockBuffer[idx]);
//
//				m_vtClockBuffer[idx] = obj;	// use atomics!!!!
//				m_vtClockBufferWeight[idx] = nDepth - obj->m_nFlushPriority;	// use atomics!!!!
//				obj->m_nPositionInCLOCK = idx;
//				//obj->reftofirstiteminpair = &m_vtClockBufferWeight[idx];
//				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//
//				m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = nDepth - obj->m_nFlushPriority;
//			}
//		}
//		
//		//for (auto it = vtObjects.begin(); it != vtObjects.end(); it++)
//		//{
//		//	auto obj = *it;
//		//	if (obj == nullptr || obj->m_nPositionInCLOCK == -1 || obj->m_ptrCoreObject == nullptr)
//		//	{
//		//		continue;
//		//	}
//
//		//	m_vtClockBufferWeight[obj->m_nPositionInCLOCK] = level - obj->m_nFlushPriority;
//		//	//*obj->reftofirstiteminpair = level - obj->m_nFlushPriority;	// use atomics!!!!	// why are you updating it everytime?
//		//}
//
//		vtObjects.clear();
//#endif //__CONCURRENT__
	}

	CacheErrorCode remove(ObjectTypePtr& ptrObject)
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		if (ptrObject->m_uid.getMediaType() > 1)
		{
			m_ptrStorage->remove(ptrObject->m_uid);
		}

		//removeFromLRU(ptrObject);

		if (ptrObject->m_nPositionInCLOCK != -1)
		{
			ASSERT(m_vtClockBuffer[ptrObject->m_nPositionInCLOCK]);

			m_vtClockBuffer[ptrObject->m_nPositionInCLOCK] = nullptr;
			m_vtClockBufferWeight[ptrObject->m_nPositionInCLOCK] = -1;
		}

		delete ptrObject;
		ptrObject = nullptr;

		return CacheErrorCode::Success;
//#else //__CONCURRENT__
//
//		if (ptrObject->m_uid.getMediaType() > 1)
//		{
//			m_ptrStorage->remove(ptrObject->m_uid);
//		}
//
//		ptrObject->deleteCoreObject();
//		//ptrObject->m_uidUpdated = std::nullopt;
//		//ptrObject->m_uid = ObjectUIDType(INT64_MAX);
//
//		ptrObject->del = true;
//		ptrObject->m_bDirty = false;
//
//		//auto idx = ptrObject->m_nPositionInCLOCK;
//		////if (idx != -1 && idx != INT_MAX)
//		//if (idx != -1)
//		//{
//		//	ASSERT(m_vtClockBuffer[idx]);
//		//	m_vtClockBuffer[idx].reset();
//		//	m_vtClockBuffer[idx] = nullptr;
//		//	m_vtClockBufferWeight[idx] = -1;
//		//	
//		//}
//
//		////ptrObject->reftofirstiteminpair = nullptr;
//		////ASSERT(!ptrObject->reftofirstiteminpair);
//		////if (ptrObject->reftofirstiteminpair != nullptr)
//		////	*ptrObject->reftofirstiteminpair = -1;
//
//		////ptrObject->m_nFlushPriority = -1;	// IMPORTANT: Dependent field do not remove it!!!!
//		//ptrObject->m_nPositionInCLOCK = -1;
//
//		//ptrObject.reset();
//
//		m_nUsedCacheCapacity--;
//#endif //__CONCURRENT__

		//ptrObject->m_nFlushPriority = -1;
		//todo
		// deffered deletetion in the clock to the batch processing for cache locality!!

		return CacheErrorCode::Success;
	}

	CacheErrorCode getCoreObject(uint16_t nDegree, const ObjectUIDType& uidObject, ObjectTypePtr& ptrObject)
	{
		m_ptrStorage->getObject(nDegree, uidObject, ptrObject);

		ASSERT(ptrObject->m_ptrCoreObject != nullptr && "The requested object does not exist.");

#ifdef __CONCURRENT__
#ifdef __ENABLE_ASSERTS__
		m_nCacheMiss.fetch_add(1, std::memory_order_relaxed);
#endif //__ENABLE_ASSERTS__
		m_nUsedCacheCapacity.fetch_add(1, std::memory_order_relaxed);
#else //__CONCURRENT__
#ifdef __ENABLE_ASSERTS__
		m_nCacheMiss++;
#endif //__ENABLE_ASSERTS__
		m_nUsedCacheCapacity++;
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}

	CacheErrorCode getObject(uint16_t nDegree, const ObjectUIDType& uidObject, ObjectTypePtr& ptrObject)
	{
		ptrObject = m_ptrStorage->getObject(nDegree, uidObject);

		ASSERT(ptrObject == nullptr || "The requested object does not exist.");

#ifdef __CONCURRENT__
#ifdef __ENABLE_ASSERTS__
		m_nCacheMiss.fetch_add(1, std::memory_order_relaxed);
#endif //__ENABLE_ASSERTS__
		m_nUsedCacheCapacity.fetch_add(1, std::memory_order_relaxed);
#else //__CONCURRENT__
#ifdef __ENABLE_ASSERTS__
		m_nCacheMiss++;
#endif //__ENABLE_ASSERTS__
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

	CacheErrorCode flush(bool bStopFlushThread = true)
	{
		if (bStopFlushThread)
		{
#ifdef __CONCURRENT__
			m_bStop = true;
			m_threadCacheFlush.join();
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

	inline size_t evitItemFromCache()
	{
		while (true)
		{
			if (m_vtClockBufferWeight[m_nClockHand] == -1)
			{
				ASSERT(!m_vtClockBuffer[m_nClockHand]);
				return m_nClockHand;
			}

			if (m_vtClockBuffer[m_nClockHand]->m_nUseCounter != 0)
			{
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

			if (m_vtClockBufferWeight[m_nClockHand] > 0)
			{
				m_vtClockBufferWeight[m_nClockHand]--;
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

			auto& obj = m_vtClockBuffer[m_nClockHand];

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

			ASSERT(m_vtClockBuffer[m_nClockHand]);
			ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);

			if (obj->m_ptrCoreObject == nullptr)
			{
				ASSERT(false);
				//obj->m_nPositionInCLOCK = -1;
				////m_vtClockBuffer[m_nClockHand] = nullptr;
				//m_vtClockBuffer[m_nClockHand] = nullptr;
				//m_vtClockBufferWeight[m_nClockHand] = -1;
				//return m_nClockHand;
			}

			if (obj->_havedependentsincache())
			{
				obj->m_mtx.unlock();
				m_vtClockBufferWeight[m_nClockHand] = 0;
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

			if (obj->hasUpdatesToBeFlushed())
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(obj, uidUpdated) != CacheErrorCode::Success)
					throw std::logic_error("Critical: failed to add object to storage during eviction.");

				obj->m_uidUpdated = uidUpdated;
			}

			ASSERT(m_nClockHand == obj->m_nPositionInCLOCK);

			//m_vtClockBuffer[idx] = nullptr;
			m_vtClockBufferWeight[m_nClockHand] = -1;

			obj->m_bDirty = false;
			obj->deleteCoreObject();
			obj->m_nPositionInCLOCK = -1;

			obj->m_mtx.unlock();
			obj = nullptr;

#ifdef __CONCURRENT__
			m_nUsedCacheCapacity.fetch_add(1, std::memory_order_relaxed);
#else //__CONCURRENT__
			m_nUsedCacheCapacity++;
#endif //__CONCURRENT__

			return m_nClockHand;
		}
	}

private:

	inline void flushItemsToStorage()
	{
//#ifdef __CONCURRENT__
//		std::vector<ObjectTypePtr> vtObjects;
//
//		int nQSiz = 0;
//		{
//			std::unique_lock<std::mutex> lock_cache(m_mtxCache);
//			if ( (nQSiz = m_vtClockQ.size()) == 0) return;
//		}
//		/*
//		if (vtEmptySlots.size() == 0)
//		{
//			do 
//			{
//				for (int nIdx = 0; nIdx < m_nCacheCapacity; nIdx++)
//				{
//					if (m_vtClockBufferWeight[m_nClockHand] == -1)
//					{
//						vtEmptySlots.push_back(m_nClockHand);
//						ASSERT(!m_vtClockBuffer[m_nClockHand]);
//						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//						continue;
//					}
//
//					auto& obj = m_vtClockBuffer[m_nClockHand];
//
//					if (obj.use_count() > 2)
//					{
//						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//						continue;
//					}
//
//					// Check if the object is in use
//					if (!obj->m_mtx.try_lock())
//					{
//						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//						continue;
//					}
//
//					if (obj.use_count() > 2)
//					{
//						obj->m_mtx.unlock();
//						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//						continue;
//					}
//
//					if (m_vtClockBufferWeight[m_nClockHand] > 0)
//					{
//						obj->m_mtx.unlock();
//						m_vtClockBufferWeight[m_nClockHand]--;
//						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//						continue;
//					}
//
//					ASSERT(m_vtClockBuffer[m_nClockHand]);
//					
//					if (obj->m_ptrCoreObject == nullptr)
//					{
//						ASSERT(obj->m_nPositionInCLOCK == m_nClockHand);
//
//						//ASSERT(false); // should not happen!!
//						if (obj->m_nPositionInCLOCK != -1)
//						{
//							ASSERT(m_vtClockBuffer[m_nClockHand]);
//							ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);
//
//							//chcnk this
//							vtEmptySlots.push_back(m_nClockHand);
//							
//							m_vtClockBufferWeight[m_nClockHand] = -1;
//						}
//						else
//						{
//							ASSERT(!m_vtClockBuffer[m_nClockHand]);
//							ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);
//						}
//
//						obj->m_nPositionInCLOCK = -1;
//						obj->reftofirstiteminpair = nullptr;
//
//
//						obj->m_mtx.unlock();
//						
//						m_vtClockBuffer[m_nClockHand].reset();
//						m_vtClockBuffer[m_nClockHand] = nullptr;
//
//						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//						continue;
//					}
//
//					ASSERT(obj->m_ptrCoreObject);
//
//					vtObjects.push_back(obj);
//
//					vtEmptySlots.push_back(m_nClockHand);
//
//					ASSERT(obj->m_nPositionInCLOCK == m_nClockHand);
//
//					m_vtClockBufferWeight[m_nClockHand] = -1;
//
//					obj->m_nPositionInCLOCK = -1;
//					obj->reftofirstiteminpair = nullptr;
//
//					m_vtClockBuffer[m_nClockHand].reset();
//					m_vtClockBuffer[m_nClockHand] = nullptr;
//
//					ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);
//
//					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//				}
//
//				std::sort(vtEmptySlots.begin(), vtEmptySlots.end());
//			} while (vtEmptySlots.size() == 0);
//		}
//		*/
//		
//		int nIdx = 0;
//		int pIdx = 0;
//		for (; nIdx < nQSiz; nIdx++)
//		{
//			auto& a_obj = m_vtClockQ[nIdx].first;
//
//			if (a_obj == nullptr)
//			{
//				continue;
//			}
//
//			if (m_vtClockQ[nIdx].second == 0)
//			{
//				//ASSERT(a_obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
//				//a_obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
//
//				ASSERT(a_obj->del);
//				//if(a_obj->reftofirstiteminpair != nullptr) *a_obj->reftofirstiteminpair = m_vtClockQ[nIdx].second;
//				continue;
//			}
//			/*
//			if (obj->del == true && obj->m_ptrCoreObject != nullptr)
//			{
//				//ASSERT(ptrObject->m_uid.getMediaType() > 1);
//				//if (obj->m_uid.getMediaType() > 1)
//				//{
//				//	m_ptrStorage->remove(obj->m_uid);
//				//}
//
//				//why not delete it instead od flag.. why you used flag??
//				obj->deleteCoreObject();
//				obj->m_uidUpdated = std::nullopt;
//				obj->m_uid = ObjectUIDType(INT64_MAX);
//
//				auto idx = obj->m_nPositionInCLOCK;
//				if (idx != -1/) //might be case where item had to bee added tot the queue but got deleted!!
//				{
//					ASSERT(m_vtClockBuffer[idx]);
//					ASSERT(m_vtClockBufferWeight[idx] != -1);
//
//					m_vtClockBuffer[idx].reset();
//					m_vtClockBuffer[idx] = nullptr;
//					m_vtClockBufferWeight[idx] = -1;
//					obj->m_nPositionInCLOCK = -1;
//					obj->reftofirstiteminpair = nullptr;
//
//					continue;
//				}
//
//				//ASSERT(obj->m_nPositionInCLOCK == -1);
//				ASSERT(!obj->reftofirstiteminpair);
//
//				continue;
//			}
//
//			if (obj->m_ptrCoreObject == nullptr) // there might be a case where them item being pushed multiple times but got evicted in the previous round!!
//			{
//				auto idx = obj->m_nPositionInCLOCK;
//				if (idx != -1) //might be case where item had to bee added tot the queue but got deleted!!
//				{
//					ASSERT(m_vtClockBuffer[idx]);
//					ASSERT(m_vtClockBufferWeight[idx] != -1);
//
//					m_vtClockBuffer[idx].reset();
//					m_vtClockBuffer[idx] = nullptr;
//					m_vtClockBufferWeight[idx] = -1;
//					obj->m_nPositionInCLOCK = -1;
//					obj->reftofirstiteminpair = nullptr;
//
//					continue;
//				}
//
//				//ASSERT(obj->m_nPositionInCLOCK == -1);
//				ASSERT(!obj->reftofirstiteminpair);
//
//				continue;
//			}
//			*/
//			//ASSERT(obj->m_nPositionInCLOCK != -1);
//			ASSERT(a_obj);
//
//			if (a_obj->m_nPositionInCLOCK == -1)
//			{
//				/*for (auto& _obj : vtObjects)
//				{
//					ASSERT(_obj->m_uid != obj->m_uid);
//				}*/
//				/*if (pIdx == vtEmptySlots.size())
//				{
//					break;
//				}*/
//				{
//					//for (int nIdx = 0; nIdx < m_nCacheCapacity; nIdx++)
//					while(true)
//					{
//						if (m_vtClockBufferWeight[m_nClockHand] == -1)
//						{
//							//vtEmptySlots.push_back(m_nClockHand);
//							ASSERT(!m_vtClockBuffer[m_nClockHand]);
//							//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//							break;
//						}
//
//						if (m_vtClockBufferWeight[m_nClockHand] > 0)
//						{
//							//obj->m_mtx.unlock();
//							m_vtClockBufferWeight[m_nClockHand]--;
//							m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//							continue;
//						}
//
//						if (m_vtClockBuffer[m_nClockHand]->m_nUseCounter != 0)
//						{
//							m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//							continue;
//						}
//
//						auto& ___obj = m_vtClockBuffer[m_nClockHand];
//
//						// Check if the ___object is in use
//						if (!___obj->m_mtx.try_lock())
//						{
//							m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//							continue;
//						}
//
//						if (___obj->m_nUseCounter != 0)
//						{
//							___obj->m_mtx.unlock();
//							m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//							continue;
//						}
//
//						//ASSERT(a_obj->m_uid != ___obj->m_uid);
//						if (___obj->del == true/* && ___obj->m_ptrCoreObject != nullptr*/)
//						{
//							ASSERT(___obj->m_ptrCoreObject);
//						/*	if (___obj->m_uid.getMediaType() > 1)
//							{
//								m_ptrStorage->remove(___obj->m_uid);
//							}*/
//
//							if (___obj->m_ptrCoreObject != nullptr) {
//								//m_nUsedCacheCapacity--;
//							}
//
//							//why not delete it instead od flag.. why you used flag??
//							___obj->deleteCoreObject();
//							___obj->m_uidUpdated = std::nullopt;
//							___obj->m_uid = ObjectUIDType(INT64_MAX);
//
//							auto idx = ___obj->m_nPositionInCLOCK;
//							if (idx != -1/* && idx != INT_MAX*/) //might be case where item had to bee added tot the queue but got deleted!!
//							{
//								ASSERT(m_vtClockBuffer[idx]);
//								ASSERT(m_vtClockBufferWeight[idx] != -1);
//
//								
//								m_vtClockBufferWeight[idx] = -1;
//								___obj->m_nPositionInCLOCK = -1;
//								//___obj->reftofirstiteminpair = nullptr;
//
//								//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//
//								___obj->m_mtx.unlock();
//								___obj = nullptr;
//
//								
//								break;
//							}
//
//							//ASSERT(___obj->m_nPositionInCLOCK == -1);
//
//							//ASSERT(!___obj->reftofirstiteminpair);
//							___obj->m_mtx.unlock();
//							___obj = nullptr;
//
//							//m_vtClockBuffer[idx].reset();
//							m_vtClockBuffer[idx] = nullptr;
//
//							//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//							break;
//						}
//
//						ASSERT(m_vtClockBuffer[m_nClockHand]);
//						//ASSERT(a_obj->m_uid != ___obj->m_uid);
//
//						if (___obj->m_ptrCoreObject == nullptr)
//						{
//							ASSERT(false);
//							/*
//							ASSERT(___obj->m_nPositionInCLOCK == m_nClockHand);
//
//							//ASSERT(false); // should not happen!!
//							if (___obj->m_nPositionInCLOCK != -1)
//							{
//								ASSERT(m_vtClockBuffer[m_nClockHand]);
//								ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);
//
//								//chcnk this
//								//vtEmptySlots.push_back(m_nClockHand);
//
//								m_vtClockBufferWeight[m_nClockHand] = -1;
//								
//							}
//							else
//							{
//								ASSERT(!m_vtClockBuffer[m_nClockHand]);
//								ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);
//							}
//
//							___obj->m_nPositionInCLOCK = -1;
//							___obj->reftofirstiteminpair = nullptr;
//
//
//							___obj->m_mtx.unlock();
//
//							ASSERT(obj->m_uid != ___obj->m_uid);
//							m_vtClockBuffer[m_nClockHand].reset();
//							m_vtClockBuffer[m_nClockHand] = nullptr;
//
//							//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//							*/
//							break;
//							
//						}
//
//						if (___obj->havedependentsincache())
//						{
//							m_vtClockBufferWeight[m_nClockHand] = 0;
//							___obj->m_mtx.unlock();
//							m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//							continue;
//						}
//
//						ASSERT(___obj->m_ptrCoreObject);
//
//						vtObjects.push_back(___obj);
//
//						//vtEmptySlots.push_back(m_nClockHand);
//
//						ASSERT(___obj->m_nPositionInCLOCK == m_nClockHand);
//
//						m_vtClockBufferWeight[m_nClockHand] = -1;
//
//						___obj->m_nPositionInCLOCK = -1;
//						//___obj->reftofirstiteminpair = nullptr;
//
//						//ASSERT(obj);
//						ASSERT(a_obj->m_uid != ___obj->m_uid);
//						//m_vtClockBuffer[m_nClockHand].reset();
//						m_vtClockBuffer[m_nClockHand] = nullptr;
//
//						ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);
//
//						//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//						break;
//					}
//				}
//
//				//if (a_obj)
//				{
//					ASSERT(a_obj->m_ptrCoreObject);
//					//int nSlotIdx = vtEmptySlots[pIdx++];
//					int nSlotIdx = m_nClockHand;
//
//					ASSERT(m_vtClockBufferWeight[nSlotIdx] == -1);
//					ASSERT(!m_vtClockBuffer[nSlotIdx]);
//
//					m_vtClockBuffer[nSlotIdx] = a_obj;	// use atomics!!!!
//					a_obj->m_nPositionInCLOCK = nSlotIdx;
//					//a_obj->reftofirstiteminpair = &m_vtClockBufferWeight[nSlotIdx];
//				}
//				//else
//				{
//				//ASSERT(!obj);
//				}
//
//			}
//			
//			//if (!a_obj->del)
//			{
//				ASSERT(a_obj->m_nUseCounter.load(std::memory_order_relaxed) > 0);
//				a_obj->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
//			}
//			//else
//			{
//			//	ASSERT(a_obj->m_nUseCounter.load(std::memory_order_relaxed) == 0);
//			}
//
//			//if (obj)
//			//{
//				//*a_obj->reftofirstiteminpair = m_vtClockQ[nIdx].second;
//				m_vtClockBufferWeight[a_obj->m_nPositionInCLOCK] = m_vtClockQ[nIdx].second;
//			//}
//			//else
//			//{
//			//	ASSERT(!obj);
//			//}
//		}
//
//		{
//			ASSERT(nIdx == nQSiz);
//			std::unique_lock<std::mutex> lock_cache(m_mtxCache);
//			m_vtClockQ.erase(m_vtClockQ.begin(), m_vtClockQ.begin() + nIdx);
//			
//		}
//		//cv.notify_all();
//		//vtEmptySlots.erase(vtEmptySlots.begin(), vtEmptySlots.begin() + pIdx);
//
//		int f = 0;
//		bool prg = false;
//		bool lov = false;
//		do {
//			prg = false;
//			lov = false;
//			for (int j = 0; j < vtObjects.size(); j++)
//				//for( auto& obj: vtObjects)
//			{
//				auto& obj = vtObjects[j];
//				if (!obj)
//					continue;
//
//				if (obj->m_ptrCoreObject == nullptr || obj->del)
//				{
//					ASSERT(false);
//					auto idx = obj->m_nPositionInCLOCK;
//					if (idx != -1) //might be case where item had to bee added tot the queue but got deleted!!
//					{
//						ASSERT(m_vtClockBuffer[idx]);
//						ASSERT(m_vtClockBufferWeight[idx] != -1);
//
//						//m_vtClockBuffer[idx].reset();
//						m_vtClockBuffer[idx] = nullptr;
//						m_vtClockBufferWeight[idx] = -1;
//						obj->m_nPositionInCLOCK = -1;
//						//obj->reftofirstiteminpair = nullptr;
//
//						continue;
//					}
//
//
//					//ASSERT(!obj->reftofirstiteminpair);
//					obj->m_mtx.unlock();
//					continue;
//				}
//
//				ASSERT(obj->m_nPositionInCLOCK == -1);
//				//ASSERT(!obj->reftofirstiteminpair);
//				if (obj->_havedependentsincache())
//				{
//					//obj->m_mtx.unlock();
//					lov = true;
//					continue;
//				}
//
//				if (obj->hasUpdatesToBeFlushed())
//				{
//					ObjectUIDType uidUpdated;
//					if (m_ptrStorage->addObject(obj, uidUpdated) != CacheErrorCode::Success)
//					{
//						std::cout << obj->m_nPositionInCLOCK << std::endl;
//						if (obj->evict) std::cout << "evict" << std::endl;
//						if (obj->reload) std::cout << "reload" << std::endl;
//						if (obj->del2) std::cout << "del2" << std::endl;
//						if (obj->del) std::cout << "del" << std::endl;
//						//throw std::logic_error("Critical: failed to add object to storage during eviction.");
//					}
//					obj->m_uidUpdated = uidUpdated;
//				}
//
//				obj->m_bDirty = false;
//				ASSERT(obj->m_ptrCoreObject);
//				obj->deleteCoreObject();
//				//obj->m_bReferenced = false;
//				obj->m_nFlushPriority = 0;
//				obj->m_nPositionInCLOCK = -1;
//				//obj->reftofirstiteminpair = nullptr;
//				obj->evict = true;
//				obj->m_mtx.unlock();
//				obj = nullptr;
//				f++;
//				prg = true;
//				//m_nUsedCacheCapacity--;
//			}
//			//prg = true;
//		} while (lov && prg);
//
//		for (int j = 0; j < vtObjects.size(); j++)
//			//for( auto& obj: vtObjects)
//		{
//			auto& obj = vtObjects[j];
//			if (!obj)
//				continue;
//
//			if (obj->_havedependentsincache())
//			{
//
//				{
//					ASSERT(nIdx == nQSiz);
//					std::unique_lock<std::mutex> lock_cache(m_mtxCache);
//					m_vtClockQ.push_back({ obj , 1});
//					ASSERT(m_vtClockBuffer.size() < m_nCacheCapacity*2);
//
//				}
//				
//
//				obj->m_mtx.unlock();
//				//lov = true;
//				//continue;
//			}
//		}
//		vtObjects.clear();
//		//lock_cache.unlock();
//				
//		{
//			std::unique_lock<std::mutex> lock_cache(m_mtxCache);
//			m_nUsedCacheCapacity -= f;
//		}
//
//		cv.notify_all();
//
//
//#else //__CONCURRENT__
//		while (true)
//		{
//			if (m_vtClockBufferWeight[m_nClockHand] == -1)
//			{
//				ASSERT(!m_vtClockBuffer[m_nClockHand]);
//				return;
//			}
//
//			if (m_vtClockBufferWeight[m_nClockHand] > 0)
//			{
//				m_vtClockBufferWeight[m_nClockHand]--;
//				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//				continue;
//			}
//
//			// but use counter is part of concurrent setting!!
//			//if (m_vtClockBuffer[m_nClockHand].use_count() > 2)
//			//{
//			//	m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//			//	continue;
//			//}
//
//			auto& obj = m_vtClockBuffer[m_nClockHand];
//			ASSERT(m_vtClockBuffer[m_nClockHand]);
//			ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);
//
//			if (obj->m_ptrCoreObject == nullptr)
//			{
//				//ASSERT(false);
//				
//				obj->m_nPositionInCLOCK = -1;
//
//				//m_vtClockBuffer[m_nClockHand] = nullptr;
//				m_vtClockBuffer[m_nClockHand] = nullptr;
//				m_vtClockBufferWeight[m_nClockHand] = -1;
//
//				return;
//				
//			}
//
//			//if (obj.use_count() > 2)
//			//{
//			//	m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//			//	continue;
//			//}
//
//			if (obj->_havedependentsincache())
//			{
//				m_vtClockBufferWeight[m_nClockHand] = 0;
//				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//				continue;
//			}
//
//			if (obj->hasUpdatesToBeFlushed())
//			{
//				ObjectUIDType uidUpdated;
//				if (m_ptrStorage->addObject(obj, uidUpdated) != CacheErrorCode::Success)
//					throw std::logic_error("Critical: failed to add object to storage during eviction.");
//				obj->m_uidUpdated = uidUpdated;
//			}
//
//			int idx = obj->m_nPositionInCLOCK;
//
//
//			//obj->deleteCoreObject();
//			obj->m_nFlushPriority = 0;
//			obj->m_nPositionInCLOCK = -1;
//			//*obj->reftofirstiteminpair = -1;
//			//obj->reftofirstiteminpair = nullptr;
//			//obj.reset();
//
//			obj->m_bDirty = false;
//			obj->deleteCoreObject();
//			obj = nullptr;
//
//			//m_vtClockBuffer[idx] = nullptr;
//			m_vtClockBuffer[idx] = nullptr;
//			m_vtClockBufferWeight[idx] = -1;
//
//			m_nUsedCacheCapacity--;
//			return;
//		}
//#endif //__CONCURRENT__
	}

	inline void flushAllItemsToStorage()
	{
//		
//#ifdef __CONCURRENT__
//		std::unique_lock<std::mutex> lock_cache(m_mtxCache);
//#endif //__CONCURRENT__
//		//return;
//		while (m_nUsedCacheCapacity > 0)
//		{
//			{
//				if (m_vtClockBufferWeight[m_nClockHand] == -1)
//				{
//					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//					continue;
//				}
//
//				if (m_vtClockBufferWeight[m_nClockHand] > 0)
//				{
//					m_vtClockBufferWeight[m_nClockHand]--;
//					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//					continue;
//				}
//
//				auto& obj = m_vtClockBuffer[m_nClockHand];
//				ASSERT(m_vtClockBuffer[m_nClockHand]);
//
//				if (obj->del == true/* && obj->m_ptrCoreObject != nullptr*/)
//				{
//					//ASSERT(ptrObject->m_uid.getMediaType() > 1);
//				/*	if (obj->m_uid.getMediaType() > 1)
//					{
//						m_ptrStorage->remove(obj->m_uid);
//					}*/
//
//					if (obj->m_ptrCoreObject != nullptr) {
//						//m_nUsedCacheCapacity--;
//					}
//
//					//why not delete it instead od flag.. why you used flag??
//					obj->deleteCoreObject();
//					obj->m_uidUpdated = std::nullopt;
//					obj->m_uid = ObjectUIDType(INT64_MAX);
//
//					auto idx = obj->m_nPositionInCLOCK;
//					if (idx != -1/* && idx != INT_MAX*/) //might be case where item had to bee added tot the queue but got deleted!!
//					{
//						ASSERT(m_vtClockBuffer[idx]);
//						ASSERT(m_vtClockBufferWeight[idx] != -1);
//
//
//						m_vtClockBufferWeight[idx] = -1;
//						obj->m_nPositionInCLOCK = -1;
//						//obj->reftofirstiteminpair = nullptr;
//
//						//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//
//#ifdef __CONCURRENT__
//						obj->m_mtx.unlock();
//#endif //__CONCURRENT__
//						obj.reset();
//
//						m_vtClockBuffer[idx].reset();
//						m_vtClockBuffer[idx] = nullptr;
//
//						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//						continue;
//					}
//
//					//ASSERT(obj->m_nPositionInCLOCK == -1);
//
//					//ASSERT(!obj->reftofirstiteminpair);
//#ifdef __CONCURRENT__
//					obj->m_mtx.unlock();
//#endif //__CONCURRENT__
//					obj.reset();
//					//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//					continue;
//				}
//
//				if (obj->m_ptrCoreObject == nullptr)
//				{
//					obj->m_nPositionInCLOCK = -1;
//
//					m_vtClockBuffer[m_nClockHand].reset();
//					m_vtClockBuffer[m_nClockHand] = nullptr;
//					m_vtClockBufferWeight[m_nClockHand] = -1;
//
//					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//					continue;
//				}
//
//				/*if (obj.use_count() > 2)
//				{
//					for (int o = 0; o < m_nCacheCapacity; o++) {
//						if (m_vtClockBufferWeight[o] == -1)
//							continue;
//
//						if (o == m_nClockHand)
//							continue;
//						ASSERT(obj->m_uid != m_vtClockBuffer[o]->m_uid);
//					}
//					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//					continue;
//				}*/
//
//#ifdef __CONCURRENT__
//				// Check if the object is in use
//				if (!obj->m_mtx.try_lock())
//				{
//					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//					continue;
//				}
//#endif //__CONCURRENT__
//
//				if (obj->havedependentsincache())
//				{
//					m_vtClockBufferWeight[m_nClockHand] = 0;
//#ifdef __CONCURRENT__
//					obj->m_mtx.unlock();
//#endif //__CONCURRENT__
//					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//					continue;
//				}
//
//
//				if (obj->hasUpdatesToBeFlushed())
//				{
//					ObjectUIDType uidUpdated;
//					if (m_ptrStorage->addObject(obj->m_uid, obj, uidUpdated) != CacheErrorCode::Success)
//						throw std::logic_error("Critical: failed to add object to storage during eviction.");
//					obj->m_uidUpdated = uidUpdated;
//				}
//
//				auto idx = obj->m_nPositionInCLOCK;
//
//				obj->deleteCoreObject();
//				obj->m_nFlushPriority = 0;
//				obj->m_nPositionInCLOCK = -1;
//				//*obj->reftofirstiteminpair = -1;
//				
//
//#ifdef __CONCURRENT__
//				obj->m_mtx.unlock();
//#endif //__CONCURRENT__
//				obj.reset();
//				m_vtClockBuffer[idx] = nullptr;
//				m_vtClockBufferWeight[idx] = -1;
//
//				m_nUsedCacheCapacity--;
//
//				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
//			}
//		}
//
//		ASSERT(m_nUsedCacheCapacity == 0);
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

		} while (false);// !ptrSelf->m_bStop);
	}
#endif //__CONCURRENT__
};
