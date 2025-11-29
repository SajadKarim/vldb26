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
	using ObjectType = typename Traits::ObjectType;
	using ObjectTypePtr = std::shared_ptr<ObjectType>;
	using ObjectUIDType = typename Traits::ObjectUIDType;

	using KeyType = typename Traits::KeyType;
	using ValueType = typename Traits::ValueType;

	using StorageType = typename Traits::StorageType;

	using WALType = typename Traits::WALType;

private:
	size_t m_nClockHand;
	std::vector<int> m_vtClockBufferWeight;
	std::vector<ObjectTypePtr> m_vtClockBuffer;
	std::vector<ObjectTypePtr> m_vtClockQ;

	std::unique_ptr<StorageType> m_ptrStorage;

	int64_t m_nCacheCapacity;
	int64_t m_nUsedCacheCapacity;

	int64_t m_nCacheMiss;

	std::unique_ptr<WALType> m_ptrWAL;
	std::condition_variable cv;

	std::atomic<std::shared_ptr<ObjectTypePtr>> a;
#ifdef __CONCURRENT__
	bool m_bStop;

	std::thread m_threadCacheFlush;

	mutable std::mutex m_mtxCache;
	mutable std::shared_mutex m_mtxStorage;
#endif //__CONCURRENT__

public:
	~CLOCKCache()
	{
#ifdef __ENABLE_ASSERTS__
		std::cout << "Total Cache Misses: " << m_nCacheMiss << std::endl;
#endif //__ENABLE_ASSERTS__

#ifdef __CONCURRENT__
		m_bStop = true;
		m_threadCacheFlush.join();
#endif //__CONCURRENT__

		flush(false);

		m_ptrStorage.reset();
	}

	template <typename... StorageArgs>
	CLOCKCache(size_t nCapacity, StorageArgs... args)
		: m_nCacheMiss(0)
		, m_nCacheCapacity(nCapacity)
		, m_nUsedCacheCapacity(0)
		//, m_ptrHead(nullptr)
		//, m_ptrTail(nullptr)
	{
		m_nClockHand = 0;
		m_vtClockBuffer.resize(nCapacity);
		m_vtClockBufferWeight.resize(nCapacity);
		for (int i = 0 ; i < nCapacity ; ++i)
		{
			m_vtClockBuffer[i] = nullptr;
			m_vtClockBufferWeight[i] = -1;
		}

		m_ptrStorage = std::make_unique<StorageType>(args...);
		
#ifdef __CONCURRENT__
		m_bStop = false;
		m_threadCacheFlush = std::thread(handlerCacheFlush, this);
#endif //__CONCURRENT__

		m_ptrWAL = std::make_unique<WALType>(this, WAL_FILE_PATH);
	}

	template <typename... InitArgs>
	CacheErrorCode init(InitArgs... args)
	{
		return m_ptrStorage->init(this);
	}

	void log(uint8_t op, const KeyType& key, const ValueType& value)
	{
		m_ptrWAL->append(op, key, value);
	}

	CacheErrorCode updateObjectsAccessMetadata(std::vector<ObjectTypePtr>& vtObjects, int level)
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::mutex> lock_cache(m_mtxCache);
		m_vtClockQ.insert(m_vtClockQ.end(), vtObjects.begin(), vtObjects.end());

		vtObjects.clear();
#else //__CONCURRENT__
		for (auto it = vtObjects.begin(); it != vtObjects.end(); it++)
		{
			auto obj = *it;
			if (obj == nullptr)
			{
				continue;
			}


			if (obj->m_ptrCoreObject == nullptr)
			{
				ASSERT(obj->m_nPositionInCLOCK == -1);
				continue;
			}

			if (obj->m_nPositionInCLOCK == -1)
			{
				int idx = m_vtClockBufferWeight[m_nClockHand];
				if (idx != -1)
				{
					ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);
					ASSERT(m_vtClockBuffer[m_nClockHand]);
					flushItemsToStorage();
				}

				idx = m_nClockHand;

				ASSERT(m_vtClockBufferWeight[idx] == -1);
				ASSERT(!m_vtClockBuffer[idx]);

				m_vtClockBuffer[idx] = *it;	// use atomics!!!!
				m_vtClockBufferWeight[idx] = level - obj->m_nFlushPriority;	// use atomics!!!!
				obj->m_nPositionInCLOCK = idx;
				obj->reftofirstiteminpair = &m_vtClockBufferWeight[idx];
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
			}
		}

		for (auto it = vtObjects.begin(); it != vtObjects.end(); it++)
		{
			auto obj = *it;
			if (obj == nullptr || obj->m_nPositionInCLOCK == -1 || obj->m_ptrCoreObject == nullptr)
			{
				continue;
			}

			*obj->reftofirstiteminpair = level - obj->m_nFlushPriority;	// use atomics!!!!
		}

		vtObjects.clear();
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}

	CacheErrorCode remove(ObjectTypePtr ptrObject)
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		//ASSERT(ptrObject->m_uid.getMediaType() > 1);
		if (ptrObject->m_uid.getMediaType() > 1)
		{
			m_ptrStorage->remove(ptrObject->m_uid);
		}

		ptrObject->deleteCoreObject();
		ptrObject->m_uidUpdated = std::nullopt;
		ptrObject->m_uid = ObjectUIDType(INT64_MAX);


		auto idx = ptrObject->m_nPositionInCLOCK;
		if (idx != -1)
		{
			ASSERT(m_vtClockBuffer[idx]);
			m_vtClockBuffer[idx].reset();
			m_vtClockBuffer[idx] = nullptr;
			m_vtClockBufferWeight[idx] = -1;
			
		}

		ptrObject->reftofirstiteminpair = nullptr;
		ASSERT(!ptrObject->reftofirstiteminpair);
		//if (ptrObject->reftofirstiteminpair != nullptr)
		//	*ptrObject->reftofirstiteminpair = -1;

		//ptrObject->m_nFlushPriority = -1;	// IMPORTANT: Dependent field do not remove it!!!!
		ptrObject->m_nPositionInCLOCK = -1;

		//ptrObject.reset();

		m_nUsedCacheCapacity--;

		//ptrObject->m_nFlushPriority = -1;
		//todo
		// deffered deletetion in the clock to the batch processing for cache locality!!

		return CacheErrorCode::Success;
	}

	CacheErrorCode getCoreObject(uint16_t nDegree, const ObjectUIDType& uidObject, ObjectTypePtr& ptrObject)
	{
		m_ptrStorage->getObject(nDegree, uidObject, ptrObject);

		if (ptrObject->m_ptrCoreObject == nullptr)
		{
			throw new std::logic_error("The requested object does not exist.");   // TODO: critical log.
		}

		//ptrObject->m_bDirty = false;
		//ptrObject->m_uid = uidObject;	// set it in the deserialization step.
		//ptrObject->m_uidUpdated = std::nullopt;

#ifdef __CONCURRENT__
		std::unique_lock<std::mutex> lock_cache(m_mtxCache); // TODO: think should this be moved to storage???
#endif //__CONCURRENT__


#ifdef __ENABLE_ASSERTS__
		m_nCacheMiss++;
#endif //__ENABLE_ASSERTS__


		m_nUsedCacheCapacity++;

		return CacheErrorCode::Success;
	}

	CacheErrorCode getObject(uint16_t nDegree, const ObjectUIDType& uidObject, ObjectTypePtr& ptrObject)
	{
		ptrObject = m_ptrStorage->getObject(nDegree, uidObject);

		if (ptrObject == nullptr)
		{
			throw new std::logic_error("The requested object does not exist.");   // TODO: critical log.
		}

		//ptrObject->m_uid = uidObject;	// set it in the deserialization step.

#ifdef __CONCURRENT__
		std::unique_lock<std::mutex> lock_cache(m_mtxCache); // TODO: think should this be moved to storage???
#endif //__CONCURRENT__

#ifdef __ENABLE_ASSERTS__
		m_nCacheMiss++;
#endif //__ENABLE_ASSERTS__

		m_nUsedCacheCapacity++;

		return CacheErrorCode::Success;
	}

	template<class Type, typename... ArgsType>
	CacheErrorCode createObjectOfType(std::optional<ObjectUIDType>& uidObject, std::shared_ptr<ObjectType>& ptrObject, const ArgsType... args)
	{
		ptrObject = std::make_shared<ObjectType>((uint8_t)Type::UID, new Type(args...));

		ObjectUIDType::createAddressFromVolatilePointer(ptrObject->m_uid, (uint8_t)Type::UID, reinterpret_cast<uintptr_t>(ptrObject.get()));

		uidObject = ptrObject->m_uid;

#ifdef __CONCURRENT__
		std::unique_lock<std::mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		m_nUsedCacheCapacity++;

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

private:

	inline void flushItemsToStorage()
	{
#ifdef __CONCURRENT__
		std::vector<ObjectTypePtr> vtObjects;

		//std::unique_lock<std::mutex> lock_cache(m_mtxCache);
		//if (m_nUsedCacheCapacity < m_nCacheCapacity)
		//	return;

		//auto np = m_nCacheCapacity * 0.9;
		//while (m_nUsedCacheCapacity > np)
		//{
		//	auto& obj = m_vtClockBuffer[m_nClockHand];
		//	if (!obj || obj->m_ptrCoreObject == nullptr)
		//	{
		//		m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
		//		continue;
		//	}

		//	//std::cout << obj.use_count() << ",";
		//	if (obj.use_count() > 2)
		//	{
		//		m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
		//		continue;
		//	}

		//	if (!obj->m_mtx.try_lock())
		//	{
		//		m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
		//		continue;
		//	}

		///*	if (obj.use_count() > 2)
		//	{
		//		obj->m_mtx.unlock();
		//		m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
		//		continue;
		//	}

		//	std::wcout << obj.use_count() << "," << std::endl;*/

		//	if (obj->m_nFlushPriority != 0)
		//	{
		//		obj->m_nFlushPriority--; //use atomic
		//		obj->m_mtx.unlock();
		//		m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
		//		continue;
		//	}

		//	if (obj->m_bReferenced)
		//	{
		//		obj->m_bReferenced = false;
		//		obj->m_mtx.unlock();
		//		m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
		//		continue;
		//	}


		//	vtObjects.push_back(obj);

		//	auto idx = obj->m_nPositionInCLOCK;
		//	m_qClockEmptySlots.push(idx);
		//	//m_vtClockBuffer[idx].reset();
		//	m_vtClockBuffer[idx] = nullptr;

		//	m_nUsedCacheCapacity--;
		//}

		//
		//for(auto itObject = vtObjects.begin(); itObject != vtObjects.end(); itObject++)
		//{
		//	auto& obj = *itObject;

		//	if (obj->hasUpdatesToBeFlushed())
		//	{
		//		ObjectUIDType uidUpdated;
		//		if (m_ptrStorage->addObject(obj->m_uid, obj, uidUpdated) != CacheErrorCode::Success)
		//			throw std::logic_error("Critical: failed to add object to storage during eviction.");
		//		obj->m_uidUpdated = uidUpdated;
		//	}

		//	obj->m_bDirty = false;
		//	obj->deleteCoreObject();
		//	obj->m_mtx.unlock();
		//	obj->m_bReferenced = false;
		//	obj->m_nFlushPriority = 0;
		//	obj->m_nPositionInCLOCK = -1;
		//	obj.reset();
		//}

		//vtObjects.clear();

		//lock_cache.unlock();
		//cv.notify_all();

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

			auto& obj = m_vtClockBuffer[m_nClockHand];
			ASSERT(m_vtClockBuffer[m_nClockHand]);
			ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);

			if (obj->m_ptrCoreObject == nullptr)
			{
				obj->m_nPositionInCLOCK = -1;

				m_vtClockBuffer[m_nClockHand].reset();
				m_vtClockBuffer[m_nClockHand] = nullptr;
				m_vtClockBufferWeight[m_nClockHand] = -1;

				return;
			}

			if (obj.use_count() > 2)
			{
				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				continue;
			}

			if (obj->hasUpdatesToBeFlushed())
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(obj->m_uid, obj, uidUpdated) != CacheErrorCode::Success)
					throw std::logic_error("Critical: failed to add object to storage during eviction.");
				obj->m_uidUpdated = uidUpdated;
			}

			int idx = obj->m_nPositionInCLOCK;

			obj->deleteCoreObject();
			obj->m_nFlushPriority = 0;
			obj->m_nPositionInCLOCK = -1;
			*obj->reftofirstiteminpair = -1;
			obj->reftofirstiteminpair = nullptr;
			obj.reset();

			m_vtClockBuffer[idx].reset();
			m_vtClockBuffer[idx] = nullptr;
			m_vtClockBufferWeight[idx] = -1;

			m_nUsedCacheCapacity--;
			return;
		}
#endif //__CONCURRENT__
	}

	inline void flushAllItemsToStorage()
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		while (m_nUsedCacheCapacity > 0)
		{
			{
				if (m_vtClockBufferWeight[m_nClockHand] == -1)
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
				ASSERT(m_vtClockBuffer[m_nClockHand]);

				if (obj->m_ptrCoreObject == nullptr)
				{
					obj->m_nPositionInCLOCK = -1;

					m_vtClockBuffer[m_nClockHand].reset();
					m_vtClockBuffer[m_nClockHand] = nullptr;
					m_vtClockBufferWeight[m_nClockHand] = -1;

					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
					continue;
				}

				if (obj.use_count() > 2)
				{
					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
					continue;
				}

#ifdef __CONCURRENT__
				// Check if the object is in use
				if (!obj->m_mtx.try_lock())
				{
					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
					continue;
				}
#endif //__CONCURRENT__

				if (obj->hasUpdatesToBeFlushed())
				{
					ObjectUIDType uidUpdated;
					if (m_ptrStorage->addObject(obj->m_uid, obj, uidUpdated) != CacheErrorCode::Success)
						throw std::logic_error("Critical: failed to add object to storage during eviction.");
					obj->m_uidUpdated = uidUpdated;
				}

				auto idx = obj->m_nPositionInCLOCK;

				obj->deleteCoreObject();
				obj->m_nFlushPriority = 0;
				obj->m_nPositionInCLOCK = -1;
				*obj->reftofirstiteminpair = -1;
				obj.reset();

#ifdef __CONCURRENT__
				obj->m_mtx.unlock();
#endif //__CONCURRENT__

				m_vtClockBuffer[idx] = nullptr;
				m_vtClockBufferWeight[idx] = -1;

				m_nUsedCacheCapacity--;

				m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
			}
		}

		ASSERT(m_nUsedCacheCapacity == 0);
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
		do
		{
			ptrSelf->flushItemsToStorage();

			std::this_thread::sleep_for(1ms);

		} while (!ptrSelf->m_bStop);
	}
#endif //__CONCURRENT__
};
