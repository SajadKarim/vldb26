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
#include <atomic>
#include <assert.h>
#include "IFlushCallback.h"
#include "VariadicNthType.h"

#define FLUSH_COUNT 100
#define MIN_CACHE_FOOTPRINT 1024 * 1024	// Safe check!

using namespace std::chrono_literals;

template <typename ICallback, typename StorageType>
class A2QCache : public ICallback
{
	typedef A2QCache<ICallback, StorageType> SelfType;

public:
	typedef StorageType::ObjectUIDType ObjectUIDType;
	typedef StorageType::ObjectType ObjectType;
	typedef std::shared_ptr<ObjectType> ObjectTypePtr;

private:
	enum QueueType {
		OTA,
		MTA,
		PF
	};

	struct Item
	{
	public:
		ObjectUIDType m_uidSelf;
		ObjectTypePtr m_ptrObject;
		std::shared_ptr<Item> m_ptrPrev;
		std::shared_ptr<Item> m_ptrNext;
		QueueType m_qtLinkedQueue;

		Item(const ObjectUIDType& uidObject, const ObjectTypePtr ptrObject)
			: m_ptrNext(nullptr)
			, m_ptrPrev(nullptr)
			, m_qtLinkedQueue(QueueType::OTA)
		{
			m_uidSelf = uidObject;
			m_ptrObject = ptrObject;
		}

		~Item()
		{
			m_ptrPrev.reset();
			m_ptrNext.reset();
			m_ptrObject.reset();
		}
	};

	ICallback* m_ptrCallback;

	std::shared_ptr<Item> m_ptrHeadOTA, m_ptrTailOTA;
	std::shared_ptr<Item> m_ptrHeadMTA, m_ptrTailMTA;
	std::shared_ptr<Item> m_ptrHeadPF, m_ptrTailPF;

	std::unique_ptr<StorageType> m_ptrStorage;

	size_t m_nOTAItemsCount, m_nMTAItemsCount, m_nPFItemsCount;

	int64_t m_nCacheFootprint;
	int64_t m_nCacheCapacity;
	int64_t m_nPFCacheCapacity;
	std::unordered_map<ObjectUIDType, std::shared_ptr<Item>> m_mpObjects;
	std::unordered_map<ObjectUIDType, std::pair<std::optional<ObjectUIDType>, ObjectTypePtr>> m_mpUIDUpdates;

#ifdef __CONCURRENT__
	bool m_bStop;

	std::thread m_threadCacheFlush;

	std::condition_variable_any m_cvUIDUpdates;

	mutable std::shared_mutex m_mtxCache;
	mutable std::shared_mutex m_mtxStorage;
#endif //__CONCURRENT__

#ifdef __CACHE_COUNTERS__
	std::atomic<uint64_t> m_nCacheHits{0};
	std::atomic<uint64_t> m_nCacheMisses{0};
	std::atomic<uint64_t> m_nEvictions{0};
	std::atomic<uint64_t> m_nDirtyEvictions{0};
#endif //__CACHE_COUNTERS__

public:
	~A2QCache()
	{
#ifdef __CONCURRENT__
		m_bStop = true;
		m_threadCacheFlush.join();
#endif //__CONCURRENT__

		//presistCurrentCacheState();
		flushAllItemsToStorage();

		m_ptrHeadOTA.reset();
		m_ptrTailOTA.reset();
		m_ptrHeadMTA.reset();
		m_ptrTailMTA.reset();
		m_ptrHeadPF.reset();
		m_ptrTailPF.reset();

		m_ptrStorage.reset();

		m_mpObjects.clear();

		assert(m_nCacheFootprint == 0);
	}

	template <typename... StorageArgs>
	A2QCache(size_t nCapacity, StorageArgs... args)
		: m_nCacheCapacity(nCapacity)
		, m_nPFCacheCapacity(nCapacity/2)
		, m_nCacheFootprint(0)
		, m_nOTAItemsCount(0)
		, m_nMTAItemsCount(0)
		, m_nPFItemsCount(0)
		, m_ptrHeadOTA(nullptr)
		, m_ptrTailOTA(nullptr)
		, m_ptrHeadMTA(nullptr)
		, m_ptrTailMTA(nullptr)
		, m_ptrHeadPF(nullptr)
		, m_ptrTailPF(nullptr)
	{
#ifdef __TRACK_CACHE_FOOTPRINT__
		m_nCacheCapacity = m_nCacheCapacity < MIN_CACHE_FOOTPRINT ? MIN_CACHE_FOOTPRINT : m_nCacheCapacity;
#endif //__TRACK_CACHE_FOOTPRINT__

		m_ptrStorage = std::make_unique<StorageType>(args...);

#ifdef __CONCURRENT__
		m_bStop = false;
		m_threadCacheFlush = std::thread(handlerCacheFlush, this);
#endif //__CONCURRENT__
	}

	void updateMemoryFootprint(int32_t nMemoryFootprint)
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		m_nCacheFootprint += nMemoryFootprint;
	}

	template <typename... InitArgs>
	CacheErrorCode init(ICallback* ptrCallback, InitArgs... args)
	{
		//#ifdef __CONCURRENT__
		//		m_ptrCallback = ptrCallback;
		//		return m_ptrStorage->init(this/*getNthElement<0>(args...)*/);
		//#else // ! __CONCURRENT__
		//		return m_ptrStorage->init(ptrCallback/*getNthElement<0>(args...)*/);
		//#endif //__CONCURRENT__

		m_ptrCallback = ptrCallback;

		return m_ptrStorage->init(this/*getNthElement<0>(args...)*/);
	}

	CacheErrorCode remove(const ObjectUIDType& uidObject)
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		auto it = m_mpObjects.find(uidObject);
		if (it != m_mpObjects.end())
		{

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint -= (*it).second->m_ptrObject->getMemoryFootprint();

			assert(m_nCacheFootprint >= 0);
#endif //__TRACK_CACHE_FOOTPRINT__

			removeFromLRU((*it).second);
			m_mpObjects.erase(((*it).first));

			// TODO:
			// m_ptrStorage->remove(uidObject);
			return CacheErrorCode::Success;
		}

		// TODO:
		// m_ptrStorage->remove(uidObject);

		return CacheErrorCode::KeyDoesNotExist;
	}

	CacheErrorCode getObject(const ObjectUIDType& uidObject, ObjectTypePtr& ptrObject, std::optional<ObjectUIDType>& uidUpdated)
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache); // std::unique_lock due to LRU's linked-list update! is there any better way?
#endif //__CONCURRENT__

		if (m_mpObjects.find(uidObject) != m_mpObjects.end())
		{
			std::shared_ptr<Item> ptrItem = m_mpObjects[uidObject];
			//moveToFront(ptrItem);
			moveToFrontOfQ2(ptrItem);
			ptrObject = ptrItem->m_ptrObject;

#ifdef __CACHE_COUNTERS__
			m_nCacheHits.fetch_add(1, std::memory_order_relaxed);
#endif //__CACHE_COUNTERS__

			return CacheErrorCode::Success;
		}

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_storage(m_mtxStorage); // TODO: requesting the same key?
		lock_cache.unlock();
#endif //__CONCURRENT__

		ObjectUIDType uidTemp = uidObject;

		if (m_mpUIDUpdates.find(uidObject) != m_mpUIDUpdates.end())
		{
#ifdef __CONCURRENT__
			std::optional< ObjectUIDType >& _condition = m_mpUIDUpdates[uidObject].first;
			m_cvUIDUpdates.wait(lock_storage, [&_condition] { return _condition != std::nullopt; });
#endif //__CONCURRENT__

			uidUpdated = m_mpUIDUpdates[uidObject].first;

#ifdef __VALIDITY_CHECK__
			assert(uidUpdated != std::nullopt);
#endif //__VALIDITY_CHECK__

			m_mpUIDUpdates.erase(uidObject);
			uidTemp = *uidUpdated;
		}

#ifdef __CONCURRENT__
		lock_storage.unlock();
#endif //__CONCURRENT__

		ptrObject = m_ptrStorage->getObject(uidTemp);

		if (ptrObject != nullptr)
		{
#ifdef __CACHE_COUNTERS__
			m_nCacheMisses.fetch_add(1, std::memory_order_relaxed);
#endif //__CACHE_COUNTERS__

			std::shared_ptr<Item> ptrItem = std::make_shared<Item>(uidTemp, ptrObject);

#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> re_lock_cache(m_mtxCache);

			if (m_mpObjects.find(uidTemp) != m_mpObjects.end())
			{
				std::cout << "Some other thread has also accessed the object." << std::endl;
				throw new std::logic_error("...");
				/*
				#ifdef __TRACK_CACHE_FOOTPRINT__
								m_nCacheFootprint -= m_mpObjects[uidTemp]->m_ptrObject->getMemoryFootprint();

								assert(m_nCacheFootprint >= 0);

								m_nCacheFootprint += ptrObject->getMemoryFootprint();
				#endif //__TRACK_CACHE_FOOTPRINT__

								std::shared_ptr<Item> ptrItem = m_mpObjects[uidTemp];
								moveToFront(ptrItem);
								return CacheErrorCode::Success;
				*/
			}
#endif //__CONCURRENT__

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint += ptrItem->m_ptrObject->getMemoryFootprint();
#endif //__TRACK_CACHE_FOOTPRINT__

			m_mpObjects[ptrItem->m_uidSelf] = ptrItem;

			if (!m_ptrHeadOTA)
			{
				assert(m_ptrTailOTA == nullptr);
				m_ptrHeadOTA = ptrItem;
				m_ptrTailOTA = ptrItem;
			}
			else
			{
				ptrItem->m_ptrNext = m_ptrHeadOTA;
				m_ptrHeadOTA->m_ptrPrev = ptrItem;
				m_ptrHeadOTA = ptrItem;
			}

			m_nOTAItemsCount++;

#ifndef __CONCURRENT__
			flushItemsToStorage();
#endif //__CONCURRENT__

			return CacheErrorCode::Success;
		}

		return CacheErrorCode::Error;
	}

	// This method reorders the recently access objects. 
	// It is necessary to ensure that the objects are flushed in order otherwise a child object (data node) may preceed its parent (internal node).
	CacheErrorCode reorder(std::vector<std::pair<ObjectUIDType, ObjectTypePtr>>& vt, bool bEnsure = true)
	{
		// TODO: Need optimization.
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		while (vt.size() > 0)
		{
			std::pair<ObjectUIDType, ObjectTypePtr> prNode = vt.back();

			if (m_mpObjects.find(prNode.first) != m_mpObjects.end())
			{
				std::shared_ptr<Item> ptrItem = m_mpObjects[prNode.first];
				moveToFront_(ptrItem);	//TODO: How about passing whole list together and re-arrange the list?
			}
			else
			{
				if (bEnsure)
				{
					std::cout << "Critical State: One or many entries in the reorder-list is missing in the cache." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}
			}

			vt.pop_back();
		}

		return CacheErrorCode::Success;
	}

	CacheErrorCode reorderOpt(std::vector<std::pair<ObjectUIDType, ObjectTypePtr>>& vtObjects, bool bEnsure = true)
	{
		size_t _test = vtObjects.size();
		std::vector<std::shared_ptr<Item>> vtItems;

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		while (vtObjects.size() > 0)
		{
			std::pair<ObjectUIDType, ObjectTypePtr> prObject = vtObjects.back();

			if (m_mpObjects.find(prObject.first) != m_mpObjects.end())
			{
				vtItems.emplace_back(m_mpObjects[prObject.first]);
			}

			vtObjects.pop_back();
		}

		if (bEnsure)
		{
			assert(_test == vtItems.size());
		}
		if (vtItems.size() > 1)
			moveToFront(vtItems);
		else
			moveToFront(vtItems[0]);

		return CacheErrorCode::Success;
	}

	//	template <typename Type>
	//	CacheErrorCode getObjectOfType(const ObjectUIDType& uidObject, Type& ptrCoreObject, ObjectTypePtr& ptrStorageObject, std::optional<ObjectUIDType>& uidUpdated)
	//	{
	//#ifdef __CONCURRENT__
	//		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
	//#endif //__CONCURRENT__
	//
	//		if (m_mpObjects.find(uidObject) != m_mpObjects.end())
	//		{
	//			std::shared_ptr<Item> ptrItem = m_mpObjects[uidObject];
	//			moveToFront(ptrItem);
	//
	//			if (std::holds_alternative<Type>(ptrItem->m_ptrObject->getInnerData()))
	//			{
	//				ptrStorageObject = ptrItem->m_ptrObject;
	//				ptrCoreObject = std::get<Type>(ptrItem->m_ptrObject->getInnerData());
	//				return CacheErrorCode::Success;
	//			}
	//
	//			return CacheErrorCode::Error;
	//		}
	//
	//#ifdef __CONCURRENT__
	//		std::unique_lock<std::shared_mutex> lock_storage(m_mtxStorage);
	//		lock_cache.unlock();
	//#endif //__CONCURRENT__
	//
	//		const ObjectUIDType* uidTemp = &uidObject;
	//		if (m_mpUIDUpdates.find(uidObject) != m_mpUIDUpdates.end())
	//		{
	//#ifdef __CONCURRENT__
	//			std::optional< ObjectUIDType >& _condition = m_mpUIDUpdates[uidObject].first;
	//			m_cvUIDUpdates.wait(lock_storage, [&_condition] { return _condition != std::nullopt; });
	//#endif //__CONCURRENT__
	//
	//			uidUpdated = m_mpUIDUpdates[uidObject].first;
	//
	//#ifdef __VALIDITY_CHECK__
	//			assert(uidUpdated != std::nullopt);
	//#endif //__VALIDITY_CHECK__
	//
	//			m_mpUIDUpdates.erase(uidObject);
	//			uidTemp = &(*uidUpdated);
	//		}
	//
	//#ifdef __CONCURRENT__
	//		lock_storage.unlock();
	//#endif //__CONCURRENT__
	//
	//		ptrStorageObject = m_ptrStorage->getObject(*uidTemp);
	//
	//		if (ptrStorageObject != nullptr)
	//		{
	//#ifdef __CONCURRENT__
	//			std::unique_lock<std::shared_mutex> re_lock_cache(m_mtxCache);
	//
	//			if (m_mpObjects.find(*uidTemp) != m_mpObjects.end())
	//			{
	//				// TODO: case where other threads might were accessing the same node and added it to the cache.
	//				// but need to adjust memory_footprint.
	//#ifdef __TRACK_CACHE_FOOTPRINT__
	//				m_nCacheFootprint -= m_mpObjects[*uidTemp]->m_ptrObject->getMemoryFootprint();
	//
	//				assert(m_nCacheFootprint >= 0);
	//					
	//				m_nCacheFootprint += ptrStorageObject->getMemoryFootprint();
	//#endif //__TRACK_CACHE_FOOTPRINT__
	//
	//				std::shared_ptr<Item> ptrItem = m_mpObjects[*uidTemp];
	//				moveToFront(ptrItem);
	//
	//				if (std::holds_alternative<Type>(ptrItem->m_ptrObject->getInnerData()))
	//				{
	//					ptrCoreObject = std::get<Type>(ptrItem->m_ptrObject->getInnerData());
	//					return CacheErrorCode::Success;
	//				}
	//
	//				return CacheErrorCode::Error;
	//			}
	//#endif //__CONCURRENT__
	//			std::shared_ptr<Item> ptrItem = std::make_shared<Item>(*uidTemp, ptrStorageObject);
	//
	//			m_mpObjects[ptrItem->m_uidSelf] = ptrItem;
	//
	//#ifdef __TRACK_CACHE_FOOTPRINT__
	//			m_nCacheFootprint += ptrStorageObject->getMemoryFootprint();
	//#endif //__TRACK_CACHE_FOOTPRINT__
	//
	//			if (!m_ptrHeadOTA)
	//			{
	//				m_ptrHeadOTA = ptrItem;
	//				m_ptrTailOTA = ptrItem;
	//			}
	//			else
	//			{
	//				ptrItem->m_ptrNext = m_ptrHeadOTA;
	//				m_ptrHeadOTA->m_ptrPrev = ptrItem;
	//				m_ptrHeadOTA = ptrItem;
	//			}
	//
	//			if (std::holds_alternative<Type>(ptrItem->m_ptrObject->getInnerData()))
	//			{
	//				ptrCoreObject = std::get<Type>(ptrItem->m_ptrObject->getInnerData());
	//			}
	//
	//#ifndef __CONCURRENT__
	//			flushItemsToStorage();
	//#endif //__CONCURRENT__
	//
	//			return CacheErrorCode::Error;
	//		}
	//
	//		ptrCoreObject = nullptr;
	//		ptrStorageObject = nullptr;
	//		return CacheErrorCode::Error;
	//	}

	template<class Type, typename... ArgsType>
	CacheErrorCode createObjectOfType(std::optional<ObjectUIDType>& uidObject, const ArgsType... args)
	{
		std::shared_ptr<Type> ptrCoreObject = std::make_shared<Type>(args...);

		std::shared_ptr<ObjectType> ptrStorageObject = std::make_shared<ObjectType>(ptrCoreObject);

		ObjectUIDType uidTemp;
		ObjectUIDType::createAddressFromVolatilePointer(uidTemp, Type::UID, reinterpret_cast<uintptr_t>(ptrStorageObject.get()));

		uidObject = uidTemp;

		std::shared_ptr<Item> ptrItem = std::make_shared<Item>(*uidObject, ptrStorageObject);

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		if (m_mpObjects.find(*uidObject) != m_mpObjects.end())
		{
			std::cout << "Critical State: UID for a newly created object already exist in the cache." << std::endl;
			throw new std::logic_error(".....");   // TODO: critical log.

			std::shared_ptr<Item> ptrItem = m_mpObjects[*uidObject];
			ptrItem->m_ptrObject = ptrStorageObject;
			moveToFront(ptrItem);
		}
		else
		{
			m_mpObjects[ptrItem->m_uidSelf] = ptrItem;

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint += ptrStorageObject->getMemoryFootprint();
#endif //__TRACK_CACHE_FOOTPRINT__

			if (!m_ptrHeadOTA)
			{
				assert(m_ptrTailOTA == nullptr);
				m_ptrHeadOTA = ptrItem;
				m_ptrTailOTA = ptrItem;
			}
			else
			{
				ptrItem->m_ptrNext = m_ptrHeadOTA;
				m_ptrHeadOTA->m_ptrPrev = ptrItem;
				m_ptrHeadOTA = ptrItem;
			}

			m_nOTAItemsCount++;
		}

#ifndef __CONCURRENT__
		flushItemsToStorage();
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}

	template<class Type, typename... ArgsType>
	CacheErrorCode createObjectOfType(std::optional<ObjectUIDType>& uidObject, ObjectTypePtr& ptrStorageObject, const ArgsType... args)
	{
		ptrStorageObject = std::make_shared<ObjectType>(std::make_shared<Type>(args...));

		ObjectUIDType uidTemp;
		ObjectUIDType::createAddressFromVolatilePointer(uidTemp, Type::UID, reinterpret_cast<uintptr_t>(ptrStorageObject.get()));

		uidObject = uidTemp;

		std::shared_ptr<Item> ptrItem = std::make_shared<Item>(*uidObject, ptrStorageObject);

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		if (m_mpObjects.find(*uidObject) != m_mpObjects.end())
		{
			std::cout << "Critical State: UID for a newly created object already exist in the cache." << std::endl;
			throw new std::logic_error(".....");   // TODO: critical log.
			std::shared_ptr<Item> ptrItem = m_mpObjects[*uidObject];
			ptrItem->m_ptrObject = ptrStorageObject;
			moveToFront(ptrItem);
		}
		else
		{
			m_mpObjects[ptrItem->m_uidSelf] = ptrItem;

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint += ptrStorageObject->getMemoryFootprint();
#endif //__TRACK_CACHE_FOOTPRINT__

			if (!m_ptrHeadOTA)
			{
				assert(m_ptrTailOTA == nullptr);
				m_ptrHeadOTA = ptrItem;
				m_ptrTailOTA = ptrItem;
			}
			else
			{
				ptrItem->m_ptrNext = m_ptrHeadOTA;
				m_ptrHeadOTA->m_ptrPrev = ptrItem;
				m_ptrHeadOTA = ptrItem;
			}

			m_nOTAItemsCount++;
		}

#ifndef __CONCURRENT__
		flushItemsToStorage();
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}

	template<class Type, typename... ArgsType>
	CacheErrorCode createObjectOfType(std::optional<ObjectUIDType>& uidObject, std::shared_ptr<Type>& ptrCoreObject, const ArgsType... args)
	{
		ptrCoreObject = std::make_shared<Type>(args...);

		std::shared_ptr<ObjectType> ptrStorageObject = std::make_shared<ObjectType>(ptrCoreObject);

		uidObject = ObjectUIDType::createAddressFromVolatilePointer(Type::UID, reinterpret_cast<uintptr_t>(ptrStorageObject.get()));

		std::shared_ptr<Item> ptrItem = std::make_shared<Item>(*uidObject, ptrStorageObject);

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		if (m_mpObjects.find(*uidObject) != m_mpObjects.end())
		{
			std::cout << "Critical State: UID for a newly created object already exist in the cache." << std::endl;
			throw new std::logic_error(".....");   // TODO: critical log.
			std::shared_ptr<Item> ptrItem = m_mpObjects[*uidObject];
			ptrItem->m_ptrObject = ptrStorageObject;
			moveToFront(ptrItem);
		}
		else
		{
			m_mpObjects[&ptrItem->m_uidSelf] = ptrItem;

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint += ptrStorageObject->getMemoryFootprint();
#endif //__TRACK_CACHE_FOOTPRINT__

			if (!m_ptrHeadOTA)
			{
				assert(m_ptrTailOTA == nullptr);
				m_ptrHeadOTA = ptrItem;
				m_ptrTailOTA = ptrItem;
			}
			else
			{
				ptrItem->m_ptrNext = m_ptrHeadOTA;
				m_ptrHeadOTA->m_ptrPrev = ptrItem;
				m_ptrHeadOTA = ptrItem;
			}

			m_nOTAItemsCount++;
		}

#ifndef __CONCURRENT__
		flushItemsToStorage();
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}

	void getCacheState(size_t& nObjectsLinkedList, size_t& nObjectsInMap)
	{
		nObjectsLinkedList = 0;
		std::shared_ptr<Item> ptrItem = m_ptrHeadOTA;

		while (ptrItem != nullptr)
		{
			nObjectsLinkedList++;
			ptrItem = ptrItem->m_ptrNext;
		}

		ptrItem = m_ptrHeadMTA;

		while (ptrItem != nullptr)
		{
			nObjectsLinkedList++;
			ptrItem = ptrItem->m_ptrNext;
		}

		ptrItem = m_ptrHeadPF;

		while (ptrItem != nullptr)
		{
			nObjectsLinkedList++;
			ptrItem = ptrItem->m_ptrNext;
		}

		nObjectsInMap = m_mpObjects.size();
	}

	CacheErrorCode flush()
	{
		flushAllItemsToStorage();
		//presistCurrentCacheState();

		return CacheErrorCode::Success;
	}

private:
	void moveToTail(std::shared_ptr<Item> tail, std::shared_ptr<Item> nodeToMove)
	{
		if (tail == nullptr || nodeToMove == nullptr)
		{
			return;
		}

		if (nodeToMove->m_ptrPrev != nullptr)
		{
			nodeToMove->m_ptrPrev->m_ptrNext = nodeToMove->m_ptrNext;
		}
		else
		{
			tail = nodeToMove->m_ptrNext;
		}

		if (nodeToMove->m_ptrNext != nullptr)
		{
			nodeToMove->m_ptrNext->m_ptrPrev = nodeToMove->m_ptrPrev;
		}

		if (tail != nullptr)
		{
			tail->m_ptrNext = nodeToMove;
			nodeToMove->m_ptrPrev = tail;
			nodeToMove->m_ptrNext = nullptr;
			tail = nodeToMove;
		}
		else
		{
			tail = nodeToMove;
		}
	}

	void interchangeWithTail(std::shared_ptr<Item> currentNode) {
		if (currentNode == nullptr || currentNode == m_ptrTailOTA)
		{
			return;
		}

		if (currentNode->m_ptrPrev)
		{
			currentNode->m_ptrPrev->m_ptrNext = currentNode->m_ptrNext;
		}
		else
		{
			m_ptrHeadOTA = currentNode->m_ptrNext;
		}

		if (currentNode->m_ptrNext)
		{
			currentNode->m_ptrNext->m_ptrPrev = currentNode->m_ptrPrev;
		}

		currentNode->m_ptrPrev = m_ptrTailOTA;
		currentNode->m_ptrNext = nullptr;

		m_ptrTailOTA->m_ptrNext = currentNode;

		m_ptrTailOTA = currentNode;
	}

	void moveToFrontOfQ2(std::shared_ptr<Item> ptrItem)
	{
		if (ptrItem->m_qtLinkedQueue == QueueType::OTA)
		{
			if (ptrItem->m_ptrPrev)
			{
				ptrItem->m_ptrPrev->m_ptrNext = ptrItem->m_ptrNext;
			}

			if (ptrItem->m_ptrNext)
			{
				ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
			}

			if (ptrItem == m_ptrTailOTA)
			{
				m_ptrTailOTA = ptrItem->m_ptrPrev;
			}

			if (ptrItem == m_ptrHeadOTA)
			{
				m_ptrHeadOTA = ptrItem->m_ptrNext;
			}


			m_nMTAItemsCount++;
			m_nOTAItemsCount--;
		}
		else if (ptrItem->m_qtLinkedQueue == QueueType::PF)
		{
			if (ptrItem->m_ptrPrev)
			{
				ptrItem->m_ptrPrev->m_ptrNext = ptrItem->m_ptrNext;
			}

			if (ptrItem->m_ptrNext)
			{
				ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
			}

			if (ptrItem == m_ptrTailPF)
			{
				m_ptrTailPF = ptrItem->m_ptrPrev;
			}

			if (ptrItem == m_ptrHeadPF)
			{
				m_ptrHeadPF = ptrItem->m_ptrNext;
			}

			m_nMTAItemsCount++;
			m_nPFItemsCount--;
		}
		else if (ptrItem->m_qtLinkedQueue == QueueType::MTA)
		{
			moveToFront_(ptrItem);
			return;
		}


		// Insert ptrItem at the front of Q2
		ptrItem->m_ptrPrev = nullptr;
		ptrItem->m_ptrNext = nullptr;

		if (!m_ptrHeadMTA)
		{
			m_ptrHeadMTA = ptrItem;
			m_ptrTailMTA = ptrItem;
		}
		else
		{
			ptrItem->m_ptrNext = m_ptrHeadMTA;
			m_ptrHeadMTA->m_ptrPrev = ptrItem;
			m_ptrHeadMTA = ptrItem;
		}

		// Update queue type
		ptrItem->m_qtLinkedQueue = QueueType::MTA;
	}

	inline void moveToFront_(std::shared_ptr<Item> ptrItem)
	{
		//if (ptrItem->m_qtLinkedQueue == QueueType::PF)
		//{
		//	std::cout << "Critical State: Can't proceed with the flushItemsToStorage operations as an object is in use." << std::endl;
		//	throw new std::logic_error(".....");   // TODO: critical log.

		//}

		if (ptrItem->m_qtLinkedQueue == QueueType::OTA)
		{
			if (ptrItem == m_ptrHeadOTA)
			{
				return;
			}

			if (ptrItem->m_ptrPrev)
			{
				ptrItem->m_ptrPrev->m_ptrNext = ptrItem->m_ptrNext;
			}

			if (ptrItem->m_ptrNext)
			{
				ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
			}

			if (ptrItem == m_ptrTailOTA)
			{
				m_ptrTailOTA = ptrItem->m_ptrPrev;
			}

			ptrItem->m_ptrPrev = nullptr;
			ptrItem->m_ptrNext = m_ptrHeadOTA;

			if (m_ptrHeadOTA)
			{
				m_ptrHeadOTA->m_ptrPrev = ptrItem;
			}
			m_ptrHeadOTA = ptrItem;
		}
		else if (ptrItem->m_qtLinkedQueue == QueueType::MTA)
		{
			if (ptrItem == m_ptrHeadMTA)
			{
				return;
			}

			if (ptrItem->m_ptrPrev)
			{
				ptrItem->m_ptrPrev->m_ptrNext = ptrItem->m_ptrNext;
			}

			if (ptrItem->m_ptrNext)
			{
				ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
			}

			if (ptrItem == m_ptrTailMTA)
			{
				m_ptrTailMTA = ptrItem->m_ptrPrev;
			}

			ptrItem->m_ptrPrev = nullptr;
			ptrItem->m_ptrNext = m_ptrHeadMTA;

			if (m_ptrHeadMTA)
			{
				m_ptrHeadMTA->m_ptrPrev = ptrItem;
			}
			m_ptrHeadMTA = ptrItem;
		}
		else if (ptrItem->m_qtLinkedQueue == QueueType::PF)
		{
			if (ptrItem == m_ptrHeadPF)
			{
				return;
			}

			if (ptrItem->m_ptrPrev)
			{
				ptrItem->m_ptrPrev->m_ptrNext = ptrItem->m_ptrNext;
			}

			if (ptrItem->m_ptrNext)
			{
				ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
			}

			if (ptrItem == m_ptrTailPF)
			{
				m_ptrTailPF = ptrItem->m_ptrPrev;
			}

			ptrItem->m_ptrPrev = nullptr;
			ptrItem->m_ptrNext = m_ptrHeadPF;

			if (m_ptrHeadPF)
			{
				m_ptrHeadPF->m_ptrPrev = ptrItem;
			}
			m_ptrHeadPF = ptrItem;
		}
	}

	inline void moveToFront(std::shared_ptr<Item> ptrItem)
	{
		if (ptrItem->m_qtLinkedQueue == QueueType::MTA)
		{
			std::cout << "Critical State: Can't proceed with the flushItemsToStorage operations as an object is in use." << std::endl;
			throw new std::logic_error(".....");   // TODO: critical log.

		}

		if (ptrItem == m_ptrHeadOTA)
		{
			return;
		}

		if (ptrItem->m_ptrPrev)
		{
			ptrItem->m_ptrPrev->m_ptrNext = ptrItem->m_ptrNext;
		}

		if (ptrItem->m_ptrNext)
		{
			ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
		}

		if (ptrItem == m_ptrTailOTA)
		{
			m_ptrTailOTA = ptrItem->m_ptrPrev;
		}

		ptrItem->m_ptrPrev = nullptr;
		ptrItem->m_ptrNext = m_ptrHeadOTA;

		if (m_ptrHeadOTA)
		{
			m_ptrHeadOTA->m_ptrPrev = ptrItem;
		}
		m_ptrHeadOTA = ptrItem;
	}

	inline void moveToFront(const std::vector<std::shared_ptr<Item>>& itemList)
	{
		if (itemList.empty())
		{
			return;
		}

		// Step 1: Rearrange the items within the vector
		for (size_t i = 0; i < itemList.size(); i++)
		{
			if (i > 0 && itemList[i - 1] == itemList[i]) {
				//throw new std::logic_error("should not occur!");
				continue;
			}

			auto ptrItem = itemList[i];

			// Detach the item from its current neighbors in the linked list
			if (ptrItem->m_ptrPrev)
			{
				ptrItem->m_ptrPrev->m_ptrNext = ptrItem->m_ptrNext;
			}

			if (ptrItem->m_ptrNext)
			{
				ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
			}

			// If the item is the tail, update the tail pointer
			if (ptrItem == m_ptrTailOTA)
			{
				m_ptrTailOTA = ptrItem->m_ptrPrev;
				//m_ptrTailOTA->m_ptrNext = nullptr;
			}

			if (ptrItem == m_ptrHeadOTA)
			{
				m_ptrHeadOTA = ptrItem->m_ptrNext;
				m_ptrHeadOTA->m_ptrPrev = nullptr;
			}

			// Prepare the item for its new position
			ptrItem->m_ptrPrev = (i > 0) ? itemList[i - 1] : nullptr;  // Previous item in the list
			ptrItem->m_ptrNext = (i < itemList.size() - 1) ? itemList[i + 1] : nullptr;  // Next item in the list
		}

		// Step 2: Link the rearranged items to the front of the linked list

		auto firstItem = itemList.front();
		auto lastItem = itemList.back();

		firstItem->m_ptrPrev = nullptr;
		lastItem->m_ptrNext = m_ptrHeadOTA;

		// Connect the last item in the vector to the current head of the linked list
		if (m_ptrHeadOTA)
		{
			m_ptrHeadOTA->m_ptrPrev = lastItem;
		}

		//lastItem->m_ptrNext = m_ptrHeadOTA;
		m_ptrHeadOTA = firstItem;  // The first item becomes the new head

		// Step 3: If the list was empty, also set the tail
		if (!m_ptrTailOTA)
		{
			m_ptrTailOTA = lastItem;
		}
	}

	inline void removeFromLRU(std::shared_ptr<Item> ptrItem)
	{
		if (ptrItem->m_qtLinkedQueue == QueueType::OTA)
		{
			if (ptrItem->m_ptrPrev != nullptr)
			{
				ptrItem->m_ptrPrev->m_ptrNext = ptrItem->m_ptrNext;
			}
			else
			{
				m_ptrHeadOTA = ptrItem->m_ptrNext;
				if (m_ptrHeadOTA != nullptr)
				{
					m_ptrHeadOTA->m_ptrPrev = nullptr;
				}
			}

			if (ptrItem->m_ptrNext != nullptr)
			{
				ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
			}
			else
			{
				m_ptrTailOTA = ptrItem->m_ptrPrev;
				if (m_ptrTailOTA != nullptr)
				{
					m_ptrTailOTA->m_ptrNext = nullptr;
				}
			}

			m_nOTAItemsCount--;
		}
		else if (ptrItem->m_qtLinkedQueue == QueueType::MTA)
		{
			if (ptrItem->m_ptrPrev != nullptr)
			{
				ptrItem->m_ptrPrev->m_ptrNext = ptrItem->m_ptrNext;
			}
			else
			{
				m_ptrHeadMTA = ptrItem->m_ptrNext;
				if (m_ptrHeadMTA != nullptr)
				{
					m_ptrHeadMTA->m_ptrPrev = nullptr;
				}
			}

			if (ptrItem->m_ptrNext != nullptr)
			{
				ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
			}
			else
			{
				m_ptrTailMTA = ptrItem->m_ptrPrev;
				if (m_ptrTailMTA != nullptr)
				{
					m_ptrTailMTA->m_ptrNext = nullptr;
				}
			}
			m_nMTAItemsCount--;
		}
		else if (ptrItem->m_qtLinkedQueue == QueueType::PF)
		{
			if (ptrItem->m_ptrPrev != nullptr)
			{
				ptrItem->m_ptrPrev->m_ptrNext = ptrItem->m_ptrNext;
			}
			else
			{
				m_ptrHeadPF = ptrItem->m_ptrNext;
				if (m_ptrHeadPF != nullptr)
				{
					m_ptrHeadPF->m_ptrPrev = nullptr;
				}
			}

			if (ptrItem->m_ptrNext != nullptr)
			{
				ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
			}
			else
			{
				m_ptrTailPF = ptrItem->m_ptrPrev;
				if (m_ptrTailPF != nullptr)
				{
					m_ptrTailPF->m_ptrNext = nullptr;
				}
			}
			m_nPFItemsCount--;
		}
	}

	inline double calculateCacheUtility(size_t nCurrentQueueSize, size_t nOtherQueueSize) 
	{
		if (nCurrentQueueSize == 0)
			return -1;

		// Emergency components
		double E1 = log((double)m_nCacheCapacity / nCurrentQueueSize);
		double E2 = log((double)nCurrentQueueSize / nOtherQueueSize);
		return E1 + E2;
	}

	inline void flushItemsToStorage()
	{
#ifdef __CONCURRENT__
		std::vector<std::pair<ObjectUIDType, std::pair<std::optional<ObjectUIDType>, std::shared_ptr<ObjectType>>>> vtObjects;

		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);

#ifdef __TRACK_CACHE_FOOTPRINT__
		if (m_nCacheFootprint <= m_nCacheCapacity)
			return;

		while (m_nCacheFootprint >= m_nCacheCapacity)
#else //__TRACK_CACHE_FOOTPRINT__
		if (m_nOTAItemsCount + m_nMTAItemsCount <= m_nCacheCapacity)
			return;

		size_t nFlushCount = (m_nOTAItemsCount + m_nMTAItemsCount) - m_nCacheCapacity;
		for (size_t idx = 0; idx < nFlushCount; idx++)
		{
			double dOTAUtility = calculateCacheUtility(m_nOTAItemsCount, m_nMTAItemsCount);
			double dMTAUtility = calculateCacheUtility(m_nMTAItemsCount, m_nOTAItemsCount);

			std::shared_ptr<Item> ptrTemp = nullptr;

			if (dMTAUtility < 0 || (dOTAUtility >= 0 && dOTAUtility > dMTAUtility))
			{
				if (m_ptrTailOTA->m_ptrObject.use_count() > 1)
				{
					/* Info:
					 * Should proceed with the preceeding one?
					 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
					 */
					break;
				}

				// Check if the object is in use
				if (!m_ptrTailOTA->m_ptrObject->tryLockObject())
				{
					/* Info:
					 * Should proceed with the preceeding one?
					 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
					 */
					break;
				}
				else
				{
					m_ptrTailOTA->m_ptrObject->unlockObject();
				}

				ptrTemp = m_ptrTailOTA;

				m_ptrTailOTA = m_ptrTailOTA->m_ptrPrev;

				if (m_ptrTailOTA)
				{
					m_ptrTailOTA->m_ptrNext = nullptr;
				}
				else
				{
					assert(ptrTemp == m_ptrHeadOTA);

					m_ptrHeadOTA = nullptr;
				}

				m_nOTAItemsCount--;

				// Evict from once-accessed queue
				//auto victim = onceAccessedQueue.back();
				//onceAccessedQueue.pop_back();
				//onceAccessedMap.erase(victim->key);
				//addToGhostCache(victim);
			}
			else
			{
				//std::cout << "..going to flush.." << std::endl;
				if (m_ptrTailMTA->m_ptrObject.use_count() > 1)
				{
					/* Info:
					 * Should proceed with the preceeding one?
					 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
					 */
					break;
				}

				// Check if the object is in use
				if (!m_ptrTailMTA->m_ptrObject->tryLockObject())
				{
					/* Info:
					 * Should proceed with the preceeding one?
					 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
					 */
					break;
				}
				else
				{
					m_ptrTailMTA->m_ptrObject->unlockObject();
				}

				ptrTemp = m_ptrTailMTA;

				m_ptrTailMTA = m_ptrTailMTA->m_ptrPrev;

				if (m_ptrTailMTA)
				{
					m_ptrTailMTA->m_ptrNext = nullptr;
				}
				else
				{
					assert(ptrTemp == m_ptrHeadMTA);

					m_ptrHeadMTA = nullptr;
				}

				m_nMTAItemsCount--;

				// Evict from multi-accessed queue
				//auto victim = multiAccessedQueue.back();
				//multiAccessedQueue.pop_back();
				//multiAccessedMap.erase(victim->key);
				//addToGhostCache(victim);
			}
			ptrTemp->m_ptrPrev = nullptr;
			ptrTemp->m_ptrNext = nullptr;
			if (!m_ptrHeadPF)
			{
				m_ptrHeadPF = ptrTemp;
				m_ptrTailPF = ptrTemp;
			}
			else
			{
				ptrTemp->m_ptrNext = m_ptrHeadPF;
				m_ptrHeadPF->m_ptrPrev = ptrTemp;
				m_ptrHeadPF = ptrTemp;
			}

			ptrTemp->m_qtLinkedQueue = QueueType::PF;
			m_nPFItemsCount++;
		}

		if (m_nPFItemsCount < m_nPFCacheCapacity)
			return;

		nFlushCount = m_nPFItemsCount - m_nPFCacheCapacity;
		for (size_t idx = 0; idx < nFlushCount; idx++)
		/*
		if (m_mpObjects.size() <= m_nCacheCapacity)
			return;

		size_t nFlushCount = m_mpObjects.size() - m_nCacheCapacity;
		for (size_t idx = 0; idx < nFlushCount; idx++)
		*/
#endif //__TRACK_CACHE_FOOTPRINT__
		{
			//std::cout << "..going to flush.." << std::endl;
			if (m_ptrTailPF->m_ptrObject.use_count() > 1)
			{
				/* Info:
				 * Should proceed with the preceeding one?
				 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
				 */
				break;
			}

			// Check if the object is in use
			if (!m_ptrTailPF->m_ptrObject->tryLockObject())
			{
				/* Info:
				 * Should proceed with the preceeding one?
				 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
				 */
				break;
			}
			else
			{
				m_ptrTailPF->m_ptrObject->unlockObject();
			}

			std::shared_ptr<Item> ptrItemToFlush = m_ptrTailPF;

			vtObjects.push_back(std::make_pair(ptrItemToFlush->m_uidSelf, std::make_pair(std::nullopt, ptrItemToFlush->m_ptrObject)));

#ifdef __CACHE_COUNTERS__
		if (ptrItemToFlush->m_ptrObject->getDirtyFlag()) {
			m_nDirtyEvictions.fetch_add(1, std::memory_order_relaxed);
		}
		// All evictions (both dirty and clean) should increment the general eviction counter
		m_nEvictions.fetch_add(1, std::memory_order_relaxed);
#endif //__CACHE_COUNTERS__

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint -= ptrItemToFlush->m_ptrObject->getMemoryFootprint();
#endif //__TRACK_CACHE_FOOTPRINT__

			m_mpObjects.erase(ptrItemToFlush->m_uidSelf);

			m_ptrTailPF = ptrItemToFlush->m_ptrPrev;

			ptrItemToFlush->m_ptrPrev = nullptr;
			ptrItemToFlush->m_ptrNext = nullptr;

			if (m_ptrTailPF)
			{
				m_ptrTailPF->m_ptrNext = nullptr;
			}
			else
			{
				m_ptrHeadPF = nullptr;
			}

			ptrItemToFlush.reset();

			m_nPFItemsCount--;
		}

		std::unique_lock<std::shared_mutex> lock_storage(m_mtxStorage);

		lock_cache.unlock();

		if (m_mpUIDUpdates.size() > 0)
		{
			m_ptrCallback->applyExistingUpdates(vtObjects, m_mpUIDUpdates);
		}

		// TODO: ensure that no other thread should touch the storage related params..
		size_t nNewOffset = 0;

		m_ptrCallback->prepareFlush(vtObjects, m_ptrStorage->getNextAvailableBlockOffset(), nNewOffset, m_ptrStorage->getBlockSize(), m_ptrStorage->getStorageType());

		//m_ptrCallback->prepareFlush(vtObjects, nPos, m_ptrStorage->getBlockSize(), m_ptrStorage->getMediaType());

		for (auto itObject = vtObjects.begin(); itObject != vtObjects.end(); itObject++)
		{
			if ((*itObject).second.second.use_count() != 1)
			{
				std::cout << "Critical State: Can't proceed with the flushItemsToStorage operations as an object is in use." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

			if (m_mpUIDUpdates.find((*itObject).first) != m_mpUIDUpdates.end())
			{
				std::cout << "Critical State: Can't proceed with the flushItemsToStorage operations as object already exists in Updates' list." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}
			m_mpUIDUpdates[(*itObject).first] = std::make_pair(std::nullopt, (*itObject).second.second);
		}

		lock_storage.unlock();

		//std::cout << m_ptrStorage->getNextAvailableBlockOffset() << ", " <<  nNewOffset << "=" << (nNewOffset - m_ptrStorage->getNextAvailableBlockOffset())*m_ptrStorage->getBlockSize() << std::endl;
		m_ptrStorage->addObjects(vtObjects, nNewOffset);

		std::unique_lock<std::shared_mutex> relock_storage(m_mtxStorage);

		for (auto itObject = vtObjects.begin(); itObject != vtObjects.end(); itObject++)
		{
			if (m_mpUIDUpdates.find((*itObject).first) == m_mpUIDUpdates.end())
			{
				std::cout << "Critical State: (flushItemsToStorage) Object with similar key does not exists in the Updates' list." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

			m_mpUIDUpdates[(*itObject).first].first = (*itObject).second.first;
		}
		relock_storage.unlock();

		m_cvUIDUpdates.notify_all();

		vtObjects.clear();
#else //__CONCURRENT__

		while (m_nOTAItemsCount + m_nMTAItemsCount > m_nCacheCapacity)
		{
			double dOTAUtility = calculateCacheUtility(m_nOTAItemsCount, m_nMTAItemsCount);
			double dMTAUtility = calculateCacheUtility(m_nMTAItemsCount, m_nOTAItemsCount);

			std::shared_ptr<Item> ptrTemp = nullptr;

			if (dMTAUtility < 0 || (dOTAUtility >= 0 && dOTAUtility > dMTAUtility))
			{
				if (m_ptrTailOTA->m_ptrObject.use_count() > 1)
				{
					/* Info:
					 * Should proceed with the preceeding one?
					 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
					 */
					break;
				}

				ptrTemp = m_ptrTailOTA;

				m_ptrTailOTA = m_ptrTailOTA->m_ptrPrev;

				if (m_ptrTailOTA)
				{
					m_ptrTailOTA->m_ptrNext = nullptr;
				}
				else
				{
					assert(ptrTemp == m_ptrHeadOTA);

					m_ptrHeadOTA = nullptr;
				}

				m_nOTAItemsCount--;

				// Evict from once-accessed queue
				//auto victim = onceAccessedQueue.back();
				//onceAccessedQueue.pop_back();
				//onceAccessedMap.erase(victim->key);
				//addToGhostCache(victim);
			}
			else
			{
				if (m_ptrTailMTA->m_ptrObject.use_count() > 1)
				{
					/* Info:
					 * Should proceed with the preceeding one?
					 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
					 */
					break;
				}

				ptrTemp = m_ptrTailMTA;

				m_ptrTailMTA = m_ptrTailMTA->m_ptrPrev;

				if (m_ptrTailMTA)
				{
					m_ptrTailMTA->m_ptrNext = nullptr;
				}
				else
				{
					assert(ptrTemp == m_ptrHeadMTA);

					m_ptrHeadMTA = nullptr;
				}

				m_nMTAItemsCount--;

				// Evict from multi-accessed queue
				//auto victim = multiAccessedQueue.back();
				//multiAccessedQueue.pop_back();
				//multiAccessedMap.erase(victim->key);
				//addToGhostCache(victim);
			}
			ptrTemp->m_ptrPrev = nullptr;
			ptrTemp->m_ptrNext = nullptr;
			if (!m_ptrHeadPF)
			{
				m_ptrHeadPF = ptrTemp;
				m_ptrTailPF = ptrTemp;
			}
			else
			{
				ptrTemp->m_ptrNext = m_ptrHeadPF;
				m_ptrHeadPF->m_ptrPrev = ptrTemp;
				m_ptrHeadPF = ptrTemp;
			}

			ptrTemp->m_qtLinkedQueue = QueueType::PF;
			m_nPFItemsCount++;
		}

		while (m_nPFItemsCount > m_nPFCacheCapacity)
		{
			if (m_ptrTailPF->m_ptrObject.use_count() > 1)
			{
				/* Info:
				 * Should proceed with the preceeding one?
				 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
				 */
				break;
			}

			if (m_mpUIDUpdates.size() > 0)
			{
				m_ptrCallback->applyExistingUpdates(m_ptrTailPF->m_ptrObject, m_mpUIDUpdates);
			}

			if (m_ptrTailPF->m_ptrObject->getDirtyFlag())
			{

				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(m_ptrTailPF->m_uidSelf, m_ptrTailPF->m_ptrObject, uidUpdated) != CacheErrorCode::Success)
				{
					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				if (m_mpUIDUpdates.find(m_ptrTailPF->m_uidSelf) != m_mpUIDUpdates.end())
				{
					std::cout << "Critical State: Can't proceed with the flushItemsToStorage operations as object already exists in Updates' list." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				m_mpUIDUpdates[m_ptrTailPF->m_uidSelf] = std::make_pair(uidUpdated, m_ptrTailPF->m_ptrObject);
			}

#ifdef __CACHE_COUNTERS__
			if (m_ptrTailPF->m_ptrObject->getDirtyFlag()) {
				m_nDirtyEvictions.fetch_add(1, std::memory_order_relaxed);
			}
			// All evictions (both dirty and clean) should increment the general eviction counter
			m_nEvictions.fetch_add(1, std::memory_order_relaxed);
#endif //__CACHE_COUNTERS__

			m_mpObjects.erase(m_ptrTailPF->m_uidSelf);

			std::shared_ptr<Item> ptrTemp = m_ptrTailPF;

			m_ptrTailPF = m_ptrTailPF->m_ptrPrev;

			if (m_ptrTailPF)
			{
				m_ptrTailPF->m_ptrNext = nullptr;
			}
			else
			{
				assert(ptrTemp == m_ptrHeadPF);
				m_ptrHeadPF = nullptr;
			}

			m_nPFItemsCount--;

			ptrTemp.reset();
		}
#endif //__CONCURRENT__
	}

	inline void flushAllItemsToStorage()
	{
		std::vector<std::pair<ObjectUIDType, std::pair<std::optional<ObjectUIDType>, std::shared_ptr<ObjectType>>>> vtObjects;

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		while (m_nOTAItemsCount + m_nMTAItemsCount != 0)
		{
			double dOTAUtility = calculateCacheUtility(m_nOTAItemsCount, m_nMTAItemsCount);
			double dMTAUtility = calculateCacheUtility(m_nMTAItemsCount, m_nOTAItemsCount);

			std::shared_ptr<Item> ptrTemp = nullptr;

			if (dMTAUtility < 0 || (dOTAUtility >= 0 && dOTAUtility > dMTAUtility))
			{
				if (m_ptrTailOTA->m_ptrObject.use_count() > 1)
				{
					/* Info:
					 * Should proceed with the preceeding one?
					 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
					 */
					break;
				}

				// Check if the object is in use
				if (!m_ptrTailOTA->m_ptrObject->tryLockObject())
				{
					/* Info:
					 * Should proceed with the preceeding one?
					 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
					 */
					break;
				}
				else
				{
					m_ptrTailOTA->m_ptrObject->unlockObject();
				}

				ptrTemp = m_ptrTailOTA;

				m_ptrTailOTA = m_ptrTailOTA->m_ptrPrev;

				if (m_ptrTailOTA)
				{
					m_ptrTailOTA->m_ptrNext = nullptr;
				}
				else
				{
					assert(ptrTemp == m_ptrHeadOTA);

					m_ptrHeadOTA = nullptr;
				}

				m_nOTAItemsCount--;

				// Evict from once-accessed queue
				//auto victim = onceAccessedQueue.back();
				//onceAccessedQueue.pop_back();
				//onceAccessedMap.erase(victim->key);
				//addToGhostCache(victim);
			}
			else
			{
				//std::cout << "..going to flush.." << std::endl;
				if (m_ptrTailMTA->m_ptrObject.use_count() > 1)
				{
					/* Info:
					 * Should proceed with the preceeding one?
					 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
					 */
					break;
				}

				// Check if the object is in use
				if (!m_ptrTailMTA->m_ptrObject->tryLockObject())
				{
					/* Info:
					 * Should proceed with the preceeding one?
					 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
					 */
					break;
				}
				else
				{
					m_ptrTailMTA->m_ptrObject->unlockObject();
				}

				ptrTemp = m_ptrTailMTA;

				m_ptrTailMTA = m_ptrTailMTA->m_ptrPrev;

				if (m_ptrTailMTA)
				{
					m_ptrTailMTA->m_ptrNext = nullptr;
				}
				else
				{
					assert(ptrTemp == m_ptrHeadMTA);

					m_ptrHeadMTA = nullptr;
				}

				m_nMTAItemsCount--;

				// Evict from multi-accessed queue
				//auto victim = multiAccessedQueue.back();
				//multiAccessedQueue.pop_back();
				//multiAccessedMap.erase(victim->key);
				//addToGhostCache(victim);
			}
			ptrTemp->m_ptrPrev = nullptr;
			ptrTemp->m_ptrNext = nullptr;
			if (!m_ptrHeadPF)
			{
				m_ptrHeadPF = ptrTemp;
				m_ptrTailPF = ptrTemp;
			}
			else
			{
				ptrTemp->m_ptrNext = m_ptrHeadPF;
				m_ptrHeadPF->m_ptrPrev = ptrTemp;
				m_ptrHeadPF = ptrTemp;
			}

			ptrTemp->m_qtLinkedQueue = QueueType::PF;
			m_nPFItemsCount++;
		}

		for (uint32_t idx = 0, idxend = m_mpObjects.size(); idx < idxend; idx++)
		{
			if (m_ptrTailPF->m_ptrObject.use_count() > 1)
			{
				std::cout << "Critical State: Can't proceed with the flushAllItemsToStorage operations as an object is in use." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

			if (!m_ptrTailPF->m_ptrObject->tryLockObject())
			{
				std::cout << "Critical State: Can't proceed with the flushAllItemsToStorage operations as lock can't be acquired on object." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}
			else
			{
				m_ptrTailPF->m_ptrObject->unlockObject();
			}

			std::shared_ptr<Item> ptrItemToFlush = m_ptrTailPF;

			vtObjects.push_back(std::make_pair(ptrItemToFlush->m_uidSelf, std::make_pair(std::nullopt, ptrItemToFlush->m_ptrObject)));

#ifdef __CACHE_COUNTERS__
		if (ptrItemToFlush->m_ptrObject->getDirtyFlag()) {
			m_nDirtyEvictions.fetch_add(1, std::memory_order_relaxed);
		}
		// All evictions (both dirty and clean) should increment the general eviction counter
		m_nEvictions.fetch_add(1, std::memory_order_relaxed);
#endif //__CACHE_COUNTERS__

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint -= ptrItemToFlush->m_ptrObject->getMemoryFootprint();
#endif //__TRACK_CACHE_FOOTPRINT__

			m_mpObjects.erase(ptrItemToFlush->m_uidSelf);

			m_ptrTailPF = ptrItemToFlush->m_ptrPrev;

			ptrItemToFlush->m_ptrPrev = nullptr;
			ptrItemToFlush->m_ptrNext = nullptr;

			if (m_ptrTailPF)
			{
				m_ptrTailPF->m_ptrNext = nullptr;
			}
			else
			{
				m_ptrHeadPF = nullptr;
			}

			ptrItemToFlush.reset();
			m_nPFItemsCount--;
		}

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_storage(m_mtxStorage);

		lock_cache.unlock();
#endif //__CONCURRENT__

		if (m_mpUIDUpdates.size() > 0)
		{
			m_ptrCallback->applyExistingUpdates(vtObjects, m_mpUIDUpdates);
		}

		// TODO: ensure that no other thread should touch the storage related params..
		size_t nNewOffset = 0;

		m_ptrCallback->prepareFlush(vtObjects, m_ptrStorage->getNextAvailableBlockOffset(), nNewOffset, m_ptrStorage->getBlockSize(), m_ptrStorage->getStorageType());

		//m_ptrCallback->prepareFlush(vtObjects, nPos, m_ptrStorage->getBlockSize(), m_ptrStorage->getMediaType());

		for (auto itObject = vtObjects.begin(); itObject != vtObjects.end(); itObject++)
		{
			if ((*itObject).second.second.use_count() != 1)
			{
				std::cout << "Critical State: Can't proceed with the flushAllItemsToStorage operations as an object is in use." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

			if (m_mpUIDUpdates.find((*itObject).first) != m_mpUIDUpdates.end())
			{
				std::cout << "Critical State: Can't proceed with the flushAllItemsToStorage operations as object already exists in Updates' list." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

			m_mpUIDUpdates[(*itObject).first] = std::make_pair(std::nullopt, (*itObject).second.second);
		}

#ifdef __CONCURRENT__
		lock_storage.unlock();
#endif //__CONCURRENT__

		m_ptrStorage->addObjects(vtObjects, nNewOffset);

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> relock_storage(m_mtxStorage);
#endif //__CONCURRENT__

		for (auto itObject = vtObjects.begin(); itObject != vtObjects.end(); itObject++)
		{
			if (m_mpUIDUpdates.find((*itObject).first) == m_mpUIDUpdates.end())
			{
				std::cout << "Critical State: Can't proceed with the flushAllItemsToStorage operations as object does not exists in Updates' list." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

			m_mpUIDUpdates[(*itObject).first].first = (*itObject).second.first;
		}

#ifdef __CONCURRENT__
		relock_storage.unlock();

		m_cvUIDUpdates.notify_all();
#endif //__CONCURRENT__

		vtObjects.clear();
	}

	inline void flushDataItemsToStorage()
	{
		std::vector<std::pair<ObjectUIDType, std::pair<std::optional<ObjectUIDType>, std::shared_ptr<ObjectType>>>> vtObjects;

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		std::shared_ptr<Item> ptrItemToFlush = m_ptrTailOTA;

		for (uint32_t idx = 0, idxend = m_mpObjects.size(); idx < idxend; idx++)
		{
			if (ptrItemToFlush->m_ptrObject.use_count() > 1)
			{
				std::cout << "Critical State: Can't proceed with the flushDatatemsToStorage operations as an object is in use." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

			if (!ptrItemToFlush->m_ptrObject->tryLockObject())
			{
				std::cout << "Critical State: Can't proceed with the flushDataItemsToStorage operations as lock can't be acquired on object." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}
			else
			{
				ptrItemToFlush->m_ptrObject->unlockObject();
			}

			vtObjects.push_back(std::make_pair(ptrItemToFlush->m_uidSelf, std::make_pair(std::nullopt, ptrItemToFlush->m_ptrObject)));

#ifdef __CACHE_COUNTERS__
		if (ptrItemToFlush->m_ptrObject->getDirtyFlag()) {
			m_nDirtyEvictions.fetch_add(1, std::memory_order_relaxed);
		}
		// All evictions (both dirty and clean) should increment the general eviction counter
		m_nEvictions.fetch_add(1, std::memory_order_relaxed);
#endif //__CACHE_COUNTERS__

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint -= ptrItemToFlush->m_ptrObject->getMemoryFootprint();
#endif //__TRACK_CACHE_FOOTPRINT__

			auto objectType = ptrItemToFlush->m_uidSelf.getObjectType();

			if (objectType == 101)
			{
				ptrItemToFlush = ptrItemToFlush->m_ptrPrev;
			}
			else
			{
				std::shared_ptr<Item> ptrTemp = ptrItemToFlush->m_ptrPrev;

				m_mpObjects.erase(ptrItemToFlush->m_uidSelf);

				if (m_ptrTailOTA == ptrItemToFlush)
				{
					m_ptrTailOTA = ptrItemToFlush->m_ptrPrev;

					ptrItemToFlush->m_ptrPrev = nullptr;
					ptrItemToFlush->m_ptrNext = nullptr;

					if (m_ptrTailOTA)
					{
						m_ptrTailOTA->m_ptrNext = nullptr;
					}
					else
					{
						m_ptrHeadOTA = nullptr;
					}

					ptrItemToFlush.reset();
				}
				else
				{

					ptrItemToFlush->m_ptrNext->m_ptrPrev = ptrItemToFlush->m_ptrPrev;
					ptrItemToFlush->m_ptrPrev->m_ptrNext = ptrItemToFlush->m_ptrPrev;

					ptrItemToFlush.reset();
				}

				ptrItemToFlush = ptrTemp;
			}
		}

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_storage(m_mtxStorage);

		lock_cache.unlock();
#endif //__CONCURRENT__

		if (m_mpUIDUpdates.size() > 0)
		{
			m_ptrCallback->applyExistingUpdates(vtObjects, m_mpUIDUpdates);
		}

		// TODO: ensure that no other thread should touch the storage related params..
		size_t nNewOffset = 0;

		m_ptrCallback->prepareFlush(vtObjects, m_ptrStorage->getNextAvailableBlockOffset(), nNewOffset, m_ptrStorage->getBlockSize(), m_ptrStorage->getStorageType());

		//m_ptrCallback->prepareFlush(vtObjects, nPos, m_ptrStorage->getBlockSize(), m_ptrStorage->getMediaType());

		for (auto itObject = vtObjects.begin(); itObject != vtObjects.end(); itObject++)
		{
			if ((*itObject).second.second.use_count() != 1)
			{
				std::cout << "Critical State: Can't proceed with the flushDatatemsToStorage operations as an object is in use." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

			if (m_mpUIDUpdates.find((*itObject).first) != m_mpUIDUpdates.end())
			{
				std::cout << "Critical State: Can't proceed with the flushDataItemsToStorage operations as object already exists in Updates' list." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

			m_mpUIDUpdates[(*itObject).first] = std::make_pair(std::nullopt, (*itObject).second.second);
		}

#ifdef __CONCURRENT__
		lock_storage.unlock();
#endif //__CONCURRENT__

		m_ptrStorage->addObjects(vtObjects, nNewOffset);

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> relock_storage(m_mtxStorage);
#endif //__CONCURRENT__

		for (auto itObject = vtObjects.begin(); itObject != vtObjects.end(); itObject++)
		{
			if (m_mpUIDUpdates.find((*itObject).first) == m_mpUIDUpdates.end())
			{
				std::cout << "Critical State: Can't proceed with the flushDataItemsToStorage operations as object does not exists in Updates' list." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

			m_mpUIDUpdates[(*itObject).first].first = (*itObject).second.first;
		}

#ifdef __CONCURRENT__
		relock_storage.unlock();

		m_cvUIDUpdates.notify_all();
#endif //__CONCURRENT__

		vtObjects.clear();
	}

	inline void presistCurrentCacheState()
	{
#ifdef __CONCURRENT__
		std::vector<std::pair<ObjectUIDType, std::pair<std::optional<ObjectUIDType>, std::shared_ptr<ObjectType>>>> vtObjects;

		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);

		std::shared_ptr<Item> ptrItemToFlush = m_ptrTailOTA;

		for (uint32_t idx = 0, idxend = m_mpObjects.size(); idx < idxend; idx++)
		{
			if (ptrItemToFlush->m_ptrObject.use_count() > 1)
			{
				/* Info:
				 * Should proceed with the preceeding one?
				 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
				 */
				break;
			}

			// Check if the object is in use
			if (!ptrItemToFlush->m_ptrObject->tryLockObject())
			{
				/* Info:
				 * Should proceed with the preceeding one?
				 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
				 */
				break;
			}
			else
			{
				ptrItemToFlush->m_ptrObject->unlockObject();
			}

			vtObjects.push_back(std::make_pair(ptrItemToFlush->m_uidSelf, std::make_pair(std::nullopt, ptrItemToFlush->m_ptrObject)));

#ifdef __CACHE_COUNTERS__
		if (ptrItemToFlush->m_ptrObject->getDirtyFlag()) {
			m_nDirtyEvictions.fetch_add(1, std::memory_order_relaxed);
		}
		// All evictions (both dirty and clean) should increment the general eviction counter
		m_nEvictions.fetch_add(1, std::memory_order_relaxed);
#endif //__CACHE_COUNTERS__

			ptrItemToFlush = ptrItemToFlush->m_ptrPrev;
		}

		std::unique_lock<std::shared_mutex> lock_storage(m_mtxStorage);

		lock_cache.unlock();

		if (m_mpUIDUpdates.size() > 0)
		{
			m_ptrCallback->applyExistingUpdates(vtObjects, m_mpUIDUpdates);
		}

		// TODO: ensure that no other thread should touch the storage related params..
		size_t nNewOffset = 0;
		m_ptrCallback->prepareFlush(vtObjects, m_ptrStorage->getNextAvailableBlockOffset(), nNewOffset, m_ptrStorage->getBlockSize(), m_ptrStorage->getStorageType());

		//m_ptrCallback->prepareFlush(vtObjects, nPos, m_ptrStorage->getBlockSize(), m_ptrStorage->getMediaType());

		for (auto itObject = vtObjects.begin(); itObject != vtObjects.end(); itObject++)
		{
			if ((*itObject).second.second.use_count() != 2)
			{
				std::cout << "Critical State: Can't proceed with the presistCurrentCacheState operations as an object is in use." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

			if (m_mpUIDUpdates.find((*itObject).first) != m_mpUIDUpdates.end())
			{
				std::cout << "Critical State: Can't proceed with the presistCurrentCacheState operations as object already exists in Updates' list.." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}
			m_mpUIDUpdates[(*itObject).first] = std::make_pair(std::nullopt, (*itObject).second.second);
		}


		m_ptrStorage->addObjects(vtObjects, nNewOffset);

		for (auto itObject = vtObjects.begin(); itObject != vtObjects.end(); itObject++)
		{
			if (m_mpUIDUpdates.find((*itObject).first) == m_mpUIDUpdates.end())
			{
				std::cout << "Critical State: Can't proceed with the persistCurrentCacheStateoperations as object does not exist in Updates' list." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

			m_mpUIDUpdates[(*itObject).first].first = (*itObject).second.first;
		}
		lock_storage.unlock();

		m_cvUIDUpdates.notify_all();

		vtObjects.clear();
#else //__CONCURRENT__
		while (m_mpObjects.size() > m_nCacheCapacity)
		{
			if (m_ptrTailOTA->m_ptrObject.use_count() > 1)
			{
				/* Info:
				 * Should proceed with the preceeding one?
				 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
				 */
				break;
			}

			if (m_mpUIDUpdates.size() > 0)
			{
				m_ptrCallback->applyExistingUpdates(m_ptrTailOTA->m_ptrObject, m_mpUIDUpdates);
			}

			if (m_ptrTailOTA->m_ptrObject->getDirtyFlag())
			{

				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(m_ptrTailOTA->m_uidSelf, m_ptrTailOTA->m_ptrObject, uidUpdated) != CacheErrorCode::Success)
				{
					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				if (m_mpUIDUpdates.find(m_ptrTailOTA->m_uidSelf) != m_mpUIDUpdates.end())
				{
					std::cout << "Critical State: Recently add object to Storage doest not exist in Updates' list." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				m_mpUIDUpdates[m_ptrTailOTA->m_uidSelf] = std::make_pair(uidUpdated, m_ptrTailOTA->m_ptrObject);
			}

#ifdef __CACHE_COUNTERS__
			if (m_ptrTailOTA->m_ptrObject->getDirtyFlag()) {
				m_nDirtyEvictions.fetch_add(1, std::memory_order_relaxed);
			}
			// All evictions (both dirty and clean) should increment the general eviction counter
			m_nEvictions.fetch_add(1, std::memory_order_relaxed);
#endif //__CACHE_COUNTERS__

			m_mpObjects.erase(m_ptrTailOTA->m_uidSelf);

			std::shared_ptr<Item> ptrTemp = m_ptrTailOTA;

			m_ptrTailOTA = m_ptrTailOTA->m_ptrPrev;

			if (m_ptrTailOTA)
			{
				m_ptrTailOTA->m_ptrNext = nullptr;
			}
			else
			{
				m_ptrHeadOTA = nullptr;
			}

			ptrTemp.reset();
		}
#endif //__CONCURRENT__
	}

#ifdef __CONCURRENT__
	static void handlerCacheFlush(SelfType* ptrSelf)
	{
		do
		{
			//std::cout << "thread..." << std::endl;
			ptrSelf->flushItemsToStorage();

			std::this_thread::sleep_for(1ms);

		} while (!ptrSelf->m_bStop);
	}
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
public:
	void applyExistingUpdates(std::vector<std::pair<ObjectUIDType, std::pair<std::optional<ObjectUIDType>, std::shared_ptr<ObjectType>>>>& vtNodes
		, std::unordered_map<ObjectUIDType, std::pair<std::optional<ObjectUIDType>, std::shared_ptr<ObjectType>>>& mpUpdatedUIDs)
	{
	}

	void applyExistingUpdates(std::shared_ptr<ObjectType> ptrObject
		, std::unordered_map<ObjectUIDType, std::pair<std::optional<ObjectUIDType>, std::shared_ptr<ObjectType>>>& mpUpdatedUIDs)
	{
	}

	void prepareFlush(std::vector<std::pair<ObjectUIDType, std::pair<std::optional<ObjectUIDType>, std::shared_ptr<ObjectType>>>>& vtNodes
		, size_t nOffset, size_t& nNewOffset, size_t nBlockSize, ObjectUIDType::StorageMedia nMediaType)
	{
	}
#endif //__TREE_WITH_CACHE__

#ifdef __CACHE_COUNTERS__
public:
	// Cache counter access methods
	uint64_t getCacheHits() const {
		return m_nCacheHits.load(std::memory_order_relaxed);
	}
	
	uint64_t getCacheMisses() const {
		return m_nCacheMisses.load(std::memory_order_relaxed);
	}
	
	uint64_t getEvictions() const {
		return m_nEvictions.load(std::memory_order_relaxed);
	}
	
	uint64_t getDirtyEvictions() const {
		return m_nDirtyEvictions.load(std::memory_order_relaxed);
	}
	
	uint64_t getTotalEvictions() const {
		return m_nEvictions.load(std::memory_order_relaxed) + 
		       m_nDirtyEvictions.load(std::memory_order_relaxed);
	}
	
	double getCacheHitRatio() const {
		uint64_t hits = m_nCacheHits.load(std::memory_order_relaxed);
		uint64_t misses = m_nCacheMisses.load(std::memory_order_relaxed);
		uint64_t total = hits + misses;
		return total > 0 ? static_cast<double>(hits) / total : 0.0;
	}
	
	void resetCounters() {
		m_nCacheHits.store(0, std::memory_order_relaxed);
		m_nCacheMisses.store(0, std::memory_order_relaxed);
		m_nEvictions.store(0, std::memory_order_relaxed);
		m_nDirtyEvictions.store(0, std::memory_order_relaxed);
	}
#endif //__CACHE_COUNTERS__
};
