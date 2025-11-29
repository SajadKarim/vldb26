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
#include <unordered_set>
#define FLUSH_COUNT 100
#define MIN_CACHE_FOOTPRINT 1024 * 1024	// Safe check!

using namespace std::chrono_literals;

template <typename Traits>
class LRUCache
{
	typedef LRUCache<Traits> SelfType;

public:
	using ObjectType = typename Traits::ObjectType;
	using ObjectTypePtr = std::shared_ptr<ObjectType>;
	using ObjectUIDType = typename Traits::ObjectUIDType;

	using KeyType = typename Traits::KeyType;
	using ValueType = typename Traits::ValueType;

	using StorageType = typename Traits::StorageType;

	using WALType = typename Traits::WALType;

private:
	std::shared_ptr<ObjectType> m_ptrHead;
	std::shared_ptr<ObjectType> m_ptrTail;

	std::unique_ptr<StorageType> m_ptrStorage;

	int64_t m_nCacheCapacity;
	int64_t m_nUsedCacheCapacity;

	int64_t m_nCacheMiss;

	std::unique_ptr<WALType> m_ptrWAL;

	std::vector<ObjectTypePtr> m_vtPendingLRUUpdates;
	static constexpr size_t kLRUFlushThreshold = 5000;


	std::list<ObjectTypePtr> lruList;
	std::unordered_map<Key, std::list<ObjectTypePtr>::iterator> map;

#ifdef __CONCURRENT__
	bool m_bStop;

	std::thread m_threadCacheFlush;

	mutable std::shared_mutex m_mtxCache;
	mutable std::shared_mutex m_mtxStorage;
#endif //__CONCURRENT__

public:
	~LRUCache()
	{
#ifdef _DEBUG
		std::cout << "Total Cache Misses: " << m_nCacheMiss << std::endl;
#endif //_DEBUG

#ifdef __CONCURRENT__
		m_bStop = true;
		m_threadCacheFlush.join();
#endif //__CONCURRENT__

		//flush(false);

		m_ptrHead.reset();
		m_ptrTail.reset();;
		m_ptrStorage.reset();
	}

	template <typename... StorageArgs>
	LRUCache(size_t nCapacity, StorageArgs... args)
		: m_nCacheMiss(0)
		, m_nCacheCapacity(nCapacity)
		, m_nUsedCacheCapacity(0)
		, m_ptrHead(nullptr)
		, m_ptrTail(nullptr)
	{
		m_vtPendingLRUUpdates.reserve(kLRUFlushThreshold+100);
#ifdef __TRACK_CACHE_FOOTPRINT__
		m_nCacheCapacity = m_nCacheCapacity < MIN_CACHE_FOOTPRINT ? MIN_CACHE_FOOTPRINT : m_nCacheCapacity;
#endif //__TRACK_CACHE_FOOTPRINT__

		m_ptrStorage = std::make_unique<StorageType>(args...);
		
#ifdef __CONCURRENT__
		m_bStop = false;
		m_threadCacheFlush = std::thread(handlerCacheFlush, this);
#endif //__CONCURRENT__

		//m_ptrWAL = std::make_unique<WALType>(this, WAL_FILE_PATH);
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

	CacheErrorCode updateObjectsAccessMetadataorif(std::vector<ObjectTypePtr>& vtObjects)
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
		{
			if (*it == nullptr || (*it)->m_ptrCoreObject == nullptr)
			{
				continue;
			}

			moveToFront(*it);
		}

		vtObjects.clear();

#ifndef __CONCURRENT__
		flushItemsToStorage();
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}

	CacheErrorCode updateObjectsAccessMetadata123(std::vector<ObjectTypePtr>& vtObjects)
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif

		size_t i = 0;

		// Skip invalid entries
		for (; i < vtObjects.size(); ++i)
		{
			ObjectTypePtr& vecItem = vtObjects[i];
			if (vecItem && vecItem->m_ptrCoreObject)
			{
				moveToFront(vecItem);
				break;
			}
		}

		if (i == vtObjects.size())
			return CacheErrorCode::Success;

		ObjectTypePtr currentLruPtr = m_ptrHead;
		ObjectTypePtr prevPtr = nullptr;

		for (; i < vtObjects.size(); ++i)
		{
			ObjectTypePtr& vecItem = vtObjects[i];
			if (!vecItem || !vecItem->m_ptrCoreObject)
				continue;

			if (!currentLruPtr)
			{
				// Append to tail
				vecItem->m_ptrPrev = m_ptrTail;
				vecItem->m_ptrNext.reset();

				if (m_ptrTail)
					m_ptrTail->m_ptrNext = vecItem;

				m_ptrTail = vecItem;
				prevPtr = vecItem;
				continue;
			}

			if (vecItem == currentLruPtr)
			{
				prevPtr = currentLruPtr;
				currentLruPtr = currentLruPtr->m_ptrNext;
				continue;
			}

			// Unlink vecItem
			auto oldPrev = vecItem->m_ptrPrev.lock();
			if (oldPrev)
				oldPrev->m_ptrNext = vecItem->m_ptrNext;

			if (vecItem->m_ptrNext)
				vecItem->m_ptrNext->m_ptrPrev = vecItem->m_ptrPrev;

			if (vecItem == m_ptrTail)
				m_ptrTail = oldPrev;

			// Insert vecItem before currentLruPtr
			vecItem->m_ptrPrev = prevPtr;
			vecItem->m_ptrNext = currentLruPtr;

			if (prevPtr)
				prevPtr->m_ptrNext = vecItem;

			currentLruPtr->m_ptrPrev = vecItem;

			if (currentLruPtr == m_ptrHead)
				m_ptrHead = vecItem;

			prevPtr = vecItem;
		}

		// Update tail if necessary
		if (prevPtr && !prevPtr->m_ptrNext)
			m_ptrTail = prevPtr;

		vtObjects.clear();

#ifndef __CONCURRENT__
		flushItemsToStorage();
#endif

		return CacheErrorCode::Success;
	}

	CacheErrorCode updateObjectsAccessMetadata_2(std::vector<ObjectTypePtr>& vtObjects)
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		size_t i = 0;

		for (; i < vtObjects.size(); ++i)
		{
			ObjectTypePtr& vecItem = vtObjects[i];
			if (vecItem == nullptr || vecItem->m_ptrCoreObject == nullptr)
			{
				continue;
			}
			break;
		}

		ObjectTypePtr currentLruPtr = nullptr;
		ObjectTypePtr prevPtr = nullptr;

		// If LRU is empty, insert the first item from the vector
		if (!m_ptrHead)
		{
			ObjectTypePtr first = vtObjects[i];
			first->m_ptrPrev.reset();
			first->m_ptrNext.reset();
			m_ptrHead = first;
			m_ptrTail = first;

			currentLruPtr = nullptr;  // No next item yet
			prevPtr = first;
			++i; // Move to next vector item
		}
		else
		{
			currentLruPtr = m_ptrHead;
		}

		// Process remaining items
		for (; i < vtObjects.size(); ++i)
		{
			ObjectTypePtr& vecItem = vtObjects[i];
			if (vecItem == nullptr || vecItem->m_ptrCoreObject == nullptr)
			{
				continue;
			}

			if (!currentLruPtr)
			{
				// Reached end of LRU list; append item to tail
				vecItem->m_ptrPrev = m_ptrTail;
				vecItem->m_ptrNext.reset();

				if (m_ptrTail)
					m_ptrTail->m_ptrNext = vecItem;

				m_ptrTail = vecItem;
				prevPtr = vecItem;
				continue;
			}

			if (vecItem == currentLruPtr)
			{
				prevPtr = currentLruPtr;
				currentLruPtr = currentLruPtr->m_ptrNext;
				continue;
			}

			// Remove vecItem from its current position if already linked
			auto oldPrev = vecItem->m_ptrPrev.lock();
			if (oldPrev)
			{
				oldPrev->m_ptrNext = vecItem->m_ptrNext;
			}
			if (vecItem->m_ptrNext)
			{
				vecItem->m_ptrNext->m_ptrPrev = vecItem->m_ptrPrev;
			}

			// Update tail if this was the tail
			if (vecItem == m_ptrTail)
			{
				m_ptrTail = oldPrev;
			}

			// Insert vecItem before currentLru
			vecItem->m_ptrPrev = prevPtr;
			vecItem->m_ptrNext = currentLruPtr;

			if (prevPtr)
				prevPtr->m_ptrNext = vecItem;

			currentLruPtr->m_ptrPrev = vecItem;

			// Update head if inserting before it
			if (currentLruPtr == m_ptrHead)
			{
				m_ptrHead = vecItem;
			}

			prevPtr = vecItem;
		}

		// Update tail if needed
		if (prevPtr && !prevPtr->m_ptrNext)
		{
			if (m_ptrTail != prevPtr)
			{
				m_ptrTail = prevPtr;
			}
		}

		vtObjects.clear();

#ifndef __CONCURRENT__
		flushItemsToStorage();
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}

	CacheErrorCode updateObjectsAccessMetadata_imp(std::vector<ObjectTypePtr>& vtObjects)
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif

		ObjectTypePtr prev = nullptr;

		for (auto& obj : vtObjects)
		{
			if (!obj || !obj->m_ptrCoreObject)
				continue;

			// Unlink from current position
			auto oldPrev = obj->m_ptrPrev.lock();
			if (oldPrev)
				oldPrev->m_ptrNext = obj->m_ptrNext;
			if (obj->m_ptrNext)
				obj->m_ptrNext->m_ptrPrev = obj->m_ptrPrev;
			if (obj == m_ptrTail)
				m_ptrTail = oldPrev;

			// Relink in new position
			obj->m_ptrPrev = prev;
			obj->m_ptrNext.reset();

			if (prev)
				prev->m_ptrNext = obj;
			else
				m_ptrHead = obj;  // First item becomes new head

			prev = obj;
		}

		m_ptrTail = prev;  // Last item becomes tail

		vtObjects.clear();

#ifndef __CONCURRENT__
		flushItemsToStorage();
#endif

	/*	int hc = 0;
		ObjectTypePtr h = m_ptrHead;
		while (h != nullptr)
		{
			hc++;
			h = h->m_ptrNext;
		}
		int tc = 0;
		ObjectTypePtr t = m_ptrTail;
		while (t != nullptr)
		{
			tc++;
			t = t->m_ptrPrev.lock();
		}
		ASSERT(hc == tc);*/

		return CacheErrorCode::Success;
		}

	CacheErrorCode updateObjectsAccessMetadata(std::vector<ObjectTypePtr>& vtObjects, bool force = false)
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif

		// Append incoming updates to pending list
		for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); ++it)
		//for (auto& obj : vtObjects)
		{
			auto& obj = *it;
			if (obj && obj->m_ptrCoreObject) {
				obj->bseen = false;
				m_vtPendingLRUUpdates.push_back(obj);
			}
		}

		vtObjects.clear(); // optional, caller’s ownership is now transferred

		// Return early if we haven't reached threshold
		if (m_vtPendingLRUUpdates.size() < kLRUFlushThreshold && !force)
			return CacheErrorCode::Success;

		// Build unique, ordered list (latest access wins)
		//std::unordered_set<ObjectType*> seen;
		//std::unordered_map<ObjectType*, bool> seen;
		std::vector<ObjectTypePtr> uniqueList;
		uniqueList.reserve(m_vtPendingLRUUpdates.size());

	/*	for (auto it = m_vtPendingLRUUpdates.rbegin(); it != m_vtPendingLRUUpdates.rend(); ++it)
		{
			auto& obj = *it;
			if (!obj || !obj->m_ptrCoreObject)
				continue;

			if (seen.insert(obj.get()).second)
			{
				uniqueList.push_back(obj);
			}
		}*/

		ObjectTypePtr prev = nullptr;

		for (auto it = m_vtPendingLRUUpdates.rbegin(); it != m_vtPendingLRUUpdates.rend(); ++it)
		{
			auto& obj = *it;
			if (!obj || !obj->m_ptrCoreObject)
				continue;

			if (obj->bseen)
				continue;

				obj->bseen = true;
			/*if (seen[obj.get()])
			{
				continue;
			}

			seen[obj.get()] = true;*/

		//for (auto& obj : uniqueList)
		//{
			//if (!obj || !obj->m_ptrCoreObject)
			//	continue;

			// Unlink from current position
			auto oldPrev = obj->m_ptrPrev.lock();
			if (oldPrev)
				oldPrev->m_ptrNext = obj->m_ptrNext;
			if (obj->m_ptrNext)
				obj->m_ptrNext->m_ptrPrev = obj->m_ptrPrev;
			if (obj == m_ptrTail)
				m_ptrTail = oldPrev;

			// Relink in new position
			obj->m_ptrPrev = prev;
			obj->m_ptrNext.reset();

			if (prev)
				prev->m_ptrNext = obj;
			else
				m_ptrHead = obj;  // First item becomes new head

			prev = obj;
		}

		m_ptrTail = prev;  // Last item becomes tail

		uniqueList.clear();


		m_vtPendingLRUUpdates.clear();
#ifndef __CONCURRENT__
		flushItemsToStorage();
#endif
		//int hc = 0;
		//ObjectTypePtr h = m_ptrHead;
		//while (h != nullptr)
		//{
		//	hc++;
		//	h = h->m_ptrNext;
		//}
		//int tc = 0;
		//ObjectTypePtr t = m_ptrTail;
		//while (t != nullptr)
		//{
		//	tc++;
		//	t = t->m_ptrPrev.lock();
		//}
		//ASSERT(hc == tc);
		//ASSERT(hc == m_nUsedCacheCapacity);




		return CacheErrorCode::Success;
	}


	CacheErrorCode remove(const ObjectTypePtr ptrObject)
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		//ASSERT(ptrObject->m_uid.getMediaType() > 1);
		if (ptrObject->m_uid.getMediaType() > 1)
		{
			m_ptrStorage->remove(ptrObject->m_uid);
		}

		removeFromLRU(ptrObject);

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
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache); // TODO: think should this be moved to storage???
#endif //__CONCURRENT__


#ifdef _DEBUG
		m_nCacheMiss++;
#endif //_DEBUG


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
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache); // TODO: think should this be moved to storage???
#endif //__CONCURRENT__

#ifdef _DEBUG
		m_nCacheMiss++;
#endif //_DEBUG

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
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		m_nUsedCacheCapacity++;

		return CacheErrorCode::Success;
	}

	void getObjectsCountInCache(size_t& nObjectsLinkedList)
	{
		nObjectsLinkedList = 0;
		ObjectTypePtr ptrItem = m_ptrHead;

		while (ptrItem != nullptr)
		{
			nObjectsLinkedList++;
			ptrItem = ptrItem->m_ptrNext;
		} 
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

		/*if (m_vtPendingLRUUpdates.size() > 0)
		{
			std::vector<ObjectTypePtr> p;
			updateObjectsAccessMetadata(p, true);
		}*/
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
	inline void moveToFront(ObjectTypePtr ptrItem)
	{
		// Check for an empty list.
		if (!m_ptrHead)
		{
			m_ptrHead = ptrItem;
			m_ptrTail = ptrItem;
			return;
		}

		// If the node is already at the front, do nothing.
		if (ptrItem == m_ptrHead)
		{
			return;
		}

		// Detach ptrItem from its current position.
		if (auto prev = ptrItem->m_ptrPrev.lock())
		{
			prev->m_ptrNext = ptrItem->m_ptrNext;
		}
		if (ptrItem->m_ptrNext)
		{
			ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
		}
		if (ptrItem == m_ptrTail)
		{
			m_ptrTail = ptrItem->m_ptrPrev.lock();
		}

		// Insert ptrItem at the front.
		ptrItem->m_ptrPrev.reset();  // Clear previous pointer.
		ptrItem->m_ptrNext = m_ptrHead;
		m_ptrHead->m_ptrPrev = ptrItem;  // weak_ptr can be directly assigned from a shared_ptr.
		m_ptrHead = ptrItem;

	}

	inline void removeFromLRU(ObjectTypePtr ptrItem)
	{
		// Lock the weak pointer for previous node
		auto prev = ptrItem->m_ptrPrev.lock();
		auto next = ptrItem->m_ptrNext;

		// Case 1: Node is both head and tail (only element)
		if (ptrItem == m_ptrHead && ptrItem == m_ptrTail) 
		{
			m_ptrHead.reset();
			m_ptrTail.reset();
		}
		// Case 2: Node is head
		else if (ptrItem == m_ptrHead) 
		{
			m_ptrHead = next;
			if (next) 
			{
				next->m_ptrPrev.reset();
			}
		}
		// Case 3: Node is tail
		else if (ptrItem == m_ptrTail) 
		{
			m_ptrTail = prev;
			if (prev) 
			{
				prev->m_ptrNext.reset();
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
		ptrItem->m_ptrNext.reset();
		ptrItem->m_ptrPrev.reset();

		ptrItem->deleteCoreObject();
		ptrItem->m_uidUpdated = std::nullopt;
		ptrItem->m_uid = ObjectUIDType(INT64_MAX);

		ptrItem.reset();

		m_nUsedCacheCapacity--;
	}

	inline void flushItemsToStorage()
	{
#ifdef __CONCURRENT__
		std::vector<ObjectTypePtr> vtObjects;

		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);

		if (m_nUsedCacheCapacity <= m_nCacheCapacity)
			return;

		size_t nFlushCount = m_nUsedCacheCapacity - m_nCacheCapacity;
		for (size_t idx = 0; idx < nFlushCount; idx++)
		{
			if (m_ptrTail == nullptr || m_ptrTail.use_count() > 3)
			{
				break; 
			}

			if (!m_ptrTail->m_mtx.try_lock())
			{
				break;
			}

			ObjectTypePtr ptrItemToFlush = m_ptrTail;

			vtObjects.push_back(ptrItemToFlush);

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTail = m_ptrTail->m_ptrPrev.lock();

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTail)
			{
				m_ptrTail->m_ptrNext.reset();  // Optional: Explicitly reset next pointer
			}
			else
			{
				ASSERT(m_ptrTail == m_ptrHead);

				// The list had only one element, so update the head as well
				m_ptrHead.reset();
			}

			// Clear the removed node's pointers
			ptrItemToFlush->m_ptrPrev.reset();
			ptrItemToFlush->m_ptrNext.reset();

			m_nUsedCacheCapacity--;
		}

		lock_cache.unlock();

		for(auto itObject = vtObjects.begin(); itObject != vtObjects.end(); itObject++)
		{
			(*itObject)->m_uidUpdated = std::nullopt;
			if ((*itObject)->hasUpdatesToBeFlushed()) //check for uidupdated and uid in the cache object and reset uidupdated once used!!!
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject((*itObject)->m_uid, (*itObject), uidUpdated) != CacheErrorCode::Success)
				{
					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				(*itObject)->m_uidUpdated = uidUpdated;
			}

			(*itObject)->m_bDirty = false;
			(*itObject)->deleteCoreObject();
			(*itObject)->m_mtx.unlock();
			(*itObject).reset();
		}

		vtObjects.clear();
#else //__CONCURRENT__
		while (m_nUsedCacheCapacity > m_nCacheCapacity)
		{
			if (m_ptrTail == nullptr || m_ptrTail.use_count() > 3)
			{
				break;
			}

			if (m_ptrTail->hasUpdatesToBeFlushed()) //check for uidupdated and uid in the cache object and reset uidupdated once used!!!
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(m_ptrTail->m_uid, m_ptrTail, uidUpdated) != CacheErrorCode::Success)
				{
					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				m_ptrTail->m_uidUpdated = uidUpdated;
			}

			m_ptrTail->m_bDirty = false;
			ObjectTypePtr ptrTemp = m_ptrTail;

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTail = m_ptrTail->m_ptrPrev.lock();

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTail)
			{
				m_ptrTail->m_ptrNext.reset();  // Optional: Explicitly reset next pointer
			}
			else
			{
				// The list had only one element, so update the head as well
				m_ptrHead.reset();
			}

			// Clear the removed node's pointers
			ptrTemp->m_ptrPrev.reset();
			ptrTemp->m_ptrNext.reset();

			// Call custom deletion logic and release the pointer
			ptrTemp->deleteCoreObject();
			ptrTemp.reset();

			m_nUsedCacheCapacity--;
		}
#endif //__CONCURRENT__
	}

	inline void flushAllItemsToStorage()
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		while (m_nUsedCacheCapacity > 0)
		{
			if (m_ptrTail.use_count() > 3)
			{
				/* Info:
				 * Should proceed with the preceeding one?
				 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
				 */
				std::cout << "Critical State: Failed to add object to Storage." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

#ifdef __CONCURRENT__
			// Check if the object is in use
			if (!m_ptrTail->m_mtx.try_lock())
			{
				throw new std::logic_error("The object is still in use"); 
			}
#endif //__CONCURRENT__

			m_ptrTail->m_uidUpdated = std::nullopt;
			if (m_ptrTail->hasUpdatesToBeFlushed())
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(m_ptrTail->m_uid , m_ptrTail, uidUpdated) != CacheErrorCode::Success)
				{
					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				m_ptrTail->m_uidUpdated = uidUpdated;
			}

			m_ptrTail->m_bDirty = false;

			ObjectTypePtr ptrItemToFlush = m_ptrTail;

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTail = m_ptrTail->m_ptrPrev.lock();

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTail)
			{
				m_ptrTail->m_ptrNext.reset();  // Optional: Explicitly reset next pointer
			}
			else
			{
				// The list had only one element, so update the head as well
				m_ptrHead.reset();
			}

			// Clear the removed node's pointers
			ptrItemToFlush->m_ptrPrev.reset();
			ptrItemToFlush->m_ptrNext.reset();

			ptrItemToFlush->deleteCoreObject();

#ifdef __CONCURRENT__
			ptrItemToFlush->m_mtx.unlock();
#endif //__CONCURRENT__

			ptrItemToFlush.reset();

			m_nUsedCacheCapacity--;
		}

		ASSERT(m_nUsedCacheCapacity == 0);
	}

public:
	inline void persistAllItems()
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		ObjectTypePtr ptrItemToFlush = m_ptrTail;

		while (ptrItemToFlush != nullptr)
		{
			//if (m_ptrTail.use_count() > 3)
			//{
			//	/* Info:
			//	 * Should proceed with the preceeding one?
			//	 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
			//	 */
			//	std::cout << "Critical State: Failed to add object to Storage." << std::endl;
			//	throw new std::logic_error(".....");   // TODO: critical log.
			//}

#ifdef __CONCURRENT__
			// Check if the object is in use
			std::shared_lock<std::shared_mutex> lock_item(ptrItemToFlush->m_mtx);
			//if (!ptrItemToFlush->m_mtx.lock())
			//{
			//	throw new std::logic_error("The object is still in use");
			//}
#endif //__CONCURRENT__

			ptrItemToFlush->m_uidUpdated = std::nullopt;
			if (ptrItemToFlush->hasUpdatesToBeFlushed())
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(ptrItemToFlush->m_uid, ptrItemToFlush, uidUpdated, ptrItemToFlush->pendingFlag) != CacheErrorCode::Success)
				{
					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				ptrItemToFlush->m_uidUpdated = uidUpdated;
			}

			ptrItemToFlush->m_bDirty = false;

			//ObjectTypePtr ptrItemToFlush = m_ptrTail;

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			ptrItemToFlush = ptrItemToFlush->m_ptrPrev.lock();

			/*
			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTail)
			{
				m_ptrTail->m_ptrNext.reset();  // Optional: Explicitly reset next pointer
			}
			else
			{
				// The list had only one element, so update the head as well
				m_ptrHead.reset();
			}

			// Clear the removed node's pointers
			ptrItemToFlush->m_ptrPrev.reset();
			ptrItemToFlush->m_ptrNext.reset();

			ptrItemToFlush->deleteCoreObject();
			*/

#ifdef __CONCURRENT__
			ptrItemToFlush->m_mtx.unlock();
#endif //__CONCURRENT__

			/*
			ptrItemToFlush.reset();

			m_nUsedCacheCapacity--;
			*/
		}

		//ASSERT(m_nUsedCacheCapacity == 0);
	}

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
