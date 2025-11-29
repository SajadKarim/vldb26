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
class CLOCKCache : public ICallback
{
	typedef CLOCKCache<ICallback, StorageType> SelfType;

public:
	typedef StorageType::ObjectUIDType ObjectUIDType;
	typedef StorageType::ObjectType ObjectType;
	typedef std::shared_ptr<ObjectType> ObjectTypePtr;

private:
	struct Item
	{
	public:
		ObjectUIDType m_uidSelf;
		ObjectTypePtr m_ptrObject;
		bool m_bReferenceBit;  // CLOCK algorithm reference bit
		bool m_bValid;         // Item validity flag

		Item() : m_bReferenceBit(false), m_bValid(false) {}
		
		Item(const ObjectUIDType& uidObject, const ObjectTypePtr ptrObject)
			: m_bReferenceBit(true)  // Set reference bit when accessed
			, m_bValid(true)
		{
			m_uidSelf = uidObject;
			m_ptrObject = ptrObject;
		}

		~Item()
		{
			m_ptrObject.reset();
		}
		
		void reset()
		{
			m_ptrObject.reset();
			m_bReferenceBit = false;
			m_bValid = false;
		}
	};

	ICallback* m_ptrCallback;

	// Circular vector-based CLOCK implementation
	std::vector<Item> m_clockBuffer;
	size_t m_clockHand;              // Current position of clock hand
	size_t m_clockSize;              // Current number of items in clock
	
	std::unique_ptr<StorageType> m_ptrStorage;

	int64_t m_nCacheFootprint;
	int64_t m_nCacheCapacity;
	std::unordered_map<ObjectUIDType, size_t> m_mpObjects;  // Maps UID to clock buffer index
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
	~CLOCKCache()
	{

		
#ifdef __CONCURRENT__
		m_bStop = true;
		m_threadCacheFlush.join();
#endif //__CONCURRENT__

		//presistCurrentCacheState();
		flushAllItemsToStorage();

		// Clear circular buffer
		for (auto& item : m_clockBuffer) {
			item.reset();
		}
		m_clockBuffer.clear();
		
		m_ptrStorage.reset();
		m_mpObjects.clear();

		assert(m_nCacheFootprint == 0);
		

	}

	template <typename... StorageArgs>
	CLOCKCache(size_t nCapacity, StorageArgs... args)
		: m_nCacheCapacity(nCapacity)
		, m_nCacheFootprint(0)
		, m_clockHand(0)
		, m_clockSize(0)
	{
#ifdef __TRACK_CACHE_FOOTPRINT__
		m_nCacheCapacity = m_nCacheCapacity < MIN_CACHE_FOOTPRINT ? MIN_CACHE_FOOTPRINT : m_nCacheCapacity;
#endif //__TRACK_CACHE_FOOTPRINT__

		// Initialize circular buffer for CLOCK algorithm
		m_clockBuffer.resize(m_nCacheCapacity);
		
		m_ptrStorage = std::make_unique<StorageType>(args...);
		
		// Debug output for CLOCK cache initialization

		
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
			size_t index = it->second;

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint -= m_clockBuffer[index].m_ptrObject->getMemoryFootprint();
			assert(m_nCacheFootprint >= 0);
#endif //__TRACK_CACHE_FOOTPRINT__

			// Mark item as invalid in circular buffer
			m_clockBuffer[index].reset();
			m_clockSize--;
			
			m_mpObjects.erase(it);
			
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
			size_t index = m_mpObjects[uidObject];
			m_clockBuffer[index].m_bReferenceBit = true;  // Set reference bit for CLOCK algorithm
			ptrObject = m_clockBuffer[index].m_ptrObject;

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

#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> re_lock_cache(m_mtxCache);

			if (m_mpObjects.find(uidTemp) != m_mpObjects.end())
			{
				std::cout << "Some other thread has also accessed the object." << std::endl;
				throw new std::logic_error("...");
			}
#endif //__CONCURRENT__

			// Find a slot in the circular buffer
			size_t targetIndex = findAvailableSlot(uidTemp, ptrObject);
			
			// Check if we couldn't find a slot (all objects in use)
			if (targetIndex == SIZE_MAX) {
				std::cout << "Warning: Cannot add object to cache - all slots in use. Returning error." << std::endl;
				return CacheErrorCode::Error;
			}

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint += ptrObject->getMemoryFootprint();
#endif //__TRACK_CACHE_FOOTPRINT__

			// Add to circular buffer and map
			m_clockBuffer[targetIndex] = Item(uidTemp, ptrObject);
			m_mpObjects[uidTemp] = targetIndex;
			m_clockSize++;

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
				size_t index = m_mpObjects[prNode.first];
				m_clockBuffer[index].m_bReferenceBit = true;  // Set reference bit for CLOCK algorithm
			}
			else
			{
				if (bEnsure)
				{
					std::cout << "Warning: Entry in reorder-list is missing in the cache. This may be due to eviction." << std::endl;
					// Don't throw error - this can happen when objects are evicted between reorder calls
					// throw new std::logic_error(".....");   // TODO: critical log.
				}
			}

			vt.pop_back();
		}

		return CacheErrorCode::Success;
	}

	CacheErrorCode reorderOpt(std::vector<std::pair<ObjectUIDType, ObjectTypePtr>>& vtObjects, bool bEnsure = true)
	{
		size_t _test = vtObjects.size();
		std::vector<size_t> vtIndices;

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		while (vtObjects.size() > 0)
		{
			std::pair<ObjectUIDType, ObjectTypePtr> prObject = vtObjects.back();

			if (m_mpObjects.find(prObject.first) != m_mpObjects.end())
			{
				vtIndices.emplace_back(m_mpObjects[prObject.first]);
			}

			vtObjects.pop_back();
		}

		if (bEnsure)
		{
			assert(_test == vtIndices.size());
		}
		// Set reference bits for all items in CLOCK algorithm
		for (size_t index : vtIndices)
		{
			m_clockBuffer[index].m_bReferenceBit = true;
		}

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
//			if (!m_ptrHead)
//			{
//				m_ptrHead = ptrItem;
//				m_ptrTail = ptrItem;
//			}
//			else
//			{
//				ptrItem->m_ptrNext = m_ptrHead;
//				m_ptrHead->m_ptrPrev = ptrItem;
//				m_ptrHead = ptrItem;
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

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		if (m_mpObjects.find(*uidObject) != m_mpObjects.end())
		{
			std::cout << "Critical State: UID for a newly created object already exist in the cache." << std::endl;
			throw new std::logic_error(".....");   // TODO: critical log.

			size_t index = m_mpObjects[*uidObject];
			m_clockBuffer[index].m_ptrObject = ptrStorageObject;
			m_clockBuffer[index].m_bReferenceBit = true;  // Set reference bit for CLOCK algorithm
		}
		else
		{
			// Find a slot in the circular buffer
			size_t targetIndex = findAvailableSlot(*uidObject, ptrStorageObject);
			
			// Check if we couldn't find a slot (all objects in use)
			if (targetIndex == SIZE_MAX) {
				std::cout << "Warning: Cannot add object to cache - all slots in use. Returning error." << std::endl;
				return CacheErrorCode::Error;
			}

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint += ptrStorageObject->getMemoryFootprint();
#endif //__TRACK_CACHE_FOOTPRINT__

			// Add to circular buffer and map
			m_clockBuffer[targetIndex] = Item(*uidObject, ptrStorageObject);
			m_mpObjects[*uidObject] = targetIndex;
			m_clockSize++;
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

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		if (m_mpObjects.find(*uidObject) != m_mpObjects.end())
		{
			std::cout << "Critical State: UID for a newly created object already exist in the cache." << std::endl;
			throw new std::logic_error(".....");   // TODO: critical log.
			size_t index = m_mpObjects[*uidObject];
			m_clockBuffer[index].m_ptrObject = ptrStorageObject;
			m_clockBuffer[index].m_bReferenceBit = true;  // Set reference bit for CLOCK algorithm
		}
		else
		{
			// Find a slot in the circular buffer
			size_t targetIndex = findAvailableSlot(*uidObject, ptrStorageObject);
			
			// Check if we couldn't find a slot (all objects in use)
			if (targetIndex == SIZE_MAX) {
				std::cout << "Warning: Cannot add object to cache - all slots in use. Returning error." << std::endl;
				return CacheErrorCode::Error;
			}

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint += ptrStorageObject->getMemoryFootprint();
#endif //__TRACK_CACHE_FOOTPRINT__

			// Add to circular buffer and map
			m_clockBuffer[targetIndex] = Item(*uidObject, ptrStorageObject);
			m_mpObjects[*uidObject] = targetIndex;
			m_clockSize++;
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

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		if (m_mpObjects.find(*uidObject) != m_mpObjects.end())
		{
			std::cout << "Critical State: UID for a newly created object already exist in the cache." << std::endl;
			throw new std::logic_error(".....");   // TODO: critical log.
			size_t index = m_mpObjects[*uidObject];
			m_clockBuffer[index].m_ptrObject = ptrStorageObject;
			m_clockBuffer[index].m_bReferenceBit = true;  // Set reference bit for CLOCK algorithm
		}
		else
		{
			// Find a slot in the circular buffer
			size_t targetIndex = findAvailableSlot(*uidObject, ptrStorageObject);
			
			// Check if we couldn't find a slot (all objects in use)
			if (targetIndex == SIZE_MAX) {
				std::cout << "Warning: Cannot add object to cache - all slots in use. Returning error." << std::endl;
				return CacheErrorCode::Error;
			}

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint += ptrStorageObject->getMemoryFootprint();
#endif //__TRACK_CACHE_FOOTPRINT__

			// Add to circular buffer and map
			m_clockBuffer[targetIndex] = Item(*uidObject, ptrStorageObject);
			m_mpObjects[*uidObject] = targetIndex;
			m_clockSize++;
		}

#ifndef __CONCURRENT__
		flushItemsToStorage();
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}

	void getCacheState(size_t& nObjectsLinkedList, size_t& nObjectsInMap)
	{
		nObjectsLinkedList = m_clockSize;  // Number of valid items in circular buffer
		nObjectsInMap = m_mpObjects.size();
	}

	CacheErrorCode flush()
	{
		flushAllItemsToStorage();
		//presistCurrentCacheState();

		return CacheErrorCode::Success;
	}

private:


	// Find an available slot in the circular buffer using CLOCK algorithm
	inline size_t findAvailableSlot(const ObjectUIDType& uidObject, const ObjectTypePtr& ptrObject)
	{
		static int evictionCount = 0;
		
		// If cache is not full, find first empty slot
		if (m_clockSize < m_nCacheCapacity) {
			for (size_t i = 0; i < m_nCacheCapacity; i++) {
				if (!m_clockBuffer[i].m_bValid) {
					return i;
				}
			}
		}
		

		
		// Cache is full, use CLOCK algorithm to find victim
		evictionCount++;

		
		size_t startHand = m_clockHand;
		int sweepCount = 0;
		
		// Sweep through the circular buffer looking for a victim
		// First pass: look for objects with reference bit = 0
		do {
			sweepCount++;
			
			// Check if current slot is valid and has reference bit = 0
			if (m_clockBuffer[m_clockHand].m_bValid && !m_clockBuffer[m_clockHand].m_bReferenceBit) {
				// Check if object is in use before evicting
				if (m_clockBuffer[m_clockHand].m_ptrObject.use_count() > 1) {
					// Object is in use, move to next
					m_clockHand = (m_clockHand + 1) % m_nCacheCapacity;
					continue;
				}
				
				// Check if object can be locked
				if (!m_clockBuffer[m_clockHand].m_ptrObject->tryLockObject()) {
					// Object can't be locked, move to next
					m_clockHand = (m_clockHand + 1) % m_nCacheCapacity;
					continue;
				}
				
				// Found victim - evict this item
				size_t victimIndex = m_clockHand;
				
				// Apply any existing updates before flushing
				if (m_mpUIDUpdates.size() > 0) {
					m_ptrCallback->applyExistingUpdates(m_clockBuffer[victimIndex].m_ptrObject, m_mpUIDUpdates);
				}
				
				// Flush to storage if dirty before evicting
				if (m_clockBuffer[victimIndex].m_ptrObject->getDirtyFlag()) {
#ifdef __CACHE_COUNTERS__
					m_nDirtyEvictions.fetch_add(1, std::memory_order_relaxed);
#endif //__CACHE_COUNTERS__

					ObjectUIDType uidUpdated;
					if (m_ptrStorage->addObject(m_clockBuffer[victimIndex].m_uidSelf, m_clockBuffer[victimIndex].m_ptrObject, uidUpdated) != CacheErrorCode::Success) {
						std::cout << "Critical State: Failed to add object to Storage during eviction." << std::endl;
						throw new std::logic_error("Failed to flush object to storage during eviction");
					}
					
					// Check if object already exists in updates list
					if (m_mpUIDUpdates.find(m_clockBuffer[victimIndex].m_uidSelf) != m_mpUIDUpdates.end()) {
						std::cout << "Critical State: Can't proceed with eviction as object already exists in Updates' list." << std::endl;
						throw new std::logic_error("Object already exists in updates list during eviction");
					}
					
					// Store the updated UID mapping for future lookups
					m_mpUIDUpdates[m_clockBuffer[victimIndex].m_uidSelf] = std::make_pair(uidUpdated, m_clockBuffer[victimIndex].m_ptrObject);
				} else {
#ifdef __CACHE_COUNTERS__
					m_nEvictions.fetch_add(1, std::memory_order_relaxed);
#endif //__CACHE_COUNTERS__
				}
				
				// Remove from objects map
				m_mpObjects.erase(m_clockBuffer[victimIndex].m_uidSelf);
				
#ifdef __TRACK_CACHE_FOOTPRINT__
				m_nCacheFootprint -= m_clockBuffer[victimIndex].m_ptrObject->getMemoryFootprint();
				assert(m_nCacheFootprint >= 0);
#endif //__TRACK_CACHE_FOOTPRINT__
				
				// Unlock the object after processing
				m_clockBuffer[victimIndex].m_ptrObject->unlockObject();
				
				// Reset the slot to make it available
				m_clockBuffer[victimIndex].reset();
				m_clockSize--;
				
				// Move clock hand to next position
				m_clockHand = (m_clockHand + 1) % m_nCacheCapacity;
				

				
				return victimIndex;
			}
			else if (m_clockBuffer[m_clockHand].m_bValid) {
				// Clear reference bit and move to next
				m_clockBuffer[m_clockHand].m_bReferenceBit = false;
			}
			
			// Move clock hand to next position
			m_clockHand = (m_clockHand + 1) % m_nCacheCapacity;
			
		} while (m_clockHand != startHand);
		
		// Second pass: clear all reference bits and look for any evictable object
		startHand = m_clockHand;
		do {
			sweepCount++;
			
			if (m_clockBuffer[m_clockHand].m_bValid) {
				// Clear reference bit (second chance)
				m_clockBuffer[m_clockHand].m_bReferenceBit = false;
				
				// Check if object is in use before evicting
				if (m_clockBuffer[m_clockHand].m_ptrObject.use_count() <= 1) {
					// Check if object can be locked
					if (m_clockBuffer[m_clockHand].m_ptrObject->tryLockObject()) {
						// Found victim - evict this item
						size_t victimIndex = m_clockHand;
						
						// Apply any existing updates before flushing
						if (m_mpUIDUpdates.size() > 0) {
							m_ptrCallback->applyExistingUpdates(m_clockBuffer[victimIndex].m_ptrObject, m_mpUIDUpdates);
						}
						
						// Flush to storage if dirty before evicting
						if (m_clockBuffer[victimIndex].m_ptrObject->getDirtyFlag()) {
#ifdef __CACHE_COUNTERS__
							m_nDirtyEvictions.fetch_add(1, std::memory_order_relaxed);
#endif //__CACHE_COUNTERS__

							ObjectUIDType uidUpdated;
							if (m_ptrStorage->addObject(m_clockBuffer[victimIndex].m_uidSelf, m_clockBuffer[victimIndex].m_ptrObject, uidUpdated) != CacheErrorCode::Success) {
								std::cout << "Critical State: Failed to add object to Storage during second pass eviction." << std::endl;
								throw new std::logic_error("Failed to flush object to storage during second pass eviction");
							}
							
							// Check if object already exists in updates list
							if (m_mpUIDUpdates.find(m_clockBuffer[victimIndex].m_uidSelf) != m_mpUIDUpdates.end()) {
								std::cout << "Critical State: Can't proceed with second pass eviction as object already exists in Updates' list." << std::endl;
								throw new std::logic_error("Object already exists in updates list during second pass eviction");
							}
							
							// Store the updated UID mapping for future lookups
							m_mpUIDUpdates[m_clockBuffer[victimIndex].m_uidSelf] = std::make_pair(uidUpdated, m_clockBuffer[victimIndex].m_ptrObject);
						} else {
#ifdef __CACHE_COUNTERS__
							m_nEvictions.fetch_add(1, std::memory_order_relaxed);
#endif //__CACHE_COUNTERS__
						}
						
						// Remove from objects map
						m_mpObjects.erase(m_clockBuffer[victimIndex].m_uidSelf);
						
		#ifdef __TRACK_CACHE_FOOTPRINT__
						m_nCacheFootprint -= m_clockBuffer[victimIndex].m_ptrObject->getMemoryFootprint();
						assert(m_nCacheFootprint >= 0);
		#endif //__TRACK_CACHE_FOOTPRINT__
						
						// Unlock the object after processing
						m_clockBuffer[victimIndex].m_ptrObject->unlockObject();
						
						// Reset the slot to make it available
						m_clockBuffer[victimIndex].reset();
						m_clockSize--;
						
						// Move clock hand to next position
						m_clockHand = (m_clockHand + 1) % m_nCacheCapacity;
						
						return victimIndex;
					}
				}
			}
			
			// Move clock hand to next position
			m_clockHand = (m_clockHand + 1) % m_nCacheCapacity;
			
		} while (m_clockHand != startHand);
		
		// If we still can't find a victim, all objects are in use
		std::cout << "Warning: All objects have reference bits set and victim is in use. Cache will grow beyond capacity." << std::endl;
		return SIZE_MAX; // Indicate failure to find victim
	}

	// Find victim for eviction using CLOCK algorithm (for flushing)
	inline size_t findVictimForEviction()
	{
		if (m_clockSize == 0) {
			return SIZE_MAX; // No items to evict
		}
		
		size_t startHand = m_clockHand;
		
		// Sweep through the circular buffer looking for a victim
		do {
			// Check if current slot is valid, has reference bit = 0, and is not in use
			if (m_clockBuffer[m_clockHand].m_bValid && 
				!m_clockBuffer[m_clockHand].m_bReferenceBit &&
				m_clockBuffer[m_clockHand].m_ptrObject.use_count() == 1) {
				// Found victim - return this index (don't evict yet, just identify)
				size_t victimIndex = m_clockHand;
				// Move clock hand to next position
				m_clockHand = (m_clockHand + 1) % m_nCacheCapacity;
				return victimIndex;
			}
			else if (m_clockBuffer[m_clockHand].m_bValid) {
				// Clear reference bit and move to next
				m_clockBuffer[m_clockHand].m_bReferenceBit = false;
			}
			
			// Move clock hand to next position
			m_clockHand = (m_clockHand + 1) % m_nCacheCapacity;
			
		} while (m_clockHand != startHand);
		
		// Second pass: if all items have reference bit set, look for any item not in use
		startHand = m_clockHand;
		do {
			if (m_clockBuffer[m_clockHand].m_bValid && 
				m_clockBuffer[m_clockHand].m_ptrObject.use_count() == 1) {
				size_t victimIndex = m_clockHand;
				m_clockHand = (m_clockHand + 1) % m_nCacheCapacity;
				return victimIndex;
			}
			
			// Move clock hand to next position
			m_clockHand = (m_clockHand + 1) % m_nCacheCapacity;
			
		} while (m_clockHand != startHand);
		
		return SIZE_MAX; // No valid items found
	}





	inline void flushItemsToStorage()
	{
#ifdef __CONCURRENT__
		std::vector<std::pair<ObjectUIDType, std::pair<std::optional<ObjectUIDType>, std::shared_ptr<ObjectType>>>> vtObjects;

		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);

#ifdef __TRACK_CACHE_FOOTPRINT__
		if (m_nCacheFootprint <= m_nCacheCapacity)
			return;

		while( m_nCacheFootprint >= m_nCacheCapacity)
#else //__TRACK_CACHE_FOOTPRINT__
		if (m_mpObjects.size() <= m_nCacheCapacity)
			return;

		size_t nFlushCount = m_mpObjects.size() - m_nCacheCapacity;
		for (size_t idx = 0; idx < nFlushCount; idx++)
#endif //__TRACK_CACHE_FOOTPRINT__
		{
			//std::cout << "..going to flush.." << std::endl;
			size_t victimIndex = findVictimForEviction();
			
			if (victimIndex == SIZE_MAX)
			{
				// No evictable victim found, cache will exceed capacity
				break;
			}
			
			Item& itemToFlush = m_clockBuffer[victimIndex];
			
			// findVictimForEviction() already ensures use_count == 1, so no need to check again
			// But we still need to check if we can lock it for concurrent access

			// Check if the object is in use
			if (!itemToFlush.m_ptrObject->tryLockObject())
			{
				/* Info:
				 * This shouldn't happen if findVictimForEviction() is working correctly,
				 * but handle it gracefully to avoid infinite loops
				 */
				break;
			}
			else
			{
				itemToFlush.m_ptrObject->unlockObject();
			}

			vtObjects.push_back(std::make_pair(itemToFlush.m_uidSelf, std::make_pair(std::nullopt, itemToFlush.m_ptrObject)));

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint -= itemToFlush.m_ptrObject->getMemoryFootprint();
#endif //__TRACK_CACHE_FOOTPRINT__

			m_mpObjects.erase(itemToFlush.m_uidSelf);
			m_clockBuffer[victimIndex].reset();
			m_clockSize--;
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
		
		for(auto itObject = vtObjects.begin(); itObject != vtObjects.end(); itObject++)
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
		while (m_mpObjects.size() > m_nCacheCapacity)
		{
			size_t victimIndex = findVictimForEviction();
			
			if (victimIndex == SIZE_MAX)
			{
				// No evictable victim found, cache will exceed capacity
				break;
			}
			
			Item& itemToFlush = m_clockBuffer[victimIndex];
			
			// findVictimForEviction() already ensures use_count == 1, so no need to check again

			if (m_mpUIDUpdates.size() > 0)
			{
				m_ptrCallback->applyExistingUpdates(itemToFlush.m_ptrObject, m_mpUIDUpdates);
			}

			if (itemToFlush.m_ptrObject->getDirtyFlag())
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(itemToFlush.m_uidSelf, itemToFlush.m_ptrObject, uidUpdated) != CacheErrorCode::Success)
				{
					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				if (m_mpUIDUpdates.find(itemToFlush.m_uidSelf) != m_mpUIDUpdates.end())
				{
					std::cout << "Critical State: Can't proceed with the flushItemsToStorage operations as object already exists in Updates' list." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				m_mpUIDUpdates[itemToFlush.m_uidSelf] = std::make_pair(uidUpdated, itemToFlush.m_ptrObject);
			}

			m_mpObjects.erase(itemToFlush.m_uidSelf);
			m_clockBuffer[victimIndex].reset();
			m_clockSize--;
		}
#endif //__CONCURRENT__
	}

	inline void flushAllItemsToStorage()
	{
		std::vector<std::pair<ObjectUIDType, std::pair<std::optional<ObjectUIDType>, std::shared_ptr<ObjectType>>>> vtObjects;

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		// Flush all items in the circular buffer
		for (size_t idx = 0; idx < m_nCacheCapacity; idx++)
		{
			if (!m_clockBuffer[idx].m_bValid) {
				continue; // Skip empty slots
			}
			
			if (m_clockBuffer[idx].m_ptrObject.use_count() > 1)
			{
				std::cout << "Critical State: Can't proceed with the flushAllItemsToStorage operations as an object is in use." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

			if (!m_clockBuffer[idx].m_ptrObject->tryLockObject())
			{
				std::cout << "Critical State: Can't proceed with the flushAllItemsToStorage operations as lock can't be acquired on object." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}
			else
			{
				m_clockBuffer[idx].m_ptrObject->unlockObject();
			}

			vtObjects.push_back(std::make_pair(m_clockBuffer[idx].m_uidSelf, std::make_pair(std::nullopt, m_clockBuffer[idx].m_ptrObject)));

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint -= m_clockBuffer[idx].m_ptrObject->getMemoryFootprint();
#endif //__TRACK_CACHE_FOOTPRINT__

			m_mpObjects.erase(m_clockBuffer[idx].m_uidSelf);
			m_clockBuffer[idx].reset();
		}
		
		m_clockSize = 0;

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

		// Iterate through all valid items in circular buffer
		for (size_t i = 0; i < m_nCacheCapacity; i++)
		{
			if (!m_clockBuffer[i].m_bValid) continue;

			Item& item = m_clockBuffer[i];
			
			if (item.m_ptrObject.use_count() > 1)
			{
				std::cout << "Critical State: Can't proceed with the flushDatatemsToStorage operations as an object is in use." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

			if (!item.m_ptrObject->tryLockObject())
			{
				std::cout << "Critical State: Can't proceed with the flushDataItemsToStorage operations as lock can't be acquired on object." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}
			else
			{
				item.m_ptrObject->unlockObject();
			}

			vtObjects.push_back(std::make_pair(item.m_uidSelf, std::make_pair(std::nullopt, item.m_ptrObject)));

#ifdef __TRACK_CACHE_FOOTPRINT__
			m_nCacheFootprint -= item.m_ptrObject->getMemoryFootprint();
#endif //__TRACK_CACHE_FOOTPRINT__

			auto objectType = item.m_uidSelf.getObjectType();

			if (objectType != 101)  // If not a special type, remove from cache
			{
				m_mpObjects.erase(item.m_uidSelf);
				item.reset();
				m_clockSize--;
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

		// Iterate through all valid items in circular buffer
		for (size_t i = 0; i < m_nCacheCapacity; i++)
		{
			if (!m_clockBuffer[i].m_bValid) continue;

			Item& item = m_clockBuffer[i];
			
			if (item.m_ptrObject.use_count() > 1)
			{
				/* Info:
				 * Should proceed with the preceeding one?
				 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
				 */
				break;
			}

			// Check if the object is in use
			if (!item.m_ptrObject->tryLockObject())
			{
				/* Info:
				 * Should proceed with the preceeding one?
				 * But since each operation reorders the items at the end, therefore, the prceeding items would be in use as well!
				 */
				break;
			}
			else
			{
				item.m_ptrObject->unlockObject();
			}

			vtObjects.push_back(std::make_pair(item.m_uidSelf, std::make_pair(std::nullopt, item.m_ptrObject)));
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
		while (m_clockSize > m_nCacheCapacity)
		{
			// Find victim using CLOCK algorithm
			size_t victimIndex = findVictimForEviction();
			if (victimIndex == SIZE_MAX) {
				// No evictable victim found, cache will exceed capacity
				break;
			}

			Item& victim = m_clockBuffer[victimIndex];
			
			// findVictimForEviction() already ensures use_count == 1, so no need to check again

			if (m_mpUIDUpdates.size() > 0)
			{
				m_ptrCallback->applyExistingUpdates(victim.m_ptrObject, m_mpUIDUpdates);
			}

			if (victim.m_ptrObject->getDirtyFlag())
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(victim.m_uidSelf, victim.m_ptrObject, uidUpdated) != CacheErrorCode::Success)
				{
					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				if (m_mpUIDUpdates.find(victim.m_uidSelf) != m_mpUIDUpdates.end())
				{
					std::cout << "Critical State: Recently add object to Storage doest not exist in Updates' list." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				m_mpUIDUpdates[victim.m_uidSelf] = std::make_pair(uidUpdated, victim.m_ptrObject);
			}

			m_mpObjects.erase(victim.m_uidSelf);
			victim.reset();
			m_clockSize--;
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
