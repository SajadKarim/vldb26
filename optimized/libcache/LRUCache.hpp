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
#include <array>
#include "CacheErrorCodes.h"
#include <optional>
#include "PMemWAL.hpp"
#include "validityasserts.h"
#ifdef __CACHE_COUNTERS__
#include "CacheStatsProvider.hpp"
#endif

#define FLUSH_COUNT 100
#define MIN_CACHE_FOOTPRINT 1024 * 1024	// Safe check!
//#define __UPDATE_IN_ORDER__
using namespace std::chrono_literals;

template <typename Traits>
class LRUCache 
#ifdef __CACHE_COUNTERS__
    : public CacheStatsProvider<LRUCache<Traits>>
#endif
{
	typedef LRUCache<Traits> SelfType;

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
#ifdef __CONCURRENT__
	// Lock-free per-thread circular buffer for LRU updates
	struct ThreadLocalBuffer {
		static constexpr size_t CAPACITY = 256;
		
		alignas(64) std::atomic<size_t> head;
		alignas(64) std::atomic<size_t> tail;
		std::array<ObjectTypePtr, CAPACITY> buffer;
		
		ThreadLocalBuffer() : head(0), tail(0) {
			buffer.fill(nullptr);
		}
		
		// Push batch of objects to buffer (never fails, always retries)
		void pushBatch(const std::vector<ObjectTypePtr>& objects) {
			size_t batch_size = objects.size();
			
			// Split large batches into chunks
			if (batch_size >= CAPACITY - 1) {
				constexpr size_t CHUNK_SIZE = 200;
				for (size_t i = 0; i < batch_size; i += CHUNK_SIZE) {
					size_t chunk_end = std::min(i + CHUNK_SIZE, batch_size);
					std::vector<ObjectTypePtr> chunk(objects.begin() + i, objects.begin() + chunk_end);
					pushBatch(chunk);
				}
				return;
			}
			
			size_t retry_count = 0;
			
			// Write all objects first
			for (size_t i = 0; i < batch_size; ++i) {
				// Reload tail for each object to get current position
				size_t write_pos = tail.load(std::memory_order_relaxed);
				size_t next_pos = (write_pos + 1) & (CAPACITY - 1);
				
				// Progressive backoff while buffer is full
				while (next_pos == head.load(std::memory_order_relaxed)) {
					if (retry_count < 10) {
						std::this_thread::yield();
					} else if (retry_count < 100) {
						std::this_thread::sleep_for(std::chrono::microseconds(1));
					} else if (retry_count < 1000) {
						std::this_thread::sleep_for(std::chrono::microseconds(10));
					} else {
						std::this_thread::sleep_for(std::chrono::microseconds(100));
					}
					++retry_count;
					
					// Reload positions after waiting
					write_pos = tail.load(std::memory_order_relaxed);
					next_pos = (write_pos + 1) & (CAPACITY - 1);
				}
				
				// Write object at current tail position
				buffer[write_pos] = objects[i];
				
				// Atomically advance tail to make this object visible
				tail.store(next_pos, std::memory_order_release);
			}
		}
		
		// Pop one object from buffer
		bool pop(ObjectTypePtr& out_object) {
			size_t current_head = head.load(std::memory_order_relaxed);
			size_t current_tail = tail.load(std::memory_order_acquire);
			
			// Check if empty
			if (current_head == current_tail) {
				return false;
			}
			
			// Read object
			out_object = buffer[current_head];
			
			// Advance head
			size_t next_head = (current_head + 1) & (CAPACITY - 1);
			head.store(next_head, std::memory_order_release);
			
			return true;
		}
		
		// Pop all pending items in one atomic snapshot
		// Handles circular buffer wraparound efficiently
		bool popBatch(std::vector<ObjectTypePtr>& out_objects) {
			size_t current_head = head.load(std::memory_order_relaxed);
			size_t current_tail = tail.load(std::memory_order_acquire);
			
			// Check if empty
			if (current_head == current_tail) {
				return false;
			}
			
			size_t count = 0;
			
			// Handle circular buffer wraparound
			if (current_head < current_tail) {
				// No wrap: items are contiguous [head, tail)
				count = current_tail - current_head;
				out_objects.resize(count);
				
				// Direct assignment with moves (pointers, so moves are just copies)
				for (size_t i = 0; i < count; ++i) {
					out_objects[i] = std::move(buffer[current_head + i]);
				}
			} else {
				// Wrap-around: items at [head, CAPACITY) AND [0, tail)
				size_t first_part = CAPACITY - current_head;   // items from head to end
				size_t second_part = current_tail;              // items from start to tail
				count = first_part + second_part;
				
				out_objects.resize(count);
				size_t write_idx = 0;
				
				// Move [head, CAPACITY)
				for (size_t i = 0; i < first_part; ++i) {
					out_objects[write_idx++] = std::move(buffer[current_head + i]);
				}
				
				// Move [0, tail)
				for (size_t i = 0; i < second_part; ++i) {
					out_objects[write_idx++] = std::move(buffer[i]);
				}
			}
			
			// Update head to tail in ONE atomic operation
			// This marks all items as "read" instantly
			head.store(current_tail, std::memory_order_release);
			
			return true;
		}
		
		// Check if buffer is empty
		bool empty() const {
			return head.load(std::memory_order_relaxed) == tail.load(std::memory_order_acquire);
		}
	};
	
	static constexpr size_t MAX_THREADS = 128;
	static constexpr size_t BUFFER_CAPACITY = 256;
	
	std::array<ThreadLocalBuffer, MAX_THREADS> m_threadBuffers;
	std::atomic<size_t> m_nThreadCount;
	std::thread m_threadLRUUpdate;
	std::atomic<bool> m_bStopLRU;
#endif //__CONCURRENT__

	ObjectTypePtr m_ptrHead;
	ObjectTypePtr m_ptrTail;

	StorageType* m_ptrStorage;

	uint64_t m_nCacheCapacity;

#ifdef __CONCURRENT__
	std::atomic<uint64_t> m_nUsedCacheCapacity;
#else //__CONCURRENT__
	uint64_t m_nUsedCacheCapacity;
#endif //__CONCURRENT__

	WALType* m_ptrWAL;

#ifdef __CONCURRENT__
	bool m_bStop;

	std::thread m_threadCacheFlush;

	mutable std::shared_mutex m_mtxCache;

#ifdef __CACHE_COUNTERS__
	// Storage for background thread's cache statistics
	std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> m_backgroundThreadHits;
	std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> m_backgroundThreadMisses;
	std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> m_backgroundThreadEvictions;
	std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> m_backgroundThreadDirtyEvictions;
#endif //__CACHE_COUNTERS__
#endif //__CONCURRENT__

public:
	~LRUCache()
	{
#ifdef __CONCURRENT__
		// Stop LRU update thread first
		m_bStopLRU.store(true, std::memory_order_release);
		if (m_threadLRUUpdate.joinable()) {
			m_threadLRUUpdate.join();
		}
		
		// Then stop cache flush thread
		m_bStop = true;
		m_threadCacheFlush.join();
#endif //__CONCURRENT__

		flush(false);

		m_ptrHead == nullptr;
		m_ptrTail == nullptr;
		
		//delete m_ptrWAL;
		//m_ptrWAL = nullptr;

		delete m_ptrStorage;
		m_ptrStorage = nullptr;
	}

	template <typename... StorageArgs>
	LRUCache(size_t nCapacity, StorageArgs... args)
		: m_ptrHead(nullptr)
		, m_ptrTail(nullptr)
		, m_nUsedCacheCapacity(0)
		, m_nCacheCapacity(nCapacity)
#ifdef __CONCURRENT__
		, m_nThreadCount(0)
		, m_bStopLRU(false)
#endif
	{
#ifdef __CACHE_COUNTERS__
		// Reset thread-local stats when creating a new cache instance
		CacheStatsProvider<LRUCache<Traits>>::resetThreadLocalStats();
#endif
		
		m_ptrStorage = new StorageType(args...);

		//m_ptrWAL = new WALType(this, WAL_FILE_PATH);

#ifdef __CONCURRENT__
		m_bStop = false;
		m_threadCacheFlush = std::thread(handlerCacheFlush, this);
		m_threadLRUUpdate = std::thread(handlerLRUUpdate, this);
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
	const CacheStatsProvider<LRUCache<Traits>>* getCacheStatsProvider() const
	{
		return static_cast<const CacheStatsProvider<LRUCache<Traits>>*>(this);
	}
#endif

#ifdef __UPDATE_IN_ORDER__
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
			first->m_ptrPrev = nullptr;
			first->m_ptrNext = nullptr;
			m_ptrHead = first;
			m_ptrTail = first;

			currentLruPtr = nullptr;  // No next item yet
			prevPtr = first;
			++i; // Move to next vector item

#if defined(__CONCURRENT__) && defined(__TREE_WITH_CACHE__)
			ASSERT(first->m_nUseCounter.load(std::memory_order_relaxed) > 0);
			first->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
#endif //__CONCURRENT__ && __TREE_WITH_CACHE__
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

#if defined(__CONCURRENT__) && defined(__TREE_WITH_CACHE__)
			ASSERT(vecItem->m_nUseCounter.load(std::memory_order_relaxed) > 0);
			vecItem->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
#endif //__CONCURRENT__ && __TREE_WITH_CACHE__

			if (!currentLruPtr)
			{
				// Reached end of LRU list; append item to tail
				vecItem->m_ptrPrev = m_ptrTail;
				vecItem->m_ptrNext = nullptr;

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
			auto oldPrev = vecItem->m_ptrPrev;
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

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		size_t i = 0;
		size_t j = 0;
		for (; i < vtObjects.size(); ++i)
		{
			for (j = 0; j < 2; ++j)
			{
				ObjectTypePtr& vecItem = j == 0 ? (vtObjects[i]).first : (vtObjects[i]).second;

				if (vecItem == nullptr || vecItem->m_ptrCoreObject == nullptr)
				{
					continue;
				}
				break;
			}
			if (j < 2) break; // Found valid item
		}

		ObjectTypePtr currentLruPtr = nullptr;
		ObjectTypePtr prevPtr = nullptr;

		// If LRU is empty, insert the first item from the vector
		if (!m_ptrHead)
		{
			ObjectTypePtr first = j == 0 ? (vtObjects[i]).first : (vtObjects[i]).second;
			first->m_ptrPrev = nullptr;
			first->m_ptrNext = nullptr;
			m_ptrHead = first;
			m_ptrTail = first;

			currentLruPtr = nullptr;  // No next item yet
			prevPtr = first;

#if defined(__CONCURRENT__) && defined(__TREE_WITH_CACHE__)
			ASSERT(first->m_nUseCounter.load(std::memory_order_relaxed) > 0);
			first->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
#endif //__CONCURRENT__ && __TREE_WITH_CACHE__

			// Move to next item
			++j;
			if (j >= 2)
			{
				++i;
				j = 0;
			}
		}
		else
		{
			currentLruPtr = m_ptrHead;
		}

		// Process remaining items
		for (; i < vtObjects.size(); ++i)
		{
			for (; j < 2; ++j)
			{
				ObjectTypePtr& vecItem = j == 0 ? (vtObjects[i]).first : (vtObjects[i]).second;

				if (vecItem == nullptr || vecItem->m_ptrCoreObject == nullptr)
				{
					continue;
				}

#if defined(__CONCURRENT__) && defined(__TREE_WITH_CACHE__)
				ASSERT(vecItem->m_nUseCounter.load(std::memory_order_relaxed) > 0);
				vecItem->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
#endif //__CONCURRENT__ && __TREE_WITH_CACHE__

				if (!currentLruPtr)
				{
					// Reached end of LRU list; append item to tail
					vecItem->m_ptrPrev = m_ptrTail;
					vecItem->m_ptrNext = nullptr;

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
				auto oldPrev = vecItem->m_ptrPrev;
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
			j = 0;
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

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
#endif //__CONCURRENT__

		for (auto it = vtObjects.begin(); it != vtObjects.end(); it++)
		{
			if ((*it).m_ptrToDiscard != nullptr) remove((*it).m_ptrToDiscard);
		}

		size_t i = 0;
		size_t j = 0;
		for (; i < vtObjects.size(); ++i)
		{
			for (j = 0; j < 2; ++j)
			{
				ObjectTypePtr& vecItem = j == 0 ? (vtObjects[i]).m_ptrPrimary : (vtObjects[i]).m_ptrAffectedSibling;

				if (vecItem == nullptr || vecItem->m_ptrCoreObject == nullptr)
				{
					continue;
				}
				break;
			}
			if (j < 2) break; // Found valid item
		}

		ObjectTypePtr currentLruPtr = nullptr;
		ObjectTypePtr prevPtr = nullptr;

		// If LRU is empty, insert the first item from the vector
		if (!m_ptrHead)
		{
			ObjectTypePtr first = j == 0 ? (vtObjects[i]).m_ptrPrimary : (vtObjects[i]).m_ptrAffectedSibling;

			first->m_ptrPrev = nullptr;
			first->m_ptrNext = nullptr;
			m_ptrHead = first;
			m_ptrTail = first;

			currentLruPtr = nullptr;  // No next item yet
			prevPtr = first;

#if defined(__CONCURRENT__) && defined(__TREE_WITH_CACHE__)
			ASSERT(first->m_nUseCounter.load(std::memory_order_relaxed) > 0);
			first->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
#endif //__CONCURRENT__ && __TREE_WITH_CACHE__

			// Move to next item
			++j;
			if (j >= 2)
			{
				++i;
				j = 0;
			}
		}
		else
		{
			currentLruPtr = m_ptrHead;
		}

		// Process remaining items
		for (; i < vtObjects.size(); ++i)
		{
			for (; j < 2; ++j)
			{
				ObjectTypePtr& vecItem = j == 0 ? (vtObjects[i]).m_ptrPrimary : (vtObjects[i]).m_ptrAffectedSibling;

				if (vecItem == nullptr || vecItem->m_ptrCoreObject == nullptr)
				{
					continue;
				}

#if defined(__CONCURRENT__) && defined(__TREE_WITH_CACHE__)
				ASSERT(vecItem->m_nUseCounter.load(std::memory_order_relaxed) > 0);
				vecItem->m_nUseCounter.fetch_sub(1, std::memory_order_relaxed);
#endif //__CONCURRENT__ && __TREE_WITH_CACHE__

				if (!currentLruPtr)
				{
					// Reached end of LRU list; append item to tail
					vecItem->m_ptrPrev = m_ptrTail;
					vecItem->m_ptrNext = nullptr;

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
				auto oldPrev = vecItem->m_ptrPrev;
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
			j = 0;
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
#else //__UPDATE_IN_ORDER__
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

#ifdef __CONCURRENT__
		// Filter valid objects into a temporary vector
		std::vector<ObjectTypePtr> validObjects;
		validObjects.reserve(vtObjects.size());
		
		for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
		{
			auto& obj = *it;
			if (obj != nullptr && obj->m_ptrCoreObject != nullptr)
			{
				validObjects.push_back(obj);
			}
		}
		
		// Get thread buffer index and push batch
		if (!validObjects.empty())
		{
			size_t thread_idx = getThreadBufferIndex();
			m_threadBuffers[thread_idx].pushBatch(validObjects);
		}
		
		vtObjects.clear();
#else //__CONCURRENT__
		for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
		{
			auto& obj = *it;

			if (obj != nullptr && obj->m_ptrCoreObject != nullptr)
			{
				moveToFront(obj);
			}
		}

		vtObjects.clear();
#endif //__CONCURRENT__

#ifndef __CONCURRENT__
		size_t nObjectsLinkedList = 0;
		getObjectsCountInCache(nObjectsLinkedList);
		ASSERT(nObjectsLinkedList == m_nUsedCacheCapacity);

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

#ifdef __CONCURRENT__
		// Filter and flatten valid objects into a temporary vector
		std::vector<ObjectTypePtr> validObjects;
		validObjects.reserve(vtObjects.size() * 2);
		
		for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
		{
			auto& lhs = (*it).first;
			auto& rhs = (*it).second;

			if (lhs != nullptr && lhs->m_ptrCoreObject != nullptr)
			{
				validObjects.push_back(lhs);
			}

			if (rhs != nullptr && rhs->m_ptrCoreObject != nullptr)
			{
				validObjects.push_back(rhs);
			}
		}
		
		// Get thread buffer index and push batch
		if (!validObjects.empty())
		{
			size_t thread_idx = getThreadBufferIndex();
			m_threadBuffers[thread_idx].pushBatch(validObjects);
		}
		
		vtObjects.clear();
#else //__CONCURRENT__
		for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
		{
			auto& lhs = (*it).first;
			auto& rhs = (*it).second;

			if (lhs != nullptr && lhs->m_ptrCoreObject != nullptr)
			{
				moveToFront(lhs);
			}

			if (rhs != nullptr && rhs->m_ptrCoreObject != nullptr)
			{
				moveToFront(rhs);
			}
		}

		vtObjects.clear();
#endif //__CONCURRENT__

#ifndef __CONCURRENT__
		size_t nObjectsLinkedList = 0;
		getObjectsCountInCache(nObjectsLinkedList);
		ASSERT(nObjectsLinkedList == m_nUsedCacheCapacity);

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

#ifdef __CONCURRENT__
		// Filter and flatten valid objects into a temporary vector
		std::vector<ObjectTypePtr> validObjects;
		validObjects.reserve(vtObjects.size() * 2);
		
		for (auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
		{
			// Handle m_ptrToDiscard first (needs immediate removal)
			if ((*it).m_ptrToDiscard != nullptr)
			{
				remove((*it).m_ptrToDiscard);
			}
			
			// Collect valid objects for batch processing
			if ((*it).m_ptrPrimary != nullptr && (*it).m_ptrPrimary->m_ptrCoreObject != nullptr)
			{
				validObjects.push_back((*it).m_ptrPrimary);
			}
			
			if ((*it).m_ptrAffectedSibling != nullptr && (*it).m_ptrAffectedSibling->m_ptrCoreObject != nullptr)
			{
				validObjects.push_back((*it).m_ptrAffectedSibling);
			}
		}
		
		// Get thread buffer index and push batch
		if (!validObjects.empty())
		{
			size_t thread_idx = getThreadBufferIndex();
			m_threadBuffers[thread_idx].pushBatch(validObjects);
		}
		
		vtObjects.clear();
#else //__CONCURRENT__
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

				moveToFront(obj);
			}
		}

		vtObjects.clear();
#endif //__CONCURRENT__

#ifndef __CONCURRENT__
		size_t nObjectsLinkedList = 0;
		getObjectsCountInCache(nObjectsLinkedList);
		ASSERT(nObjectsLinkedList == m_nUsedCacheCapacity);

		flushItemsToStorage();
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}
#endif //__UPDATE_IN_ORDER__

	CacheErrorCode remove(ObjectTypePtr& ptrObject)
	{
		if (ptrObject->m_uid.getMediaType() > 1)
		{
			m_ptrStorage->remove(ptrObject->m_uid);
		}

#ifdef __CONCURRENT__
		//std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
		// BUG: enable thissssss !!
#endif //__CONCURRENT__

		removeFromLRU(ptrObject);

		delete ptrObject;
		ptrObject = nullptr;

		return CacheErrorCode::Success;
	}

	CacheErrorCode getCoreObject(uint16_t nDegree, const ObjectUIDType& uidObject, ObjectTypePtr& ptrObject)
	{
		m_ptrStorage->getObject(nDegree, uidObject, ptrObject);

		ASSERT(ptrObject->m_ptrCoreObject != nullptr && "The requested object does not exist.");

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

		ASSERT (ptrObject == nullptr || "The requested object does not exist.");

#ifdef __CONCURRENT__
		m_nUsedCacheCapacity.fetch_add(1, std::memory_order_relaxed);
#else //__CONCURRENT__
		ASSERT(m_nUsedCacheCapacity != 0);
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
		//ASSERT(m_nUsedCacheCapacity != 0);
		m_nUsedCacheCapacity++;
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}

	void getObjectsCountInCache(size_t& nObjects)
	{
		nObjects = 0;
		ObjectTypePtr ptrItem = m_ptrHead;

		while (ptrItem != nullptr)
		{
			nObjects++;
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

#ifdef __CACHE_COUNTERS__
			// Move background thread's copied data to main thread's thread-local variables
			// This is efficient since we're transferring ownership using move semantics
			auto* statsProvider = static_cast<CacheStatsProvider<LRUCache<Traits>>*>(this);
			
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
	inline void moveToFront(ObjectTypePtr& ptrItem)
	{
		// Check for an empty list.
		if (m_ptrHead == nullptr)
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
		if (ptrItem->m_ptrPrev != nullptr)
		{
			ptrItem->m_ptrPrev->m_ptrNext = ptrItem->m_ptrNext;
		}
		if (ptrItem->m_ptrNext != nullptr)
		{
			ptrItem->m_ptrNext->m_ptrPrev = ptrItem->m_ptrPrev;
		}
		if (ptrItem == m_ptrTail)
		{
			m_ptrTail = ptrItem->m_ptrPrev;
		}

		ptrItem->m_ptrPrev = nullptr;
		ptrItem->m_ptrNext = m_ptrHead;
		m_ptrHead->m_ptrPrev = ptrItem;
		m_ptrHead = ptrItem;
	}

	inline void removeFromLRU(ObjectTypePtr& ptrItem)
	{
		// Lock the weak pointer for previous node
		ObjectTypePtr& prev = ptrItem->m_ptrPrev;
		ObjectTypePtr& next = ptrItem->m_ptrNext;

		// Case 1: Node is both head and tail (only element)
		if (ptrItem == m_ptrHead && ptrItem == m_ptrTail) 
		{
			m_ptrHead = nullptr;
			m_ptrTail = nullptr;
		}
		// Case 2: Node is head
		else if (ptrItem == m_ptrHead) 
		{
			m_ptrHead = next;
			if (next) 
			{
				next->m_ptrPrev = nullptr;
			}
		}
		// Case 3: Node is tail
		else if (ptrItem == m_ptrTail) 
		{
			m_ptrTail = prev;
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
		m_nUsedCacheCapacity.fetch_sub(1, std::memory_order_relaxed);
#else //__CONCURRENT__
		ASSERT(m_nUsedCacheCapacity != 0);
		m_nUsedCacheCapacity--;
#endif //__CONCURRENT__

	}

	inline void flushItemsToStorage()
	{
#ifdef __CONCURRENT__
		std::vector<ObjectTypePtr> vtObjects;

		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
		
		auto nUsedCacheCapacity = m_nUsedCacheCapacity.load(std::memory_order_relaxed);

		if (nUsedCacheCapacity <= m_nCacheCapacity)
			return;

		int nFlushCount = nUsedCacheCapacity - m_nCacheCapacity;
		for (; nFlushCount > 0; nFlushCount--)
		{
			//auto nUseCounter = m_ptrTail->m_nUseCounter.load(std::memory_order_relaxed);
			//ASSERT(nUseCounter >= 0);

			if (m_ptrTail->m_nUseCounter.load(std::memory_order_relaxed) != 0) break;

			if (!m_ptrTail->m_mtx.try_lock()) break;
			
			if (m_ptrTail->m_nUseCounter.load(std::memory_order_relaxed) != 0)
			{
				m_ptrTail->m_mtx.unlock();
				break;
			}

			ObjectTypePtr ptrItemToFlush = m_ptrTail;

			ASSERT (ptrItemToFlush->m_ptrNext == nullptr);
			ASSERT(ptrItemToFlush->m_ptrCoreObject != nullptr);

			vtObjects.push_back(ptrItemToFlush);

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTail = m_ptrTail->m_ptrPrev;

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTail != nullptr)
			{
				ASSERT(m_ptrHead != nullptr);

				m_ptrTail->m_ptrNext = nullptr;  // Optional: Explicitly reset next pointer
			}
			else
			{
				ASSERT(m_ptrHead == ptrItemToFlush);

				// The list had only one element, so update the head as well
				m_ptrHead = nullptr;
			}

			// Clear the removed node's pointers
			ptrItemToFlush->m_ptrPrev = nullptr;
			ptrItemToFlush->m_ptrNext = nullptr;

			m_nUsedCacheCapacity.fetch_sub(1, std::memory_order_relaxed);

			ASSERT(ptrItemToFlush->m_nUseCounter.load(std::memory_order_relaxed) == 0);
		}

		lock_cache.unlock();

		//for(auto itObject = vtObjects.begin(); itObject != vtObjects.end(); itObject++)
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

			// Clear the removed node's pointers
			ASSERT(obj->m_ptrPrev == nullptr);
			ASSERT(obj->m_ptrNext == nullptr);

			obj->m_bDirty = false;
			obj->deleteCoreObject();
			obj->m_mtx.unlock();
			obj = nullptr;
		}

		vtObjects.clear();
#else //__CONCURRENT__
		while (m_nUsedCacheCapacity > m_nCacheCapacity)
		{
			ASSERT(m_ptrTail != nullptr);

			bool is_dirty = m_ptrTail->hasUpdatesToBeFlushed();
			if (is_dirty) //check for uidupdated and uid in the cache object and reset uidupdated once used!!!
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(m_ptrTail, uidUpdated) != CacheErrorCode::Success)
				{
					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				m_ptrTail->m_uidUpdated = uidUpdated;
			}

#ifdef __CACHE_COUNTERS__
			// Record eviction with dirty flag
			this->recordEviction(is_dirty);
#endif

			m_ptrTail->m_bDirty = false;
			ObjectTypePtr ptrTemp = m_ptrTail;

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTail = m_ptrTail->m_ptrPrev;

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTail)
			{
				m_ptrTail->m_ptrNext = nullptr;
			}
			else
			{
				ASSERT (m_ptrHead == ptrTemp);

				// The list had only one element, so update the head as well
				m_ptrHead = nullptr;
			}

			// Clear the removed node's pointers
			ptrTemp->m_ptrPrev = nullptr;
			ptrTemp->m_ptrNext = nullptr;

			// Call custom deletion logic and release the pointer
			ptrTemp->m_bDirty = false;
			ptrTemp->deleteCoreObject();
			ptrTemp = nullptr;

			ASSERT(m_nUsedCacheCapacity != 0);
			m_nUsedCacheCapacity--;
		}
#endif //__CONCURRENT__
	}

	inline void flushAllItemsToStorage()
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_cache(m_mtxCache);
		while (m_nUsedCacheCapacity.load(std::memory_order_relaxed) > 0)
#else //__CONCURRENT__
	while (m_nUsedCacheCapacity > 0)
#endif //__CONCURRENT__

		{
			if (m_ptrTail == nullptr) break;

#ifdef __CONCURRENT__
			if (m_ptrTail->m_nUseCounter.load(std::memory_order_relaxed) != 0) break;

			if (!m_ptrTail->m_mtx.try_lock()) break;

			if (m_ptrTail->m_nUseCounter.load(std::memory_order_relaxed) != 0)
			{
				m_ptrTail->m_mtx.unlock();
				break;
			}
#endif //__CONCURRENT__

			bool is_dirty = m_ptrTail->hasUpdatesToBeFlushed();
			if (is_dirty) //check for uidupdated and uid in the cache object and reset uidupdated once used!!!
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(m_ptrTail, uidUpdated) != CacheErrorCode::Success)
				{
					std::cout << "Critical State: Failed to add object to Storage." << std::endl;
					throw new std::logic_error(".....");   // TODO: critical log.
				}

				m_ptrTail->m_uidUpdated = uidUpdated;
			}

#ifdef __CACHE_COUNTERS__
			// Record eviction with dirty flag
			this->recordEviction(is_dirty);
#endif

			m_ptrTail->m_bDirty = false;
			ObjectTypePtr ptrTemp = m_ptrTail;

			// Update tail pointer using lock() because m_ptrPrev is a weak_ptr
			m_ptrTail = m_ptrTail->m_ptrPrev;

			// If the list still has elements, update the new tail's next pointer
			if (m_ptrTail)
			{
				m_ptrTail->m_ptrNext = nullptr;
			}
			else
			{
				ASSERT(m_ptrHead == ptrTemp);

				// The list had only one element, so update the head as well
				m_ptrHead = nullptr;
			}

			// Clear the removed node's pointers
			ptrTemp->m_ptrPrev = nullptr;
			ptrTemp->m_ptrNext = nullptr;

			// Call custom deletion logic and release the pointer
			ptrTemp->m_bDirty = false;
			ptrTemp->deleteCoreObject();
			ptrTemp = nullptr;

#ifdef __CONCURRENT__
			m_nUsedCacheCapacity.fetch_sub(1, std::memory_order_relaxed);
#else //__CONCURRENT__
			ASSERT(m_nUsedCacheCapacity != 0);
			m_nUsedCacheCapacity--;
#endif //__CONCURRENT__
		}

		ASSERT(m_nUsedCacheCapacity == 0);
		ASSERT(m_ptrTail == nullptr);
	}

public:
	inline void persistAllItems()
	{
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
	}

#ifdef __CONCURRENT__
	// Get thread-local buffer index with caching
	inline size_t getThreadBufferIndex()
	{
		thread_local size_t cached_index = std::numeric_limits<size_t>::max();
		
		if (cached_index == std::numeric_limits<size_t>::max())
		{
			cached_index = m_nThreadCount.fetch_add(1, std::memory_order_acq_rel);
			ASSERT(cached_index < MAX_THREADS);
		}
		
		return cached_index;
	}

	// Background thread handler for LRU metadata updates
	static void handlerLRUUpdate(SelfType* ptrSelf)
	{
		size_t round_robin_idx = 0;
		
		while (!ptrSelf->m_bStopLRU.load(std::memory_order_acquire))
		{
			bool did_work = false;
			
			// Get current number of registered threads
			size_t num_threads = ptrSelf->m_nThreadCount.load(std::memory_order_acquire);
			
			// If no threads registered yet, sleep and continue
			if (num_threads == 0)
			{
				std::this_thread::sleep_for(std::chrono::microseconds(10));
				continue;
			}
			
			// Acquire lock once for this iteration
			std::unique_lock<std::shared_mutex> lock_cache(ptrSelf->m_mtxCache);
			
			// Reusable batch vector to avoid repeated allocations
			std::vector<ObjectTypePtr> batch_objects;
			
			// Process each registered thread's buffer (round-robin for fairness)
			for (size_t i = 0; i < num_threads; ++i)
			{
				size_t thread_idx = (round_robin_idx + i) % num_threads;
				auto& buffer = ptrSelf->m_threadBuffers[thread_idx];
				
				// Pop all pending objects from this buffer in one atomic operation
				if (buffer.popBatch(batch_objects))
				{
					did_work = true;
					
					// Process all objects in the batch
					for (ObjectTypePtr obj : batch_objects)
					{
						// Call moveToFront for this object
						ptrSelf->moveToFront(obj);
						
						// Decrement use counter
						obj->m_nUseCounter.fetch_sub(1, std::memory_order_release);
					}
					
					// Clear for reuse in next iteration
					batch_objects.clear();
				}
			}
			
			// Update round-robin index for next iteration
			if (num_threads > 0)
			{
				round_robin_idx = (round_robin_idx + 1) % num_threads;
			}
			
			// Release lock
			lock_cache.unlock();
			
			// If no work was done, sleep briefly to avoid busy-waiting
			if (!did_work)
			{
				std::this_thread::sleep_for(std::chrono::microseconds(10));
			}
		}
		
		// Final drain on shutdown - process all remaining objects
		std::unique_lock<std::shared_mutex> lock_cache(ptrSelf->m_mtxCache);
		
		std::vector<ObjectTypePtr> batch_objects;
		size_t num_threads = ptrSelf->m_nThreadCount.load(std::memory_order_acquire);
		for (size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx)
		{
			auto& buffer = ptrSelf->m_threadBuffers[thread_idx];
			
			// Drain all remaining objects using batch pop
			while (buffer.popBatch(batch_objects))
			{
				for (ObjectTypePtr obj : batch_objects)
				{
					ptrSelf->moveToFront(obj);
					obj->m_nUseCounter.fetch_sub(1, std::memory_order_release);
				}
				batch_objects.clear();
			}
		}
	}

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
		const auto* statsProvider = static_cast<const CacheStatsProvider<LRUCache<Traits>>*>(ptrSelf);
		
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