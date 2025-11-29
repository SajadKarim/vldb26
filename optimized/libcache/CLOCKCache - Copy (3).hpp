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

protected:
	size_t m_nClockHand;
	std::vector<int> m_vtClockBufferWeight;
	std::vector<ObjectTypePtr> m_vtClockBuffer;
	std::vector<std::pair<ObjectTypePtr, int>> m_vtClockQ;

	std::unique_ptr<StorageType> m_ptrStorage;

	int64_t m_nCacheCapacity;
	int64_t m_nUsedCacheCapacity;
	int m_pendingclock;
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

		{
			std::unique_lock<std::mutex> lock_cache(m_mtxCache);
			cv.wait(lock_cache, [&] { return m_vtClockQ.size() == 0; });
		}

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

	CacheErrorCode updateObjectsAccessMetadata(std::vector<ObjectTypePtr>& vtObjects, int level, std::vector<int> &vt)
	{
#ifdef __CONCURRENT__
		bool bUpdatesOnly = false;
		std::vector<std::pair<ObjectTypePtr, int>> vtBuffer; 
		vtBuffer.reserve(vtObjects.size());

		int y = -1;
		for (auto obj : vtObjects)
		//int lvl = 1;
		//for(auto it = vtObjects.rbegin(); it != vtObjects.rend(); it++)
		{
			y++;
			//auto& obj = *it;
			if (obj == nullptr)
			{
				continue;
			}

			if (obj->del)
			{
				//ASSERT(vt[y] == 0);
				if (obj->m_nPositionInCLOCK.load(std::memory_order_acquire) == -1)
				{
					//ASSERT(obj.use_count() == 1);
				}
				else
				{
					//ASSERT(obj.use_count() == 2);
				}

				bUpdatesOnly = false;
				vtBuffer.push_back({obj, 0 });
				continue;
			}

			ASSERT(obj->m_ptrCoreObject); //failed.. might be object has been moved while it has bene in the queue.. but how.. I do have check for ref_count!!

			if (obj->m_nPositionInCLOCK.load(std::memory_order_acquire) == -1)
			//if (obj->m_nPositionInCLOCK == -1)
			{
				ASSERT(obj->m_ptrCoreObject);

				//obj->m_nPositionInCLOCK = INT_MAX;
				bUpdatesOnly = false;
			}
			else
			{
				ASSERT(y < vt.size());
			}

			ASSERT(y < vt.size());
			ASSERT((level - vt[y]) > 0);
			vtBuffer.push_back({ obj, level - vt[y]});
		}

		if (bUpdatesOnly)
		{
			return CacheErrorCode::Success;
		}

		{
			std::unique_lock<std::mutex> lock_cache(m_mtxCache);

			cv.wait(lock_cache, [&] { return m_vtClockQ.size() + vtBuffer.size() < m_nCacheCapacity; });

			m_vtClockQ.insert(m_vtClockQ.end(), vtBuffer.begin(), vtBuffer.end());
		}

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

			*obj->reftofirstiteminpair = level - obj->m_nFlushPriority;	// use atomics!!!!	// why are you updating it everytime?
		}

		vtObjects.clear();
#endif //__CONCURRENT__

		return CacheErrorCode::Success;
	}

	CacheErrorCode remove(const ObjectTypePtr ptrObject)
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::mutex> lock_cache(m_mtxCache);
		 
		if (ptrObject->m_uid.getMediaType() > 1)
		{
			m_ptrStorage->remove(ptrObject->m_uid);
		}

		////why not delete it instead od flag.. why you used flag??
		//ptrObject->deleteCoreObject();
		//ptrObject->m_uidUpdated = std::nullopt;
		//ptrObject->m_uid = ObjectUIDType(INT64_MAX);
		//ptrObject->m_nFlushPriority = 0;
		ptrObject->del = true;
		ptrObject->m_bDirty = false;
		m_nUsedCacheCapacity--;
		return CacheErrorCode::Success;

#else //__CONCURRENT__

		//ASSERT(ptrObject->m_uid.getMediaType() > 1);
		if (ptrObject->m_uid.getMediaType() > 1)
		{
			m_ptrStorage->remove(ptrObject->m_uid);
		}

		ptrObject->deleteCoreObject();
		ptrObject->m_uidUpdated = std::nullopt;
		ptrObject->m_uid = ObjectUIDType(INT64_MAX);


		auto idx = ptrObject->m_nPositionInCLOCK;
		//if (idx != -1 && idx != INT_MAX)
		if (idx != -1)
		{
			ASSERT(m_vtClockBuffer[idx]);
			m_vtClockBuffer[idx].reset();
			m_vtClockBuffer[idx] = nullptr;
			m_vtClockBufferWeight[idx] = -1;
			
		}

		//ptrObject->reftofirstiteminpair = nullptr;
		//ASSERT(!ptrObject->reftofirstiteminpair);
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
#endif //__CONCURRENT__
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

	inline void flushItemsToStorage(std::vector<int>& vtEmptySlots)
	{
#ifdef __CONCURRENT__
		std::vector<ObjectTypePtr> vtObjects;

		int nQSiz = 0;
		{
			std::unique_lock<std::mutex> lock_cache(m_mtxCache);
			if ( (nQSiz = m_vtClockQ.size()) == 0) return;
		}
		/*
		if (vtEmptySlots.size() == 0)
		{
			do 
			{
				for (int nIdx = 0; nIdx < m_nCacheCapacity; nIdx++)
				{
					if (m_vtClockBufferWeight[m_nClockHand] == -1)
					{
						vtEmptySlots.push_back(m_nClockHand);
						ASSERT(!m_vtClockBuffer[m_nClockHand]);
						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					auto& obj = m_vtClockBuffer[m_nClockHand];

					if (obj.use_count() > 2)
					{
						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					// Check if the object is in use
					if (!obj->m_mtx.try_lock())
					{
						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					if (obj.use_count() > 2)
					{
						obj->m_mtx.unlock();
						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					if (m_vtClockBufferWeight[m_nClockHand] > 0)
					{
						obj->m_mtx.unlock();
						m_vtClockBufferWeight[m_nClockHand]--;
						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					ASSERT(m_vtClockBuffer[m_nClockHand]);
					
					if (obj->m_ptrCoreObject == nullptr)
					{
						ASSERT(obj->m_nPositionInCLOCK == m_nClockHand);

						//ASSERT(false); // should not happen!!
						if (obj->m_nPositionInCLOCK != -1)
						{
							ASSERT(m_vtClockBuffer[m_nClockHand]);
							ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);

							//chcnk this
							vtEmptySlots.push_back(m_nClockHand);
							
							m_vtClockBufferWeight[m_nClockHand] = -1;
						}
						else
						{
							ASSERT(!m_vtClockBuffer[m_nClockHand]);
							ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);
						}

						obj->m_nPositionInCLOCK = -1;
						obj->reftofirstiteminpair = nullptr;


						obj->m_mtx.unlock();
						
						m_vtClockBuffer[m_nClockHand].reset();
						m_vtClockBuffer[m_nClockHand] = nullptr;

						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					ASSERT(obj->m_ptrCoreObject);

					vtObjects.push_back(obj);

					vtEmptySlots.push_back(m_nClockHand);

					ASSERT(obj->m_nPositionInCLOCK == m_nClockHand);

					m_vtClockBufferWeight[m_nClockHand] = -1;

					obj->m_nPositionInCLOCK = -1;
					obj->reftofirstiteminpair = nullptr;

					m_vtClockBuffer[m_nClockHand].reset();
					m_vtClockBuffer[m_nClockHand] = nullptr;

					ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);

					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
				}

				std::sort(vtEmptySlots.begin(), vtEmptySlots.end());
			} while (vtEmptySlots.size() == 0);
		}
		*/
		
		int nIdx = 0;
		int pIdx = 0;
		for (; nIdx < nQSiz; nIdx++)
		{
			auto a_obj = m_vtClockQ[nIdx].first;

			if (a_obj == nullptr)
			{
				continue;
			}

			if (m_vtClockQ[nIdx].second == 0)
			{
				ASSERT(a_obj->del);
				//if(a_obj->reftofirstiteminpair != nullptr) *a_obj->reftofirstiteminpair = m_vtClockQ[nIdx].second;
				continue;
			}
			/*
			if (obj->del == true && obj->m_ptrCoreObject != nullptr)
			{
				//ASSERT(ptrObject->m_uid.getMediaType() > 1);
				//if (obj->m_uid.getMediaType() > 1)
				//{
				//	m_ptrStorage->remove(obj->m_uid);
				//}

				//why not delete it instead od flag.. why you used flag??
				obj->deleteCoreObject();
				obj->m_uidUpdated = std::nullopt;
				obj->m_uid = ObjectUIDType(INT64_MAX);

				auto idx = obj->m_nPositionInCLOCK;
				if (idx != -1/) //might be case where item had to bee added tot the queue but got deleted!!
				{
					ASSERT(m_vtClockBuffer[idx]);
					ASSERT(m_vtClockBufferWeight[idx] != -1);

					m_vtClockBuffer[idx].reset();
					m_vtClockBuffer[idx] = nullptr;
					m_vtClockBufferWeight[idx] = -1;
					obj->m_nPositionInCLOCK = -1;
					obj->reftofirstiteminpair = nullptr;

					continue;
				}

				//ASSERT(obj->m_nPositionInCLOCK == -1);
				ASSERT(!obj->reftofirstiteminpair);

				continue;
			}

			if (obj->m_ptrCoreObject == nullptr) // there might be a case where them item being pushed multiple times but got evicted in the previous round!!
			{
				auto idx = obj->m_nPositionInCLOCK;
				if (idx != -1) //might be case where item had to bee added tot the queue but got deleted!!
				{
					ASSERT(m_vtClockBuffer[idx]);
					ASSERT(m_vtClockBufferWeight[idx] != -1);

					m_vtClockBuffer[idx].reset();
					m_vtClockBuffer[idx] = nullptr;
					m_vtClockBufferWeight[idx] = -1;
					obj->m_nPositionInCLOCK = -1;
					obj->reftofirstiteminpair = nullptr;

					continue;
				}

				//ASSERT(obj->m_nPositionInCLOCK == -1);
				ASSERT(!obj->reftofirstiteminpair);

				continue;
			}
			*/
			//ASSERT(obj->m_nPositionInCLOCK != -1);
			ASSERT(a_obj);

			
			if (a_obj->m_nPositionInCLOCK.load(std::memory_order_acquire) == -1)
			{
				/*for (auto& _obj : vtObjects)
				{
					ASSERT(_obj->m_uid != obj->m_uid);
				}*/
				/*if (pIdx == vtEmptySlots.size())
				{
					break;
				}*/
				{
					//for (int nIdx = 0; nIdx < m_nCacheCapacity; nIdx++)
					while(true)
					{
						if (m_vtClockBufferWeight[m_nClockHand] == -1)
						{
							//vtEmptySlots.push_back(m_nClockHand);
							ASSERT(!m_vtClockBuffer[m_nClockHand]);
							//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
							break;
						}

						if (m_vtClockBufferWeight[m_nClockHand] > 0)
						{
							//obj->m_mtx.unlock();
							m_vtClockBufferWeight[m_nClockHand]--;
							m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
							continue;
						}

						if (m_vtClockBuffer[m_nClockHand].use_count() > 2)
						{
							m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
							continue;
						}

						auto ___obj = m_vtClockBuffer[m_nClockHand];

						// Check if the ___object is in use
						if (!___obj->m_mtx.try_lock())
						{
							m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
							continue;
						}

						if (___obj.use_count() > 3)
						{
							___obj->m_mtx.unlock();
							m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
							continue;
						}

						ASSERT(a_obj->m_uid != ___obj->m_uid);
						if (___obj->del == true/* && ___obj->m_ptrCoreObject != nullptr*/)
						{
							ASSERT(___obj->m_ptrCoreObject);
						/*	if (___obj->m_uid.getMediaType() > 1)
							{
								m_ptrStorage->remove(___obj->m_uid);
							}*/
							ASSERT(!___obj->m_bDirty);
							if (___obj->m_ptrCoreObject != nullptr) {
								//m_nUsedCacheCapacity--;
							}

							//why not delete it instead od flag.. why you used flag??
							___obj->deleteCoreObject();
							___obj->m_uidUpdated = std::nullopt;
							___obj->m_uid = ObjectUIDType(INT64_MAX);

							auto idx = ___obj->m_nPositionInCLOCK.load(std::memory_order_acquire);// ___obj->m_nPositionInCLOCK;
							if (idx != -1/* && idx != INT_MAX*/) //might be case where item had to bee added tot the queue but got deleted!!
							{
								ASSERT(m_vtClockBuffer[idx]);
								ASSERT(m_vtClockBufferWeight[idx] != -1);

								
								m_vtClockBufferWeight[idx] = -1;
								//___obj->m_nPositionInCLOCK = -1;
								___obj->m_nPositionInCLOCK.store(-1, std::memory_order_release);
								//___obj->reftofirstiteminpair = nullptr;

								//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();

								___obj->m_mtx.unlock();
								___obj.reset();

								
								break;
							}

							//ASSERT(___obj->m_nPositionInCLOCK == -1);

							//ASSERT(!___obj->reftofirstiteminpair);
							___obj->m_mtx.unlock();
							___obj.reset();

							m_vtClockBuffer[idx].reset();
							m_vtClockBuffer[idx] = nullptr;

							//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
							break;
						}

						ASSERT(m_vtClockBuffer[m_nClockHand]);
						ASSERT(a_obj->m_uid != ___obj->m_uid);

						if (___obj->m_ptrCoreObject == nullptr)
						{
							ASSERT(false);
							/*
							ASSERT(___obj->m_nPositionInCLOCK == m_nClockHand);

							//ASSERT(false); // should not happen!!
							if (___obj->m_nPositionInCLOCK != -1)
							{
								ASSERT(m_vtClockBuffer[m_nClockHand]);
								ASSERT(m_vtClockBufferWeight[m_nClockHand] != -1);

								//chcnk this
								//vtEmptySlots.push_back(m_nClockHand);

								m_vtClockBufferWeight[m_nClockHand] = -1;
								
							}
							else
							{
								ASSERT(!m_vtClockBuffer[m_nClockHand]);
								ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);
							}

							___obj->m_nPositionInCLOCK = -1;
							___obj->reftofirstiteminpair = nullptr;


							___obj->m_mtx.unlock();

							ASSERT(obj->m_uid != ___obj->m_uid);
							m_vtClockBuffer[m_nClockHand].reset();
							m_vtClockBuffer[m_nClockHand] = nullptr;

							//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
							*/
							break;
							
						}

						if (___obj->havedependentsincache())
						{
							m_vtClockBufferWeight[m_nClockHand] = 0;
							___obj->m_mtx.unlock();
							m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
							continue;
						}

						ASSERT(___obj->m_ptrCoreObject);

						vtObjects.push_back(___obj);

						//vtEmptySlots.push_back(m_nClockHand);

						ASSERT(___obj->m_nPositionInCLOCK.load(std::memory_order_acquire) == m_nClockHand);

						m_vtClockBufferWeight[m_nClockHand] = -1;

						___obj->m_nPositionInCLOCK.store(-1, std::memory_order_release);
						//___obj->m_nPositionInCLOCK = -1;
						//___obj->reftofirstiteminpair = nullptr;

						//ASSERT(obj);
						ASSERT(a_obj->m_uid != ___obj->m_uid);
						m_vtClockBuffer[m_nClockHand].reset();
						m_vtClockBuffer[m_nClockHand] = nullptr;

						ASSERT(m_vtClockBufferWeight[m_nClockHand] == -1);

						//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						break;
					}
				}

				//if (a_obj)
				{
					ASSERT(a_obj->m_ptrCoreObject);
					//int nSlotIdx = vtEmptySlots[pIdx++];
					int nSlotIdx = m_nClockHand;

					ASSERT(m_vtClockBufferWeight[nSlotIdx] == -1);
					ASSERT(!m_vtClockBuffer[nSlotIdx]);

					m_vtClockBuffer[nSlotIdx] = a_obj;	// use atomics!!!!

					a_obj->m_nPositionInCLOCK.store(nSlotIdx, std::memory_order_release);
					//a_obj->m_nPositionInCLOCK = nSlotIdx;
					//a_obj->reftofirstiteminpair = &m_vtClockBufferWeight[nSlotIdx];
				}
				//else
				{
				//ASSERT(!obj);
				}
			}
			
			//if (obj)
			//{
				//*a_obj->reftofirstiteminpair = m_vtClockQ[nIdx].second;
				m_vtClockBufferWeight[a_obj->m_nPositionInCLOCK.load(std::memory_order_acquire)] = m_vtClockQ[nIdx].second;
			//}
			//else
			//{
			//	ASSERT(!obj);
			//}
		}

		{
			ASSERT(nIdx == nQSiz);
			std::unique_lock<std::mutex> lock_cache(m_mtxCache);
			m_vtClockQ.erase(m_vtClockQ.begin(), m_vtClockQ.begin() + nIdx);
			
		}
		cv.notify_all();
		//vtEmptySlots.erase(vtEmptySlots.begin(), vtEmptySlots.begin() + pIdx);

		int f = 0;
		for( auto& obj: vtObjects)
		{
			if (obj->m_ptrCoreObject == nullptr || obj->del)
			{
				ASSERT(false);
				auto idx = obj->m_nPositionInCLOCK.load(std::memory_order_acquire);// obj->m_nPositionInCLOCK;
				if (idx != -1) //might be case where item had to bee added tot the queue but got deleted!!
				{
					ASSERT(m_vtClockBuffer[idx]);
					ASSERT(m_vtClockBufferWeight[idx] != -1);

					m_vtClockBuffer[idx].reset();
					m_vtClockBuffer[idx] = nullptr;
					m_vtClockBufferWeight[idx] = -1;
					//obj->m_nPositionInCLOCK = -1;
					obj->m_nPositionInCLOCK.store(-1, std::memory_order_release);
					//obj->reftofirstiteminpair = nullptr;

					continue;
				}


				//ASSERT(!obj->reftofirstiteminpair);
				obj->m_mtx.unlock();
				continue;
			}

			ASSERT(obj->m_nPositionInCLOCK.load(std::memory_order_acquire) == -1);
			//ASSERT(!obj->reftofirstiteminpair);

			ASSERT(!obj->havedependentsincache());

			if (obj->hasUpdatesToBeFlushed())
			{
				ObjectUIDType uidUpdated;
				if (m_ptrStorage->addObject(obj->m_uid, obj, uidUpdated) != CacheErrorCode::Success)
				{
					//std::cout << obj->m_nPositionInCLOCK << std::endl;
					/*if (obj->evict) std::cout << "evict" << std::endl;
					if (obj->reload) std::cout << "reload" << std::endl;
					if (obj->del2) std::cout << "del2" << std::endl;
					if (obj->del) std::cout << "del" << std::endl;*/
					//throw std::logic_error("Critical: failed to add object to storage during eviction.");
				}
				obj->m_uidUpdated = uidUpdated;
			}

			obj->m_bDirty = false;
			ASSERT(obj->m_ptrCoreObject);
			obj->deleteCoreObject();
			//obj->m_bReferenced = false;
			obj->m_nFlushPriority = 0;
			//obj->m_nPositionInCLOCK = -1;
			obj->m_nPositionInCLOCK.store(-1, std::memory_order_release);
			//obj->reftofirstiteminpair = nullptr;
			obj->evict = true;
			obj->m_mtx.unlock();
			obj.reset();
			f++;
			//m_nUsedCacheCapacity--;
		}

		vtObjects.clear();
		//lock_cache.unlock();
				
		{
			std::unique_lock<std::mutex> lock_cache(m_mtxCache);
			m_nUsedCacheCapacity -= f;
		}

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
		//return;
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

				if (obj->del == true/* && obj->m_ptrCoreObject != nullptr*/)
				{
					//ASSERT(ptrObject->m_uid.getMediaType() > 1);
				/*	if (obj->m_uid.getMediaType() > 1)
					{
						m_ptrStorage->remove(obj->m_uid);
					}*/

					if (obj->m_ptrCoreObject != nullptr) {
						//m_nUsedCacheCapacity--;
					}

					//why not delete it instead od flag.. why you used flag??
					obj->deleteCoreObject();
					obj->m_uidUpdated = std::nullopt;
					obj->m_uid = ObjectUIDType(INT64_MAX);


					auto idx = obj->m_nPositionInCLOCK.load(std::memory_order_acquire);
					if (idx != -1/* && idx != INT_MAX*/) //might be case where item had to bee added tot the queue but got deleted!!
					{
						ASSERT(m_vtClockBuffer[idx]);
						ASSERT(m_vtClockBufferWeight[idx] != -1);


						m_vtClockBufferWeight[idx] = -1;
						obj->m_nPositionInCLOCK.store(-1, std::memory_order_release);
						//obj->m_nPositionInCLOCK = -1;
						//obj->reftofirstiteminpair = nullptr;

						//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();

						obj->m_mtx.unlock();
						obj.reset();

						m_vtClockBuffer[idx].reset();
						m_vtClockBuffer[idx] = nullptr;

						m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
						continue;
					}

					//ASSERT(obj->m_nPositionInCLOCK == -1);

					//ASSERT(!obj->reftofirstiteminpair);
					obj->m_mtx.unlock();
					obj.reset();
					//m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
					continue;
				}

				if (obj->m_ptrCoreObject == nullptr)
				{
					//obj->m_nPositionInCLOCK = -1;
					obj->m_nPositionInCLOCK.store(-1, std::memory_order_release);
					m_vtClockBuffer[m_nClockHand].reset();
					m_vtClockBuffer[m_nClockHand] = nullptr;
					m_vtClockBufferWeight[m_nClockHand] = -1;

					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
					continue;
				}

				/*if (obj.use_count() > 2)
				{
					for (int o = 0; o < m_nCacheCapacity; o++) {
						if (m_vtClockBufferWeight[o] == -1)
							continue;

						if (o == m_nClockHand)
							continue;
						ASSERT(obj->m_uid != m_vtClockBuffer[o]->m_uid);
					}
					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
					continue;
				}*/

#ifdef __CONCURRENT__
				// Check if the object is in use
				if (!obj->m_mtx.try_lock())
				{
					m_nClockHand = (m_nClockHand + 1) % m_vtClockBuffer.size();
					continue;
				}
#endif //__CONCURRENT__

				if (obj->havedependentsincache())
				{
					m_vtClockBufferWeight[m_nClockHand] = 0;
					obj->m_mtx.unlock();
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

				auto idx = obj->m_nPositionInCLOCK.load(std::memory_order_acquire);// obj->m_nPositionInCLOCK;

				obj->deleteCoreObject();
				obj->m_nFlushPriority = 0;
				obj->m_nPositionInCLOCK.store(-1, std::memory_order_release);
				//obj->m_nPositionInCLOCK = -1;
				//*obj->reftofirstiteminpair = -1;
				

#ifdef __CONCURRENT__
				obj->m_mtx.unlock();
#endif //__CONCURRENT__
				obj.reset();
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
		std::vector<int> vtEmptySlots;
		vtEmptySlots.reserve(ptrSelf->m_nCacheCapacity);

		do
		{
			ptrSelf->flushItemsToStorage(vtEmptySlots);

			std::this_thread::sleep_for(1ms);

		} while (!ptrSelf->m_bStop);
	}
#endif //__CONCURRENT__
};
