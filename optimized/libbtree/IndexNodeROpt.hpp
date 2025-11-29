#pragma once
#include <memory>
#include <vector>
#include <string>
#include <map>
#include <sstream>
#include <iterator>
#include <iostream>
#include <cmath>
#include <optional>
#include <iostream>
#include <fstream>
#include "ErrorCodes.h"
#include <mutex>
#include <shared_mutex>
#include <cstring>
#include "validityasserts.h"

using namespace std;

#define MILLISEC_CHECK_FOR_FREQUENT_REQUESTS_TO_MEMORY 100
#define ACCESSED_FREQUENCY_AS_PER_THE_TIME_CHECK 5

using namespace std;

template <typename Traits>
class IndexNodeROpt
{
public:
	using KeyType = typename Traits::KeyType;
	using ValueType = typename Traits::ValueType;
	using ObjectUIDType = typename Traits::ObjectUIDType;
	using DataNodeType = typename Traits::DataNodeType;
	using CacheObjectType = typename Traits::ObjectType;
	using CacheObject = typename Traits::ObjectType;
	using CacheType = typename Traits::CacheType;

	static const uint8_t UID = Traits::IndexNodeUID;

private:
	typedef IndexNodeROpt<Traits> SelfType;
	typedef CacheType::ObjectTypePtr CacheObjectPtr;

public:
	struct PivotData
	{
		ObjectUIDType uid;
		CacheObjectPtr ptr;
	};

	struct PivotDataEx
	{
		uint16_t idx;
		PivotData data;
	};

	struct RAWDATA
	{
		uint8_t nUID;
		uint16_t nTotalPivots;
		const KeyType* ptrPivots;
		const ObjectUIDType* ptrChildren;
		std::vector<PivotDataEx> vtCachedChildren;

		uint8_t nCounter;

#ifdef _MSC_VER
		std::chrono::time_point<std::chrono::steady_clock> tLastAccessTime;
#else //_MSC_VER
		std::chrono::time_point<std::chrono::system_clock> tLastAccessTime;
#endif //_MSC_VER

		~RAWDATA()
		{
			//delete ptrPivots;
			//delete ptrChildren;

			ptrPivots = NULL;
			ptrChildren = NULL;
		}

		RAWDATA(const char* szData, uint32_t nDataLen, uint16_t nBlockSize)
		{
			nUID = szData[0];
			nTotalPivots = *reinterpret_cast<const uint16_t*>(&szData[1]);

			// Step 1: Calculate aligned offsets
			size_t offset_key_count = sizeof(uint8_t);
			size_t offset_keys = (offset_key_count + sizeof(uint16_t) + alignof(KeyType) - 1) & ~(alignof(KeyType) - 1);
			size_t keys_size = nTotalPivots * sizeof(KeyType);

			size_t offset_children = (offset_keys + keys_size + alignof(ObjectUIDType) - 1) & ~(alignof(ObjectUIDType) - 1);

			// Step 2: Assign aligned pointers
			ptrPivots = reinterpret_cast<const KeyType*>(szData + offset_keys);
			ptrChildren = reinterpret_cast<const ObjectUIDType*>(szData + offset_children);

			//nUID = szData[0];
			//nTotalPivots = *reinterpret_cast<const uint16_t*>(&szData[1]);
			//ptrPivots = reinterpret_cast<const KeyType*>(szData + sizeof(uint8_t) + sizeof(uint16_t));
			//ptrChildren = reinterpret_cast<const ObjectUIDType*>(szData + sizeof(uint8_t) + sizeof(uint16_t) + (nTotalPivots * sizeof(KeyType)));

			nCounter = 0;
			tLastAccessTime = std::chrono::high_resolution_clock::now();
		}
	};

	typedef std::vector<KeyType>::const_iterator KeyIterator;
	typedef std::vector<PivotData>::const_iterator PivotIterator;

private:
	uint16_t m_nDegree;
	std::vector<KeyType> m_vtKeys;
	std::vector<PivotData> m_vtPivots;

	RAWDATA* m_ptrRawData = nullptr;

public:
	~IndexNodeROpt()
	{
		m_vtKeys.clear();
		m_vtPivots.clear();

		if (m_ptrRawData != nullptr)
		{
			delete m_ptrRawData;
			m_ptrRawData = nullptr;
		}
	}

	IndexNodeROpt(uint16_t nDegree)
		: m_nDegree(nDegree)
		, m_ptrRawData(nullptr)
	{
	}

	IndexNodeROpt(uint16_t nDegree, const char* szData, uint32_t nDataLen, uint16_t nBlockSize)
		: m_nDegree(nDegree)
		, m_ptrRawData(nullptr)
	{
		m_vtKeys.reserve(0);
		m_vtPivots.reserve(0);

		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<typename ObjectUIDType::NodeUID>::value &&
			std::is_standard_layout<typename ObjectUIDType::NodeUID>::value)
		{
			m_ptrRawData = new RAWDATA(szData, nDataLen, nBlockSize);

			ASSERT(UID == m_ptrRawData->nUID);

			//moveDataToDRAM();
		}
		else
		{
			static_assert(
				std::is_trivial<KeyType>::value &&
				std::is_standard_layout<KeyType>::value &&
				std::is_trivial<typename ObjectUIDType::NodeUID>::value &&
				std::is_standard_layout<typename ObjectUIDType::NodeUID>::value,
				"Non-POD type is provided. Kindly implement custome de/serializer.");
		}
	}

	IndexNodeROpt(uint16_t nDegree, KeyIterator itBeginPivots, KeyIterator itEndPivots, PivotIterator itBeginChildren, PivotIterator itEndChildren)
		: m_nDegree(nDegree)
		, m_ptrRawData(nullptr)
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		m_vtKeys.reserve(std::max<size_t>(
			static_cast<size_t>(2 * m_nDegree + 1),
			static_cast<size_t>(std::distance(itBeginPivots, itEndPivots))
			));

		m_vtPivots.reserve(std::max<size_t>(
			static_cast<size_t>(2 * m_nDegree + 2),
			static_cast<size_t>(std::distance(itBeginChildren, itEndChildren))
			));
#endif //__PROD__

		m_vtKeys.assign(itBeginPivots, itEndPivots);
		m_vtPivots.assign(itBeginChildren, itEndChildren);
	}

	IndexNodeROpt(uint16_t nDegree, const KeyType& pivotKey, const ObjectUIDType& uidLHSNode, const CacheObjectPtr ptrLHSNode, const ObjectUIDType& uidRHSNode, const CacheObjectPtr ptrRHSNode)
		: m_nDegree(nDegree)
		, m_ptrRawData(nullptr)
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		m_vtKeys.reserve(2 * m_nDegree + 1);
		m_vtPivots.reserve(2 * m_nDegree + 2);
#endif //__PROD__

		m_vtKeys.push_back(pivotKey);
		m_vtPivots.push_back(PivotData(uidLHSNode, ptrLHSNode));
		m_vtPivots.push_back(PivotData(uidRHSNode, ptrRHSNode));
	}

public:
	inline void movePivotsToDRAM()
	{
		ASSERT(m_ptrRawData != nullptr);

#ifdef __PROD__
		fix this
#else //__PROD__
		m_vtKeys.reserve(std::max<size_t>(static_cast<size_t>(2 * m_nDegree + 1), static_cast<size_t>(m_ptrRawData->nTotalPivots)));
#endif //__PROD__

		m_vtKeys.resize(m_ptrRawData->nTotalPivots);

		memcpy(m_vtKeys.data(), m_ptrRawData->ptrPivots, m_ptrRawData->nTotalPivots * sizeof(KeyType));

		m_ptrRawData->ptrPivots = nullptr;
	}

	inline void movePivotsDataToDRAM()
	{
		ASSERT(m_ptrRawData != nullptr);

#ifdef __PROD__
		fix this
#else //__PROD__
		m_vtPivots.reserve(std::max<size_t>(static_cast<size_t>(2 * m_nDegree + 2), static_cast<size_t>(m_ptrRawData->nTotalPivots + 1)));
#endif //__PROD__

		m_vtPivots.resize(m_ptrRawData->nTotalPivots + 1);

		for (size_t nIdx = 0; nIdx <= m_ptrRawData->nTotalPivots; nIdx++)
		{
			ObjectUIDType uid;
			memcpy(&uid, m_ptrRawData->ptrChildren + nIdx, sizeof(ObjectUIDType));

			m_vtPivots[nIdx] = PivotData(uid, nullptr);
		}

		for (size_t nIdx = 0; nIdx < m_ptrRawData->vtCachedChildren.size(); nIdx++)
		{
			m_vtPivots[m_ptrRawData->vtCachedChildren[nIdx].idx].uid = m_ptrRawData->vtCachedChildren[nIdx].data.uid;
			m_vtPivots[m_ptrRawData->vtCachedChildren[nIdx].idx].ptr = m_ptrRawData->vtCachedChildren[nIdx].data.ptr;
		}
	}

	inline void moveDataToDRAM()
	{
		ASSERT(m_ptrRawData != nullptr);

		if (m_ptrRawData->ptrPivots != nullptr)
		{
			movePivotsToDRAM();
		}

		movePivotsDataToDRAM();

		delete m_ptrRawData;
		m_ptrRawData = nullptr;
	}

	// Serializes the node's data into a char buffer
	inline void serialize(char*& szData, uint32_t& nDataLen, uint16_t nBlockSize, void*& ptrBlockAppendOffset, bool& bAlignedAllocation)
	{
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<typename ObjectUIDType::NodeUID>::value &&
			std::is_standard_layout<typename ObjectUIDType::NodeUID>::value)
		{
			bAlignedAllocation = true;

			uint16_t nKeyCount = m_vtKeys.size();
			uint16_t nValueCount = m_vtPivots.size();

			size_t nKeysSize = nKeyCount * sizeof(KeyType);
			size_t nHeaderSize = sizeof(uint8_t) + sizeof(uint16_t);

			size_t nKeysOffset = (nHeaderSize + alignof(KeyType) - 1) & ~(alignof(KeyType) - 1);
			size_t nValuesOffset = (nKeysOffset + nKeysSize + alignof(ObjectUIDType) - 1) & ~(alignof(ObjectUIDType) - 1);

			nDataLen = nValuesOffset + nValueCount * sizeof(ObjectUIDType);

#ifdef _MSC_VER
			szData = (char*)_aligned_malloc(nDataLen, std::max(alignof(KeyType), alignof(ObjectUIDType)));
#else
			szData = (char*)std::aligned_alloc(std::max(alignof(KeyType), alignof(ObjectUIDType)), nDataLen);
#endif

			memset(szData, 0, nDataLen);

			memcpy(szData, &UID, sizeof(uint8_t));
			memcpy(szData + sizeof(uint8_t), &nKeyCount, sizeof(uint16_t));
			memcpy(szData + nKeysOffset, m_vtKeys.data(), nKeysSize);

			char* ptrValue = szData + nValuesOffset;
			for (size_t nIdx = 0; nIdx < nValueCount; ++nIdx)
			{
				const ObjectUIDType& uid = (m_vtPivots[nIdx].ptr != nullptr && m_vtPivots[nIdx].ptr->m_uidUpdated != std::nullopt)
					? *m_vtPivots[nIdx].ptr->m_uidUpdated
					: m_vtPivots[nIdx].uid;

				memcpy(ptrValue + nIdx * sizeof(ObjectUIDType), &uid, sizeof(ObjectUIDType));
			}

#ifdef __ENABLE_ASSERTS__
			ASSERT(m_vtKeys.size() + 1 == m_vtPivots.size());

			SelfType validate(m_nDegree, szData, nDataLen, nBlockSize);
			validate.moveDataToDRAM();
			for (int idx = 0; idx < m_vtPivots.size(); idx++)
			{
				if (m_vtPivots[idx].ptr != nullptr && m_vtPivots[idx].ptr->m_uidUpdated != std::nullopt)
				{
					ASSERT(m_vtPivots[idx].ptr->m_uidUpdated == validate.m_vtPivots[idx].uid);
					ASSERT(m_vtPivots[idx].ptr->m_uidUpdated.value().getMediaType() >= 2);
				}
				else
				{
					ASSERT(m_vtPivots[idx].uid == validate.m_vtPivots[idx].uid);
					ASSERT(m_vtPivots[idx].uid.getMediaType() >= 2);
				}
			}
#endif //__ENABLE_ASSERTS__
		}
		else
		{
			static_assert(
				std::is_trivial<KeyType>::value &&
				std::is_standard_layout<KeyType>::value &&
				std::is_trivial<typename ObjectUIDType::NodeUID>::value &&
				std::is_standard_layout<typename ObjectUIDType::NodeUID>::value,
				"Non-POD type is provided. Kindly implement custome de/serializer.");
		}
	}

public:
	inline bool hasUIDUpdates() const
	{
		if (m_ptrRawData != nullptr)
		{
			return std::any_of(m_ptrRawData->vtCachedChildren.begin(), m_ptrRawData->vtCachedChildren.end(), [](const auto& pivot) {
				return pivot.data.ptr && pivot.data.ptr->m_uidUpdated != std::nullopt;
				});

			//for (const auto& value : m_ptrRawData->vtCachedChildren)
			//{
			//	if (value.data.ptr != nullptr && value.data.ptr->m_uidUpdated != std::nullopt)
			//	{
			//		return true;
			//	}
			//}

			//return false;
		}

		return std::any_of(m_vtPivots.begin(), m_vtPivots.end(), [](const auto& pivot) {
			return pivot.ptr && pivot.ptr->m_uidUpdated != std::nullopt;
			});

		//for (const auto& value : m_vtPivots)
		//{
		//	if (value.ptr != nullptr && value.ptr->m_uidUpdated != std::nullopt)
		//	{
		//		return true;
		//	}
		//}
		//return false;
	}

	inline bool canAccessPivotsDirectly()
	{
		if (m_ptrRawData == nullptr || m_ptrRawData->ptrPivots == nullptr)
			return false;

		auto now = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - m_ptrRawData->tLastAccessTime).count();

		m_ptrRawData->tLastAccessTime = now;

		if (duration < MILLISEC_CHECK_FOR_FREQUENT_REQUESTS_TO_MEMORY)
		{
			m_ptrRawData->nCounter++;

			if (m_ptrRawData->nCounter >= ACCESSED_FREQUENCY_AS_PER_THE_TIME_CHECK)
			{
				movePivotsToDRAM();
				return false;
			}
		}
		else
		{
			m_ptrRawData->nCounter = std::max(0, m_ptrRawData->nCounter - 1);
			//m_ptrRawData->nCounter = 0; // reset counter if more than 100 nanoseconds have passed		
		}

		return true;
	}

	inline size_t getKeysCount() const
	{
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalPivots;
		}

		return m_vtKeys.size();
	}

	inline size_t getPivotDataIdx(const KeyType& key)
	{
		if (canAccessPivotsDirectly())
		{
			auto it = std::upper_bound(m_ptrRawData->ptrPivots, m_ptrRawData->ptrPivots + m_ptrRawData->nTotalPivots, key);
			return std::distance(m_ptrRawData->ptrPivots, it);
		}
		else
		{
			auto it = std::upper_bound(m_vtKeys.begin(), m_vtKeys.end(), key);
			return std::distance(m_vtKeys.begin(), it);
		}
	}

	inline PivotData& getPivotDataByIdx(uint16_t nIdx)
	{
		if (m_ptrRawData != nullptr)
		{
			auto it = std::lower_bound(m_ptrRawData->vtCachedChildren.begin(), m_ptrRawData->vtCachedChildren.end(), nIdx,
				[](const PivotDataEx& data, uint16_t key) { return data.idx < key; });

			if (it != m_ptrRawData->vtCachedChildren.end() && it->idx == nIdx)
			{
				return (*it).data;
			}
			else
			{
				ObjectUIDType uid;
				memcpy(&uid, m_ptrRawData->ptrChildren + nIdx, sizeof(ObjectUIDType));

				PivotDataEx dt(nIdx, { uid, nullptr });
				auto itInserted = m_ptrRawData->vtCachedChildren.insert(it, dt);

				return (*itInserted).data;
			}
		}

		return m_vtPivots[nIdx];
	}

	inline const void getFirstChildDetails(ObjectUIDType& uid, CacheObjectPtr& ptr, uint16_t nIdx = 0)
	{
		const PivotData& oData = getPivotDataByIdx(nIdx);

		uid = oData.uid;
		ptr = oData.ptr;
	}

	template <typename CacheType>
#ifdef __CONCURRENT__
	inline bool getChild(std::shared_ptr<CacheType>& ptrCache, const KeyType& key, ObjectUIDType& uid, CacheObjectPtr& ptr, std::vector<std::unique_lock<std::shared_mutex>>& vtLocks)
#else //__CONCURRENT__
	inline bool getChild(std::shared_ptr<CacheType>& ptrCache, const KeyType& key, ObjectUIDType& uid, CacheObjectPtr& ptr)
#endif //__CONCURRENT__
	{
		bool bUpdate = false;

		PivotData& oData = getPivotDataByIdx(getPivotDataIdx(key));

		if (oData.ptr == nullptr)
		{
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;
			ptrCache->getObject(m_nDegree, oData.uid, oData.ptr);

#ifdef __CONCURRENT__
			vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(oData.ptr->m_mtx));
#endif //__CONCURRENT__
		}
		else
		{
#ifdef __CONCURRENT__
			vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(oData.ptr->m_mtx));
#endif //__CONCURRENT__

			if (oData.ptr->m_ptrCoreObject == nullptr)
			{
				if (oData.ptr->m_uidUpdated != std::nullopt)
				{
					bUpdate = true;
					oData.uid = *oData.ptr->m_uidUpdated;
				}

				ptrCache->getCoreObject(m_nDegree, oData.uid, oData.ptr);
			}
		}

		uid = oData.uid;
		ptr = oData.ptr;


		ASSERT(oData.ptr->m_ptrCoreObject != nullptr);

		return bUpdate;
	}

#ifdef __CONCURRENT__
	inline void getChild(const KeyType& key, ObjectUIDType& uid, CacheObjectPtr& ptr, std::vector<std::unique_lock<std::shared_mutex>>& vtLocks)
#else //__CONCURRENT__
	inline void getChild(const KeyType& key, ObjectUIDType& uid, CacheObjectPtr& ptr)
#endif //__CONCURRENT__
	{
		PivotData& oData = getPivotDataByIdx(getPivotDataIdx(key));

#ifdef __CONCURRENT__
		vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(oData.ptr->m_mtx));
#endif //__CONCURRENT__

		uid = oData.uid;
		ptr = oData.ptr;


		ASSERT(oData.ptr->m_ptrCoreObject != nullptr);
	}

	template <typename CacheType>
#ifdef __CONCURRENT__
	inline bool getChildAtIdx(std::shared_ptr<CacheType>& ptrCache, size_t idx, CacheObjectPtr& ptr, std::unique_lock<std::shared_mutex>& lock)
#else //__CONCURRENT__
	inline bool getChildAtIdx(std::shared_ptr<CacheType>& ptrCache, size_t idx, CacheObjectPtr& ptr)
#endif //__CONCURRENT__
	{
		bool bUpdate = false;

		PivotData& oData = getPivotDataByIdx(idx);

		if (oData.ptr == nullptr)
		{
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;
			ptrCache->getObject(m_nDegree, oData.uid, oData.ptr);

#ifdef __CONCURRENT__
			lock = std::unique_lock<std::shared_mutex>(oData.ptr->m_mtx);
#endif //__CONCURRENT__

		}
		else
		{
#ifdef __CONCURRENT__
			lock = std::unique_lock<std::shared_mutex>(oData.ptr->m_mtx);
#endif //__CONCURRENT__

			if (oData.ptr->m_ptrCoreObject == nullptr)
			{
				if (oData.ptr->m_uidUpdated != std::nullopt)
				{
					bUpdate = true;
					oData.uid = *oData.ptr->m_uidUpdated;
				}

				ptrCache->getCoreObject(m_nDegree, oData.uid, oData.ptr);
			}
		}

		ptr = oData.ptr;

		ASSERT(oData.ptr->m_ptrCoreObject != nullptr);

		return bUpdate;
	}

#ifdef __CONCURRENT__
	inline void getChildAtIdx(size_t idx, CacheObjectPtr& ptr, std::unique_lock<std::shared_mutex>& lock)
#else //__CONCURRENT__
	inline void getChildAtIdx(size_t idx, CacheObjectPtr& ptr)
#endif //__CONCURRENT__
	{
		PivotData& oData = getPivotDataByIdx(idx);

#ifdef __CONCURRENT__
		lock = std::unique_lock<std::shared_mutex>(oData.ptr->m_mtx);
#endif //__CONCURRENT__

		//uid = oData.uid;
		ptr = oData.ptr;

		ASSERT(oData.ptr->m_ptrCoreObject != nullptr);
	}

	inline bool canTriggerSplit() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalPivots == (2 * m_nDegree - 1);;
		}

		return m_vtKeys.size() == (2 * m_nDegree - 1);;
#endif //__PROD__
	}

	inline bool requireSplit() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalPivots > (2 * m_nDegree - 1);
		}

		return m_vtKeys.size() > (2 * m_nDegree - 1);
#endif //__PROD__
	}

	inline bool canTriggerMerge() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalPivots < m_nDegree;
		}

		return m_vtKeys.size() < m_nDegree;
#endif //__PROD__
	}

	inline bool requireMerge() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalPivots < (m_nDegree - 1);
		}

		return m_vtKeys.size() < (m_nDegree - 1);
#endif //__PROD__
	}

public:
	inline ErrorCode insert(const KeyType& pivotKey, const ObjectUIDType& uidSibling, CacheObjectPtr ptrSibling)
	{
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		auto it = std::lower_bound(m_vtKeys.begin(), m_vtKeys.end(), pivotKey);

		if (it != m_vtKeys.end() && *it == pivotKey) // lay dal ayte.. make sure ayte ke duplicate omanish?
			return ErrorCode::KeyAlreadyExists;

		auto nChildIdx = std::distance(m_vtKeys.begin(), it);

		m_vtKeys.insert(m_vtKeys.begin() + nChildIdx, pivotKey);
		m_vtPivots.insert(m_vtPivots.begin() + nChildIdx + 1, PivotData(uidSibling, ptrSibling));

		return ErrorCode::Success;
	}

	template <typename CacheType>
#ifdef __TREE_WITH_CACHE__
#ifndef __PROD__
	inline ErrorCode rebalanceIndexNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, SelfType* ptrChildNode, const KeyType& key, std::optional<ObjectUIDType>& uidObjectToDelete, CacheType::ObjectTypePtr& ptrObjectToDelete, std::optional<ObjectUIDType>& uidAffectedNode, CacheType::ObjectTypePtr& ptrAffectedNode)
#else //__PROD__
	inline ErrorCode rebalanceIndexNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, SelfType* ptrChildNode, const KeyType& key, std::optional<ObjectUIDType>& uidObjectToDelete, CacheType::ObjectTypePtr& ptrObjectToDelete, std::optional<ObjectUIDType>& uidAffectedNode, CacheType::ObjectTypePtr& ptrAffectedNode)
#endif //__PROD__
#else //__TREE_WITH_CACHE__
#ifndef __PROD__
	inline ErrorCode rebalanceIndexNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, SelfType* ptrChildNode, const KeyType& key, std::optional<ObjectUIDType>& uidObjectToDelete, CacheType::ObjectTypePtr& ptrObjectToDelete)
#else //__PROD__
	inline ErrorCode rebalanceIndexNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, SelfType* ptrChildNode, const KeyType& key, std::optional<ObjectUIDType>& uidObjectToDelete, CacheType::ObjectTypePtr& ptrObjectToDelete)
#endif //__PROD__
#endif //__TREE_WITH_CACHE__
	{
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		if (ptrChildNode->m_ptrRawData != nullptr)
		{
			ptrChildNode->moveDataToDRAM();
		}

		typedef typename CacheType::ObjectTypePtr ObjectTypePtr;

		ObjectTypePtr ptrLHSStorageObject = nullptr;
		ObjectTypePtr ptrRHSStorageObject = nullptr;

		SelfType* ptrLHSNode = nullptr;
		SelfType* ptrRHSNode = nullptr;

#ifdef __TREE_WITH_CACHE__
		uidAffectedNode = std::nullopt;
		ptrAffectedNode = nullptr;
#endif //__TREE_WITH_CACHE__

		size_t nChildIdx = getPivotDataIdx(key);

		if (nChildIdx > 0)
		{
#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock;
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;

#ifdef __CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx - 1, ptrLHSStorageObject, lock);	// Donot regarding the return value as the parent is already being set to dirty.
#else //__CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx - 1, ptrLHSStorageObject);
#endif //__CONCURRENT__			

#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
			this->getChildAtIdx(nChildIdx - 1, ptrLHSStorageObject, lock);	// Donot regarding the return value as the parent is already being set to dirty.
#else //__CONCURRENT__
			this->getChildAtIdx(nChildIdx - 1, ptrLHSStorageObject);
#endif //__CONCURRENT__			

#endif //__TREE_WITH_CACHE__

			ptrLHSNode = reinterpret_cast<SelfType*>(ptrLHSStorageObject->m_ptrCoreObject);

#ifdef __TREE_WITH_CACHE__
			if (uidUpdated != std::nullopt)
			{
				m_vtPivots[nChildIdx - 1].uid = *uidUpdated;
			}

			ptrLHSStorageObject->m_bDirty = true;

			uidAffectedNode = m_vtPivots[nChildIdx - 1].uid;
			ptrAffectedNode = ptrLHSStorageObject;
#endif //__TREE_WITH_CACHE__

#ifndef __PROD__
			if (ptrLHSNode->getKeysCount() > std::ceil(m_nDegree / 2.0f))	// TODO: macro?
#else //__PROD__
			if (ptrLHSNode->getKeysCount() > CEIL_HALF_FANOUT(Traits::Fanout))	// TODO: macro?
#endif //__PROD__
			{
				KeyType key;

				ptrChildNode->moveAnEntityFromLHSSibling(ptrLHSNode, m_vtKeys[nChildIdx - 1], key);

				m_vtKeys[nChildIdx - 1] = key;
				return ErrorCode::Success;
			}

			ptrLHSNode->mergeNodes(ptrChildNode, m_vtKeys[nChildIdx - 1]);

			uidObjectToDelete = m_vtPivots[nChildIdx].uid;
			ptrObjectToDelete = m_vtPivots[nChildIdx].ptr;
			ASSERT(uidObjectToDelete == uidChild);

			m_vtKeys.erase(m_vtKeys.begin() + nChildIdx - 1);
			m_vtPivots.erase(m_vtPivots.begin() + nChildIdx);

			return ErrorCode::Success;
		}

		if (nChildIdx < m_vtKeys.size())
		{
#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock;
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;

#ifdef __CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx + 1, ptrRHSStorageObject, lock);
#else //__CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx + 1, ptrRHSStorageObject);
#endif //__CONCURRENT__

#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
			this->getChildAtIdx(nChildIdx + 1, ptrRHSStorageObject, lock);
#else //__CONCURRENT__
			this->getChildAtIdx(nChildIdx + 1, ptrRHSStorageObject);
#endif //__CONCURRENT__

#endif //__TREE_WITH_CACHE__

			ptrRHSNode = reinterpret_cast<SelfType*>(ptrRHSStorageObject->m_ptrCoreObject);

#ifdef __TREE_WITH_CACHE__
			if (uidUpdated != std::nullopt)
			{
				m_vtPivots[nChildIdx + 1].uid = *uidUpdated;
			}

			ptrRHSStorageObject->m_bDirty = true;

			uidAffectedNode = m_vtPivots[nChildIdx + 1].uid;
			ptrAffectedNode = ptrRHSStorageObject;
#endif //__TREE_WITH_CACHE__

#ifndef __PROD__
			if (ptrRHSNode->getKeysCount() > std::ceil(m_nDegree / 2.0f))
#else //__PROD__
			if (ptrRHSNode->getKeysCount() > CEIL_HALF_FANOUT(Traits::Fanout))
#endif //__PROD__
			{
				KeyType key;

				ptrChildNode->moveAnEntityFromRHSSibling(ptrRHSNode, m_vtKeys[nChildIdx], key);

				m_vtKeys[nChildIdx] = key;
				return ErrorCode::Success;
			}

			ptrChildNode->mergeNodes(ptrRHSNode, m_vtKeys[nChildIdx]);

			ASSERT(uidChild == m_vtPivots[nChildIdx].uid);

			uidObjectToDelete = m_vtPivots[nChildIdx + 1].uid;
			ptrObjectToDelete = m_vtPivots[nChildIdx + 1].ptr;

			m_vtKeys.erase(m_vtKeys.begin() + nChildIdx);
			m_vtPivots.erase(m_vtPivots.begin() + nChildIdx + 1);

			return ErrorCode::Success;
		}

		std::cout << "Critical State: The rebalance logic for IndexNode failed." << std::endl;
		throw new std::logic_error(".....");   // TODO: critical log.
	}

	template <typename CacheType>
#ifdef __TREE_WITH_CACHE__
	inline ErrorCode rebalanceDataNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, DataNodeType* ptrChild, const KeyType& key, std::optional<ObjectUIDType>& uidObjectToDelete, CacheType::ObjectTypePtr& ptrObjectToDelete, std::optional<ObjectUIDType>& uidAffectedNode, CacheType::ObjectTypePtr& ptrAffectedNode)
#else //__TREE_WITH_CACHE__
	inline ErrorCode rebalanceDataNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, DataNodeType* ptrChild, const KeyType& key, std::optional<ObjectUIDType>& uidObjectToDelete, CacheType::ObjectTypePtr& ptrObjectToDelete)
#endif //__TREE_WITH_CACHE__
	{
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		if (ptrChild->m_ptrRawData != nullptr)
		{
			ptrChild->moveDataToDRAM();
		}

		typedef typename CacheType::ObjectTypePtr ObjectTypePtr;

		ObjectTypePtr ptrLHSStorageObject = nullptr;
		ObjectTypePtr ptrRHSStorageObject = nullptr;

		DataNodeType* ptrLHSNode = nullptr;
		DataNodeType* ptrRHSNode = nullptr;

#ifdef __TREE_WITH_CACHE__
		uidAffectedNode = std::nullopt;
		ptrAffectedNode = nullptr;
#endif //__TREE_WITH_CACHE__

		size_t nChildIdx = getPivotDataIdx(key);

		if (nChildIdx > 0)
		{
#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock;
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;
#ifdef __CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx - 1, ptrLHSStorageObject, lock);
#else //__CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx - 1, ptrLHSStorageObject);
#endif //__CONCURRENT__
#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
			this->getChildAtIdx(nChildIdx - 1, ptrLHSStorageObject, lock);
#else //__CONCURRENT__
			this->getChildAtIdx(nChildIdx - 1, ptrLHSStorageObject);
#endif //__CONCURRENT__
#endif //__TREE_WITH_CACHE__


			ptrLHSNode = reinterpret_cast<DataNodeType*>(ptrLHSStorageObject->m_ptrCoreObject);

#ifdef __TREE_WITH_CACHE__
			if (uidUpdated != std::nullopt)
			{
				m_vtPivots[nChildIdx - 1].uid = *uidUpdated;
			}

			ptrLHSStorageObject->m_bDirty = true;

			uidAffectedNode = m_vtPivots[nChildIdx - 1].uid;
			ptrAffectedNode = ptrLHSStorageObject;
#endif //__TREE_WITH_CACHE__

#ifndef __PROD__
			if (ptrLHSNode->getKeysCount() > std::ceil(m_nDegree / 2.0f))	// TODO: macro?
#else //__PROD__
			if (ptrLHSNode->getKeysCount() > CEIL_HALF_FANOUT(Traits::Fanout))	// TODO: macro?
#endif //__PROD__
			{
				KeyType key;

				ptrChild->moveAnEntityFromLHSSibling(ptrLHSNode, key);

				m_vtKeys[nChildIdx - 1] = key;

				return ErrorCode::Success;
			}

			ptrLHSNode->mergeNode(ptrChild);

			uidObjectToDelete = m_vtPivots[nChildIdx].uid;
			ptrObjectToDelete = m_vtPivots[nChildIdx].ptr;
			ASSERT(uidObjectToDelete == uidChild);

			m_vtKeys.erase(m_vtKeys.begin() + nChildIdx - 1);
			m_vtPivots.erase(m_vtPivots.begin() + nChildIdx);

			return ErrorCode::Success;
		}

		if (nChildIdx < m_vtKeys.size())
		{
#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock;
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;

#ifdef __CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx + 1, ptrRHSStorageObject, lock);
#else //__CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx + 1, ptrRHSStorageObject);
#endif //__CONCURRENT__

#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
			this->getChildAtIdx(nChildIdx + 1, ptrRHSStorageObject, lock);
#else //__CONCURRENT__
			this->getChildAtIdx(nChildIdx + 1, ptrRHSStorageObject);
#endif //__CONCURRENT__

#endif //__TREE_WITH_CACHE__

			ptrRHSNode = reinterpret_cast<DataNodeType*>(ptrRHSStorageObject->m_ptrCoreObject);

#ifdef __TREE_WITH_CACHE__
			if (uidUpdated != std::nullopt)
			{
				m_vtPivots[nChildIdx + 1].uid = *uidUpdated;
			}

			ptrRHSStorageObject->m_bDirty = true;

			uidAffectedNode = m_vtPivots[nChildIdx + 1].uid;
			ptrAffectedNode = ptrRHSStorageObject;
#endif //__TREE_WITH_CACHE__

#ifndef __PROD__
			if (ptrRHSNode->getKeysCount() > std::ceil(m_nDegree / 2.0f))
#else //__PROD__
			if (ptrRHSNode->getKeysCount() > CEIL_HALF_FANOUT(Traits::Fanout))
#endif //__PROD__
			{
				KeyType key;

				ptrChild->moveAnEntityFromRHSSibling(ptrRHSNode, key);

				m_vtKeys[nChildIdx] = key;
				return ErrorCode::Success;
			}

			ptrChild->mergeNode(ptrRHSNode);

			uidObjectToDelete = m_vtPivots[nChildIdx + 1].uid;
			ptrObjectToDelete = m_vtPivots[nChildIdx + 1].ptr;

			m_vtKeys.erase(m_vtKeys.begin() + nChildIdx);
			m_vtPivots.erase(m_vtPivots.begin() + nChildIdx + 1);

			return ErrorCode::Success;
		}

		std::cout << "Critical State: The rebalance logic for DataNode failed." << std::endl;
		throw new std::logic_error(".....");   // TODO: critical log.
	}

	template <typename CacheType, typename CacheObjectTypePtr>
	inline ErrorCode split(std::shared_ptr<CacheType> ptrCache, std::optional<ObjectUIDType>& uidSibling, CacheObjectTypePtr& ptrSibling, KeyType& pivotKeyForParent)
	{
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		size_t nMid = m_vtKeys.size() / 2;

#ifdef __PROD__
		fix this
#else //__PROD__
		ptrCache->template createObjectOfType<SelfType>(uidSibling, ptrSibling, m_nDegree,
			m_vtKeys.begin() + nMid + 1, m_vtKeys.end(), m_vtPivots.begin() + nMid + 1, m_vtPivots.end());
#endif //__PROD__

		if (!uidSibling)
		{
			return ErrorCode::Error;
		}

		pivotKeyForParent = m_vtKeys[nMid];

		m_vtKeys.resize(nMid);
		m_vtPivots.resize(nMid + 1);

		return ErrorCode::Success;
	}

	inline void moveAnEntityFromLHSSibling(SelfType* ptrLHSSibling, KeyType& pivotKeyForEntity, KeyType& pivotKeyForParent)
	{
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		if (ptrLHSSibling->m_ptrRawData != nullptr)
		{
			ptrLHSSibling->moveDataToDRAM();
		}

		KeyType key = ptrLHSSibling->m_vtKeys.back();
		PivotData value = ptrLHSSibling->m_vtPivots.back();

		ptrLHSSibling->m_vtKeys.pop_back();
		ptrLHSSibling->m_vtPivots.pop_back();

		ASSERT(ptrLHSSibling->m_vtKeys.size() > 0);

		m_vtKeys.insert(m_vtKeys.begin(), pivotKeyForEntity);
		m_vtPivots.insert(m_vtPivots.begin(), value);

		pivotKeyForParent = key;
	}

	inline void moveAnEntityFromRHSSibling(SelfType* ptrRHSSibling, KeyType& pivotKeyForEntity, KeyType& pivotKeyForParent)
	{
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		if (ptrRHSSibling->m_ptrRawData != nullptr)
		{
			ptrRHSSibling->moveDataToDRAM();
		}

		KeyType key = ptrRHSSibling->m_vtKeys.front();
		PivotData value = ptrRHSSibling->m_vtPivots.front();

		ptrRHSSibling->m_vtKeys.erase(ptrRHSSibling->m_vtKeys.begin());
		ptrRHSSibling->m_vtPivots.erase(ptrRHSSibling->m_vtPivots.begin());

		ASSERT(ptrRHSSibling->m_vtKeys.size() > 0);

		m_vtKeys.push_back(pivotKeyForEntity);
		m_vtPivots.push_back(value);

		pivotKeyForParent = key;
	}

	inline void mergeNodes(SelfType* ptrSibling, KeyType& pivotKey)
	{
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		if (ptrSibling->m_ptrRawData != nullptr)
		{
#ifdef __PROD__
			fix this
#else //__PROD__
			std::size_t nRequiredCapacity = m_vtKeys.size() + ptrSibling->m_ptrRawData->nTotalPivots + 1;
			if (m_vtKeys.capacity() < nRequiredCapacity)
				m_vtKeys.reserve(nRequiredCapacity);

			nRequiredCapacity = m_vtPivots.size() + ptrSibling->m_ptrRawData->nTotalPivots;
			if (m_vtPivots.capacity() < nRequiredCapacity)
				m_vtPivots.reserve(nRequiredCapacity);
#endif //__PROD__

			size_t nOldPivotsCount = m_vtPivots.size();

			m_vtKeys.push_back(pivotKey);

			if (ptrSibling->m_ptrRawData->ptrPivots != nullptr)
			{
				m_vtKeys.insert(m_vtKeys.end(), ptrSibling->m_ptrRawData->ptrPivots, ptrSibling->m_ptrRawData->ptrPivots + ptrSibling->m_ptrRawData->nTotalPivots);
			}
			else
			{
				m_vtKeys.insert(m_vtKeys.end(), ptrSibling->m_vtKeys.begin(), ptrSibling->m_vtKeys.end());
			}

			for (size_t i = 0; i <= ptrSibling->m_ptrRawData->nTotalPivots; ++i) 
			{
				m_vtPivots.push_back(PivotData(ptrSibling->m_ptrRawData->ptrChildren[i], nullptr));
			}

			for (size_t idx = 0; idx < ptrSibling->m_ptrRawData->vtCachedChildren.size(); idx++)
			{
				m_vtPivots[ptrSibling->m_ptrRawData->vtCachedChildren[idx].idx + nOldPivotsCount].uid = ptrSibling->m_ptrRawData->vtCachedChildren[idx].data.uid;
				m_vtPivots[ptrSibling->m_ptrRawData->vtCachedChildren[idx].idx + nOldPivotsCount].ptr = ptrSibling->m_ptrRawData->vtCachedChildren[idx].data.ptr;
			}
		}
		else
		{
#ifdef __PROD__
			fix this
#else //__PROD__
			std::size_t nRequiredCapacity = m_vtKeys.size() + ptrSibling->m_vtKeys.size() + 1;
			if (m_vtKeys.capacity() < nRequiredCapacity)
				m_vtKeys.reserve(nRequiredCapacity);

			nRequiredCapacity = m_vtPivots.size() + ptrSibling->m_vtPivots.size();
			if (m_vtPivots.capacity() < nRequiredCapacity)
				m_vtPivots.reserve(nRequiredCapacity);
#endif //__PROD__

			m_vtKeys.push_back(pivotKey);
			m_vtKeys.insert(m_vtKeys.end(), ptrSibling->m_vtKeys.begin(), ptrSibling->m_vtKeys.end());
			m_vtPivots.insert(m_vtPivots.end(), ptrSibling->m_vtPivots.begin(), ptrSibling->m_vtPivots.end());
		}
	}

public:
	template <typename CacheType, typename CacheObjectType>
	bool print(std::ofstream& os, std::shared_ptr<CacheType>& ptrCache, size_t nLevel, string stPrefix)
	{
		bool bUpdate = false;

		if (m_ptrRawData != nullptr)
		{
			std::cout << "Critical State: " << __LINE__ << std::endl;
			throw new std::logic_error(".....");   // TODO: critical log.
			throw new std::logic_error("fix...");
			return printRONode(os, ptrCache, nLevel, stPrefix);
		}

		uint8_t nSpaceCount = 7;

#ifdef __TREE_WITH_CACHE__		
		std::vector<CacheObjectType> vtAccessedNodes;
#endif //__TREE_WITH_CACHE__
		stPrefix.append(std::string(nSpaceCount - 1, ' '));
		stPrefix.append("|");
		for (size_t nIndex = 0; nIndex < m_vtPivots.size(); nIndex++)
		{
			os
				<< " "
				<< stPrefix
				<< std::endl;

			os
				<< " "
				<< stPrefix
				<< std::string(nSpaceCount, '-').c_str();

			if (nIndex < m_vtKeys.size())
			{
				os
					<< " < ("
					<< m_vtKeys[nIndex] << ")";
			}
			else
			{
				os
					<< " >= ("
					<< m_vtKeys[nIndex - 1]
					<< ")";
			}

			CacheObjectType ptrNode = nullptr;

#ifdef __TREE_WITH_CACHE__
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;

			if (this->getChildAtIdx<CacheType>(ptrCache, nIndex, ptrNode))
			{
				bUpdate = true;
			}

			//ptrCache->getObject(m_vtPivots[nIndex].uid, ptrNode, uidUpdated);

			//if (uidUpdated != std::nullopt)
			//{
			//	m_vtPivots[nIndex].uid = *uidUpdated;
			//}
#else //__TREE_WITH_CACHE__
			this->getChildAtIdx(nIndex, ptrNode);
			//ptrCache->getObject(m_vtPivots[nIndex], ptrNode);
#endif //__TREE_WITH_CACHE__

			os << std::endl;

#ifdef __TREE_WITH_CACHE__			
			vtAccessedNodes.push_back(ptrNode);
#endif //__TREE_WITH_CACHE__
			if (ptrNode->getObjectType() == SelfType::UID)
				//if (std::holds_alternative<shared_ptr<SelfType>>(ptrNode->m_ptrCoreObject))
			{
				//shared_ptr<SelfType> ptrIndexNode = std::get<shared_ptr<SelfType>>(ptrNode->m_ptrCoreObject);
				SelfType* ptrIndexNode = reinterpret_cast<SelfType*>(ptrNode->m_ptrCoreObject);
				if (ptrIndexNode->template print<CacheType, CacheObjectType>(os, ptrCache, nLevel + 1, stPrefix))
				{
#ifdef __TREE_WITH_CACHE__
					ptrNode->setDirtyFlag(true);
#endif //__TREE_WITH_CACHE__
				}
			}
			else //if (std::holds_alternative<shared_ptr<DataNodeType>>(ptrNode->m_ptrCoreObject))
			{
				DataNodeType* ptrDataNode = reinterpret_cast<DataNodeType*>(ptrNode->m_ptrCoreObject);
				ptrDataNode->print(os, nLevel + 1, stPrefix);
			}
		}
	}

	template <typename CacheType, typename CacheObjectType>
	void printRONode(std::ofstream& os, std::shared_ptr<CacheType>& ptrCache, size_t nLevel, string stPrefix)
	{
		//		uint8_t nSpaceCount = 7;
		//
		//		stPrefix.append(std::string(nSpaceCount - 1, ' '));
		//		stPrefix.append("|");
		//		for (size_t nIndex = 0; nIndex < m_ptrRawData->nTotalPivots + 1; nIndex++)
		//		{
		//			os
		//				<< " "
		//				<< stPrefix
		//				<< std::endl;
		//
		//			os
		//				<< " "
		//				<< stPrefix
		//				<< std::string(nSpaceCount, '-').c_str();
		//
		//			if (nIndex < m_ptrRawData->nTotalPivots.size())
		//			{
		//				os
		//					<< " < ("
		//					<< m_ptrRawData->ptrPivots[nIndex] << ")";
		//			}
		//			else
		//			{
		//				os
		//					<< " >= ("
		//					<< m_ptrRawData->ptrPivots[nIndex - 1]
		//					<< ")";
		//			}
		//
		//			CacheObjectType ptrNode = nullptr;
		//
		//#ifdef __TREE_WITH_CACHE__
		//			std::optional<ObjectUIDType> uidUpdated = std::nullopt;
		//			ptrCache->getObject(m_ptrRawData->ptrChildren[nIndex], ptrNode, uidUpdated);
		//
		//			if (uidUpdated != std::nullopt)
		//			{
		//				//..
		//				std::cout << "Critical State: " << __LINE__ << std::endl;
		//				throw new std::logic_error(".....");   // TODO: critical log.
		//				m_vtPivots[nIndex] = *uidUpdated;
		//			}
		//#else //__TREE_WITH_CACHE__
		//			ptrCache->getObject(m_vtPivots[nIndex], ptrNode);
		//#endif //__TREE_WITH_CACHE__
		//
		//			os << std::endl;
		//
		//			if (std::holds_alternative<shared_ptr<SelfType>>(ptrNode->getInnerData()))
		//			{
		//				shared_ptr<SelfType> ptrIndexNode = std::get<shared_ptr<SelfType>>(ptrNode->getInnerData());
		//				ptrIndexNode->template print<CacheType, CacheObjectType>(os, ptrCache, nLevel + 1, stPrefix);
		//			}
		//			else //if (std::holds_alternative<shared_ptr<DataNodeType>>(ptrNode->getInnerData()))
		//			{
		//				shared_ptr<DataNodeType> ptrDataNode = std::get<shared_ptr<DataNodeType>>(ptrNode->getInnerData());
		//				ptrDataNode->print(os, nLevel + 1, stPrefix);
		//			}
		//		}
	}

	void wieHiestDu()
	{
		printf("ich heisse InternalNode.\n");
	}
};