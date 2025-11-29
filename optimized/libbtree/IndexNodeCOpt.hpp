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
#include "validityasserts.h"

using namespace std;

template <typename Traits>
class IndexNodeCOpt 
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
	typedef IndexNodeCOpt<Traits> SelfType;
	typedef CacheType::ObjectTypePtr CacheObjectPtr;

public:
	struct PivotData 
	{
		ObjectUIDType uid;
		CacheObjectPtr ptr;
	};

	struct Data 
	{
		KeyType    pivot;
		PivotData  right;
	};

	typedef std::vector<Data>::const_iterator DataIterator;

public:
	uint16_t m_nDegree;	
	PivotData m_oFirstPivotData; // Leftmost child for keys < first pivot
	std::vector<Data> m_vtEntries;

public:
	~IndexNodeCOpt()
	{
		m_vtEntries.clear();
	}

	IndexNodeCOpt(uint16_t nDegree, const char* szData, uint32_t nDataLen, uint16_t nBlockSize)
		: m_nDegree(nDegree)
	{
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<typename ObjectUIDType::NodeUID>::value &&
			std::is_standard_layout<typename ObjectUIDType::NodeUID>::value)
		{
			uint16_t nEntriesCount = 0;
			uint32_t nOffset = sizeof(uint8_t);

			memcpy(&nEntriesCount, szData + nOffset, sizeof(uint16_t));
			nOffset += sizeof(uint16_t);

			memcpy(&m_oFirstPivotData.uid, szData + nOffset, sizeof(ObjectUIDType));
			nOffset += sizeof(ObjectUIDType);

			m_oFirstPivotData.ptr = nullptr;

			KeyType key;
			ObjectUIDType uid;

#ifdef __PROD__
			fix this
#else //__PROD__
			m_vtEntries.reserve(std::max<size_t>(static_cast<size_t>(2 * m_nDegree + 1), static_cast<size_t>(nEntriesCount)));
#endif //__PROD__

			for (int nIdx = 0; nIdx < nEntriesCount; ++nIdx)
			{
				memcpy(&key, szData + nOffset, sizeof(KeyType));
				nOffset += sizeof(KeyType);

				memcpy(&uid, szData + nOffset, sizeof(ObjectUIDType));
				nOffset += sizeof(ObjectUIDType);

				m_vtEntries.push_back({ key, {uid, nullptr} });
			}
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

	IndexNodeCOpt(uint16_t nDegree, PivotData oPivotData, DataIterator itBegin, DataIterator itEnd)
		: m_nDegree(nDegree)
		, m_oFirstPivotData(std::move(oPivotData))
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		m_vtEntries.reserve(std::max<size_t>(
			static_cast<size_t>(2 * m_nDegree + 1),
			static_cast<size_t>(std::distance(itBegin, itEnd))
			));
#endif //__PROD__

		m_vtEntries.assign(itBegin, itEnd);
	}

	IndexNodeCOpt(uint16_t nDegree, const KeyType& pivot, const ObjectUIDType& uidLHSNode, const CacheObjectPtr ptrLHSNode, const ObjectUIDType& uidRHSNode, const CacheObjectPtr ptrRHSNode)
		: m_nDegree(nDegree) 
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		m_vtEntries.reserve(2 * m_nDegree + 1);
#endif //__PROD__

		m_oFirstPivotData = { uidLHSNode, ptrLHSNode };
		m_vtEntries.push_back({ pivot, {uidRHSNode, ptrRHSNode} });
	}

public:
	inline void serialize(char*& szData, uint32_t& nDataLen, uint16_t nBlockSize, void*& ptrBlockAppendOffset, bool& bAlignedAllocation) const
	{
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<typename ObjectUIDType::NodeUID>::value &&
			std::is_standard_layout<typename ObjectUIDType::NodeUID>::value)
		{
			uint16_t nEntriesCount = m_vtEntries.size();

			nDataLen = sizeof(uint8_t)											// UID
				+ sizeof(uint16_t)												// Total keys
				+ sizeof(ObjectUIDType)											// First Pivot Data
				+ (nEntriesCount * (sizeof(KeyType) + sizeof(ObjectUIDType)))	// All entries
				+ 1;

			szData = new char[nDataLen];
			memset(szData, 0, nDataLen);

			size_t nOffset = 0;
			memcpy(szData, &UID, sizeof(uint8_t));
			nOffset += sizeof(uint8_t);

			memcpy(szData + nOffset, &nEntriesCount, sizeof(uint16_t));
			nOffset += sizeof(uint16_t);

			if (m_oFirstPivotData.ptr != nullptr && m_oFirstPivotData.ptr->m_uidUpdated != std::nullopt)
			{
				memcpy(szData + nOffset, &m_oFirstPivotData.ptr->m_uidUpdated, sizeof(ObjectUIDType));
			}
			else
			{
				memcpy(szData + nOffset, &m_oFirstPivotData.uid, sizeof(ObjectUIDType));
			}

			nOffset += sizeof(ObjectUIDType);

			for (auto& entry : m_vtEntries) 
			{
				memcpy(szData + nOffset, &entry.pivot, sizeof(KeyType));
				nOffset += sizeof(KeyType);

				if (entry.right.ptr != nullptr && entry.right.ptr->m_uidUpdated != std::nullopt)
				{
					memcpy(szData + nOffset, &entry.right.ptr->m_uidUpdated, sizeof(ObjectUIDType));
				}
				else
				{
					memcpy(szData + nOffset, &entry.right.uid, sizeof(ObjectUIDType));
				}
				nOffset += sizeof(ObjectUIDType);
			}

#ifdef __ENABLE_ASSERTS__
			SelfType oClone(m_nDegree, szData, nDataLen, nBlockSize);
			if (m_oFirstPivotData.ptr != nullptr && m_oFirstPivotData.ptr->m_uidUpdated != std::nullopt)
			{
				ASSERT(m_oFirstPivotData.ptr->m_uidUpdated == oClone.m_oFirstPivotData.uid);
				ASSERT(m_oFirstPivotData.ptr->m_uidUpdated.value().getMediaType() >= 2);
			}
			else
			{
				ASSERT(m_oFirstPivotData.uid == oClone.m_oFirstPivotData.uid);
				ASSERT(m_oFirstPivotData.uid.getMediaType() >= 2);
			}

			
			for (int idx = 0; idx < m_vtEntries.size(); idx++)
			{
				if (m_vtEntries[idx].right.ptr != nullptr && m_vtEntries[idx].right.ptr->m_uidUpdated != std::nullopt)
				{
					ASSERT(m_vtEntries[idx].right.ptr->m_uidUpdated == oClone.m_vtEntries[idx].right.uid);
					ASSERT(m_vtEntries[idx].right.ptr->m_uidUpdated.value().getMediaType() >= 2);
				}
				else
				{
					ASSERT(m_vtEntries[idx].right.uid == oClone.m_vtEntries[idx].right.uid);
					ASSERT(m_vtEntries[idx].right.uid.getMediaType() >= 2);
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
		if (m_oFirstPivotData.ptr != nullptr && m_oFirstPivotData.ptr->m_uidUpdated)
		{
			return true;
		}

		for (auto& entry : m_vtEntries)
		{
			if (entry.right.ptr != nullptr && entry.right.ptr->m_uidUpdated)
			{
				return true;
			}
		}

		return false;
	}

	inline size_t getKeysCount() const
	{
		return m_vtEntries.size();
	}

	PivotData& getChild(const KeyType& key) 
	{
		auto it = std::upper_bound(
			m_vtEntries.begin(), m_vtEntries.end(), key,
			[](KeyType key, Data const& entry) { return key < entry.pivot; });

		if (it == m_vtEntries.begin())
		{
			return m_oFirstPivotData;
		}
		
		return std::prev(it)->right;
	}

	bool getChild(const KeyType& key, PivotData*& pdTargetNode, uint16_t& idxTargetNode, PivotData*& ptrLHSPivotData, PivotData*& ptrRHSPivotData)
	{
		auto it = std::upper_bound(
			m_vtEntries.begin(), m_vtEntries.end(), key,
			[](KeyType key, Data const& entry) { return key < entry.pivot; });

		idxTargetNode = std::distance(m_vtEntries.begin(), it);

		if (it == m_vtEntries.begin())
		{
			pdTargetNode = &m_oFirstPivotData;
			ptrRHSPivotData = &it->right;
			return false;	// Non on the left side.
		}

		pdTargetNode = &std::prev(it)->right;
		if (it == m_vtEntries.begin() + 1)
		{
			ptrLHSPivotData = &m_oFirstPivotData;
		}
		else
		{
			ptrLHSPivotData = &std::prev(std::prev(it))->right;
		}
		return true;
	}

	inline const void getFirstChildDetails(ObjectUIDType& uid, CacheObjectPtr& ptr, uint16_t nIdx = 0) const
	{
		uid = m_oFirstPivotData.uid;
		ptr = m_oFirstPivotData.ptr;
	}

	template <typename CacheType>
#ifdef __CONCURRENT__
	inline bool getChild(std::shared_ptr<CacheType>& ptrCache, const KeyType& key, ObjectUIDType& uid, CacheObjectPtr& ptr, std::vector<std::unique_lock<std::shared_mutex>>& vtLocks)
#else //__CONCURRENT__
	inline bool getChild(std::shared_ptr<CacheType>& ptrCache, const KeyType& key, ObjectUIDType& uid, CacheObjectPtr& ptr)
#endif //__CONCURRENT__
	{
		bool bUpdate = false;

		PivotData& oPivotData = getChild(key);

		if (oPivotData.ptr == nullptr)
		{
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;
			ptrCache->getObject(m_nDegree, oPivotData.uid, oPivotData.ptr);

#ifdef __CONCURRENT__
			vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(oPivotData.ptr->m_mtx));
#endif //__CONCURRENT__
		} 
		else
		{
#ifdef __CONCURRENT__
			vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(oPivotData.ptr->m_mtx));
#endif //__CONCURRENT__

			if (oPivotData.ptr->m_ptrCoreObject == nullptr)
			{
				if (oPivotData.ptr->m_uidUpdated != std::nullopt)
				{
					bUpdate = true;
					oPivotData.uid = *oPivotData.ptr->m_uidUpdated;
				}

				ptrCache->getCoreObject(m_nDegree, oPivotData.uid, oPivotData.ptr);
			}
		}
		
		uid = oPivotData.uid;
		ptr = oPivotData.ptr;

		ASSERT(oPivotData.ptr->m_ptrCoreObject != nullptr);

		return bUpdate;
	}

#ifdef __CONCURRENT__
	inline void getChild(const KeyType& key, ObjectUIDType& uid, CacheObjectPtr& ptr, std::vector<std::unique_lock<std::shared_mutex>>& vtLocks)
#else //__CONCURRENT__
	inline void getChild(const KeyType& key, ObjectUIDType& uid, CacheObjectPtr& ptr)
#endif //__CONCURRENT__
	{
		PivotData& oPivotData = getChild(key);

#ifdef __CONCURRENT__
		vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(oPivotData.ptr->m_mtx));
#endif //__CONCURRENT__

		uid = oPivotData.uid;
		ptr = oPivotData.ptr;

		ASSERT(oPivotData.ptr->m_ptrCoreObject != nullptr);
	}

	template <typename CacheType>
#ifdef __CONCURRENT__
	inline bool getChildAtIdx(std::shared_ptr<CacheType>& ptrCache, PivotData& oPivotData, CacheObjectPtr& ptr, std::unique_lock<std::shared_mutex>& lock)
#else //__CONCURRENT__
	inline bool getChildAtIdx(std::shared_ptr<CacheType>& ptrCache, PivotData& oPivotData, CacheObjectPtr& ptr)
#endif //__CONCURRENT__
	{
		bool bUpdate = false;

		if (oPivotData.ptr == nullptr)
		{
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;
			ptrCache->getObject(m_nDegree, oPivotData.uid, oPivotData.ptr);

#ifdef __CONCURRENT__
			lock = std::unique_lock<std::shared_mutex>(oPivotData.ptr->m_mtx);
#endif //__CONCURRENT__

		}
		else
		{
#ifdef __CONCURRENT__
			lock = std::unique_lock<std::shared_mutex>(oPivotData.ptr->m_mtx);
#endif //__CONCURRENT__

			if (oPivotData.ptr->m_ptrCoreObject == nullptr)
			{
				if (oPivotData.ptr->m_uidUpdated != std::nullopt)
				{
					bUpdate = true;
					oPivotData.uid = *oPivotData.ptr->m_uidUpdated;
				}

				ptrCache->getCoreObject(m_nDegree, oPivotData.uid, oPivotData.ptr);
			}
		}

		ptr = oPivotData.ptr;

		ASSERT(oPivotData.ptr->m_ptrCoreObject != nullptr);
			
		return bUpdate;
	}

#ifdef __CONCURRENT__
	inline void getChildAtIdx(PivotData& oPivotData, CacheObjectPtr& ptr, std::unique_lock<std::shared_mutex>& lock)
#else //__CONCURRENT__
	inline void getChildAtIdx(PivotData& oPivotData, CacheObjectPtr& ptr)
#endif //__CONCURRENT__
	{
#ifdef __CONCURRENT__
		lock = std::unique_lock<std::shared_mutex>(oPivotData.ptr->m_mtx);
#endif //__CONCURRENT__

		ptr = oPivotData.ptr;

		ASSERT(oPivotData.ptr->m_ptrCoreObject != nullptr);
	}

	inline bool canTriggerSplit() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		return m_vtEntries.size() == (2 * m_nDegree - 1);
#endif //__PROD__
	}

	inline bool requireSplit() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		return m_vtEntries.size() > (2 * m_nDegree - 1);
#endif //__PROD__
	}

	inline bool canTriggerMerge() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		return m_vtEntries.size() < (m_nDegree );
#endif //__PROD__
	}

	inline bool requireMerge() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		return m_vtEntries.size() < (m_nDegree - 1);
#endif //__PROD__
	}

public:
	inline ErrorCode insert(const KeyType& pivot, const ObjectUIDType& uidSibling, CacheObjectPtr ptrSibling)
	{
		auto it = std::lower_bound(
			m_vtEntries.begin(), m_vtEntries.end(), pivot,
			[](Data const& entry, KeyType key) { return entry.pivot < key; });

		if (it != m_vtEntries.end() && (*it).pivot == pivot)
			return ErrorCode::KeyAlreadyExists;

		size_t nChildIdx = std::distance(m_vtEntries.begin(), it);
		m_vtEntries.insert(m_vtEntries.begin() + nChildIdx, { pivot, { uidSibling, ptrSibling} });

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
		typedef typename CacheType::ObjectTypePtr ObjectTypePtr;

		ObjectTypePtr ptrLHSCacheObject = nullptr;
		ObjectTypePtr ptrRHSCacheObject = nullptr;

		SelfType* ptrLHSNode = nullptr;
		SelfType* ptrRHSNode = nullptr;

#ifdef __TREE_WITH_CACHE__
		uidAffectedNode = std::nullopt;
		ptrAffectedNode = nullptr;
#endif //__TREE_WITH_CACHE__

		uint16_t nPivotIdx;
		PivotData* ptrLHSPivotData = nullptr;
		PivotData* ptrRHSPivotData = nullptr;
		PivotData* ptrChildPivotData = nullptr;
		bool bMoveFromLHS = getChild(key, ptrChildPivotData, nPivotIdx, ptrLHSPivotData, ptrRHSPivotData);

		if (bMoveFromLHS)
		{
#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock;
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
#ifdef __CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, *ptrLHSPivotData, ptrLHSCacheObject, lock);	// Donot regarding the return value as the parent is already being set to dirty.
#else //__CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, *ptrLHSPivotData, ptrLHSCacheObject);
#endif //__CONCURRENT__			
#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
			this->getChildAtIdx(*ptrLHSPivotData, ptrLHSCacheObject, lock);	// Donot regarding the return value as the parent is already being set to dirty.
#else //__CONCURRENT__
			this->getChildAtIdx(*ptrLHSPivotData, ptrLHSCacheObject);
#endif //__CONCURRENT__						
#endif //__TREE_WITH_CACHE__

			ptrLHSNode = reinterpret_cast<SelfType*>(ptrLHSCacheObject->m_ptrCoreObject);

#ifdef __TREE_WITH_CACHE__
			ptrLHSCacheObject->m_bDirty = true;

			uidAffectedNode = ptrLHSPivotData->uid;
			ptrAffectedNode = ptrLHSCacheObject;
#endif //__TREE_WITH_CACHE__

#ifndef __PROD__
			if (ptrLHSNode->getKeysCount() > std::ceil(m_nDegree / 2.0f))	// TODO: macro?
#else //__PROD__
			if (ptrLHSNode->getKeysCount() > CEIL_HALF_FANOUT(Traits::Fanout))	// TODO: macro?
#endif //__PROD__
			{
				KeyType key;

				ptrChildNode->moveAnEntityFromLHSSibling(ptrLHSNode, m_vtEntries[nPivotIdx - 1].pivot, key);

				m_vtEntries[nPivotIdx - 1].pivot = key;
				return ErrorCode::Success;
			}

			ptrLHSNode->mergeNodes(ptrChildNode, m_vtEntries[nPivotIdx - 1].pivot);

			uidObjectToDelete = ptrChildPivotData->uid;
			ptrObjectToDelete = ptrChildPivotData->ptr;


			ASSERT(uidObjectToDelete == uidChild);


			m_vtEntries.erase(m_vtEntries.begin() + nPivotIdx - 1);

			return ErrorCode::Success;
		}



		ASSERT(ptrLHSPivotData == nullptr && ptrRHSPivotData != nullptr);

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock;
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
#ifdef __CONCURRENT__
		this->getChildAtIdx<CacheType>(ptrCache, *ptrRHSPivotData, ptrRHSCacheObject, lock);
#else //__CONCURRENT__
		this->getChildAtIdx<CacheType>(ptrCache, *ptrRHSPivotData, ptrRHSCacheObject);
#endif //__CONCURRENT__
#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
		this->getChildAtIdx(*ptrRHSPivotData, ptrRHSCacheObject, lock);
#else //__CONCURRENT__
		this->getChildAtIdx(*ptrRHSPivotData, ptrRHSCacheObject);
#endif //__CONCURRENT__
#endif //__TREE_WITH_CACHE__

		ptrRHSNode = reinterpret_cast<SelfType*>(ptrRHSCacheObject->m_ptrCoreObject);

#ifdef __TREE_WITH_CACHE__
		ptrRHSCacheObject->m_bDirty = true;

		uidAffectedNode = ptrRHSPivotData->uid;
		ptrAffectedNode = ptrRHSCacheObject;
#endif //__TREE_WITH_CACHE__

#ifndef __PROD__
		if (ptrRHSNode->getKeysCount() > std::ceil(m_nDegree / 2.0f))
#else //__PROD__
		if (ptrRHSNode->getKeysCount() > CEIL_HALF_FANOUT(Traits::Fanout))
#endif //__PROD__
		{
			KeyType key;

			ptrChildNode->moveAnEntityFromRHSSibling(ptrRHSNode, m_vtEntries[nPivotIdx].pivot, key);

			m_vtEntries[nPivotIdx].pivot = key;
			return ErrorCode::Success;
		}

		ptrChildNode->mergeNodes(ptrRHSNode, m_vtEntries[nPivotIdx].pivot);


		ASSERT(ptrRHSPivotData->uid == m_vtEntries[nPivotIdx].right.uid);


		uidObjectToDelete = ptrRHSPivotData->uid;
		ptrObjectToDelete = ptrRHSPivotData->ptr;

		m_vtEntries.erase(m_vtEntries.begin() + nPivotIdx);

		return ErrorCode::Success;
	}

	template <typename CacheType>
#ifdef __TREE_WITH_CACHE__
#ifndef __PROD__
	inline ErrorCode rebalanceDataNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, DataNodeType* ptrChildNode, const KeyType& key, std::optional<ObjectUIDType>& uidObjectToDelete, CacheType::ObjectTypePtr& ptrObjectToDelete, std::optional<ObjectUIDType>& uidAffectedNode, CacheType::ObjectTypePtr& ptrAffectedNode)
#else //__PROD__
	inline ErrorCode rebalanceDataNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, DataNodeType* ptrChildNode, const KeyType& key, std::optional<ObjectUIDType>& uidObjectToDelete, CacheType::ObjectTypePtr& ptrObjectToDelete, std::optional<ObjectUIDType>& uidAffectedNode, CacheType::ObjectTypePtr& ptrAffectedNode)
#endif //__PROD__
#else //__TREE_WITH_CACHE__
#ifndef __PROD__
	inline ErrorCode rebalanceDataNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, DataNodeType* ptrChildNode, const KeyType& key, std::optional<ObjectUIDType>& uidObjectToDelete, CacheType::ObjectTypePtr& ptrObjectToDelete)
#else //__PROD__
	inline ErrorCode rebalanceDataNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, DataNodeType* ptrChildNode, const KeyType& key, std::optional<ObjectUIDType>& uidObjectToDelete, CacheType::ObjectTypePtr& ptrObjectToDelete)
#endif //__PROD__
#endif //__TREE_WITH_CACHE__
	{
		typedef typename CacheType::ObjectTypePtr ObjectTypePtr;

		ObjectTypePtr ptrLHSCacheObject = nullptr;
		ObjectTypePtr ptrRHSCacheObject = nullptr;

		DataNodeType* ptrLHSNode = nullptr;
		DataNodeType* ptrRHSNode = nullptr;

#ifdef __TREE_WITH_CACHE__
		uidAffectedNode = std::nullopt;
		ptrAffectedNode = nullptr;
#endif //__TREE_WITH_CACHE__

		uint16_t nPivotIdx;
		PivotData* ptrLHSPivotData = nullptr;
		PivotData* ptrRHSPivotData = nullptr;
		PivotData* ptrChildPivotData = nullptr;
		bool bMoveFromLHS = getChild(key, ptrChildPivotData, nPivotIdx, ptrLHSPivotData, ptrRHSPivotData);

		if (bMoveFromLHS)
		{
#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock;
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
#ifdef __CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, *ptrLHSPivotData, ptrLHSCacheObject, lock);	// Donot regarding the return value as the parent is already being set to dirty.
#else //__CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, *ptrLHSPivotData, ptrLHSCacheObject);
#endif //__CONCURRENT__			
#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
			this->getChildAtIdx(*ptrLHSPivotData, ptrLHSCacheObject, lock);	// Donot regarding the return value as the parent is already being set to dirty.
#else //__CONCURRENT__
			this->getChildAtIdx(*ptrLHSPivotData, ptrLHSCacheObject);
#endif //__CONCURRENT__
#endif //__TREE_WITH_CACHE__

			ptrLHSNode = reinterpret_cast<DataNodeType*>(ptrLHSCacheObject->m_ptrCoreObject);

#ifdef __TREE_WITH_CACHE__
			ptrLHSCacheObject->m_bDirty = true;

			uidAffectedNode = ptrLHSPivotData->uid;
			ptrAffectedNode = ptrLHSCacheObject;
#endif //__TREE_WITH_CACHE__

#ifndef __PROD__
			if (ptrLHSNode->getKeysCount() > std::ceil(m_nDegree / 2.0f))	// TODO: macro?
#else //__PROD__
			if (ptrLHSNode->getKeysCount() > CEIL_HALF_FANOUT(Traits::Fanout))	// TODO: macro?
#endif //__PROD__
			{
				KeyType key;

				ptrChildNode->moveAnEntityFromLHSSibling(ptrLHSNode, key);

				m_vtEntries[nPivotIdx - 1].pivot = key;

				return ErrorCode::Success;
			}

			ptrLHSNode->mergeNode(ptrChildNode);

			uidObjectToDelete = ptrChildPivotData->uid;
			ptrObjectToDelete = ptrChildPivotData->ptr;


			ASSERT(uidObjectToDelete == uidChild);


			m_vtEntries.erase(m_vtEntries.begin() + nPivotIdx - 1);

			return ErrorCode::Success;
		}


		ASSERT(ptrLHSPivotData == nullptr && ptrRHSPivotData != nullptr);

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock;
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
#ifdef __CONCURRENT__
		this->getChildAtIdx<CacheType>(ptrCache, *ptrRHSPivotData, ptrRHSCacheObject, lock);
#else //__CONCURRENT__
		this->getChildAtIdx<CacheType>(ptrCache, *ptrRHSPivotData, ptrRHSCacheObject);
#endif //__CONCURRENT__
#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
		this->getChildAtIdx(*ptrRHSPivotData, ptrRHSCacheObject, lock);
#else //__CONCURRENT__
		this->getChildAtIdx(*ptrRHSPivotData, ptrRHSCacheObject);
#endif //__CONCURRENT__
#endif //__TREE_WITH_CACHE__

		ptrRHSNode = reinterpret_cast<DataNodeType*>(ptrRHSCacheObject->m_ptrCoreObject);

#ifdef __TREE_WITH_CACHE__
		ptrRHSCacheObject->m_bDirty = true;

		uidAffectedNode = ptrRHSPivotData->uid;
		ptrAffectedNode = ptrRHSCacheObject;
#endif //__TREE_WITH_CACHE__

#ifndef __PROD__
		if (ptrRHSNode->getKeysCount() > std::ceil(m_nDegree / 2.0f))
#else //__PROD__
		if (ptrRHSNode->getKeysCount() > CEIL_HALF_FANOUT(Traits::Fanout))
#endif //__PROD__
		{
			KeyType key;

			ptrChildNode->moveAnEntityFromRHSSibling(ptrRHSNode, key);

			m_vtEntries[nPivotIdx].pivot = key;
			return ErrorCode::Success;
		}

		ptrChildNode->mergeNode(ptrRHSNode);


		ASSERT(ptrRHSPivotData->uid == m_vtEntries[nPivotIdx].right.uid);


		uidObjectToDelete = ptrRHSPivotData->uid;
		ptrObjectToDelete = ptrRHSPivotData->ptr;

		m_vtEntries.erase(m_vtEntries.begin() + nPivotIdx);

		return ErrorCode::Success;
	}

	template <typename CacheType, typename CacheObjectTypePtr>
	inline ErrorCode split(std::shared_ptr<CacheType> ptrCache, std::optional<ObjectUIDType>& uidSibling, CacheObjectTypePtr& ptrSibling, KeyType& pivotKeyForParent)
	{
		size_t nMid = m_vtEntries.size() / 2;

#ifdef __PROD__
		fix this
#else //__PROD__
		ptrCache->template createObjectOfType<SelfType>(uidSibling, ptrSibling, m_nDegree,
			m_vtEntries[nMid].right, m_vtEntries.begin() + nMid + 1, m_vtEntries.end());
#endif //__PROD__

		if (!uidSibling)
		{
			return ErrorCode::Error;
		}

		pivotKeyForParent = m_vtEntries[nMid].pivot;

		m_vtEntries.resize(nMid);

		return ErrorCode::Success;
	}

	inline void moveAnEntityFromLHSSibling(SelfType* ptrLHSSibling, KeyType& pivotKeyForEntity, KeyType& pivotKeyForParent)
	{
		Data key = ptrLHSSibling->m_vtEntries.back();

		ptrLHSSibling->m_vtEntries.pop_back();

		ASSERT(ptrLHSSibling->m_vtEntries.size() > 0);

		m_vtEntries.insert(m_vtEntries.begin(), { pivotKeyForEntity, m_oFirstPivotData });

		m_oFirstPivotData = key.right;

		pivotKeyForParent = key.pivot;
	}

	inline void moveAnEntityFromRHSSibling(SelfType* ptrRHSSibling, KeyType& pivotKeyForEntity, KeyType& pivotKeyForParent)
	{
		Data key = ptrRHSSibling->m_vtEntries.front();
		
		ptrRHSSibling->m_vtEntries.erase(ptrRHSSibling->m_vtEntries.begin());

		ASSERT(ptrRHSSibling->m_vtEntries.size() > 0);

		m_vtEntries.push_back({ pivotKeyForEntity, ptrRHSSibling->m_oFirstPivotData });

		ptrRHSSibling->m_oFirstPivotData = key.right;

		pivotKeyForParent = key.pivot;
	}

	inline void mergeNodes(SelfType* ptrSibling, KeyType& pivotKey)
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		std::size_t nRequiredCapacity = m_vtEntries.size() + ptrSibling->m_vtEntries.size() + 1;
		if (m_vtEntries.capacity() < nRequiredCapacity)
			m_vtEntries.reserve(nRequiredCapacity);
#endif //__PROD__

		m_vtEntries.push_back({ pivotKey, ptrSibling->m_oFirstPivotData });
		m_vtEntries.insert(m_vtEntries.end(), ptrSibling->m_vtEntries.begin(), ptrSibling->m_vtEntries.end());
	}

public:
	template <typename CacheType, typename CacheObjectType>
	bool print(std::ofstream& os, std::shared_ptr<CacheType>& ptrCache, size_t nLevel, string stPrefix)
	{
		bool bUpdate = false;
		uint8_t nSpaceCount = 7;

#ifdef __TREE_WITH_CACHE__		
		std::vector<CacheObjectType> vtAccessedNodes;
#endif //__TREE_WITH_CACHE__

		stPrefix.append(std::string(nSpaceCount - 1, ' '));
		stPrefix.append("|");
		for (int nIndex = 0; nIndex <= m_vtEntries.size(); nIndex++)
		{
			os
				<< " "
				<< stPrefix
				<< std::endl;

			os
				<< " "
				<< stPrefix
				<< std::string(nSpaceCount, '-').c_str();

			if (nIndex < m_vtEntries.size())
			{
				os
					<< " < ("
					<< m_vtEntries[nIndex].pivot << ")";
			}
			else
			{
				os
					<< " >= ("
					<< m_vtEntries[nIndex - 1].pivot
					<< ")";
			}

			CacheObjectType ptrNode = nullptr;

#ifdef __TREE_WITH_CACHE__
#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock;
			if (nIndex == 0)
			{
				this->getChildAtIdx<CacheType>(ptrCache, m_oFirstPivotData, ptrNode, lock);
			}
			else
			{
				this->getChildAtIdx<CacheType>(ptrCache, m_vtEntries[nIndex - 1].right, ptrNode, lock);
			}
#else //__CONCURRENT__
			if (nIndex == 0)
			{
				this->getChildAtIdx<CacheType>(ptrCache, m_oFirstPivotData, ptrNode);
			}
			else
			{
				this->getChildAtIdx<CacheType>(ptrCache, m_vtEntries[nIndex - 1].right, ptrNode);
			}
#endif //__CONCURRENT__
#else //__TREE_WITH_CACHE__

#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock;
			if (nIndex == 0)
			{
				this->getChildAtIdx(m_oFirstPivotData, ptrNode, lock);
			}
			else
			{
				this->getChildAtIdx(m_vtEntries[nIndex - 1].right, ptrNode, lock);
			}
#else //__CONCURRENT__
			if (nIndex == 0)
			{
				this->getChildAtIdx(m_oFirstPivotData, ptrNode);
			}
			else
			{
				this->getChildAtIdx(m_vtEntries[nIndex - 1].right, ptrNode);
			}
#endif //__CONCURRENT__
#endif //__TREE_WITH_CACHE__

			os << std::endl;

#ifdef __TREE_WITH_CACHE__			
			vtAccessedNodes.push_back(ptrNode);
#endif //__TREE_WITH_CACHE__
			if (ptrNode->m_nCoreObjectType == SelfType::UID)
			{
				SelfType* ptrIndexNode = reinterpret_cast<SelfType*>(ptrNode->m_ptrCoreObject);
				if (ptrIndexNode->template print<CacheType, CacheObjectType>(os, ptrCache, nLevel + 1, stPrefix))
				{
#ifdef __TREE_WITH_CACHE__
					ptrNode->m_bDirty = true;
#endif //__TREE_WITH_CACHE__
				}
			}
			else
			{
				DataNodeType* ptrDataNode = reinterpret_cast<DataNodeType*>(ptrNode->m_ptrCoreObject);
				ptrDataNode->print(os, nLevel + 1, stPrefix);
			}
		}

#ifdef __TREE_WITH_CACHE__
		ptrCache->updateObjectsAccessMetadata(vtAccessedNodes);
		vtAccessedNodes.clear();
#endif //__TREE_WITH_CACHE__

		return bUpdate;
	}

	void wieHiestDu()
	{
		printf("ich heisse IndexNode.\n");
	}
};
