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
#include <mutex>
#include <shared_mutex>

using namespace std;

template <typename Traits>
class IndexNode
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
	typedef IndexNode<Traits> SelfType;
	typedef typename CacheType::ObjectTypePtr CacheObjectPtr;

public:
	struct PivotData
	{
		ObjectUIDType uid;
		CacheObjectPtr ptr;
	};

	typedef std::vector<KeyType>::const_iterator KeyIterator;
	typedef std::vector<PivotData>::const_iterator PivotIterator;

private:
	uint16_t m_nDegree;
	std::vector<KeyType> m_vtKeys;
	std::vector<PivotData> m_vtPivots;

public:
	~IndexNode()
	{
		for (auto& pivotdata : m_vtPivots)
		{
			if (pivotdata.ptr != nullptr) delete pivotdata.ptr;
		}

		m_vtKeys.clear();
		m_vtPivots.clear();
	}

	IndexNode(uint16_t nDegree, const char* szData, uint32_t nDataLen, uint16_t nBlockSize)
		: m_nDegree(nDegree)
	{
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<typename ObjectUIDType::NodeUID>::value &&
			std::is_standard_layout<typename ObjectUIDType::NodeUID>::value)
		{
			ASSERT(UID == szData[0]);

			uint16_t nKeyCount = 0;

			uint32_t nOffset = sizeof(uint8_t);

			memcpy(&nKeyCount, szData + nOffset, sizeof(uint16_t));
			nOffset += sizeof(uint16_t);

#ifdef __PROD__
			fix this
#else //__PROD__
			m_vtKeys.reserve(std::max<size_t>(static_cast<size_t>(2 * m_nDegree + 1), static_cast<size_t>(nKeyCount)));
			m_vtPivots.reserve(std::max<size_t>(static_cast<size_t>(2 * m_nDegree + 2), static_cast<size_t>(nKeyCount + 1)));
#endif //__PROD__

			m_vtKeys.resize(nKeyCount);
			m_vtPivots.resize(nKeyCount + 1);

			uint32_t nKeysSize = nKeyCount * sizeof(KeyType);
			memcpy(m_vtKeys.data(), szData + nOffset, nKeysSize);
			nOffset += nKeysSize;

			size_t nValueSize = sizeof(ObjectUIDType);
			for (size_t idx = 0; idx <= nKeyCount; idx++)
			{
				ObjectUIDType uid;
				memcpy(&uid, szData + nOffset, nValueSize);

				m_vtPivots[idx] = PivotData(uid, nullptr);

				nOffset += nValueSize;
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

	IndexNode(uint16_t nDegree, KeyIterator itBeginKeys, KeyIterator itEndKeys, PivotIterator itBeginPivots, PivotIterator itEndPivots)
		: m_nDegree(nDegree)
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		m_vtKeys.reserve(std::max<size_t>(
			static_cast<size_t>(2 * m_nDegree + 1),
			static_cast<size_t>(std::distance(itBeginKeys, itEndKeys))
			));

		m_vtPivots.reserve(std::max<size_t>(
			static_cast<size_t>(2 * m_nDegree + 2),
			static_cast<size_t>(std::distance(itBeginPivots, itEndPivots))
			));
#endif //__PROD__

		m_vtKeys.assign(itBeginKeys, itEndKeys);
		m_vtPivots.assign(itBeginPivots, itEndPivots);
	}

	IndexNode(uint16_t nDegree, const KeyType& pivotKey, const ObjectUIDType& uidLHSNode, const CacheObjectPtr ptrLHSNode, const ObjectUIDType& uidRHSNode, const CacheObjectPtr ptrRHSNode)
		: m_nDegree(nDegree)
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
	inline void serialize(char*& szData, uint32_t& nDataLen, uint16_t nBlockSize, void*& ptrBlockAppendOffset, bool& bAlignedAllocation) const
	{
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<typename ObjectUIDType::NodeUID>::value &&
			std::is_standard_layout<typename ObjectUIDType::NodeUID>::value)
		{
			uint16_t nKeysCount = m_vtKeys.size();
			uint16_t nPivotsCount = m_vtPivots.size();

			nDataLen
				= sizeof(uint8_t)
				+ sizeof(uint16_t)
				+ (nKeysCount * sizeof(KeyType))
				+ (nPivotsCount * sizeof(ObjectUIDType))
				+ 1;

			szData = new char[nDataLen];
			memset(szData, 0, nDataLen);

			size_t nOffset = 0;
			memcpy(szData, &UID, sizeof(uint8_t));
			nOffset += sizeof(uint8_t);

			memcpy(szData + nOffset, &nKeysCount, sizeof(uint16_t));
			nOffset += sizeof(uint16_t);

			size_t nKeysSize = nKeysCount * sizeof(KeyType);
			memcpy(szData + nOffset, m_vtKeys.data(), nKeysSize);
			nOffset += nKeysSize;

			for (const auto& pivot : m_vtPivots)
			{
				const ObjectUIDType* pUID = (pivot.ptr && pivot.ptr->m_uidUpdated.has_value())
					? &pivot.ptr->m_uidUpdated.value()
					: &pivot.uid;

				memcpy(szData + nOffset, pUID, sizeof(ObjectUIDType));
				nOffset += sizeof(ObjectUIDType);
			}

#ifdef __ENABLE_ASSERTS__
			ASSERT(m_vtKeys.size() + 1 == m_vtPivots.size());

			SelfType oClone(m_nDegree, szData, nDataLen, nBlockSize);
			for (int idx = 0; idx < m_vtPivots.size(); idx++)
			{
				if (m_vtPivots[idx].ptr != nullptr && m_vtPivots[idx].ptr->m_uidUpdated != std::nullopt)
				{
					ASSERT(m_vtPivots[idx].ptr->m_uidUpdated == oClone.m_vtPivots[idx].uid);
					ASSERT(m_vtPivots[idx].ptr->m_uidUpdated.value().getMediaType() >= 2);
				}
				else
				{
					ASSERT(m_vtPivots[idx].uid == oClone.m_vtPivots[idx].uid);
					ASSERT(m_vtPivots[idx].uid.getMediaType() >= 2);
				}

				if (m_vtPivots[idx].ptr != nullptr) {
					ASSERT(!m_vtPivots[idx].ptr->m_bDirty);
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
	inline bool havedependentsincache() const
	{
		bool res = false;
		for (int idx = 0; idx < m_vtPivots.size(); idx++)
		{
			if (m_vtPivots[idx].ptr != nullptr && m_vtPivots[idx].ptr->m_uidUpdated != std::nullopt)
			{
				if (!(m_vtPivots[idx].ptr->m_uidUpdated.value().getMediaType() >= 2))
				{
					return  true;
				}
			}
			else
			{
				if (!(m_vtPivots[idx].uid.getMediaType() >= 2))
				{
					return  true;
				}
			}

			if (m_vtPivots[idx].ptr != nullptr && m_vtPivots[idx].ptr->m_bDirty)
			{
				return  true;
			}

#ifdef __CONCURRENT__
			if (m_vtPivots[idx].ptr != nullptr && m_vtPivots[idx].ptr->m_nUseCounter != 0)
			{
				return  true;
			}
#endif //__CONCURRENT__

			if (m_vtPivots[idx].ptr != nullptr && m_vtPivots[idx].ptr->m_nPositionInCLOCK != -1)
			{
				return  true;
			}

		}
		return false;
	}

	inline bool _havedependentsincache() const
	{
		bool res = false;
		for (int idx = 0; idx < m_vtPivots.size(); idx++)
		{
			if (m_vtPivots[idx].ptr != nullptr && m_vtPivots[idx].ptr->m_uidUpdated != std::nullopt)
			{
				if (!(m_vtPivots[idx].ptr->m_uidUpdated.value().getMediaType() >= 2))
				{
					return  true;
				}
			}
			else
			{
				if (!(m_vtPivots[idx].uid.getMediaType() >= 2))
				{
					return  true;
				}
			}

			if (m_vtPivots[idx].ptr != nullptr && m_vtPivots[idx].ptr->m_bDirty)
			{
				return  true;
			}

#ifdef __CONCURRENT__
			if (m_vtPivots[idx].ptr != nullptr && m_vtPivots[idx].ptr->m_nUseCounter != 0)
			{
				return  true;
			}
#endif //__CONCURRENT__
			if (m_vtPivots[idx].ptr != nullptr && m_vtPivots[idx].ptr->m_nPositionInCLOCK != -1)
			{
				return  true;
			}

		}
		return false;
	}

	inline bool hasUIDUpdates() const
	{
		return std::any_of(m_vtPivots.begin(), m_vtPivots.end(), [](const auto& pivot) {
			return pivot.ptr && pivot.ptr->m_uidUpdated != std::nullopt;
			});

		//for (const auto& pivot : m_vtPivots)
		//{
		//	if (pivot.ptr != nullptr && pivot.ptr->m_uidUpdated != std::nullopt)
		//	{
		//		return true;
		//	}
		//}
		//return false;
	}

	inline size_t getKeysCount() const
	{
		return m_vtKeys.size();
	}

	inline size_t getChildNodeIdx(const KeyType& key) const
	{
		/*std::cout << "getChildNodeIdx::";
		if constexpr (std::is_arithmetic_v<KeyType>) std::cout << " [" << key << "] ::";

		for (size_t nIndex = 0; nIndex < m_vtPivots.size(); nIndex++)
		{
			if (nIndex < m_vtKeys.size())
			{
				if constexpr (std::is_arithmetic_v<KeyType>) std::cout << "< " << m_vtKeys[nIndex] << ",";
			}
			else
			{
				if constexpr (std::is_arithmetic_v<KeyType>) std::cout << " >= " << m_vtKeys[nIndex - 1] << "";
			}
		}
		std::cout << std::endl;*/
		auto it = std::upper_bound(m_vtKeys.begin(), m_vtKeys.end(), key);
		return std::distance(m_vtKeys.begin(), it);
	}

	inline const void getFirstChildDetails(ObjectUIDType& uid, CacheObjectPtr& ptr, size_t nIdx = 0) const
	{
		const PivotData& oData = m_vtPivots[nIdx];

		uid = oData.uid;
		ptr = oData.ptr;
	}

	inline const void suppressCurrentLevel(ObjectUIDType& uidParentNode, CacheObjectPtr& ptrParentNode, size_t nIdx = 0)
	{
		ASSERT(m_vtKeys.size() == 0 && m_vtPivots.size() == 1);

		const PivotData& oData = m_vtPivots[nIdx];

		uidParentNode = oData.uid;
		ptrParentNode = oData.ptr;

#ifdef __CONCURRENT__
		ASSERT(ptrParentNode->m_nUseCounter.load(std::memory_order_relaxed) > 0);
#endif //__CONCURRENT__

		m_vtPivots.clear();
	}

	inline const ObjectUIDType& getChild(const KeyType& key) const
	{
		return m_vtPivots[getChildNodeIdx(key)].uid;
	}

	template <typename CacheType>
#ifdef __CONCURRENT__
#ifdef __SELECTIVE_UPDATE__
	inline bool getChild(std::shared_ptr<CacheType>& ptrCache, const KeyType& key, CacheObjectPtr& ptr, std::vector<std::unique_lock<std::shared_mutex>>& vtLocks, bool& hasNewNodes)
#else
	inline bool getChild(std::shared_ptr<CacheType>& ptrCache, const KeyType& key, CacheObjectPtr& ptr, std::vector<std::unique_lock<std::shared_mutex>>& vtLocks)
#endif
#else //__CONCURRENT__
#ifdef __SELECTIVE_UPDATE__
	inline bool getChild(std::shared_ptr<CacheType>& ptrCache, const KeyType& key, CacheObjectPtr& ptr, bool& hasNewNodes)
#else
	inline bool getChild(std::shared_ptr<CacheType>& ptrCache, const KeyType& key, CacheObjectPtr& ptr)
#endif
#endif //__CONCURRENT__
	{
		bool bUpdate = false;

		size_t nIdx = getChildNodeIdx(key);
		PivotData& oData = m_vtPivots[nIdx];

		if (oData.ptr == nullptr)
		{
#ifdef __SELECTIVE_UPDATE__
			hasNewNodes = true;  // Fetching entire node from storage
#endif
#ifdef __CACHE_COUNTERS__
			// Cache miss - object not in cache, need to load from storage
			ptrCache->recordMiss();
#endif
			ptrCache->getObject(m_nDegree, oData.uid, oData.ptr);

#ifdef __CONCURRENT__
			//vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(oData.ptr->m_mtx));
			vtLocks.emplace_back([&]() {
				std::unique_lock<std::shared_mutex> lock(oData.ptr->m_mtx);
#ifdef __TREE_WITH_CACHE__
				oData.ptr->m_nUseCounter.fetch_add(1, std::memory_order_relaxed);
#endif //__TREE_WITH_CACHE__
				return lock;
				}());
#else //__CONCURRENT__
			if constexpr (CacheType::MARK_INUSE_FLAG) oData.ptr->m_bInUse = true;
#endif //__CONCURRENT__
		}
		else
		{
#ifdef __CONCURRENT__
			//vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(oData.ptr->m_mtx));
			vtLocks.emplace_back([&]() {
				std::unique_lock<std::shared_mutex> lock(oData.ptr->m_mtx);
#ifdef __TREE_WITH_CACHE__
				oData.ptr->m_nUseCounter.fetch_add(1, std::memory_order_relaxed);
#endif //__TREE_WITH_CACHE__
				return lock;
				}());
#else //__CONCURRENT__
			if constexpr (CacheType::MARK_INUSE_FLAG) oData.ptr->m_bInUse = true;
#endif //__CONCURRENT__

			if (oData.ptr->m_ptrCoreObject == nullptr)
			{
#ifdef __SELECTIVE_UPDATE__
				hasNewNodes = true;  // Fetching core object from storage
#endif
#ifdef __CACHE_COUNTERS__
				// Cache miss - object in cache but core object not loaded
				ptrCache->recordMiss();
#endif

				if (oData.ptr->m_uidUpdated != std::nullopt)
				{
					bUpdate = true;
					oData.uid = *oData.ptr->m_uidUpdated;
				}

				ptrCache->getCoreObject(m_nDegree, oData.uid, oData.ptr);
			}
			else
			{
#ifdef __CACHE_COUNTERS__
				// Cache hit - object and core object both available
				ptrCache->recordHit();
#endif
			}
		}

		ptr = oData.ptr;

		ASSERT(oData.ptr->m_ptrCoreObject != nullptr);

		return bUpdate;
	}

#ifdef __CONCURRENT__
	inline void getChild(const KeyType& key, CacheObjectPtr& ptr, std::vector<std::unique_lock<std::shared_mutex>>& vtLocks)
#else //__CONCURRENT__
	inline void getChild(const KeyType& key, CacheObjectPtr& ptr)
#endif //__CONCURRENT__
	{
		size_t nIdx = getChildNodeIdx(key);
		PivotData& oData = m_vtPivots[nIdx];

#ifdef __CONCURRENT__
		vtLocks.emplace_back(std::unique_lock<std::shared_mutex>(oData.ptr->m_mtx));
		ASSERT(!oData.ptr->m_mtx.try_lock());
#endif //__CONCURRENT__

		ptr = oData.ptr;

		ASSERT(oData.ptr->m_ptrCoreObject != nullptr);
	}

	template <typename CacheType>
#ifdef __CONCURRENT__
#ifdef __SELECTIVE_UPDATE__
	inline bool getChild_readonly(std::shared_ptr<CacheType>& ptrCache, const KeyType& key, CacheObjectPtr& ptr, std::vector<std::shared_lock<std::shared_mutex>>& vtLocks, bool& hasNewNodes)
#else
	inline bool getChild_readonly(std::shared_ptr<CacheType>& ptrCache, const KeyType& key, CacheObjectPtr& ptr, std::vector<std::shared_lock<std::shared_mutex>>& vtLocks)
#endif
#else //__CONCURRENT__
#ifdef __SELECTIVE_UPDATE__
	inline bool getChild_readonly(std::shared_ptr<CacheType>& ptrCache, const KeyType& key, CacheObjectPtr& ptr, bool& hasNewNodes)
#else
	inline bool getChild_readonly(std::shared_ptr<CacheType>& ptrCache, const KeyType& key, CacheObjectPtr& ptr)
#endif
#endif //__CONCURRENT__
	{
		bool bUpdate = false;

		size_t nIdx = getChildNodeIdx(key);
		PivotData& oData = m_vtPivots[nIdx];

		if (oData.ptr == nullptr)
		{
#ifdef __SELECTIVE_UPDATE__
			hasNewNodes = true;  // Fetching entire node from storage
#endif
#ifdef __CACHE_COUNTERS__
			// Cache miss - object not in cache, need to load from storage
			ptrCache->recordMiss();
#endif
			ptrCache->getObject(m_nDegree, oData.uid, oData.ptr);

#ifdef __CONCURRENT__
			//vtLocks.emplace_back(std::shared_lock<std::shared_mutex>(oData.ptr->m_mtx));
			vtLocks.emplace_back([&]() {
				std::shared_lock<std::shared_mutex> lock(oData.ptr->m_mtx);
#ifdef __TREE_WITH_CACHE__
				oData.ptr->m_nUseCounter.fetch_add(1, std::memory_order_relaxed);
#endif //__TREE_WITH_CACHE__
				return lock;
				}());
#else //__CONCURRENT__
			if constexpr (CacheType::MARK_INUSE_FLAG) oData.ptr->m_bInUse = true;
#endif //__CONCURRENT__
		}
		else
		{
#ifdef __CONCURRENT__
			//vtLocks.emplace_back(std::shared_lock<std::shared_mutex>(oData.ptr->m_mtx));
			vtLocks.emplace_back([&]() {
				std::shared_lock<std::shared_mutex> lock(oData.ptr->m_mtx);
#ifdef __TREE_WITH_CACHE__
				oData.ptr->m_nUseCounter.fetch_add(1, std::memory_order_relaxed);
#endif //__TREE_WITH_CACHE__
				return lock;
				}());
#else //__CONCURRENT__
			if constexpr (CacheType::MARK_INUSE_FLAG) oData.ptr->m_bInUse = true;
#endif //__CONCURRENT__

			if (oData.ptr->m_ptrCoreObject == nullptr)
			{
#ifdef __SELECTIVE_UPDATE__
				hasNewNodes = true;  // Fetching core object from storage
#endif
#ifdef __CACHE_COUNTERS__
				// Cache miss - object in cache but core object not loaded
				ptrCache->recordMiss();
#endif

				if (oData.ptr->m_uidUpdated != std::nullopt)
				{
					bUpdate = true;
					oData.uid = *oData.ptr->m_uidUpdated;
				}

				ptrCache->getCoreObject(m_nDegree, oData.uid, oData.ptr);
			}
			else
			{
#ifdef __CACHE_COUNTERS__
				// Cache hit - object and core object both available
				ptrCache->recordHit();
#endif
			}
		}

		ptr = oData.ptr;

		ASSERT(oData.ptr->m_ptrCoreObject != nullptr);

		return bUpdate;
	}

	template <typename CacheType>
#ifdef __SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
	inline bool getChildAtIdx(std::shared_ptr<CacheType>& ptrCache, size_t idx, CacheObjectPtr& ptr, std::unique_lock<std::shared_mutex>& lock, bool& hasNewNodes)
#else //__CONCURRENT__
	inline bool getChildAtIdx(std::shared_ptr<CacheType>& ptrCache, size_t idx, CacheObjectPtr& ptr, bool& hasNewNodes)
#endif //__CONCURRENT__
#else //__SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
	inline bool getChildAtIdx(std::shared_ptr<CacheType>& ptrCache, size_t idx, CacheObjectPtr& ptr, std::unique_lock<std::shared_mutex>& lock)
#else //__CONCURRENT__
	inline bool getChildAtIdx(std::shared_ptr<CacheType>& ptrCache, size_t idx, CacheObjectPtr& ptr)
#endif //__CONCURRENT__
#endif //__SELECTIVE_UPDATE__
	{
		bool bUpdate = false;

		PivotData& oData = m_vtPivots[idx];

		if (oData.ptr == nullptr)
		{
#ifdef __SELECTIVE_UPDATE__
				hasNewNodes = true;  // Fetching core object from storage
#endif
#ifdef __CACHE_COUNTERS__
			// Cache miss - object not in cache, need to load from storage
			ptrCache->recordMiss();
#endif
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;
			ptrCache->getObject(m_nDegree, oData.uid, oData.ptr);

#ifdef __CONCURRENT__
			lock = std::unique_lock<std::shared_mutex>(oData.ptr->m_mtx);
#ifdef __TREE_WITH_CACHE__
			oData.ptr->m_nUseCounter.fetch_add(1, std::memory_order_relaxed);
#endif //__TREE_WITH_CACHE__
#else //__CONCURRENT__
			if constexpr (CacheType::MARK_INUSE_FLAG) oData.ptr->m_bInUse = true;
#endif //__CONCURRENT__

		}
		else
		{
#ifdef __CONCURRENT__
			lock = std::unique_lock<std::shared_mutex>(oData.ptr->m_mtx);
#ifdef __TREE_WITH_CACHE__
			oData.ptr->m_nUseCounter.fetch_add(1, std::memory_order_relaxed);
#endif //__TREE_WITH_CACHE__
#else //__CONCURRENT__
			if constexpr (CacheType::MARK_INUSE_FLAG) oData.ptr->m_bInUse = true;
#endif //__CONCURRENT__

			if (oData.ptr->m_ptrCoreObject == nullptr)
			{
#ifdef __SELECTIVE_UPDATE__
				hasNewNodes = true;  // Fetching core object from storage
#endif
#ifdef __CACHE_COUNTERS__
				// Cache miss - object in cache but core object not loaded
				ptrCache->recordMiss();
#endif

				if (oData.ptr->m_uidUpdated != std::nullopt)
				{
					bUpdate = true;
					oData.uid = *oData.ptr->m_uidUpdated;
				}

				ptrCache->getCoreObject(m_nDegree, oData.uid, oData.ptr);
			}
			else
			{
#ifdef __CACHE_COUNTERS__
				// Cache hit - object and core object both available
				ptrCache->recordHit();
#endif
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
		PivotData& oData = m_vtPivots[idx];

#ifdef __CONCURRENT__
		lock = std::unique_lock<std::shared_mutex>(oData.ptr->m_mtx);
#endif //__CONCURRENT__

		ptr = oData.ptr;

		ASSERT(oData.ptr->m_ptrCoreObject != nullptr);
	}

	inline const KeyType& getFirstChild() const
	{
		return m_vtKeys[0];
	}


	inline bool canTriggerSplit() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		return m_vtKeys.size() == (2 * m_nDegree - 1);
#endif //__PROD__
	}

	inline bool requireSplit() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		return m_vtKeys.size() > (2 * m_nDegree - 1);
#endif //__PROD__
	}

	inline bool canTriggerMerge() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		return m_vtKeys.size() < m_nDegree;
#endif //__PROD__
	}

	inline bool requireMerge() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		return m_vtKeys.size() < (m_nDegree - 1);
#endif //__PROD__
	}

public:
	inline ErrorCode insert(const KeyType& pivotKey, const ObjectUIDType& uidSibling, CacheObjectPtr& ptrSibling)
	{
		auto it = std::lower_bound(m_vtKeys.begin(), m_vtKeys.end(), pivotKey);

		if (it != m_vtKeys.end() && *it == pivotKey)
		{// lay dal ayte.. make sure ayte ke duplicate omanish?
			return ErrorCode::KeyAlreadyExists;
		}

		size_t nIdx = static_cast<size_t>(std::distance(m_vtKeys.begin(), it));

		m_vtKeys.insert(m_vtKeys.begin() + nIdx, pivotKey);
		m_vtPivots.insert(m_vtPivots.begin() + nIdx + 1, PivotData(uidSibling, ptrSibling));

		return ErrorCode::Success;
	}

	template <typename CacheType>
#ifdef __TREE_WITH_CACHE__
#ifdef __SELECTIVE_UPDATE__
	inline ErrorCode rebalanceIndexNode(std::shared_ptr<CacheType>& ptrCache, SelfType* ptrChildNodeToBalance, const KeyType& key, CacheType::ObjectTypePtr& ptrNodeToDiscard, CacheType::ObjectTypePtr& ptrAffectedChildSibling, bool& hasNewNodes)
#else //__SELECTIVE_UPDATE__
	inline ErrorCode rebalanceIndexNode(std::shared_ptr<CacheType>& ptrCache, SelfType* ptrChildNodeToBalance, const KeyType& key, CacheType::ObjectTypePtr& ptrNodeToDiscard, CacheType::ObjectTypePtr& ptrAffectedChildSibling)
#endif //__SELECTIVE_UPDATE__

#else //__TREE_WITH_CACHE__
	inline ErrorCode rebalanceIndexNode(std::shared_ptr<CacheType>& ptrCache, SelfType* ptrChildNodeToBalance, const KeyType& key, CacheType::ObjectTypePtr& ptrNodeToDiscard)
#endif //__TREE_WITH_CACHE__
	{
		typedef typename CacheType::ObjectTypePtr ObjectTypePtr;

		SelfType* ptrLHSNode = nullptr;
		SelfType* ptrRHSNode = nullptr;

		ObjectTypePtr ptrLHSStorageObject = nullptr;
		ObjectTypePtr ptrRHSStorageObject = nullptr;

#ifdef __TREE_WITH_CACHE__
		ptrAffectedChildSibling = nullptr;
		//uidAffectedNode = std::nullopt;
#endif //__TREE_WITH_CACHE__

		size_t nChildIdx = getChildNodeIdx(key);

		if (nChildIdx > 0)
		{
#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock;
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;
#ifdef __SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx - 1, ptrLHSStorageObject, lock, hasNewNodes);	// Donot regarding the return value as the parent is already being set to dirty.
#else //__CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx - 1, ptrLHSStorageObject, hasNewNodes);
#endif //__CONCURRENT__			
#else //__SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx - 1, ptrLHSStorageObject, lock);	// Donot regarding the return value as the parent is already being set to dirty.
#else //__CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx - 1, ptrLHSStorageObject);
#endif //__CONCURRENT__			
#endif //__SELECTIVE_UPDATE__


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

			ptrAffectedChildSibling = ptrLHSStorageObject;
#endif //__TREE_WITH_CACHE__

			if (!ptrLHSNode->requireMerge())
			{
				KeyType key;

				ptrChildNodeToBalance->moveAnEntityFromLHSSibling(ptrLHSNode, m_vtKeys[nChildIdx - 1], key);

				m_vtKeys[nChildIdx - 1] = key;

#ifdef __TREE_WITH_CACHE__
				ptrAffectedChildSibling = ptrLHSStorageObject;
#endif //__TREE_WITH_CACHE__

				return ErrorCode::Success;
			}

			ptrLHSNode->mergeNodes(ptrChildNodeToBalance, m_vtKeys[nChildIdx - 1]);

			ptrNodeToDiscard = m_vtPivots[nChildIdx].ptr;

			m_vtKeys.erase(m_vtKeys.begin() + nChildIdx - 1);
			m_vtPivots.erase(m_vtPivots.begin() + nChildIdx);

#ifdef __TREE_WITH_CACHE__
			ptrAffectedChildSibling = ptrLHSStorageObject;
#endif //__TREE_WITH_CACHE__

			return ErrorCode::Success;
		}

		if (nChildIdx < m_vtKeys.size())
		{
#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock;
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__

#ifdef __SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx + 1, ptrRHSStorageObject, lock, hasNewNodes);
#else //__CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx + 1, ptrRHSStorageObject, hasNewNodes);
#endif //__CONCURRENT__
#else //__SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx + 1, ptrRHSStorageObject, lock);
#else //__CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx + 1, ptrRHSStorageObject);
#endif //__CONCURRENT__
#endif //__SELECTIVE_UPDATE__

#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
			this->getChildAtIdx(nChildIdx + 1, ptrRHSStorageObject, lock);
#else //__CONCURRENT__
			this->getChildAtIdx(nChildIdx + 1, ptrRHSStorageObject);
#endif //__CONCURRENT__			
#endif //__TREE_WITH_CACHE__

			ptrRHSNode = reinterpret_cast<SelfType*>(ptrRHSStorageObject->m_ptrCoreObject);

#ifdef __TREE_WITH_CACHE__
			ptrRHSStorageObject->m_bDirty = true;
#endif //__TREE_WITH_CACHE__

			if (!ptrRHSNode->requireMerge())
			{
				KeyType key;

				ptrChildNodeToBalance->moveAnEntityFromRHSSibling(ptrRHSNode, m_vtKeys[nChildIdx], key);

				m_vtKeys[nChildIdx] = key;

#ifdef __TREE_WITH_CACHE__
				ptrAffectedChildSibling = ptrRHSStorageObject;
#endif //__TREE_WITH_CACHE__

				return ErrorCode::Success;
			}

			ptrChildNodeToBalance->mergeNodes(ptrRHSNode, m_vtKeys[nChildIdx]);

			ptrNodeToDiscard = m_vtPivots[nChildIdx + 1].ptr;

			m_vtKeys.erase(m_vtKeys.begin() + nChildIdx);
			m_vtPivots.erase(m_vtPivots.begin() + nChildIdx + 1);

#ifdef __TREE_WITH_CACHE__
			ptrAffectedChildSibling = nullptr;
#endif //__TREE_WITH_CACHE__

			return ErrorCode::Success;
		}

		if (m_vtKeys.size() != 0) throw std::runtime_error("Node's (IndexNode) rebalanace logic failed!");
	}

	template <typename CacheType>
#ifdef __TREE_WITH_CACHE__
#ifdef __SELECTIVE_UPDATE__
	inline ErrorCode rebalanceDataNode(std::shared_ptr<CacheType>& ptrCache, DataNodeType* ptrChildNodeToBalance, const KeyType& key, CacheType::ObjectTypePtr& ptrNodeToDiscard, CacheType::ObjectTypePtr& ptrAffectedChildSibling, bool& hasNewNodes)
#else //__SELECTIVE_UPDATE__
	inline ErrorCode rebalanceDataNode(std::shared_ptr<CacheType>& ptrCache, DataNodeType* ptrChildNodeToBalance, const KeyType& key, CacheType::ObjectTypePtr& ptrNodeToDiscard, CacheType::ObjectTypePtr& ptrAffectedChildSibling)
#endif //__SELECTIVE_UPDATE__
#else //__TREE_WITH_CACHE__
	inline ErrorCode rebalanceDataNode(std::shared_ptr<CacheType>& ptrCache, DataNodeType* ptrChildNodeToBalance, const KeyType& key, CacheType::ObjectTypePtr& ptrNodeToDiscard)
#endif //__TREE_WITH_CACHE__
	{
		typedef typename CacheType::ObjectTypePtr ObjectTypePtr;

		DataNodeType* ptrLHSNode = nullptr;
		DataNodeType* ptrRHSNode = nullptr;

		ObjectTypePtr ptrLHSStorageObject = nullptr;
		ObjectTypePtr ptrRHSStorageObject = nullptr;

#ifdef __TREE_WITH_CACHE__
		ptrAffectedChildSibling = nullptr;
#endif //__TREE_WITH_CACHE__

		size_t nChildIdx = getChildNodeIdx(key);	// TODO: we are go through the search again.. the position can be stored ???

		if (nChildIdx > 0)
		{
#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock;
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__

#ifdef __SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx - 1, ptrLHSStorageObject, lock, hasNewNodes);
#else //__CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx - 1, ptrLHSStorageObject, hasNewNodes);
#endif //__CONCURRENT__
#else //__SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx - 1, ptrLHSStorageObject, lock);
#else //__CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx - 1, ptrLHSStorageObject);
#endif //__CONCURRENT__
#endif //__SELECTIVE_UPDATE__

#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
			this->getChildAtIdx(nChildIdx - 1, ptrLHSStorageObject, lock);
#else //__CONCURRENT__
			this->getChildAtIdx(nChildIdx - 1, ptrLHSStorageObject);
#endif //__CONCURRENT__
#endif //__TREE_WITH_CACHE__

			ptrLHSNode = reinterpret_cast<DataNodeType*>(ptrLHSStorageObject->m_ptrCoreObject);

#ifdef __TREE_WITH_CACHE__
			ptrLHSStorageObject->m_bDirty = true;
#endif //__TREE_WITH_CACHE__

			if (!ptrLHSNode->requireMerge())
			{
				KeyType key;

				ptrChildNodeToBalance->moveAnEntityFromLHSSibling(ptrLHSNode, key);

				m_vtKeys[nChildIdx - 1] = key;


#ifdef __TREE_WITH_CACHE__
				ptrAffectedChildSibling = ptrLHSStorageObject;
#endif //__TREE_WITH_CACHE__

				return ErrorCode::Success;
			}

			ptrLHSNode->mergeNode(ptrChildNodeToBalance);

			ptrNodeToDiscard = m_vtPivots[nChildIdx].ptr;

			m_vtKeys.erase(m_vtKeys.begin() + nChildIdx - 1);
			m_vtPivots.erase(m_vtPivots.begin() + nChildIdx);

#ifdef __TREE_WITH_CACHE__
			ptrAffectedChildSibling = ptrLHSStorageObject;
#endif //__TREE_WITH_CACHE__

			return ErrorCode::Success;
		}

		if (nChildIdx < m_vtKeys.size())
		{
#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock;
#endif //__CONCURRENT__

#ifdef __TREE_WITH_CACHE__

#ifdef __SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx + 1, ptrRHSStorageObject, lock, hasNewNodes);
#else //__CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx + 1, ptrRHSStorageObject, hasNewNodes);
#endif //__CONCURRENT__
#else //__SELECTIVE_UPDATE__
#ifdef __CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx + 1, ptrRHSStorageObject, lock);
#else //__CONCURRENT__
			this->getChildAtIdx<CacheType>(ptrCache, nChildIdx + 1, ptrRHSStorageObject);
#endif //__CONCURRENT__
#endif //__SELECTIVE_UPDATE__

#else //__TREE_WITH_CACHE__
#ifdef __CONCURRENT__
			this->getChildAtIdx(nChildIdx + 1, ptrRHSStorageObject, lock);
#else //__CONCURRENT__
			this->getChildAtIdx(nChildIdx + 1, ptrRHSStorageObject);
#endif //__CONCURRENT__			
#endif //__TREE_WITH_CACHE__

			ptrRHSNode = reinterpret_cast<DataNodeType*>(ptrRHSStorageObject->m_ptrCoreObject);

#ifdef __TREE_WITH_CACHE__
			ptrRHSStorageObject->m_bDirty = true;
#endif //__TREE_WITH_CACHE__

			if (!ptrRHSNode->requireMerge())
			{
				KeyType key;

				ptrChildNodeToBalance->moveAnEntityFromRHSSibling(ptrRHSNode, key);

				m_vtKeys[nChildIdx] = key;

#ifdef __TREE_WITH_CACHE__
				ptrAffectedChildSibling = ptrRHSStorageObject;
#endif //__TREE_WITH_CACHE__

				return ErrorCode::Success;
			}

			ptrChildNodeToBalance->mergeNode(ptrRHSNode);

			ptrNodeToDiscard = m_vtPivots[nChildIdx + 1].ptr;

			m_vtKeys.erase(m_vtKeys.begin() + nChildIdx);
			m_vtPivots.erase(m_vtPivots.begin() + nChildIdx + 1);

#ifdef __TREE_WITH_CACHE__
			ptrAffectedChildSibling = nullptr;
#endif //__TREE_WITH_CACHE__

			return ErrorCode::Success;
		}

		if (m_vtKeys.size() != 0) throw std::runtime_error("Node's (DataNode) rebalanace logic failed!");
	}

	template <typename CacheType, typename CacheObjectTypePtr>
	inline ErrorCode split(std::shared_ptr<CacheType> ptrCache, ObjectUIDType& uidSibling, CacheObjectTypePtr& ptrSibling, KeyType& pivotKeyForParent)
	{
		size_t nMid = m_vtKeys.size() / 2;

#ifdef __PROD__
		fix this
#else //__PROD__
		ptrCache->template createObjectOfType<SelfType>(uidSibling, ptrSibling, m_nDegree,
			m_vtKeys.begin() + nMid + 1, m_vtKeys.end(), m_vtPivots.begin() + nMid + 1, m_vtPivots.end());
#endif //__PROD__

		if (ptrSibling == nullptr)
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

		ptrSibling->m_vtKeys.clear();
		ptrSibling->m_vtPivots.clear();
	}

public:
#ifndef __SELECTIVE_UPDATE__
	template <typename CacheType, typename CacheObjectType>
	bool print(std::ofstream& os, std::shared_ptr<CacheType>& ptrCache, size_t nLevel, string stPrefix)
	{
#ifdef __CONCURRENT__

#endif //__CONCURRENT__

		bool bUpdate = false;
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
				if constexpr (std::is_arithmetic_v<KeyType>) os << " < (" << m_vtKeys[nIndex] << ")";
				else os << "< ( not printable )";
			}
			else
			{
				if constexpr (std::is_arithmetic_v<KeyType>) os << " >= (" << m_vtKeys[nIndex - 1] << ")";
				else os << ">= ( not printable )";
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
			{

#ifdef __CONCURRENT__
				std::unique_lock<std::shared_mutex> lock;
				this->getChildAtIdx(nIndex, ptrNode, lock);
#else //__CONCURRENT__
				this->getChildAtIdx(nIndex, ptrNode);
#endif //__CONCURRENT__
			}
			//ptrCache->getObject(m_vtPivots[nIndex], ptrNode);
#endif //__TREE_WITH_CACHE__

			os << std::endl;

#ifdef __TREE_WITH_CACHE__			
			vtAccessedNodes.push_back(ptrNode);
#endif //__TREE_WITH_CACHE__
			if (ptrNode->m_nCoreObjectType == SelfType::UID)
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

#ifdef __TREE_WITH_CACHE__
		ptrCache->reorder(vtAccessedNodes);
		vtAccessedNodes.clear();
#endif //__TREE_WITH_CACHE__
		return bUpdate;
	}
#endif //__SELECTIVE_UPDATE__
	void wieHiestDu()
	{
		printf("ich heisse InternalNode.\n");
	}
};
