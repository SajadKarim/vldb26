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
#include <assert.h>
#include "ErrorCodes.h"

#ifdef _MSC_VER
#define PACKED_STRUCT __pragma(pack(push, 1))
#define END_PACKED_STRUCT __pragma(pack(pop))
#else
#define PACKED_STRUCT _Pragma("pack(push, 1)")
#define END_PACKED_STRUCT _Pragma("pack(pop)")
#endif

#define MICROSEC_CHECK_FOR_FREQUENT_REQUESTS_TO_MEMORY 100
#define ACCESSED_FREQUENCY_AS_PER_THE_TIME_CHECK 10

using namespace std;

template <typename KeyType, typename ValueType, typename ObjectUIDType, typename DataNodeType, uint8_t TYPE_UID>
class IndexNodeROpt
{
private:

PACKED_STRUCT
	struct RAWDATA
	{
		uint8_t nUID;
		uint16_t nTotalPivots;
		const KeyType* ptrPivots;
		const ObjectUIDType* ptrChildren;

		uint8_t nCounter;

#ifdef _MSC_VER
		std::chrono::time_point<std::chrono::steady_clock> tLastAccessTime;
#else
		std::chrono::time_point<std::chrono::system_clock> tLastAccessTime;
#endif

		~RAWDATA()
		{
			//delete ptrPivots;
			//delete ptrChildren;

			ptrPivots = NULL;
			ptrChildren = NULL;
		}

		RAWDATA(const char* szData)
		{
			nUID = szData[0];
			nTotalPivots = *reinterpret_cast<const uint16_t*>(&szData[1]);
			ptrPivots = reinterpret_cast<const KeyType*>(szData + sizeof(uint8_t) + sizeof(uint16_t));
			ptrChildren = reinterpret_cast<const ObjectUIDType*>(szData + sizeof(uint8_t) + sizeof(uint16_t) + (nTotalPivots * sizeof(KeyType)));

			nCounter = 0;
			tLastAccessTime = std::chrono::high_resolution_clock::now();
		}
	};
END_PACKED_STRUCT

public:
	// Static UID to identify the type of the node
	static const uint8_t UID = TYPE_UID;

private:
	typedef IndexNodeROpt<KeyType, ValueType, ObjectUIDType, DataNodeType, UID> SelfType;

	typedef std::vector<KeyType>::const_iterator KeyTypeIterator;
	typedef std::vector<ObjectUIDType>::const_iterator CacheKeyTypeIterator;

private:
	// Vector to store pivot keys and child node UIDs
	std::vector<KeyType> m_vtPivots;
	std::vector<ObjectUIDType> m_vtChildren;

	RAWDATA* m_ptrRawData = nullptr;
public:
	// Destructor: Clears pivot and child vectors
	~IndexNodeROpt()
	{
		m_vtPivots.clear();
		m_vtChildren.clear();

		delete m_ptrRawData;
		m_ptrRawData = nullptr;
	}

	// Default constructor
	IndexNodeROpt()
		: m_ptrRawData(nullptr)
	{
	}

	// Copy constructor: Copies pivot keys and child UIDs from another IndexNodeROpt
	IndexNodeROpt(const IndexNodeROpt& source)
		: m_ptrRawData(nullptr)
	{
		m_vtPivots.assign(source.m_vtPivots.begin(), source.m_vtPivots.end());
		m_vtChildren.assign(source.m_vtChildren.begin(), source.m_vtChildren.end());
	}

	// Constructor that deserializes the node from raw data
	IndexNodeROpt(const char* szData)
		: m_ptrRawData(nullptr)
	{
		m_vtPivots.reserve(0);
		m_vtChildren.reserve(0);

		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<typename ObjectUIDType::NodeUID>::value &&
			std::is_standard_layout<typename ObjectUIDType::NodeUID>::value)
		{
			m_ptrRawData = new RAWDATA(szData);

			assert(UID == m_ptrRawData->nUID);

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

	// Constructor that deserializes the node from a file stream
	IndexNodeROpt(std::fstream& fs)
		: m_ptrRawData(nullptr)
	{
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<typename ObjectUIDType::NodeUID>::value &&
			std::is_standard_layout<typename ObjectUIDType::NodeUID>::value)
		{
			uint16_t nPivotCount;
			fs.read(reinterpret_cast<char*>(&nPivotCount), sizeof(uint16_t));

			m_vtPivots.resize(nPivotCount);
			m_vtChildren.resize(nPivotCount + 1);

			fs.read(reinterpret_cast<char*>(m_vtPivots.data()), nPivotCount * sizeof(KeyType));
			fs.read(reinterpret_cast<char*>(m_vtChildren.data()), (nPivotCount + 1) * sizeof(typename ObjectUIDType::NodeUID));
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

	// Constructor that initializes the node with iterators over pivot keys and children
	IndexNodeROpt(KeyTypeIterator itBeginPivots, KeyTypeIterator itEndPivots, CacheKeyTypeIterator itBeginChildren, CacheKeyTypeIterator itEndChildren)
		: m_ptrRawData(nullptr)
	{
		m_vtPivots.assign(itBeginPivots, itEndPivots);
		m_vtChildren.assign(itBeginChildren, itEndChildren);
	}

	// Constructor that creates an internal node with a pivot key and two child UIDs
	IndexNodeROpt(const KeyType& pivotKey, const ObjectUIDType& ptrLHSNode, const ObjectUIDType& ptrRHSNode)
		: m_ptrRawData(nullptr)
	{
		m_vtPivots.push_back(pivotKey);
		m_vtChildren.push_back(ptrLHSNode);
		m_vtChildren.push_back(ptrRHSNode);
	}

public:
#ifdef __TRACK_CACHE_FOOTPRINT__
	inline int32_t moveDataToDRAM()
#else //__TRACK_CACHE_FOOTPRINT__
	inline void moveDataToDRAM()
#endif //__TRACK_CACHE_FOOTPRINT__
	{
		assert(m_ptrRawData != nullptr);

#ifdef __TRACK_CACHE_FOOTPRINT__
		int32_t nMemoryFootprint = 0;
		nMemoryFootprint -= getMemoryFootprint();
#endif //__TRACK_CACHE_FOOTPRINT__

		m_vtPivots.resize(m_ptrRawData->nTotalPivots);
		m_vtChildren.resize(m_ptrRawData->nTotalPivots + 1);

		memcpy(m_vtPivots.data(), m_ptrRawData->ptrPivots, m_ptrRawData->nTotalPivots * sizeof(KeyType));
		memcpy(m_vtChildren.data(), m_ptrRawData->ptrChildren, (m_ptrRawData->nTotalPivots + 1) * sizeof(typename ObjectUIDType::NodeUID));

#ifdef __VALIDITY_CHECK__
		assert(m_vtPivots.size() + 1 == m_vtChildren.size());

		assert(m_ptrRawData->nUID == UID);
		assert(m_ptrRawData->nTotalPivots == m_vtPivots.size());
		for (int idx = 0, end = m_ptrRawData->nTotalPivots; idx < end; idx++)
		{
			assert(*(m_ptrRawData->ptrPivots + idx) == m_vtPivots[idx]);
			assert(*(m_ptrRawData->ptrChildren + idx) == m_vtChildren[idx]);
			assert(*(m_ptrRawData->ptrChildren + idx + 1) == m_vtChildren[idx + 1]);
		}
#endif //__VALIDITY_CHECK__

		delete m_ptrRawData;
		m_ptrRawData = nullptr;

#ifdef __TRACK_CACHE_FOOTPRINT__
		nMemoryFootprint += getMemoryFootprint();
		return nMemoryFootprint;
#endif //__TRACK_CACHE_FOOTPRINT__
	}

	// Writes the node's data to a file stream
	inline void writeToStream(std::fstream& fs, uint8_t& uidObjectType, uint32_t& nDataSize) const
	{
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<typename ObjectUIDType::NodeUID>::value &&
			std::is_standard_layout<typename ObjectUIDType::NodeUID>::value)
		{
			uidObjectType = SelfType::UID;

			uint16_t nPivotCount = m_vtPivots.size();

			nDataSize = sizeof(uint8_t)					// UID
				+ sizeof(uint16_t)						// Total keys
				+ (nPivotCount * sizeof(KeyType))			// Size of all keys
				+ ((nPivotCount + 1) * sizeof(typename ObjectUIDType::NodeUID));	// Size of all values

			fs.write(reinterpret_cast<const char*>(&uidObjectType), sizeof(uint8_t));
			fs.write(reinterpret_cast<const char*>(&nPivotCount), sizeof(uint16_t));
			fs.write(reinterpret_cast<const char*>(m_vtPivots.data()), nPivotCount * sizeof(KeyType));
			fs.write(reinterpret_cast<const char*>(m_vtChildren.data()), (nPivotCount + 1) * sizeof(typename ObjectUIDType::NodeUID));	// fix it!

#ifdef __VALIDITY_CHECK__
			for (auto it = m_vtChildren.begin(); it != m_vtChildren.end(); it++)
			{
				assert((*it).getMediaType() > 2);
			}
#endif //__VALIDITY_CHECK__
		}
		else
		{
			static_assert(
				std::is_trivial<KeyType>::value &&
				std::is_standard_layout<KeyType>::value &&
				std::is_trivial<ValueType>::value &&
				std::is_standard_layout<ValueType>::value,
				"Non-POD type is provided. Kindly implement custome de/serializer.");
		}
	}

	// Serializes the node's data into a char buffer
	inline void serialize(char*& szBuffer, uint8_t& uidObjectType, uint32_t& nBufferSize) const
	{
		assert(m_ptrRawData == nullptr);

		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<typename ObjectUIDType::NodeUID>::value &&
			std::is_standard_layout<typename ObjectUIDType::NodeUID>::value)
		{
			uidObjectType = UID;

			uint16_t nPivotCount = m_vtPivots.size();

			nBufferSize = sizeof(uint8_t)				// UID
				+ sizeof(uint16_t)						// Total keys
				+ (nPivotCount * sizeof(KeyType))			// Size of all keys
				+ ( (nPivotCount +1)* sizeof(typename ObjectUIDType::NodeUID));	// Size of all values

			szBuffer = new char[nBufferSize + 1];
			memset(szBuffer, 0, nBufferSize + 1);

			size_t nOffset = 0;
			memcpy(szBuffer, &uidObjectType, sizeof(uint8_t));
			nOffset += sizeof(uint8_t);

			memcpy(szBuffer + nOffset, &nPivotCount, sizeof(uint16_t));
			nOffset += sizeof(uint16_t);

			size_t nKeysSize = nPivotCount * sizeof(KeyType);
			memcpy(szBuffer + nOffset, m_vtPivots.data(), nKeysSize);
			nOffset += nKeysSize;

			size_t nValuesSize = (nPivotCount + 1) * sizeof(typename ObjectUIDType::NodeUID);
			memcpy(szBuffer + nOffset, m_vtChildren.data(), nValuesSize);

#ifdef __VALIDITY_CHECK__
			const RAWDATA* ptrRawData = new RAWDATA(szBuffer);
			assert(ptrRawData->nUID == UID);
			assert(ptrRawData->nTotalPivots == m_vtPivots.size());
			for (int idx = 0, end = ptrRawData->nTotalPivots; idx < end; idx++)
			{
				assert(*(ptrRawData->ptrPivots + idx) == m_vtPivots[idx]);
				assert(*(ptrRawData->ptrChildren + idx) == m_vtChildren[idx]);
				assert(*(ptrRawData->ptrChildren + idx + 1) == m_vtChildren[idx + 1]);
			}

			for (auto it = m_vtChildren.begin(); it != m_vtChildren.end(); it++)
			{
				assert((*it).getMediaType() > 1);
			}
#endif //__VALIDITY_CHECK__
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
	
	inline bool canAccessDataDirectly()
	{
		if (m_ptrRawData == nullptr)
			return false;

		auto now = std::chrono::high_resolution_clock::now();
		auto duration = now - m_ptrRawData->tLastAccessTime;

		if (std::chrono::duration_cast<std::chrono::microseconds>(duration).count() < MICROSEC_CHECK_FOR_FREQUENT_REQUESTS_TO_MEMORY)
		{
			m_ptrRawData->nCounter++;

			if (m_ptrRawData->nCounter >= ACCESSED_FREQUENCY_AS_PER_THE_TIME_CHECK)
			{
				moveDataToDRAM();
				return false;
			}
		}

		m_ptrRawData->nCounter = 0; // reset counter if more than 100 nanoseconds have passed		
		m_ptrRawData->tLastAccessTime = now;
		return true;
	}
	
	// Returns the number of keys (pivots) in the node
	inline size_t getKeysCount() const
	{
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalPivots;
		}

		return m_vtPivots.size();
	}

	// Finds the index of the child node for the given key
	inline size_t getChildNodeIdx(const KeyType& key)
	{
		if (canAccessDataDirectly())
		{
			auto it = std::upper_bound(m_ptrRawData->ptrPivots, m_ptrRawData->ptrPivots + m_ptrRawData->nTotalPivots, key);
			return std::distance(m_ptrRawData->ptrPivots, it);
		}

		assert(m_vtPivots.size() + 1 == m_vtChildren.size());

		auto it = std::upper_bound(m_vtPivots.begin(), m_vtPivots.end(), key);
		return std::distance(m_vtPivots.begin(), it);
	}

	// Gets the child at the given index
	inline const ObjectUIDType& getChildAt(size_t nIdx)
	{
		if( canAccessDataDirectly())
		{
			return m_ptrRawData->ptrChildren[nIdx];
		}

		return m_vtChildren[nIdx];
	}

	// Gets the child node corresponding to the given key
	inline const ObjectUIDType& getChild(const KeyType& key)
	{
		if (canAccessDataDirectly())
		{
			return m_ptrRawData->ptrChildren[getChildNodeIdx(key)];
		}

		assert(m_vtPivots.size() + 1 == m_vtChildren.size());

		return m_vtChildren[getChildNodeIdx(key)];
	}

	// Returns the first pivot key
	inline const KeyType& getFirstChild()
	{
		if (canAccessDataDirectly())
		{
			return m_ptrRawData->ptrPivots[0];
		}

		return m_vtPivots[0];
	}

	// Checks if the node requires a split based on the degree
	inline bool requireSplit(size_t nDegree) const
	{
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalPivots > nDegree;
		}

		return m_vtPivots.size() > nDegree;
	}

	inline bool canTriggerSplit(size_t nDegree) const
	{
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalPivots + 1 > nDegree;
		}

		return m_vtPivots.size() + 1 > nDegree;
	}

	inline bool canTriggerMerge(size_t nDegree) const
	{
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalPivots <= std::ceil(nDegree / 2.0f) + 1;
		}

		return m_vtPivots.size() <= std::ceil(nDegree / 2.0f) + 1;	// TODO: macro!
	}

	inline bool requireMerge(size_t nDegree) const
	{
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalPivots <= std::ceil(nDegree / 2.0f);
		}

		return m_vtPivots.size() <= std::ceil(nDegree / 2.0f);
	}

	inline size_t getSize() const
	{
		assert(m_ptrRawData == nullptr);

		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			return 
				sizeof(uint8_t)
				+ sizeof(int16_t)
				+ (m_vtPivots.size() * sizeof(KeyType))
				+ (m_vtChildren.size() * sizeof(typename ObjectUIDType::NodeUID));
		}
		else
		{
			static_assert(
				std::is_trivial<KeyType>::value &&
				std::is_standard_layout<KeyType>::value &&
				std::is_trivial<ValueType>::value &&
				std::is_standard_layout<ValueType>::value,
				"Non-POD type is provided. Kindly provide functionality to calculate size.");
		}
	}

	template <typename CacheObjectType>
#ifdef __TRACK_CACHE_FOOTPRINT__
	int32_t updateChildUID(std::shared_ptr<CacheObjectType> ptrChildNode, const ObjectUIDType& uidOld, const ObjectUIDType& uidNew)
#else //__TRACK_CACHE_FOOTPRINT__
	void updateChildUID(std::shared_ptr<CacheObjectType> ptrChildNode, const ObjectUIDType& uidOld, const ObjectUIDType& uidNew)
#endif //__TRACK_CACHE_FOOTPRINT__
	{
#ifdef __TRACK_CACHE_FOOTPRINT__
		int32_t nMemoryFootprint = 0;
#endif //__TRACK_CACHE_FOOTPRINT__

		if (m_ptrRawData != nullptr)
		{
#ifdef __TRACK_CACHE_FOOTPRINT__
			nMemoryFootprint += moveDataToDRAM();
#else //__TRACK_CACHE_FOOTPRINT__
			moveDataToDRAM();
#endif //__TRACK_CACHE_FOOTPRINT__
		}

		const KeyType* key = nullptr;
		if (std::holds_alternative<std::shared_ptr<SelfType>>(ptrChildNode->getInnerData()))
		{
			std::shared_ptr<SelfType> ptrIndexNode = std::get<std::shared_ptr<SelfType>>(ptrChildNode->getInnerData());
			key = &ptrIndexNode->getFirstChild();
		}
		else //if (std::holds_alternative<std::shared_ptr<DataNodeType>>(ptrChildNode->getInnerData()))
		{
			std::shared_ptr<DataNodeType> ptrDataNode = std::get<std::shared_ptr<DataNodeType>>(ptrChildNode->getInnerData());
			key = &ptrDataNode->getFirstChild();
		}

		auto it = std::upper_bound(m_vtPivots.begin(), m_vtPivots.end(), *key);
		auto index = std::distance(m_vtPivots.begin(), it);

		assert(m_vtChildren[index] == uidOld);

		m_vtChildren[index] = uidNew;

#ifdef __TRACK_CACHE_FOOTPRINT__
		return nMemoryFootprint;
#else //__TRACK_CACHE_FOOTPRINT__
		return;
#endif //__TRACK_CACHE_FOOTPRINT__
	}

	template <typename CacheObjectType>
	bool updateChildrenUIDs(std::unordered_map<ObjectUIDType, std::pair<std::optional<ObjectUIDType>, std::shared_ptr<CacheObjectType>>>& mpUIDUpdates)
	{
		bool bDirty = false;

		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		for (auto it = m_vtChildren.begin(), itend = m_vtChildren.end(); it != itend; it++)
		{
			if (mpUIDUpdates.find(*it) != mpUIDUpdates.end())
			{
				ObjectUIDType uidTemp = *it;

				*it = *(mpUIDUpdates[*it].first);

				mpUIDUpdates.erase(uidTemp);

				bDirty = true;
			}
		}

		return bDirty;
	}

	inline size_t getMemoryFootprint() const
	{
		if (m_ptrRawData != nullptr)
		{
			return 
				sizeof(*this) 
				+ sizeof(RAWDATA);
		}

		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			return 
				sizeof(*this) 
				+ (m_vtPivots.capacity() * sizeof(KeyType)) 
				+ (m_vtChildren.capacity() * sizeof(ObjectUIDType));
		}
		else
		{
			static_assert(
				std::is_trivial<KeyType>::value &&
				std::is_standard_layout<KeyType>::value &&
				std::is_trivial<ValueType>::value &&
				std::is_standard_layout<ValueType>::value,
				"Non-POD type is provided. Kindly provide functionality to calculate size.");
		}
	}

public:
#ifdef __TRACK_CACHE_FOOTPRINT__
	inline ErrorCode insert(const KeyType& pivotKey, const ObjectUIDType& uidSibling, int32_t& nMemoryFootprint)
#else //__TRACK_CACHE_FOOTPRINT__
	inline ErrorCode insert(const KeyType& pivotKey, const ObjectUIDType& uidSibling)
#endif //__TRACK_CACHE_FOOTPRINT__
	{
		if (m_ptrRawData != nullptr)
		{
#ifdef __TRACK_CACHE_FOOTPRINT__
			nMemoryFootprint += moveDataToDRAM();
#else //__TRACK_CACHE_FOOTPRINT__
			moveDataToDRAM();
#endif //__TRACK_CACHE_FOOTPRINT__
		}

#ifdef __TRACK_CACHE_FOOTPRINT__
		uint32_t nPivotContainerCapacity = m_vtPivots.capacity();
		uint32_t nChildrenContainerCapacity = m_vtChildren.capacity();
#endif //__TRACK_CACHE_FOOTPRINT__

		auto it = std::lower_bound(m_vtPivots.begin(), m_vtPivots.end(), pivotKey);
		auto nChildIdx = std::distance(m_vtPivots.begin(), it);

		m_vtPivots.insert(m_vtPivots.begin() + nChildIdx, pivotKey);
		m_vtChildren.insert(m_vtChildren.begin() + nChildIdx + 1, uidSibling);

#ifdef __TRACK_CACHE_FOOTPRINT__
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			if (nPivotContainerCapacity != m_vtPivots.capacity())
			{
				nMemoryFootprint -= nPivotContainerCapacity * sizeof(KeyType);
				nMemoryFootprint += m_vtPivots.capacity() * sizeof(KeyType);
			}

			if (nChildrenContainerCapacity != m_vtChildren.capacity())
			{
				nMemoryFootprint -= nChildrenContainerCapacity * sizeof(ObjectUIDType);
				nMemoryFootprint += m_vtChildren.capacity() * sizeof(ObjectUIDType);
			}
		}
		else
		{
			static_assert(
				std::is_trivial<KeyType>::value &&
				std::is_standard_layout<KeyType>::value &&
				std::is_trivial<ValueType>::value &&
				std::is_standard_layout<ValueType>::value,
				"Non-POD type is provided. Kindly provide functionality to calculate size.");
		}
#endif //__TRACK_CACHE_FOOTPRINT__

		return ErrorCode::Success;
	}

	template <typename CacheType>
#ifdef __TREE_WITH_CACHE__

#ifdef __TRACK_CACHE_FOOTPRINT__
	inline ErrorCode rebalanceIndexNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, std::shared_ptr<SelfType>& ptrChild, const KeyType& key, size_t nDegree, std::optional<ObjectUIDType>& uidObjectToDelete, std::optional<ObjectUIDType>& uidAffectedNode, CacheType::ObjectTypePtr& ptrAffectedNode, int32_t& nMemoryFootprint)
#else //__TRACK_CACHE_FOOTPRINT__
		inline ErrorCode rebalanceIndexNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, std::shared_ptr<SelfType>& ptrChild, const KeyType& key, size_t nDegree, std::optional<ObjectUIDType>& uidObjectToDelete, std::optional<ObjectUIDType>& uidAffectedNode, CacheType::ObjectTypePtr& ptrAffectedNode)
#endif //__TRACK_CACHE_FOOTPRINT__

#else //__TREE_WITH_CACHE__
	inline ErrorCode rebalanceIndexNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, std::shared_ptr<SelfType>& ptrChild, const KeyType& key, size_t nDegree, std::optional<ObjectUIDType>& uidObjectToDelete)
#endif //__TREE_WITH_CACHE__
	{
		if (m_ptrRawData != nullptr)
		{
#ifdef __TRACK_CACHE_FOOTPRINT__
			nMemoryFootprint += moveDataToDRAM();
#else //__TRACK_CACHE_FOOTPRINT__
			moveDataToDRAM();
#endif //__TRACK_CACHE_FOOTPRINT__
		}

		if (ptrChild->m_ptrRawData != nullptr)
		{
#ifdef __TRACK_CACHE_FOOTPRINT__
			nMemoryFootprint += ptrChild->moveDataToDRAM();
#else //__TRACK_CACHE_FOOTPRINT__
			ptrChild->moveDataToDRAM();
#endif //__TRACK_CACHE_FOOTPRINT__
		}
		
		typedef typename CacheType::ObjectTypePtr ObjectTypePtr;

		ObjectTypePtr ptrLHSStorageObject = nullptr;
		ObjectTypePtr ptrRHSStorageObject = nullptr;

		std::shared_ptr<SelfType> ptrLHSNode = nullptr;
		std::shared_ptr<SelfType> ptrRHSNode = nullptr;

#ifdef __TREE_WITH_CACHE__
		uidAffectedNode = std::nullopt;
		ptrAffectedNode = nullptr;
#endif //__TREE_WITH_CACHE__

		size_t nChildIdx = getChildNodeIdx(key);

		if (nChildIdx > 0)
		{
#ifdef __TREE_WITH_CACHE__
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;
			ptrCache->getObject(m_vtChildren[nChildIdx - 1], ptrLHSStorageObject, uidUpdated);    //TODO: lock
#else //__TREE_WITH_CACHE__
			ptrCache->template getObjectOfType<std::shared_ptr<SelfType>>(m_vtChildren[nChildIdx - 1], ptrLHSNode, ptrLHSStorageObject);    //TODO: lock
#endif //__TREE_WITH_CACHE__

#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock(ptrLHSStorageObject->getMutex());
#endif //__CONCURRENT__

			//if (std::holds_alternative<std::shared_ptr<SelfType>>(ptrLHSStorageObject->getInnerData()))
			{
				ptrLHSNode = std::get<std::shared_ptr<SelfType>>(ptrLHSStorageObject->getInnerData());
			}

#ifdef __TREE_WITH_CACHE__
			if (uidUpdated != std::nullopt)
			{
				m_vtChildren[nChildIdx - 1] = *uidUpdated;
			}

			ptrLHSStorageObject->setDirtyFlag(true);

			uidAffectedNode = m_vtChildren[nChildIdx - 1];
			ptrAffectedNode = ptrLHSStorageObject;
#endif //__TREE_WITH_CACHE__

			if (ptrLHSNode->getKeysCount() > std::ceil(nDegree / 2.0f))	// TODO: macro?
			{
				KeyType key;

#ifdef __TRACK_CACHE_FOOTPRINT__
				ptrChild->moveAnEntityFromLHSSibling(ptrLHSNode, m_vtPivots[nChildIdx - 1], key, nMemoryFootprint);
#else //__TRACK_CACHE_FOOTPRINT__
				ptrChild->moveAnEntityFromLHSSibling(ptrLHSNode, m_vtPivots[nChildIdx - 1], key);
#endif //__TRACK_CACHE_FOOTPRINT__

				m_vtPivots[nChildIdx - 1] = key;
				return ErrorCode::Success;
			}

#ifdef __TRACK_CACHE_FOOTPRINT__
			ptrLHSNode->mergeNodes(ptrChild, m_vtPivots[nChildIdx - 1], nMemoryFootprint);
#else //__TRACK_CACHE_FOOTPRINT__
			ptrLHSNode->mergeNodes(ptrChild, m_vtPivots[nChildIdx - 1]);
#endif //__TRACK_CACHE_FOOTPRINT__

			uidObjectToDelete = m_vtChildren[nChildIdx];
			assert(uidObjectToDelete == uidChild);

			m_vtPivots.erase(m_vtPivots.begin() + nChildIdx - 1);
			m_vtChildren.erase(m_vtChildren.begin() + nChildIdx);

			return ErrorCode::Success;
		}

		if (nChildIdx < m_vtPivots.size())
		{
#ifdef __TREE_WITH_CACHE__
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;
			ptrCache->getObject(m_vtChildren[nChildIdx + 1], ptrRHSStorageObject, uidUpdated);    //TODO: lock
#else //__TREE_WITH_CACHE__
			ptrCache->template getObjectOfType<std::shared_ptr<SelfType>>(m_vtChildren[nChildIdx + 1], ptrRHSNode, ptrRHSStorageObject);    //TODO: lock
#endif //__TREE_WITH_CACHE__

#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock(ptrRHSStorageObject->getMutex());
#endif //__CONCURRENT__

			//if (std::holds_alternative<std::shared_ptr<SelfType>>(ptrRHSStorageObject->getInnerData()))
			{
				ptrRHSNode = std::get<std::shared_ptr<SelfType>>(ptrRHSStorageObject->getInnerData());
			}

#ifdef __TREE_WITH_CACHE__
			if (uidUpdated != std::nullopt)
			{
				m_vtChildren[nChildIdx + 1] = *uidUpdated;
			}

			ptrRHSStorageObject->setDirtyFlag(true);

			uidAffectedNode = m_vtChildren[nChildIdx + 1];
			ptrAffectedNode = ptrRHSStorageObject;
#endif //__TREE_WITH_CACHE__

			if (ptrRHSNode->getKeysCount() > std::ceil(nDegree / 2.0f))
			{
				KeyType key;

#ifdef __TRACK_CACHE_FOOTPRINT__
				ptrChild->moveAnEntityFromRHSSibling(ptrRHSNode, m_vtPivots[nChildIdx], key, nMemoryFootprint);
#else //__TRACK_CACHE_FOOTPRINT__
				ptrChild->moveAnEntityFromRHSSibling(ptrRHSNode, m_vtPivots[nChildIdx], key);
#endif //__TRACK_CACHE_FOOTPRINT__

				m_vtPivots[nChildIdx] = key;
				return ErrorCode::Success;
			}

#ifdef __TRACK_CACHE_FOOTPRINT__
			ptrChild->mergeNodes(ptrRHSNode, m_vtPivots[nChildIdx], nMemoryFootprint);
#else //__TRACK_CACHE_FOOTPRINT__
			ptrChild->mergeNodes(ptrRHSNode, m_vtPivots[nChildIdx]);
#endif //__TRACK_CACHE_FOOTPRINT__

			assert(uidChild == m_vtChildren[nChildIdx]);

			uidObjectToDelete = m_vtChildren[nChildIdx + 1];

			m_vtPivots.erase(m_vtPivots.begin() + nChildIdx);
			m_vtChildren.erase(m_vtChildren.begin() + nChildIdx + 1);

			return ErrorCode::Success;
		}

		std::cout << "Critical State: The rebalance logic for IndexNode failed." << std::endl;
		throw new std::logic_error(".....");   // TODO: critical log.
	}

	template <typename CacheType>
#ifdef __TREE_WITH_CACHE__

#ifdef __TRACK_CACHE_FOOTPRINT__
	inline ErrorCode rebalanceDataNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, std::shared_ptr<DataNodeType>& ptrChild, const KeyType& key, size_t nDegree, std::optional<ObjectUIDType>& uidObjectToDelete, std::optional<ObjectUIDType>& uidAffectedNode, CacheType::ObjectTypePtr& ptrAffectedNode, int32_t& nMemoryFootprint)
#else //__TRACK_CACHE_FOOTPRINT__
	inline ErrorCode rebalanceDataNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, std::shared_ptr<DataNodeType>& ptrChild, const KeyType& key, size_t nDegree, std::optional<ObjectUIDType>& uidObjectToDelete, std::optional<ObjectUIDType>& uidAffectedNode, CacheType::ObjectTypePtr& ptrAffectedNode)
#endif //__TRACK_CACHE_FOOTPRINT__

#else //__TREE_WITH_CACHE__
	inline ErrorCode rebalanceDataNode(std::shared_ptr<CacheType>& ptrCache, const ObjectUIDType& uidChild, std::shared_ptr<DataNodeType>& ptrChild, const KeyType& key, size_t nDegree, std::optional<ObjectUIDType>& uidObjectToDelete)
#endif //__TREE_WITH_CACHE__
	{
		if (m_ptrRawData != nullptr)
		{
#ifdef __TRACK_CACHE_FOOTPRINT__
			nMemoryFootprint += moveDataToDRAM();
#else //__TRACK_CACHE_FOOTPRINT__
			moveDataToDRAM();
#endif //__TRACK_CACHE_FOOTPRINT__
		}

		typedef typename CacheType::ObjectTypePtr ObjectTypePtr;

		ObjectTypePtr ptrLHSStorageObject = nullptr;
		ObjectTypePtr ptrRHSStorageObject = nullptr;

		std::shared_ptr<DataNodeType> ptrLHSNode = nullptr;
		std::shared_ptr<DataNodeType> ptrRHSNode = nullptr;

#ifdef __TREE_WITH_CACHE__
		uidAffectedNode = std::nullopt;
		ptrAffectedNode = nullptr;
#endif //__TREE_WITH_CACHE__

		size_t nChildIdx = getChildNodeIdx(key);

		if (nChildIdx > 0)
		{
#ifdef __TREE_WITH_CACHE__
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;
			ptrCache->getObject(m_vtChildren[nChildIdx - 1], ptrLHSStorageObject, uidUpdated);    //TODO: lock
#else //__TREE_WITH_CACHE__
			ptrCache->template getObjectOfType<std::shared_ptr<DataNodeType>>(m_vtChildren[nChildIdx - 1], ptrLHSNode, ptrLHSStorageObject);    //TODO: lock
#endif //__TREE_WITH_CACHE__

#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock(ptrLHSStorageObject->getMutex());
#endif //__CONCURRENT__

			//if (std::holds_alternative<std::shared_ptr<DataNodeType>>(ptrLHSStorageObject->getInnerData()))
			{
				ptrLHSNode = std::get<std::shared_ptr<DataNodeType>>(ptrLHSStorageObject->getInnerData());
			}

#ifdef __TREE_WITH_CACHE__
			if (uidUpdated != std::nullopt)
			{
				m_vtChildren[nChildIdx - 1] = *uidUpdated;
			}

			ptrLHSStorageObject->setDirtyFlag(true);

			uidAffectedNode = m_vtChildren[nChildIdx - 1];
			ptrAffectedNode = ptrLHSStorageObject;
#endif //__TREE_WITH_CACHE__

			if (ptrLHSNode->getKeysCount() > std::ceil(nDegree / 2.0f))
			{
				KeyType key;

#ifdef __TRACK_CACHE_FOOTPRINT__
				ptrChild->moveAnEntityFromLHSSibling(ptrLHSNode, key, nMemoryFootprint);
#else //__TRACK_CACHE_FOOTPRINT__
				ptrChild->moveAnEntityFromLHSSibling(ptrLHSNode, key);
#endif //__TRACK_CACHE_FOOTPRINT__

				m_vtPivots[nChildIdx - 1] = key;

				return ErrorCode::Success;
			}

#ifdef __TRACK_CACHE_FOOTPRINT__
			ptrLHSNode->mergeNode(ptrChild, nMemoryFootprint);
#else //__TRACK_CACHE_FOOTPRINT__
			ptrLHSNode->mergeNode(ptrChild);
#endif //__TRACK_CACHE_FOOTPRINT__

			uidObjectToDelete = m_vtChildren[nChildIdx];
			assert(uidObjectToDelete == uidChild);

			m_vtPivots.erase(m_vtPivots.begin() + nChildIdx - 1);
			m_vtChildren.erase(m_vtChildren.begin() + nChildIdx);

			return ErrorCode::Success;
		}

		if (nChildIdx < m_vtPivots.size())
		{
#ifdef __TREE_WITH_CACHE__
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;
			ptrCache->getObject(m_vtChildren[nChildIdx + 1], ptrRHSStorageObject, uidUpdated);    //TODO: lock
#else //__TREE_WITH_CACHE__
			ptrCache->template getObjectOfType<std::shared_ptr<DataNodeType>>(m_vtChildren[nChildIdx + 1], ptrRHSNode, ptrRHSStorageObject);    //TODO: lock
#endif //__TREE_WITH_CACHE__

#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock(ptrRHSStorageObject->getMutex());
#endif //__CONCURRENT__

			//if (std::holds_alternative<std::shared_ptr<DataNodeType>>(ptrRHSStorageObject->getInnerData()))
			{
				ptrRHSNode = std::get<std::shared_ptr<DataNodeType>>(ptrRHSStorageObject->getInnerData());
			}

#ifdef __TREE_WITH_CACHE__
			if (uidUpdated != std::nullopt)
			{
				m_vtChildren[nChildIdx + 1] = *uidUpdated;
			}

			ptrRHSStorageObject->setDirtyFlag(true);

			uidAffectedNode = m_vtChildren[nChildIdx + 1];
			ptrAffectedNode = ptrRHSStorageObject;
#endif //__TREE_WITH_CACHE__

			if (ptrRHSNode->getKeysCount() > std::ceil(nDegree / 2.0f))
			{
				KeyType key;

#ifdef __TRACK_CACHE_FOOTPRINT__
				ptrChild->moveAnEntityFromRHSSibling(ptrRHSNode, key, nMemoryFootprint);
#else //__TRACK_CACHE_FOOTPRINT__
				ptrChild->moveAnEntityFromRHSSibling(ptrRHSNode, key);
#endif //__TRACK_CACHE_FOOTPRINT__

				m_vtPivots[nChildIdx] = key;
				return ErrorCode::Success;
			}

#ifdef __TRACK_CACHE_FOOTPRINT__
			ptrChild->mergeNode(ptrRHSNode, nMemoryFootprint);
#else //__TRACK_CACHE_FOOTPRINT__
			ptrChild->mergeNode(ptrRHSNode);
#endif //__TRACK_CACHE_FOOTPRINT__

			uidObjectToDelete = m_vtChildren[nChildIdx + 1];

			m_vtPivots.erase(m_vtPivots.begin() + nChildIdx);
			m_vtChildren.erase(m_vtChildren.begin() + nChildIdx + 1);

			return ErrorCode::Success;
		}

		std::cout << "Critical State: The rebalance logic for DataNode failed." << std::endl;
		throw new std::logic_error(".....");   // TODO: critical log.
	}

	template <typename CacheType, typename CacheObjectTypePtr>
#ifdef __TRACK_CACHE_FOOTPRINT__
	inline ErrorCode split(std::shared_ptr<CacheType> ptrCache, std::optional<ObjectUIDType>& uidSibling, CacheObjectTypePtr& ptrSibling, KeyType& pivotKeyForParent, int32_t& nMemoryFootprint)
#else //__TRACK_CACHE_FOOTPRINT__
	inline ErrorCode split(std::shared_ptr<CacheType> ptrCache, std::optional<ObjectUIDType>& uidSibling, CacheObjectTypePtr& ptrSibling, KeyType& pivotKeyForParent)
#endif //__TRACK_CACHE_FOOTPRINT__
	{
		if (m_ptrRawData != nullptr)
		{
#ifdef __TRACK_CACHE_FOOTPRINT__
			nMemoryFootprint += moveDataToDRAM();
#else //__TRACK_CACHE_FOOTPRINT__
			moveDataToDRAM();
#endif //__TRACK_CACHE_FOOTPRINT__
		}

#ifdef __TRACK_CACHE_FOOTPRINT__
		uint32_t nPivotContainerCapacity = m_vtPivots.capacity();
		uint32_t nChildrenContainerCapacity = m_vtChildren.capacity();
#endif //__TRACK_CACHE_FOOTPRINT__

		size_t nMid = m_vtPivots.size() / 2;

		ptrCache->template createObjectOfType<SelfType>(uidSibling, ptrSibling,
			m_vtPivots.begin() + nMid + 1, m_vtPivots.end(),
			m_vtChildren.begin() + nMid + 1, m_vtChildren.end());

		if (!uidSibling)
		{
			return ErrorCode::Error;
		}

		pivotKeyForParent = m_vtPivots[nMid];

		m_vtPivots.resize(nMid);
		m_vtChildren.resize(nMid + 1);

#ifdef __TRACK_CACHE_FOOTPRINT__
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			if (nPivotContainerCapacity != m_vtPivots.capacity())
			{
				nMemoryFootprint -= nPivotContainerCapacity * sizeof(KeyType);
				nMemoryFootprint += m_vtPivots.capacity() * sizeof(KeyType);
			}

			if (nChildrenContainerCapacity!= m_vtChildren.capacity())
			{
				nMemoryFootprint -= nChildrenContainerCapacity * sizeof(ValueType);
				nMemoryFootprint += m_vtChildren.capacity() * sizeof(ValueType);
			}
		}
		else
		{
			static_assert(
				std::is_trivial<KeyType>::value &&
				std::is_standard_layout<KeyType>::value &&
				std::is_trivial<ValueType>::value &&
				std::is_standard_layout<ValueType>::value,
				"Non-POD type is provided. Kindly provide functionality to calculate size.");
		}
#endif //__TRACK_CACHE_FOOTPRINT__

		return ErrorCode::Success;
	}

#ifdef __TRACK_CACHE_FOOTPRINT__
	inline void moveAnEntityFromLHSSibling(shared_ptr<SelfType> ptrLHSSibling, KeyType& pivotKeyForEntity, KeyType& pivotKeyForParent, int32_t& nMemoryFootprint)
#else //__TRACK_CACHE_FOOTPRINT__
	inline void moveAnEntityFromLHSSibling(shared_ptr<SelfType> ptrLHSSibling, KeyType& pivotKeyForEntity, KeyType& pivotKeyForParent)
#endif //__TRACK_CACHE_FOOTPRINT__
	{
		if (m_ptrRawData != nullptr)
		{
#ifdef __TRACK_CACHE_FOOTPRINT__
			nMemoryFootprint += moveDataToDRAM();
#else //__TRACK_CACHE_FOOTPRINT__
			moveDataToDRAM();
#endif //__TRACK_CACHE_FOOTPRINT__
		}

		if (ptrLHSSibling->m_ptrRawData != nullptr)
		{
#ifdef __TRACK_CACHE_FOOTPRINT__
			nMemoryFootprint += ptrLHSSibling->moveDataToDRAM();
#else //__TRACK_CACHE_FOOTPRINT__
			ptrLHSSibling->moveDataToDRAM();
#endif //__TRACK_CACHE_FOOTPRINT__
		}

#ifdef __TRACK_CACHE_FOOTPRINT__
		uint32_t nPivotContainerCapacity = m_vtPivots.capacity();
		uint32_t nChildrenContainerCapacity = m_vtChildren.capacity();

		uint32_t nLHSPivotContainerCapacity = ptrLHSSibling->m_vtPivots.capacity();
		uint32_t nLHSChildrenContainerCapacity = ptrLHSSibling->m_vtChildren.capacity();
#endif //__TRACK_CACHE_FOOTPRINT__

		KeyType key = ptrLHSSibling->m_vtPivots.back();
		ObjectUIDType value = ptrLHSSibling->m_vtChildren.back();

		ptrLHSSibling->m_vtPivots.pop_back();
		ptrLHSSibling->m_vtChildren.pop_back();

		assert(ptrLHSSibling->m_vtPivots.size() > 0);

		m_vtPivots.insert(m_vtPivots.begin(), pivotKeyForEntity);
		m_vtChildren.insert(m_vtChildren.begin(), value);

		pivotKeyForParent = key;

#ifdef __TRACK_CACHE_FOOTPRINT__
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			if (nPivotContainerCapacity != m_vtPivots.capacity())
			{
				nMemoryFootprint -= nPivotContainerCapacity * sizeof(KeyType);
				nMemoryFootprint += m_vtPivots.capacity() * sizeof(KeyType);
			}

			if (nChildrenContainerCapacity != m_vtChildren.capacity())
			{
				nMemoryFootprint -= nChildrenContainerCapacity * sizeof(ObjectUIDType);
				nMemoryFootprint += m_vtChildren.capacity() * sizeof(ObjectUIDType);
			}

			if (nLHSPivotContainerCapacity != ptrLHSSibling->m_vtPivots.capacity())
			{
				nMemoryFootprint -= nLHSPivotContainerCapacity * sizeof(KeyType);
				nMemoryFootprint += ptrLHSSibling->m_vtPivots.capacity() * sizeof(KeyType);
			}

			if (nLHSChildrenContainerCapacity != ptrLHSSibling->m_vtChildren.capacity())
			{
				nMemoryFootprint -= nLHSChildrenContainerCapacity * sizeof(ObjectUIDType);
				nMemoryFootprint += ptrLHSSibling->m_vtChildren.capacity() * sizeof(ObjectUIDType);
			}
		}
		else
		{
			static_assert(
				std::is_trivial<KeyType>::value &&
				std::is_standard_layout<KeyType>::value &&
				std::is_trivial<ValueType>::value &&
				std::is_standard_layout<ValueType>::value,
				"Non-POD type is provided. Kindly provide functionality to calculate size.");
		}
#endif //__TRACK_CACHE_FOOTPRINT__
	}

#ifdef __TRACK_CACHE_FOOTPRINT__
	inline void moveAnEntityFromRHSSibling(shared_ptr<SelfType> ptrRHSSibling, KeyType& pivotKeyForEntity, KeyType& pivotKeyForParent, int32_t& nMemoryFootprint)
#else //__TRACK_CACHE_FOOTPRINT__
	inline void moveAnEntityFromRHSSibling(shared_ptr<SelfType> ptrRHSSibling, KeyType& pivotKeyForEntity, KeyType& pivotKeyForParent)
#endif //__TRACK_CACHE_FOOTPRINT__
	{
		if (m_ptrRawData != nullptr)
		{
#ifdef __TRACK_CACHE_FOOTPRINT__
			nMemoryFootprint += moveDataToDRAM();
#else //__TRACK_CACHE_FOOTPRINT__
			moveDataToDRAM();
#endif //__TRACK_CACHE_FOOTPRINT__
		}

		if (ptrRHSSibling->m_ptrRawData != nullptr)
		{
#ifdef __TRACK_CACHE_FOOTPRINT__
			nMemoryFootprint += ptrRHSSibling->moveDataToDRAM();
#else //__TRACK_CACHE_FOOTPRINT__
			ptrRHSSibling->moveDataToDRAM();
#endif //__TRACK_CACHE_FOOTPRINT__
		}

#ifdef __TRACK_CACHE_FOOTPRINT__
		uint32_t nPivotContainerCapacity = m_vtPivots.capacity();
		uint32_t nChildrenContainerCapacity = m_vtChildren.capacity();

		uint32_t nRHSPivotContainerCapacity = ptrRHSSibling->m_vtPivots.capacity();
		uint32_t nRHSChildrenContainerCapacity = ptrRHSSibling->m_vtChildren.capacity();
#endif //__TRACK_CACHE_FOOTPRINT__

		KeyType key = ptrRHSSibling->m_vtPivots.front();
		ObjectUIDType value = ptrRHSSibling->m_vtChildren.front();

		ptrRHSSibling->m_vtPivots.erase(ptrRHSSibling->m_vtPivots.begin());
		ptrRHSSibling->m_vtChildren.erase(ptrRHSSibling->m_vtChildren.begin());

		assert(ptrRHSSibling->m_vtPivots.size() > 0);

		m_vtPivots.push_back(pivotKeyForEntity);
		m_vtChildren.push_back(value);

		pivotKeyForParent = key;

#ifdef __TRACK_CACHE_FOOTPRINT__
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			if (nPivotContainerCapacity != m_vtPivots.capacity())
			{
				nMemoryFootprint -= nPivotContainerCapacity * sizeof(KeyType);
				nMemoryFootprint += m_vtPivots.capacity() * sizeof(KeyType);
			}

			if (nChildrenContainerCapacity != m_vtChildren.capacity())
			{
				nMemoryFootprint -= nChildrenContainerCapacity * sizeof(ObjectUIDType);
				nMemoryFootprint += m_vtChildren.capacity() * sizeof(ObjectUIDType);
			}

			if (nRHSPivotContainerCapacity != ptrRHSSibling->m_vtPivots.capacity())
			{
				nMemoryFootprint -= nRHSPivotContainerCapacity * sizeof(KeyType);
				nMemoryFootprint += ptrRHSSibling->m_vtPivots.capacity() * sizeof(KeyType);
			}

			if (nRHSChildrenContainerCapacity != ptrRHSSibling->m_vtChildren.capacity())
			{
				nMemoryFootprint -= nRHSChildrenContainerCapacity * sizeof(ObjectUIDType);
				nMemoryFootprint += ptrRHSSibling->m_vtChildren.capacity() * sizeof(ObjectUIDType);
			}
		}
		else
		{
			static_assert(
				std::is_trivial<KeyType>::value &&
				std::is_standard_layout<KeyType>::value &&
				std::is_trivial<ValueType>::value &&
				std::is_standard_layout<ValueType>::value,
				"Non-POD type is provided. Kindly provide functionality to calculate size.");
		}
#endif //__TRACK_CACHE_FOOTPRINT__
	}

#ifdef __TRACK_CACHE_FOOTPRINT__
	inline void mergeNodes(shared_ptr<SelfType> ptrSibling, KeyType& pivotKey, int32_t& nMemoryFootprint)
#else //__TRACK_CACHE_FOOTPRINT__
	inline void mergeNodes(shared_ptr<SelfType> ptrSibling, KeyType& pivotKey)
#endif //__TRACK_CACHE_FOOTPRINT__
	{
		if (m_ptrRawData != nullptr)
		{
#ifdef __TRACK_CACHE_FOOTPRINT__
			nMemoryFootprint += moveDataToDRAM();
#else //__TRACK_CACHE_FOOTPRINT__
			moveDataToDRAM();
#endif //__TRACK_CACHE_FOOTPRINT__
		}

#ifdef __TRACK_CACHE_FOOTPRINT__
		uint32_t nPivotContainerCapacity = m_vtPivots.capacity();
		uint32_t nChildrenContainerCapacity = m_vtChildren.capacity();
#endif //__TRACK_CACHE_FOOTPRINT__

		if (ptrSibling->m_ptrRawData != nullptr)
		{
			m_vtPivots.push_back(pivotKey);
			m_vtPivots.insert(m_vtPivots.end(), ptrSibling->m_ptrRawData->ptrPivots, ptrSibling->m_ptrRawData->ptrPivots + ptrSibling->m_ptrRawData->nTotalPivots);
			m_vtChildren.insert(m_vtChildren.end(), ptrSibling->m_ptrRawData->ptrChildren, ptrSibling->m_ptrRawData->ptrChildren + ptrSibling->m_ptrRawData->nTotalPivots + 1);
		}
		else
		{
			m_vtPivots.push_back(pivotKey);
			m_vtPivots.insert(m_vtPivots.end(), ptrSibling->m_vtPivots.begin(), ptrSibling->m_vtPivots.end());
			m_vtChildren.insert(m_vtChildren.end(), ptrSibling->m_vtChildren.begin(), ptrSibling->m_vtChildren.end());
		}

#ifdef __TRACK_CACHE_FOOTPRINT__
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			if (nPivotContainerCapacity != m_vtPivots.capacity())
			{
				nMemoryFootprint -= nPivotContainerCapacity * sizeof(KeyType);
				nMemoryFootprint += m_vtPivots.capacity() * sizeof(KeyType);
			}

			if (nChildrenContainerCapacity != m_vtChildren.capacity())
			{
				nMemoryFootprint -= nChildrenContainerCapacity * sizeof(ObjectUIDType);
				nMemoryFootprint += m_vtChildren.capacity() * sizeof(ObjectUIDType);
			}
		}
		else
		{
			static_assert(
				std::is_trivial<KeyType>::value &&
				std::is_standard_layout<KeyType>::value &&
				std::is_trivial<ValueType>::value &&
				std::is_standard_layout<ValueType>::value,
				"Non-POD type is provided. Kindly provide functionality to calculate size.");
		}
#endif //__TRACK_CACHE_FOOTPRINT__
	}

public:
	template <typename CacheType, typename CacheObjectType>
	void print(std::ofstream& os, std::shared_ptr<CacheType>& ptrCache, size_t nLevel, string stPrefix)
	{
		if (m_ptrRawData != nullptr)
		{
			std::cout << "Critical State: " << __LINE__ << std::endl;
			throw new std::logic_error(".....");   // TODO: critical log.
			throw new std::logic_error("fix...");
			return printRONode(os, ptrCache, nLevel, stPrefix);
		}

		uint8_t nSpaceCount = 7;

		stPrefix.append(std::string(nSpaceCount - 1, ' '));
		stPrefix.append("|");
		for (size_t nIndex = 0; nIndex < m_vtChildren.size(); nIndex++)
		{
			os
				<< " "
				<< stPrefix
				<< std::endl;

			os
				<< " "
				<< stPrefix
				<< std::string(nSpaceCount, '-').c_str();

			if (nIndex < m_vtPivots.size())
			{
				os
					<< " < ("
					<< m_vtPivots[nIndex] << ")";
			}
			else
			{
				os
					<< " >= ("
					<< m_vtPivots[nIndex - 1]
					<< ")";
			}

			CacheObjectType ptrNode = nullptr;

#ifdef __TREE_WITH_CACHE__
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;
			ptrCache->getObject(m_vtChildren[nIndex], ptrNode, uidUpdated);

			if (uidUpdated != std::nullopt)
			{
				m_vtChildren[nIndex] = *uidUpdated;
			}
#else //__TREE_WITH_CACHE__
			ptrCache->getObject(m_vtChildren[nIndex], ptrNode);
#endif //__TREE_WITH_CACHE__

			os << std::endl;

			if (std::holds_alternative<shared_ptr<SelfType>>(ptrNode->getInnerData()))
			{
				shared_ptr<SelfType> ptrIndexNode = std::get<shared_ptr<SelfType>>(ptrNode->getInnerData());
				ptrIndexNode->template print<CacheType, CacheObjectType>(os, ptrCache, nLevel + 1, stPrefix);
			}
			else //if (std::holds_alternative<shared_ptr<DataNodeType>>(ptrNode->getInnerData()))
			{
				shared_ptr<DataNodeType> ptrDataNode = std::get<shared_ptr<DataNodeType>>(ptrNode->getInnerData());
				ptrDataNode->print(os, nLevel + 1, stPrefix);
			}
		}
	}

	template <typename CacheType, typename CacheObjectType>
	void printRONode(std::ofstream & os, std::shared_ptr<CacheType>&ptrCache, size_t nLevel, string stPrefix)
	{
		uint8_t nSpaceCount = 7;

		stPrefix.append(std::string(nSpaceCount - 1, ' '));
		stPrefix.append("|");
		for (size_t nIndex = 0; nIndex < m_ptrRawData->nTotalPivots + 1; nIndex++)
		{
			os
				<< " "
				<< stPrefix
				<< std::endl;

			os
				<< " "
				<< stPrefix
				<< std::string(nSpaceCount, '-').c_str();

			if (nIndex < m_ptrRawData->nTotalPivots.size())
			{
				os
					<< " < ("
					<< m_ptrRawData->ptrPivots[nIndex] << ")";
			}
			else
			{
				os
					<< " >= ("
					<< m_ptrRawData->ptrPivots[nIndex - 1]
					<< ")";
			}

			CacheObjectType ptrNode = nullptr;

#ifdef __TREE_WITH_CACHE__
			std::optional<ObjectUIDType> uidUpdated = std::nullopt;
			ptrCache->getObject(m_ptrRawData->ptrChildren[nIndex], ptrNode, uidUpdated);

			if (uidUpdated != std::nullopt)
			{
				//..
				std::cout << "Critical State: " << __LINE__ << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
				m_vtChildren[nIndex] = *uidUpdated;
			}
#else //__TREE_WITH_CACHE__
			ptrCache->getObject(m_vtChildren[nIndex], ptrNode);
#endif //__TREE_WITH_CACHE__

			os << std::endl;

			if (std::holds_alternative<shared_ptr<SelfType>>(ptrNode->getInnerData()))
			{
				shared_ptr<SelfType> ptrIndexNode = std::get<shared_ptr<SelfType>>(ptrNode->getInnerData());
				ptrIndexNode->template print<CacheType, CacheObjectType>(os, ptrCache, nLevel + 1, stPrefix);
			}
			else //if (std::holds_alternative<shared_ptr<DataNodeType>>(ptrNode->getInnerData()))
			{
				shared_ptr<DataNodeType> ptrDataNode = std::get<shared_ptr<DataNodeType>>(ptrNode->getInnerData());
				ptrDataNode->print(os, nLevel + 1, stPrefix);
			}
		}
	}

	void wieHiestDu()
	{
		printf("ich heisse InternalNode.\n");
	}
};
