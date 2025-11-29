#pragma once
#include <memory>
#include <vector>
#include <string>
#include <map>
#include <cmath>
#include <optional>
#include <iostream>
#include <fstream>
#include <assert.h>
#include "ErrorCodes.h"
#include <chrono>
#include <atomic>

#ifdef _MSC_VER
#define PACKED_STRUCT __pragma(pack(push, 1))
#define END_PACKED_STRUCT __pragma(pack(pop))
#else
#define PACKED_STRUCT _Pragma("pack(push, 1)")
#define END_PACKED_STRUCT _Pragma("pack(pop)")
#endif

#define MICROSEC_CHECK_FOR_FREQUENT_REQUESTS_TO_MEMORY 100
#define ACCESSED_FREQUENCY_AS_PER_THE_TIME_CHECK 10

template <typename KeyType, typename ValueType, typename ObjectUIDType, uint8_t TYPE_UID>
class DataNodeROpt
{
private:

PACKED_STRUCT
	struct RAWDATA
	{
		uint8_t nUID;
		uint16_t nTotalEntries;
		const KeyType* ptrKeys;
		const ValueType* ptrValues;

		uint8_t nCounter;

#ifdef _MSC_VER
		std::chrono::time_point<std::chrono::steady_clock> tLastAccessTime;
#else
		std::chrono::time_point<std::chrono::system_clock> tLastAccessTime;
#endif

		~RAWDATA()
		{
			ptrKeys = nullptr;
			ptrValues = nullptr;
		}

		RAWDATA(const char* szData)
		{
			nUID = szData[0];
			nTotalEntries = *reinterpret_cast<const uint16_t*>(&szData[1]);
			ptrKeys = reinterpret_cast<const KeyType*>(szData + sizeof(uint8_t) + sizeof(uint16_t));
			ptrValues = reinterpret_cast<const ValueType*>(szData + sizeof(uint8_t) + sizeof(uint16_t) + (nTotalEntries * sizeof(KeyType)));

			nCounter = 0;
			tLastAccessTime = std::chrono::high_resolution_clock::now();
		}
	};
END_PACKED_STRUCT

public:
	// Unique identifier for the type of this node
	static const uint8_t UID = TYPE_UID;

private:
	typedef DataNodeROpt<KeyType, ValueType, ObjectUIDType, TYPE_UID> SelfType;

	// Aliases for iterators over key and value vectors
	typedef std::vector<KeyType>::const_iterator KeyTypeIterator;
	typedef std::vector<ValueType>::const_iterator ValueTypeIterator;

	// Vectors to hold keys and corresponding values
	std::vector<KeyType> m_vtKeys;
	std::vector<ValueType> m_vtValues;

public:
	RAWDATA* m_ptrRawData = nullptr;
public:
	// Destructor: Clears the keys and values vectors
	~DataNodeROpt()
	{
		m_vtKeys.clear();
		m_vtValues.clear();

		if (m_ptrRawData != nullptr)
		{
			delete m_ptrRawData;
		}
	}

	// Default constructor
	DataNodeROpt()
		: m_ptrRawData(nullptr)
	{
	}
	
	// Copy constructor: Copies keys and values from another DataNodeROpt instance
	DataNodeROpt(const DataNodeROpt& source)
		: m_ptrRawData(nullptr)
	{
		m_vtKeys.assign(source.m_vtKeys.begin(), source.m_vtKeys.end());
		m_vtValues.assign(source.m_vtValues.begin(), source.m_vtValues.end());
	}

	// Constructor that deserializes data from a char array (for POD types)
	DataNodeROpt(const char* szData)
		: m_ptrRawData(nullptr)
	{
		m_vtKeys.reserve(0);
		m_vtValues.reserve(0);

		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			m_ptrRawData = new RAWDATA(szData);

			assert( UID == m_ptrRawData->nUID);

			//moveDataToDRAM();
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

	// Constructor that deserializes data from a file stream (for POD types)
	DataNodeROpt(std::fstream& fs)
		: m_ptrRawData(nullptr)
	{
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			uint16_t nTotalEntries = 0;

			fs.read(reinterpret_cast<char*>(&nTotalEntries), sizeof(uint16_t));

			m_vtKeys.resize(nTotalEntries);
			m_vtValues.resize(nTotalEntries);

			fs.read(reinterpret_cast<char*>(m_vtKeys.data()), nTotalEntries * sizeof(KeyType));
			fs.read(reinterpret_cast<char*>(m_vtValues.data()), nTotalEntries * sizeof(ValueType));
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
	
	// Constructor that initializes DataNodeROpt with iterators over keys and values
	DataNodeROpt(KeyTypeIterator itBeginKeys, KeyTypeIterator itEndKeys, ValueTypeIterator itBeginValues, ValueTypeIterator itEndValues)
		: m_ptrRawData(nullptr)
	{
		m_vtKeys.resize(std::distance(itBeginKeys, itEndKeys));
		m_vtValues.resize(std::distance(itBeginValues, itEndValues));

		std::move(itBeginKeys, itEndKeys, m_vtKeys.begin());
		std::move(itBeginValues, itEndValues, m_vtValues.begin());
		//m_vtKeys.assign(itBeginKeys, itEndKeys);
		//m_vtValues.assign(itBeginValues, itEndValues);
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

		m_vtKeys.resize(m_ptrRawData->nTotalEntries);
		m_vtValues.resize(m_ptrRawData->nTotalEntries);

		memcpy(m_vtKeys.data(), m_ptrRawData->ptrKeys, m_ptrRawData->nTotalEntries * sizeof(KeyType));
		memcpy(m_vtValues.data(), m_ptrRawData->ptrValues, m_ptrRawData->nTotalEntries * sizeof(ValueType));

#ifdef __VALIDITY_CHECK__
		assert(m_ptrRawData->nUID == UID);
		assert(m_ptrRawData->nTotalEntries == m_vtKeys.size());
		for (int idx = 0, end = m_ptrRawData->nTotalEntries; idx < end; idx++)
		{
			assert(*(m_ptrRawData->ptrKeys + idx) == m_vtKeys[idx]);
			assert(*(m_ptrRawData->ptrValues + idx) == m_vtValues[idx]);
		}
#endif //__VALIDITY_CHECK__

		delete m_ptrRawData;
		m_ptrRawData = nullptr;

#ifdef __TRACK_CACHE_FOOTPRINT__
		nMemoryFootprint += getMemoryFootprint();
		return nMemoryFootprint;
#endif //__TRACK_CACHE_FOOTPRINT__
	}

	// Serializes the node's data into a char buffer
	inline void serialize(char*& szBuffer, uint8_t& uidObjectType, uint32_t& nBufferSize) const
	{
		assert(m_ptrRawData == nullptr);

		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			uidObjectType = UID;

			uint16_t nTotalEntries = m_vtKeys.size();

			nBufferSize 
				= sizeof(uint8_t)						// UID
				+ sizeof(uint16_t)						// Total entries
				+ (nTotalEntries * sizeof(KeyType))		// Size of all keys
				+ (nTotalEntries * sizeof(ValueType));	// Size of all values

			szBuffer = new char[nBufferSize + 1];
			memset(szBuffer, 0, nBufferSize + 1);

			uint16_t nOffset = 0;
			memcpy(szBuffer, &UID, sizeof(uint8_t));
			nOffset += sizeof(uint8_t);

			memcpy(szBuffer + nOffset, &nTotalEntries, sizeof(uint16_t));
			nOffset += sizeof(uint16_t);

			uint16_t nKeysSize = nTotalEntries * sizeof(KeyType);
			memcpy(szBuffer + nOffset, m_vtKeys.data(), nKeysSize);
			nOffset += nKeysSize;

			uint16_t nValuesSize = nTotalEntries * sizeof(ValueType);
			memcpy(szBuffer + nOffset, m_vtValues.data(), nValuesSize);
			nOffset += nValuesSize;

#ifdef __VALIDITY_CHECK__
			const RAWDATA* ptrRawData = new RAWDATA(szBuffer);

			assert( ptrRawData->nUID == UID);
			assert( ptrRawData->nTotalEntries == m_vtKeys.size());
			for (int idx = 0, end = ptrRawData->nTotalEntries; idx < end; idx++)
			{
				assert(*(ptrRawData->ptrKeys + idx) == m_vtKeys[idx]);
				assert(*(ptrRawData->ptrValues + idx) == m_vtValues[idx]);
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

	// Writes the node's data to a file stream
	inline void writeToStream(std::fstream& os, uint8_t& uidObjectType, uint32_t& nDataSize) const
	{
		assert(m_ptrRawData == nullptr);

		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			uidObjectType = SelfType::UID;

			uint16_t nTotalEntries = m_vtKeys.size();

			nDataSize 
				= sizeof(uint8_t)						// UID
				+ sizeof(uint16_t)						// Total entries
				+ (nTotalEntries * sizeof(KeyType))		// Size of all keys
				+ (nTotalEntries * sizeof(ValueType));	// Size of all values

			os.write(reinterpret_cast<const char*>(&uidObjectType), sizeof(uint8_t));
			os.write(reinterpret_cast<const char*>(&nTotalEntries), sizeof(uint16_t));
			os.write(reinterpret_cast<const char*>(m_vtKeys.data()), nTotalEntries * sizeof(KeyType));
			os.write(reinterpret_cast<const char*>(m_vtValues.data()), nTotalEntries * sizeof(ValueType));
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

public:
	
	inline bool canAccessDataDirectly()
	{
		if (m_ptrRawData == nullptr)
			return false;

		auto now = std::chrono::high_resolution_clock::now();
		auto duration = now - m_ptrRawData->tLastAccessTime;

		if(std::chrono::duration_cast<std::chrono::microseconds>(duration).count() < MICROSEC_CHECK_FOR_FREQUENT_REQUESTS_TO_MEMORY)
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

	// Determines if the node requires a split based on the given degree
	inline bool requireSplit(size_t nDegree) const
	{
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalEntries > nDegree;
		}

		return m_vtKeys.size() > nDegree;
	}

	// Determines if the node requires merging based on the given degree
	inline bool requireMerge(size_t nDegree) const
	{
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalEntries <= std::ceil(nDegree / 2.0f);
		}

		return m_vtKeys.size() <= std::ceil(nDegree / 2.0f);
	}

	// Retrieves the first key from the node
	inline const KeyType& getFirstChild() const
	{
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->ptrKeys[0];
		}

		return m_vtKeys[0];
	}

	// Returns the number of keys in the node
	inline size_t getKeysCount() const
	{
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalEntries;
		}

		return m_vtKeys.size();
	}

	// Retrieves the value for a given key
	inline ErrorCode getValue(const KeyType& key, ValueType& value)
	{
		//if (m_ptrRawData != nullptr)
		if( canAccessDataDirectly())
		{
			const KeyType* it = &(*std::lower_bound(m_ptrRawData->ptrKeys, m_ptrRawData->ptrKeys + m_ptrRawData->nTotalEntries, key));
			if (it != m_ptrRawData->ptrKeys + m_ptrRawData->nTotalEntries && *it == key) {
				value = m_ptrRawData->ptrValues[it - m_ptrRawData->ptrKeys];
				return ErrorCode::Success;
			}
			return ErrorCode::KeyDoesNotExist;
		}

		KeyTypeIterator it = std::lower_bound(m_vtKeys.begin(), m_vtKeys.end(), key);
		if (it != m_vtKeys.end() && *it == key)
		{
			size_t index = it - m_vtKeys.begin();
			value = m_vtValues[index];

			return ErrorCode::Success;
		}

		return ErrorCode::KeyDoesNotExist;
	}

	// Returns the size of the serialized node
	inline size_t getSize() const
	{
		assert(m_ptrRawData == nullptr);

		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			return sizeof(uint8_t)
				+ sizeof(uint16_t)
				+ (m_vtKeys.size() * sizeof(KeyType))
				+ (m_vtValues.size() * sizeof(ValueType));
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

	inline size_t getMemoryFootprint() const
	{
		if (m_ptrRawData != nullptr)
		{
			return sizeof(*this)
				+ sizeof(RAWDATA);
		}

		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			return sizeof(*this) 
				+ (m_vtKeys.capacity() * sizeof(KeyType)) 
				+ (m_vtValues.capacity() * sizeof(ValueType));
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
	// Removes a key-value pair from the node
#ifdef __TRACK_CACHE_FOOTPRINT__
	inline ErrorCode remove(const KeyType& key, int32_t& nMemoryFootprint)
#else //__TRACK_CACHE_FOOTPRINT__
	inline ErrorCode remove(const KeyType& key)
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

		KeyTypeIterator it = std::lower_bound(m_vtKeys.begin(), m_vtKeys.end(), key);

		if (it != m_vtKeys.end() && *it == key)
		{
#ifdef __TRACK_CACHE_FOOTPRINT__
			uint32_t nKeyContainerCapacity = m_vtKeys.capacity();
			uint32_t nValueContainerCapacity = m_vtValues.capacity();
#endif //__TRACK_CACHE_FOOTPRINT__

			int index = it - m_vtKeys.begin();
			m_vtKeys.erase(it);
			m_vtValues.erase(m_vtValues.begin() + index);

#ifdef __TRACK_CACHE_FOOTPRINT__
			if constexpr (std::is_trivial<KeyType>::value &&
				std::is_standard_layout<KeyType>::value &&
				std::is_trivial<ValueType>::value &&
				std::is_standard_layout<ValueType>::value)
			{
				if (nKeyContainerCapacity != m_vtKeys.capacity())
				{
					nMemoryFootprint -= nKeyContainerCapacity * sizeof(KeyType);
					nMemoryFootprint += m_vtKeys.capacity() * sizeof(KeyType);
				}

				if (nValueContainerCapacity != m_vtValues.capacity())
				{
					nMemoryFootprint -= nValueContainerCapacity * sizeof(ValueType);
					nMemoryFootprint += m_vtValues.capacity() * sizeof(ValueType);
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

		return ErrorCode::KeyDoesNotExist;
	}

#ifdef __TRACK_CACHE_FOOTPRINT__
	inline ErrorCode insert(const KeyType& key, const ValueType& value, int32_t& nMemoryFootprint)
#else //__TRACK_CACHE_FOOTPRINT__
	inline ErrorCode insert(const KeyType& key, const ValueType& value)
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
		uint32_t nKeyContainerCapacity = m_vtKeys.capacity();
		uint32_t nValueContainerCapacity = m_vtValues.capacity();
#endif //__TRACK_CACHE_FOOTPRINT__

		size_t nChildIdx = m_vtKeys.size();
		for (int nIdx = 0; nIdx < m_vtKeys.size(); ++nIdx)
		{
			if (key < m_vtKeys[nIdx])
			{
				nChildIdx = nIdx;
				break;
			}
		}

		m_vtKeys.insert(m_vtKeys.begin() + nChildIdx, key);
		m_vtValues.insert(m_vtValues.begin() + nChildIdx, value);

#ifdef __TRACK_CACHE_FOOTPRINT__
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			if (nKeyContainerCapacity != m_vtKeys.capacity())
			{
				nMemoryFootprint -= nKeyContainerCapacity * sizeof(KeyType);
				nMemoryFootprint += m_vtKeys.capacity() * sizeof(KeyType);
			}

			if (nValueContainerCapacity != m_vtValues.capacity())
			{
				nMemoryFootprint -= nValueContainerCapacity * sizeof(ValueType);
				nMemoryFootprint += m_vtValues.capacity() * sizeof(ValueType);
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

	// Splits the node into two nodes and returns the pivot key for the parent node
	template <typename CacheType, typename CacheObjectTypePtr>
#ifdef __TRACK_CACHE_FOOTPRINT__
	inline ErrorCode split(std::shared_ptr<CacheType>& ptrCache, std::optional<ObjectUIDType>& uidSibling, CacheObjectTypePtr& ptrSibling, KeyType& pivotKeyForParent, int32_t& nMemoryFootprint)
#else //__TRACK_CACHE_FOOTPRINT__
	inline ErrorCode split(std::shared_ptr<CacheType>& ptrCache, std::optional<ObjectUIDType>& uidSibling, CacheObjectTypePtr& ptrSibling, KeyType& pivotKeyForParent)
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
		uint32_t nKeyContainerCapacity = m_vtKeys.capacity();
		uint32_t nValueContainerCapacity = m_vtValues.capacity();
#endif //__TRACK_CACHE_FOOTPRINT__

		size_t nMid = m_vtKeys.size() / 2;

		ptrCache->template createObjectOfType<SelfType>(uidSibling, ptrSibling,
			m_vtKeys.begin() + nMid, m_vtKeys.end(),
			m_vtValues.begin() + nMid, m_vtValues.end());

		if (!uidSibling)
		{
			return ErrorCode::Error;
		}

		pivotKeyForParent = m_vtKeys[nMid];

		m_vtKeys.resize(nMid);
		m_vtValues.resize(nMid);
		//m_vtKeys.erase(m_vtKeys.begin() + nMid, m_vtKeys.end());
		//m_vtValues.erase(m_vtValues.begin() + nMid, m_vtValues.end());

#ifdef __TRACK_CACHE_FOOTPRINT__
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			if (nKeyContainerCapacity != m_vtKeys.capacity())
			{
				nMemoryFootprint -= nKeyContainerCapacity * sizeof(KeyType);
				nMemoryFootprint += m_vtKeys.capacity() * sizeof(KeyType);
			}

			if (nValueContainerCapacity != m_vtValues.capacity())
			{
				nMemoryFootprint -= nValueContainerCapacity * sizeof(ValueType);
				nMemoryFootprint += m_vtValues.capacity() * sizeof(ValueType);
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

	// Moves an entity from the left-hand sibling to the current node
#ifdef __TRACK_CACHE_FOOTPRINT__
	inline void moveAnEntityFromLHSSibling(std::shared_ptr<SelfType> ptrLHSSibling, KeyType& pivotKeyForParent, int32_t& nMemoryFootprint)
#else //__TRACK_CACHE_FOOTPRINT__
	inline void moveAnEntityFromLHSSibling(std::shared_ptr<SelfType> ptrLHSSibling, KeyType& pivotKeyForParent)
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
		uint32_t nKeyContainerCapacity = m_vtKeys.capacity();
		uint32_t nValueContainerCapacity = m_vtValues.capacity();

		uint32_t nLHSKeyContainerCapacity = ptrLHSSibling->m_vtKeys.capacity();
		uint32_t nLHSValueContainerCapacity = ptrLHSSibling->m_vtValues.capacity();
#endif //__TRACK_CACHE_FOOTPRINT__

		KeyType key = ptrLHSSibling->m_vtKeys.back();
		ValueType value = ptrLHSSibling->m_vtValues.back();

		ptrLHSSibling->m_vtKeys.pop_back();
		ptrLHSSibling->m_vtValues.pop_back();

		assert(ptrLHSSibling->m_vtKeys.size() > 0);

		m_vtKeys.insert(m_vtKeys.begin(), key);
		m_vtValues.insert(m_vtValues.begin(), value);

		pivotKeyForParent = key;

#ifdef __TRACK_CACHE_FOOTPRINT__
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			if (nKeyContainerCapacity != m_vtKeys.capacity())
			{
				nMemoryFootprint -= nKeyContainerCapacity * sizeof(KeyType);
				nMemoryFootprint += m_vtKeys.capacity() * sizeof(KeyType);
			}

			if (nValueContainerCapacity != m_vtValues.capacity())
			{
				nMemoryFootprint -= nValueContainerCapacity * sizeof(ValueType);
				nMemoryFootprint += m_vtValues.capacity() * sizeof(ValueType);
			}

			if (nLHSKeyContainerCapacity != ptrLHSSibling->m_vtKeys.capacity())
			{
				nMemoryFootprint -= nLHSKeyContainerCapacity * sizeof(KeyType);
				nMemoryFootprint += ptrLHSSibling->m_vtKeys.capacity() * sizeof(KeyType);
			}

			if (nLHSValueContainerCapacity != ptrLHSSibling->m_vtValues.capacity())
			{
				nMemoryFootprint -= nLHSValueContainerCapacity * sizeof(ValueType);
				nMemoryFootprint += ptrLHSSibling->m_vtValues.capacity() * sizeof(ValueType);
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

	// Merges the sibling node with the current node
#ifdef __TRACK_CACHE_FOOTPRINT__
	inline void mergeNode(std::shared_ptr<SelfType> ptrSibling, int32_t& nMemoryFootprint)
#else //__TRACK_CACHE_FOOTPRINT__
	inline void mergeNode(std::shared_ptr<SelfType> ptrSibling)
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
		uint32_t nKeyContainerCapacity = m_vtKeys.capacity();
		uint32_t nValueContainerCapacity = m_vtValues.capacity();
#endif //__TRACK_CACHE_FOOTPRINT__

		if (ptrSibling->m_ptrRawData != nullptr)
		{
			m_vtKeys.insert(m_vtKeys.end(), ptrSibling->m_ptrRawData->ptrKeys, ptrSibling->m_ptrRawData->ptrKeys + ptrSibling->m_ptrRawData->nTotalEntries);
			m_vtValues.insert(m_vtValues.end(), ptrSibling->m_ptrRawData->ptrValues, ptrSibling->m_ptrRawData->ptrValues + ptrSibling->m_ptrRawData->nTotalEntries);
		}
		else
		{
			m_vtKeys.insert(m_vtKeys.end(), ptrSibling->m_vtKeys.begin(), ptrSibling->m_vtKeys.end());
			m_vtValues.insert(m_vtValues.end(), ptrSibling->m_vtValues.begin(), ptrSibling->m_vtValues.end());
		}

#ifdef __TRACK_CACHE_FOOTPRINT__
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			if (nKeyContainerCapacity != m_vtKeys.capacity())
			{
				nMemoryFootprint -= nKeyContainerCapacity * sizeof(KeyType);
				nMemoryFootprint += m_vtKeys.capacity() * sizeof(KeyType);
			}

			if (nValueContainerCapacity != m_vtValues.capacity())
			{
				nMemoryFootprint -= nValueContainerCapacity * sizeof(ValueType);
				nMemoryFootprint += m_vtValues.capacity() * sizeof(ValueType);
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

	// Moves an entity from the right-hand sibling to the current node
#ifdef __TRACK_CACHE_FOOTPRINT__
	inline void moveAnEntityFromRHSSibling(std::shared_ptr<SelfType> ptrRHSSibling, KeyType& pivotKeyForParent, int32_t& nMemoryFootprint)
#else //__TRACK_CACHE_FOOTPRINT__
	inline void moveAnEntityFromRHSSibling(std::shared_ptr<SelfType> ptrRHSSibling, KeyType& pivotKeyForParent)
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
		uint32_t nKeyContainerCapacity = m_vtKeys.capacity();
		uint32_t nValueContainerCapacity = m_vtValues.capacity();

		uint32_t nRHSKeyContainerCapacity = ptrRHSSibling->m_vtKeys.capacity();
		uint32_t nRHSValueContainerCapacity = ptrRHSSibling->m_vtValues.capacity();
#endif //__TRACK_CACHE_FOOTPRINT__

		KeyType key = ptrRHSSibling->m_vtKeys.front();
		ValueType value = ptrRHSSibling->m_vtValues.front();

		ptrRHSSibling->m_vtKeys.erase(ptrRHSSibling->m_vtKeys.begin());
		ptrRHSSibling->m_vtValues.erase(ptrRHSSibling->m_vtValues.begin());

		assert(ptrRHSSibling->m_vtKeys.size() > 0);

		m_vtKeys.push_back(key);
		m_vtValues.push_back(value);

		pivotKeyForParent = ptrRHSSibling->m_vtKeys.front();

#ifdef __TRACK_CACHE_FOOTPRINT__
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			if (nKeyContainerCapacity != m_vtKeys.capacity())
			{
				nMemoryFootprint -= nKeyContainerCapacity * sizeof(KeyType);
				nMemoryFootprint += m_vtKeys.capacity() * sizeof(KeyType);
			}

			if (nValueContainerCapacity != m_vtValues.capacity())
			{
				nMemoryFootprint -= nValueContainerCapacity * sizeof(ValueType);
				nMemoryFootprint += m_vtValues.capacity() * sizeof(ValueType);
			}

			if (nRHSKeyContainerCapacity != ptrRHSSibling->m_vtKeys.capacity())
			{
				nMemoryFootprint -= nRHSKeyContainerCapacity * sizeof(KeyType);
				nMemoryFootprint += ptrRHSSibling->m_vtKeys.capacity() * sizeof(KeyType);
			}

			if (nRHSValueContainerCapacity != ptrRHSSibling->m_vtValues.capacity())
			{
				nMemoryFootprint -= nRHSValueContainerCapacity * sizeof(ValueType);
				nMemoryFootprint += ptrRHSSibling->m_vtValues.capacity() * sizeof(ValueType);
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
	// Prints the node's keys and values to an output file stream
	void print(std::ofstream& os, size_t nLevel, std::string stPrefix)
	{
		if (m_ptrRawData != nullptr)
		{
			return printRONode(os, nLevel, stPrefix);
		}

		uint8_t nSpaceCount = 7;

		stPrefix.append(std::string(nSpaceCount - 1, ' '));
		stPrefix.append("|");

		for (size_t nIndex = 0; nIndex < m_vtKeys.size(); nIndex++)
		{
			os
				<< " "
				<< stPrefix
				<< std::string(nSpaceCount, '-').c_str()
				<< "(K: "
				<< m_vtKeys[nIndex]
				<< ", V: "
				<< m_vtValues[nIndex]
				<< ")"
				<< std::endl;
		}
	}

	void printRONode(std::ofstream& os, size_t nLevel, std::string stPrefix)
	{
		uint8_t nSpaceCount = 7;

		stPrefix.append(std::string(nSpaceCount - 1, ' '));
		stPrefix.append("|");

		for (size_t nIndex = 0; nIndex < m_ptrRawData->nTotalEntries; nIndex++)
		{
			os
				<< " "
				<< stPrefix
				<< std::string(nSpaceCount, '-').c_str()
				<< "(K: "
				<< m_ptrRawData->ptrKeys[nIndex]
				<< ", V: "
				<< m_ptrRawData->ptrValues[nIndex]
				<< ")"
				<< std::endl;
		}
	}

	void wieHiestDu()
	{
		printf("ich heisse DataNodeROpt :).\n");
	}
};
