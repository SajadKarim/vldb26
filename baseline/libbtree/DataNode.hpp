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

template <typename KeyType, typename ValueType, typename ObjectUIDType, uint8_t TYPE_UID>
class DataNode
{
public:
	// Unique identifier for the type of this node
	static const uint8_t UID = TYPE_UID;

private:
	typedef DataNode<KeyType, ValueType, ObjectUIDType, TYPE_UID> SelfType;

	// Aliases for iterators over key and value vectors
	typedef std::vector<KeyType>::const_iterator KeyTypeIterator;
	typedef std::vector<ValueType>::const_iterator ValueTypeIterator;

	// Vectors to hold keys and corresponding values
	std::vector<KeyType> m_vtKeys;
	std::vector<ValueType> m_vtValues;

public:
	// Destructor: Clears the keys and values vectors
	~DataNode()
	{
		m_vtKeys.clear();
		m_vtValues.clear();
	}

	// Default constructor
	DataNode()
	{
	}

	// Copy constructor: Copies keys and values from another DataNode instance
	DataNode(const DataNode& source)
	{
		m_vtKeys.assign(source.m_vtKeys.begin(), source.m_vtKeys.end());
		m_vtValues.assign(source.m_vtValues.begin(), source.m_vtValues.end());
	}

	// Constructor that deserializes data from a char array (for POD types)
	DataNode(const char* szData)
	{
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			uint16_t nTotalEntries = 0;

			uint32_t nOffset = sizeof(uint8_t);

			memcpy(&nTotalEntries, szData + nOffset, sizeof(uint16_t));
			nOffset += sizeof(uint16_t);

			m_vtKeys.resize(nTotalEntries);
			m_vtValues.resize(nTotalEntries);

			uint32_t nKeysSize = nTotalEntries * sizeof(KeyType);
			memcpy(m_vtKeys.data(), szData + nOffset, nKeysSize);

			nOffset += nKeysSize;

			uint32_t nValuesSize = nTotalEntries * sizeof(ValueType);
			memcpy(m_vtValues.data(), szData + nOffset, nValuesSize);
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
	DataNode(std::fstream& fs)
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

	// Constructor that initializes DataNode with iterators over keys and values
	DataNode(KeyTypeIterator itBeginKeys, KeyTypeIterator itEndKeys, ValueTypeIterator itBeginValues, ValueTypeIterator itEndValues)
	{
		m_vtKeys.resize(std::distance(itBeginKeys, itEndKeys));
		m_vtValues.resize(std::distance(itBeginValues, itEndValues));

		std::move(itBeginKeys, itEndKeys, m_vtKeys.begin());
		std::move(itBeginValues, itEndValues, m_vtValues.begin());
		//m_vtKeys.assign(itBeginKeys, itEndKeys);
		//m_vtValues.assign(itBeginValues, itEndValues);
	}

public:
	// Serializes the node's data into a char buffer
	inline void serialize(char*& szBuffer, uint8_t& uidObjectType, uint32_t& nBufferSize) const
	{
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			uidObjectType = UID;

			uint16_t nTotalEntries = m_vtKeys.size();

			nBufferSize = sizeof(uint8_t)				// UID
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
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			uidObjectType = SelfType::UID;

			uint16_t nTotalEntries = m_vtKeys.size();

			nDataSize = sizeof(uint8_t)					// UID
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
	// Determines if the node requires a split based on the given degree
	inline bool requireSplit(size_t nDegree) const
	{
		return m_vtKeys.size() > (2 * nDegree - 1);
	}

	// Determines if the node requires merging based on the given degree
	inline bool requireMerge(size_t nDegree) const
	{
		return m_vtKeys.size() < (nDegree - 1);
	}

	// Retrieves the first key from the node
	inline const KeyType& getFirstChild() const
	{
		return m_vtKeys[0];
	}

	// Returns the number of keys in the node
	inline size_t getKeysCount() const
	{
		return m_vtKeys.size();
	}

	// Retrieves the value for a given key
	inline ErrorCode getValue(const KeyType& key, ValueType& value) const
	{
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
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			return
				sizeof(*this)
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
#ifdef __TRACK_CACHE_FOOTPRINT__
		uint32_t nKeyContainerCapacity = m_vtKeys.capacity();
		uint32_t nValueContainerCapacity = m_vtValues.capacity();
#endif //__TRACK_CACHE_FOOTPRINT__

		m_vtKeys.insert(m_vtKeys.end(), ptrSibling->m_vtKeys.begin(), ptrSibling->m_vtKeys.end());
		m_vtValues.insert(m_vtValues.end(), ptrSibling->m_vtValues.begin(), ptrSibling->m_vtValues.end());

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

	void wieHiestDu()
	{
		printf("ich heisse DataNode :).\n");
	}
};