#pragma once
#include <memory>
#include <vector>
#include <string>
#include <map>
#include <cmath>
#include <optional>
#include <iostream>
#include <fstream>
#include "ErrorCodes.h"
#include "validityasserts.h"

template <typename Traits>
class DataNode 
{
private:
	typedef DataNode<Traits> SelfType;

public:
	static const uint8_t UID = Traits::DataNodeUID;

	using KeyType = typename Traits::KeyType;
	using ValueType = typename Traits::ValueType;
	using ObjectUIDType = typename Traits::ObjectUIDType;

	typedef std::vector<KeyType>::const_iterator KeyIterator;
	typedef std::vector<ValueType>::const_iterator ValueIterator;

	uint16_t m_nDegree;
	std::vector<KeyType> m_vtKeys;
	std::vector<ValueType> m_vtValues;

public:
	~DataNode()
	{
		m_vtKeys.clear();
		m_vtValues.clear();
	}

	DataNode(uint16_t nDegree)
		: m_nDegree(nDegree)
	{
		m_vtKeys.reserve(static_cast<size_t>(2 * m_nDegree + 1));
		m_vtValues.reserve(static_cast<size_t>(2 * m_nDegree + 1));
	}

	DataNode(uint16_t nDegree, const char* szData, uint32_t nDataLen, uint16_t nBlockSize)
		: m_nDegree(nDegree)
	{
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			ASSERT(UID == szData[0]);

			uint16_t nTotalEntries = 0;

			uint32_t nOffset = sizeof(uint8_t);

			memcpy(&nTotalEntries, szData + nOffset, sizeof(uint16_t));
			nOffset += sizeof(uint16_t);

#ifdef __PROD__
			fix this
#else //__PROD__
			size_t nRequiredCapacity = std::max<size_t>(
				static_cast<size_t>(2 * m_nDegree + 1),
				static_cast<size_t>(nTotalEntries));

			m_vtKeys.reserve(nRequiredCapacity);
			m_vtValues.reserve(nRequiredCapacity);
#endif //__PROD__

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

	DataNode(uint16_t nDegree, KeyIterator itBeginKeys, KeyIterator itEndKeys, ValueIterator itBeginValues, ValueIterator itEndValues)
		: m_nDegree(nDegree)
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		size_t nRequiredCapacity = std::max<size_t>(
			static_cast<size_t>(2 * m_nDegree + 1),
			static_cast<size_t>(std::distance(itBeginKeys, itEndKeys)));

		m_vtKeys.reserve(nRequiredCapacity);
		m_vtValues.reserve(nRequiredCapacity);
#endif //__PROD__

		m_vtKeys.assign(itBeginKeys, itEndKeys);
		m_vtValues.assign(itBeginValues, itEndValues);
	}

public:
	inline void serialize(char*& szData, uint32_t& nDataLen, uint16_t nBlockSize, void*& ptrBlockAppendOffset, bool& bAlignedAllocation) const
	{
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			uint16_t nTotalEntries = m_vtKeys.size();

			nDataLen = sizeof(uint8_t)					// UID
				+ sizeof(uint16_t)						// Total entries
				+ (nTotalEntries * sizeof(KeyType))		// Size of all keys
				+ (nTotalEntries * sizeof(ValueType))	// Size of all values
				+ 1;

			szData = new char[nDataLen];
			memset(szData, 0, nDataLen);

			uint16_t nOffset = 0;
			memcpy(szData, &UID, sizeof(uint8_t));
			nOffset += sizeof(uint8_t);

			memcpy(szData + nOffset, &nTotalEntries, sizeof(uint16_t));
			nOffset += sizeof(uint16_t);

			uint16_t nKeysSize = nTotalEntries * sizeof(KeyType);
			memcpy(szData + nOffset, m_vtKeys.data(), nKeysSize);
			nOffset += nKeysSize;

			uint16_t nValuesSize = nTotalEntries * sizeof(ValueType);
			memcpy(szData + nOffset, m_vtValues.data(), nValuesSize);
			nOffset += nValuesSize;

#ifdef __ENABLE_ASSERTS__
			SelfType oClone(m_nDegree, szData, nDataLen, nBlockSize);
			for (int idx = 0; idx < m_vtKeys.size(); idx++)
			{
				ASSERT(m_vtKeys[idx] == oClone.m_vtKeys[idx]);
				ASSERT(m_vtValues[idx] == oClone.m_vtValues[idx]);
			}
#endif //__ENABLE_ASSERTS__
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
	inline bool hasUIDUpdates() const
	{
		return false;
	}

	// TODO: root node might have fewer nodes than degree -1 so check if you are adressing it, if not then check if it really impacting the performance?

	inline bool requireSplit() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		return m_vtKeys.size() > (2 * m_nDegree - 1);
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

	inline const KeyType& getFirstChild() const
	{
		return m_vtKeys[0];
	}

	inline size_t getKeysCount() const
	{
		return m_vtKeys.size();
	}

	inline ErrorCode getValue(const KeyType& key, ValueType& value) const
	{
		KeyIterator it = std::lower_bound(m_vtKeys.begin(), m_vtKeys.end(), key);
		if (it != m_vtKeys.end() && *it == key)
		{
			value = m_vtValues[it - m_vtKeys.begin()];

			return ErrorCode::Success;
		}

		return ErrorCode::KeyDoesNotExist;
	}

public:
	inline ErrorCode remove(const KeyType& key)
	{
		KeyIterator it = std::lower_bound(m_vtKeys.begin(), m_vtKeys.end(), key);
		if (it != m_vtKeys.end() && *it == key)
		{
			size_t nIdx = static_cast<size_t>(it - m_vtKeys.begin());
			m_vtKeys.erase(it);
			m_vtValues.erase(m_vtValues.begin() + nIdx);

			return ErrorCode::Success;
		}

		return ErrorCode::KeyDoesNotExist;
	}

	inline ErrorCode insert(const KeyType& key, const ValueType& value)
	{
		auto it = std::lower_bound(m_vtKeys.begin(), m_vtKeys.end(), key);
		if (it != m_vtKeys.end() && *it == key)
		{
			// lay dal ayte.. make sure ayte ke duplicate omanish?
			return ErrorCode::KeyAlreadyExists;
		}

		size_t nIdx = static_cast<size_t>(std::distance(m_vtKeys.begin(), it));

		m_vtKeys.insert(it, key);
		m_vtValues.insert(m_vtValues.begin() + nIdx, value);

		return ErrorCode::Success;
	}

	template <typename CacheType, typename CacheObjectTypePtr>
	inline ErrorCode split(std::shared_ptr<CacheType>& ptrCache, ObjectUIDType& uidSibling, CacheObjectTypePtr& ptrSibling, KeyType& pivotKeyForParent)
	{
		size_t nMid = m_vtKeys.size() / 2;

#ifdef __PROD__
		fix this
#else //__PROD__
		ptrCache->template createObjectOfType<SelfType>(uidSibling, ptrSibling, m_nDegree,
			m_vtKeys.begin() + nMid, m_vtKeys.end(), m_vtValues.begin() + nMid, m_vtValues.end());
#endif //__PROD__

		if (ptrSibling == nullptr)
		{
			return ErrorCode::Error;
		}

		pivotKeyForParent = m_vtKeys[nMid];

		m_vtKeys.resize(nMid);
		m_vtValues.resize(nMid);

		return ErrorCode::Success;
	}

	inline void moveAnEntityFromLHSSibling(SelfType* ptrLHSSibling, KeyType& pivotKeyForParent)
	{
		KeyType key = ptrLHSSibling->m_vtKeys.back();
		ValueType value = ptrLHSSibling->m_vtValues.back();

		ptrLHSSibling->m_vtKeys.pop_back();
		ptrLHSSibling->m_vtValues.pop_back();

		ASSERT(ptrLHSSibling->m_vtKeys.size() > 0);

		m_vtKeys.insert(m_vtKeys.begin(), key);
		m_vtValues.insert(m_vtValues.begin(), value);

		pivotKeyForParent = key;
	}

	inline void mergeNode(SelfType* ptrSibling)
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		std::size_t nRequiredCapacity = m_vtKeys.size() + ptrSibling->m_vtKeys.size();
		if (m_vtKeys.capacity() < nRequiredCapacity)
		{
			m_vtKeys.reserve(nRequiredCapacity);
			m_vtValues.reserve(nRequiredCapacity);	// because keys and values have the same size.
		}
#endif //__PROD__

		m_vtKeys.insert(m_vtKeys.end(), ptrSibling->m_vtKeys.begin(), ptrSibling->m_vtKeys.end());
		m_vtValues.insert(m_vtValues.end(), ptrSibling->m_vtValues.begin(), ptrSibling->m_vtValues.end());
	}

	inline void moveAnEntityFromRHSSibling(SelfType* ptrRHSSibling, KeyType& pivotKeyForParent)
	{
		KeyType key = ptrRHSSibling->m_vtKeys.front();
		ValueType value = ptrRHSSibling->m_vtValues.front();

		ptrRHSSibling->m_vtKeys.erase(ptrRHSSibling->m_vtKeys.begin());
		ptrRHSSibling->m_vtValues.erase(ptrRHSSibling->m_vtValues.begin());

		ASSERT(ptrRHSSibling->m_vtKeys.size() > 0);

		m_vtKeys.push_back(key);
		m_vtValues.push_back(value);

		pivotKeyForParent = ptrRHSSibling->m_vtKeys.front();
	}

public:
	void print(std::ofstream& os, size_t nLevel, std::string stPrefix)
	{
		uint8_t nSpaceCount = 7;

		stPrefix.append(std::string(nSpaceCount - 1, ' '));
		stPrefix.append("|");

		for (size_t nIndex = 0; nIndex < m_vtKeys.size(); nIndex++)
		{
			if constexpr (std::is_arithmetic_v<KeyType>) {
				os << " "
					<< stPrefix
					<< std::string(nSpaceCount, '-').c_str()
					<< "(K: "
					<< m_vtKeys[nIndex]
					<< ", V: "
					<< m_vtValues[nIndex]
					<< ")"
					<< std::endl;
			}
			else
			{
				os << " " << stPrefix << std::string(nSpaceCount, '-').c_str() << "not printable";
			}
		}
	}

	void wieHiestDu()
	{
		printf("ich heisse DataNode :).\n");
	}
};