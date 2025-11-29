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
class DataNodeCOpt 
{
public:
	using KeyType = typename Traits::KeyType;
	using ValueType = typename Traits::ValueType;
	using ObjectUIDType = typename Traits::ObjectUIDType;

public:
	static const uint8_t UID = Traits::DataNodeUID;

private:
	typedef DataNodeCOpt<Traits> SelfType;

	struct Data 
	{
		KeyType   key;
		ValueType value;
	};

	typedef std::vector<Data>::const_iterator EntryIterator;

	uint16_t m_nDegree;
	std::vector<Data> m_vtEntries;

public:
	~DataNodeCOpt()
	{
		m_vtEntries.clear();
	}

	DataNodeCOpt(uint16_t nDegree)
		: m_nDegree(nDegree)
	{
	}

	DataNodeCOpt(uint16_t nDegree, const char* szData, uint32_t nDataLen, uint16_t nBlockSize)
		: m_nDegree(nDegree)
	{
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			ASSERT(UID == static_cast<uint8_t>(szData[0]));

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

			m_vtEntries.reserve(nRequiredCapacity);
#endif //__PROD__

			m_vtEntries.resize(nTotalEntries);

			memcpy(m_vtEntries.data(), szData + nOffset, nTotalEntries * sizeof(Data));
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

	DataNodeCOpt(uint16_t nDegree, EntryIterator itBegin, EntryIterator itEnd)
		: m_nDegree(nDegree)
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		size_t nRequiredCapacity = std::max<size_t>(
			static_cast<size_t>(2 * m_nDegree + 1),
			static_cast<size_t>(std::distance(itBegin, itEnd)));

		m_vtEntries.reserve(nRequiredCapacity);
#endif //__PROD__

		m_vtEntries.assign(itBegin, itEnd);
	}

public:
	inline void serialize(char*& szData, uint32_t& nDataLen, uint16_t nBlockSize, void*& ptrBlockAppendOffset, bool& bAlignedAllocation) const
	{
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			uint16_t nTotalEntries = m_vtEntries.size();

			nDataLen = sizeof(uint8_t)							// UID
				+ sizeof(uint16_t)								// Total entries
				+ (nTotalEntries * sizeof(Data))				// Size of all entries
				+ 1;

			szData = new char[nDataLen];
			memset(szData, 0, nDataLen);

			uint16_t nOffset = 0;
			memcpy(szData, &UID, sizeof(uint8_t));
			nOffset += sizeof(uint8_t);

			memcpy(szData + nOffset, &nTotalEntries, sizeof(uint16_t));
			nOffset += sizeof(uint16_t);

			memcpy(szData + nOffset, m_vtEntries.data(), nTotalEntries * sizeof(Data));

#ifdef __ENABLE_ASSERTS__
			DataNodeCOpt oClone(m_nDegree, szData, nDataLen, nBlockSize);
			for (int idx = 0; idx < m_vtEntries.size(); idx++)
			{
				ASSERT(m_vtEntries[idx].key == oClone.m_vtEntries[idx].key);
				ASSERT(m_vtEntries[idx].value == oClone.m_vtEntries[idx].value);
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
	inline bool hasUIDUpdates()
	{
		return false;
	}


	inline bool requireSplit() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		return m_vtEntries.size() > (2 * m_nDegree - 1);
#endif //__PROD__
	}

	inline bool requireMerge() const
	{
#ifdef __PROD__
		fix thiss
#else //__PROD__
		return m_vtEntries.size() < (m_nDegree - 1);
#endif //__PROD__
	}

	inline size_t getKeysCount() const
	{
		return m_vtEntries.size();
	}

	inline ErrorCode getValue(const KeyType& key, ValueType& value) const
	{
		EntryIterator it = std::lower_bound(m_vtEntries.begin(), m_vtEntries.end(), key, 
			[](auto const& data, auto const& key) { return data.key < key; });

		if (it != m_vtEntries.end() && it->key == key)
		{
			value = it->value;
			return ErrorCode::Success;
		}

		return ErrorCode::KeyDoesNotExist;
	}

public:
	inline ErrorCode remove(const KeyType& key)
	{
		EntryIterator it = std::lower_bound(m_vtEntries.begin(), m_vtEntries.end(), key, 
			[](auto const& data, auto const& key) { return data.key < key; });

		if (it != m_vtEntries.end() && it->key == key)
		{
			m_vtEntries.erase(it);
			return ErrorCode::Success;
		}

		return ErrorCode::KeyDoesNotExist;
	}

	inline ErrorCode insert(const KeyType& key, const ValueType& value)
	{
		EntryIterator it = std::lower_bound(m_vtEntries.begin(), m_vtEntries.end(), key, 
			[](auto const& data, auto const& key) { return data.key < key; });

		if (it != m_vtEntries.end() && (*it).key == key)
			return ErrorCode::KeyAlreadyExists;

		m_vtEntries.insert(it, { key, value });

		return ErrorCode::Success;
	}

	template <typename CacheType, typename CacheObjectTypePtr>
	inline ErrorCode split(std::shared_ptr<CacheType>& ptrCache, std::optional<ObjectUIDType>& uidSibling, CacheObjectTypePtr& ptrSibling, KeyType& pivotKeyForParent)
	{
		size_t nMid = m_vtEntries.size() / 2;

#ifdef __PROD__
		fix this
#else //__PROD__
		ptrCache->template createObjectOfType<SelfType>(uidSibling, ptrSibling, m_nDegree, m_vtEntries.begin() + nMid, m_vtEntries.end());
#endif //__PROD__

		if (!uidSibling)
		{
			return ErrorCode::Error;
		}

		pivotKeyForParent = m_vtEntries[nMid].key;

		m_vtEntries.resize(nMid);

		return ErrorCode::Success;
	}

	inline void moveAnEntityFromLHSSibling(SelfType* ptrLHSSibling, KeyType& pivotKeyForParent)
	{
		Data entry = ptrLHSSibling->m_vtEntries.back();

		ptrLHSSibling->m_vtEntries.pop_back();

		ASSERT(ptrLHSSibling->m_vtEntries.size() > 0);

		m_vtEntries.insert(m_vtEntries.begin(), entry);

		pivotKeyForParent = entry.key;
	}

	inline void mergeNode(SelfType* ptrSibling)
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		std::size_t nRequiredCapacity = m_vtEntries.size() + ptrSibling->m_vtEntries.size();
		if (m_vtEntries.capacity() < nRequiredCapacity)
		{
			m_vtEntries.reserve(nRequiredCapacity);
		}
#endif //__PROD__

		m_vtEntries.insert(m_vtEntries.end(), ptrSibling->m_vtEntries.begin(), ptrSibling->m_vtEntries.end());
	}

	inline void moveAnEntityFromRHSSibling(SelfType* ptrRHSSibling, KeyType& pivotKeyForParent)
	{
		Data entry = ptrRHSSibling->m_vtEntries.front();

		ptrRHSSibling->m_vtEntries.erase(ptrRHSSibling->m_vtEntries.begin());

		ASSERT(ptrRHSSibling->m_vtEntries.size() > 0);

		m_vtEntries.push_back(entry);

		pivotKeyForParent = ptrRHSSibling->m_vtEntries.front().key;
	}

public:
	void print(std::ofstream& os, size_t nLevel, std::string stPrefix)
	{
		uint8_t nSpaceCount = 7;
		
		stPrefix.append(std::string(nSpaceCount - 1, ' '));
		stPrefix.append("|");

		for (size_t nIndex = 0; nIndex < m_vtEntries.size(); nIndex++)
		{
			os  << " "
				<< stPrefix
				<< std::string(nSpaceCount, '-').c_str()
				<< "(K: "
				<< m_vtEntries[nIndex].key
				<< ", V: "
				<< m_vtEntries[nIndex].value
				<< ")"
				<< std::endl;
		}
	}

	void wieHiestDu()
	{
		printf("ich heisse DataNode :).\n");
	}
};