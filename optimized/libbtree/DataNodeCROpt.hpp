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

#define MILLISEC_CHECK_FOR_FREQUENT_REQUESTS_TO_MEMORY 10
#define ACCESSED_FREQUENCY_AS_PER_THE_TIME_CHECK 10

template <typename Traits>
class DataNodeCROpt 
{
public:
	using KeyType = typename Traits::KeyType;
	using ValueType = typename Traits::ValueType;
	using ObjectUIDType = typename Traits::ObjectUIDType;

public:
	static const uint8_t UID = Traits::DataNodeUID;

private:
	typedef DataNodeCROpt<Traits> SelfType;

	struct Data 
	{
		KeyType   key;
		ValueType value;
	};

	struct RAWDATA
	{
		uint8_t nUID;
		uint16_t nTotalEntries;
		const Data* ptrData;

		uint8_t nCounter;

#ifdef _MSC_VER
		std::chrono::time_point<std::chrono::steady_clock> tLastAccessTime;
#else //_MSC_VER
		std::chrono::time_point<std::chrono::system_clock> tLastAccessTime;
#endif //_MSC_VER

		~RAWDATA()
		{
			ptrData = nullptr;
		}

		RAWDATA(const char* szData, uint32_t nDataLen, uint16_t nBlockSize)
		{
			nUID = szData[0];
			nTotalEntries = *reinterpret_cast<const uint16_t*>(&szData[1]);

			size_t nHeaderSize = sizeof(uint8_t) + sizeof(uint16_t);  // 3 bytes
			size_t nDataOffset = (nHeaderSize + alignof(Data) - 1) & ~(alignof(Data) - 1);

			ptrData = reinterpret_cast<const Data*>(szData + nDataOffset);

			nCounter = 0;
			tLastAccessTime = std::chrono::high_resolution_clock::now();
		}
	};
	//END_PACKED_STRUCT

	typedef std::vector<Data>::const_iterator EntryIterator;

	uint16_t m_nDegree;
	std::vector<Data> m_vtEntries;

public:
	RAWDATA* m_ptrRawData = nullptr;

public:
	~DataNodeCROpt()
	{
		m_vtEntries.clear();

		if (m_ptrRawData != nullptr)
		{
			delete m_ptrRawData;
			m_ptrRawData = nullptr;
		}
	}

	DataNodeCROpt(uint16_t nDegree)
		: m_nDegree(nDegree)
		, m_ptrRawData(nullptr)
	{
	}

	DataNodeCROpt(uint16_t nDegree, const char* szData, uint32_t nDataLen, uint16_t nBlockSize)
		: m_nDegree(nDegree)
		, m_ptrRawData(nullptr)
	{
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			ASSERT(UID == static_cast<uint8_t>(szData[0]));

			m_ptrRawData = new RAWDATA(szData, nDataLen, nBlockSize);

			ASSERT(UID == m_ptrRawData->nUID);

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

	DataNodeCROpt(uint16_t nDegree, EntryIterator itBegin, EntryIterator itEnd)
		: m_nDegree(nDegree)
		, m_ptrRawData(nullptr)
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
	inline void moveDataToDRAM()
	{
		ASSERT(m_ptrRawData != nullptr);

#ifdef __PROD__
		fix this
#else //__PROD__
		auto nRequiredCapacity = std::max<size_t>(
			static_cast<size_t>(2 * m_nDegree + 1),
			static_cast<size_t>(m_ptrRawData->nTotalEntries));

		m_vtEntries.reserve(nRequiredCapacity);
#endif //__PROD__

		m_vtEntries.resize(m_ptrRawData->nTotalEntries);

		for (size_t nIdx = 0; nIdx < m_ptrRawData->nTotalEntries; ++nIdx) {
			m_vtEntries[nIdx].key = m_ptrRawData->ptrData[nIdx].key;
			m_vtEntries[nIdx].value = m_ptrRawData->ptrData[nIdx].value;
		}

#ifdef __ENABLE_ASSERTS__
		ASSERT(m_ptrRawData->nUID == UID);
		ASSERT(m_ptrRawData->nTotalEntries == m_vtEntries.size());
		for (int idx = 0, end = m_ptrRawData->nTotalEntries; idx < end; idx++)
		{
			ASSERT(reinterpret_cast<const Data*>(m_ptrRawData->ptrData + idx)->key == m_vtEntries[idx].key);
			ASSERT(reinterpret_cast<const Data*>(m_ptrRawData->ptrData + idx)->value == m_vtEntries[idx].value);
		}
#endif //__ENABLE_ASSERTS__

		delete m_ptrRawData;
		m_ptrRawData = nullptr;
	}

	inline void serialize(char*& szData, uint32_t& nDataLen, uint16_t nBlockSize, void*& ptrBlockAppendOffset, bool& bAlignedAllocation) const
	{
		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			bAlignedAllocation = true;

			uint16_t nTotalEntries = m_vtEntries.size();

			nDataLen = sizeof(uint8_t)					// UID
				+ sizeof(uint16_t)						// Total entries
				+ (nTotalEntries * sizeof(Data))		// Size of all entries
				+ 1;


			size_t nHeaderSize = sizeof(uint8_t) + sizeof(uint16_t);  // 3 bytes
			size_t nDataOffset = (nHeaderSize + alignof(Data) - 1) & ~(alignof(Data) - 1);
			nDataLen = nDataOffset + nTotalEntries * sizeof(Data);

#ifdef _MSC_VER
			szData = (char*)_aligned_malloc(nDataLen, alignof(Data));
#else // _MSC_VER
			/*char**/ szData = (char*)std::aligned_alloc(alignof(Data), nDataLen);
#endif //_MSC_VER

			memset(szData, 0, nDataLen);

			uint16_t nOffset = 0;
			memcpy(szData, &UID, sizeof(uint8_t));
			nOffset += sizeof(uint8_t);

			memcpy(szData + nOffset, &nTotalEntries, sizeof(uint16_t));
			nOffset += sizeof(uint16_t);

			auto ptrData = reinterpret_cast<const Data*>(szData + nDataOffset);
			memcpy((void*)ptrData, m_vtEntries.data(), nTotalEntries * sizeof(Data));

#ifdef __ENABLE_ASSERTS__
			SelfType oClone(m_nDegree, szData, nDataLen, nBlockSize);
			oClone.moveDataToDRAM();
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


	inline bool canAccessDataDirectly()
	{
		if (m_ptrRawData == nullptr)
			return false;

		auto now = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - m_ptrRawData->tLastAccessTime).count();

		m_ptrRawData->tLastAccessTime = now;

		if (duration < MILLISEC_CHECK_FOR_FREQUENT_REQUESTS_TO_MEMORY)
		{
			m_ptrRawData->nCounter++;

			if (m_ptrRawData->nCounter >= ACCESSED_FREQUENCY_AS_PER_THE_TIME_CHECK)
			{
				moveDataToDRAM();
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

	inline bool requireSplit() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalEntries > (2 * m_nDegree - 1);
		}

		return m_vtEntries.size() > (2 * m_nDegree - 1);
#endif //__PROD__
	}

	inline bool requireMerge() const
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalEntries < (m_nDegree - 1);
		}

		return m_vtEntries.size() < (m_nDegree - 1);
#endif //__PROD__
	}

	inline size_t getKeysCount() const
	{
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalEntries;
		}

		return m_vtEntries.size();
	}

	inline ErrorCode getValue(const KeyType& key, ValueType& value)
	{
		if (canAccessDataDirectly())
		{
			const Data* it = &(*std::lower_bound(m_ptrRawData->ptrData, m_ptrRawData->ptrData + m_ptrRawData->nTotalEntries, key,
				[](auto const& data, auto const& key) { return data.key < key; }));

			if (it != m_ptrRawData->ptrData + m_ptrRawData->nTotalEntries && (*it).key == key)
			{
				value = (*it).value;
				return ErrorCode::Success;
			}
			return ErrorCode::KeyDoesNotExist;
		}

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
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

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
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		EntryIterator it = std::lower_bound(m_vtEntries.begin(), m_vtEntries.end(), key, 
			[](auto const& data, auto const& key) { return data.key < key; });

		if (it != m_vtEntries.end() && (*it).key == key)
		{
			// lay dal ayte.. make sure ayte ke duplicate omanish?
			return ErrorCode::KeyAlreadyExists;
		}

		m_vtEntries.insert(it, { key, value });

		return ErrorCode::Success;
	}

	template <typename CacheType, typename CacheObjectTypePtr>
	inline ErrorCode split(std::shared_ptr<CacheType>& ptrCache, std::optional<ObjectUIDType>& uidSibling, CacheObjectTypePtr& ptrSibling, KeyType& pivotKeyForParent)
	{
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		size_t nMid = m_vtEntries.size() / 2;

#ifdef __PROD__
		fix this
#else //__PROD__
		ptrCache->template createObjectOfType<SelfType>(uidSibling, ptrSibling, m_nDegree,
			m_vtEntries.begin() + nMid, m_vtEntries.end());
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
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		if (ptrLHSSibling->m_ptrRawData != nullptr)
		{
			ptrLHSSibling->moveDataToDRAM();
		}

		Data entry = ptrLHSSibling->m_vtEntries.back();

		ptrLHSSibling->m_vtEntries.pop_back();

		ASSERT(ptrLHSSibling->m_vtEntries.size() > 0);

		m_vtEntries.insert(m_vtEntries.begin(), entry);

		pivotKeyForParent = entry.key;
	}

	inline void mergeNode(SelfType* ptrSibling)
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
			std::size_t nRequiredCapacity = m_vtEntries.size() + ptrSibling->m_ptrRawData->nTotalEntries;
			if (m_vtEntries.capacity() < nRequiredCapacity)
			{
				m_vtEntries.reserve(nRequiredCapacity);
		}
#endif //__PROD__

			m_vtEntries.insert(m_vtEntries.end(), ptrSibling->m_ptrRawData->ptrData, ptrSibling->m_ptrRawData->ptrData + ptrSibling->m_ptrRawData->nTotalEntries);
		}
		else
		{
#ifdef __PROD__
			fix this
#else //__PROD__
			std::size_t nRequiredCapacity = m_vtEntries.size() + m_vtEntries.size();
			if (m_vtEntries.capacity() < nRequiredCapacity)
			{
				m_vtEntries.reserve(nRequiredCapacity);
			}
#endif //__PROD__

			m_vtEntries.insert(m_vtEntries.end(), ptrSibling->m_vtEntries.begin(), ptrSibling->m_vtEntries.end());
		}
	}

	inline void moveAnEntityFromRHSSibling(SelfType* ptrRHSSibling, KeyType& pivotKeyForParent)
	{
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		if (ptrRHSSibling->m_ptrRawData != nullptr)
		{
			ptrRHSSibling->moveDataToDRAM();
		}

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