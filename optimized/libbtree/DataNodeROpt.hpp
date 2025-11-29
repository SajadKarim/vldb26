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
#include <chrono>
#include <atomic>
#include <cstring>
#include "validityasserts.h"

#define MILLISEC_CHECK_FOR_FREQUENT_REQUESTS_TO_MEMORY 100
#define ACCESSED_FREQUENCY_AS_PER_THE_TIME_CHECK 5

template <typename Traits>
class DataNodeROpt
{
public:
	using KeyType = typename Traits::KeyType;
	using ValueType = typename Traits::ValueType;
	using ObjectUIDType = typename Traits::ObjectUIDType;

public:
	static const uint8_t UID = Traits::DataNodeUID;

private:
	typedef DataNodeROpt<Traits> SelfType;

private:
	struct RAWDATA
	{
		uint8_t nUID;
		uint16_t nTotalEntries;
		const KeyType* ptrKeys;
		const ValueType* ptrValues;

		uint8_t nCounter;

#ifdef _MSC_VER
		std::chrono::time_point<std::chrono::steady_clock> tLastAccessTime;
#else //_MSC_VER
		std::chrono::time_point<std::chrono::system_clock> tLastAccessTime;
#endif //_MSC_VER

		~RAWDATA()
		{
			ptrKeys = nullptr;
			ptrValues = nullptr;
		}

		RAWDATA(const char* szData, uint32_t nDataLen, uint16_t nBlockSize)
		{
			nUID = szData[0];
			nTotalEntries = *reinterpret_cast<const uint16_t*>(&szData[1]);

			size_t nKeysSize = nTotalEntries * sizeof(KeyType);
			size_t nKeysOffset = (sizeof(uint8_t) + sizeof(uint16_t) + alignof(KeyType) - 1) & ~(alignof(KeyType) - 1);

			size_t nValuesOffset = (nKeysOffset + nKeysSize + alignof(ValueType) - 1) & ~(alignof(ValueType) - 1);

			ptrKeys = reinterpret_cast<const KeyType*>(szData + nKeysOffset);
			ptrValues = reinterpret_cast<const ValueType*>(szData + nValuesOffset);

			nCounter = 0;
			tLastAccessTime = std::chrono::high_resolution_clock::now();
		}
	};

	typedef std::vector<KeyType>::const_iterator KeyTypeIterator;
	typedef std::vector<ValueType>::const_iterator ValueTypeIterator;

	uint16_t m_nDegree;
	std::vector<KeyType> m_vtKeys;
	std::vector<ValueType> m_vtValues;

public:
	RAWDATA* m_ptrRawData = nullptr;

public:
	~DataNodeROpt()
	{
		m_vtKeys.clear();
		m_vtValues.clear();

		if (m_ptrRawData != nullptr)
		{
			delete m_ptrRawData;
			m_ptrRawData = nullptr;
		}
	}

	DataNodeROpt(uint16_t nDegree)
		: m_nDegree(nDegree)
		, m_ptrRawData(nullptr)
	{
	}

	DataNodeROpt(uint16_t nDegree, const char* szData, uint32_t nDataLen, uint16_t nBlockSize)
		: m_nDegree(nDegree)
		, m_ptrRawData(nullptr)
	{
		m_vtKeys.reserve(0);
		m_vtValues.reserve(0);

		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
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
				std::is_trivial<ValueType>::value &&
				std::is_standard_layout<ValueType>::value,
				"Non-POD type is provided. Kindly implement custome de/serializer.");
		}
	}

	DataNodeROpt(uint16_t nDegree, KeyTypeIterator itBeginKeys, KeyTypeIterator itEndKeys, ValueTypeIterator itBeginValues, ValueTypeIterator itEndValues)
		: m_nDegree(nDegree)
		, m_ptrRawData(nullptr)
	{
#ifdef __PROD__
		fix this
#else //__PROD__
		size_t nRequiredCapacity = std::max<size_t>(
			static_cast<size_t>(2 * m_nDegree + 1),
			static_cast<size_t>(std::distance(itBeginKeys, itEndKeys)));
#endif //__PROD__

		m_vtKeys.reserve(nRequiredCapacity);
		m_vtValues.reserve(nRequiredCapacity);

		m_vtKeys.assign(itBeginKeys, itEndKeys);
		m_vtValues.assign(itBeginValues, itEndValues);
	}

public:
	inline bool hasUIDUpdates()
	{
		return false;
	}

	inline void moveDataToDRAM()
	{
		ASSERT(m_ptrRawData != nullptr);

#ifdef __PROD__
		fix this
#else //__PROD__
		auto nRequiredCapacity = std::max<size_t>(
			static_cast<size_t>(2 * m_nDegree + 1),
			static_cast<size_t>(m_ptrRawData->nTotalEntries));

		m_vtKeys.reserve(nRequiredCapacity);
		m_vtValues.reserve(nRequiredCapacity);
#endif //__PROD__

		m_vtKeys.resize(m_ptrRawData->nTotalEntries);
		m_vtValues.resize(m_ptrRawData->nTotalEntries);

		memcpy(m_vtKeys.data(), m_ptrRawData->ptrKeys, m_ptrRawData->nTotalEntries * sizeof(KeyType));
		memcpy(m_vtValues.data(), m_ptrRawData->ptrValues, m_ptrRawData->nTotalEntries * sizeof(ValueType));

#ifdef __ENABLE_ASSERTS__
		ASSERT(m_ptrRawData->nUID == UID);
		ASSERT(m_ptrRawData->nTotalEntries == m_vtKeys.size());
		for (int idx = 0, end = m_ptrRawData->nTotalEntries; idx < end; idx++)
		{
			ASSERT(*(m_ptrRawData->ptrKeys + idx) == m_vtKeys[idx]);
			ASSERT(*(m_ptrRawData->ptrValues + idx) == m_vtValues[idx]);
		}
#endif //__ENABLE_ASSERTS__

		delete m_ptrRawData;
		m_ptrRawData = nullptr;
	}

	inline void serialize(char*& szData, uint32_t& nDataLen, uint16_t nBlockSize, void*& ptrBlockAppendOffset, bool& bAlignedAllocation) const
	{
		ASSERT(m_ptrRawData == nullptr);

		if constexpr (std::is_trivial<KeyType>::value &&
			std::is_standard_layout<KeyType>::value &&
			std::is_trivial<ValueType>::value &&
			std::is_standard_layout<ValueType>::value)
		{
			bAlignedAllocation = true;

			uint16_t nTotalEntries = m_vtKeys.size();
			size_t nKeysSize = nTotalEntries * sizeof(KeyType);

			size_t nHeaderSize = sizeof(uint8_t) + sizeof(uint16_t);
			size_t nKeysOffset = (nHeaderSize + alignof(KeyType) - 1) & ~(alignof(KeyType) - 1);
			size_t nValuesOffset = (nKeysOffset + nKeysSize + alignof(ValueType) - 1) & ~(alignof(ValueType) - 1);

			nDataLen = nValuesOffset + nTotalEntries * sizeof(ValueType);

#ifdef _MSC_VER
			szData = (char*)_aligned_malloc(nDataLen, std::max(alignof(KeyType), alignof(ValueType)));
#else
			szData = (char*)std::aligned_alloc(std::max(alignof(KeyType), alignof(ValueType)), nDataLen);
#endif

			memset(szData, 0, nDataLen);

			memcpy(szData, &UID, sizeof(uint8_t));
			memcpy(szData + sizeof(uint8_t), &nTotalEntries, sizeof(uint16_t));
			memcpy(szData + nKeysOffset, m_vtKeys.data(), nKeysSize);
			memcpy(szData + nValuesOffset, m_vtValues.data(), nTotalEntries * sizeof(ValueType));

#ifdef __ENABLE_ASSERTS__
			SelfType oClone(m_nDegree, szData, nDataLen, nBlockSize);
			oClone.moveDataToDRAM();
			for (int idx = 0, end = oClone.m_vtKeys.size(); idx < end; idx++)
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
				ASSERT(m_ptrRawData == nullptr);
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

		return m_vtKeys.size() > (2 * m_nDegree - 1);
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

		return m_vtKeys.size() < (m_nDegree - 1);
#endif //__PROD__
	}

	inline size_t getKeysCount() const
	{
		if (m_ptrRawData != nullptr)
		{
			return m_ptrRawData->nTotalEntries;
		}

		return m_vtKeys.size();
	}

	inline ErrorCode getValue(const KeyType& key, ValueType& value)
	{
		if (canAccessDataDirectly())
		{
			const KeyType* it = &(*std::lower_bound(m_ptrRawData->ptrKeys, m_ptrRawData->ptrKeys + m_ptrRawData->nTotalEntries, key));
			if (it != m_ptrRawData->ptrKeys + m_ptrRawData->nTotalEntries && *it == key)
			{
				value = m_ptrRawData->ptrValues[it - m_ptrRawData->ptrKeys];
				return ErrorCode::Success;
			}
			return ErrorCode::KeyDoesNotExist;
		}

		KeyTypeIterator it = std::lower_bound(m_vtKeys.begin(), m_vtKeys.end(), key);
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
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		KeyTypeIterator it = std::lower_bound(m_vtKeys.begin(), m_vtKeys.end(), key);

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
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

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
	inline ErrorCode split(std::shared_ptr<CacheType>& ptrCache, std::optional<ObjectUIDType>& uidSibling, CacheObjectTypePtr& ptrSibling, KeyType& pivotKeyForParent)
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
			m_vtKeys.begin() + nMid, m_vtKeys.end(), m_vtValues.begin() + nMid, m_vtValues.end());
#endif //__PROD__

		if (!uidSibling)
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
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		if (ptrLHSSibling->m_ptrRawData != nullptr)
		{
			ptrLHSSibling->moveDataToDRAM();
		}

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
		if (m_ptrRawData != nullptr)
		{
			moveDataToDRAM();
		}

		if (ptrSibling->m_ptrRawData != nullptr)
		{
#ifdef __PROD__
			fix this
#else //__PROD__
			std::size_t nRequiredCapacity = m_vtKeys.size() + ptrSibling->m_ptrRawData->nTotalEntries;
			if (m_vtKeys.capacity() < nRequiredCapacity)
			{
				m_vtKeys.reserve(nRequiredCapacity);
				m_vtValues.reserve(nRequiredCapacity);
		}
#endif //__PROD__

			m_vtKeys.insert(m_vtKeys.end(), ptrSibling->m_ptrRawData->ptrKeys, ptrSibling->m_ptrRawData->ptrKeys + ptrSibling->m_ptrRawData->nTotalEntries);
			m_vtValues.insert(m_vtValues.end(), ptrSibling->m_ptrRawData->ptrValues, ptrSibling->m_ptrRawData->ptrValues + ptrSibling->m_ptrRawData->nTotalEntries);
		}
		else
		{
#ifdef __PROD__
			fix this
#else //__PROD__
			std::size_t nRequiredCapacity = m_vtKeys.size() + ptrSibling->m_vtKeys.size();
			if (m_vtKeys.capacity() < nRequiredCapacity)
			{
				m_vtKeys.reserve(nRequiredCapacity);
				m_vtValues.reserve(nRequiredCapacity);
			}
#endif //__PROD__

			m_vtKeys.insert(m_vtKeys.end(), ptrSibling->m_vtKeys.begin(), ptrSibling->m_vtKeys.end());
			m_vtValues.insert(m_vtValues.end(), ptrSibling->m_vtValues.begin(), ptrSibling->m_vtValues.end());
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
		printf("ich heisse DataNode :).\n");
	}
};