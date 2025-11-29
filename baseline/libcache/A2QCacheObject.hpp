#pragma once
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <syncstream>
#include <thread>
#include <variant>
#include <typeinfo>
#include <iostream>
#include <fstream>
#include "CacheErrorCodes.h"


template <typename CoreTypesMarshaller, typename... ValueCoreTypes>
class A2QCacheObject
{
private:
	typedef std::variant<std::shared_ptr<ValueCoreTypes>...> ValueCoreTypesWrapper;

public:
	typedef std::tuple<ValueCoreTypes...> ValueCoreTypesTuple;

private:
	bool m_bDirty;
	ValueCoreTypesWrapper m_objData;
	std::shared_mutex m_mtx;

public:
	~A2QCacheObject()
	{
		resetVaraint(m_objData);
	}

	template<class ValueCoreType>
	A2QCacheObject(std::shared_ptr<ValueCoreType> ptrCoreObject)
		: m_bDirty(true)
	{
		m_objData = ptrCoreObject;
	}

	A2QCacheObject(std::fstream& fs)
		: m_bDirty(false)
	{
		CoreTypesMarshaller::template deserialize<ValueCoreTypesWrapper, ValueCoreTypes...>(fs, m_objData);
	}

	A2QCacheObject(const char* szBuffer)
		: m_bDirty(false)
	{
		CoreTypesMarshaller::template deserialize<ValueCoreTypesWrapper, ValueCoreTypes...>(szBuffer, m_objData);
	}

	inline void serialize(std::fstream& fs, uint8_t& uidObject, uint32_t& nBufferSize)
	{
		CoreTypesMarshaller::template serialize<ValueCoreTypes...>(fs, m_objData, uidObject, nBufferSize);
	}

	inline void serialize(char*& szBuffer, uint8_t& uidObject, uint32_t& nBufferSize)
	{
		CoreTypesMarshaller::template serialize<ValueCoreTypes...>(szBuffer, m_objData, uidObject, nBufferSize);
	}

	inline bool getDirtyFlag() const 
	{
		return m_bDirty;
	}

	inline void setDirtyFlag(bool bDirty)
	{
		m_bDirty = bDirty;
	}

	inline const ValueCoreTypesWrapper& getInnerData() const
	{
		return m_objData;
	}

	inline std::shared_mutex& getMutex()
	{
		return m_mtx;
	}

	inline bool tryLockObject()
	{
		return m_mtx.try_lock();
	}

	inline void unlockObject()
	{
		m_mtx.unlock();
	}

	inline size_t getMemoryFootprint()
	{
		return sizeof(*this) + getVariantMemoryFootprint(m_objData);
	}

	inline bool isIndexNode()
	{
		return sizeof(*this) + doesVariantContainIndex(m_objData);
	}
};