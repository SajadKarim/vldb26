#pragma once
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <syncstream>
#include <thread>
#include <variant>
#include <typeinfo>

#include "ErrorCodes.h"

template <typename... ValueCoreTypes>
class NoCacheObject
{
private:
	typedef std::variant<std::shared_ptr<ValueCoreTypes>...> ValueCoreTypesWrapper;

public:
	typedef std::tuple<ValueCoreTypes...> ValueCoreTypesTuple;

private:
	ValueCoreTypesWrapper m_objData;
	mutable std::shared_mutex m_mtx;

public:
	~NoCacheObject()
	{
	}

	template<class CoreObjectType>
	NoCacheObject(std::shared_ptr<CoreObjectType> ptrCoreObject)
	{
		m_objData = ptrCoreObject;
	}

	inline const ValueCoreTypesWrapper& getInnerData() const
	{
		return m_objData;
	}

	inline std::shared_mutex& getMutex()
	{
		return m_mtx;
	}
};