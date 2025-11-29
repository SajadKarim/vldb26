#pragma once
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <syncstream>
#include <thread>
#include <variant>
#include <typeinfo>

#include "CacheErrorCodes.h"
#include "IFlushCallback.h"

template <typename Traits>
class NoCache
{
	typedef NoCache<Traits> SelfType;

public:
	using ObjectType = typename Traits::ObjectType;
	using ObjectTypePtr = ObjectType*;
	using ObjectUIDType = typename Traits::ObjectUIDType;
	

public:
	~NoCache()
	{
	}

	template <typename... InitArgs>
	CacheErrorCode init(InitArgs... args)
	{
		return CacheErrorCode::Success;
	}

	CacheErrorCode remove(ObjectTypePtr& ptr)
	{
		delete ptr;
		ptr = nullptr;

		return CacheErrorCode::Success;
	}

	CacheErrorCode getObject(ObjectUIDType objKey, ObjectTypePtr& ptrObject)
	{
		ASSERT(false);
		ptrObject = reinterpret_cast<ObjectTypePtr>(objKey);
		return CacheErrorCode::Success;
	}

	template<class Type, typename... ArgsType>
	CacheErrorCode createObjectOfType(ObjectUIDType& key, ObjectTypePtr& ptrObject, const ArgsType... args)
	{
		ptrObject = new ObjectType(new Type(args...), Type::UID);

		key = reinterpret_cast<ObjectUIDType>(ptrObject);
		ptrObject->setuid(key);
		return CacheErrorCode::Success;
	}
};
