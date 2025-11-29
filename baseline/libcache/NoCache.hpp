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

template<typename KeyType, template <typename...> typename ValueType, typename... ValueCoreTypes>
class NoCache
{
public:
	typedef KeyType ObjectUIDType;
	typedef ValueType<ValueCoreTypes...> ObjectType;
	typedef ValueType<ValueCoreTypes...>* ObjectTypePtr;

public:
	~NoCache()
	{
	}

	NoCache()
	{
	}

	template <typename... InitArgs>
	CacheErrorCode init(InitArgs... args)
	{
		return CacheErrorCode::Success;
	}

	CacheErrorCode remove(ObjectUIDType objKey)
	{
		ObjectTypePtr ptrValue = reinterpret_cast<ObjectTypePtr>(objKey);
		delete ptrValue;

		return CacheErrorCode::KeyDoesNotExist;
	}

	CacheErrorCode getObject(ObjectUIDType objKey, ObjectTypePtr& ptrObject)
	{
		ptrObject = reinterpret_cast<ObjectTypePtr>(objKey);
		return CacheErrorCode::Success;
	}

	template <typename Type>
	CacheErrorCode getObjectOfType(ObjectUIDType objKey, Type& ptrObject, ObjectTypePtr& ptrValue)
	{
		ptrValue = reinterpret_cast<ObjectTypePtr>(objKey);

		if (std::holds_alternative<Type>(ptrValue->getInnerData()))
		{
			ptrObject = std::get<Type>(ptrValue->getInnerData());
			return CacheErrorCode::Success;
		}

		return CacheErrorCode::Error;
	}

	template <typename Type>
	CacheErrorCode getObjectOfType(ObjectUIDType objKey, Type& ptrObject)
	{
		ObjectTypePtr ptrValue = reinterpret_cast<ObjectTypePtr>(objKey);

		if (std::holds_alternative<Type>(ptrValue->getInnerData()))
		{
			ptrObject = std::get<Type>(ptrValue->getInnerData());
			return CacheErrorCode::Success;
		}

		return CacheErrorCode::Error;
	}

	template<class Type, typename... ArgsType>
	CacheErrorCode createObjectOfType(std::optional<ObjectUIDType>& key, const ArgsType... args)
	{
		ObjectTypePtr ptrObject = new ObjectType(std::make_shared<Type>(args...));

		key = reinterpret_cast<ObjectUIDType>(ptrObject);
		return CacheErrorCode::Success;
	}

	template<class Type, typename... ArgsType>
	CacheErrorCode createObjectOfType(std::optional<ObjectUIDType>& key, ObjectTypePtr& ptrObject, const ArgsType... args)
	{
		ptrObject = new ObjectType(std::make_shared<Type>(args...));

		key = reinterpret_cast<ObjectUIDType>(ptrObject);
		return CacheErrorCode::Success;
	}

	template<class Type, typename... ArgsType>
	CacheErrorCode createObjectOfType(std::optional<ObjectUIDType>& key, std::shared_ptr<Type>& ptrCoreObject, const ArgsType... args)
	{
		ptrCoreObject = std::make_shared<Type>(args...);
		ObjectTypePtr ptrObject = new ObjectType(ptrCoreObject);

		key = reinterpret_cast<ObjectUIDType>(ptrObject);
		return CacheErrorCode::Success;
	}
};
