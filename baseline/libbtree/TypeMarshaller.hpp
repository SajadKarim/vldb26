#pragma once
#include <variant>
#include <typeinfo>
#include <iostream>
#include <fstream>

class TypeMarshaller
{
public:
	template <typename... ValueCoreTypes>
	static void serialize(std::fstream& os, const std::variant<std::shared_ptr<ValueCoreTypes>...>& ptrObject, uint8_t& uidObject, uint32_t& nBufferSize)
	{
		std::visit([&os, &uidObject, &nBufferSize](const auto& value) {
			value->writeToStream(os, uidObject, nBufferSize);
			}, ptrObject);
	}

	template <typename... ValueCoreTypes>
	static void serialize(char*& szBuffer, const std::variant<std::shared_ptr<ValueCoreTypes>...>& ptrObject, uint8_t& uidObject, uint32_t& nBufferSize)
	{
		std::visit([&szBuffer, &uidObject, &nBufferSize](const auto& value) {
			value->serialize(szBuffer, uidObject, nBufferSize);
			}, ptrObject);
	}

	template <typename ObjectType, typename... ValueCoreTypes>
	static void deserialize(std::fstream& fs, ObjectType& ptrObject)
	{
		using TypeA = typename NthType<0, ValueCoreTypes...>::type;
		using TypeB = typename NthType<1, ValueCoreTypes...>::type;

		uint8_t uidObjectType;
		fs.read(reinterpret_cast<char*>(&uidObjectType), sizeof(uint8_t));

		switch (uidObjectType)
		{
		case TypeA::UID:
			ptrObject = std::make_shared<TypeA>(fs);
			break;
		case TypeB::UID:
			ptrObject = std::make_shared<TypeB>(fs);
			break;
		}
	}

	template <typename ObjectType, typename... ValueCoreTypes>
	static void deserialize(const char* szData, ObjectType& ptrObject)
	{
		using TypeA = typename NthType<0, ValueCoreTypes...>::type;
		using TypeB = typename NthType<1, ValueCoreTypes...>::type;

		uint8_t uidObjectType;

		switch (szData[0])
		{
		case TypeA::UID:
			ptrObject = std::make_shared<TypeA>(szData);
			break;
		case TypeB::UID:
			ptrObject = std::make_shared<TypeB>(szData);
			break;
		default:
			std::cout << "Deserialization request for Uknown UID." << std::endl;
			throw new std::logic_error(".....");
		}
	}
};
