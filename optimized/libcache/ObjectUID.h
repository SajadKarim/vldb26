#pragma once
#include <cstdint>
#include <memory>

class ObjectUID
{
public:
	uintptr_t m_ptrVolatile;

	static ObjectUID createAddressFromVolatilePointer(uintptr_t ptr)
	{
		ObjectUID key;
		key.m_ptrVolatile = ptr;

		return key;
	}

	bool operator==(const ObjectUID& rhs) const {
		return m_ptrVolatile == rhs.m_ptrVolatile;
	}

	bool operator <(const ObjectUID& rhs) const
	{
		return m_ptrVolatile < rhs.m_ptrVolatile;
	}

	struct HashFunction
	{
	public:
		size_t operator()(const ObjectUID& rhs) const
		{
			return std::hash<uint32_t>()(rhs.m_ptrVolatile);
		}
	};

	struct EqualFunction
	{
	public:
		bool operator()(const ObjectUID& lhs, const ObjectUID& rhs) const {
			return lhs.m_ptrVolatile == rhs.m_ptrVolatile;
		}
	};


public:
	ObjectUID()
	{
	}
};

namespace std {
	template <>
	struct hash<ObjectUID> {
		size_t operator()(const ObjectUID& rhs) const
		{
			return std::hash<uint32_t>()(rhs.m_ptrVolatile);
		}
	};
}