#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <optional>

class ObjectFatUID
{
public:
	enum StorageMedia
	{
		None = 0,
		Volatile,
		DRAM,
		PMem,
		File
	};

	struct FilePointer
	{
		size_t m_nOffset;
		uint32_t m_nSize;
	};

	struct NodeUID
	{
		uint8_t m_nType;
		uint8_t m_nMediaType;
		union
		{
			uintptr_t m_ptrVolatile;
			FilePointer m_ptrFile;
		} FATPOINTER;
	};

private:
	NodeUID m_uid;

public:
	inline uint8_t getObjectType() const
	{
		return m_uid.m_nType;
	}

	inline uint8_t getMediaType() const
	{
		return m_uid.m_nMediaType;
	}

	inline uintptr_t getVolatilePointerValue() const
	{
		return m_uid.FATPOINTER.m_ptrVolatile;
	}

	inline size_t getPersistentPointerValue() const
	{
		return m_uid.FATPOINTER.m_ptrFile.m_nOffset;
	}

	inline uint32_t getPersistentObjectSize() const
	{
		return m_uid.FATPOINTER.m_ptrFile.m_nSize;
	}

public:
	template <typename... Args>
	static void createAddressFromArgs(ObjectFatUID& uid, StorageMedia nMediaType, Args... args)
	{
		switch (nMediaType)
		{
		case Volatile:
			return createAddressFromVolatilePointer(uid, args...);
			break;
		case DRAM:
			return createAddressFromDRAMCacheCounter(uid, args...);
			break;
		case PMem:
			return createAddressFromPMemOffset(uid, args...);
			break;
		case File:
			return createAddressFromFileOffset(uid, args...);
			break;
		}

		std::cout << "Critical State: UID is requested for an invalid UID." << std::endl;
		throw new std::logic_error(".....");   // TODO: critical log.
	}

        static void createAddressFromPMemOffset(ObjectFatUID& uid, uint8_t nType, size_t nOffset, uint32_t nSize)
        {
                uid.m_uid.m_nType = nType;
                uid.m_uid.m_nMediaType = PMem;
                uid.m_uid.FATPOINTER.m_ptrFile.m_nOffset = nOffset;
                uid.m_uid.FATPOINTER.m_ptrFile.m_nSize = nSize;
        }

	static void createAddressFromFileOffset(ObjectFatUID& uid, uint8_t nType, size_t nOffset, uint32_t nSize)
	{
		uid.m_uid.m_nType = nType;
		uid.m_uid.m_nMediaType = File;
		uid.m_uid.FATPOINTER.m_ptrFile.m_nOffset = nOffset;
		uid.m_uid.FATPOINTER.m_ptrFile.m_nSize = nSize;
	}

	static void createAddressFromVolatilePointer(ObjectFatUID& uid, uint8_t nType, uintptr_t ptr, ...)
	{
		uid.m_uid.m_nType = nType;
		uid.m_uid.m_nMediaType = Volatile;
		uid.m_uid.FATPOINTER.m_ptrVolatile = ptr;
	}

	static void createAddressFromDRAMCacheCounter(ObjectFatUID& uid, uint8_t nType, size_t nOffset, uint32_t nSize)
	{
		uid.m_uid.m_nType = nType;
		uid.m_uid.m_nMediaType = DRAM;
		uid.m_uid.FATPOINTER.m_ptrFile.m_nOffset = nOffset;
		uid.m_uid.FATPOINTER.m_ptrFile.m_nSize = nSize;
	}

	bool operator==(const ObjectFatUID& rhs) const
	{
		if (m_uid.m_nType != rhs.m_uid.m_nType
			|| m_uid.m_nMediaType != rhs.m_uid.m_nMediaType)
		{
			return false;
		}

		switch (m_uid.m_nMediaType)
		{
		case Volatile:
			return m_uid.FATPOINTER.m_ptrVolatile == rhs.m_uid.FATPOINTER.m_ptrVolatile;
		case DRAM:
			return m_uid.FATPOINTER.m_ptrVolatile == rhs.m_uid.FATPOINTER.m_ptrVolatile;
		case PMem:
                        return m_uid.FATPOINTER.m_ptrFile.m_nOffset == rhs.m_uid.FATPOINTER.m_ptrFile.m_nOffset &&
                                m_uid.FATPOINTER.m_ptrFile.m_nSize == rhs.m_uid.FATPOINTER.m_ptrFile.m_nSize;
		case File:
			return m_uid.FATPOINTER.m_ptrFile.m_nOffset == rhs.m_uid.FATPOINTER.m_ptrFile.m_nOffset &&
				m_uid.FATPOINTER.m_ptrFile.m_nSize == rhs.m_uid.FATPOINTER.m_ptrFile.m_nSize;
		default:
			return std::memcmp(this, &rhs.m_uid, sizeof(NodeUID)) == 0;
		}

		//Following is comiler specific!
		//return memcmp(&m_uid, &rhs.m_uid, sizeof(NodeUID)) == 0;
	}

	bool operator <(const ObjectFatUID& rhs) const
	{
		return std::memcmp(this, &rhs, sizeof(NodeUID)) < 0;
	}

	size_t gethash() const
	{
		size_t hashValue = 0;
		hashValue ^= std::hash<uint8_t>()(m_uid.m_nType);
		hashValue ^= std::hash<uint8_t>()(m_uid.m_nMediaType);

		switch (m_uid.m_nMediaType)
		{
		case ObjectFatUID::StorageMedia::None:
			break;
		case ObjectFatUID::StorageMedia::Volatile:
			hashValue ^= std::hash<uintptr_t>()(m_uid.FATPOINTER.m_ptrVolatile);
			break;
		case ObjectFatUID::StorageMedia::DRAM:
			hashValue ^= std::hash<uintptr_t>()(m_uid.FATPOINTER.m_ptrVolatile);
			break;
                case ObjectFatUID::StorageMedia::PMem:
			{
                        size_t offsetHash = std::hash<size_t>()(m_uid.FATPOINTER.m_ptrFile.m_nOffset);
                        size_t sizeHash = std::hash<uint32_t>()(m_uid.FATPOINTER.m_ptrFile.m_nSize);
                        hashValue ^= offsetHash ^ (sizeHash + 0x9e3779b9 + (offsetHash << 6) + (offsetHash >> 2));
			}
                        break;
		case ObjectFatUID::StorageMedia::File:
			{
			size_t offsetHash = std::hash<size_t>()(m_uid.FATPOINTER.m_ptrFile.m_nOffset);
			size_t sizeHash = std::hash<uint32_t>()(m_uid.FATPOINTER.m_ptrFile.m_nSize);
			hashValue ^= offsetHash ^ (sizeHash + 0x9e3779b9 + (offsetHash << 6) + (offsetHash >> 2));
			}
			break;
		}

		return hashValue;
	}

public:
	~ObjectFatUID()
	{
		m_uid.m_nType = 0;
		m_uid.m_nMediaType = 0;
		m_uid.FATPOINTER.m_ptrVolatile = 0;
		m_uid.FATPOINTER.m_ptrFile.m_nOffset = 0;
		m_uid.FATPOINTER.m_ptrFile.m_nSize = 0;
	}

	ObjectFatUID()
	{
		m_uid.m_nType = 0;
		m_uid.m_nMediaType = 0;
		m_uid.FATPOINTER.m_ptrVolatile = 0;
		m_uid.FATPOINTER.m_ptrFile.m_nOffset = 0;
		m_uid.FATPOINTER.m_ptrFile.m_nSize = 0;
	}

	ObjectFatUID(const ObjectFatUID& other)
	{
		m_uid.m_nType = other.m_uid.m_nType;
		m_uid.m_nMediaType = other.m_uid.m_nMediaType;
		m_uid.FATPOINTER.m_ptrVolatile = other.m_uid.FATPOINTER.m_ptrVolatile;
		m_uid.FATPOINTER.m_ptrFile.m_nOffset = other.m_uid.FATPOINTER.m_ptrFile.m_nOffset;
		m_uid.FATPOINTER.m_ptrFile.m_nSize = other.m_uid.FATPOINTER.m_ptrFile.m_nSize;
	}

	ObjectFatUID& operator=(const ObjectFatUID& other)
	{
		m_uid = other.m_uid;
		return *this;
	}

	ObjectFatUID(ObjectFatUID&& other) noexcept
	{
		m_uid = other.m_uid;
	}

	ObjectFatUID& operator=(ObjectFatUID&& other) noexcept {
		
		if (this == &other) 
			return *this;
		
		m_uid = std::move(other.m_uid);

		return *this;
	}

	std::string toString() const
	{
		std::string szData;
		switch (m_uid.m_nMediaType)
		{
		case None:
			break;
		case Volatile:
			szData.append("V:");
			szData.append(std::to_string(m_uid.FATPOINTER.m_ptrVolatile));
			break;
		case DRAM:
			szData.append("D:");
			szData.append(std::to_string(m_uid.FATPOINTER.m_ptrVolatile));
			break;
		case PMem:
			szData.append("P:");
                        szData.append(std::to_string(m_uid.FATPOINTER.m_ptrFile.m_nOffset));
                        szData.append(":");
                        szData.append(std::to_string(m_uid.FATPOINTER.m_ptrFile.m_nSize));
			break;
		case File:
			szData.append("F:");
			szData.append(std::to_string(m_uid.FATPOINTER.m_ptrFile.m_nOffset));
			szData.append(":");
			szData.append(std::to_string(m_uid.FATPOINTER.m_ptrFile.m_nSize));
			break;
		}
		return szData;
	}
};

namespace std {
	template <>
	struct hash<ObjectFatUID> {
		size_t operator()(const ObjectFatUID& rhs) const
		{
			return rhs.gethash();
		}
	};

	template <>
	struct hash<const ObjectFatUID*> {
		size_t operator()(const ObjectFatUID* rhs) const
		{
			return rhs->gethash();
		}
	};

	template <>
	struct hash<const std::shared_ptr<ObjectFatUID>> {
		size_t operator()(const std::shared_ptr<ObjectFatUID> rhs) const
		{
			return rhs->gethash();
		}
	};

	template <>
	struct equal_to<ObjectFatUID> {
		bool operator()(const ObjectFatUID& lhs, const ObjectFatUID& rhs) const
		{
			return lhs == rhs;
		}
	};

	template <>
	struct equal_to<const ObjectFatUID*> {
		bool operator()(const ObjectFatUID* lhs, const ObjectFatUID* rhs) const
		{
			return *lhs == *rhs;
		}
	};

	template <>
	struct equal_to<const std::shared_ptr<ObjectFatUID>> {
		bool operator()(const std::shared_ptr<ObjectFatUID> lhs, const std::shared_ptr<ObjectFatUID> rhs) const
		{
			return *lhs == *rhs;
		}
	};
}
