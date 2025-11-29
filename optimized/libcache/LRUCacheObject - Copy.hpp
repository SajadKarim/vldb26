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
#include "ObjectFatUID.h"

template <typename Traits>
class LRUCacheObject 
{
private:
	using DataNode = typename Traits::DataNodeType;
	using IndexNode = typename Traits::IndexNodeType;
	
	typedef LRUCacheObject<Traits> SelfType;
	using ValueCoreTypes = std::tuple<DataNode, IndexNode>;
	typedef std::variant<std::shared_ptr<ValueCoreTypes>> ValueCoreTypesWrapper;

public:	// TODO: Make the following private and add setters and getters
	bool m_bDirty;
	ObjectFatUID m_uid;
	std::optional<ObjectFatUID> m_uidUpdated;

#ifdef __CONCURRENT__
	std::shared_mutex m_mtx;
#endif //__CONCURRENT__

	void* m_ptrCoreObject;
	uint8_t m_nCoreObjectType;

	std::atomic<bool> pendingFlag;
	std::weak_ptr<SelfType> m_ptrPrev;
	std::shared_ptr<SelfType> m_ptrNext;

	bool bseen;
public:
	~LRUCacheObject()
	{
		m_ptrNext.reset();

		deleteCoreObject();
	}

	LRUCacheObject(uint8_t nCoreObjectType, void* ptrCoreObject)
		: m_bDirty(true)
		, pendingFlag(false)
		, m_uidUpdated(std::nullopt)
	{
		bseen = true;
		m_ptrCoreObject = ptrCoreObject;
		m_nCoreObjectType = nCoreObjectType;
	}

	LRUCacheObject(uint16_t nDegree, const ObjectFatUID& uidObject, const char* szBuffer, uint32_t nDataLen, uint16_t nBlockSize)
		: m_bDirty(false)
		, m_uid(uidObject)
		, m_uidUpdated(std::nullopt)
	{
		bseen = true;
		m_nCoreObjectType = szBuffer[0];

		switch (m_nCoreObjectType)
		{
		case DataNode::UID:
		{
			m_ptrCoreObject = new DataNode(nDegree, szBuffer, nDataLen, nBlockSize);
			break;
		}
		case IndexNode::UID:
		{
			m_ptrCoreObject = new IndexNode(nDegree, szBuffer, nDataLen, nBlockSize);
			break;
		}
		default:
		{
			std::cout << "Deserialization Request for Unknown UID." << std::endl;
			throw new std::logic_error("Deserialization Request for Unknown UID.");
		}
		}
	}

	inline void deleteCoreObject()
	{
		if (m_ptrCoreObject != nullptr)
		{
			switch (m_nCoreObjectType)
			{
				case DataNode::UID:
				{
					delete static_cast<DataNode*>(m_ptrCoreObject);
					break;
				}
				case IndexNode::UID:
				{
					delete static_cast<IndexNode*>(m_ptrCoreObject);
					break;
				}
				default:
				{
					std::cout << "Destruction Request for Unknown UID." << std::endl;
					throw new std::logic_error("Destruction Request for Unknown UID.");
				}
			}

			m_ptrCoreObject = nullptr;
		}
	}

	CacheErrorCode updateCoreObject(uint16_t nDegree, const char* szBuffer, const ObjectFatUID& uidUpdated, uint32_t nDataLen, uint16_t nBlockSize)
	{
		m_bDirty = false;
		m_uid = uidUpdated;
		m_uidUpdated = std::nullopt;

		switch (m_nCoreObjectType)
		{
			case DataNode::UID:
			{
				ASSERT(DataNode::UID == szBuffer[0]);

				m_ptrCoreObject = new DataNode(nDegree, szBuffer, nDataLen, nBlockSize);
				return CacheErrorCode::Success;
			}
			case IndexNode::UID:
			{
				ASSERT(IndexNode::UID == szBuffer[0]);

				m_ptrCoreObject = new IndexNode(nDegree, szBuffer, nDataLen, nBlockSize);
				return CacheErrorCode::Success;
			}
			default:
			{
				std::cout << "Deserialization Request for Unknown UID." << std::endl;
				throw new std::logic_error("Deserialization Request for Unknown UID.");
			}
		}
		return CacheErrorCode::Error;
	}

	inline void serialize(char*& szData, uint32_t& nDataLen, uint16_t nBlockSize, void*& ptrBlockAppendOffset, bool& bAlignedAllocation) const
	{
		switch (m_nCoreObjectType)
		{
		case DataNode::UID:
		{
			DataNode* ptrObject = reinterpret_cast<DataNode*>(m_ptrCoreObject);
			ptrObject->serialize(szData, nDataLen, nBlockSize, ptrBlockAppendOffset, bAlignedAllocation);
			break;
		}
		case IndexNode::UID:
		{
			IndexNode* ptrObject = reinterpret_cast<IndexNode*>(m_ptrCoreObject);
			ptrObject->serialize(szData, nDataLen, nBlockSize, ptrBlockAppendOffset, bAlignedAllocation);
			break;
		}
		}
	}

	inline bool hasUpdatesToBeFlushed() const 
	{
		if (m_bDirty)
		{
			return true;
		}

		switch (m_nCoreObjectType)
		{
			case DataNode::UID:
			{
				//return false;
				return reinterpret_cast<DataNode*>(m_ptrCoreObject)->hasUIDUpdates();
			}
			case IndexNode::UID:
			{
				return reinterpret_cast<IndexNode*>(m_ptrCoreObject)->hasUIDUpdates();
			}
			default:
			{
				std::cout << "Unknown Object Found." << std::endl;
				throw new std::logic_error("Unknown Object Found.");
			}
		}
	}

	inline void validate(std::shared_ptr<SelfType> ptr)
	{

		switch (m_nCoreObjectType)
		{
		case DataNode::UID:
		{
			//return true;
			//return reinterpret_cast<DataNode*>(m_ptrCoreObject)->hasUIDUpdates();
			break;
		}
		case IndexNode::UID:
		{
			//return true;
			//reinterpret_cast<IndexNode*>(m_ptrCoreObject)->validate(reinterpret_cast<IndexNode*>(ptr->m_ptrCoreObject));
			break;
		}
		default:
		{
			std::cout << "Unknown Object Found." << std::endl;
			throw new std::logic_error("Unknown Object Found.");
		}
		}
	}
};