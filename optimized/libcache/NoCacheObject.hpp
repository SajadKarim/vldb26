#pragma once
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <syncstream>
#include <thread>
#include <variant>
#include <typeinfo>
#include <signal.h>
#include <setjmp.h>

#include "ErrorCodes.h"

//// A quick and dirty fix for BEpsilonStore because I initially did not implement for NoCache, but for a quick
//// comparision with BPlusStore, I enabled NoCache but there are still some places where I need to handle NoCacheObject
//// properly. This is a temporary solution until I refactor the code to handle NoCacheObject.
//static thread_local jmp_buf deletion_jmp_buf;
//static thread_local bool deletion_signal_setup = false;
//static void segfault_handler(int sig) {
//    if (deletion_signal_setup) {
//        longjmp(deletion_jmp_buf, 1);
//    }
//}

template <typename Traits>
class NoCacheObject 
{
private:
	using DataNode = typename Traits::DataNodeType;
	using IndexNode = typename Traits::IndexNodeType;
	using ObjectUIDType = typename Traits::ObjectUIDType;

	using ValueCoreTypes = std::tuple<DataNode, IndexNode>;
	typedef std::variant<std::shared_ptr<ValueCoreTypes>> ValueCoreTypesWrapper;

public:
	ObjectUIDType m_uid;
	void* m_ptrCoreObject;
	uint8_t m_nCoreObjectType;
	mutable std::shared_mutex m_mtx;

public:
	~NoCacheObject()
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
		}
	}

	//template<class CoreObjectType>
	NoCacheObject(void* ptrCoreObject, uint8_t nCoreObjectType)
	{
		m_nCoreObjectType = nCoreObjectType;
		m_ptrCoreObject = ptrCoreObject;
	}

	inline void setuid(ObjectUIDType& uid)
	{
		m_uid = uid;
	}
};