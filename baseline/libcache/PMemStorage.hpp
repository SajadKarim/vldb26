#pragma once
#include <memory>
#include <iostream>
#include <fcntl.h>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <variant>
#include <cmath>

#ifndef _MSC_VER
#include <libpmem.h>
#endif //_MSC_VER

template<
	typename ICallback,
	typename ObjectUIDType_,
	template <typename, typename...> typename ValueType,
	typename CoreTypesMarshaller,
	typename... ValueCoreTypes
>
class PMemStorage
{
	bool createMMapFile(void*& hMemory, const char* szPath, size_t nFileSize, size_t& nMappedLen, int& bIsPMem)
	{
#ifndef _MSC_VER
		if ((hMemory = pmem_map_file(szPath,
			nFileSize,
			PMEM_FILE_CREATE | PMEM_FILE_EXCL,
			0666, &nMappedLen, &bIsPMem)) == NULL)
		{
			return false;
		}
#endif //_MSC_VER

		return true;
	}

	bool openMMapFile(void*& hMemory, const char* szPath, size_t& nMappedLen, int& bIsPMem)
	{
#ifndef _MSC_VER
		if ((hMemory = pmem_map_file(szPath,
			0,
			0,
			0666, &nMappedLen, &bIsPMem)) == NULL)
		{
			return false;
		}
#endif //_MSC_VER

		return true;
	}

	bool writeMMapFile(void* hMemory, const char* szBuf, size_t nLen)
	{
#ifndef _MSC_VER
		void* hDestBuf = pmem_memcpy_persist(hMemory, szBuf, nLen);

		if (hDestBuf == NULL)
		{
			return false;
		}
		//pmem_drain();
#endif //_MSC_VER

		return true;
	}

	bool readMMapFile(const void* hMemory, char* szBuf, size_t nLen)
	{
#ifndef _MSC_VER
		void* hDestBuf = pmem_memcpy(szBuf, hMemory, nLen, PMEM_F_MEM_NOFLUSH);

		if (hDestBuf == NULL)
		{
			return false;
		}
#endif //_MSC_VER

		return true;
	}

	void closeMMapFile(void* hMemory, size_t nMappedLen)
	{
#ifndef _MSC_VER
		pmem_unmap(hMemory, nMappedLen);
#endif //_MSC_VER
	}

	typedef PMemStorage<ICallback, ObjectUIDType_, ValueType, CoreTypesMarshaller, ValueCoreTypes...> SelfType;

public:
	typedef ObjectUIDType_ ObjectUIDType;
	typedef ValueType<CoreTypesMarshaller, ValueCoreTypes...> ObjectType;

private:
	int nIsPMem;
	size_t m_nMappedLen;
	void* m_hMemory = NULL;

	size_t m_nNextBlock;

	size_t m_nBlockSize;
	size_t m_nStorageSize;
	std::string m_stFilename;

	ICallback* m_ptrCallback;

	std::vector<bool> m_vtAllocationTable;


#ifdef __CONCURRENT__
	bool m_bStopFlush;
	std::thread m_threadBatchFlush;

	mutable std::shared_mutex m_mtxFile;
	mutable std::shared_mutex m_mtxStorage;

	std::unordered_map<ObjectUIDType, std::shared_ptr<ObjectType>> m_mpObjects;
#endif //__CONCURRENT__

public:
	~PMemStorage()
	{
		closeMMapFile(m_hMemory, m_nMappedLen);

#ifdef __CONCURRENT__
		m_bStopFlush = true;
		//m_threadBatchFlush.join();

		m_mpObjects.clear();
#endif //__CONCURRENT__
	}

	PMemStorage(size_t nBlockSize, size_t nStorageSize, const std::string& stFilename)
		: m_nStorageSize(nStorageSize)
		, m_nBlockSize(nBlockSize)
		, m_stFilename(stFilename)
		, m_nNextBlock(0)
		, m_nMappedLen(0)		
		, m_hMemory(nullptr)
		, m_ptrCallback(NULL)
	{
		//nStorageSize = 10ULL *1024*1024*1024;
		//m_nStorageSize = 10ULL*1024*1024*1024;

		if( !openMMapFile(m_hMemory, stFilename.c_str(), m_nMappedLen, nIsPMem))
		{
			std::cout << stFilename.c_str() << std::endl;
			std::cout << nStorageSize << std::endl;
			if( !createMMapFile(m_hMemory, stFilename.c_str(), nStorageSize, m_nMappedLen, nIsPMem))
			{
				std::cout << "Critical State: Failed to create mmap file for PMemStorage." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}
		}

		assert(m_hMemory != nullptr);

		assert(m_nMappedLen == nStorageSize);

		m_vtAllocationTable.resize(nStorageSize / nBlockSize, false);

#ifdef __CONCURRENT__
		m_bStopFlush = false;
		//m_threadBatchFlush = std::thread(handlerBatchFlush, this);
#endif //__CONCURRENT__
	}

public:
	inline size_t getNextAvailableBlockOffset() const
	{
		return m_nNextBlock;
	}

	inline size_t getBlockSize() const
	{
		return m_nBlockSize;
	}

	inline ObjectUIDType::StorageMedia getStorageType()
	{
		return ObjectUIDType::PMem;
	}

public:
	template <typename... InitArgs>
	CacheErrorCode init(ICallback* ptrCallback, InitArgs... args)
	{
		m_ptrCallback = ptrCallback;// getNthElement<0>(args...);
		return CacheErrorCode::Success;
	}

	std::shared_ptr<ObjectType> getObject(const ObjectUIDType& uidObject)
	{
		//if(uidObject.getMediaType() != 3)
		//std::cout << "----------------------------------------------------->> " << static_cast<int>(uidObject.getMediaType()) << "," << uidObject.getPersistentPointerValue() << std::endl;
		return std::make_shared<ObjectType>((char*)m_hMemory + uidObject.getPersistentPointerValue());

		/*
		char* szBuffer = new char[uidObject.m_uid.FATPOINTER.m_ptrFile.m_nSize + 1];
		memset(szBuffer, 0, uidObject.m_uid.FATPOINTER.m_ptrFile.m_nSize + 1);

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_file_storage(m_mtxStorage);
#endif //__CONCURRENT__

		m_fsStorage.seekg(uidObject.m_uid.FATPOINTER.m_ptrFile.m_nOffset);
		std::shared_ptr<ObjectType> ptrObject = std::make_shared<ObjectType>(m_fsStorage);
		m_fsStorage.read(szBuffer, uidObject.m_uid.FATPOINTER.m_ptrFile.m_nSize);

#ifdef __CONCURRENT__
		lock_file_storage.unlock();
#endif //__CONCURRENT__

		std::shared_ptr<ObjectType> ptrObject = std::make_shared<ObjectType>(szBuffer);

		ptrObject->setDirtyFlag( false);

		delete[] szBuffer;

		return ptrObject;
		*/
	}

	CacheErrorCode remove(const ObjectUIDType& ptrKey)
	{
		//throw new std::logic_error("no implementation!");
		return CacheErrorCode::Success;
	}

	CacheErrorCode addObject(const ObjectUIDType& uidObject, std::shared_ptr<ObjectType> ptrObject, ObjectUIDType& uidUpdated)
	{
		uint32_t nBufferSize = 0;
		uint8_t uidObjectType = 0;

		char* szBuffer = NULL;
		ptrObject->serialize(szBuffer, uidObjectType, nBufferSize);

		size_t nOffset = m_nNextBlock * m_nBlockSize;

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_storage(m_mtxStorage);
#endif //__CONCURRENT__

#ifndef _MSC_VER
		if (!writeMMapFile(static_cast<char*>(m_hMemory) + nOffset, szBuffer, nBufferSize))
#endif //_MSC_VER
		{
			std::cout << "Critical State: Failed to write object to PMemStorage." << std::endl;
			throw new std::logic_error(".....");   // TODO: critical log.
		}

		m_nNextBlock += std::ceil(nBufferSize / (float)m_nBlockSize);

#ifdef __CONCURRENT__
		lock_storage.unlock();
#endif //__CONCURRENT__

		delete[] szBuffer;

		ObjectUIDType::createAddressFromPMemOffset(uidUpdated, uidObject.getObjectType(), nOffset, nBufferSize);

		return CacheErrorCode::Success;
	}

	/*CacheErrorCode addObject(ObjectUIDType uidObject, std::shared_ptr<ObjectType> ptrObject, ObjectUIDType& uidUpdated)
	{
		size_t nBufferSize = 0;
		uint8_t uidObjectType = 0;

		char* szBuffer = NULL; //2
		ptrObject->serialize(szBuffer, uidObjectType, nBufferSize); //2

#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_file_storage(m_mtxStorage);
#endif //__CONCURRENT__

		// memcpy(m_szStorage + (m_nNextBlock * m_nBlockSize), szBuffer, nBufferSize);
		if(!writeMMapFile(hMemory + ( m_nNextBlock * m_nBlockSize  ), szBuffer, nBufferSize))
		{
			throw new std::logic_error("failed to write data!");
		}
		
		

		//m_fsStorage.seekp(m_nNextBlock * m_nBlockSize);
		//ptrObject->serialize(m_fsStorage, uidObjectType, nBufferSize); //1
		//m_fsStorage.write(szBuffer, nBufferSize); //2
		//m_fsStorage.flush();

		size_t nRequiredBlocks = std::ceil((nBufferSize + sizeof(uint8_t)) / (float)m_nBlockSize);

#ifdef __CONCURRENT__
		lock_file_storage.unlock();
#endif //__CONCURRENT__

		delete[] szBuffer; //2

		uidUpdated = ObjectUIDType::createAddressFromFileOffset(m_nNextBlock, m_nBlockSize, nBufferSize + sizeof(uint8_t));

		for (int idx = 0; idx < nRequiredBlocks; idx++)
		{
			m_vtAllocationTable[m_nNextBlock++] = true;
		}

		return CacheErrorCode::Success;
	}*/

	CacheErrorCode addObjects(std::vector<std::pair<ObjectUIDType, std::pair<std::optional<ObjectUIDType>, std::shared_ptr<ObjectType>>>>& vtObjects, size_t nNewOffset)
	{
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_file_storage(m_mtxStorage);
#endif //__CONCURRENT__

		for (auto it = vtObjects.begin(); it != vtObjects.end(); it++)
		{
			uint32_t nBufferSize = 0;
			uint8_t uidObjectType = 0;

			char* szBuffer = NULL; //2
			(*it).second.second->serialize(szBuffer, uidObjectType, nBufferSize);
			
#ifndef _MSC_VER
			if (!writeMMapFile(static_cast<char*>(m_hMemory) + (*(*it).second.first).getPersistentPointerValue(), szBuffer, nBufferSize))
#endif //_MSC_VER
			{
				std::cout << "Critical State: Failed to write objects to PMemStorage." << std::endl;
				throw new std::logic_error(".....");   // TODO: critical log.
			}

			delete[] szBuffer;
		}

		m_nNextBlock = nNewOffset;

		return CacheErrorCode::Success;
	}

#ifdef __CONCURRENT__
	/*void performBatchFlush()
	{
		std::unordered_map<ObjectUIDType, ObjectUIDType> mpUpdatedUIDs;
#ifdef __CONCURRENT__
		std::unique_lock<std::shared_mutex> lock_file_storage(m_mtxStorage);
#endif //__CONCURRENT__

		auto it = m_mpObjects.begin();
		while (it != m_mpObjects.end())
		{
			std::tuple<uint8_t, const std::byte*, size_t> tpSerializedData = it->second->serialize();

			m_fsStorage.seekp(m_nNextBlock * m_nBlockSize);
			m_fsStorage.write((char*)(&std::get<0>(tpSerializedData)), sizeof(uint8_t));
			m_fsStorage.write((char*)(std::get<1>(tpSerializedData)), std::get<2>(tpSerializedData));

			size_t nBlockRequired = std::ceil(std::get<2>(tpSerializedData) / (float)m_nBlockSize);

			ObjectUIDType uid = ObjectUIDType::createAddressFromFileOffset(m_nBlockSize, nBlockRequired * m_nBlockSize);
			mpUpdatedUIDs[it->first] = uid;

			for (int idx = 0; idx < nBlockRequired; idx++)
			{
				m_vtAllocationTable[m_nNextBlock++] = true;
			}
		}
		m_fsStorage.flush();

		m_ptrCallback->keysUpdate(mpUpdatedUIDs);
	}*/

	static void handlerBatchFlush(SelfType* ptrSelf)
	{
		do
		{
			//ptrSelf->performBatchFlush();

			std::this_thread::sleep_for(100ms);

		} while (!ptrSelf->m_bStopFlush);
	}
#endif //__CONCURRENT__
};

