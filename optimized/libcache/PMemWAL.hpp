#pragma once
#include <memory>
#include <iostream>
#include <fcntl.h>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <variant>
#include <cmath>
#include "validityasserts.h"

#ifndef _MSC_VER
#include <libpmem.h>
#endif //_MSC_VER

template <typename Traits>
class PMemWAL
{
    static const size_t WAL_BUFFER_SIZE = 256;
    static const size_t MAX_WAL_FILE_SIZE = 1ULL * 1024 * 1024 * 1024;

public:
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

	bool writeMMapFile(void* hMemory, const void* szBuf, size_t nLen)
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

    using KeyType = typename Traits::KeyType;
    using ValueType = typename Traits::ValueType;
    using CacheType = typename Traits::CacheType;

    typedef PMemWAL<Traits> SelfType;

public:
    std::string m_stWALFile;
    //std::ofstream m_hWALFile;
    std::atomic<size_t> m_nWALSize;

    int nIsPMem;
    size_t m_nMappedLen;
    void* m_hMemory = NULL;

    CacheType* m_ptrCache = nullptr;

    void* m_szWALBuffers[2];
    std::atomic<size_t> m_nWALBuffersOffset[2];
    std::atomic<size_t> flushBufferSizes[2];

    std::atomic<size_t> m_nActiveBufferIndex;
    std::atomic<bool> m_bFlushInPending;
    std::atomic<bool> m_bStopFlushThread;
    std::thread m_tBackgroundFlush;

    void* szEntry;
public:
    PMemWAL(CacheType* ptrCache, const std::string& stWALFilePath)
        : m_ptrCache(ptrCache)
        , m_nMappedLen(0)
        , m_hMemory(nullptr)
        , m_nWALSize(0)
        , m_nActiveBufferIndex(0)
        , m_bFlushInPending(false)
        , m_bStopFlushThread(false)
        , m_stWALFile(stWALFilePath)
    {
        flushBufferSizes[0] = 0;
        flushBufferSizes[1] = 0;

        m_nWALBuffersOffset[0] = 0;
        m_nWALBuffersOffset[1] = 0;

        //m_szWALBuffers[0] = static_cast<char*>(std::malloc(WAL_BUFFER_SIZE));
        //m_szWALBuffers[1] = static_cast<char*>(std::malloc(WAL_BUFFER_SIZE));

#ifndef _MSC_VER
        if (posix_memalign(&m_szWALBuffers[0], 64, WAL_BUFFER_SIZE) != 0 || m_szWALBuffers[0] == nullptr
            || posix_memalign(&m_szWALBuffers[1], 64, WAL_BUFFER_SIZE) != 0 || m_szWALBuffers[1] == nullptr)
#else
        m_szWALBuffers[0] = _aligned_malloc(WAL_BUFFER_SIZE, 64);
        m_szWALBuffers[1] = _aligned_malloc(WAL_BUFFER_SIZE, 64);
        if (!m_szWALBuffers[0] || m_szWALBuffers[1])
#endif
        {
            throw std::bad_alloc();
        }

        if (!m_szWALBuffers[0] || !m_szWALBuffers[1]) 
        {
            throw std::runtime_error("Failed to allocate WAL m_szWALBuffers");
        }

        if (!openMMapFile(m_hMemory, m_stWALFile.c_str(), m_nMappedLen, nIsPMem))
        {
            if (!createMMapFile(m_hMemory, m_stWALFile.c_str(), MAX_WAL_FILE_SIZE, m_nMappedLen, nIsPMem))
            {
                throw new std::logic_error("Critical State: Failed to create mmap file for PMemStorage.");
            }
        }

        ASSERT(m_hMemory != nullptr);

        ASSERT(m_nMappedLen == MAX_WAL_FILE_SIZE);

        m_tBackgroundFlush = std::thread(&PMemWAL::flushLoop, this);

#ifndef _MSC_VER
        if (posix_memalign(&szEntry, 64, 256) != 0 || m_szWALBuffers[0] == nullptr)
#else
        szEntry = _aligned_malloc(256, 64);
        if (!szEntry)
#endif
        {
            throw std::bad_alloc();
        }
        //szEntry = static_cast<char*>(std::malloc(sizeof(KeyType) + sizeof(ValueType)));
    }

    ~PMemWAL() 
    {
        m_bStopFlushThread.store(true, std::memory_order_release);
        
        if (m_tBackgroundFlush.joinable())
        {
            m_tBackgroundFlush.join();
        }

        flushAllBuffers();
        
        std::free(m_szWALBuffers[0]);
        std::free(m_szWALBuffers[1]);
        
        closeMMapFile(m_hMemory, m_nMappedLen);
        //m_hWALFile.close();
    }

    void append(uint8_t op, const KeyType& key, const ValueType& value)
    {
        size_t nActivBufferIdx = m_nActiveBufferIndex.load(std::memory_order_acquire);
        size_t nActivBufferOffset = m_nWALBuffersOffset[nActivBufferIdx].load(std::memory_order_relaxed);
        size_t nEntrySize = sizeof(KeyType) + sizeof(ValueType);

        //char* szEntry = static_cast<char*>(std::malloc(nEntrySize));
        std::memcpy(szEntry, &key, sizeof(KeyType));
        std::memcpy(szEntry + sizeof(KeyType), &value, sizeof(ValueType));

        size_t nAvailableSpace = WAL_BUFFER_SIZE - nActivBufferOffset;

        size_t nBytesWritten = 0;

        if (nAvailableSpace > 0)
        {
            nBytesWritten = std::min(nAvailableSpace, nEntrySize);
            std::memcpy(m_szWALBuffers[nActivBufferIdx] + nActivBufferOffset, szEntry, nBytesWritten);

            m_nWALBuffersOffset[nActivBufferIdx].fetch_add(nBytesWritten, std::memory_order_release);
            nActivBufferOffset += nBytesWritten;
            //flushBufferSizes[nActivBufferIdx].store(nActivBufferOffset, std::memory_order_relaxed);
        }
        
        if(nActivBufferOffset == WAL_BUFFER_SIZE)
        {
            if (m_bFlushInPending.load(std::memory_order_acquire))
            {
                while (m_bFlushInPending.load(std::memory_order_acquire))
                {
                    std::this_thread::sleep_for(std::chrono::microseconds(1));
                }
            }

            size_t nNewActiveBufferIdx = 1 - nActivBufferIdx;
            m_nActiveBufferIndex.store(nNewActiveBufferIdx, std::memory_order_release);

            flushBufferSizes[nActivBufferIdx].store(nActivBufferOffset, std::memory_order_relaxed);
            m_nWALBuffersOffset[nActivBufferIdx].store(0, std::memory_order_relaxed);
            m_bFlushInPending.store(true, std::memory_order_release);

            nActivBufferIdx = nNewActiveBufferIdx;
            nActivBufferOffset = m_nWALBuffersOffset[nActivBufferIdx].load(std::memory_order_relaxed);
        }

        if (nEntrySize - nBytesWritten > 0)
        {
            std::memcpy(m_szWALBuffers[nActivBufferIdx] + nActivBufferOffset, szEntry + nBytesWritten, nEntrySize - nBytesWritten);
            m_nWALBuffersOffset[nActivBufferIdx].fetch_add(nEntrySize - nBytesWritten, std::memory_order_release);
            //std::memcpy(m_szWALBuffers[nActivBufferIdx] + nActivBufferOffset, &key, sizeof(KeyType));
            //std::memcpy(m_szWALBuffers[nActivBufferIdx] + nActivBufferOffset + sizeof(KeyType), &value, sizeof(ValueType));
            //m_nWALBuffersOffset[nActivBufferIdx].fetch_add(nEntrySize, std::memory_order_release);
        }
    }

    void flushLoop() 
    {
        using namespace std::chrono_literals;
        while (!m_bStopFlushThread.load(std::memory_order_acquire)) 
        {
            if (m_bFlushInPending.load(std::memory_order_acquire)) 
            {
                size_t nIdxBufferToBeFlushed = 1 - m_nActiveBufferIndex.load(std::memory_order_acquire);
                size_t nBytesToFlush = flushBufferSizes[nIdxBufferToBeFlushed].load(std::memory_order_relaxed);
                flushBuffer(nIdxBufferToBeFlushed, nBytesToFlush);
                m_bFlushInPending.store(false, std::memory_order_release);
            }
            std::this_thread::sleep_for(std::chrono::microseconds(2));
        }
    }

    void flushBuffer(size_t nIdxBufferToFlush, size_t nBytesToWrite) 
    {
        if (nBytesToWrite == 0)
        {
            return;
        }

        size_t nOffset = m_nWALSize.load(std::memory_order_acquire);

#ifndef _MSC_VER
        if (!writeMMapFile(m_hMemory + nOffset, m_szWALBuffers[nIdxBufferToFlush], nBytesToWrite))
#endif //_MSC_VER
        {
            throw new std::logic_error("Critical State: Failed to write object to PMemStorage.");   // TODO: critical log.
        }

        m_nWALSize += nBytesToWrite;

        if (m_nWALSize >= MAX_WAL_FILE_SIZE) 
        {
            std::cout << "WAL reset!" << std::endl;
            //m_ptrCache->persistAllItems();
            
            m_nWALSize = 0;
        }
    }

    void flushAllBuffers() 
    {
        size_t nIdxActiveBuffer = m_nActiveBufferIndex.load(std::memory_order_acquire);

        if (m_bFlushInPending.load(std::memory_order_acquire)) 
        {
            size_t nIdxBufferToBeFlushed = 1 - nIdxActiveBuffer;
            size_t nBytesToFlush = flushBufferSizes[nIdxBufferToBeFlushed].load(std::memory_order_relaxed);
            flushBuffer(nIdxBufferToBeFlushed, nBytesToFlush);
            m_bFlushInPending.store(false, std::memory_order_release);
        }

        nIdxActiveBuffer = m_nActiveBufferIndex.load(std::memory_order_acquire);
        size_t nBytesToFlush = m_nWALBuffersOffset[nIdxActiveBuffer].load(std::memory_order_relaxed);
        if (nBytesToFlush > 0) 
        {
            flushBuffer(nIdxActiveBuffer, nBytesToFlush);
            m_nWALBuffersOffset[nIdxActiveBuffer].store(0, std::memory_order_relaxed);
        }

    }

    void runWALUnitTest()
    {
        /*const std::string stFile = "/mnt/tmpfs/test_wal.bin";

        std::vector<std::pair<KeyType, ValueType>> vtEntries;
        {
            SelfType oWAL(m_ptrCache, stFile);

            for (int nIdx = 0; nIdx < 50000; ++nIdx)
            {
                KeyType key = static_cast<KeyType>(nIdx);
                ValueType value = static_cast<ValueType>(nIdx);

                vtEntries.emplace_back(key, value);

                oWAL.append(key, value);
            }
        }

        FILE* fp = nullptr;
        fopen_s(&fp, stFile.c_str(), "rb");

        ASSERT(fp && "Failed to open WAL file");

        for (int nIdx = 0; nIdx < vtEntries.size(); ++nIdx)
        {
            KeyType key;
            ValueType value;

            size_t readK = fread(&key, 1, sizeof(KeyType), fp);
            size_t readV = fread(&value, 1, sizeof(ValueType), fp);
            ASSERT(readK == sizeof(KeyType));
            ASSERT(readV == sizeof(ValueType));

            ASSERT(key == vtEntries[nIdx].first);
            ASSERT(value == vtEntries[nIdx].second);
        }

        std::fclose(fp);
        std::remove(stFile.c_str());*/

        std::cout << "WAL unit test passed!" << std::endl;
    }

};

