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

template <typename Traits>
class FileWAL
{
private:

    static const size_t WAL_BUFFER_SIZE = 4096;
    static const size_t MAX_WAL_FILE_SIZE = 1ULL * 1024 * 1024 * 1024;

    using KeyType = typename Traits::KeyType;
    using ValueType = typename Traits::ValueType;
    using CacheType = typename Traits::CacheType;

    typedef FileWAL<Traits> SelfType;

public:
    std::string m_stWALFile;
    std::ofstream m_hWALFile;
    std::atomic<size_t> m_nWALSize;

    CacheType* m_ptrCache = nullptr;

    char* m_szWALBuffers[2];
    std::atomic<size_t> m_nWALBuffersOffset[2];
    std::atomic<size_t> flushBufferSizes[2];

    std::atomic<size_t> m_nActiveBufferIndex;
    std::atomic<bool> m_bFlushInPending;
    std::atomic<bool> m_bStopFlushThread;
    std::thread m_tBackgroundFlush;
    
public:
    FileWAL(CacheType* ptrCache, const std::string& stWALFilePath)
        : m_ptrCache(ptrCache) 
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

        m_szWALBuffers[0] = static_cast<char*>(std::malloc(WAL_BUFFER_SIZE));
        m_szWALBuffers[1] = static_cast<char*>(std::malloc(WAL_BUFFER_SIZE));

        if (!m_szWALBuffers[0] || !m_szWALBuffers[1]) 
        {
            throw std::runtime_error("Failed to allocate WAL m_szWALBuffers");
        }

        m_hWALFile.open(m_stWALFile, std::ios::binary | std::ios::trunc);

        if (!m_hWALFile.is_open()) 
        {
            throw std::runtime_error("Failed to open WAL file");
        }

        m_tBackgroundFlush = std::thread(&FileWAL::flushLoop, this);
    }

    ~FileWAL() 
    {
        m_bStopFlushThread.store(true, std::memory_order_release);
        
        if (m_tBackgroundFlush.joinable())
        {
            m_tBackgroundFlush.join();
        }

        flushAllBuffers();
        
        std::free(m_szWALBuffers[0]);
        std::free(m_szWALBuffers[1]);
        
        m_hWALFile.close();
    }

    void append(uint8_t op, const KeyType& key, const ValueType& value)
    {
#ifndef __CONCURRENT__
        size_t nActivBufferIdx = m_nActiveBufferIndex.load(std::memory_order_acquire);
        size_t nActivBufferOffset = m_nWALBuffersOffset[nActivBufferIdx].load(std::memory_order_relaxed);
        size_t nEntrySize = sizeof(KeyType) + sizeof(ValueType);

        char* szEntry = static_cast<char*>(std::malloc(nEntrySize));
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

            //bool _b = (m_nWALSize + WAL_BUFFER_SIZE >= MAX_WAL_FILE_SIZE);

            size_t nNewActiveBufferIdx = 1 - nActivBufferIdx;
            m_nActiveBufferIndex.store(nNewActiveBufferIdx, std::memory_order_release);

            flushBufferSizes[nActivBufferIdx].store(nActivBufferOffset, std::memory_order_relaxed);
            m_nWALBuffersOffset[nActivBufferIdx].store(0, std::memory_order_relaxed);
            m_bFlushInPending.store(true, std::memory_order_release);

            nActivBufferIdx = nNewActiveBufferIdx;
            nActivBufferOffset = m_nWALBuffersOffset[nActivBufferIdx].load(std::memory_order_relaxed);

            //if (_b)
            //{
            //    while (m_bFlushInPending.load(std::memory_order_acquire))
            //    {
            //        std::this_thread::sleep_for(std::chrono::microseconds(1));
            //    }
            //}
        }

        if (nEntrySize - nBytesWritten > 0)
        {
            std::memcpy(m_szWALBuffers[nActivBufferIdx] + nActivBufferOffset, szEntry + nBytesWritten, nEntrySize - nBytesWritten);
            m_nWALBuffersOffset[nActivBufferIdx].fetch_add(nEntrySize - nBytesWritten, std::memory_order_release);
            //std::memcpy(m_szWALBuffers[nActivBufferIdx] + nActivBufferOffset, &key, sizeof(KeyType));
            //std::memcpy(m_szWALBuffers[nActivBufferIdx] + nActivBufferOffset + sizeof(KeyType), &value, sizeof(ValueType));
            //m_nWALBuffersOffset[nActivBufferIdx].fetch_add(nEntrySize, std::memory_order_release);
        }
#else //__CONCURRENT__
#endif //__CONCURRENT__

    }

    void flushLoop() 
    {
#ifndef __CONCURRENT__
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
#else //__CONCURRENT__
#endif //__CONCURRENT__
    }

    void flushBuffer(size_t nIdxBufferToFlush, size_t nBytesToWrite)
    {
#ifndef __CONCURRENT__
        if (nBytesToWrite == 0)
        {
            return;
        }

        m_hWALFile.write(m_szWALBuffers[nIdxBufferToFlush], nBytesToWrite);
        m_hWALFile.flush();
        m_nWALSize += nBytesToWrite;

        if (m_nWALSize >= MAX_WAL_FILE_SIZE) 
        {
            //m_ptrCache->persistAllItems();

            m_hWALFile.seekp(0);
            //m_hWALFile.seekg(0);

            m_nWALSize = 0;
        }
#else //__CONCURRENT__
#endif //__CONCURRENT__
    }

    void flushAllBuffers() 
    {
#ifndef __CONCURRENT__
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
#else //__CONCURRENT__
#endif //__CONCURRENT__
    }

	void runWALUnitTest() {
		const std::string m_hWALFile = "c:\\test_wal.bin";
#ifdef _MSC_VER
		// Clean up from previous run
		std::remove(m_hWALFile.c_str());
		std::vector<std::pair<KeyType, ValueType>> entries;

		{
			SelfType wal(m_ptrCache, m_hWALFile);

			/*for (int i = 0; i < 50000; ++i) {
				KeyType key = static_cast<KeyType>(i);
				ValueType value = static_cast<ValueType>(i + 1);

				entries.emplace_back(key, value);
				wal.append(0, key, value);
			}*/
		}
		//std::this_thread::sleep_for(std::chrono::milliseconds(000));

		// Read and verify

		FILE* fp = nullptr;
		fopen_s(&fp, m_hWALFile.c_str(), "rb");

		ASSERT(fp && "Failed to open WAL file");

		for (size_t i = 0; i < entries.size(); ++i) 
        {
			KeyType k;
			ValueType v;

			size_t readK = fread(&k, 1, sizeof(KeyType), fp);
			size_t readV = fread(&v, 1, sizeof(ValueType), fp);
			ASSERT(readK == sizeof(KeyType));
			ASSERT(readV == sizeof(ValueType));

			ASSERT(k == entries[i].first);
			ASSERT(v == entries[i].second);
		}

		ASSERT(fread(&entries[0].first, 1, 1, fp) == 0);  // Expect EOF
		std::fclose(fp);
		std::remove(m_hWALFile.c_str());

		std::cout << "WAL unit test passed for KeyType="
			<< typeid(KeyType).name()
			<< ", ValueType=" << typeid(ValueType).name() << "\n";
#endif //_MSC_VER
	}
};

