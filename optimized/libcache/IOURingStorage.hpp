#pragma once
#include <memory>
#include <iostream>
#include <fcntl.h>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <variant>
#include <cmath>
#include <vector>
#include <optional>
#include <unordered_map>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <mutex>
#include "CacheErrorCodes.h"
#include "SIMDBitmapAllocator.hpp"
#include <future>
#ifndef _MSC_VER
#include <sys/mman.h>
#include <unistd.h>
#include <liburing.h>
#endif //_MSC_VER


template <typename Traits>
class IOURingStorage
{
public:
	using ObjectType = typename Traits::ObjectType;
	using ObjectTypePtr = std::shared_ptr<ObjectType>;
	using ObjectUIDType = typename Traits::ObjectUIDType;

private:
	size_t m_nStorageSize;
	std::string m_stFilename;

#ifdef _MSC_VER
	std::fstream m_fsStorage;
	std::mutex m_mtxFile;
	std::unordered_map<size_t, bool> m_mpWritesInFlight;
#else //_MSC_VER
	int m_fdStorage;
	char* m_pMappedStorage;

	struct IOContext {
		enum class Type { Read, Write } type;
		char* buffer;
		size_t offset;
		bool finish;
	};

	struct io_uring m_ring;
	std::unordered_map<size_t, IOContext*> m_mpWritesInFlight;

#endif //_MSC_VER

	SIMDBitmapAllocator* m_ptrAllocator;

	struct WriteRequest
	{
		size_t offset;
		size_t size;
		char* buffer;
		bool bAlignedAllocation;
	};

	// Write buffers

	std::thread m_tBackgroundFlush;
	std::atomic<bool> m_bStopBackground{ false };

	std::mutex m_mtxWritesInFlight;

	std::vector<WriteRequest> m_vtWrites;

	std::condition_variable cv;

#ifdef __CONCURRENT__
	mutable std::shared_mutex m_mtxAllocator;
#endif //__CONCURRENT__

public:
	~IOURingStorage()
	{
		m_bStopBackground.store(true, std::memory_order_release);
		if (m_tBackgroundFlush.joinable()) {
			m_tBackgroundFlush.join();
		}

		delete m_ptrAllocator;

#ifdef _MSC_VER
		m_fsStorage.close();
#else //_MSC_VER
		io_uring_queue_exit(&m_ring);

		if (m_pMappedStorage && m_pMappedStorage != MAP_FAILED)
		{
			msync(m_pMappedStorage, m_nStorageSize, MS_SYNC);
			munmap(m_pMappedStorage, m_nStorageSize);
		}

		close(m_fdStorage);
#endif //_MSC_VER
	}

	IOURingStorage(uint32_t nBlockSize, uint64_t nStorageSize, const std::string& stFilename)
		: m_stFilename(stFilename)
		, m_nStorageSize(nStorageSize)
	{
#ifdef _MSC_VER
		//m_fsStorage.rdbuf()->pubsetbuf(0, 0);
		m_fsStorage.open(stFilename.c_str(), std::ios::out | std::ios::binary);
		m_fsStorage.close();

		m_fsStorage.open(stFilename.c_str(), std::ios::out | std::ios::binary | std::ios::in);
		m_fsStorage.seekp(0);
		m_fsStorage.seekg(0);

		if (!m_fsStorage.is_open())
		{
			throw new std::logic_error("Failed to open file as a storage.");
		}
#else //_MSC_VER
		m_fdStorage = open(m_stFilename.c_str(), O_RDWR | O_CREAT, 0644);
		if (m_fdStorage < 0)
		{
			throw std::logic_error("Failed to open file as storage");
		}

		if (ftruncate(m_fdStorage, m_nStorageSize) < 0)
		{
			close(m_fdStorage);
			throw std::logic_error("Failed to set storage size");
		}

		m_pMappedStorage = static_cast<char*>(mmap(nullptr, m_nStorageSize, PROT_READ | PROT_WRITE, MAP_SHARED, m_fdStorage, 0));
		if (m_pMappedStorage == MAP_FAILED)
		{
			close(m_fdStorage);
			throw std::logic_error("Failed to mmap storage");
		}

		if (io_uring_queue_init(256, &m_ring, 0) < 0) 
		{
			throw std::logic_error("io_uring_queue_init failed");
		}
#endif //_MSC_VER

		//m_mpWritesInFlight.reserve(9024); // Adjust size based on expected entries

		m_ptrAllocator = new SIMDBitmapAllocator(nBlockSize, nStorageSize);

		m_tBackgroundFlush = std::thread(&IOURingStorage::backgroundFlushLoop, this);
	}

public:
	template <typename... InitArgs>
	CacheErrorCode init(InitArgs... args)
	{
		return CacheErrorCode::Success;
	}

	CacheErrorCode remove(const ObjectUIDType& uidObject)
	{
		{
#ifdef __CONCURRENT__
			std::unique_lock<std::shared_mutex> lock(m_mtxAllocator);
#endif //__CONCURRENT__

			m_ptrAllocator->free(uidObject.getPersistentPointerValue(), uidObject.getPersistentObjectSize());
		}

		return CacheErrorCode::Success;
	}

	CacheErrorCode getObject(uint16_t nDegree, const ObjectUIDType& uidObject, std::shared_ptr<ObjectType>& ptrObject)
	{
		uint32_t nSize = uidObject.getPersistentObjectSize();
		size_t nOffset = uidObject.getPersistentPointerValue();

		char* szBuffer = new char[nSize + 1];
		memset(szBuffer, 0, nSize + 1);

#ifdef _MSC_VER
		{
			std::unique_lock<std::mutex> lock(m_mtxWritesInFlight);

			// TODO: Locate the in-fligh bytes and return?
			cv.wait(lock, [&] {
				auto it = m_mpWritesInFlight.find(nOffset);
				return (it == m_mpWritesInFlight.end()) || it->second;
				});
		}

		{
			std::unique_lock<std::mutex> lock(m_mtxFile);

			m_fsStorage.seekg(nOffset);
			m_fsStorage.read(szBuffer, nSize);
		}
#else //_MSC_VER
#ifdef __CONCURRENT__
		{
			std::unique_lock<std::mutex> lock(m_mtxWritesInFlight);

			// TODO: Locate the in-fligh bytes and return?
			cv.wait(lock, [&] {
				auto it = m_mpWritesInFlight.find(nOffset);
				if ((it == m_mpWritesInFlight.end()) /*|| it->second == nullptr*/) {
					//std::cout << "q read" << std::endl;

				std::cout << "r1: " << nOffset << "," << nSize << std::endl;
					auto* ctx = new IOContext{IOContext::Type::Read, szBuffer, nOffset, false};

					m_mpWritesInFlight[nOffset] = ctx;

					io_uring_sqe* sqe = io_uring_get_sqe(&m_ring);

					if (!sqe) throw std::runtime_error("Failed to get SQE for read");
					
					io_uring_prep_read(sqe, m_fdStorage, szBuffer, nSize, nOffset);

					sqe->user_data = reinterpret_cast<uint64_t>(ctx);
					
					if(io_uring_submit(&m_ring) < 0)
					{
						throw std::runtime_error("...");
					}

					return false;
				} else {
				//std::cout << "r11: " << nOffset << "," << nSize <<std::endl;
					if(it->second->type == IOContext::Type::Write)
					{
						std::cout << "r11: " << nOffset << "," << nSize <<std::endl;
					//std::cout << "already in queue - return bytes" << std::endl;
					memcpy(szBuffer, it->second->buffer, nSize);
					return true;
					}
					else if(it->second->finish){
						std::cout << "r12: " << nOffset << "," << nSize <<std::endl;
					//std::cout << "job done - read" << std::endl;
					delete it->second;
						m_mpWritesInFlight.erase(it);
						return true;
					}
					//std::cout << "job pending - read" << std::endl;
					return false;
				}

			});
		}
#else //__CONCURRENT__
		{
			std::unique_lock<std::mutex> lock(m_mtxWritesInFlight);
			std::cout << nOffset << "," << nSize <<  std::endl;
			// TODO: Locate the in-fligh bytes and return?
			cv.wait(lock, [&] {
				auto it = m_mpWritesInFlight.find(nOffset);
				return (it == m_mpWritesInFlight.end()) || it->second == nullptr;
				});
		}

		memcpy(szBuffer, m_pMappedStorage + nOffset, nSize);
#endif //__CONCURRENT__
#endif //_MSC_VER


		ptrObject->updateCoreObject(nDegree, szBuffer, uidObject, uidObject.getPersistentObjectSize(), m_ptrAllocator->m_nBlockSize);
		//ptrObject->updateCoreObject(nDegree, szBuffer, uidObject, uidObject.getPersistentObjectSize(), (uint16_t)m_nBlockSize);

		delete[] szBuffer;

		return CacheErrorCode::Success;
	}

	std::shared_ptr<ObjectType> getObject(uint16_t nDegree, const ObjectUIDType& uidObject)
	{
		uint32_t nSize = uidObject.getPersistentObjectSize();
		size_t nOffset = uidObject.getPersistentPointerValue();

		char* szBuffer = new char[nSize + 1];
		memset(szBuffer, 0, nSize + 1);

#ifdef _MSC_VER
		{
			std::unique_lock<std::mutex> lock(m_mtxWritesInFlight);

			// TODO: Locate the in-fligh bytes and return?
			cv.wait(lock, [&] {
				auto it = m_mpWritesInFlight.find(nOffset);
				return (it == m_mpWritesInFlight.end()) || it->second;
				});
		}

		{
			std::unique_lock<std::mutex> lock(m_mtxFile);

			m_fsStorage.seekg(nOffset);
			m_fsStorage.read(szBuffer, nSize);
		}
#else //_MSC_VER
#ifdef __CONCURRENT__
		{
			std::unique_lock<std::mutex> lock(m_mtxWritesInFlight);

			// TODO: Locate the in-fligh bytes and return?
			cv.wait(lock, [&] {
				auto it = m_mpWritesInFlight.find(nOffset);
				if ((it == m_mpWritesInFlight.end()) || it->second == nullptr) {
					//std::cout << "q read" << std::endl;

				std::cout << "r0: " << nOffset << "," << nSize << std::endl;
					auto* ctx = new IOContext{IOContext::Type::Read, szBuffer, nOffset, false};

					m_mpWritesInFlight[nOffset] = ctx;

					io_uring_sqe* sqe = io_uring_get_sqe(&m_ring);

					if (!sqe) throw std::runtime_error("Failed to get SQE for read");
					
					io_uring_prep_read(sqe, m_fdStorage, szBuffer, nSize, nOffset);

					sqe->user_data = reinterpret_cast<uint64_t>(ctx);
					
					if(io_uring_submit(&m_ring) < 0)
					{
						throw std::runtime_error("...");
					}

					return false;
				} else {
					//std::cout << "r00: " << nOffset << "," << nSize << ".";
					if(it->second->type == IOContext::Type::Write)
					{
						//std::cout << "r00: " << nOffset << "," << nSize <<std::endl;
					//std::cout << "already in queue - return bytes" << std::endl;
					memcpy(szBuffer, it->second->buffer, nSize);
					return true;
					}
					else if(it->second->finish){
						std::cout << "r01: " << nOffset << "," << nSize <<std::endl;
					//std::cout << "job done - read" << std::endl;
						m_mpWritesInFlight.erase(it);
						return true;
					}
					//std::cout << "job pending - read" << std::endl;
					return false;
				}

			});
		}
#else //__CONCURRENT__
		{
			std::unique_lock<std::mutex> lock(m_mtxWritesInFlight);

			// TODO: Locate the in-fligh bytes and return?
			cv.wait(lock, [&] {
				auto it = m_mpWritesInFlight.find(nOffset);
				return (it == m_mpWritesInFlight.end()) || it->second == nullptr;
				});
		}

		memcpy(szBuffer, m_pMappedStorage + nOffset, nSize);
#endif //__CONCURRENT__
#endif //_MSC_VER

		std::shared_ptr<ObjectType> ptrObject = std::make_shared<ObjectType>(nDegree, uidObject, szBuffer, uidObject.getPersistentObjectSize(), m_ptrAllocator->m_nBlockSize);
		
		//std::shared_ptr<ObjectType> ptrObject = std::make_shared<ObjectType>(nDegree, uidObject, szBuffer, uidObject.getPersistentObjectSize(), (uint16_t)m_nBlockSize);

		delete[] szBuffer;

		return ptrObject;
	}

	inline size_t nextPowerOf2(size_t nNumber)
	{
		if (nNumber <= 1)
		{
			return 1;
		}

#if defined(_MSC_VER)
		unsigned long index;
		_BitScanReverse64(&index, nNumber - 1);
		return 1ULL << (index + 1);
#elif defined(__GNUC__) || defined(__clang__)
		return 1ULL << (64 - __builtin_clzl(nNumber - 1));
#else
		// Fallback to bit-smearing
		--nNumber;
		nNumber |= nNumber >> 1;
		nNumber |= nNumber >> 2;
		nNumber |= nNumber >> 4;
		nNumber |= nNumber >> 8;
		nNumber |= nNumber >> 16;
		nNumber |= nNumber >> 32;
		return nNumber + 1;
#endif
	}

	CacheErrorCode addObject(ObjectUIDType uidObject, std::shared_ptr<ObjectType> ptrObject, ObjectUIDType& uidUpdated)
	{
		uint32_t nBufferSize = 0;
		bool bAlignedAllocation = false;

		char* szBuffer = nullptr;
		void* ptrOffset = nullptr;

		ptrObject->serialize(szBuffer, nBufferSize, m_ptrAllocator->m_nBlockSize, ptrOffset, bAlignedAllocation);

		//ptrObject->serialize(szBuffer, nBufferSize, (uint16_t)m_nBlockSize, ptrOffset, bAlignedAllocation);


		if (ptrOffset == nullptr)
		{
			//size_t nBlocks = (nBufferSize + m_nBlockSize - 1) / m_nBlockSize;
			//size_t nBlocksRequired = nextPowerOf2(nBlocks);

			std::optional<size_t> oResult = std::nullopt;

			{
#ifdef __CONCURRENT__
				std::unique_lock<std::shared_mutex> lock(m_mtxAllocator);
#endif //__CONCURRENT__

				oResult = m_ptrAllocator->allocate(nBufferSize);
			}

			if (!oResult)
			{
				if (!bAlignedAllocation)
				{
					delete[] szBuffer;
				}
				else
				{
#ifdef _MSC_VER
					_aligned_free(szBuffer);
#else
					free(szBuffer);
#endif
				}
				return CacheErrorCode::OutOfStorage;
			}

			size_t nOffset = *oResult;

			//ssize_t ret = pwrite(m_fdStorage, szBuffer, nBufferSize, nOffset);
			//std::cout << "pwrite returned: " << ret << std::endl;


			//ASSERT(nOffset + nBufferSize <= m_nStorageSize);

			{
				std::unique_lock<std::mutex> lock(m_mtxWritesInFlight);

				auto it = m_mpWritesInFlight.find(nOffset);
				if (it != m_mpWritesInFlight.end())
				{
					//m_mpWritesInFlight[nOffset] = false;
					throw std::runtime_error("Write request for same offset!");
				}
				std::cout << "pw: " << nOffset << "," << nBufferSize << std::endl;
				auto* ctx = new IOContext{IOContext::Type::Write, szBuffer, nOffset, false};

				m_mpWritesInFlight[nOffset] = ctx;

				auto sqe = io_uring_get_sqe(&m_ring);

				if (!sqe) throw std::runtime_error("Failed to get SQE for read");

				io_uring_prep_write(sqe, m_fdStorage, szBuffer, nBufferSize, nOffset);
					
				sqe->user_data = reinterpret_cast<uint64_t>(ctx);

struct stat st;
fstat(m_fdStorage, &st);
std::cout << "file type: " << (st.st_mode & S_IFMT) << ", size: " << st.st_size << std::endl;


				if(io_uring_submit(&m_ring) < 0)
				{
					throw std::runtime_error("...");
				}


				std::cout << "fd=" << sqe->fd
          << ", offset=" << sqe->off
          << ", addr=" << reinterpret_cast<void*>(sqe->addr)
          << ", len=" << sqe->len << std::endl;

				//WriteRequest oRequest{ nOffset, nBufferSize, szBuffer, bAlignedAllocation };
				//m_vtWrites.emplace_back(std::move(oRequest));

			}

			//ObjectUIDType::createAddressFromFileOffset(uidUpdated, uidObject.getObjectType(), nOffset, nBufferSize);
			ObjectUIDType::createAddressFromFileOffset(uidUpdated, uidObject.getObjectType(), nOffset, nBufferSize);

			if (uidObject.isPersistedObject())
			{
				m_ptrAllocator->free(uidObject.getPersistentPointerValue(), uidObject.getPersistentObjectSize());
			}
		}
		else
		{
			uidUpdated = uidObject;

			throw new std::logic_error("unimplemented!.");

			//memcpy(ptrOffset, szBuffer, nBufferSize);

#ifdef __ENABLE_ASSERTS__
			//ObjectTypePtr ptrTmp = std::make_shared<ObjectType>(3, uidObject, GetStoragePtr(uidObject.getPersistentPointerValue()), uidObject.getPersistentObjectSize() + nBufferSize, m_nBlockSize);
			//ptrObject->validate(ptrTmp);
#endif //__ENABLE_ASSERTS__

			if (!bAlignedAllocation)
			{
				delete[] szBuffer;
			}
			else
			{
#ifdef _MSC_VER
				_aligned_free(szBuffer);
#else
				free(szBuffer);
#endif
			}

		}
		return CacheErrorCode::Success;
	}

	void backgroundFlushLoop()
	{
		while (!m_bStopBackground.load(std::memory_order_acquire))
		{
			{
				std::unique_lock<std::mutex> lock(m_mtxWritesInFlight);

				// if (m_vtWrites.size() == 0)
				// {
				// 	lock.unlock();
				// 	std::this_thread::sleep_for(std::chrono::microseconds(1000));
				// 	continue;
				// }

#ifdef _MSC_VER
				std::unique_lock<std::mutex> _lock(m_mtxFile);

				for (auto& b : m_vtWrites)
				{
					m_fsStorage.seekp(b.offset);
					m_fsStorage.write(b.buffer, b.size);

					if (!b.bAlignedAllocation)
					{
						delete[] b.buffer;
					}
					else
					{
						//#ifdef _MSC_VER
						_aligned_free(b.buffer);
						//#else
						//						free(b.buffer);
						//#endif
					}

					m_mpWritesInFlight[b.offset] = true;
				}

				m_vtWrites.clear();
#else //_MSC_VER
				//std::unique_lock<std::mutex> _lock(m_mtxFile);

#ifdef __CONCURRENT__
				io_uring_cqe* cqe;

				while (io_uring_peek_cqe(&m_ring, &cqe) == 0)
				{
					std::cout << "Write " << cqe->res << " bytes into buffer" << std::endl;

					IOContext* ctx = reinterpret_cast<IOContext*>(cqe->user_data);
					if (ctx->type == IOContext::Type::Write) {
						delete[] ctx->buffer;
						ctx->buffer = nullptr;

						m_mpWritesInFlight.erase(ctx->offset);

std::cout << "w: " << ctx->offset << std::endl;
delete ctx;
ctx = nullptr;

					}
					else if (ctx->type == IOContext::Type::Read) {
						std::cout << "Read " << cqe->res << " bytes into buffer" << std::endl;
						ctx->finish = true;
					}

					//delete ctx;
					io_uring_cqe_seen(&m_ring, cqe);
				}

#else //__CONCURRENT__

				io_uring_cqe* cqe;

				while (io_uring_peek_cqe(&m_ring, &cqe) == 0)
				{
					IOContext* ctx = reinterpret_cast<IOContext*>(cqe->user_data);
					if (ctx->type == IOContext::Type::Write) {
						delete[] ctx->buffer;
						m_mpWritesInFlight.erase(ctx->offset);
						std::cout << "w: " << ctx->offset <<std::endl;

					}
					else if (ctx->type == IOContext::Type::Read) {
						throw new std::logic_error("unimplemented!.");
					}

					delete ctx;
					io_uring_cqe_seen(&m_ring, cqe);
				}
#endif //__CONCURRENT__

				
#endif //_MSC_VER

				cv.notify_all();

				//m_mpWritesInFlight.clear();				
			}
			std::this_thread::sleep_for(std::chrono::microseconds(10));
		}
	}

};
