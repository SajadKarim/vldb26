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
#include <immintrin.h> // SIMD intrinsics
#include "CacheErrorCodes.h"
#include "validityasserts.h"
#include <iostream>
#include <bitset>
#include <deque>

#define __MAX_BRIEF_LOOKUP_VECTOR_DEPTH__ 10
#define __MAX_SLAB_Q_DEPTH__ 50
#define __WORDS_PER_BATCH__ 4
#define __BLOCKS_PER_WORD__ 64
#define __BITS_PER_BATCH__ __WORDS_PER_BATCH__*__BLOCKS_PER_WORD__
#define ALL_ONES_256 _mm256_set1_epi32(-1)


#ifdef _MSC_VER
#include <intrin.h>
#endif
#include <nmmintrin.h>  
class SIMDBitmapAllocator
{
private:
    struct FreeSlab
    {
        uint32_t m_nSlabSize;             // Size of block
        uint32_t m_nSlabsCount;           // Number of such blocks available
        uint32_t m_nMinOffset;            // Minimum offset among these blocks
        std::deque<uint64_t> m_qOffsets;  // A brief queue
    };

public:
    uint32_t m_nNextBlock;
    uint32_t m_nBlockSize;
private:
    uint32_t m_nTotalBlocks;
    uint32_t m_nTotalWords;
    uint64_t m_nStorageSize;

    std::vector<uint64_t> m_vtBitmap;
    std::vector<FreeSlab> m_vtFreeSlabs;
    uint32_t m_nAvailableBlocks;

public:
    ~SIMDBitmapAllocator()
    {
#ifdef __ENABLE_ASSERTS__
        uint32_t nTotalBlocks = 0;
        for (auto nWord : m_vtBitmap) 
        {
            nTotalBlocks += _mm_popcnt_u64(~nWord);
        }

        /*std::cout << "Next offset (default iterator): " << m_nNextBlock << std::endl;
        std::cout << "Available blocks (class' counter): " << m_nAvailableBlocks << std::endl;
        std::cout << "Available blocks: " << nTotalBlocks << ", Max: " << m_nTotalBlocks << std::endl;

        for (auto it = m_vtFreeSlabs.begin(); it != m_vtFreeSlabs.end(); it++)
        {
            std::cout << "Free Slabs: Size = " << it->m_nSlabSize << ", Count = " << it->m_nSlabsCount << ", Min Offset = " << it->m_nMinOffset<< std::endl;
        }*/
#endif
    }

    SIMDBitmapAllocator(uint32_t nBlockSize, uint64_t nStorageSize)
        : m_nNextBlock(0)
        , m_nBlockSize(nBlockSize)
        , m_nStorageSize(nStorageSize)
    {
        m_nTotalBlocks = m_nStorageSize / m_nBlockSize;
        m_nTotalWords = m_nTotalBlocks / __BLOCKS_PER_WORD__;

        m_vtBitmap.resize(m_nTotalWords, 0);

        m_vtFreeSlabs.reserve(__MAX_BRIEF_LOOKUP_VECTOR_DEPTH__);

        m_nAvailableBlocks = m_nTotalBlocks;
    }

    std::optional<uint64_t> allocate(uint32_t nBytes)
    {
        uint8_t nRequiredBlocks = nextPowerOf2((nBytes + m_nBlockSize - 1) / m_nBlockSize);

        ASSERT(nRequiredBlocks > 0 && nRequiredBlocks < __BLOCKS_PER_WORD__); // TODO: Uncomment this and the other related checks if a logical block can consist of 64 or more blocks.

        if (m_nNextBlock + nRequiredBlocks < m_nTotalBlocks)
        {
            auto oResult = tryFindBlock(nRequiredBlocks, m_nNextBlock);
            if (oResult)
            {
                m_nAvailableBlocks -= nRequiredBlocks;
                ASSERT(m_nAvailableBlocks >= 0 || m_nAvailableBlocks <= m_nTotalBlocks);

                m_nNextBlock = (*oResult + nRequiredBlocks);
                return *oResult * m_nBlockSize;
            }
        }

        // Look through buckets to find a suitable block
        for (auto it = m_vtFreeSlabs.begin(); it != m_vtFreeSlabs.end(); )
        {
            if (it->m_nSlabSize >= nRequiredBlocks && it->m_nSlabsCount > 0)
            {
                uint32_t nOffset = it->m_nMinOffset;

                if (it->m_qOffsets.size() > 0)
                {
                    nOffset = it->m_qOffsets.front();
                    it->m_qOffsets.pop_front();
                }

                auto oResult = tryFindBlock(nRequiredBlocks, nOffset);
                if (oResult)
                {
                    m_nAvailableBlocks -= nRequiredBlocks;
                    ASSERT(m_nAvailableBlocks >= 0 || m_nAvailableBlocks <= m_nTotalBlocks);

                    uint32_t nRemainingBlocks = it->m_nSlabSize - nRequiredBlocks;
                    uint32_t nNewOffset = nOffset + nRequiredBlocks;

                    it->m_nSlabsCount--;

                    if (it->m_nSlabsCount== 0)
                    {
                        ASSERT(it->m_qOffsets.size() == 0);
                        it = m_vtFreeSlabs.erase(it);
                    }
                    else
                    {
                        it->m_nMinOffset = std::min(it->m_nMinOffset, nNewOffset);
                    }

                    // Add leftover portion back to buckets if any
                    if (nRemainingBlocks > 0)
                    {
                        insertOrMergeSlab(nRemainingBlocks, 1, nOffset);
                    }

                    return *oResult * m_nBlockSize;
                }
                else
                {
                    it = m_vtFreeSlabs.erase(it);
                }
            }
            else
            {
                ++it;
            }
        }

        uint32_t nMinOffset = m_nTotalBlocks;
        for (auto it = m_vtFreeSlabs.begin(); it != m_vtFreeSlabs.end(); it++) 
        {
            nMinOffset = std::min(it->m_nMinOffset, nMinOffset);
        }

        // TODO: make an overload that along side the allocation should also keep populating the list of available blocks! do profiling first!!
        auto oResult = tryFindBlock(nRequiredBlocks, nMinOffset);
        if (oResult)
        {
            m_nAvailableBlocks -= nRequiredBlocks;
            ASSERT(m_nAvailableBlocks >= 0 || m_nAvailableBlocks <= m_nTotalBlocks);

            return *oResult * m_nBlockSize;
        }

        return std::nullopt;
    }

    bool free(uint64_t nOffset, uint32_t nBytes)
    {
        uint64_t nBlockOffset = nOffset / m_nBlockSize;
        uint8_t nBlocksCount = nextPowerOf2((nBytes + m_nBlockSize - 1) / m_nBlockSize);

        ASSERT(nBlockOffset < m_nTotalBlocks);
        ASSERT(nBlocksCount > 0 && nBlocksCount < __BLOCKS_PER_WORD__); // TODO: Uncomment this and the other related checks if a logical block can consist of 64 or more blocks.

        bool b = reclaimBitmap(nBlockOffset, nBlocksCount);
        insertOrMergeSlab(nBlocksCount, 1, nBlockOffset);
        return b;
    }

private:
    inline uint8_t nextPowerOf2(uint32_t nNumber)
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

    bool reclaimBitmap(uint64_t nBlockOffset, uint8_t nBlocksCount)
    {
        m_nAvailableBlocks += nBlocksCount;
        ASSERT(m_nAvailableBlocks >= 0 || m_nAvailableBlocks <= m_nTotalBlocks);

        while (nBlocksCount > 0)
        {
            uint64_t nWordIdx = nBlockOffset / __BLOCKS_PER_WORD__;
            uint8_t nBitOffset = nBlockOffset % __BLOCKS_PER_WORD__;
            uint8_t nTrailingBitsCount = __BLOCKS_PER_WORD__ - nBitOffset;
            uint8_t nBitsToReset = std::min(nBlocksCount, nTrailingBitsCount);

            uint64_t _MASK;

            // TODO: Uncomment the following and remove checks if a logical block can consist of 64 or more blocks.
            /*
            if (nBitsToReset == 64)
            {
                _mask = ~0ULL;
            }
            else
            {
                _mask = ((1ULL << nBitsToReset) - 1) << nBitOffset;
            }
            */
            _MASK = ((1ULL << nBitsToReset) - 1) << nBitOffset;

            //if ((m_vtBitmap[nWordIdx] & _MASK) != _MASK)
            //    return false;

            ASSERT((m_vtBitmap[nWordIdx] & _MASK) == _MASK);

            m_vtBitmap[nWordIdx] &= ~_MASK;

            nBlockOffset += nBitsToReset;
            nBlocksCount -= nBitsToReset;
        }
        return true;
    }

    void insertOrMergeSlab(uint32_t nSlabSize, uint32_t nCount, uint32_t nOffset)
    {
        auto it = std::lower_bound(m_vtFreeSlabs.begin(), m_vtFreeSlabs.end(), nSlabSize,
            [](const FreeSlab& bucket, uint32_t size) { return bucket.m_nSlabSize < size; });

        if (it != m_vtFreeSlabs.end() && it->m_nSlabSize == nSlabSize)
        {
            it->m_nSlabsCount += nCount;
            it->m_nMinOffset = std::min(it->m_nMinOffset, nOffset);
            if (it->m_qOffsets.size() < __MAX_SLAB_Q_DEPTH__)
            {
                it->m_qOffsets.push_back(nOffset);
            }
        }
        else
        {
            if (m_vtFreeSlabs.size() < __MAX_BRIEF_LOOKUP_VECTOR_DEPTH__)
            {
                m_vtFreeSlabs.insert(it, { nSlabSize, nCount, nOffset, {} });
            }
        }
    }

    std::optional<uint64_t> tryFindBlock(uint64_t nRequiredBlocks, uint32_t nOffsetHint = 0)
    {
        ASSERT(nOffsetHint < m_nTotalBlocks);
        ASSERT(nRequiredBlocks > 0 && nRequiredBlocks < __BLOCKS_PER_WORD__); // TODO: Uncomment this and the other related checks if a logical block can consist of 64 or more blocks.

        uint64_t nBitIdx = (nOffsetHint / __BLOCKS_PER_WORD__) * __BLOCKS_PER_WORD__; // align DOWN to word boundary

        for (; nBitIdx + __BITS_PER_BATCH__ < m_nTotalBlocks; nBitIdx += __BITS_PER_BATCH__)
        {
            uint64_t nWordIdx = nBitIdx / __BLOCKS_PER_WORD__;

            if (nWordIdx + __WORDS_PER_BATCH__ > m_vtBitmap.size())
            {
                break; // not enough words for a full batch
            }

            // Load 4 consecutive words
            __m256i vtBits = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&m_vtBitmap[nWordIdx]));

            // If all zero => entirely free 256-bit block
            if (_mm256_testz_si256(vtBits, vtBits))
            {
                // Currently this section does not account for block sizes that exceed the batch's capacity.
                m_vtBitmap[nWordIdx] |= ((1ULL << nRequiredBlocks) - 1ULL);
                return nBitIdx;
            }

            // TODO: Overhead for initial blind run!
            if (_mm256_testc_si256(vtBits, ALL_ONES_256))
            {
                continue; // All bits are ones — do your processing here
            }

            // Some bits used in this 256-bit block: inspect per-word
            //alignas(32) uint64_t nFrames[4];
            //_mm256_store_si256(reinterpret_cast<__m256i*>(nFrames), vtBits);

            for (size_t i = 0; i < __WORDS_PER_BATCH__; ++i)
            {
                uint64_t _nWordIdx = nWordIdx + i;
                uint64_t nInvertedWord = ~m_vtBitmap[_nWordIdx];

                bool _lastBit = nInvertedWord << __BLOCKS_PER_WORD__ - 1;

                uint8_t nFreeBitOffset = _tzcnt_u64(nInvertedWord);

                while (nInvertedWord)
                {
                    // Try fitting entirely within this word
                    if (nFreeBitOffset + nRequiredBlocks <= 64)
                    {
                        uint64_t _MASK = ((1ULL << nRequiredBlocks) - 1) << nFreeBitOffset;

                        // Check actual bitmap word for freedom
                        if ((m_vtBitmap[_nWordIdx] & _MASK) == 0ULL)
                        {
                            m_vtBitmap[_nWordIdx] |= _MASK;
                            return nBitIdx + i * __BLOCKS_PER_WORD__ + nFreeBitOffset;
                        }
                    }
                    // Advance nInvertedWord: clear least-significant 1-bit so we don't revisit it
                    nInvertedWord &= (nInvertedWord - 1);

                    nFreeBitOffset = _tzcnt_u64(nInvertedWord);
                }

                size_t _nNextWordIdx = _nWordIdx + 1;
                if (!_lastBit && _nNextWordIdx < m_nTotalBlocks)
                {
                    // Try spanning into next word
                    uint64_t _word_b = m_vtBitmap[_nNextWordIdx];        // Not inverted
                    uint8_t _word_b_nFreeBitRun = _tzcnt_u64(_word_b);
                    uint8_t _word_b_nFreeBitOffset = _tzcnt_u64(~_word_b);

                    size_t _nBitsAvailableInCurrentWork = __BLOCKS_PER_WORD__ - nFreeBitOffset;
                    size_t _nBitsGaps = nRequiredBlocks - _nBitsAvailableInCurrentWork;

                    if (_word_b_nFreeBitOffset == 0 && nRequiredBlocks <= _nBitsAvailableInCurrentWork + _word_b_nFreeBitRun)
                    {
                        uint64_t _mask = ((1ULL << _nBitsAvailableInCurrentWork) - 1) << nFreeBitOffset;

                        ASSERT((m_vtBitmap[_nWordIdx] & _mask) == 0ULL);

                        m_vtBitmap[_nWordIdx] |= _mask;

                        _mask = ((1ULL << _nBitsGaps) - 1);

                        ASSERT((m_vtBitmap[_nNextWordIdx] & _mask) == 0ULL);

                            m_vtBitmap[_nNextWordIdx] |= _mask;

                        return _nWordIdx * __BLOCKS_PER_WORD__ + nFreeBitOffset;;
                    }
                }
            }
        }

        for (size_t i = nOffsetHint; i < m_nTotalBlocks; i = i + __BLOCKS_PER_WORD__)
        {
            uint64_t _nWordIdx = i / __BLOCKS_PER_WORD__;
            uint64_t nInvertedWord = ~m_vtBitmap[_nWordIdx]; // 1s where bitmap is 0 (free)

            bool _lastBit = nInvertedWord << 63;
            uint8_t nFreeBitOffset = _tzcnt_u64(nInvertedWord);

            while (nInvertedWord)
            {
                // Try fitting entirely within this word
                if (nFreeBitOffset + nRequiredBlocks < __BLOCKS_PER_WORD__)
                {
                    uint64_t _mask = ((1ULL << nRequiredBlocks) - 1ULL) << nFreeBitOffset;

                    // Check actual bitmap word for freedom
                    if ((m_vtBitmap[_nWordIdx] & _mask) == 0ULL)
                    {
                        m_vtBitmap[_nWordIdx] |= _mask;
                        return _nWordIdx * __BLOCKS_PER_WORD__ + nFreeBitOffset;
                    }
                }
                // Advance nInvertedWord: clear least-significant 1-bit so we don't revisit it
                nInvertedWord &= (nInvertedWord - 1);

                nFreeBitOffset = _tzcnt_u64(nInvertedWord);
            }

            size_t _nNextWordIdx = _nWordIdx + 1;
            if (!_lastBit && _nNextWordIdx < m_nTotalWords)
            {
                // Try spanning into next word
                uint64_t _word_b = m_vtBitmap[_nNextWordIdx];        // Not inverted
                uint8_t _word_b_nFreeBitRun = _tzcnt_u64(_word_b);
                uint8_t _word_b_nFreeBitOffset = _tzcnt_u64(~_word_b);

                size_t _nBitsAvailableInCurrentWork = __BLOCKS_PER_WORD__ - nFreeBitOffset;
                size_t _nBitsGaps = nRequiredBlocks - _nBitsAvailableInCurrentWork;

                if (_word_b_nFreeBitOffset == 0 && nRequiredBlocks <= _nBitsAvailableInCurrentWork + _word_b_nFreeBitRun)
                {
                    uint64_t _mask = ((1ULL << _nBitsAvailableInCurrentWork) - 1) << nFreeBitOffset;

                    ASSERT((m_vtBitmap[_nWordIdx] & _mask) == 0ULL);

                    m_vtBitmap[_nWordIdx] |= _mask;

                    _mask = ((1ULL << _nBitsGaps) - 1);

                    ASSERT((m_vtBitmap[_nNextWordIdx] & _mask) == 0ULL);

                        m_vtBitmap[_nNextWordIdx] |= _mask;

                    return _nWordIdx * __BLOCKS_PER_WORD__ + nFreeBitOffset;;
                }
            }
        }

        return std::nullopt;
    }
};