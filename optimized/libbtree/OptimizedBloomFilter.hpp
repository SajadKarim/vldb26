#pragma once
#include <vector>
#include <cmath>
#include <functional>
#include <algorithm>
#include <immintrin.h>  // For AVX2/SSE SIMD operations
#include <nmmintrin.h>  // For _mm_popcnt_u64

// High-performance Bloom Filter using SIMD operations and packed uint64_t words
template<typename KeyType, size_t FilterBits = 1024>
class BloomFilter 
{
private:
    static constexpr size_t BITS_PER_WORD = 64;
    static constexpr size_t NUM_WORDS = (FilterBits + BITS_PER_WORD - 1) / BITS_PER_WORD;
    static constexpr size_t NUM_HASH_FUNCTIONS = 3;  // Optimal balance for buffer use case
    
    // Ensure alignment for SIMD operations (32-byte aligned for AVX2)
    alignas(32) std::vector<uint64_t> m_words;

    // Hash function 1: std::hash with bit mixing
    inline size_t hash1(const KeyType& key) const {
        size_t h = std::hash<KeyType>{}(key);
        // Additional bit mixing for better distribution
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccdULL;
        h ^= h >> 33;
        return h % FilterBits;
    }

    // Hash function 2: multiplicative hash with different constant
    inline size_t hash2(const KeyType& key) const {
        size_t h = std::hash<KeyType>{}(key);
        h *= 0xc4ceb9fe1a85ec53ULL;
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccdULL;
        return h % FilterBits;
    }

    // Hash function 3: XOR-based hash with bit rotation
    inline size_t hash3(const KeyType& key) const {
        size_t h = std::hash<KeyType>{}(key);
        h ^= (h >> 16);
        h *= 0x85ebca6b;
        h ^= (h >> 13);
        h *= 0xc2b2ae35;
        h ^= (h >> 16);
        return h % FilterBits;
    }

    // Set bit at given position using optimized bit manipulation
    inline void setBit(size_t bitPos) {
        size_t wordIdx = bitPos >> 6;  // Faster division by 64
        size_t bitOffset = bitPos & 63;  // Faster modulo 64
        m_words[wordIdx] |= (1ULL << bitOffset);
    }

    // Check if bit is set at given position using optimized bit manipulation
    inline bool getBit(size_t bitPos) const {
        size_t wordIdx = bitPos >> 6;  // Faster division by 64
        size_t bitOffset = bitPos & 63;  // Faster modulo 64
        return (m_words[wordIdx] & (1ULL << bitOffset)) != 0;
    }

    // SIMD-optimized bulk bit setting for multiple positions
    inline void setBitsSimd(size_t pos1, size_t pos2, size_t pos3) {
        // Group operations by word index for better cache performance
        size_t word1 = pos1 >> 6, word2 = pos2 >> 6, word3 = pos3 >> 6;
        size_t bit1 = pos1 & 63, bit2 = pos2 & 63, bit3 = pos3 & 63;

        if (word1 == word2 && word2 == word3) {
            // All bits in same word - single atomic operation
            uint64_t mask = (1ULL << bit1) | (1ULL << bit2) | (1ULL << bit3);
            m_words[word1] |= mask;
        } else {
            // Different words - set individually (still faster than separate calls)
            m_words[word1] |= (1ULL << bit1);
            m_words[word2] |= (1ULL << bit2);
            m_words[word3] |= (1ULL << bit3);
        }
    }

    // SIMD-optimized bulk bit checking for multiple positions
    inline bool getBitsSimd(size_t pos1, size_t pos2, size_t pos3) const {
        size_t word1 = pos1 >> 6, word2 = pos2 >> 6, word3 = pos3 >> 6;
        size_t bit1 = pos1 & 63, bit2 = pos2 & 63, bit3 = pos3 & 63;

        if (word1 == word2 && word2 == word3) {
            // All bits in same word - single memory access
            uint64_t word = m_words[word1];
            uint64_t mask = (1ULL << bit1) | (1ULL << bit2) | (1ULL << bit3);
            return (word & mask) == mask;
        } else {
            // Different words - check individually
            return (m_words[word1] & (1ULL << bit1)) &&
                   (m_words[word2] & (1ULL << bit2)) &&
                   (m_words[word3] & (1ULL << bit3));
        }
    }

public:
    BloomFilter() {
        // Initialize with zero-fill
        // The alignas(32) declaration should handle alignment automatically
        m_words.resize(NUM_WORDS, 0ULL);
    }

    // Add a key to the bloom filter using SIMD-optimized operations
    inline void add(const KeyType& key) {
        size_t h1 = hash1(key);
        size_t h2 = hash2(key);
        size_t h3 = hash3(key);
        
        // Use SIMD-optimized bulk bit setting
        setBitsSimd(h1, h2, h3);
    }

    // Check if a key might be in the set using SIMD-optimized operations
    inline bool contains(const KeyType& key) const {
        size_t h1 = hash1(key);
        size_t h2 = hash2(key);
        size_t h3 = hash3(key);
        
        // Use SIMD-optimized bulk bit checking
        return getBitsSimd(h1, h2, h3);
    }

    // SIMD-optimized clear operation
    inline void clear() {
        // Use SIMD to clear multiple words at once
        size_t simd_words = (NUM_WORDS / 4) * 4;  // Process 4 words (256 bits) at a time
        
        if (simd_words > 0) {
            __m256i zero = _mm256_setzero_si256();
            __m256i* simd_ptr = reinterpret_cast<__m256i*>(m_words.data());
            
            for (size_t i = 0; i < simd_words / 4; ++i) {
                _mm256_store_si256(&simd_ptr[i], zero);
            }
        }
        
        // Handle remaining words
        for (size_t i = simd_words; i < NUM_WORDS; ++i) {
            m_words[i] = 0ULL;
        }
    }

    // SIMD-optimized popcount for false positive rate calculation
    double getApproximateFalsePositiveRate() const {
        size_t setBits = 0;
        
        // Use SIMD for bulk popcount operations
        size_t simd_words = (NUM_WORDS / 4) * 4;  // Process 4 words at a time
        
        if (simd_words > 0) {
            for (size_t i = 0; i < simd_words; i += 4) {
                // Load 4 uint64_t values (256 bits) at once
                __m256i data = _mm256_load_si256(reinterpret_cast<const __m256i*>(&m_words[i]));
                
                // Extract individual 64-bit values and count bits
                uint64_t w0 = _mm256_extract_epi64(data, 0);
                uint64_t w1 = _mm256_extract_epi64(data, 1);
                uint64_t w2 = _mm256_extract_epi64(data, 2);
                uint64_t w3 = _mm256_extract_epi64(data, 3);
                
                setBits += _mm_popcnt_u64(w0) + _mm_popcnt_u64(w1) + 
                          _mm_popcnt_u64(w2) + _mm_popcnt_u64(w3);
            }
        }
        
        // Handle remaining words
        for (size_t i = simd_words; i < NUM_WORDS; ++i) {
            setBits += _mm_popcnt_u64(m_words[i]);
        }
        
        double ratio = static_cast<double>(setBits) / FilterBits;
        // Approximate false positive rate: (1 - e^(-k*n/m))^k
        return std::pow(ratio, NUM_HASH_FUNCTIONS);
    }

    // Get memory usage in bytes
    inline size_t getMemoryUsage() const {
        return NUM_WORDS * sizeof(uint64_t);
    }

    // Get number of set bits (for debugging)
    inline size_t getSetBitCount() const {
        size_t setBits = 0;
        for (uint64_t word : m_words) {
            setBits += _mm_popcnt_u64(word);
        }
        return setBits;
    }

    // Bulk operations for better performance when adding/checking multiple keys
    inline void addBulk(const std::vector<KeyType>& keys) {
        for (const auto& key : keys) {
            add(key);
        }
    }

    // Check multiple keys at once with better cache utilization
    inline std::vector<bool> containsBulk(const std::vector<KeyType>& keys) const {
        std::vector<bool> results;
        results.reserve(keys.size());
        
        for (const auto& key : keys) {
            results.push_back(contains(key));
        }
        
        return results;
    }

    // Get filter statistics for monitoring
    struct FilterStats {
        size_t totalBits;
        size_t setBits;
        double loadFactor;
        double falsePositiveRate;
        size_t memoryUsage;
    };

    FilterStats getStats() const {
        FilterStats stats;
        stats.totalBits = FilterBits;
        stats.setBits = getSetBitCount();
        stats.loadFactor = static_cast<double>(stats.setBits) / FilterBits;
        stats.falsePositiveRate = getApproximateFalsePositiveRate();
        stats.memoryUsage = getMemoryUsage();
        return stats;
    }
};