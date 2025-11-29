#pragma once

#include <string>
#include <chrono>
#include <vector>
#include <cstring>
#include <cstdio>
#include <type_traits>

// Common constants and utilities for benchmarking

// File paths for storage
#ifdef _MSC_VER
#define FILE_STORAGE_PATH "c:\\filestore.hdb"
#define PMEM_STORAGE_PATH "c:\\pmemstore.hdb"
#else
#define FILE_STORAGE_PATH "/home/skarim/file_storage.bin"
#define PMEM_STORAGE_PATH "/mnt/tmpfs/pmem_storage.bin"
#endif

// Benchmark configuration constants
constexpr size_t DEFAULT_CACHE_SIZE = 100;
constexpr size_t DEFAULT_PAGE_SIZE = 4096;
constexpr size_t DEFAULT_MEMORY_SIZE = 1073741824; // 1GB
constexpr size_t DEFAULT_RECORDS = 100000;
constexpr size_t DEFAULT_DEGREE = 64;
constexpr int DEFAULT_RUNS = 1;

// CHAR16 type definition (16-byte character string)
// Made POD-compliant for serialization compatibility
struct CHAR16 {
    char data[16];

    // Default constructor (trivial)
    CHAR16() = default;

    // Copy constructor (trivial)
    CHAR16(const CHAR16& other) = default;

    // Assignment operator (trivial)
    CHAR16& operator=(const CHAR16& other) = default;

    // Static factory function to create from string
    static CHAR16 from_string(const char* str) {
        CHAR16 result{};
        std::memset(result.data, 0, sizeof(result.data));
#ifndef _MSC_VER
        strncpy(result.data, str, sizeof(result.data) - 1);
#else //_MSC_VER
        strncpy_s(result.data, sizeof(result.data), str, sizeof(result.data) - 1);
#endif //_MSC_VER
        return result;
    }

    // Static factory function to create from numeric value
    template<typename T, typename = typename std::enable_if<std::is_integral<T>::value>::type>
    static CHAR16 from_value(T value) {
        CHAR16 result{};
        std::memset(result.data, 0, sizeof(result.data));
        char str[16];
        snprintf(str, sizeof(str), "str_%08llu", static_cast<unsigned long long>(value));
#ifndef _MSC_VER
        strncpy(result.data, str, sizeof(result.data) - 1);
#else //_MSC_VER
        strncpy_s(result.data, sizeof(result.data), str, sizeof(result.data) - 1);
#endif //_MSC_VER
        return result;
    }

    // Comparison operators
    bool operator==(const CHAR16& other) const {
        return std::memcmp(data, other.data, sizeof(data)) == 0;
    }

    bool operator!=(const CHAR16& other) const {
        return !(*this == other);
    }

    bool operator<(const CHAR16& other) const {
        return std::memcmp(data, other.data, sizeof(data)) < 0;
    }

    bool operator<=(const CHAR16& other) const {
        return std::memcmp(data, other.data, sizeof(data)) <= 0;
    }

    bool operator>(const CHAR16& other) const {
        return std::memcmp(data, other.data, sizeof(data)) > 0;
    }

    bool operator>=(const CHAR16& other) const {
        return std::memcmp(data, other.data, sizeof(data)) >= 0;
    }

    // String conversion
    std::string to_string() const {
        return std::string(data, strnlen(data, sizeof(data)));
    }

    // C-string access
    const char* c_str() const {
        return data;
    }
};

// Hash function for CHAR16 (needed for unordered containers)
namespace std {
    template<>
    struct hash<CHAR16> {
        size_t operator()(const CHAR16& c) const {
            size_t hash_value = 0;
            for (size_t i = 0; i < sizeof(c.data); ++i) {
                hash_value = hash_value * 31 + static_cast<size_t>(c.data[i]);
            }
            return hash_value;
        }
    };
}

// Alias for convenience
using char16 = CHAR16;

// Timing utilities
using TimePoint = std::chrono::high_resolution_clock::time_point;
using Duration = std::chrono::nanoseconds;

inline TimePoint get_time() {
    return std::chrono::high_resolution_clock::now();
}

inline Duration get_duration(const TimePoint& start, const TimePoint& end) {
    return std::chrono::duration_cast<Duration>(end - start);
}

inline double duration_to_seconds(const Duration& duration) {
    return duration.count() / 1e9;
}

inline double duration_to_microseconds(const Duration& duration) {
    return duration.count() / 1e3;
}

// Throughput calculation
inline double calculate_throughput(size_t operations, const Duration& duration) {
    double seconds = duration_to_seconds(duration);
    return seconds > 0 ? operations / seconds : 0.0;
}

// Random number generation utilities
class RandomGenerator {
private:
    std::mt19937 gen_;
    
public:
    RandomGenerator(uint32_t seed = std::chrono::steady_clock::now().time_since_epoch().count()) 
        : gen_(seed) {}
    
    int random_int(int min, int max) {
        std::uniform_int_distribution<int> dist(min, max);
        return dist(gen_);
    }
    
    std::vector<int> generate_random_sequence(size_t count, int min, int max) {
        std::vector<int> sequence;
        sequence.reserve(count);
        
        for (size_t i = 0; i < count; ++i) {
            sequence.push_back(random_int(min, max));
        }
        
        return sequence;
    }
    
    std::vector<int> generate_sequential_sequence(size_t count, int start = 1) {
        std::vector<int> sequence;
        sequence.reserve(count);
        
        for (size_t i = 0; i < count; ++i) {
            sequence.push_back(start + static_cast<int>(i));
        }
        
        return sequence;
    }
    
    void shuffle_sequence(std::vector<int>& sequence) {
        std::shuffle(sequence.begin(), sequence.end(), gen_);
    }
    
    // Generate different search patterns
    std::vector<int> generate_random_search_sequence(size_t count, int min, int max) {
        // Generate unique random sequence (no duplicates)
        std::vector<int> sequence;
        sequence.reserve(count);
        
        // Fill with sequential values first
        for (int i = min; i <= max && sequence.size() < count; ++i) {
            sequence.push_back(i);
        }
        
        // If we need more values than the range, fill with random values
        while (sequence.size() < count) {
            sequence.push_back(random_int(min, max));
        }
        
        // Shuffle to randomize order
        std::shuffle(sequence.begin(), sequence.end(), gen_);
        return sequence;
    }
    
    std::vector<int> generate_uniform_search_sequence(size_t count, int min, int max) {
        // Generate uniform distribution (may have duplicates)
        std::vector<int> sequence;
        sequence.reserve(count);
        
        std::uniform_int_distribution<int> dist(min, max);
        for (size_t i = 0; i < count; ++i) {
            sequence.push_back(dist(gen_));
        }
        
        return sequence;
    }
    
    std::vector<int> generate_zipfian_search_sequence(size_t count, int min, int max) {
        // Generate Zipfian-like distribution (skewed access pattern)
        std::vector<int> sequence;
        sequence.reserve(count);
        
        std::uniform_real_distribution<double> uniform(0.0, 1.0);
        int range = max - min + 1;
        
        for (size_t i = 0; i < count; ++i) {
            double u = uniform(gen_);
            int rank = static_cast<int>(1.0 / std::pow(u, 1.0 / 1.1));
            int value = min + ((rank - 1) % range);
            sequence.push_back(value);
        }
        
        return sequence;
    }
};

// Benchmark result structure
struct BenchmarkResult {
    std::string tree_type;
    std::string policy_name;  // renamed from cache_type
    std::string storage_type;
    std::string config_name;
    double cache_size;
    size_t cache_page_limit;
    int thread_count;
    std::string timestamp;
    std::string key_type;
    std::string value_type;
    size_t record_count;
    size_t degree;
    std::string operation;
    Duration duration;
    double throughput_ops_sec;
    int test_run_id;  // renamed from run_id
    
#ifdef __CACHE_COUNTERS__
    // Cache performance counters
    uint64_t cache_hits = 0;
    uint64_t cache_misses = 0;
    uint64_t cache_evictions = 0;  // renamed from evictions
    uint64_t cache_dirty_evictions = 0;  // renamed from dirty_evictions
    double cache_hit_rate = 0.0;  // renamed from cache_hit_ratio
#endif //__CACHE_COUNTERS__
    
    BenchmarkResult() = default;
    
    BenchmarkResult(const std::string& tree, const std::string& cache, const std::string& storage,
                   double cache_sz, size_t cache_page_lim, const std::string& key, const std::string& value,
                   const std::string& op, size_t deg, size_t records, int run,
                   int threads, const Duration& dur, const std::string& config = "")
        : tree_type(tree), policy_name(cache), storage_type(storage), config_name(config), cache_size(cache_sz),
          cache_page_limit(cache_page_lim), thread_count(threads), key_type(key), value_type(value), 
          record_count(records), degree(deg), operation(op), duration(dur),
          throughput_ops_sec(calculate_throughput(records, dur)), test_run_id(run) {
        
        // Generate timestamp
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        auto tm = *std::localtime(&time_t);
        
        char buffer[100];
        std::strftime(buffer, sizeof(buffer), "%Y%m%d_%H%M%S", &tm);
        timestamp = buffer;
    }

#ifdef __CACHE_COUNTERS__
    // Constructor with cache counters
    BenchmarkResult(const std::string& tree, const std::string& cache, const std::string& storage,
                   double cache_sz, size_t cache_page_lim, const std::string& key, const std::string& value,
                   const std::string& op, size_t deg, size_t records, int run,
                   int threads, const Duration& dur, const std::string& config,
                   uint64_t hits, uint64_t misses, uint64_t evict, uint64_t dirty_evict)
        : tree_type(tree), policy_name(cache), storage_type(storage), config_name(config), cache_size(cache_sz),
          cache_page_limit(cache_page_lim), thread_count(threads), key_type(key), value_type(value), 
          record_count(records), degree(deg), operation(op), duration(dur),
          throughput_ops_sec(calculate_throughput(records, dur)), test_run_id(run),
          cache_hits(hits), cache_misses(misses), cache_evictions(evict), cache_dirty_evictions(dirty_evict) {
        
        // Calculate cache hit ratio
        uint64_t total_accesses = cache_hits + cache_misses;
        cache_hit_rate = total_accesses > 0 ? static_cast<double>(cache_hits) / total_accesses : 0.0;
        
        // Generate timestamp
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        auto tm = *std::localtime(&time_t);
        
        char buffer[100];
        std::strftime(buffer, sizeof(buffer), "%Y%m%d_%H%M%S", &tm);
        timestamp = buffer;
    }
#endif //__CACHE_COUNTERS__
};

// Utility functions for string conversion
inline std::string to_string(const Duration& duration) {
    return std::to_string(duration.count());
}

// Memory management utilities
inline void clear_system_cache() {
    // On Linux, this would require root privileges
    // system("echo 3 > /proc/sys/vm/drop_caches");
    // For now, just a placeholder
}

// Validation utilities
inline bool validate_cache_type(const std::string& cache_type) {
    return cache_type == "LRU" || cache_type == "A2Q" || cache_type == "CLOCK";
}

inline bool validate_storage_type(const std::string& storage_type) {
    return storage_type == "VolatileStorage" || storage_type == "FileStorage" || storage_type == "PMemStorage";
}

inline bool validate_operation(const std::string& operation) {
    return operation == "insert" || operation == "search" || operation == "delete";
}

inline bool validate_key_value_type(const std::string& type) {
    return type == "int" || type == "uint64_t" || type == "char16";
}