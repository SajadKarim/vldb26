#pragma once

#ifdef __CACHE_COUNTERS__
#include <chrono>
#include <vector>
#include <utility>
#include <map>
#include <algorithm>
#include <mutex>
#include <set>

template<typename DerivedCache>
class CacheStatsProvider {
private:
    // Time-based counter vectors - each entry has timestamp and count
    static thread_local std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> thread_hits;
    static thread_local std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> thread_misses;
    static thread_local std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> thread_evictions;
    static thread_local std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> thread_dirty_evictions;
    
    // Last timestamp when we added a new entry
    static thread_local std::chrono::steady_clock::time_point last_timestamp;
    static thread_local bool initialized;
    
    static constexpr std::chrono::milliseconds SAMPLE_INTERVAL{500}; // 500ms as suggested
    
    void recordEvent(std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>& event_vector) {
        auto current_time = std::chrono::steady_clock::now();
        
        // Initialize on first use
        if (!initialized) {
            last_timestamp = current_time;
            initialized = true;
            // Add first entry
            event_vector.emplace_back(current_time, 1);
            return;
        }
        
        // Check if interval has passed
        if (current_time - last_timestamp >= SAMPLE_INTERVAL) {
            // Add new entry with current time and count of 1
            event_vector.emplace_back(current_time, 1);
            last_timestamp = current_time;
        } else {
            // Increment the count of the last entry
            if (!event_vector.empty()) {
                event_vector.back().second++;
            } else {
                // Edge case: vector is empty but initialized is true
                event_vector.emplace_back(current_time, 1);
            }
        }
        
        // Optional: limit vector size to prevent unbounded growth
        constexpr size_t MAX_SAMPLES = 7200; // 1 hour at 500ms intervals
        if (event_vector.size() > MAX_SAMPLES) {
            event_vector.erase(event_vector.begin());
        }
    }
    
public:
    // Public methods for recording cache events
    void recordHit() {
        recordEvent(thread_hits);
    }
    
    void recordMiss() {
        recordEvent(thread_misses);
    }
    
    void recordEviction(bool is_dirty) {
        recordEvent(thread_evictions);
        if (is_dirty) {
            recordEvent(thread_dirty_evictions);
        }
    }
    
    // Public interface for getting time-series data
    const std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>& getHitsTimeline() const {
        return thread_hits;
    }
    
    const std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>& getMissesTimeline() const {
        return thread_misses;
    }
    
    const std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>& getEvictionsTimeline() const {
        return thread_evictions;
    }
    
    const std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>& getDirtyEvictionsTimeline() const {
        return thread_dirty_evictions;
    }
    
    // Static function to aggregate stats from multiple threads
    static void aggregateThreadStats(
        const CacheStatsProvider* thread_provider,
        std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>& aggregated_hits,
        std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>& aggregated_misses,
        std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>& aggregated_evictions,
        std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>& aggregated_dirty_evictions
    ) {
        // Helper lambda to merge timeline data
        auto mergeTimeline = [](
            const std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>& source,
            std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>>& target
        ) {
            for (const auto& entry : source) {
                // Look for existing entry with same timestamp
                auto it = std::find_if(target.begin(), target.end(),
                    [&entry](const auto& existing) {
                        return existing.first == entry.first;
                    });
                
                if (it != target.end()) {
                    // Found existing timestamp, increment the counter
                    it->second += entry.second;
                } else {
                    // New timestamp, add new entry
                    target.push_back(entry);
                }
            }
            
            // Sort by timestamp to maintain temporal order
            std::sort(target.begin(), target.end(),
                [](const auto& a, const auto& b) {
                    return a.first < b.first;
                });
        };
        
        // Merge all timeline data
        mergeTimeline(thread_provider->getHitsTimeline(), aggregated_hits);
        mergeTimeline(thread_provider->getMissesTimeline(), aggregated_misses);
        mergeTimeline(thread_provider->getEvictionsTimeline(), aggregated_evictions);
        mergeTimeline(thread_provider->getDirtyEvictionsTimeline(), aggregated_dirty_evictions);
    }
    
    // Static method to reset thread-local statistics
    // This should be called when creating a new cache instance to ensure clean statistics
    static void resetThreadLocalStats() {
        thread_hits.clear();
        thread_misses.clear();
        thread_evictions.clear();
        thread_dirty_evictions.clear();
        initialized = false;
    }
};

// Static member definitions
template<typename DerivedCache>
thread_local std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> 
    CacheStatsProvider<DerivedCache>::thread_hits;

template<typename DerivedCache>
thread_local std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> 
    CacheStatsProvider<DerivedCache>::thread_misses;

template<typename DerivedCache>
thread_local std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> 
    CacheStatsProvider<DerivedCache>::thread_evictions;

template<typename DerivedCache>
thread_local std::vector<std::pair<std::chrono::steady_clock::time_point, uint64_t>> 
    CacheStatsProvider<DerivedCache>::thread_dirty_evictions;

template<typename DerivedCache>
thread_local std::chrono::steady_clock::time_point CacheStatsProvider<DerivedCache>::last_timestamp;

template<typename DerivedCache>
thread_local bool CacheStatsProvider<DerivedCache>::initialized = false;

#endif // __CACHE_COUNTERS__