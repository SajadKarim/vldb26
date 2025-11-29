#pragma once

#include <string>
#include <unordered_map>
#include <utility>

namespace DeviceAware {

// Workload types based on YCSB benchmarks
enum class WorkloadType {
    YCSB_A,  // Update heavy: 50% reads, 50% updates
    YCSB_B,  // Read mostly: 95% reads, 5% updates
    YCSB_C,  // Read only: 100% reads
    YCSB_D,  // Read latest: 95% reads, 5% inserts (temporal locality)
    YCSB_E,  // Scan heavy: 95% scans, 5% inserts
    YCSB_F,  // Read-modify-write: 50% reads, 50% read-modify-write
    UNKNOWN
};

// Storage device types
enum class StorageDeviceType {
    VOLATILE,   // In-memory storage (DRAM)
    PMEM,       // Persistent memory (Intel Optane)
    FILE,       // File-based storage (SSD/HDD)
    IOURING,    // io_uring based storage
    UNKNOWN
};

// Cache policy types
enum class CachePolicyType {
    LRU,
    A2Q,
    CLOCK,
    UNKNOWN
};

// Configuration flags that affect cache behavior
struct PolicyConfig {
    CachePolicyType policy_type;
    std::string policy_name;           // "LRU", "A2Q", "CLOCK"
    std::string build_config;          // e.g., "non_concurrent_relaxed"
    
    // Behavioral flags (automatically derived from build_config)
    bool enable_concurrent;            // __CONCURRENT__
    bool enable_selective_update;      // __SELECTIVE_UPDATE__ (relaxed mode)
    bool enable_update_in_order;       // __UPDATE_IN_ORDER__ (LRU specific)
    bool enable_manage_ghost_q;        // __MANAGE_GHOST_Q__ (A2Q specific)
    bool enable_clock_with_buffer;     // __CLOCK_WITH_BUFFER__ (CLOCK specific)
    
    // Rationale for the selection
    std::string selection_rationale;
    
private:
    // Helper function to derive flags from config name
    void deriveFlagsFromConfig() {
        // Reset all flags
        enable_concurrent = false;
        enable_selective_update = false;
        enable_update_in_order = false;
        enable_manage_ghost_q = false;
        enable_clock_with_buffer = false;
        
        // Parse config name and set appropriate flags
        if (build_config.find("concurrent") != std::string::npos && 
            build_config.find("non_concurrent") == std::string::npos) {
            enable_concurrent = true;
        }
        
        if (build_config.find("relaxed") != std::string::npos) {
            enable_selective_update = true;
        }
        
        if (build_config.find("update_in_order") != std::string::npos) {
            enable_update_in_order = true;
        }
        
        if (build_config.find("ghost_q_enabled") != std::string::npos) {
            enable_manage_ghost_q = true;
        }
        
        if (build_config.find("clock_with_buffer") != std::string::npos) {
            enable_clock_with_buffer = true;
        }
    }
    
public:
    // Default constructor for use in containers
    PolicyConfig()
        : policy_type(CachePolicyType::LRU)
        , policy_name("LRU")
        , build_config("non_concurrent_default")
        , enable_concurrent(false)
        , enable_selective_update(false)
        , enable_update_in_order(false)
        , enable_manage_ghost_q(false)
        , enable_clock_with_buffer(false)
        , selection_rationale("")
    {}
    
    // Simplified constructor - flags are automatically derived from config name
    PolicyConfig(
        CachePolicyType type,
        const std::string& name,
        const std::string& config,
        const std::string& rationale = ""
    ) : policy_type(type)
      , policy_name(name)
      , build_config(config)
      , selection_rationale(rationale)
    {
        deriveFlagsFromConfig();
    }
};

// DeviceAwarePolicy: Selects optimal cache policy and configuration
// based on workload characteristics and storage device type
class DeviceAwarePolicy {
private:
    // Decision matrix: (workload, storage) -> PolicyConfig
    using DecisionKey = std::pair<WorkloadType, StorageDeviceType>;
    std::unordered_map<std::string, PolicyConfig> decision_matrix_;
    
    // Helper to create decision key
    static std::string makeKey(WorkloadType workload, StorageDeviceType storage) {
        return std::to_string(static_cast<int>(workload)) + "_" + 
               std::to_string(static_cast<int>(storage));
    }
    
    void initializeDecisionMatrix() {
        // YCSB-A: Update heavy (50% reads, 50% updates)
        // - High write pressure, need efficient dirty page management
        // - VolatileStorage: CLOCK with relaxed updates (best for mixed workload)
        decision_matrix_[makeKey(WorkloadType::YCSB_A, StorageDeviceType::VOLATILE)] = 
            PolicyConfig(CachePolicyType::CLOCK, "CLOCK", "non_concurrent_relaxed",
                        "CLOCK with relaxed updates: optimal for update-heavy workload on DRAM");
        
        // - PMemStorage: A2Q with ghost queue (persistence-aware)
        decision_matrix_[makeKey(WorkloadType::YCSB_A, StorageDeviceType::PMEM)] = 
            PolicyConfig(CachePolicyType::A2Q, "A2Q", "non_concurrent_a2q_ghost_q_enabled",
                        "A2Q with ghost queue: ensures consistency for persistent memory");
        
        // - FileStorage: A2Q with ghost queue (adaptive for I/O bound)
        decision_matrix_[makeKey(WorkloadType::YCSB_A, StorageDeviceType::FILE)] = 
            PolicyConfig(CachePolicyType::A2Q, "A2Q", "non_concurrent_a2q_ghost_q_enabled",
                        "A2Q with ghost queue: adaptive for update-heavy I/O-bound workload");
        
        // YCSB-B: Read mostly (95% reads, 5% updates)
        // - Dominated by reads, need efficient hit rate
        // - VolatileStorage: LRU with ordered updates (efficient for read-mostly)
        decision_matrix_[makeKey(WorkloadType::YCSB_B, StorageDeviceType::VOLATILE)] = 
            PolicyConfig(CachePolicyType::LRU, "LRU", "non_concurrent_lru_metadata_update_in_order",
                        "LRU with ordered updates: efficient for read-mostly workload");
        
        // - PMemStorage: A2Q with relaxed (leverage multi-queue for read optimization)
        decision_matrix_[makeKey(WorkloadType::YCSB_B, StorageDeviceType::PMEM)] = 
            PolicyConfig(CachePolicyType::A2Q, "A2Q", "non_concurrent_relaxed",
                        "A2Q with relaxed: multi-queue structure benefits read-heavy persistent workload");
        
        // - FileStorage: A2Q with relaxed (maximize cache hits to reduce I/O)
        decision_matrix_[makeKey(WorkloadType::YCSB_B, StorageDeviceType::FILE)] = 
            PolicyConfig(CachePolicyType::A2Q, "A2Q", "non_concurrent_relaxed",
                        "A2Q with relaxed: maximize hit rate to minimize expensive disk I/O");
        
        // YCSB-C: Read only (100% reads)
        // - Pure read workload, maximize hit rate
        // - VolatileStorage: A2Q with relaxed (no updates, can skip metadata)
        decision_matrix_[makeKey(WorkloadType::YCSB_C, StorageDeviceType::VOLATILE)] = 
            PolicyConfig(CachePolicyType::A2Q, "A2Q", "non_concurrent_relaxed",
                        "A2Q with relaxed: optimal for read-only workload, skip unnecessary metadata updates");
        
        // - PMemStorage: CLOCK with relaxed (simpler, sufficient for read-only)
        decision_matrix_[makeKey(WorkloadType::YCSB_C, StorageDeviceType::PMEM)] = 
            PolicyConfig(CachePolicyType::CLOCK, "CLOCK", "non_concurrent_relaxed",
                        "CLOCK with relaxed: simple and efficient for read-only persistent workload");
        
        // - FileStorage: LRU with ordered updates (maximize hits to avoid disk reads)
        decision_matrix_[makeKey(WorkloadType::YCSB_C, StorageDeviceType::FILE)] = 
            PolicyConfig(CachePolicyType::LRU, "LRU", "non_concurrent_lru_metadata_update_in_order",
                        "LRU with ordered updates: maximize hit rate for read-only disk workload");
        
        // YCSB-D: Read latest (95% reads, 5% inserts, temporal locality)
        // - Strong temporal locality, recent items accessed frequently
        // - VolatileStorage: A2Q with relaxed (perfect for temporal locality)
        decision_matrix_[makeKey(WorkloadType::YCSB_D, StorageDeviceType::VOLATILE)] = 
            PolicyConfig(CachePolicyType::A2Q, "A2Q", "non_concurrent_relaxed",
                        "A2Q with relaxed: ideal for temporal locality in read-latest workload");
        
        // - PMemStorage: CLOCK default (temporal locality + persistence)
        decision_matrix_[makeKey(WorkloadType::YCSB_D, StorageDeviceType::PMEM)] = 
            PolicyConfig(CachePolicyType::CLOCK, "CLOCK", "non_concurrent_default",
                        "CLOCK: temporal locality + persistence guarantees");
        
        // - FileStorage: A2Q with relaxed (temporal locality reduces I/O)
        decision_matrix_[makeKey(WorkloadType::YCSB_D, StorageDeviceType::FILE)] = 
            PolicyConfig(CachePolicyType::A2Q, "A2Q", "non_concurrent_relaxed",
                        "A2Q with relaxed: temporal locality minimizes disk access");
        
        // YCSB-E: Scan heavy (95% scans, 5% inserts)
        // - Sequential access patterns, large working set
        // - VolatileStorage: LRU with ordered updates (resistant to scan pollution)
        decision_matrix_[makeKey(WorkloadType::YCSB_E, StorageDeviceType::VOLATILE)] = 
            PolicyConfig(CachePolicyType::LRU, "LRU", "non_concurrent_lru_metadata_update_in_order",
                        "LRU with ordered updates: efficient for scan-heavy patterns");
        
        // - PMemStorage: CLOCK default (consistency for scans)
        decision_matrix_[makeKey(WorkloadType::YCSB_E, StorageDeviceType::PMEM)] = 
            PolicyConfig(CachePolicyType::CLOCK, "CLOCK", "non_concurrent_default",
                        "CLOCK: ensures scan consistency on persistent memory");
        
        // - FileStorage: LRU with ordered updates and relaxed (balance read and write efficiency)
        decision_matrix_[makeKey(WorkloadType::YCSB_E, StorageDeviceType::FILE)] = 
            PolicyConfig(CachePolicyType::LRU, "LRU", "non_concurrent_lru_metadata_update_in_order_and_relaxed",
                        "LRU with ordered updates and relaxed: balanced performance for scans on disk");    
        
        // YCSB-F: Read-modify-write (50% reads, 50% RMW)
        // - High contention, need efficient dirty page handling
        // - VolatileStorage: CLOCK default (efficient for mixed operations)
        decision_matrix_[makeKey(WorkloadType::YCSB_F, StorageDeviceType::VOLATILE)] = 
            PolicyConfig(CachePolicyType::CLOCK, "CLOCK", "non_concurrent_default",
                        "CLOCK: efficient for read-modify-write patterns");
        
        // - PMemStorage: CLOCK with relaxed (consistency for RMW)
        decision_matrix_[makeKey(WorkloadType::YCSB_F, StorageDeviceType::PMEM)] = 
            PolicyConfig(CachePolicyType::CLOCK, "CLOCK", "non_concurrent_relaxed",
                        "CLOCK with relaxed: ensures RMW consistency on persistent memory");
        
        // - FileStorage: A2Q with relaxed (balance read and write efficiency)
        decision_matrix_[makeKey(WorkloadType::YCSB_F, StorageDeviceType::FILE)] = 
            PolicyConfig(CachePolicyType::A2Q, "A2Q", "non_concurrent_relaxed",
                        "A2Q with relaxed: balanced performance for RMW on disk");                
    }
    
public:
    DeviceAwarePolicy() {
        initializeDecisionMatrix();
    }
    
    // Main selection method
    PolicyConfig selectPolicy(WorkloadType workload, StorageDeviceType storage) const {
        std::string key = makeKey(workload, storage);
        auto it = decision_matrix_.find(key);
        
        if (it != decision_matrix_.end()) {
            return it->second;
        }
        
        // Default fallback: LRU with default config
        return PolicyConfig(
            CachePolicyType::LRU, 
            "LRU", 
            "non_concurrent_default",
            "Default fallback: LRU for unknown workload/storage combination"
        );
    }
    
    // Parse workload string to enum
    static WorkloadType parseWorkload(const std::string& workload_str) {
        std::string lower = workload_str;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        
        if (lower == "ycsb_a" || lower == "ycsb-a" || lower == "a") return WorkloadType::YCSB_A;
        if (lower == "ycsb_b" || lower == "ycsb-b" || lower == "b") return WorkloadType::YCSB_B;
        if (lower == "ycsb_c" || lower == "ycsb-c" || lower == "c") return WorkloadType::YCSB_C;
        if (lower == "ycsb_d" || lower == "ycsb-d" || lower == "d") return WorkloadType::YCSB_D;
        if (lower == "ycsb_e" || lower == "ycsb-e" || lower == "e") return WorkloadType::YCSB_E;
        if (lower == "ycsb_f" || lower == "ycsb-f" || lower == "f") return WorkloadType::YCSB_F;
        
        return WorkloadType::UNKNOWN;
    }
    
    // Parse storage string to enum
    static StorageDeviceType parseStorage(const std::string& storage_str) {
        if (storage_str == "VolatileStorage" || storage_str == "volatile" || storage_str == "VOLATILE") 
            return StorageDeviceType::VOLATILE;
        if (storage_str == "PMemStorage" || storage_str == "pmem" || storage_str == "PMEM") 
            return StorageDeviceType::PMEM;
        if (storage_str == "FileStorage" || storage_str == "file" || storage_str == "FILE") 
            return StorageDeviceType::FILE;
        if (storage_str == "IOURingStorage" || storage_str == "iouring" || storage_str == "IOURING") 
            return StorageDeviceType::IOURING;
        
        return StorageDeviceType::UNKNOWN;
    }
    
    // Get workload name
    static std::string getWorkloadName(WorkloadType workload) {
        switch (workload) {
            case WorkloadType::YCSB_A: return "YCSB-A";
            case WorkloadType::YCSB_B: return "YCSB-B";
            case WorkloadType::YCSB_C: return "YCSB-C";
            case WorkloadType::YCSB_D: return "YCSB-D";
            case WorkloadType::YCSB_E: return "YCSB-E";
            case WorkloadType::YCSB_F: return "YCSB-F";
            default: return "UNKNOWN";
        }
    }
    
    // Get storage name
    static std::string getStorageName(StorageDeviceType storage) {
        switch (storage) {
            case StorageDeviceType::VOLATILE: return "VolatileStorage";
            case StorageDeviceType::PMEM: return "PMemStorage";
            case StorageDeviceType::FILE: return "FileStorage";
            case StorageDeviceType::IOURING: return "IOURingStorage";
            default: return "UNKNOWN";
        }
    }
    
    // Print decision matrix (for debugging/documentation)
    void printDecisionMatrix() const {
        std::cout << "\n=== DeviceAwarePolicy Decision Matrix ===" << std::endl;
        std::cout << "Format: [Workload] x [Storage] -> Policy (Config) : Rationale\n" << std::endl;
        
        for (const auto& workload : {WorkloadType::YCSB_A, WorkloadType::YCSB_B, WorkloadType::YCSB_C,
                                      WorkloadType::YCSB_D, WorkloadType::YCSB_E, WorkloadType::YCSB_F}) {
            for (const auto& storage : {StorageDeviceType::VOLATILE, StorageDeviceType::PMEM, 
                                        StorageDeviceType::FILE, StorageDeviceType::IOURING}) {
                auto config = selectPolicy(workload, storage);
                std::cout << "[" << getWorkloadName(workload) << "] x [" << getStorageName(storage) << "]" << std::endl;
                std::cout << "  -> " << config.policy_name << " (" << config.build_config << ")" << std::endl;
                std::cout << "  Rationale: " << config.selection_rationale << std::endl;
                std::cout << std::endl;
            }
        }
    }
};

} // namespace DeviceAware