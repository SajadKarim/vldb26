#pragma once

#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <string>
#include <map>
#include "common.h"

namespace ycsbworkloadgenerator {

// YCSB Operation types
enum class OperationType {
    READ = 0,
    UPDATE = 1,
    INSERT = 2,
    SCAN = 3,
    DELETE = 4,
    READ_MODIFY_WRITE = 5
};

// YCSB Request distribution types
enum class RequestDistribution {
    UNIFORM,
    ZIPFIAN,
    LATEST
};

// YCSB Workload types
enum class WorkloadType {
    WORKLOAD_A,  // Update heavy: 50% reads, 50% updates
    WORKLOAD_B,  // Read mostly: 95% reads, 5% updates
    WORKLOAD_C,  // Read only: 100% reads
    WORKLOAD_D,  // Read latest: 95% reads, 5% inserts
    WORKLOAD_E,  // Short ranges: 95% scans, 5% inserts
    WORKLOAD_F   // Read-modify-write: 50% reads, 50% read-modify-write
};

// YCSB Operation structure
template<typename KeyType>
struct YCSBOperation {
    OperationType operation;
    KeyType key;
    KeyType end_key;  // For scan operations
    int scan_length;  // For scan operations
    
    YCSBOperation() : operation(OperationType::READ), scan_length(0) {}
    YCSBOperation(OperationType op, const KeyType& k) : operation(op), key(k), scan_length(0) {}
    YCSBOperation(OperationType op, const KeyType& k, const KeyType& end_k, int len) 
        : operation(op), key(k), end_key(end_k), scan_length(len) {}
};

// Workload configuration
struct WorkloadConfig {
    std::map<OperationType, double> operation_proportions;
    RequestDistribution request_distribution;
    size_t record_count;
    size_t operation_count;
    int scan_length_min;
    int scan_length_max;
    
    WorkloadConfig() : request_distribution(RequestDistribution::UNIFORM), 
                      record_count(1000000), operation_count(1000000),
                      scan_length_min(1), scan_length_max(100) {}
};

// Get workload configuration for each YCSB workload type
inline WorkloadConfig get_workload_config(WorkloadType workload_type) {
    WorkloadConfig config;
    
    switch (workload_type) {
        case WorkloadType::WORKLOAD_A:  // Update heavy
            config.operation_proportions[OperationType::READ] = 0.5;
            config.operation_proportions[OperationType::UPDATE] = 0.5;
            config.request_distribution = RequestDistribution::ZIPFIAN;
            break;
            
        case WorkloadType::WORKLOAD_B:  // Read mostly
            config.operation_proportions[OperationType::READ] = 0.95;
            config.operation_proportions[OperationType::UPDATE] = 0.05;
            config.request_distribution = RequestDistribution::ZIPFIAN;
            break;
            
        case WorkloadType::WORKLOAD_C:  // Read only
            config.operation_proportions[OperationType::READ] = 1.0;
            config.request_distribution = RequestDistribution::ZIPFIAN;
            break;
            
        case WorkloadType::WORKLOAD_D:  // Read latest
            config.operation_proportions[OperationType::READ] = 0.95;
            config.operation_proportions[OperationType::INSERT] = 0.05;
            config.request_distribution = RequestDistribution::LATEST;
            break;
            
        case WorkloadType::WORKLOAD_E:  // Short ranges
            config.operation_proportions[OperationType::SCAN] = 0.95;
            config.operation_proportions[OperationType::INSERT] = 0.05;
            config.request_distribution = RequestDistribution::ZIPFIAN;
            config.scan_length_min = 1;
            config.scan_length_max = 100;
            break;
            
        case WorkloadType::WORKLOAD_F:  // Read-modify-write
            config.operation_proportions[OperationType::READ] = 0.5;
            config.operation_proportions[OperationType::READ_MODIFY_WRITE] = 0.5;
            config.request_distribution = RequestDistribution::ZIPFIAN;
            break;
    }
    
    return config;
}

// Zipfian distribution generator
class ZipfianGenerator {
private:
    double alpha;
    double zetan;
    double eta;
    double theta;
    size_t n;
    std::mt19937 gen;
    std::uniform_real_distribution<double> uniform_dist;
    
public:
    ZipfianGenerator(size_t num_items, double zipfian_constant = 0.99) 
        : alpha(zipfian_constant), n(num_items), gen(std::random_device{}()), uniform_dist(0.0, 1.0) {
        
        theta = alpha;
        zetan = zeta(n, theta);
        eta = (1.0 - std::pow(2.0 / n, 1.0 - theta)) / (1.0 - zeta(2, theta) / zetan);
    }
    
    size_t next() {
        double u = uniform_dist(gen);
        double uz = u * zetan;
        
        if (uz < 1.0) {
            return 0;
        }
        
        if (uz < 1.0 + std::pow(0.5, theta)) {
            return 1;
        }
        
        return static_cast<size_t>((n - 1) * std::pow(eta * u - eta + 1.0, alpha));
    }
    
private:
    double zeta(size_t num, double theta) {
        double sum = 0.0;
        for (size_t i = 1; i <= num; i++) {
            sum += 1.0 / std::pow(i, theta);
        }
        return sum;
    }
};

// Generate key based on distribution
template<typename KeyType>
KeyType generate_key(RequestDistribution distribution, size_t record_count, 
                    ZipfianGenerator& zipf_gen, std::mt19937& uniform_gen, 
                    size_t& latest_key) {
    
    if constexpr (std::is_same_v<KeyType, uint64_t>) {
        switch (distribution) {
            case RequestDistribution::UNIFORM: {
                std::uniform_int_distribution<uint64_t> dist(0, record_count - 1);
                return dist(uniform_gen);
            }
            case RequestDistribution::ZIPFIAN: {
                return static_cast<uint64_t>(zipf_gen.next() % record_count);
            }
            case RequestDistribution::LATEST: {
                // Focus on recently inserted keys
                size_t range = std::min(record_count / 10, latest_key + 1);
                std::uniform_int_distribution<size_t> dist(0, range - 1);
                return static_cast<uint64_t>(latest_key - dist(uniform_gen));
            }
        }
    } else if constexpr (std::is_same_v<KeyType, CHAR16>) {
        size_t key_num;
        switch (distribution) {
            case RequestDistribution::UNIFORM: {
                std::uniform_int_distribution<size_t> dist(0, record_count - 1);
                key_num = dist(uniform_gen);
                break;
            }
            case RequestDistribution::ZIPFIAN: {
                key_num = zipf_gen.next() % record_count;
                break;
            }
            case RequestDistribution::LATEST: {
                size_t range = std::min(record_count / 10, latest_key + 1);
                std::uniform_int_distribution<size_t> dist(0, range - 1);
                key_num = latest_key - dist(uniform_gen);
                break;
            }
        }
        
        char str[16];
#ifdef _MSC_VER
        sprintf_s(str, sizeof(str), "key_%08zu", key_num);
#else
        snprintf(str, sizeof(str), "key_%08zu", key_num);
#endif
        return CHAR16(str);
    }
    
    return KeyType{};
}

// Generate YCSB workload operations
template<typename KeyType>
std::vector<YCSBOperation<KeyType>> generate_ycsb_operations(const WorkloadConfig& config) {
    std::vector<YCSBOperation<KeyType>> operations;
    operations.reserve(config.operation_count);
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<double> op_dist(0.0, 1.0);
    std::uniform_int_distribution<int> scan_dist(config.scan_length_min, config.scan_length_max);
    
    ZipfianGenerator zipf_gen(config.record_count);
    size_t latest_key = config.record_count - 1;
    
    // Build cumulative distribution for operation selection
    std::vector<std::pair<double, OperationType>> op_cumulative;
    double cumulative = 0.0;
    for (const auto& [op_type, proportion] : config.operation_proportions) {
        cumulative += proportion;
        op_cumulative.emplace_back(cumulative, op_type);
    }
    
    for (size_t i = 0; i < config.operation_count; ++i) {
        // Select operation type
        double op_rand = op_dist(gen);
        OperationType selected_op = OperationType::READ;
        for (const auto& [threshold, op_type] : op_cumulative) {
            if (op_rand <= threshold) {
                selected_op = op_type;
                break;
            }
        }
        
        // Generate key(s) based on operation
        if (selected_op == OperationType::SCAN) {
            KeyType start_key = generate_key<KeyType>(config.request_distribution, 
                                                     config.record_count, zipf_gen, gen, latest_key);
            int scan_length = scan_dist(gen);
            
            KeyType end_key;
            if constexpr (std::is_same_v<KeyType, uint64_t>) {
                end_key = start_key + scan_length;
            } else if constexpr (std::is_same_v<KeyType, CHAR16>) {
                // For CHAR16, we'll use the scan_length as a hint
                char str[16];
                if constexpr (std::is_same_v<KeyType, CHAR16>) {
                    // Extract number from start_key and add scan_length
                    size_t start_num = 0;
#ifdef _MSC_VER
                    sscanf_s(start_key.data, "key_%08zu", &start_num);
                    sprintf_s(str, sizeof(str), "key_%08zu", start_num + scan_length);
#else
                    sscanf(start_key.data, "key_%08zu", &start_num);
                    snprintf(str, sizeof(str), "key_%08zu", start_num + scan_length);
#endif
                    end_key = CHAR16(str);
                }
            }
            
            operations.emplace_back(selected_op, start_key, end_key, scan_length);
        } else {
            KeyType key = generate_key<KeyType>(config.request_distribution, 
                                               config.record_count, zipf_gen, gen, latest_key);
            operations.emplace_back(selected_op, key);
            
            // Update latest_key for INSERT operations
            if (selected_op == OperationType::INSERT) {
                if constexpr (std::is_same_v<KeyType, uint64_t>) {
                    latest_key = std::max(latest_key, static_cast<size_t>(key));
                }
            }
        }
    }
    
    return operations;
}

// Save YCSB operations to file
template<typename KeyType>
void save_ycsb_operations(const std::vector<YCSBOperation<KeyType>>& operations, 
                         const std::string& filepath) {
    std::ofstream file(filepath, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot create file: " + filepath);
    }
    
    size_t count = operations.size();
    file.write(reinterpret_cast<const char*>(&count), sizeof(count));
    file.write(reinterpret_cast<const char*>(operations.data()), count * sizeof(YCSBOperation<KeyType>));
    file.close();
}

// Load YCSB operations from file
template<typename KeyType>
std::vector<YCSBOperation<KeyType>> load_ycsb_operations(const std::string& filepath) {
    std::ifstream file(filepath, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }
    
    size_t count;
    file.read(reinterpret_cast<char*>(&count), sizeof(count));
    
    std::vector<YCSBOperation<KeyType>> operations(count);
    file.read(reinterpret_cast<char*>(operations.data()), count * sizeof(YCSBOperation<KeyType>));
    file.close();
    
    return operations;
}

// Get workload name string
inline std::string get_workload_name(WorkloadType workload_type) {
    switch (workload_type) {
        case WorkloadType::WORKLOAD_A: return "workload_a";
        case WorkloadType::WORKLOAD_B: return "workload_b";
        case WorkloadType::WORKLOAD_C: return "workload_c";
        case WorkloadType::WORKLOAD_D: return "workload_d";
        case WorkloadType::WORKLOAD_E: return "workload_e";
        case WorkloadType::WORKLOAD_F: return "workload_f";
        default: return "unknown";
    }
}

// Generate filename for YCSB workload
inline std::string generate_ycsb_filename(const std::string& type, WorkloadType workload_type, 
                                         size_t record_count, size_t operation_count) {
    std::string workload_name = get_workload_name(workload_type);
    return "ycsb/" + type + "_" + workload_name + "_" + std::to_string(record_count) + 
           "_ops_" + std::to_string(operation_count) + ".dat";
}

// Create YCSB workload for specific type and workload
template<typename KeyType>
void create_ycsb_workload(WorkloadType workload_type, size_t record_count, size_t operation_count) {
    std::string type_name;
    if constexpr (std::is_same_v<KeyType, uint64_t>) {
        type_name = "uint64";
    } else if constexpr (std::is_same_v<KeyType, CHAR16>) {
        type_name = "char16";
    }
    
    std::string filename = generate_ycsb_filename(type_name, workload_type, record_count, operation_count);
    
    // Check if file exists
    if (std::filesystem::exists(filename)) {
        std::cout << "File " << filename << " already exists, skipping generation." << std::endl;
        return;
    }
    
    // Create ycsb directory if it doesn't exist
    std::filesystem::create_directories("ycsb");
    
    // Get workload configuration
    WorkloadConfig config = get_workload_config(workload_type);
    config.record_count = record_count;
    config.operation_count = operation_count;
    
    // Generate operations
    std::vector<YCSBOperation<KeyType>> operations = generate_ycsb_operations<KeyType>(config);
    
    // Save to file
    save_ycsb_operations(operations, filename);
    std::cout << "Generated " << filename << " with " << operation_count << " operations." << std::endl;
}

// Generate all YCSB workloads for all combinations
inline void generate_all_ycsb_workloads() {
    std::vector<size_t> record_counts = {100000, 500000, 1000000, 5000000};
    std::vector<WorkloadType> workload_types = {
        WorkloadType::WORKLOAD_A,
        WorkloadType::WORKLOAD_B,
        WorkloadType::WORKLOAD_C,
        WorkloadType::WORKLOAD_D,
        WorkloadType::WORKLOAD_E,
        WorkloadType::WORKLOAD_F
    };
    
    std::cout << "Generating YCSB workloads for all combinations..." << std::endl;
    
    // Generate uint64_t workloads
    for (size_t record_count : record_counts) {
        size_t operation_count = record_count; // Same number of operations as records
        for (WorkloadType workload_type : workload_types) {
            create_ycsb_workload<uint64_t>(workload_type, record_count, operation_count);
        }
    }
    
    // Generate CHAR16 workloads
    for (size_t record_count : record_counts) {
        size_t operation_count = record_count; // Same number of operations as records
        for (WorkloadType workload_type : workload_types) {
            create_ycsb_workload<CHAR16>(workload_type, record_count, operation_count);
        }
    }
    
    std::cout << "YCSB workload generation completed." << std::endl;
}

} // namespace ycsbworkloadgenerator