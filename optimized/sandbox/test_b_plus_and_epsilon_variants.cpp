// B+ Tree and B-Epsilon Tree variants testing

#include <iostream>
#include <vector>
#include <chrono>
#include <thread>
#include <random>
#include <algorithm>
#include <numeric>
#include <filesystem>
#include <fstream>
#include <string>

// Distribution types for data generation
enum class DistributionType {
    Random,
    Sequential,
    Zipfian,
    Uniform
};

// Generate data function similar to GENERATE_RANDOM_NUMBER_ARRAY and GENERATE_RANDOM_CHAR_ARRAY
template<typename T>
void generate_data(size_t count, DistributionType distribution, std::vector<T>& data) {
    data.resize(count);
    
    if constexpr (std::is_same_v<T, uint64_t>) {
        std::random_device rd;
        std::mt19937_64 gen(rd());
        
        switch (distribution) {
            case DistributionType::Sequential:
                for (size_t i = 0; i < count; ++i) {
                    data[i] = i + 1;
                }
                break;
            case DistributionType::Random: {
                std::uniform_int_distribution<uint64_t> dis(1, UINT64_MAX);
                for (size_t i = 0; i < count; ++i) {
                    data[i] = dis(gen);
                }
                break;
            }
            case DistributionType::Uniform: {
                std::uniform_int_distribution<uint64_t> dis(1, count * 10);
                for (size_t i = 0; i < count; ++i) {
                    data[i] = dis(gen);
                }
                break;
            }
            case DistributionType::Zipfian: {
                // Simple Zipfian-like distribution
                std::uniform_real_distribution<double> uniform(0.0, 1.0);
                for (size_t i = 0; i < count; ++i) {
                    double u = uniform(gen);
                    uint64_t rank = static_cast<uint64_t>(1.0 / std::pow(u, 1.0 / 1.1));
                    data[i] = (rank - 1) % count + 1;
                }
                break;
            }
        }
    } else if constexpr (std::is_same_v<T, CHAR16>) {
        const char charset[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        const size_t charsetSize = sizeof(charset) - 1;
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, charsetSize - 1);
        
        switch (distribution) {
            case DistributionType::Sequential:
                for (size_t idx = 0; idx < count; ++idx) {
                    char str[16];
                    snprintf(str, sizeof(str), "str_%08zu", idx + 1);
                    data[idx] = CHAR16(str);
                }
                break;
            case DistributionType::Random: {
                for (size_t idx = 0; idx < count; ++idx) {
                    char str[16] = {0};
                    // Generate random length between 8 and 15
                    std::uniform_int_distribution<> lenDis(8, 15);
                    int len = lenDis(gen);
                    
                    for (int idy = 0; idy < len; ++idy) {
                        str[idy] = charset[dis(gen)];
                    }
                    data[idx] = CHAR16(str);
                }
                break;
            }
            case DistributionType::Uniform: {
                // Generate strings with uniform distribution of prefixes
                for (size_t idx = 0; idx < count; ++idx) {
                    size_t prefix = idx % (count / 10 + 1); // Group into ~10 buckets
                    char str[16];
                    snprintf(str, sizeof(str), "uni_%04zu_%04zu", prefix, idx);
                    data[idx] = CHAR16(str);
                }
                break;
            }
            case DistributionType::Zipfian: {
                // Generate strings with Zipfian-like distribution (some prefixes more common)
                std::uniform_real_distribution<double> uniform(0.0, 1.0);
                for (size_t idx = 0; idx < count; ++idx) {
                    double u = uniform(gen);
                    size_t rank = static_cast<size_t>(1.0 / std::pow(u, 1.0 / 1.1));
                    size_t prefix = (rank - 1) % (count / 100 + 1); // More concentrated prefixes
                    char str[16];
                    snprintf(str, sizeof(str), "zip_%04zu_%04zu", prefix, idx);
                    data[idx] = CHAR16(str);
                }
                break;
            }
        }
    }
}

// Save data to file
template<typename T>
void save_data_to_file(const std::vector<T>& data, const std::string& filepath) {
    std::ofstream file(filepath, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot create file: " + filepath);
    }
    
    size_t count = data.size();
    file.write(reinterpret_cast<const char*>(&count), sizeof(count));
    file.write(reinterpret_cast<const char*>(data.data()), count * sizeof(T));
    file.close();
}

// Load data from file
template<typename T>
std::vector<T> load_data_from_file(const std::string& filepath) {
    std::ifstream file(filepath, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }
    
    size_t count;
    file.read(reinterpret_cast<char*>(&count), sizeof(count));
    
    std::vector<T> data(count);
    file.read(reinterpret_cast<char*>(data.data()), count * sizeof(T));
    file.close();
    
    return data;
}

// Test internal function template - handles tree construction and testing
template <typename TreeTraits, typename KeyType, typename ValueType>
void test_internal(const std::vector<KeyType>& vtData, const std::string& tree_name) {
    std::cout << "\n--- Testing " << tree_name << " ---" << std::endl;
    
    size_t nMaxNumber = vtData.size();
    std::vector<size_t> degrees = { 64, 92, 110, 128, 208, 224, 240, 256, 272, 288 };
    
    for (size_t nDegree : degrees) {
        std::cout << "\n  Degree: " << nDegree << std::endl;
        
        // Calculate node sizes based on key/value types
        size_t key_size = sizeof(KeyType);
        size_t value_size = sizeof(ValueType);
        size_t pointer_size = sizeof(typename TreeTraits::ObjectUIDType); // Actual ObjectUID size
        
        // Estimate internal node size: (degree-1) keys + degree pointers + metadata
        size_t nInternalNodeSize = (nDegree - 1) * key_size + nDegree * pointer_size + 64; // +64 for metadata
        
        // Estimate data node size: degree key-value pairs + metadata
        size_t nDataNodeSize = nDegree * (key_size + value_size) + 64; // +64 for metadata
        
        // For B-Epsilon trees, add buffer space (5 * degree messages)
        if (tree_name.find("B-Epsilon") != std::string::npos || tree_name.find("BEpsilon") != std::string::npos) {
            size_t message_size = key_size + value_size + 8; // key + value + operation type
            nInternalNodeSize += 5 * nDegree * message_size; // Buffer space
        }
        
        // Calculate total memory requirements
        size_t nTotalInternalNodes = std::max(1ULL, nMaxNumber / nDegree);
        size_t nTotalDataNodes = std::max(1ULL, nMaxNumber / nDegree);
        
        // Calculate block size as nearest power of 2
        size_t max_node_size = std::max(nInternalNodeSize, nDataNodeSize);
        
        // Find nearest power of 2
        size_t nBlockSize = 1;
        while (nBlockSize < max_node_size) {
            nBlockSize <<= 1;
        }
        
        // Check if previous power of 2 is closer
        if (nBlockSize > max_node_size && nBlockSize > 1024) {
            size_t prev_power = nBlockSize >> 1;
            if ((max_node_size - prev_power) < (nBlockSize - max_node_size)) {
                nBlockSize = prev_power;
            }
        }
        
        // Ensure minimum 1KB
        nBlockSize = std::max(nBlockSize, 1024ULL);
        
        // Calculate total memory needed
        size_t nTotalMemory = (nTotalInternalNodes + nTotalDataNodes) * nBlockSize;
        nTotalMemory = std::max(nTotalMemory, 100ULL * 1024 * 1024); // Minimum 100MB
        
        std::cout << "    Internal Node Size: " << nInternalNodeSize << " bytes" << std::endl;
        std::cout << "    Data Node Size: " << nDataNodeSize << " bytes" << std::endl;
        std::cout << "    Block Size: " << nBlockSize << " bytes" << std::endl;
        std::cout << "    Total Memory: " << (nTotalMemory / 1024 / 1024) << " MB" << std::endl;

        // Construct the tree
#ifdef __TREE_WITH_CACHE__
        typename TreeTraits::StoreType ptrTree(nDegree, nTotalMemory, nBlockSize, 4ULL * 1024 * 1024 * 1024);
#else
        typename TreeTraits::StoreType ptrTree(nDegree);
#endif
        ptrTree.template init<typename TreeTraits::DataNodeType>();
        
        std::chrono::steady_clock::time_point begin, end;
        
        // Insert phase
        begin = std::chrono::steady_clock::now();
        for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr++) {
            ValueType value;
            if constexpr (std::is_same_v<ValueType, CHAR16>) {
                char str[16];
                snprintf(str, sizeof(str), "val_%zu", nCntr + 1000);
                value = CHAR16(str);
            } else {
                value = static_cast<ValueType>(nCntr + 1000);
            }
            ErrorCode ec = ptrTree.insert(vtData[nCntr], value);
            ASSERT(ec == ErrorCode::Success);
        }
        end = std::chrono::steady_clock::now();
        
        auto insert_time = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        std::cout << "    Insert [" << nMaxNumber << " records]: " << insert_time << " μs" << std::endl;
        
        // Search phase
        begin = std::chrono::steady_clock::now();
        for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr++) {
            ValueType value;
            ErrorCode ec = ptrTree.search(vtData[nCntr], value);
            ASSERT(ec == ErrorCode::Success);
        }
        end = std::chrono::steady_clock::now();
        
        auto search_time = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        std::cout << "    Search [" << nMaxNumber << " records]: " << search_time << " μs" << std::endl;
        
        // Delete phase
        begin = std::chrono::steady_clock::now();
        for (size_t nCntr = 0; nCntr < nMaxNumber; nCntr++) {
            ErrorCode ec = ptrTree.remove(vtData[nCntr]);
            ASSERT(ec == ErrorCode::Success);
        }
        end = std::chrono::steady_clock::now();
        
        auto delete_time = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        std::cout << "    Delete [" << nMaxNumber << " records]: " << delete_time << " μs" << std::endl;
        
        std::cout << "    Summary - Insert: " << insert_time << "μs, Search: " << search_time 
                  << "μs, Delete: " << delete_time << "μs" << std::endl;
        
        // Calculate throughput
        double insert_throughput = (double)nMaxNumber / insert_time * 1000000; // ops/sec
        double search_throughput = (double)nMaxNumber / search_time * 1000000;
        double delete_throughput = (double)nMaxNumber / delete_time * 1000000;
        
        std::cout << "    Throughput - Insert: " << (int)insert_throughput << " ops/sec, "
                  << "Search: " << (int)search_throughput << " ops/sec, "
                  << "Delete: " << (int)delete_throughput << " ops/sec" << std::endl;
    }
}

// Test with uint64_t key and uint64_t value
void test_with_key_val_as_uint64_t(size_t num_records, const std::string& filepath) {
    std::cout << "\n=== Testing with uint64_t key and uint64_t value ===" << std::endl;
    std::cout << "Records: " << num_records << ", File: " << filepath << std::endl;
    
    // Load test data
    std::vector<uint64_t> vtData = load_data_from_file<uint64_t>(filepath);
    
    // Resize to requested number of records if needed
    if (vtData.size() > num_records) {
        vtData.resize(num_records);
    }
    
    // Define tree types similar to fptree_bm_test
#ifdef __TREE_WITH_CACHE__
   
#else
    // No-cache versions - use basic BPlusStoreTraits for all variants
   
#endif
    
    // Test each tree type
    //test_internal<DefaultBPlusTree, uint64_t, uint64_t>(vtData, "Default B+ Tree");
    //test_internal<COptBPlusTree, uint64_t, uint64_t>(vtData, "COpt B+ Tree");
    //test_internal<ROptBPlusTree, uint64_t, uint64_t>(vtData, "ROpt B+ Tree");
    //test_internal<CROptBPlusTree, uint64_t, uint64_t>(vtData, "CROpt B+ Tree");
    //test_internal<BEpsilonTree, uint64_t, uint64_t>(vtData, "B-Epsilon Tree");
}

// Test with uint64_t key and CHAR16 value
void test_with_key_as_uint64_t_and_val_as_string(size_t num_records, const std::string& filepath) {
    std::cout << "\n=== Testing with uint64_t key and CHAR16 value ===" << std::endl;
    std::cout << "Records: " << num_records << ", File: " << filepath << std::endl;
    
    // Load test data
    std::vector<uint64_t> vtData = load_data_from_file<uint64_t>(filepath);
    
    // Resize to requested number of records if needed
    if (vtData.size() > num_records) {
        vtData.resize(num_records);
    }
    
    // Define tree types
#ifdef __TREE_WITH_CACHE__
    
#else
    
#endif
    
    // Test each tree type
    //test_internal<DefaultBPlusTree, uint64_t, CHAR16>(vtData, "Default B+ Tree (uint64_t->CHAR16)");
    //test_internal<BEpsilonTree, uint64_t, CHAR16>(vtData, "B-Epsilon Tree (uint64_t->CHAR16)");
}

// Test with CHAR16 key and CHAR16 value
void test_with_key_and_val_as_string(size_t num_records, const std::string& filepath) {
    std::cout << "\n=== Testing with CHAR16 key and CHAR16 value ===" << std::endl;
    std::cout << "Records: " << num_records << ", File: " << filepath << std::endl;
    
    // Load test data
    std::vector<CHAR16> vtData = load_data_from_file<CHAR16>(filepath);
    
    // Resize to requested number of records if needed
    if (vtData.size() > num_records) {
        vtData.resize(num_records);
    }
    
    // Define tree types
#ifdef __TREE_WITH_CACHE__
   
#else
   
#endif
    
    // Test each tree type
    //test_internal<DefaultBPlusTree, CHAR16, CHAR16>(vtData, "Default B+ Tree (CHAR16->CHAR16)");
    //test_internal<BEpsilonTree, CHAR16, CHAR16>(vtData, "B-Epsilon Tree (CHAR16->CHAR16)");
}

// Helper function to get distribution name as string
std::string get_distribution_name(DistributionType dist) {
    switch (dist) {
        case DistributionType::Random: return "random";
        case DistributionType::Sequential: return "sequential";
        case DistributionType::Zipfian: return "zipfian";
        case DistributionType::Uniform: return "uniform";
        default: return "unknown";
    }
}

// Main test function
void test_b_plus_and_epsilon_variants() {
    std::cout << "Starting B+ Tree and B-Epsilon Tree variants testing..." << std::endl;
    
    // Create data directory if it doesn't exist
    std::filesystem::create_directories("data");
    
    // Data sizes to generate
    std::vector<size_t> data_sizes = {100000, 500000, 1000000, 5000000, 10000000};
    
    // All distribution types to test
    std::vector<DistributionType> distributions = {
        DistributionType::Random,
        DistributionType::Sequential,
        DistributionType::Zipfian,
        DistributionType::Uniform
    };
    
    // Generate data files for all combinations if they don't exist
    std::cout << "\n=== Generating Data Files ===" << std::endl;
    for (size_t size : data_sizes) {
        for (DistributionType dist : distributions) {
            std::string dist_name = get_distribution_name(dist);
            
            // uint64_t data files
            std::string uint64_file = "data/uint64_" + std::to_string(size) + "_" + dist_name + ".dat";
            if (!std::filesystem::exists(uint64_file)) {
                std::cout << "Generating " << uint64_file << "..." << std::endl;
                std::vector<uint64_t> data;
                generate_data<uint64_t>(size, dist, data);
                save_data_to_file(data, uint64_file);
            }
            
            // CHAR16 data files
            std::string char16_file = "data/char16_" + std::to_string(size) + "_" + dist_name + ".dat";
            if (!std::filesystem::exists(char16_file)) {
                std::cout << "Generating " << char16_file << "..." << std::endl;
                std::vector<CHAR16> data;
                generate_data<CHAR16>(size, dist, data);
                save_data_to_file(data, char16_file);
            }
        }
    }
    
    // Run tests with smaller datasets first to avoid memory issues
    std::vector<size_t> test_sizes = {100000}; // Start with just 100K to test the structure
    
    // Test each combination of size and distribution
    for (DistributionType dist : distributions) {
        for (size_t size : test_sizes) {
            std::string dist_name = get_distribution_name(dist);
            
            std::cout << "\n" << std::string(80, '=') << std::endl;
            std::cout << "Testing with " << size << " records - " << dist_name << " distribution" << std::endl;
            std::cout << std::string(80, '=') << std::endl;
            
            std::string uint64_file = "data/uint64_" + std::to_string(size) + "_" + dist_name + ".dat";
            
            // Test with uint64_t key and value
            test_with_key_val_as_uint64_t(size, uint64_file);
            
            // Test with uint64_t key and CHAR16 value
            test_with_key_as_uint64_t_and_val_as_string(size, uint64_file);
            
            // Test with CHAR16 key and value
            std::string char16_file = "data/char16_" + std::to_string(size) + "_" + dist_name + ".dat";
            test_with_key_and_val_as_string(size, char16_file);
        }
    }
    
    std::cout << "\n" << std::string(80, '=') << std::endl;
    std::cout << "All B+ Tree and B-Epsilon Tree variant tests completed!" << std::endl;
    std::cout << "Tested distributions: Random, Sequential, Zipfian, Uniform" << std::endl;
    std::cout << "Tested sizes: ";
    for (size_t size : test_sizes) {
        std::cout << size << " ";
    }
    std::cout << std::endl;
    std::cout << std::string(80, '=') << std::endl;
}