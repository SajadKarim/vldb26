#include <iostream>
#include <string>
#include <map>
#include <vector>
#include <chrono>
#include <random>
#include <algorithm>
#include <numeric>
#include <filesystem>
#include <fstream>
#include <thread>
#include <memory>
#include <cassert>

#include "common.h"
#include "csv_logger.hpp"
#include "workloadgenerator.hpp"
#include "bm_bplus_with_cache.hpp"
#include "bm_bplus_with_cache_uint64_uint64.hpp"
#include "bm_bplus_with_cache_char16_char16.hpp"
#include "bm_bplus_with_cache_uint64_char16.hpp"

// Function to run full benchmark suite for all data type combinations
void run_full_benchmark_suite(
    const std::string& cache_type,
    int runs,
    const std::string& output_dir,
    const std::string& storage_type,
    int cache_size,
    double cache_percentage,
    int page_size,
    long long memory_size,
    const std::vector<std::string>& operations,
    const std::vector<size_t>& degrees,
    const std::vector<size_t>& record_counts,
    int threads,
    const std::string& config_name,
    const std::string& data_path) {
    
    // Define all supported key-value type combinations
    std::vector<std::pair<std::string, std::string>> type_combinations = {
        {"int", "int"},
        {"uint64_t", "uint64_t"},
        {"char16", "char16"},
        {"uint64_t", "char16"}
    };
    
    for (const auto& [key_type, value_type] : type_combinations) {
        std::cout << "\n=== Running benchmarks for " << key_type << " -> " << value_type << " ===" << std::endl;
        
        if (key_type == "int" && value_type == "int") {
            bm_bplus_with_cache::test_with_shell_parameters(
                cache_type, runs, output_dir, storage_type, cache_size, cache_percentage, page_size, memory_size,
                operations, degrees, record_counts, threads, config_name, data_path);
        } else {
            // For now, just print that other types are not yet implemented in full mode
            std::cout << "Full benchmark suite for " << key_type << " -> " << value_type 
                      << " not yet implemented" << std::endl;
        }
    }
}

// Function to run benchmark with specific key-value type combination
void run_benchmark_for_types(
    const std::string& key_type,
    const std::string& value_type,
    const std::string& cache_type,
    const std::string& storage_type,
    int cache_size,
    double cache_percentage,
    int page_size,
    long long memory_size,
    const std::string& operation,
    int degree,
    int records,
    int runs,
    int threads,
    const std::string& output_dir,
    const std::string& config_name,
    const std::string& data_path) {
    
    if (key_type == "int" && value_type == "int") {
        bm_bplus_with_cache::test_single_configuration(
            cache_type, storage_type, cache_size, cache_percentage, page_size, memory_size,
            key_type, value_type, operation, degree, records, runs, threads,
            output_dir, config_name, data_path);
    } else if (key_type == "uint64_t" && value_type == "uint64_t") {
        bm_bplus_with_cache_uint64_uint64::test_single_configuration(
            cache_type, storage_type, cache_size, cache_percentage, page_size, memory_size,
            key_type, value_type, operation, degree, records, runs, threads,
            output_dir, config_name, data_path);
    } else if (key_type == "char16" && value_type == "char16") {
        bm_bplus_with_cache_char16_char16::test_single_configuration(
            cache_type, storage_type, cache_size, cache_percentage, page_size, memory_size,
            key_type, value_type, operation, degree, records, runs, threads,
            output_dir, config_name, data_path);
    } else if (key_type == "uint64_t" && value_type == "char16") {
        bm_bplus_with_cache_uint64_char16::test_single_configuration(
            cache_type, storage_type, cache_size, cache_percentage, page_size, memory_size,
            key_type, value_type, operation, degree, records, runs, threads,
            output_dir, config_name, data_path);
    } else {
        std::cerr << "Error: Unsupported key-value type combination: " 
                  << key_type << " -> " << value_type << std::endl;
        std::cerr << "Supported combinations: int->int, uint64_t->uint64_t, char16->char16, uint64_t->char16" << std::endl;
    }
}

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [OPTIONS]\n";
    std::cout << "       " << program_name << " single <tree_type> <key_type> <value_type> <operation> <degree> [records] [runs]\n\n";
    std::cout << "Options:\n";
    std::cout << "  --config <config>      Configuration: bm_cache (default)\n";
    std::cout << "  --cache-type <type>    Cache type: LRU (default), SSARC, CLOCK\n";
    std::cout << "  --cache-size <size>    Cache size (default: 100)\n";
    std::cout << "  --storage-type <type>  Storage type: VolatileStorage (default), FileStorage\n";
    std::cout << "  --page-size <size>     Page size (default: 4096)\n";
    std::cout << "  --memory-size <size>   Memory size in bytes (default: 1073741824 = 1GB)\n";
    std::cout << "  --tree-type <type>     Tree type: BPlusStore\n";
    std::cout << "  --key-type <type>      Key type: int, uint64_t, char16\n";
    std::cout << "  --value-type <type>    Value type: int, uint64_t, char16\n";
    std::cout << "  --operation <op>       Operation: insert, search_random, search_sequential, search_uniform, search_zipfian, delete\n";
    std::cout << "  --degree <degree>      Tree degree (16-320)\n";
    std::cout << "  --records <count>      Number of records (100000, 500000, 1000000, 5000000, 10000000)\n";
    std::cout << "  --runs <count>         Number of test runs (default: 1)\n";
    std::cout << "  --threads <count>      Number of threads for concurrent operations (default: 1)\n";
    std::cout << "  --output-dir <dir>     Output directory for CSV files (default: current directory)\n";
    std::cout << "  --config-name <name>   Configuration name for CSV logging (default: empty)\n";
    std::cout << "  --data-path <path>     Data files directory (default: /home/skarim/Code/haldendb_ex/haldendb/benchmark/data)\n";
    std::cout << "  --help                 Show this help message\n";
    std::cout << "\nPositional Arguments (single mode):\n";
    std::cout << "  tree_type              Tree type (required)\n";
    std::cout << "  key_type               Key type (required)\n";
    std::cout << "  value_type             Value type (required)\n";
    std::cout << "  operation              Operation (required)\n";
    std::cout << "  degree                 Tree degree (required)\n";
    std::cout << "  records                Number of records (optional, default: 100000)\n";
    std::cout << "  runs                   Number of test runs (optional, default: 1)\n";
    std::cout << "\nExamples:\n";
    std::cout << "  " << program_name << " single BPlusStore int int insert 64 100000 1\n";
    std::cout << "  " << program_name << " single BPlusStore uint64_t uint64_t search 128\n";
    std::cout << "  " << program_name << " single BPlusStore char16 char16 insert 64\n";
    std::cout << "  " << program_name << " single BPlusStore uint64_t char16 search 128\n";
    std::cout << "  " << program_name << " --config bm_cache --runs 3\n";
    std::cout << "  " << program_name << " --runs 5\n";
    std::cout << "\nIf no arguments provided, runs full benchmark suite for default configuration.\n";
    std::cout << "The --runs parameter works in both single and full benchmark modes.\n";
}

int main(int argc, char* argv[])
{
    // Parse command line arguments
    std::map<std::string, std::string> args;
    std::vector<std::string> positional_args;
    
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if (arg == "--help") {
            print_usage(argv[0]);
            return 0;
        }
        
        if (arg.substr(0, 2) == "--" && i + 1 < argc) {
            args[arg.substr(2)] = argv[i + 1];
            i++; // Skip next argument as it's the value
        } else {
            positional_args.push_back(arg);
        }
    }
    
    // Handle positional arguments for single configuration mode
    // Format: single <tree_type> <key_type> <value_type> <operation> <degree> <records> <runs>
    if (!positional_args.empty() && positional_args[0] == "single") {
        if (positional_args.size() >= 6) {
            // Only set default config if not already specified
            if (!args.count("config")) {
                args["config"] = "bm_cache";  // Default to cache
            }
            args["tree-type"] = positional_args[1];
            args["key-type"] = positional_args[2];
            args["value-type"] = positional_args[3];
            args["operation"] = positional_args[4];
            args["degree"] = positional_args[5];
            if (positional_args.size() >= 7) args["records"] = positional_args[6];
            if (positional_args.size() >= 8) args["runs"] = positional_args[7];
        } else {
            std::cerr << "Error: 'single' mode requires at least 6 arguments: single <tree_type> <key_type> <value_type> <operation> <degree> [records] [runs]\n";
            print_usage(argv[0]);
            return 1;
        }
    }
    
    // Get configuration (default to bm_cache)
    std::string config = args.count("config") ? args["config"] : "bm_cache";
    
    // Get cache type (default to LRU)
    std::string cache_type = args.count("cache-type") ? args["cache-type"] : "LRU";
    
    // Get storage type (default to VolatileStorage)
    std::string storage_type = args.count("storage-type") ? args["storage-type"] : "VolatileStorage";
    
    // Get cache size (default to 100)
    int cache_size = args.count("cache-size") ? std::stoi(args["cache-size"]) : 100;
    
    // Get cache percentage (default to 0.05 for 5%)
    double cache_percentage = args.count("cache-percentage") ? std::stod(args["cache-percentage"]) : 0.05;
    
    // Get page size (default to 4096)
    int page_size = args.count("page-size") ? std::stoi(args["page-size"]) : 4096;
    
    // Get memory size (default to 1GB)
    long long memory_size = args.count("memory-size") ? std::stoll(args["memory-size"]) : 1073741824LL;
    
    // Get runs parameter (default to 1)
    int runs = args.count("runs") ? std::stoi(args["runs"]) : 1;
    
    // Get threads parameter (default to 1)
    int threads = args.count("threads") ? std::stoi(args["threads"]) : 1;
    
    // Get config name parameter (default to empty)
    std::string config_name = args.count("config-name") ? args["config-name"] : "";
    
    // Get data path parameter (default to hardcoded path)
    std::string data_path = args.count("data-path") ? args["data-path"] : "/home/skarim/Code/haldendb_ex/haldendb/benchmark/data";
    
    // Determine benchmark mode:
    // Full benchmark mode: no tree-type specified
    // Single benchmark mode: tree-type specified
    if (!args.count("tree-type")) {
        std::cout << "BPlusStore Benchmark Suite\n";
        
        // Generate workloads if they don't exist
        std::cout << "Ensuring workload data files exist..." << std::endl;
        workloadgenerator::generate_all_workloads(data_path);
        std::cout << "Workload generation completed." << std::endl;
        
        // Run full benchmark based on configuration
        if (config == "bm_cache") {
#ifdef __TREE_WITH_CACHE__
            std::cout << "Testing BPlusStore with " << cache_type << " Cache..." << std::endl;
            std::cout << "Number of runs per configuration: " << runs << std::endl;
            std::string output_dir = args.count("output-dir") ? args["output-dir"] : "";
            
            // Prepare operations, degrees, and record counts based on provided parameters
            std::vector<std::string> operations = {"insert", "search_random", "search_sequential", "search_uniform", "search_zipfian", "delete"};
            std::vector<size_t> degrees = {64, 128};
            std::vector<size_t> record_counts = {100000, 500000, 1000000};
            
            // Override with specific parameters if provided
            if (args.count("operation")) {
                operations = {args["operation"]};
            }
            if (args.count("degree")) {
                degrees = {static_cast<size_t>(std::stoi(args["degree"]))};
            }
            if (args.count("records")) {
                record_counts = {static_cast<size_t>(std::stoi(args["records"]))};
            }
            
            run_full_benchmark_suite(
                cache_type, runs, output_dir, storage_type, cache_size, cache_percentage, page_size, memory_size,
                operations, degrees, record_counts, threads, config_name, data_path);
#else
            std::cerr << "Error: Cache configuration not enabled. Please build with -D__TREE_WITH_CACHE__" << std::endl;
            return 1;
#endif
        } else {
            std::cerr << "Error: Unknown configuration: " << config << std::endl;
            std::cerr << "Available configurations: bm_cache" << std::endl;
            return 1;
        }
        return 0;
    } else if (args.empty()) {
        // No arguments at all - run default configuration
        std::cout << "BPlusStore Benchmark Suite\n";
        
        // Generate workloads if they don't exist
        std::cout << "Ensuring workload data files exist..." << std::endl;
        workloadgenerator::generate_all_workloads(data_path);
        std::cout << "Workload generation completed." << std::endl;
        
        // Default to cache configuration
#ifdef __TREE_WITH_CACHE__
        std::cout << "Testing BPlusStore with LRU Cache..." << std::endl;
        std::cout << "Number of runs per configuration: " << runs << std::endl;
        
        std::vector<std::string> operations = {"insert", "search_random", "search_sequential", "search_uniform", "search_zipfian", "delete"};
        std::vector<size_t> degrees = {64, 128};
        std::vector<size_t> record_counts = {100000, 500000};
        
        run_full_benchmark_suite(
            "LRU", runs, "", "VolatileStorage", 100, 0.05, 4096, 1073741824LL,
            operations, degrees, record_counts, 1, "", data_path);
#else
        std::cerr << "Error: Cache configuration not enabled. Please build with -D__TREE_WITH_CACHE__" << std::endl;
        return 1;
#endif
        return 0;
    }
    
    // Single benchmark mode
    std::string tree_type = args["tree-type"];
    std::string key_type = args["key-type"];
    std::string value_type = args["value-type"];
    std::string operation = args["operation"];
    int degree = std::stoi(args["degree"]);
    int records = args.count("records") ? std::stoi(args["records"]) : 100000;
    
    std::cout << "Running single BPlusStore benchmark:\n";
    std::cout << "Tree: " << tree_type << ", Cache: " << cache_type << "/" << storage_type << "\n";
    std::cout << "Key/Value: " << key_type << "/" << value_type << ", Operation: " << operation << "\n";
    std::cout << "Degree: " << degree << ", Records: " << records << ", Runs: " << runs << "\n";
    std::cout << "Threads: " << threads << ", Cache Size: " << cache_size << "\n";
    
    // Generate workloads if they don't exist (needed for single configuration mode)
    std::cout << "Ensuring workload data files exist..." << std::endl;
    workloadgenerator::generate_all_workloads(data_path);
    std::cout << "Workload generation completed." << std::endl;
    
    if (config == "bm_cache") {
#ifdef __TREE_WITH_CACHE__
        std::string output_dir = args.count("output-dir") ? args["output-dir"] : "";
        
        run_benchmark_for_types(
            key_type, value_type, cache_type, storage_type, cache_size, cache_percentage, page_size, memory_size,
            operation, degree, records, runs, threads, output_dir, config_name, data_path);
#else
        std::cerr << "Error: Cache configuration not enabled. Please build with -D__TREE_WITH_CACHE__" << std::endl;
        return 1;
#endif
    } else {
        std::cerr << "Error: Unknown configuration: " << config << std::endl;
        return 1;
    }
    
    return 0;
}