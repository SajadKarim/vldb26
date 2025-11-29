#include <iostream>
#include <string>
#include <map>
#include "common.h"
#include "workloadgenerator.hpp"
#include "ycsbworkloadgenerator.hpp"
#include "bm_tree_with_no_cache.hpp"
#include "bm_tree_with_cache_real.hpp"
#include "bm_tree_with_cache_ycsb.hpp"

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [OPTIONS]\n";
    std::cout << "       " << program_name << " single <tree_type> <key_type> <value_type> <operation> <degree> [records] [runs]\n\n";
    std::cout << "Options:\n";
    std::cout << "  --config <config>      Configuration: bm_nocache (default), bm_cache, bm_cache_ycsb\n";
    std::cout << "  --cache-type <type>    Cache type: LRU (default), CLOCK, A2Q (only for bm_cache)\n";
    std::cout << "  --cache-size <size>    Cache size (default: 100)\n";
    std::cout << "  --storage-type <type>  Storage type: VolatileStorage (default), FileStorage, PMemStorage (only for bm_cache)\n";
    std::cout << "  --page-size <size>     Page size (default: 2048)\n";
    std::cout << "  --memory-size <size>   Memory size in bytes (default: 1073741824 = 1GB)\n";
    std::cout << "  --tree-type <type>     Tree type: BplusTreeSOA, BplusTreeAOS, BepsilonTreeSOA,\n";
    std::cout << "                         BepsilonTreeAOS, BepsilonTreeSOALazyNodes, BepsilonTreeSOALazyIndex,\n";
    std::cout << "                         BepsilonTreeSOAII\n";
    std::cout << "  --key-type <type>      Key type: uint64_t, char16\n";
    std::cout << "  --value-type <type>    Value type: uint64_t, char16\n";
    std::cout << "  --operation <op>       Operation: insert, delete, search_random, search_sequential,\n";
    std::cout << "                         search_uniform, search_zipfian\n";
    std::cout << "  --workload-type <wl>   YCSB Workload: ycsb_a, ycsb_b, ycsb_c, ycsb_d, ycsb_e, ycsb_f\n";
    std::cout << "                         (only for bm_cache_ycsb config)\n";
    std::cout << "  --degree <degree>      Tree degree (16-320)\n";
    std::cout << "  --records <count>      Number of records (100000, 500000, 1000000, 5000000, 10000000)\n";
    std::cout << "  --runs <count>         Number of test runs (default: 1)\n";
    std::cout << "  --threads <count>      Number of threads for concurrent operations (default: 1)\n";
    std::cout << "  --output-dir <dir>     Output directory for CSV files (default: current directory)\n";
    std::cout << "  --config-name <name>   Configuration name for CSV logging (default: empty)\n";
    std::cout << "  --cache-size-percentage <pct>  Cache size percentage for CSV logging (e.g., '10%')\n";
    std::cout << "  --cache-page-limit <limit>     Cache page limit for CSV logging (numeric value)\n";
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
    std::cout << "  " << program_name << " single BepsilonTreeSOA uint64_t uint64_t insert 64 100000 1\n";
    std::cout << "  " << program_name << " single BplusTreeSOA uint64_t char16 search_random 128\n";
    std::cout << "  " << program_name << " --config bm_nocache --runs 3\n";
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
                args["config"] = "bm_nocache";  // Default to no cache
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
    
    // Get configuration (default to bm_nocache)
    std::string config = args.count("config") ? args["config"] : "bm_nocache";
    
    // Get cache type (default to LRU)
    std::string cache_type = args.count("cache-type") ? args["cache-type"] : "LRU";
    
    // Get storage type (default to VolatileStorage)
    std::string storage_type = args.count("storage-type") ? args["storage-type"] : "VolatileStorage";
    
    // Get cache size (default to 100)
    int cache_size = args.count("cache-size") ? std::stoi(args["cache-size"]) : 100;
    
    // Get page size (default to 2048)
    int page_size = args.count("page-size") ? std::stoi(args["page-size"]) : 2048;
    
    // Get memory size (default to 1GB)
    long long memory_size = args.count("memory-size") ? std::stoll(args["memory-size"]) : 1073741824LL;
    
    // Get runs parameter (default to 1)
    int runs = args.count("runs") ? std::stoi(args["runs"]) : 1;
    
    // Get threads parameter (default to 1)
    int threads = args.count("threads") ? std::stoi(args["threads"]) : 1;
    
    // Get config name parameter (default to empty)
    std::string config_name = args.count("config-name") ? args["config-name"] : "";
    
    // Get cache size percentage parameter (default to empty)
    std::string cache_size_percentage = args.count("cache-size-percentage") ? args["cache-size-percentage"] : "";
    
    // Get cache page limit parameter (default to 0)
    size_t cache_page_limit = args.count("cache-page-limit") ? std::stoull(args["cache-page-limit"]) : 0;
    
    // Debug output

    
    // Determine benchmark mode:
    // Full benchmark mode: no tree-type specified
    // Single benchmark mode: tree-type specified
    if (!args.count("tree-type")) {
        std::cout << "Benchmark workload generator\n";
        
        // Generate all basic workloads
        workloadgenerator::generate_all_workloads();
        
        std::cout << "\n" << std::endl;
        
        // Generate all YCSB workloads
        ycsbworkloadgenerator::generate_all_ycsb_workloads();
        
        std::cout << "\n" << std::endl;
        
        // Run full benchmark based on configuration
        if (config == "bm_nocache") {
#ifndef __TREE_WITH_CACHE__
            std::cout << "Testing B+ Tree with No Cache..." << std::endl;
            std::cout << "Number of runs per configuration: " << runs << std::endl;
            bm_tree_with_no_cache::test(runs);
#endif
        } else if (config == "bm_cache") {
#ifdef __TREE_WITH_CACHE__
            std::cout << "Testing B+ Tree with " << cache_type << " Cache..." << std::endl;
            std::cout << "Number of runs per configuration: " << runs << std::endl;
            std::string output_dir = args.count("output-dir") ? args["output-dir"] : "";
            
            // Prepare operations, degrees, and record counts based on provided parameters
            std::vector<std::string> operations = {"insert", "search_random", "search_sequential", "search_uniform", "search_zipfian", "delete"};
            std::vector<size_t> degrees = {64, 128, 256};
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
            
            bm_tree_with_cache_real::test_with_shell_parameters(
                cache_type, runs, output_dir, storage_type, cache_size, page_size, memory_size,
                operations, degrees, record_counts, threads, config_name, cache_size_percentage, cache_page_limit);
#endif
        } else if (config == "bm_cache_ycsb") {
#ifdef __TREE_WITH_CACHE__
            std::cout << "Testing B+ Tree with " << cache_type << " Cache (YCSB Workloads)..." << std::endl;
            std::cout << "Number of runs per configuration: " << runs << std::endl;
            std::string output_dir = args.count("output-dir") ? args["output-dir"] : "";
            
            // Prepare YCSB workloads, degrees, and record counts based on provided parameters
            std::vector<std::string> workload_types = {"ycsb_a", "ycsb_b", "ycsb_c", "ycsb_d", "ycsb_e", "ycsb_f"};
            std::vector<size_t> degrees = {64, 128, 256};
            std::vector<size_t> record_counts = {100000, 500000, 1000000};
            
            // Override with specific parameters if provided
            if (args.count("workload-type")) {
                workload_types = {args["workload-type"]};
            }
            if (args.count("degree")) {
                degrees = {static_cast<size_t>(std::stoi(args["degree"]))};
            }
            if (args.count("records")) {
                record_counts = {static_cast<size_t>(std::stoi(args["records"]))};
            }
            
            bm_tree_with_cache_ycsb::test_ycsb_with_shell_parameters(
                cache_type, runs, output_dir, storage_type, cache_size, page_size, memory_size,
                workload_types, degrees, record_counts, threads, config_name, cache_size_percentage, cache_page_limit);
#endif
        } else {
            std::cerr << "Error: Unknown configuration: " << config << std::endl;
            std::cerr << "Available configurations: bm_nocache, bm_cache, bm_cache_ycsb" << std::endl;
            return 1;
        }
        return 0;
    } else if (args.empty()) {
        // No arguments at all - run default configuration
        std::cout << "Benchmark workload generator\n";
        
        // Generate all basic workloads
        workloadgenerator::generate_all_workloads();
        
        std::cout << "\n" << std::endl;
        
        // Generate all YCSB workloads
        ycsbworkloadgenerator::generate_all_ycsb_workloads();
        
        std::cout << "\n" << std::endl;
        
        // Default to no cache configuration
#ifndef __TREE_WITH_CACHE__
        std::cout << "Testing B+ Tree with No Cache..." << std::endl;
        std::cout << "Number of runs per configuration: " << runs << std::endl;
        bm_tree_with_no_cache::test(runs);
#endif
        return 0;
    }
    
    // Single tree/operation benchmark mode
    std::cout << "Running single benchmark configuration..." << std::endl;
    
    // Extract parameters
    std::string tree_type = args.count("tree-type") ? args["tree-type"] : "";
    std::string key_type = args.count("key-type") ? args["key-type"] : "uint64_t";
    std::string value_type = args.count("value-type") ? args["value-type"] : "uint64_t";
    std::string operation = args.count("operation") ? args["operation"] : "";
    std::string workload_type = args.count("workload-type") ? args["workload-type"] : "";
    int degree = args.count("degree") ? std::stoi(args["degree"]) : 64;
    int records = args.count("records") ? std::stoi(args["records"]) : 100000;
    // runs is already declared earlier
    std::string output_dir = args.count("output-dir") ? args["output-dir"] : "";
    
    // Validate required parameters for single benchmark mode
    if (config == "bm_cache_ycsb") {
        if (tree_type.empty() || workload_type.empty()) {
            std::cerr << "Error: --tree-type and --workload-type are required for YCSB benchmark mode\n";
            print_usage(argv[0]);
            return 1;
        }
    } else {
        if (tree_type.empty() || operation.empty()) {
            std::cerr << "Error: --tree-type and --operation are required for single benchmark mode\n";
            print_usage(argv[0]);
            return 1;
        }
    }
    
    // Generate workloads if they don't exist (needed for single configuration mode)
    std::cout << "Ensuring workload data files exist..." << std::endl;
    workloadgenerator::generate_all_workloads();
    ycsbworkloadgenerator::generate_all_ycsb_workloads();
    std::cout << "Workload generation completed." << std::endl;
    
    // Print configuration
    std::cout << "Configuration:\n";
    std::cout << "  Config: " << config << "\n";
    if (config == "bm_cache" || config == "bm_cache_ycsb") {
        std::cout << "  Cache Type: " << cache_type << "\n";
        std::cout << "  Cache Size: " << cache_size << "\n";
        std::cout << "  Storage Type: " << storage_type << "\n";
        std::cout << "  Page Size: " << page_size << "\n";
        std::cout << "  Memory Size: " << memory_size << "\n";
    }
    std::cout << "  Tree Type: " << tree_type << "\n";
    std::cout << "  Key Type: " << key_type << "\n";
    std::cout << "  Value Type: " << value_type << "\n";
    if (config == "bm_cache_ycsb") {
        std::cout << "  Workload Type: " << workload_type << "\n";
    } else {
        std::cout << "  Operation: " << operation << "\n";
    }
    std::cout << "  Degree: " << degree << "\n";
    std::cout << "  Records: " << records << "\n";
    std::cout << "  Runs: " << runs << "\n";
    std::cout << "  Threads: " << threads << "\n";
    std::cout << "  Output Dir: " << (output_dir.empty() ? "current directory" : output_dir) << "\n";
    
    // Call appropriate benchmark function based on configuration
    if (config == "bm_nocache") {
#ifndef __TREE_WITH_CACHE__
        bm_tree_with_no_cache::test_single_configuration(
            tree_type, key_type, value_type, operation, degree, records, runs, output_dir);
#else
        std::cerr << "Error: bm_nocache configuration not available in this build" << std::endl;
        return 1;
#endif
    } else if (config == "bm_cache") {
#ifdef __TREE_WITH_CACHE__
        bm_tree_with_cache_real::test_single_config(
            tree_type, key_type, value_type, operation, degree, records, runs, output_dir, 
            cache_type, storage_type, cache_size, page_size, memory_size, threads, config_name,
            cache_size_percentage, cache_page_limit);
#else
        std::cerr << "Error: bm_cache configuration not available in this build" << std::endl;
        return 1;
#endif
    } else if (config == "bm_cache_ycsb") {
#ifdef __TREE_WITH_CACHE__
        // For YCSB, we need to call a different function that handles workload types
        // Note: This is a simplified single config - for full testing use the shell script
        std::vector<std::string> workload_types = {workload_type};
        std::vector<size_t> degrees_vec = {static_cast<size_t>(degree)};
        std::vector<size_t> records_vec = {static_cast<size_t>(records)};
        
        bm_tree_with_cache_ycsb::test_ycsb_with_shell_parameters(
            cache_type, runs, output_dir, storage_type, cache_size, page_size, memory_size,
            workload_types, degrees_vec, records_vec, threads, config_name, 
            cache_size_percentage, cache_page_limit);
#else
        std::cerr << "Error: bm_cache_ycsb configuration not available in this build" << std::endl;
        return 1;
#endif
    } else {
        std::cerr << "Error: Unknown configuration: " << config << std::endl;
        std::cerr << "Available configurations: bm_nocache, bm_cache, bm_cache_ycsb" << std::endl;
        return 1;
    }
    
    return 0;
}