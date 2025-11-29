/**
 * Policy Selector CLI
 * 
 * Command-line utility to query DeviceAwarePolicy for optimal cache configuration.
 * Can be called from shell scripts to determine which policy to use.
 * 
 * Usage:
 *   ./policy_selector_cli --workload ycsb_a --storage VolatileStorage
 *   ./policy_selector_cli --workload ycsb_c --storage PMemStorage --verbose
 *   ./policy_selector_cli --print-matrix
 */

#include <iostream>
#include <string>
#include <algorithm>
#include "../libcache/DeviceAwarePolicy.hpp"

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [OPTIONS]\n\n";
    std::cout << "Options:\n";
    std::cout << "  --workload <type>    YCSB workload type (ycsb_a, ycsb_b, ycsb_c, ycsb_d, ycsb_e, ycsb_f)\n";
    std::cout << "  --storage <type>     Storage device type (VolatileStorage, PMemStorage, FileStorage, IOURingStorage)\n";
    std::cout << "  --verbose            Print detailed information including rationale\n";
    std::cout << "  --print-matrix       Print the entire decision matrix\n";
    std::cout << "  --help               Show this help message\n\n";
    std::cout << "Output format (default):\n";
    std::cout << "  <cache_policy>,<build_config>\n";
    std::cout << "  Example: A2Q,non_concurrent_relaxed\n\n";
    std::cout << "Output format (verbose):\n";
    std::cout << "  Policy: <cache_policy>\n";
    std::cout << "  Config: <build_config>\n";
    std::cout << "  Rationale: <selection_rationale>\n";
}

int main(int argc, char* argv[]) {
    std::string workload_str;
    std::string storage_str;
    bool verbose = false;
    bool print_matrix = false;
    
    // Parse command-line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        
        if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }
        else if (arg == "--workload" && i + 1 < argc) {
            workload_str = argv[++i];
        }
        else if (arg == "--storage" && i + 1 < argc) {
            storage_str = argv[++i];
        }
        else if (arg == "--verbose" || arg == "-v") {
            verbose = true;
        }
        else if (arg == "--print-matrix") {
            print_matrix = true;
        }
        else {
            std::cerr << "Unknown option: " << arg << std::endl;
            print_usage(argv[0]);
            return 1;
        }
    }
    
    DeviceAware::DeviceAwarePolicy policy;
    
    // Print decision matrix if requested
    if (print_matrix) {
        policy.printDecisionMatrix();
        return 0;
    }
    
    // Validate required arguments
    if (workload_str.empty() || storage_str.empty()) {
        std::cerr << "Error: Both --workload and --storage are required\n" << std::endl;
        print_usage(argv[0]);
        return 1;
    }
    
    // Parse workload and storage
    auto workload = DeviceAware::DeviceAwarePolicy::parseWorkload(workload_str);
    auto storage = DeviceAware::DeviceAwarePolicy::parseStorage(storage_str);
    
    // Validate parsing
    if (workload == DeviceAware::WorkloadType::UNKNOWN) {
        std::cerr << "Error: Unknown workload type: " << workload_str << std::endl;
        std::cerr << "Valid types: ycsb_a, ycsb_b, ycsb_c, ycsb_d, ycsb_e, ycsb_f" << std::endl;
        return 1;
    }
    
    if (storage == DeviceAware::StorageDeviceType::UNKNOWN) {
        std::cerr << "Error: Unknown storage type: " << storage_str << std::endl;
        std::cerr << "Valid types: VolatileStorage, PMemStorage, FileStorage, IOURingStorage" << std::endl;
        return 1;
    }
    
    // Get policy recommendation
    auto config = policy.selectPolicy(workload, storage);
    
    // Output results
    if (verbose) {
        std::cout << "Workload: " << DeviceAware::DeviceAwarePolicy::getWorkloadName(workload) << std::endl;
        std::cout << "Storage: " << DeviceAware::DeviceAwarePolicy::getStorageName(storage) << std::endl;
        std::cout << "Policy: " << config.policy_name << std::endl;
        std::cout << "Config: " << config.build_config << std::endl;
        std::cout << "Rationale: " << config.selection_rationale << std::endl;
        
        // Print configuration flags
        std::cout << "\nConfiguration Flags:" << std::endl;
        std::cout << "  Concurrent: " << (config.enable_concurrent ? "yes" : "no") << std::endl;
        std::cout << "  Selective Update: " << (config.enable_selective_update ? "yes" : "no") << std::endl;
        std::cout << "  Update In Order: " << (config.enable_update_in_order ? "yes" : "no") << std::endl;
        std::cout << "  Manage Ghost Queue: " << (config.enable_manage_ghost_q ? "yes" : "no") << std::endl;
        std::cout << "  CLOCK with Buffer: " << (config.enable_clock_with_buffer ? "yes" : "no") << std::endl;
    } else {
        // Simple output for shell script parsing
        std::cout << config.policy_name << "," << config.build_config << std::endl;
    }
    
    return 0;
}