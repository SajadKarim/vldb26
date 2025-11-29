#pragma once

#include <fstream>
#include <string>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <stdexcept>

class CSVLogger {
private:
    std::ofstream file_;
    std::string filename_;
    bool header_written_;
    
public:
    CSVLogger(const std::string& filename, const std::string& output_dir = "") 
        : header_written_(false) {
        if (output_dir.empty()) {
            filename_ = filename;
        } else {
            filename_ = output_dir + "/" + filename;
        }
        file_.open(filename_, std::ios::out | std::ios::app);
        if (!file_.is_open()) {
            throw std::runtime_error("Cannot open CSV file: " + filename_);
        }
    }
    
    ~CSVLogger() {
        if (file_.is_open()) {
            file_.close();
        }
    }
    
    void write_header() {
        if (!header_written_) {
            file_ << "tree_type,policy_name,storage_type,config_name,cache_size,cache_page_limit,thread_count,timestamp,key_type,value_type,record_count,degree,operation,time_us,throughput_ops_sec,test_run_id,cache_hits,cache_misses,cache_evictions,cache_dirty_evictions,cache_hit_rate\n";
            file_.flush();
            header_written_ = true;
        }
    }
    
    void log_result(const std::string& tree_type,
                   const std::string& key_type,
                   const std::string& value_type,
                   const std::string& policy_name,
                   const std::string& storage_type,
                   const std::string& config_name,
                   size_t record_count,
                   size_t degree,
                   const std::string& operation,
                   long long time_us,
                   double throughput_ops_sec,
                   int test_run_id) {
        // Call the extended version with default cache stats and new fields
        log_result(tree_type, key_type, value_type, policy_name, storage_type, config_name, record_count, 
                  degree, operation, time_us, throughput_ops_sec, test_run_id,
                  0, 0, 0, 0, 0.0, "", 0, 1);
    }
    
    void log_result(const std::string& tree_type,
                   const std::string& key_type,
                   const std::string& value_type,
                   const std::string& policy_name,
                   const std::string& storage_type,
                   const std::string& config_name,
                   size_t record_count,
                   size_t degree,
                   const std::string& operation,
                   long long time_us,
                   double throughput_ops_sec,
                   int test_run_id,
                   uint64_t cache_hits,
                   uint64_t cache_misses,
                   uint64_t cache_evictions,
                   uint64_t cache_dirty_evictions,
                   double cache_hit_rate,
                   const std::string& cache_size,
                   size_t cache_page_limit,
                   int thread_count) {
        

        
        // Get current timestamp
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        
#ifdef _MSC_VER
        struct tm timeinfo;
        localtime_s(&timeinfo, &time_t);
        ss << std::put_time(&timeinfo, "%Y-%m-%d %H:%M:%S");
#else
        ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
#endif
        
        file_ << tree_type << ","
              << policy_name << ","
              << storage_type << ","
              << config_name << ","
              << cache_size << ","
              << cache_page_limit << ","
              << thread_count << ","
              << ss.str() << ","
              << key_type << ","
              << value_type << ","
              << record_count << ","
              << degree << ","
              << operation << ","
              << time_us << ","
              << std::fixed << std::setprecision(2) << throughput_ops_sec << ","
              << test_run_id << ","
              << cache_hits << ","
              << cache_misses << ","
              << cache_evictions << ","
              << cache_dirty_evictions << ","
              << std::fixed << std::setprecision(2) << cache_hit_rate << "\n";
        file_.flush();
    }
    
    // Static method to get unique filename with timestamp
    static std::string generate_filename(const std::string& prefix = "benchmark_results") {
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        
#ifdef _MSC_VER
        struct tm timeinfo;
        localtime_s(&timeinfo, &time_t);
        ss << prefix << "_" << std::put_time(&timeinfo, "%Y%m%d_%H%M%S") << ".csv";
#else
        ss << prefix << "_" << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S") << ".csv";
#endif
        return ss.str();
    }
};