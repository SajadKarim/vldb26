#pragma once

#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <filesystem>
#include <chrono>
#include <iomanip>
#include "common.h"

class CSVLogger {
private:
    std::ofstream file_;
    std::string filename_;
    bool header_written_;
    
public:
    CSVLogger(const std::string& filename) 
        : filename_(filename), header_written_(false) {
        
        // Ensure directory exists
        std::filesystem::path filepath(filename);
        if (filepath.has_parent_path()) {
            std::filesystem::create_directories(filepath.parent_path());
        }
        
        file_.open(filename, std::ios::out | std::ios::app);
        if (!file_.is_open()) {
            throw std::runtime_error("Cannot open CSV file: " + filename);
        }
    }
    
    ~CSVLogger() {
        if (file_.is_open()) {
            file_.close();
        }
    }
    
    void write_header() {
        if (!header_written_) {
            file_ << "tree_type,policy_name,storage_type,config_name,cache_size,cache_page_limit,"
                  << "thread_count,timestamp,key_type,value_type,record_count,degree,"
                  << "operation,time_us,throughput_ops_sec,test_run_id";
#ifdef __CACHE_COUNTERS__
            file_ << ",cache_hits,cache_misses,cache_evictions,cache_dirty_evictions,cache_hit_rate";
#endif //__CACHE_COUNTERS__
            file_ << "\n";
            header_written_ = true;
        }
    }
    
    void log_result(const BenchmarkResult& result) {
        write_header();
        
        file_ << result.tree_type << ","
              << result.policy_name << ","
              << result.storage_type << ","
              << result.config_name << ","
              << result.cache_size << ","
              << result.cache_page_limit << ","
              << result.thread_count << ","
              << result.timestamp << ","
              << result.key_type << ","
              << result.value_type << ","
              << result.record_count << ","
              << result.degree << ","
              << result.operation << ","
              << std::fixed << std::setprecision(0) << duration_to_microseconds(result.duration) << ","
              << std::fixed << std::setprecision(2) << result.throughput_ops_sec << ","
              << result.test_run_id;
#ifdef __CACHE_COUNTERS__
        file_ << "," << result.cache_hits
              << "," << result.cache_misses
              << "," << result.cache_evictions
              << "," << result.cache_dirty_evictions
              << "," << std::fixed << std::setprecision(4) << result.cache_hit_rate;
#endif //__CACHE_COUNTERS__
        file_ << "\n";
        
        file_.flush();
    }
    
    void log_results(const std::vector<BenchmarkResult>& results) {
        for (const auto& result : results) {
            log_result(result);
        }
    }
    
    static std::string generate_filename(const std::string& prefix = "benchmark",
                                       const std::string& suffix = "",
                                       const std::string& output_dir = "") {
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        auto tm = *std::localtime(&time_t);
        
        std::ostringstream oss;
        oss << prefix << "_" 
            << std::put_time(&tm, "%Y%m%d_%H%M%S");
        
        if (!suffix.empty()) {
            oss << "_" << suffix;
        }
        
        oss << ".csv";
        
        if (output_dir.empty()) {
            return oss.str();
        } else {
            return output_dir + "/" + oss.str();
        }
    }
    
    void flush() {
        file_.flush();
    }
    
    bool is_open() const {
        return file_.is_open();
    }
    
    const std::string& get_filename() const {
        return filename_;
    }
};

// Utility class for batch CSV logging
class BatchCSVLogger {
private:
    std::vector<BenchmarkResult> results_;
    std::string output_dir_;
    std::string prefix_;
    
public:
    BatchCSVLogger(const std::string& output_dir = "", const std::string& prefix = "benchmark")
        : output_dir_(output_dir), prefix_(prefix) {}
    
    void add_result(const BenchmarkResult& result) {
        results_.push_back(result);
    }
    
    void add_results(const std::vector<BenchmarkResult>& results) {
        results_.insert(results_.end(), results.begin(), results.end());
    }
    
    void flush_to_file(const std::string& suffix = "") {
        if (results_.empty()) {
            return;
        }
        
        std::string filename = CSVLogger::generate_filename(prefix_, suffix, output_dir_);
        CSVLogger logger(filename);
        logger.log_results(results_);
        
        std::cout << "Results saved to: " << filename << " (" << results_.size() << " entries)" << std::endl;
        results_.clear();
    }
    
    void clear() {
        results_.clear();
    }
    
    size_t size() const {
        return results_.size();
    }
    
    bool empty() const {
        return results_.empty();
    }
    
    const std::vector<BenchmarkResult>& get_results() const {
        return results_;
    }
};

// Specialized logger for latency measurements
class LatencyLogger {
private:
    std::ofstream file_;
    std::string filename_;
    bool header_written_;
    
public:
    LatencyLogger(const std::string& filename) 
        : filename_(filename), header_written_(false) {
        
        // Ensure directory exists
        std::filesystem::path filepath(filename);
        if (filepath.has_parent_path()) {
            std::filesystem::create_directories(filepath.parent_path());
        }
        
        file_.open(filename, std::ios::out);
        if (!file_.is_open()) {
            throw std::runtime_error("Cannot open latency file: " + filename);
        }
    }
    
    ~LatencyLogger() {
        if (file_.is_open()) {
            file_.close();
        }
    }
    
    void write_header() {
        if (!header_written_) {
            file_ << "operation_index,latency_ns,latency_us\n";
            header_written_ = true;
        }
    }
    
    void log_latency(size_t index, const Duration& latency) {
        write_header();
        
        file_ << index << ","
              << latency.count() << ","
              << std::fixed << std::setprecision(0) << duration_to_microseconds(latency) << "\n";
    }
    
    void log_latencies(const std::vector<Duration>& latencies) {
        write_header();
        
        for (size_t i = 0; i < latencies.size(); ++i) {
            file_ << i << ","
                  << latencies[i].count() << ","
                  << std::fixed << std::setprecision(0) << duration_to_microseconds(latencies[i]) << "\n";
        }
        
        file_.flush();
    }
    
    void flush() {
        file_.flush();
    }
    
    static std::string generate_latency_filename(const std::string& operation,
                                                const std::string& cache_type,
                                                const std::string& storage_type,
                                                size_t degree, size_t records, int run_id,
                                                const std::string& output_dir = "",
                                                int thread_id = -1) {
        std::ostringstream oss;
        oss << "latency_" << operation << "_" << cache_type << "_" << storage_type
            << "_deg" << degree << "_rec" << records << "_run" << run_id;
        
        if (thread_id >= 0) {
            oss << "_thread" << thread_id;
        }
        
        oss << ".csv";
        
        if (output_dir.empty()) {
            return oss.str();
        } else {
            return output_dir + "/" + oss.str();
        }
    }
};