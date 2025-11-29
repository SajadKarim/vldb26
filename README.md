# VLDB26: Beyond Block-Oriented Semantics - Adaptive Buffer Management Across Heterogeneous Storage

Repository: https://github.com/anonymousbytes/vldb26

## Project Structure

- **baseline/**: Baseline implementation (Version 1)
- **optimized/**: Optimized implementation (Version 2))
- **plots/**: Plotting scripts for generating performance analysis figures
- **benchmark_data/**: Benchmark data directory (will be auto-generated)

> **Note:** For Docker-based setup and execution, see the [Docker Setup](#docker-setup) section at the end of this document.

## Prerequisites

### Build and Development Tools

The following tools are required to compile the baseline and optimized implementations:

- **CMake** (≥ 3.10): Build system configuration and generation
- **g++-11**: C++ compiler (GCC 11 with C++20 support)
- **GNU Make**: Build system for executing CMake-generated makefiles
- **Git**: Version control (required for cloning project dependencies)
- **Build Essential**: Base compilation tools (`sudo apt-get install build-essential`)

### Required System Libraries

The following persistent memory and I/O libraries are essential for both implementations:

- **libpmem-dev**: Intel Persistent Memory Development Library
  - For persistent memory storage operations
  - Install: `sudo apt-get install libpmem-dev`

- **liburing-dev**: Asynchronous I/O Library
  - For efficient I/O operations (used in optimized version)
  - Install: `sudo apt-get install liburing-dev`

- **libnuma-dev**: NUMA Support Library
  - For NUMA-aware memory operations (used in optimized version)
  - Install: `sudo apt-get install libnuma-dev`

### Runtime Tools

- **perf**: Linux performance analysis tool
  - Used for cache-aware performance monitoring and profiling
  - Install: `sudo apt-get install linux-tools-generic` or `linux-tools-common`
  - Required for collecting:
    - cache-misses, cache-references
    - cycles, instructions
    - branch-misses, page-faults
    - L1/L3 cache metrics

### Python Dependencies (For Plotting)

Required for generating comparison plots (Figure 3 & 4):

- **Python 3**: Interpreter (usually pre-installed)
- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computing
- **matplotlib**: Data visualization

Install Python dependencies:
```bash
pip install pandas numpy matplotlib
```

### Installation Command

Install all required dependencies at once:

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake g++-11 git libpmem-dev liburing-dev libnuma-dev linux-tools-generic
pip install pandas numpy matplotlib
```

### System Specifications (Reference)

This code has been tested on the following system configuration:

**Hardware:**
- **CPU**: Intel Xeon Gold 5220R @ 2.20GHz
  - Cores: 24 per socket × 2 sockets = 48 physical cores
  - Threads: 96 logical threads (2 threads per core)
  - NUMA: 2 nodes with balanced core distribution
  - L3 Cache: 71.5 MiB
  - L2 Cache: 48 MiB  
  - L1 Cache: 1.5 MiB (Data + Instructions)

**Memory:**
- **RAM**: 376 GiB total
- **Swap**: 8 GiB

**Storage:**
- **NVMe SSD**: Samsung PM983 (~880 GiB, mounted as '/')
- **NVM**: Optane DCPMM Series 100 (4x128GB per Socket)

**Operating System:**
- **OS**: Ubuntu 20.04.6 LTS (Focal Fossa)
- **Kernel**: Linux 5.4.0-216 (Ubuntu)
- **Architecture**: x86_64 (64-bit)

**Key CPU Features:**
- AVX-512 support (full vector instruction set)
- AVX2 support (required for optimized build)
- BMI instructions (Bit Manipulation Instructions)
- VT-x virtualization support

Note: While this code can run on smaller systems, the reference system specifications provide baseline performance context. Minimum recommended: 8+ cores, 64+ GiB RAM, and ~10 GiB free disk space.

## Running the Experiments

### 4.1.1 Algorithmic Impact Analysis

This section describes how to run the benchmarking pipeline to analyze the algorithmic impact discussed in Section 4.1.1 of the paper.

#### Prerequisites

First, checkout the evaluation branch containing YCSB macrobenchmark support:

```bash
git fetch origin eval/4_1_1_algorithmic_impact_analysis
git checkout eval/4_1_1_algorithmic_impact_analysis
```

- Both baseline and optimized versions must be built
- Requires `perf` tool for performance monitoring

##### Important

You must configure the storage paths in `common.h` before running the benchmarks:

- **`FILE_STORAGE_PATH`** → set this to a file located on the **SSD** (e.g., "/home/skarim/file_storage.bin")
- **`PMEM_STORAGE_PATH`** → set this to a file located on the **NVM** (e.g., "/mnt/tmpfs/pmem_storage.bin")

Apply these settings in both:  
- `baseline/benchmark/common.h`  
- `optimized/benchmark/common.h`


#### Automated Pipeline (Recommended)

For end-to-end benchmark and plot generation, use:

```bash
# Full pipeline with automatic CSV discovery and plot generation
./microbenchmark_algorithmic_impact_analysis.sh

# With custom base data path
./microbenchmark_algorithmic_impact_analysis.sh --data-path <benchmark_data_dir>

# Run specific phases only
./microbenchmark_algorithmic_impact_analysis.sh --baseline-only   # Baseline benchmarks
./microbenchmark_algorithmic_impact_analysis.sh --optimized-only  # Optimized benchmarks
./microbenchmark_algorithmic_impact_analysis.sh --plots-only      # Plot generation (uses existing CSV files)
```

This orchestrates:
1. **Phase 1**: Baseline (Version 1) cache profiling
2. **Phase 2**: Optimized (Version 2) cache profiling
3. **Phase 3**: Plot generation, plots are saved as `plot_figure3.png/pdf` and `plot_figure4.png/pdf`.

The `--data-path` specifies the base directory. Subdirectories `/data` and `/ycsb` are created automatically. (Default: \tmp\benchmark_data)

For more options:
```bash
./microbenchmark_algorithmic_impact_analysis.sh --help
```

#### Manual Pipeline

For manual execution, run benchmarks separately:

```bash
# Baseline benchmarks
cd baseline/benchmark
DATA_PATH=<benchmark_data_dir>/data ./profile_and_benchmark_bplus_with_cache.sh full

# Optimized benchmarks
cd optimized/benchmark
DATA_PATH=<benchmark_data_dir> ./profile_and_benchmark_tree_with_cache.sh

# Generate plots
python3 plots/plot_figure3.py <baseline_csv> <optimized_csv>
python3 plots/plot_figure4.py <baseline_csv> <optimized_csv>
```

Plots are saved as `plot_figure3.png/pdf` and `plot_figure4.png/pdf`.

### 4.1.2 Storage Impact Analysis

This section describes how to run comprehensive cache profiling benchmarks to analyze the impact of different storage types and cache policies on system performance (discussed in Section 4.1.2 of the paper).

#### Prerequisites

First, checkout the evaluation branch containing YCSB macrobenchmark support:

```bash
git fetch origin eval/4_1_2_storage_impact_analysis
git checkout eval/4_1_2_storage_impact_analysis
```

##### Important

You must configure the storage paths in `common.h` before running the benchmarks:

- **`FILE_STORAGE_PATH`** → set this to a file located on the **SSD** (e.g., "/home/skarim/file_storage.bin")
- **`PMEM_STORAGE_PATH`** → set this to a file located on the **NVM** (e.g., "/mnt/tmpfs/pmem_storage.bin")

Apply these settings in:  
- `optimized/benchmark/common.h`

#### Running the Storage Impact Benchmarks

To generate comprehensive cache profiling data with all storage types, cache policies, and thread counts, run:

```bash
cd /home/vldb26/optimized/benchmark
./profile_and_benchmark_tree_with_cache.sh
```

This script executes benchmarks with:
- **Cache Policies:** Different variants of LRU, A2Q, 2Q, and CLOCK 
- **Storage Types:** VolatileStorage, PMemStorage, FileStorage
- **Cache Sizes:** 2%, 10%, 25% of B+ tree size
- **Thread Counts:** 1, 4, 8 threads
- **Operations:** insert, search_random, search_sequential, search_uniform, search_zipfian, delete
- **Record Count:** 500,000 key-value pairs
- **Runs:** 5

The script outputs a timestamped directory containing:
- Combined benchmark results CSV file: `/home/vldb26/optimized/benchmark/build/cache_profiling_results_<TIMESTAMP>/combined_benchmark_results_with_perf_<TIMESTAMP>.csv`
- Individual policy result files
- Performance metrics (`cache-misses,cache-references,cycles,instructions,branch-misses,page-faults,L1-dcache-load-misses,L1-dcache-loads,LLC-load-misses,LLC-loads,L1-icache-load-misses,L1-icache-loads`)

#### Generating Visualization Plots

After benchmarking completes, generate publication-quality plots using the plot scripts in `plots/`:

**Available Plot Scripts:**

| Figure | Script |
|--------|--------|
| Figure 5 | `plot_figure5.py` |
| Figure 6 | `plot_figure6.py` |
| Figure 7 | `plot_figure7.py` |
| Figure 8 | `plot_figure8.py` |
| Figure 9 | `plot_figure9.py` |

**Example: Generate Figure 5 (Policy Performance Distribution)**

```bash
cd /home/vldb26/plots

# Using default data file
python3 plot_figure5.py

# Using custom benchmark CSV file
python3 plot_figure5.py --csv /path/to/combined_benchmark_results_with_perf_TIMESTAMP.csv
```

This generates:
- `plot_figure5.png` - High-resolution PNG (300 DPI)
- `plot_figure5.pdf` - Vector format PDF

or 

```bash
# Set the path to your new CSV file
CSV_FILE="/path/to/combined_benchmark_results_with_perf_20251003_110829.csv"

# Generate all plots with custom data
python3 plots/plot_figure5.py --csv "$CSV_FILE"
python3 plots/plot_figure6.py --csv "$CSV_FILE"
python3 plots/plot_figure7.py --csv "$CSV_FILE"
python3 plots/plot_figure8.py --csv "$CSV_FILE"
python3 plots/plot_figure9.py --csv "$CSV_FILE"
```

#### Complete Workflow Example

```bash
# 1. Run benchmarks from optimized folder
cd /home/vldb26/optimized/benchmark
./profile_and_benchmark_tree_with_cache.sh

# 2. Wait for benchmarks to complete (outputs timestamped directory)
# Note the timestamp from the output, e.g., cache_profiling_results_20251003_110829

# 3. Copy the generated CSV to plots/data/ or note its path
CSV_PATH="build/cache_profiling_results_20251003_110829/combined_benchmark_results_with_perf_20251003_110829.csv"

# 4. Generate all plots from the plots directory
cd /home/vldb26/plots
python3 plot_figure5.py --csv "$CSV_PATH"
python3 plot_figure6.py --csv "$CSV_PATH"
python3 plot_figure7.py --csv "$CSV_PATH"
python3 plot_figure8.py --csv "$CSV_PATH"
python3 plot_figure9.py --csv "$CSV_PATH"

# 5. All plots are now available in the plots directory
ls -lh plot_figure*.png plot_figure*.pdf
```

### 4.2 Macrobenchmarks

This section describes how to run YCSB (Yahoo Cloud Serving Benchmark) workload benchmarks to evaluate cache performance under real-world scenarios.

#### Prerequisites

First, checkout the evaluation branch containing YCSB macrobenchmark support:

```bash
git fetch origin eval/4_2_ycsb
git checkout eval/4_2_ycsb
```

##### Important

You must configure the storage paths in `common.h` before running the benchmarks:

- **`FILE_STORAGE_PATH`** → set this to a file located on the **SSD** (e.g., "/home/skarim/file_storage.bin")
- **`PMEM_STORAGE_PATH`** → set this to a file located on the **NVM** (e.g., "/mnt/tmpfs/pmem_storage.bin")

Apply these settings in:  
- `optimized/benchmark/common.h`

#### Running YCSB Workload Benchmarks

To run comprehensive YCSB workload benchmarks with all cache types and storage configurations:

```bash
cd /home/vldb26/optimized/benchmark
./profile_and_benchmark_tree_with_cache_ycsb.sh
```

This script executes YCSB benchmarks with:
- **Workloads:** A, B, C, D, E, F (different read/write/scan ratios)
- **Cache Policies:** Different variants of LRU, A2Q, 2Q, and CLOCK 
- **Storage Types:** VolatileStorage, PMemStorage, FileStorage
- **Cache Sizes:** 2%, 10%, 25% of B+ tree size
- **Record Count:** 500,000 key-value pairs
- **Runs:** 5

The script outputs a timestamped directory containing:
- Combined benchmark results CSV file: `/home/vldb26/optimized/benchmark/build/cache_profiling_results_<TIMESTAMP>/combined_ycsb_benchmark_results_with_perf_<TIMESTAMP>.csv`
- Individual workload and cache policy result files
- Detailed performance metrics (cache hits/misses, throughput, latency)

#### Generating YCSB Visualization Plots

After benchmarking completes, generate publication-quality plots using:

```bash
cd /home/vldb26/plots

# Using default data file
python3 plot_figure10.py

# Using custom YCSB benchmark CSV file
python3 plot_figure10.py --csv /path/to/combined_ycsb_benchmark_results_with_perf_TIMESTAMP.csv
```

This generates:
- `plot_figure10.png` - YCSB workload performance comparison
- `plot_figure10.pdf` - Vector format PDF

#### Complete YCSB Workflow Example

```bash
# 1. Checkout the YCSB evaluation branch
cd /home/vldb26
git fetch origin eval/4_2_ycsb
git checkout eval/4_2_ycsb

# 2. Run YCSB benchmarks
cd /home/vldb26/optimized/benchmark
./profile_and_benchmark_tree_with_cache_ycsb.sh

# 3. Wait for benchmarks to complete (outputs timestamped directory)
# Note the timestamp from the output, e.g., ycsb_cache_profiling_results_20251010_030225

# 4. Generate YCSB performance plots
cd /home/vldb26/plots
python3 plot_figure10.py --csv /path/to/combined_ycsb_benchmark_results_with_perf_TIMESTAMP.csv
# e.g., python3 ./plot_figure10.py --csv ./data/combined_ycsb_benchmark_results_with_perf_20251004_051834.csv

# 5. Plots are now available in the plots directory
ls -lh plot_figure10*.png plot_figure10*.pdf
```

#### Concurrent Behavior

This section describes how to evaluate cache performance under concurrent multi-threaded access patterns using YCSB workloads.

**Prerequisites**

First, checkout the evaluation branch containing concurrent YCSB benchmark support:

```bash
git fetch origin eval/4_2_ycsb_concurrent
git checkout eval/4_2_ycsb_concurrent
```

**Running Concurrent YCSB Benchmarks**

To run YCSB benchmarks with multiple thread configurations for analyzing concurrent behavior:

```bash
cd /home/vldb26/optimized/benchmark
./profile_and_benchmark_tree_with_cache_ycsb.sh
```

This script executes YCSB benchmarks with various thread counts to evaluate how cache policies scale with concurrency.

**Generating Thread Scalability Plots**

After benchmarking completes, generate thread scalability visualization plots using:

```bash
cd /home/vldb26/plots

# Using custom concurrent YCSB benchmark CSV file
python3 plot_figure11.py --csv /path/to/combined_ycsb_benchmark_results_with_perf_TIMESTAMP.csv
```

This generates:
- `plot_figure11.png` - Thread scalability plot with coefficient of variation (CV) measurements
- `plot_figure11.pdf` - Vector format PDF

The plot shows throughput scaling across different thread counts with CV annotations indicating performance variability at each concurrency level.

## Section 5: Advanced Cache Policies

### 5.1 Cost-Weighted Eviction

This section describes how to run YCSB benchmarks with Cost-Weighted Eviction (CWE) policies that account for the cost of accessing different storage tiers.

#### Prerequisites

First, checkout the evaluation branch containing Cost-Weighted Eviction support:

```bash
git fetch origin eval/4_2_ycsb
git checkout eval/4_2_ycsb
```

#### Running Cost-Weighted Eviction Benchmarks

To run comprehensive YCSB workload benchmarks with cost-weighted eviction policies:

```bash
cd /home/vldb26/optimized/benchmark
./profile_and_benchmark_tree_with_cache_ycsb_cwe.sh
```

This script executes YCSB benchmarks with:
- **Workloads:** A, B, C, D, E, F (different read/write/scan ratios)
- **Cost-Weighted Policies:** LRU-CWE, A2Q-CWE, CLOCK-CWE
- **Storage Types:** VolatileStorage, PMemStorage, FileStorage
- **Cache Sizes:** 2%, 10%, 25% of B+ tree size
- **Cost Models:** Storage-aware eviction decisions
- **Record Count:** 500,000 key-value pairs

The script outputs a timestamped directory containing:
- Combined benchmark results CSV file: `combined_ycsb_benchmark_results_with_perf_TIMESTAMP.csv`
- Cost-weighted policy result files
- Performance metrics comparing standard vs. cost-weighted policies

#### Generating Cost-Weighted Eviction Visualization Plots

After benchmarking completes, generate publication-quality plots using:

```bash
cd /home/vldb26/plots

# Using default data file
python3 plot_figure12.py

# Using custom CWE benchmark CSV file
python3 plot_figure12.py --csv /path/to/combined_ycsb_benchmark_results_with_perf_TIMESTAMP.csv
```

This generates:
- `plot_figure12.png` - CWE policy performance comparison
- `plot_figure12.pdf` - Vector format PDF

### 5.2 Device-aware Policy

This section describes how to run YCSB benchmarks with device-aware cache policies that dynamically adapt to heterogeneous storage characteristics.

#### Prerequisites

First, checkout the evaluation branch containing device-aware policy support:

```bash
git fetch origin eval/4_2_ycsb
git checkout eval/4_2_ycsb
```

#### Running Device-aware Policy Benchmarks

To run comprehensive YCSB workload benchmarks with device-aware cache policies:

```bash
cd /home/vldb26/optimized/benchmark
./run_device_aware_ycsb.sh
```

This script executes YCSB benchmarks with:
- **Workloads:** A, B, C, D, E, F (different read/write/scan ratios)
- **Device-aware Policies:** LRU-DA, A2Q-DA, CLOCK-DA
- **Storage Types:** VolatileStorage, PMemStorage, FileStorage
- **Cache Sizes:** 2%, 10%, 25% of B+ tree size
- **Device Characteristics:** Latency and bandwidth models for different storage tiers
- **Adaptive Strategies:** Runtime optimization based on device properties
- **Record Count:** 500,000 key-value pairs

The script outputs a timestamped directory containing:
- Combined benchmark results CSV file: `combined_device_aware_ycsb_results_with_perf_TIMESTAMP.csv`
- Device-aware policy result files
- Performance metrics with device-specific optimizations

#### Generating Device-aware Policy Visualization Plots

After benchmarking completes, generate publication-quality plots using:

```bash
cd /home/vldb26/plots

# Using default data file
python3 plot_figure13.py

# Using custom device-aware benchmark CSV files
python3 plot_figure13.py --csv-original /path/to/original_results.csv --csv-device-aware /path/to/device_aware_results.csv
```

This generates:
- `plot_figure13.png` - Device-aware vs. standard policies
- `plot_figure13.pdf` - Vector format PDF

The comparison includes both storage-aware baseline policies and the fully device-aware implementations, demonstrating performance improvements across all workloads and storage configurations.

## Docker Setup

**Note:** Docker is currently supported and tested on the `eval/4_1_1_algorithmic_impact_analysis` branch. Other evaluation branches may have Docker support in the future.

**Available evaluation branches:**
- `eval/4_1_1_algorithmic_impact_analysis` - For algorithmic impact analysis **(currently supported)**
- `eval/4_1_2_storage_impact_analysis` - For storage impact analysis (Docker not yet configured)
- `eval/4_2_ycsb` - For YCSB macrobenchmarks (Docker not yet configured)

To use Docker, ensure you are on the supported branch:

```bash
git checkout eval/4_1_1_algorithmic_impact_analysis
```

### Building and Running with Docker

The `docker-run.sh` helper script handles both image building and container execution. It automatically builds the Docker image (if not already built) and runs the benchmark pipeline.

#### Quick Start (Default Configuration)

```bash
chmod +x ./docker-run.sh
sudo ./docker-run.sh \
  --image vldb26:latest \
  --out /tmp/vldb_results \
  --data /home/skarim/benchmark_data
```

#### With Network Proxy (for restricted networks)

If your environment requires an HTTP proxy to access package repositories:

```bash
sudo ./docker-run.sh \
  --image vldb26:latest \
  --out /tmp/vldb_results \
  --data /home/skarim/benchmark_data \
  --http-proxy http://proxy_address:port \
  --https-proxy http://proxy_address:port \
  --privileged
```

Replace `proxy_address:port` with your actual proxy server details. If your proxy requires authentication, use `http://username:password@proxy_address:port`.

#### Script Options

- `--image NAME` - Docker image tag (default: `vldb26:latest`)
- `--out HOST_DIR` - Host directory where results are saved (default: `./results`)
- `--data HOST_DIR` - Host directory for benchmark data (default: `$HOME/benchmark_data`)
- `--http-proxy URL` - HTTP proxy URL for package downloads
- `--https-proxy URL` - HTTPS proxy URL for package downloads
- `--privileged` - Run container in privileged mode (recommended for `perf` profiling)
- `--help` - Show usage information

### Output Files

After the pipeline completes, results are copied to your host's output directory (`/tmp/vldb_results` by default):

- `plots_generated/` - Generated comparison plots (PNG/PDF) from `microbenchmark_algorithmic_impact_analysis.sh`
- `csvs/` - Combined benchmark result CSV files with performance metrics

### Requirements

- Docker must be installed and running
- At least 20 GiB free disk space for the image and results
- Internet connectivity (for package downloads during build, or configure a proxy)
- If network is restricted, provide proxy settings via `--http-proxy` and `--https-proxy` flags
- Root or `sudo` access to run Docker commands
- For performance profiling (`perf`) to work correctly, use `--privileged` flag or configure appropriate Docker capabilities
