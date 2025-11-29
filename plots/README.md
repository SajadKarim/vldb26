# VLDB26 Plotting Scripts - Reproducible Benchmark Visualizations

This directory contains the plotting scripts for generating the figures from the VLDB'26 paper. The benchmark data can be generated using the instructions provided in each section of the main README.md at the root path. However, to demonstrate and verify the plots, the precomputed benchmark data from the paper is used here to generate visualizations.

## Quick Start

```bash
# Generate both figures with included benchmark data
python3 plot_figure3.py data/v1_benchmark.csv data/v2_benchmark.csv
python3 plot_figure4.py data/v1_benchmark.csv data/v2_benchmark.csv
```

---

## Dependencies

### System Requirements
- Python 3.6 or higher
- pip (Python package manager)

### Python Libraries

Install required dependencies:

```bash
pip install pandas numpy matplotlib
```

Verify installation:
```bash
python3 -c "import pandas, numpy, matplotlib; print('✓ All dependencies installed')"
```

---

## Figure 3: Throughput Comparison Plot

**Script:** `plot_figure3.py`

**Purpose:** Generates a comparison plot showing operations per second (throughput) between Version 1 (BPlusStore) and Version 2 (BplusTreeSOA) implementations across different storage types.

### Plot Contents
- 3 subplots for different storage types:
  - SSD NVMe (FileStorage)
  - NVM (PMemStorage)
  - NVDIMM (VolatileStorage)
- Box plots comparing:
  - Version 1 with 1 thread (V1-1T)
  - Version 2 with 1 thread (V2-1T)
  - Version 1 with 4 threads (V1-4T)
  - Version 2 with 4 threads (V2-4T)
- Individual data points overlaid on box plots
- Performance improvement indicators

### Using the Original Benchmark Data

Run Figure 3 with the included benchmark data files (recommended):

```bash
# From the plots directory:
python3 plot_figure3.py data/v1_benchmark.csv data/v2_benchmark.csv
```

Or with full paths:

```bash
python3 ./vldb26/plots/plot_figure3.py \
  ./vldb26/plots/data/v1_benchmark.csv \
  ./vldb26/plots/data/v2_benchmark.csv
```

### Output

The script generates:
- `plot_figure3.png`
- `plot_figure3.pdf`

By default, output files are saved in the same directory as the script (`./vldb26/plots/`).

---

## Figure 4: CPI (Cycles Per Instruction) Comparison Plot

**Script:** `plot_figure4.py`

**Purpose:** Generates a comparison plot showing Cycles Per Instruction (CPI) - a measure of instruction efficiency - between Version 1 and Version 2 implementations.

### Plot Contents
- 3 subplots for different storage types:
  - SSD NVMe (FileStorage)
  - NVM (PMemStorage)
  - NVDIMM (VolatileStorage)
- Box plots comparing instruction efficiency across versions and thread counts
- Individual operation data points
- Lower CPI values indicate better instruction efficiency
- Performance improvement indicators

### Using the Original Benchmark Data

Run Figure 4 with the included benchmark data files (recommended):

```bash
# From the plots directory:
python3 plot_figure4.py data/v1_benchmark.csv data/v2_benchmark.csv
```

Or with full paths:

```bash
python3 ./vldb26/plots/plot_figure4.py \
  ./vldb26/plots/data/v1_benchmark.csv \
  ./vldb26/plots/data/v2_benchmark.csv
```

### Output

The script generates:
- `plot_figure4.png`
- `plot_figure4.pdf`

By default, output files are saved in the same directory as the script (`./vldb26/plots/`).

---

## Figure 5:

Example:

```bash
python3 ./vldb26/plots/plot_figure5.py --csv ./vldb26/plots/data/combined_benchmark_results_with_perf_20251002_072156.csv 
```

---

## Figure 6:

Example:

```bash
python3 ./vldb26/plots/plot_figure6.py --csv ./vldb26/plots/data/combined_benchmark_results_with_perf_20251003_110829.csv 
```

---

## Figure 7:

Example:

```bash
python3 ./vldb26/plots/plot_figure7.py --csv ./vldb26/plots/data/combined_benchmark_results_with_perf_20251002_072156.csv 
```

---

## Figure 8:

Example:

```bash
python3 ./vldb26/plots/plot_figure8.py --csv ./vldb26/plots/data/combined_benchmark_results_with_perf_20251002_072156.csv 
```

---

## Figure 9:

Example:

```bash
python3 ./vldb26/plots/plot_figure9.py --csv ./vldb26/plots/data/combined_benchmark_results_with_perf_20251002_072156.csv 
```

---

## Figure 10:

Example:

```bash
python3 ./vldb26/plots/plot_figure10.py --csv ./vldb26/plots/data/combined_ycsb_benchmark_results_with_perf_20251004_051834.csv 
```

---

## Figure 11:

Example:

```bash
python3 ./vldb26/plots/plot_figure11.py --csv ./vldb26/plots/data/combined_ycsb_benchmark_results_with_perf_20251015_043151.csv 
```

## Figure 12:

Example:

```bash
python3 ./vldb26/plots/plot_figure12.py --csv ./vldb26/plots/data/combined_ycsb_benchmark_results_with_perf_20251008_152420.csv 
```

---

## Figure 13:

Example:

```bash
python3 ./vldb26/plots/plot_figure13.py --csv-original ./vldb26/plots/data/combined_ycsb_benchmark_results_with_perf_20251009_185641.csv --csv-device-aware ./vldb26/plots/data/combined_device_aware_ycsb_results_with_perf_20251010_030225.csv
```

---
