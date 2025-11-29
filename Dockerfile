# Dockerfile for VLDB26 Microbenchmark Algorithmic Impact Analysis
# Base image: Ubuntu 20.04 (matches the reference system)
FROM ubuntu:22.04

# Non-interactive frontend
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# Prefer specific compilers
ENV CC=gcc-11 CXX=g++-11

# Install required system packages (build tools, libraries, perf tooling, utilities)
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    g++-11 \
    gcc-11 \
    make \
    git \
    libpmem-dev \
    liburing-dev \
    libnuma-dev \
    numactl \
    bc \
    pkg-config \
    linux-tools-common \
    linux-tools-generic \
    python3 \
    python3-pip \
    wget \
    ca-certificates \
    curl \
    rsync \
    && rm -rf /var/lib/apt/lists/*

# Python plotting deps
RUN pip3 install --no-cache-dir pandas numpy matplotlib

# Workspace
WORKDIR /vldb26

# Copy repository into image
COPY . .

# Make common scripts executable (if present)
RUN chmod +x ./microbenchmark_algorithmic_impact_analysis.sh || true
RUN chmod +x ./baseline/setup.sh || true
RUN chmod +x ./optimized/setup.sh || true

# Clean build directories to avoid CMake cache/source mismatch
RUN rm -rf /vldb26/baseline/benchmark/build
RUN set -ex && cd /vldb26/baseline && bash setup.sh
RUN rm -rf /vldb26/optimized/benchmark/build
RUN set -ex && cd /vldb26/optimized && bash setup.sh

# Create mountpoints for runtime data and output
RUN mkdir -p /benchmark_data /out

# Default ENTRY that runs the analysis and copies generated plot files and combined CSVs to /out
ENTRYPOINT ["/bin/bash", "-lc", "set -e; ./microbenchmark_algorithmic_impact_analysis.sh --data-path /benchmark_data || true; mkdir -p /out/plots_generated /out/csvs; \n# Copy combined CSVs if they exist\nfor f in baseline/benchmark/cache_profiling_results*/combined_benchmark_results_with_perf_*.csv optimized/benchmark/cache_profiling_results*/combined_benchmark_results_with_perf_*.csv; do \n  if [ -f \"$f\" ]; then rsync -a \"$f\" /out/csvs/; fi; \ndone; \n# Copy generated plot images (png/pdf) from plots/ if present (do not copy source scripts)\nfor p in plots/*.png plots/*.pdf plots/*standalone*.*; do \n  [ -e \"$p\" ] || continue; \n  cp -a \"$p\" /out/plots_generated/; \ndone; \necho 'Outputs copied to /out (plots_generated and csvs)'; ls -R /out || true"]

# Default command (can be overridden). Using ENTRYPOINT above, plain run will execute pipeline.
CMD [""]