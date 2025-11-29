#!/bin/bash

# Setup and Build Script for VLDB26 Optimized B+ Tree Cache Benchmark
# This script prepares the environment and builds the project

set -e  # Exit on error

echo "=========================================="
echo "VLDB26 Reproducible Build Setup"
echo "=========================================="

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "Step 1: Checking system dependencies..."
echo "=========================================="

# Check for required tools
required_tools=("cmake" "g++-11" "make" "git")
missing_tools=()

for tool in "${required_tools[@]}"; do
    if ! command -v "$tool" &> /dev/null; then
        missing_tools+=("$tool")
    else
        echo "[OK] Found: $tool"
    fi
done

if [ ${#missing_tools[@]} -gt 0 ]; then
    echo ""
    echo "[ERROR] Missing tools: ${missing_tools[*]}"
    echo ""
    echo "Install missing dependencies with:"
    echo "  sudo apt-get update"
    echo "  sudo apt-get install -y build-essential cmake g++-11 libpmem-dev liburing-dev libnuma-dev"
    exit 1
fi

# Check for required libraries
echo ""
echo "Checking required libraries..."
required_libs=("libpmem" "liburing" "libnuma")

for lib in "${required_libs[@]}"; do
    if pkg-config --exists "$lib"; then
        echo "[OK] Found: $lib"
    else
        echo "[ERROR] Missing: $lib"
        echo "  Install with: sudo apt-get install lib${lib}-dev"
    fi
done

echo ""
echo "Step 2: Preparing build directory..."
echo "=========================================="

if [ -d "benchmark/build" ]; then
    echo "Build directory already exists"
    read -p "Clean and rebuild? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -rf "benchmark/build"
        mkdir -p "benchmark/build"
    fi
else
    mkdir -p "benchmark/build"
    echo "[OK] Created benchmark/build directory"
fi

echo ""
echo "Step 3: Building benchmark executable..."
echo "=========================================="

cd "$SCRIPT_DIR/benchmark/build"

# Default optimization flags for Release builds
RELEASE_OPTS="-O3 -DNDEBUG -march=native -mtune=native -mavx2"

echo "Configuring CMake..."
cmake .. -DCMAKE_BUILD_TYPE=Release \
         -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ $RELEASE_OPTS"

echo ""
echo "Compiling (using $(nproc) CPU cores)..."
make -j$(nproc)

if [ -f "benchmark" ]; then
    echo ""
    echo "[OK] Build successful!"
    echo ""
    echo "Benchmark executable: $(pwd)/benchmark"
else
    echo "[ERROR] Build failed!"
    exit 1
fi

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "To run benchmarks, execute:"
echo "  cd $SCRIPT_DIR"
echo "  ./profile_and_benchmark_tree_with_cache.sh"
echo ""

