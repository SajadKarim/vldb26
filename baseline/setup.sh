#!/bin/bash
# Quick setup script for HaldenDB reproducible package

set -e  # Exit on error

echo "=========================================="
echo "HaldenDB Reproducible Setup"
echo "=========================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_DIR="$SCRIPT_DIR/benchmark"
BUILD_DIR="$BENCHMARK_DIR/build"

# Check prerequisites
echo ""
echo "Checking prerequisites..."

# Check for required commands
for cmd in cmake g++-11 make; do
    if ! command -v $cmd &> /dev/null; then
        echo "ERROR: $cmd not found. Please install it:"
        echo "  sudo apt-get install cmake g++-11 build-essential"
        exit 1
    fi
done

# Check for libpmem
if ! pkg-config --exists libpmem; then
    echo "ERROR: libpmem not found. Please install it:"
    echo "  sudo apt-get install libpmem-dev"
    exit 1
fi

echo "✓ All prerequisites found"

# Create build directory
echo ""
echo "Setting up build directory..."
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Configure cmake
echo "Configuring CMake..."
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CACHE_COUNTERS__ -O3 -DNDEBUG -march=native"

# Build
echo ""
echo "Building benchmark (this may take a few minutes)..."
make -j$(nproc)

echo ""
echo "=========================================="
echo "✓ Setup complete!"
echo "=========================================="
echo ""
echo "To run benchmarks, execute:"
echo "  cd $BENCHMARK_DIR"
echo "  bash ./profile_and_benchmark_bplus_with_cache.sh"
echo ""
echo "For more information, see SETUP_AND_RUN.md"
echo ""
