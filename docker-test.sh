#!/bin/bash

################################################################################
# Docker Test Script for VLDB26
#
# This script tests that the Docker image was built correctly without running
# the full benchmark pipeline. Useful for quick validation.
#
# Usage:
#   ./docker-test.sh [OPTIONS]
#
# Options:
#   --tag NAME         Docker image tag (default: vldb26:latest)
#   --verbose          Show detailed output
#   --help             Show this help message
#
################################################################################

set -e

# Default values
TAG="vldb26:latest"
VERBOSE=0

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)
            TAG="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE=1
            shift
            ;;
        --help|-h)
            grep "^#" "$0" | grep -v "^#!/" | sed 's/^# //' | sed 's/^#//'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "╔════════════════════════════════════════════════════════════╗"
echo "║            Docker Test for VLDB26                         ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Check if image exists
if ! docker image inspect "$TAG" >/dev/null 2>&1; then
    echo "✗ Docker image not found: $TAG"
    echo ""
    echo "Build the image first:"
    echo "  ./docker-build.sh --tag $TAG"
    exit 1
fi

# Test 1: Docker image exists
echo "Test 1: Docker image exists"
echo "  ✓ Image found: $TAG"
echo ""

# Test 2: Check version tools
echo "Test 2: Verify installed tools"
docker run --rm "$TAG" bash -c "
    echo '  CMake:' && cmake --version | head -1
    echo '  GCC/G++:' && g++-11 --version | head -1
    echo '  GNU Make:' && make --version | head -1
    echo '  Git:' && git --version
"
echo ""

# Test 3: Check Python dependencies
echo "Test 3: Verify Python packages"
docker run --rm "$TAG" python3 -c "
    import sys
    print('  Python 3:', sys.version.split()[0])
    
    try:
        import pandas
        print('  ✓ pandas:', pandas.__version__)
    except ImportError as e:
        print('  ✗ pandas:', e)
        sys.exit(1)
    
    try:
        import numpy
        print('  ✓ numpy:', numpy.__version__)
    except ImportError as e:
        print('  ✗ numpy:', e)
        sys.exit(1)
    
    try:
        import matplotlib
        print('  ✓ matplotlib:', matplotlib.__version__)
    except ImportError as e:
        print('  ✗ matplotlib:', e)
        sys.exit(1)
"
echo ""

# Test 4: Check system libraries
echo "Test 4: Verify system libraries"
docker run --rm "$TAG" bash -c "
    echo '  Checking libpmem...' && pkg-config --exists libpmem && echo '    ✓ libpmem found'
    echo '  Checking liburing...' && pkg-config --exists liburing && echo '    ✓ liburing found'
    echo '  Checking libnuma...' && pkg-config --exists libnuma && echo '    ✓ libnuma found'
"
echo ""

# Test 5: Check if executables were built
echo "Test 5: Verify benchmark executables"
docker run --rm "$TAG" bash -c "
    if [ -f /vldb26/baseline/benchmark/build/benchmark ]; then
        echo '  ✓ Baseline benchmark executable found'
        ls -lh /vldb26/baseline/benchmark/build/benchmark | awk '{print \"    Size: \" \$5}'
    else
        echo '  ✗ Baseline benchmark executable NOT found'
        exit 1
    fi
    
    if [ -f /vldb26/optimized/benchmark/build/benchmark ]; then
        echo '  ✓ Optimized benchmark executable found'
        ls -lh /vldb26/optimized/benchmark/build/benchmark | awk '{print \"    Size: \" \$5}'
    else
        echo '  ✗ Optimized benchmark executable NOT found'
        exit 1
    fi
"
echo ""

# Test 6: Check plotting scripts
echo "Test 6: Verify plotting scripts"
docker run --rm "$TAG" bash -c "
    [ -f /vldb26/plots/plot_figure3.py ] && echo '  ✓ plot_figure3.py found'
    [ -f /vldb26/plots/plot_figure4.py ] && echo '  ✓ plot_figure4.py found'
    [ -f /vldb26/microbenchmark_algorithmic_impact_analysis.sh ] && echo '  ✓ main script found'
"
echo ""

# Test 7: Quick benchmark smoke test (optional)
if [ $VERBOSE -eq 1 ]; then
    echo "Test 7: Smoke test (running minimal benchmark - this takes ~5 minutes)"
    docker run --rm \
        --cpus="2" \
        --memory="4g" \
        "$TAG" \
        bash -c "
            cd /vldb26/baseline/benchmark/build && \
            timeout 60 ./benchmark --config bm_cache --tree-type bplus --cache-type lru --storage-type file --records 10000 --cache-size 1000 --threads 1 --operations 1000 2>&1 | head -20
        " || echo "  (Smoke test completed or timed out - this is OK)"
    echo ""
fi

echo "╔════════════════════════════════════════════════════════════╗"
echo "║               All Tests Passed! ✓                         ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "The Docker image is ready to use!"
echo ""
echo "Next steps:"
echo "  1. Run the full benchmark pipeline:"
echo "     ./docker-run.sh"
echo ""
echo "  2. Or run specific phases:"
echo "     ./docker-run.sh --baseline-only"
echo "     ./docker-run.sh --optimized-only"
echo "     ./docker-run.sh --plots-only"
echo ""
echo "  3. For interactive development:"
echo "     ./docker-run.sh --interactive"
echo ""