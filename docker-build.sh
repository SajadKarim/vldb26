#!/bin/bash

################################################################################
# Docker Build Script for VLDB26
#
# This script builds the Docker image for the VLDB26 benchmarking pipeline
#
# Usage:
#   ./docker-build.sh [OPTIONS]
#
# Options:
#   --no-cache        Build without using cache (slower, but fresh build)
#   --buildkit        Use Docker BuildKit for faster builds
#   --tag NAME        Custom image tag (default: vldb26:latest)
#   --help            Show this help message
#
################################################################################

set -e

# Parse arguments
NO_CACHE=""
USE_BUILDKIT=""
TAG="vldb26:latest"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-cache)
            NO_CACHE="--no-cache"
            shift
            ;;
        --buildkit)
            USE_BUILDKIT=1
            shift
            ;;
        --tag)
            TAG="$2"
            shift 2
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

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║          Docker Build for VLDB26 Benchmark                ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Image tag: $TAG"
echo "No cache: ${NO_CACHE:-false}"
echo "BuildKit: ${USE_BUILDKIT:-false}"
echo ""

# Build the image
if [ -n "$USE_BUILDKIT" ]; then
    echo "Building with Docker BuildKit..."
    DOCKER_BUILDKIT=1 docker build -t "$TAG" $NO_CACHE "$SCRIPT_DIR"
else
    echo "Building with standard Docker..."
    docker build -t "$TAG" $NO_CACHE "$SCRIPT_DIR"
fi

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Build successful!"
    echo ""
    echo "Next steps:"
    echo "  1. Run the container:"
    echo "     ./docker-run.sh"
    echo ""
    echo "  2. Or run with custom options:"
    echo "     docker run --rm -v ~/vldb26_results:/results $TAG"
    echo ""
else
    echo ""
    echo "✗ Build failed!"
    exit 1
fi