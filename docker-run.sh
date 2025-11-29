#!/bin/bash

################################################################################
# Docker Run Script for VLDB26
#
# This script runs the VLDB26 benchmark pipeline in a Docker container
#
# Usage:
#   ./docker-run.sh [OPTIONS]
#
# Options:
#   --output PATH      Output directory for results (default: ~/vldb26_results)
#   --cpus N           Number of CPUs to use (default: 4)
#   --memory SIZE      Memory limit (default: 8g)
#   --tag NAME         Docker image tag (default: vldb26:latest)
#   --interactive      Run in interactive mode (shell access)
#   --baseline-only    Run only baseline benchmarks
#   --optimized-only   Run only optimized benchmarks
#   --plots-only       Generate plots from existing CSV files
#   --help             Show this help message
#
################################################################################

set -e

# Default values
OUTPUT_DIR="$HOME/vldb26_results"
CPUS="4"
MEMORY="8g"
TAG="vldb26:latest"
INTERACTIVE=0
PHASE_ARGS=""
#!/usr/bin/env bash
set -euo pipefail

# Simple helper script to build the Docker image and run the microbenchmark
# Usage:
#   ./docker-run.sh [--image NAME] [--out HOST_DIR] [--data HOST_DIR] [--privileged]
# Examples:
#   ./docker-run.sh --image vldb26:latest --out /tmp/vldb_results --data /home/user/benchmark_data

IMAGE="vldb26:latest"
HOST_OUT_DIR="$PWD/results"
HOST_DATA_DIR="$HOME/benchmark_data"
PRIVILEGED=0
HTTP_PROXY=""
HTTPS_PROXY=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image) IMAGE="$2"; shift 2 ;; 
    --out) HOST_OUT_DIR="$2"; shift 2 ;; 
    --data) HOST_DATA_DIR="$2"; shift 2 ;; 
    --privileged) PRIVILEGED=1; shift ;; 
    --http-proxy) HTTP_PROXY="$2"; shift 2 ;;
    --https-proxy) HTTPS_PROXY="$2"; shift 2 ;;
    --help|-h) echo "Usage: $0 [--image NAME] [--out HOST_DIR] [--data HOST_DIR] [--privileged] [--http-proxy URL] [--https-proxy URL]"; exit 0 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

echo "Image: $IMAGE"
echo "Host results dir: $HOST_OUT_DIR"
echo "Host data dir: $HOST_DATA_DIR"
echo "Privileged: $PRIVILEGED"
echo "HTTP Proxy: ${HTTP_PROXY:-(none)}"
echo "HTTPS Proxy: ${HTTPS_PROXY:-(none)}"

mkdir -p "$HOST_OUT_DIR"
mkdir -p "$HOST_DATA_DIR"

echo "Building Docker image (this may take several minutes)..."
BUILD_ARGS=(-t "$IMAGE")
if [ -n "$HTTP_PROXY" ]; then
  BUILD_ARGS+=(--build-arg "http_proxy=$HTTP_PROXY")
fi
if [ -n "$HTTPS_PROXY" ]; then
  BUILD_ARGS+=(--build-arg "https_proxy=$HTTPS_PROXY")
fi
docker build "${BUILD_ARGS[@]}" .

echo "Running container... (this will execute the pipeline inside the container and copy outputs to $HOST_OUT_DIR)"

DOCKER_ARGS=(--rm -v "$HOST_DATA_DIR":/benchmark_data -v "$HOST_OUT_DIR":/out -v /sys:/sys:ro -v /proc:/proc:ro)

if [ "$PRIVILEGED" -eq 1 ]; then
  DOCKER_ARGS=(--rm --privileged -v "$HOST_DATA_DIR":/benchmark_data -v "$HOST_OUT_DIR":/out)
else
  # recommended capabilities for perf and ptrace
  DOCKER_ARGS+=(--cap-add=SYS_ADMIN --cap-add=SYS_PTRACE --security-opt seccomp=unconfined)
fi

docker run "${DOCKER_ARGS[@]}" "$IMAGE"

echo "Container finished. Results should be in: $HOST_OUT_DIR"

echo "Listing results (top-level):"
ls -lah "$HOST_OUT_DIR" || true

echo "You can inspect nested result directories copied from baseline/benchmark and optimized/benchmark which start with 'cache_profiling_results'"
