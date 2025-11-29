#!/bin/bash

# SIMPLE DEBUG VERSION: Run original script with timeout to detect infinite loops
# This script runs the original profile_and_benchmark_tree_with_cache.sh with a timeout
# to detect where infinite loops occur

echo "=========================================="
echo "DEBUG: Running original script with timeout to detect infinite loops"
echo "=========================================="

ORIGINAL_SCRIPT="/home/skarim/Code/haldendb_ex/haldendb_pvt/benchmark/profile_and_benchmark_tree_with_cache.sh"
TIMEOUT_SECONDS=600  # 10 minutes timeout
LOG_FILE="/tmp/benchmark_debug_$(date +%Y%m%d_%H%M%S).log"

echo "DEBUG: Original script: $ORIGINAL_SCRIPT"
echo "DEBUG: Timeout: ${TIMEOUT_SECONDS}s"
echo "DEBUG: Log file: $LOG_FILE"
echo ""

# Start the original script in background and monitor it
echo "DEBUG: Starting original script..."
echo "DEBUG: Start time: $(date)"

# Run with timeout and capture output
timeout ${TIMEOUT_SECONDS}s "$ORIGINAL_SCRIPT" full 2>&1 | tee "$LOG_FILE" &
SCRIPT_PID=$!

echo "DEBUG: Script PID: $SCRIPT_PID"

# Monitor the process
while kill -0 $SCRIPT_PID 2>/dev/null; do
    echo "DEBUG: Script still running at $(date)..."
    sleep 30
done

# Wait for the process to complete
wait $SCRIPT_PID
EXIT_CODE=$?

echo ""
echo "DEBUG: Script completed at $(date)"
echo "DEBUG: Exit code: $EXIT_CODE"

if [ $EXIT_CODE -eq 124 ]; then
    echo "=========================================="
    echo "INFINITE LOOP DETECTED!"
    echo "=========================================="
    echo "The script was killed after ${TIMEOUT_SECONDS} seconds"
    echo "This indicates an infinite loop in the benchmark execution"
    echo ""
    echo "Check the log file for the last operations before timeout:"
    echo "Log file: $LOG_FILE"
    echo ""
    echo "Last 50 lines of output:"
    tail -50 "$LOG_FILE"
    echo ""
    echo "To investigate further:"
    echo "1. Look at the last benchmark configuration that was running"
    echo "2. Check if it's stuck in a specific operation (insert, search, delete)"
    echo "3. Check if it's related to a specific cache type or configuration"
elif [ $EXIT_CODE -eq 0 ]; then
    echo "=========================================="
    echo "SCRIPT COMPLETED SUCCESSFULLY"
    echo "=========================================="
    echo "No infinite loop detected - the script completed normally"
    echo "If you were experiencing infinite loops before, they may have been fixed"
else
    echo "=========================================="
    echo "SCRIPT FAILED"
    echo "=========================================="
    echo "The script failed with exit code: $EXIT_CODE"
    echo "Check the log file for error details: $LOG_FILE"
fi

echo ""
echo "Full log available at: $LOG_FILE"