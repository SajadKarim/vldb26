#!/bin/bash

# Device-Aware Policy Demonstration Script
# This script demonstrates the DeviceAwarePolicy system without running full benchmarks
# It shows how the system makes intelligent policy selections based on workload and storage

POLICY_SELECTOR="/home/skarim/Code/haldendb_ex/haldendb_pvt_imp/benchmark/build/policy_selector_cli"

echo "=========================================="
echo "Device-Aware Policy System Demonstration"
echo "=========================================="
echo ""

# Check if policy selector exists
if [ ! -f "$POLICY_SELECTOR" ]; then
    echo "Error: policy_selector_cli not found at $POLICY_SELECTOR"
    echo "Please build the project first:"
    echo "  cd /home/skarim/Code/haldendb_ex/haldendb_pvt_imp/benchmark/build"
    echo "  cmake .. && make -j\$(nproc)"
    exit 1
fi

echo "1. Basic Policy Queries"
echo "========================"
echo ""

# Example 1: Read-only workload on DRAM
echo "Query: YCSB-C (read-only) on VolatileStorage (DRAM)"
echo "---------------------------------------------------"
$POLICY_SELECTOR --workload ycsb_c --storage VolatileStorage --verbose
echo ""

# Example 2: Update-heavy workload on PMem
echo "Query: YCSB-A (update-heavy) on PMemStorage"
echo "--------------------------------------------"
$POLICY_SELECTOR --workload ycsb_a --storage PMemStorage --verbose
echo ""

# Example 3: Scan-heavy workload on SSD
echo "Query: YCSB-E (scan-heavy) on FileStorage (SSD)"
echo "------------------------------------------------"
$POLICY_SELECTOR --workload ycsb_e --storage FileStorage --verbose
echo ""

echo "2. Comparison Across Storage Types"
echo "==================================="
echo ""

for workload in ycsb_a ycsb_c ycsb_e; do
    echo "Workload: $workload"
    echo "-------------------"
    
    for storage in VolatileStorage PMemStorage FileStorage IOURingStorage; do
        result=$($POLICY_SELECTOR --workload $workload --storage $storage)
        IFS=',' read -r policy config <<< "$result"
        printf "  %-20s -> %-10s (%s)\n" "$storage" "$policy" "$config"
    done
    echo ""
done

echo "3. Comparison Across Workloads"
echo "==============================="
echo ""

for storage in VolatileStorage PMemStorage; do
    echo "Storage: $storage"
    echo "-------------------"
    
    for workload in ycsb_a ycsb_b ycsb_c ycsb_d ycsb_e ycsb_f; do
        result=$($POLICY_SELECTOR --workload $workload --storage $storage)
        IFS=',' read -r policy config <<< "$result"
        printf "  %-10s -> %-10s (%s)\n" "$workload" "$policy" "$config"
    done
    echo ""
done

echo "4. Shell Script Integration Example"
echo "===================================="
echo ""

cat << 'EOF'
# Example: Integrate into existing benchmark script

# Query optimal policy for current workload/storage
WORKLOAD="ycsb_c"
STORAGE="VolatileStorage"

POLICY_OUTPUT=$(./policy_selector_cli --workload $WORKLOAD --storage $STORAGE)
IFS=',' read -r POLICY CONFIG <<< "$POLICY_OUTPUT"

echo "Selected Policy: $POLICY"
echo "Selected Config: $CONFIG"

# Use the selected configuration to build
case "$CONFIG" in
    "non_concurrent_default")
        CMAKE_FLAGS="-D__TREE_WITH_CACHE__"
        ;;
    "non_concurrent_relaxed")
        CMAKE_FLAGS="-D__TREE_WITH_CACHE__ -D__SELECTIVE_UPDATE__"
        ;;
    "non_concurrent_lru_metadata_update_in_order")
        CMAKE_FLAGS="-D__TREE_WITH_CACHE__ -D__UPDATE_IN_ORDER__"
        ;;
    *)
        CMAKE_FLAGS="-D__TREE_WITH_CACHE__"
        ;;
esac

# Build with selected configuration
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="$CMAKE_FLAGS -O3"
make -j$(nproc)

# Run benchmark with selected policy
./benchmark --cache-type $POLICY --storage-type $STORAGE ...
EOF

echo ""
echo ""

echo "5. Complete Decision Matrix"
echo "============================"
echo ""
$POLICY_SELECTOR --print-matrix

echo ""
echo "=========================================="
echo "Demonstration Complete!"
echo "=========================================="
echo ""
echo "Key Takeaways:"
echo "-------------"
echo "1. Different workloads benefit from different cache policies"
echo "2. Storage characteristics influence optimal policy selection"
echo "3. Configuration variants (relaxed, in_order, etc.) are automatically selected"
echo "4. Easy integration into existing shell scripts"
echo "5. All decisions include rationale for transparency"
echo ""
echo "Next Steps:"
echo "-----------"
echo "1. Run full benchmarks: ./run_device_aware_ycsb.sh"
echo "2. Compare with fixed policies: ./compare_device_aware_vs_fixed.sh"
echo "3. Integrate into your own benchmark scripts"
echo ""