#!/bin/bash

cd /home/skarim/Code/haldendb_ex/haldendb_pvt/benchmark/build

echo "Starting GDB debugging session..."
echo "This will catch the exception when it occurs."

gdb -batch -ex "set print pretty on" \
    -ex "set print object on" \
    -ex "set print demangle on" \
    -ex "catch throw" \
    -ex "run --config \"bm_cache\" --cache-type \"CLOCK\" --storage-type \"VolatileStorage\" --cache-size \"500\" --tree-type \"BplusTreeSOA\" --key-type \"uint64_t\" --value-type \"uint64_t\" --operation \"delete\" --degree \"64\" --records \"500000\" --runs \"1\"" \
    -ex "bt" \
    -ex "info threads" \
    -ex "thread apply all bt" \
    -ex "quit" \
    ./benchmark