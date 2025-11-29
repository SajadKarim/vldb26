cmake_minimum_required(VERSION 3.10)

project(haldendb_benchmark VERSION 1.0)

# Set C++ standard
#set(CMAKE_CXX_STANDARD 20)
#set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
#find_package(glog 0.7.0 REQUIRED)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

# Set build type to Release by default
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Configure build-specific flags
#set(CMAKE_CXX_FLAGS_DEBUG "-g3 -O0 -ggdb3 -fno-omit-frame-pointer")
#set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native -mtune=native")
#set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DNDEBUG -march=native -mtune=native")

# Create compiler flags interface library
add_library(haldendb_compiler_flags INTERFACE)
target_compile_features(haldendb_compiler_flags INTERFACE cxx_std_20)

# Find required packages
find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(PMEM REQUIRED libpmem)

# Conditional assertions - only enable in Debug mode
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(__ENABLE_ASSERTS__)
endif()

# Add compile definitions based on build configuration
# These can be overridden by command line -DCMAKE_CXX_FLAGS
add_compile_definitions(__TREE_WITH_CACHE__)

# Conditionally add other definitions
if(ENABLE_CONCURRENT)
    add_compile_definitions(__CONCURRENT__)
endif()

if(ENABLE_TRACK_FOOTPRINT)
    add_compile_definitions(__TRACK_CACHE_FOOTPRINT__)
endif()

if(ENABLE_VALIDITY_CHECK)
    add_compile_definitions(__VALIDITY_CHECK__)
endif()

# Add subdirectories for libraries
add_subdirectory(../libcache ${PROJECT_BINARY_DIR}/libcache)
add_subdirectory(../libbtree ${PROJECT_BINARY_DIR}/libbtree)

# Set compiler
set(CMAKE_CXX_COMPILER g++-11)

# Create benchmark executable
add_executable(benchmark benchmark.cpp)

# Set target properties
set_target_properties(benchmark PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)

target_compile_options(benchmark PRIVATE -mbmi)

# Build-type specific compile options
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    # Additional compile options for debugging deadlocks
    target_compile_options(benchmark PRIVATE
        -g3      # Maximum debug information
        -O0      # No optimization for better debugging
        -ggdb3   # GDB-specific debug info
        -fno-omit-frame-pointer  # Keep frame pointers for better stack traces
        -Wno-unused-variable  # Ignore unused variable warnings
        -Wno-endif-labels     # Ignore endif label warnings
        -fno-optimize-sibling-calls  # Better stack traces
    )
else()
    # Release optimizations - only add warning suppressions
    target_compile_options(benchmark PRIVATE
        -Wno-unused-variable  # Ignore unused variable warnings
        -Wno-endif-labels     # Ignore endif label warnings
    )
endif()

# Link libraries
target_link_libraries(benchmark PUBLIC libcache haldendb_compiler_flags)
target_link_libraries(benchmark PUBLIC libbtree haldendb_compiler_flags)
target_link_libraries(benchmark PRIVATE Threads::Threads)
target_link_libraries(benchmark PRIVATE ${PMEM_LIBRARIES})

# Include directories
target_include_directories(benchmark PUBLIC
    "${PROJECT_BINARY_DIR}"
    "${PROJECT_SOURCE_DIR}/../libcache"
    "${PROJECT_SOURCE_DIR}/../libbtree"
    "${PROJECT_SOURCE_DIR}"
    ${PMEM_INCLUDE_DIRS}
)

# Add custom targets for different build configurations

# Non-concurrent default configuration
add_custom_target(build_non_concurrent_default
    COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Release 
            -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -O3 -DNDEBUG -march=native -mtune=native" 
            ${CMAKE_SOURCE_DIR}
    COMMAND ${CMAKE_COMMAND} --build . --target benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Building non-concurrent default configuration"
)

# Non-concurrent with footprint tracking
add_custom_target(build_non_concurrent_track_footprint
    COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Release 
            -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__TRACK_CACHE_FOOTPRINT__ -O3 -DNDEBUG -march=native -mtune=native" 
            ${CMAKE_SOURCE_DIR}
    COMMAND ${CMAKE_COMMAND} --build . --target benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Building non-concurrent with footprint tracking configuration"
)

# Concurrent default configuration
add_custom_target(build_concurrent_default
    COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Release 
            -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -O3 -DNDEBUG -march=native -mtune=native" 
            ${CMAKE_SOURCE_DIR}
    COMMAND ${CMAKE_COMMAND} --build . --target benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Building concurrent default configuration"
)

# Concurrent with footprint tracking
add_custom_target(build_concurrent_track_footprint
    COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Release 
            -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__CONCURRENT__ -D__TRACK_CACHE_FOOTPRINT__ -O3 -DNDEBUG -march=native -mtune=native" 
            ${CMAKE_SOURCE_DIR}
    COMMAND ${CMAKE_COMMAND} --build . --target benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Building concurrent with footprint tracking configuration"
)

# Debug configuration
add_custom_target(build_debug
    COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Debug 
            -DCMAKE_CXX_FLAGS="-D__TREE_WITH_CACHE__ -D__VALIDITY_CHECK__ -O0 -g3 -ggdb3" 
            ${CMAKE_SOURCE_DIR}
    COMMAND ${CMAKE_COMMAND} --build . --target benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Building debug configuration"
)

# Print configuration information
message(STATUS "=== HaldenDB Benchmark Configuration ===")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ compiler: ${CMAKE_CXX_COMPILER}")
message(STATUS "C++ standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Source directory: ${CMAKE_SOURCE_DIR}")
message(STATUS "Binary directory: ${CMAKE_BINARY_DIR}")

# Print available custom targets
message(STATUS "")
message(STATUS "Available build targets:")
message(STATUS "  benchmark                          - Build with current configuration")
message(STATUS "  build_non_concurrent_default       - Build non-concurrent default")
message(STATUS "  build_non_concurrent_track_footprint - Build non-concurrent with footprint tracking")
message(STATUS "  build_concurrent_default           - Build concurrent default")
message(STATUS "  build_concurrent_track_footprint   - Build concurrent with footprint tracking")
message(STATUS "  build_debug                        - Build debug configuration")
message(STATUS "========================================")

# Installation rules (optional)
install(TARGETS benchmark
    RUNTIME DESTINATION bin
)

# Create build directory structure
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/results)
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/logs)