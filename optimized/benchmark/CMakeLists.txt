cmake_minimum_required(VERSION 3.10)

project(benchmark VERSION 1.0)

#set(CMAKE_CXX_STANDARD 20)
#set(CMAKE_CXX_STANDARD_REQUIRED ON)
#set(CMAKE_PREFIX_PATH "/usr/lib/x86_64-linux-gnu")

#find_package (glog 0.7.0 REQUIRED)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2")

# Set default build type if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Configure build-specific flags
#set(CMAKE_CXX_FLAGS_DEBUG "-g3 -O0 -ggdb3 -fno-omit-frame-pointer")
#set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG -march=native -mtune=native -mavx2")
#set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DNDEBUG -march=native -mtune=native")
#set(CMAKE_PREFIX_PATH "/usr/lib/x86_64-linux-gnu/")

add_library(haldendb_compiler_flags INTERFACE)
target_compile_features(haldendb_compiler_flags  INTERFACE cxx_std_20)

#configure_file(TutorialConfig.h.in TutorialConfig.h)


# Conditional assertions - only enable in Debug mode
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(__ENABLE_ASSERTS__)
endif()
#add_compile_definitions(__CONCURRENT__)
# Note: Benchmark tests no cache trees, so we don't define __TREE_WITH_CACHE__
add_compile_definitions(__TREE_WITH_CACHE__)
#add_compile_definitions(__VALIDITY_CHECK__)

add_subdirectory(../libcache ${PROJECT_BINARY_DIR}/libcache)
add_subdirectory(../libbtree ${PROJECT_BINARY_DIR}/libbtree)


set(CMAKE_CXX_COMPILER g++-11)

add_executable(benchmark benchmark.cpp)


find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
#find_library(URING_LIB uring REQUIRED)
pkg_check_modules(LIBPMEM REQUIRED libpmem)
pkg_check_modules(LIBURING REQUIRED liburing)

target_link_libraries(benchmark PRIVATE Threads::Threads)

target_include_directories(benchmark PRIVATE ${LIBURING_INCLUDE_DIRS})
link_directories(${LIBURING_LIBRARY_DIRS})
target_link_libraries(benchmark PRIVATE ${LIBURING_LIBRARIES})

set_target_properties(benchmark PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)

target_compile_options(benchmark PRIVATE -mbmi)

#target_compile_options(benchmark PRIVATE
#    -ggdb3
#    -O0
#    -g3
#    -Wall)

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

target_link_libraries(benchmark PUBLIC libcache haldendb_compiler_flags)
target_link_libraries(benchmark PUBLIC libbtree haldendb_compiler_flags)
#target_link_libraries (benchmark PUBLIC glog::glog)
#target_link_libraries(benchmark PRIVATE uring)

target_link_libraries(benchmark PRIVATE pmem uring)

target_include_directories(benchmark PUBLIC
                           "${PROJECT_BINARY_DIR}"
                           "${PROJECT_SOURCE_DIR}/../libcache"
                           "${PROJECT_SOURCE_DIR}/../libbtree"
                           )