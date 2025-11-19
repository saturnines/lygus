cmake_minimum_required(VERSION 3.15)
project(lygus C)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m64")
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -march=native)
endif()

# Find threads (pthread)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Utility library (shared code)
add_library(lygus_util STATIC
        src/util/crc32c.c
        src/util/varint.c
        src/util/logging.c
        src/state/kv_store.c
)
target_include_directories(lygus_util PUBLIC
        src/util
        src/public
        src/state
)

# Link pthread to util library
target_link_libraries(lygus_util PUBLIC Threads::Threads)

# O librt for clock_gettime
if(UNIX AND NOT APPLE)
    target_link_libraries(lygus_util PUBLIC rt)
endif()

# Tests  set to OFF to skip
option(BUILD_TESTS "Build test executables" ON)

if(BUILD_TESTS)
    add_executable(test_crc32c src/tests/test_crc32c.c)
    target_link_libraries(test_crc32c lygus_util)

    add_executable(test_varint src/tests/test_varint.c)
    target_link_libraries(test_varint lygus_util)

    add_executable(test_logging src/tests/test_logging.c)
    target_link_libraries(test_logging lygus_util)

    add_executable(test_kv_store src/tests/test_kv_store.c)
    target_link_libraries(test_kv_store lygus_util)
endif()