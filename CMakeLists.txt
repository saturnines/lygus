cmake_minimum_required(VERSION 3.15)
project(lygus C)

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
)
target_include_directories(lygus_util PUBLIC
        src/util
        src/public
)

# Link pthread to util library
target_link_libraries(lygus_util PUBLIC Threads::Threads)

# On some systems (older glibc), need librt for clock_gettime
if(UNIX AND NOT APPLE)
    target_link_libraries(lygus_util PUBLIC rt)
endif()

# Test for CRC32C
add_executable(test_crc32c src/tests/test_crc32c.c
        )
target_link_libraries(test_crc32c lygus_util)

# Test for varint
add_executable(test_varint src/tests/test_varint.c)
target_link_libraries(test_varint lygus_util)

# Optional: Test for logging
add_executable(test_logging src/tests/test_logging.c)
target_link_libraries(test_logging lygus_util)