cmake_minimum_required(VERSION 3.15)
project(lygus C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Compiler warnings
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra)
endif()
if(MSVC)
    add_compile_options(/W4)
endif()

# ZeroMQ build options
set(ZMQ_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_PERF_TOOL OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC ON CACHE BOOL "" FORCE)
set(ENABLE_DRAFTS OFF CACHE BOOL "" FORCE)

# Dependencies
add_subdirectory(deps/libzmq)
add_subdirectory(deps/lil-raft)

# Platform sources
if(WIN32)
    set(PLATFORM_SOURCES src/platform/platform_win32.c)
else()
    set(PLATFORM_SOURCES src/platform/platform_posix.c)
endif()

# Library
add_library(lygus STATIC
        # Utils
        src/util/crc32c.c
        src/util/varint.c
        src/util/logging.c

        # State
        src/state/kv_store.c
        src/state/kv_op.c

        # Storage
        src/storage/compression/zstd_engine.c
        src/storage/wal/block_format.c
        src/storage/wal/writer.c
        src/storage/wal/recovery.c
        src/storage/wal/wal.c
        src/storage/wal/wal_index.c
        src/storage/snapshot/snapshot.c
        src/storage/storage_mgr.c

        # Raft
        src/raft/raft_glue.c

        # Network
        src/network/network.c
        src/network/mailbox.c

        # Public API
        src/public/lygus_errors.c

        # Platform
        ${PLATFORM_SOURCES}
)

target_include_directories(lygus PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/deps/lil-raft/src
        ${CMAKE_CURRENT_SOURCE_DIR}/deps/libzmq/include
)

find_package(Threads REQUIRED)

target_link_libraries(lygus PUBLIC
        Threads::Threads
        raft
        libzmq-static
)

# Platform specific
if(UNIX AND NOT APPLE)
    target_link_libraries(lygus PUBLIC rt)
endif()
if(WIN32)
    target_link_libraries(lygus PUBLIC ws2_32 iphlpapi)
endif()