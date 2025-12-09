cmake_minimum_required(VERSION 3.15)
project(lygus C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra)
endif()

if(MSVC)
    add_compile_options(/W4)
endif()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_library(lygus_util STATIC
        # Util
        src/util/crc32c.c
        src/util/varint.c
        src/util/logging.c

        # State
        src/state/kv_store.c

        # Compression
        src/storage/compression/zstd_engine.c

        # WAL
        src/storage/wal/block_format.c
        src/storage/wal/writer.c
        src/storage/wal/recovery.c
        src/storage/wal/wal.c
        src/storage/wal/wal_index.c

        # Snapshot
        src/storage/snapshot/snapshot.c

        # Public API
        src/public/lygus.c
        src/public/lygus_errors.c
        src/storage/storage_mgr.c
)

target_include_directories(lygus_util PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(lygus_util PUBLIC Threads::Threads)

if(UNIX AND NOT APPLE)
    target_link_libraries(lygus_util PUBLIC rt)
endif()

# Tests
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

    add_executable(test_wal src/tests/test_wal.c)
    target_link_libraries(test_wal lygus_util)

    add_executable(test_wal_corruption src/tests/test_wal_corruption.c)
    target_link_libraries(test_wal_corruption lygus_util)

    add_executable(test_wal_index src/tests/test_wal_index.c)
    target_link_libraries(test_wal_index lygus_util)

    add_executable(test_snapshot src/tests/test_snapshot.c)
    target_link_libraries(test_snapshot lygus_util)

    add_executable(test_snapshot_edgecases src/tests/test_snapshot_edgecases.c)
    target_link_libraries(test_snapshot_edgecases lygus_util)


    add_executable(test_storage_mgr src/tests/test_storage_mgr.c)
    target_link_libraries(test_storage_mgr lygus_util)

    if(NOT WIN32)
        add_executable(test_wal_crash src/tests/test_wal_crash.c)
        target_link_libraries(test_wal_crash lygus_util)
    endif()
endif()