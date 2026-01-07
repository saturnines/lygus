cmake_minimum_required(VERSION 3.15)
project(lygus C CXX)

option(ENABLE_ASAN "Enable AddressSanitizer" OFF)

if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=address)
endif()

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Compiler warnings
if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra)
endif()
if(MSVC)
    add_compile_options(/W4)
endif()



# =============================================================================
# Dependencies
# =============================================================================

# ZeroMQ build options
set(ZMQ_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_PERF_TOOL OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(BUILD_STATIC ON CACHE BOOL "" FORCE)
set(ENABLE_DRAFTS OFF CACHE BOOL "" FORCE)

add_subdirectory(deps/libzmq)
add_subdirectory(deps/lil-raft)

# =============================================================================
# Platform Sources
# =============================================================================

if(WIN32)
    set(PLATFORM_SOURCES src/platform/platform_win32.c)
else()
    set(PLATFORM_SOURCES src/platform/platform_posix.c)
endif()

# =============================================================================
# Lygus Library
# =============================================================================

add_library(lygus STATIC
        # Platform abstraction
        ${PLATFORM_SOURCES}

        # Utils
        src/util/crc32c.c
        src/util/varint.c
        src/util/logging.c

        # State
        src/state/kv_store.c
        src/state/kv_op.c

        # Storage
        src/storage/wal/block_format.c
        src/storage/wal/writer.c
        src/storage/wal/recovery.c
        src/storage/wal/wal.c
        src/storage/wal/wal_index.c
        src/storage/snapshot/snapshot.c
        src/storage/storage_mgr.c

        # Raft glue
        src/raft/raft_glue.c

        # Network
        src/network/network.c
        src/network/mailbox.c

        # Server
        src/server/server.c
        src/server/handler.c
        src/server/conn.c
        src/server/protocol.c
        src/server/pending.c

        # ALR (Asynchronous Linearizable Reads)
        src/ALRs/alr.c

        # Event loop
        src/event/event_loop.c

        # Public API
        src/public/lygus_errors.c
)

target_include_directories(lygus PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/deps/lil-raft/src
        ${CMAKE_CURRENT_SOURCE_DIR}/deps/libzmq/include
)

# =============================================================================
# Linking
# =============================================================================

find_package(Threads REQUIRED)

target_link_libraries(lygus PUBLIC
        Threads::Threads
        raft
        libzmq-static
        stdc++
)


if(UNIX AND NOT APPLE)
    target_link_libraries(lygus PUBLIC rt)
endif()
if(WIN32)
    target_link_libraries(lygus PUBLIC ws2_32 iphlpapi)
endif()

# =============================================================================
# Server Binary
# =============================================================================

add_executable(lygus-server
        cmd/lygus-server/main.c
)

target_include_directories(lygus-server PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/cmd/lygus-server
)

target_link_libraries(lygus-server PRIVATE lygus stdc++)

# =============================================================================
# Install
# =============================================================================

install(TARGETS lygus-server RUNTIME DESTINATION bin)
install(TARGETS lygus ARCHIVE DESTINATION lib)
install(FILES
        src/public/lygus.h
        src/public/lygus_errors.h
        DESTINATION include/lygus
)