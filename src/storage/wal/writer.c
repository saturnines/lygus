#include "writer.h"
#include "../../util/logging.h"
#include "../../util/timing.h"
#include "../compression/zstd_engine.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include "recovery.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>


// Cross-platform binary mode flag
#ifdef _WIN32
    #define O_BINARY _O_BINARY
#else
    #define O_BINARY 0
#endif

// ============================================================================
// Internal State
// ============================================================================

struct wal_writer {
    // File state
    int         fd;             // Current segment file descriptor
    uint64_t    segment_num;    // Current segment number (1-based)
    uint64_t    write_offset;   // Current write position in segment
    char        data_dir[256];  // Data directory path

    // Block buffering
    uint8_t     block_buf[WAL_BLOCK_SIZE];  // 64 KiB staging buffer
    size_t      block_fill;                  // Bytes used in block_buf
    uint64_t    block_seq;                   // Global block sequence number
    uint16_t    block_flags;                 // Flags for next block flush

    // Compression
    lygus_zstd_ctx_t *zctx;     // Zstd compression context
    int                zstd_level;

    // Fsync tracking (for group commit)
    uint64_t    bytes_since_fsync;      // Bytes written since last fsync
    uint64_t    fsync_interval_us;      // Microseconds between fsyncs
    size_t      fsync_bytes;            // Bytes threshold for fsync
    uint64_t    last_fsync_time_us;     // Last fsync timestamp (TODO: add timing)
};

// ============================================================================
// Helpers
// ============================================================================

/**
 * Get current segment file path
 */
static void get_segment_path(const char *data_dir, uint64_t seg_num, char *out, size_t out_len) {
    snprintf(out, out_len, "%s/WAL-%06llu.log", data_dir, seg_num);
}

/**
 * Find highest segment number in directory
 * Returns 0 if no segments exist
 */
static uint64_t find_latest_segment(const char *data_dir) {
    DIR *dir = opendir(data_dir);
    if (!dir) return 0;

    uint64_t max_seg = 0;
    struct dirent *entry;

    while ((entry = readdir(dir)) != NULL) {
        uint64_t seg_num;
        if (sscanf(entry->d_name, "WAL-%llu.log", &seg_num) == 1) {
            if (seg_num > max_seg) {
                max_seg = seg_num;
            }
        }
    }

    closedir(dir);
    return max_seg;
}

/**
 * Open segment file for appending
 * Returns fd or -1 on error
 */
static int open_segment(const char *data_dir, uint64_t seg_num, int create) {
    char path[512];
    get_segment_path(data_dir, seg_num, path, sizeof(path));

    int flags = O_RDWR | O_BINARY;  // O_BINARY is critical on Windows!
    if (create) {
        flags |= O_CREAT | O_EXCL;
    }

    int fd = open(path, flags, 0644);
    if (fd < 0) {
        return -1;
    }

    // Seek to end if appending to existing file
    if (!create) {
        off_t end = lseek(fd, 0, SEEK_END);
        if (end < 0) {
            close(fd);
            return -1;
        }
    }

    return fd;
}

/**
 * Check if entry fits in remaining block space
 */
static int entry_fits(const wal_writer_t *w,
                      wal_entry_type_t type,
                      uint64_t index, uint64_t term,
                      size_t klen, size_t vlen)
{
    ssize_t entry_size = wal_entry_size(type, index, term, klen, vlen);
    if (entry_size < 0) {
        return 0;  // Invalid entry
    }

    return (w->block_fill + entry_size) <= WAL_BLOCK_SIZE;
}

/**
 * Write block to disk (internal)
 * Compresses, writes header + payload, updates offsets
 */
static int write_block(wal_writer_t *w) {
    if (w->block_fill == 0) {
        return LYGUS_OK;  // Nothing to write
    }



    uint64_t start_ns = lygus_now_ns();  // ← START TIMER

    // Allocate compressed buffer (needs headroom for Zstd metadata)
    uint8_t comp_buf[WAL_BLOCK_SIZE + 1024];
    wal_block_hdr_t hdr;

    // Compress block
    ssize_t comp_len = wal_block_compress(w->block_buf, w->block_fill,
                                          w->block_seq, w->zctx,
                                          &hdr, comp_buf, sizeof(comp_buf));
    if (comp_len < 0) {
        LOG_ERROR(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_CORRUPTION,
                  0, 0, NULL, 0);
        return (int)comp_len;
    }

    // Apply any pending flags
    hdr.flags |= w->block_flags;
    w->block_flags = 0;  // Clear flags after applying

    // Write header
    ssize_t n = write(w->fd, &hdr, sizeof(hdr));
    if (n != sizeof(hdr)) {
        return (errno == ENOSPC) ? LYGUS_ERR_DISK_FULL : LYGUS_ERR_WRITE;
    }

    // Write compressed payload
    n = write(w->fd, comp_buf, comp_len);
    if (n != comp_len) {
        return (errno == ENOSPC) ? LYGUS_ERR_DISK_FULL : LYGUS_ERR_WRITE;
    }

    uint64_t end_ns = lygus_now_ns();  // ← END TIMER

    // Update state
    size_t total_written = sizeof(hdr) + comp_len;
    w->write_offset += total_written;
    w->bytes_since_fsync += total_written;
    w->block_seq++;
    w->block_fill = 0;  // Reset buffer

    // Log block write WITH LATENCY
    struct {
        uint64_t seq_no;
        uint32_t raw_len;
        uint32_t comp_len;
        uint32_t latency_us;
    } log_data = {
        .seq_no = hdr.seq_no,
        .raw_len = hdr.raw_len,
        .comp_len = hdr.comp_len,
        .latency_us = lygus_ns_to_us(end_ns - start_ns),  // ← FIXED
    };
    LOG_DEBUG(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_BLOCK_DONE,
              0, 0, &log_data, sizeof(log_data));

    return LYGUS_OK;
}


/**
 * Scan segment file to find highest block sequence number
 * Returns highest seq_no found, or 0 if file is empty/unreadable
 */
static uint64_t find_highest_block_seq(int fd) {
    uint64_t highest_seq = 0;
    off_t offset = 0;  // Track absolute position

    // Seek to start
    if (lseek(fd, 0, SEEK_SET) < 0) {
        return 0;  // Can't seek, assume empty
    }

    while (1) {
        wal_block_hdr_t hdr;
        ssize_t n = read(fd, &hdr, sizeof(hdr));

        if (n == 0) {
            break;  // EOF
        }

        if (n != sizeof(hdr)) {
            break;  // Partial header, stop
        }

        // Validate header magic (basic sanity check)
        if (hdr.magic != WAL_BLOCK_MAGIC) {
            break;  // Corrupt, stop
        }

        // Track highest sequence number
        if (hdr.seq_no > highest_seq) {
            highest_seq = hdr.seq_no;
        }

        // Move to next block (skip compressed payload)
        // FIXED: Use SEEK_CUR to skip relative to current position
        off_t skip = (off_t)hdr.comp_len;
        if (lseek(fd, skip, SEEK_CUR) < 0) {
            break;  // Seek failed, stop
        }

        offset += sizeof(hdr) + hdr.comp_len;  // Track for debugging (optional)
    }

    // Restore file position to end for appending
    lseek(fd, 0, SEEK_END);

    return highest_seq;
}

// ============================================================================
// Public API - Lifecycle
// ============================================================================

wal_writer_t* wal_writer_open(const wal_writer_opts_t *opts) {
    if (!opts || !opts->data_dir) {
        errno = EINVAL;
        return NULL;
    }

    // Allocate writer
    wal_writer_t *w = calloc(1, sizeof(wal_writer_t));
    if (!w) {
        return NULL;
    }

    // Copy config
    strncpy(w->data_dir, opts->data_dir, sizeof(w->data_dir) - 1);
    w->zstd_level = (opts->zstd_level > 0) ? opts->zstd_level : 3;
    w->fsync_interval_us = opts->fsync_interval_us;
    w->fsync_bytes = opts->fsync_bytes;

    // Create compression context
    w->zctx = lygus_zstd_create(w->zstd_level);
    if (!w->zctx) {
        free(w);
        return NULL;
    }

    // Ensure data directory exists
    #ifdef _WIN32
        mkdir(opts->data_dir);
    #else
        mkdir(opts->data_dir, 0755);
    #endif

    // Find latest segment
    uint64_t latest = find_latest_segment(opts->data_dir);

    if (latest > 0) {
        // Append to existing segment
        w->segment_num = latest;
        w->fd = open_segment(opts->data_dir, latest, 0);
        if (w->fd < 0) {
            lygus_zstd_destroy(w->zctx);
            free(w);
            return NULL;
        }

        // Get current offset
        w->write_offset = lseek(w->fd, 0, SEEK_CUR);

        // Scan to recover highest block_seq
        uint64_t highest_seq = find_highest_block_seq(w->fd);
        w->block_seq = highest_seq + 1;  // Continue from next sequence number

        LOG_INFO(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_RECOVERY,
                 0, 0, &highest_seq, sizeof(highest_seq));
    } else {
        // Create first segment
        w->segment_num = 1;
        w->fd = open_segment(opts->data_dir, 1, 1);
        if (w->fd < 0) {
            lygus_zstd_destroy(w->zctx);
            free(w);
            return NULL;
        }

        w->write_offset = 0;
        w->block_seq = 0;
    }

    w->block_fill = 0;
    w->block_flags = 0;
    w->bytes_since_fsync = 0;
    w->last_fsync_time_us = 0;

    LOG_INFO_SIMPLE(LYGUS_MODULE_WAL, LYGUS_EVENT_INIT, 0, 0);

    return w;
}

int wal_writer_close(wal_writer_t *w) {
    if (!w) {
        return LYGUS_ERR_INVALID_ARG;
    }

    printf("DEBUG: wal_writer_close() segment=%llu, block_fill=%zu\n",
           w->segment_num, w->block_fill);

    // Mark last block and flush
    wal_writer_mark_last_block(w);
    int ret = wal_writer_flush(w, 1);  // Flush and sync

    // Close file
    if (w->fd >= 0) {
        close(w->fd);
    }

    // Cleanup
    lygus_zstd_destroy(w->zctx);
    free(w);

    LOG_INFO_SIMPLE(LYGUS_MODULE_WAL, LYGUS_EVENT_SHUTDOWN, 0, 0);

    return ret;
}

// ============================================================================
// Public API - Write Operations
// ============================================================================

int wal_writer_append(wal_writer_t *w,
                      wal_entry_type_t type,
                      uint64_t index, uint64_t term,
                      const void *key, size_t klen,
                      const void *val, size_t vlen)
{
    if (!w) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Check if segment is getting too large
    if (w->write_offset >= WAL_SEGMENT_MAX_SIZE) {
        return LYGUS_ERR_WAL_FULL;  // Caller should rotate
    }

    // Check if entry fits in current block
    if (!entry_fits(w, type, index, term, klen, vlen)) {
        printf("DEBUG: Entry doesn't fit! block_fill=%zu, flushing...\n", w->block_fill);
        // Flush current block first
        int ret = write_block(w);
        if (ret < 0) {
            return ret;
        }
    }

    // Encode entry into block buffer
    ssize_t encoded = wal_entry_encode(type, index, term,
                                       key, klen, val, vlen,
                                       &w->block_buf[w->block_fill],
                                       WAL_BLOCK_SIZE - w->block_fill);
    if (encoded < 0) {  //
        return (int)encoded;  // Propagate error
    }

    w->block_fill += (size_t)encoded;  // Now safe to update

    if (w->segment_num == 2) {  // Only log for segment 2
        printf("DEBUG SEG2: append index=%llu, block_fill now=%zu\n",
               index, w->block_fill);
    }

    // NEW: Flush block if it's full, but DON'T fsync
    if (w->block_fill >= WAL_BLOCK_SIZE * 0.9) {
        int ret = write_block(w);
        if (ret < 0) {
            return ret;
        }
    }

    // CRITICAL: For Paxos I2 invariant, we MUST fsync before returning
    // Option A: Always fsync (simplest, correct, but slow)
    //  can prolly optimize it with batching at the raft layer
    int ret = wal_writer_flush(w, 1);  // Force flush + sync
    if (ret < 0) {
        return ret;
    }

    return LYGUS_OK;  // Now guaranteed durable
}

int wal_writer_flush(wal_writer_t *w, int sync) {
    if (!w) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Write current block
    int ret = write_block(w);
    if (ret < 0) {
        return ret;
    }

    // Fsync if requested
    if (sync) {
        return wal_writer_fsync(w);
    }

    return LYGUS_OK;
}

int wal_writer_fsync(wal_writer_t *w) {
    if (!w) {
        return LYGUS_ERR_INVALID_ARG;
    }

    uint64_t start_ns = lygus_now_ns();  // ← START TIMER

#ifdef _WIN32
    if (_commit(w->fd) < 0) {
#else
    if (fdatasync(w->fd) < 0) {
#endif
        LOG_ERROR(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_FSYNC_DONE,
                  0, 0, NULL, 0);
        return LYGUS_ERR_FSYNC;
    }

    uint64_t end_ns = lygus_now_ns();  // ← END TIMER

    w->bytes_since_fsync = 0;
    w->last_fsync_time_us = lygus_now_us();  // ← FIXED (update last fsync time)

    // Log fsync WITH LATENCY
    struct {
        uint32_t latency_us;
    } log_data = {
        .latency_us = lygus_ns_to_us(end_ns - start_ns),  // ← FIXED
    };
    LOG_INFO(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_FSYNC_DONE,
             0, 0, &log_data, sizeof(log_data));

    return LYGUS_OK;
}

// ============================================================================
// Public API - Segment Management
// ============================================================================

int wal_writer_rotate(wal_writer_t *w) {
    if (!w) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Mark last block and flush current segment
    wal_writer_mark_last_block(w);
    int ret = wal_writer_flush(w, 1);  // Flush and sync
    if (ret < 0) {
        return ret;
    }

    // Close current segment
    close(w->fd);
    w->fd = -1;

    // Open next segment
    w->segment_num++;
    w->fd = open_segment(w->data_dir, w->segment_num, 1);
    if (w->fd < 0) {
        return LYGUS_ERR_OPEN_FILE;
    }

    // Reset state
    w->write_offset = 0;
    w->block_fill = 0;
    w->bytes_since_fsync = 0;
    // Note: block_seq continues monotonically

    LOG_INFO_SIMPLE(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_RECOVERY,
                    0, w->segment_num);

    return LYGUS_OK;
}

int wal_writer_mark_last_block(wal_writer_t *w) {
    if (!w) {
        return LYGUS_ERR_INVALID_ARG;
    }

    w->block_flags |= WAL_FLAG_LAST_BLOCK;
    return LYGUS_OK;
}

int wal_writer_is_durable(const wal_writer_t *w) {
    if (!w) return 0;

    // If there's data in the block buffer, it's not durable
    if (w->block_fill > 0) {
        return 0;
    }

    // If there are bytes written but not fsynced, not durable
    if (w->bytes_since_fsync > 0) {
        return 0;
    }

    return 1;
}

// ============================================================================
// Public API - Observability
// ============================================================================

uint64_t wal_writer_segment_num(const wal_writer_t *w) {
    return w ? w->segment_num : 0;
}

uint64_t wal_writer_offset(const wal_writer_t *w) {
    return w ? w->write_offset : 0;
}

uint64_t wal_writer_block_seq(const wal_writer_t *w) {
    return w ? w->block_seq : 0;
}

size_t wal_writer_block_fill(const wal_writer_t *w) {
    return w ? w->block_fill : 0;
}

int wal_writer_block_is_empty(const wal_writer_t *w) {
    return w ? (w->block_fill == 0) : 1;
}