#include "recovery.h"
#include "../../util/logging.h"
#include "../../util/timing.h"
#include "../compression/zstd_engine.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>


#ifdef _WIN32
    #define fsync_wrapper(fd) _commit(fd)
#else
    #define fsync_wrapper(fd) fdatasync(fd)
#endif



// ============================================================================
// Scanner State
// ============================================================================

struct wal_scanner {
    int                   fd;          // File descriptor
    uint64_t              offset;      // Current read position
    lygus_zstd_ctx_t     *zctx;        // Decompression context (borrowed)
    char                  path[512];   // File path (for truncation)
};

// ============================================================================
// Helpers
// ============================================================================

/**
 * Compare function for qsort (segment numbers)
 */
static int compare_uint64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

// ============================================================================
// Public API - Segment Listing
// ============================================================================

int wal_list_segments(const char *data_dir,
                      uint64_t **out_segs,
                      size_t *out_count)
{
    if (!data_dir || !out_segs || !out_count) {
        return LYGUS_ERR_INVALID_ARG;
    }

    DIR *dir = opendir(data_dir);
    if (!dir) {
        return LYGUS_ERR_IO;
    }

    // First pass: count segments
    size_t count = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        uint64_t seg_num;
        if (sscanf(entry->d_name, "WAL-%llu.log", &seg_num) == 1) {
            count++;
        }
    }

    if (count == 0) {
        closedir(dir);
        *out_segs = NULL;
        *out_count = 0;
        return LYGUS_OK;
    }

    // Allocate array
    uint64_t *segs = malloc(count * sizeof(uint64_t));
    if (!segs) {
        closedir(dir);
        return LYGUS_ERR_NOMEM;
    }

    // Second pass: collect segment numbers
    rewinddir(dir);
    size_t idx = 0;
    while ((entry = readdir(dir)) != NULL && idx < count) {
        uint64_t seg_num;
        if (sscanf(entry->d_name, "WAL-%llu.log", &seg_num) == 1) {
            segs[idx++] = seg_num;
        }
    }
    closedir(dir);

    // Sort segments
    qsort(segs, count, sizeof(uint64_t), compare_uint64);

    *out_segs = segs;
    *out_count = count;
    return LYGUS_OK;
}

// ============================================================================
// Public API - Scanner
// ============================================================================

wal_scanner_t* wal_scanner_open(const char *path, void *zctx) {
    if (!path || !zctx) {
        errno = EINVAL;
        return NULL;
    }

    int fd = open(path, O_RDWR | O_BINARY);  // You must open files in binary mode during binary i/o or windows will fuck with it
    if (fd < 0) {
        return NULL;
    }

    wal_scanner_t *scanner = malloc(sizeof(wal_scanner_t));
    if (!scanner) {
        close(fd);
        return NULL;
    }

    scanner->fd = fd;
    scanner->offset = 0;
    scanner->zctx = (lygus_zstd_ctx_t *)zctx;
    strncpy(scanner->path, path, sizeof(scanner->path) - 1);
    scanner->path[sizeof(scanner->path) - 1] = '\0';

    return scanner;
}

ssize_t wal_scanner_next_block(wal_scanner_t *scanner,
                                wal_block_hdr_t *hdr,
                                uint8_t *raw_buf, size_t raw_cap)
{
    if (!scanner || !hdr || !raw_buf) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (raw_cap < WAL_BLOCK_SIZE) {
        return LYGUS_ERR_INCOMPLETE;
    }

    // CRITICAL: Save offset before reading anything
    uint64_t block_start_offset = scanner->offset;

    // Read block header
    ssize_t n = read(scanner->fd, hdr, sizeof(*hdr));
    if (n == 0) {
        return 0;  // EOF
    }
    if (n != sizeof(*hdr)) {
        // Partial header at EOF - DON'T update offset, truncate at block start
        return LYGUS_ERR_TRUNCATED;
    }

    scanner->offset += sizeof(*hdr);

    // Validate header
    // int ret = wal_block_validate_header(hdr);
    // if (ret < 0) {
        // Bad header - will truncate at block_start_offset
    //    return ret;
    // }

    // Allocate buffer for compressed data
    uint8_t comp_buf[WAL_BLOCK_SIZE + 1024];
    size_t comp_len = hdr->comp_len;

    if (comp_len > sizeof(comp_buf)) {
        return LYGUS_ERR_BAD_BLOCK;
    }

    // Read compressed payload
    n = read(scanner->fd, comp_buf, comp_len);
    if (n != (ssize_t)comp_len) {
        // Partial block at EOF - will truncate at block_start_offset
        return LYGUS_ERR_TRUNCATED;
    }

    scanner->offset += comp_len;

    // Decompress block
    ssize_t decompressed = wal_block_decompress(hdr, comp_buf, comp_len,
                                                 scanner->zctx,
                                                 raw_buf, raw_cap);
    if (decompressed < 0) {
        // Corruption - will truncate at block_start_offset
        return decompressed;
    }

    return decompressed;
}

uint64_t wal_scanner_offset(const wal_scanner_t *scanner) {
    return scanner ? scanner->offset : 0;
}

int wal_scanner_truncate(wal_scanner_t *scanner) {
    if (!scanner) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // CRITICAL: offset should already be at block_start before calling this
    // The caller (wal_recover) needs to track the last good position

    // Truncate at current offset (which should be start of bad block)
    if (ftruncate(scanner->fd, scanner->offset) < 0) {
        return LYGUS_ERR_IO;
    }

    // Fsync to ensure truncation is durable
#ifdef _WIN32
    if (_commit(scanner->fd) < 0) {
#else
    if (fdatasync(scanner->fd) < 0) {
#endif
        return LYGUS_ERR_FSYNC;
    }

    LOG_WARN(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_CORRUPTION,
             0, 0, NULL, 0);

    return LYGUS_OK;
}

void wal_scanner_close(wal_scanner_t *scanner) {
    if (!scanner) return;

    if (scanner->fd >= 0) {
        close(scanner->fd);
    }
    free(scanner);
}

// ============================================================================
// Public API - Entry Iterator
// ============================================================================

int wal_block_next_entry(const uint8_t *buf, size_t buf_len,
                         size_t *offset,
                         wal_entry_t *entry)
{
    if (!buf || !offset || !entry) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Check if we're at the end
    if (*offset >= buf_len) {
        return LYGUS_ERR_INCOMPLETE;
    }

    // Try to decode entry
    ssize_t consumed = wal_entry_decode(&buf[*offset], buf_len - *offset, entry);
    if (consumed < 0) {
        return (int)consumed;
    }

    *offset += consumed;
    return LYGUS_OK;
}

int wal_scanner_truncate_at(wal_scanner_t *scanner, uint64_t offset) {
    if (!scanner) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Seek to the truncation point
    if (lseek(scanner->fd, offset, SEEK_SET) < 0) {
        return LYGUS_ERR_IO;
    }

    // Truncate at this offset
    if (ftruncate(scanner->fd, offset) < 0) {
        return LYGUS_ERR_IO;
    }

    // Fsync
#ifdef _WIN32
    if (_commit(scanner->fd) < 0) {
#else
    if (fdatasync(scanner->fd) < 0) {
#endif
        return LYGUS_ERR_FSYNC;
    }

    scanner->offset = offset;

    LOG_WARN(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_CORRUPTION, 0, 0, NULL, 0);

    return LYGUS_OK;
}

// ============================================================================
// Public API - High-Level Recovery
// ============================================================================

int wal_recover(const char *data_dir,
                void *zctx,
                wal_entry_callback_t callback,
                void *user_data,
                wal_recovery_result_t *result) {
    if (!data_dir || !zctx || !result) {
        return LYGUS_ERR_INVALID_ARG;
    }

    uint64_t start_ns = lygus_now_ns();  // ← START TIMER

    // Initialize result
    memset(result, 0, sizeof(*result));

    // List all segments
    uint64_t *segments = NULL;
    size_t num_segments = 0;
    int ret = wal_list_segments(data_dir, &segments, &num_segments);
    if (ret < 0) {
        return ret;
    }

    if (num_segments == 0) {
        // No WAL segments - fresh start
        return LYGUS_OK;
    }

    result->num_segments = num_segments;

    LOG_INFO_SIMPLE(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_RECOVERY, 0, 0);

    // Scan each segment in order
    for (size_t i = 0; i < num_segments; i++) {
        uint64_t seg_num = segments[i];

        // Build segment path
        char seg_path[512];
        snprintf(seg_path, sizeof(seg_path), "%s/WAL-%06llu.log", data_dir, seg_num);

        // Open scanner
        wal_scanner_t *scanner = wal_scanner_open(seg_path, zctx);

        if (!scanner) {
            free(segments);
            return LYGUS_ERR_IO;
        }

        // Allocate block buffer
        uint8_t block_buf[WAL_BLOCK_SIZE];
        wal_block_hdr_t hdr;

        // Read blocks from this segment
        int corrupted = 0;

        while (1) {
            uint64_t block_start = wal_scanner_offset(scanner);  // Before reading

            ssize_t decompressed = wal_scanner_next_block(scanner, &hdr,
                                                          block_buf, sizeof(block_buf));

            if (decompressed == 0) {
                // EOF - normal termination
                break;
            }

            if (decompressed < 0) {
                printf("DEBUG: Segment %llu - Block read error: %zd at offset %llu\n",
                       (unsigned long long)seg_num, decompressed,
                       (unsigned long long)block_start);
                result->corruptions++;
                wal_scanner_truncate_at(scanner, block_start);
                result->truncated = 1;
                corrupted = 1;
                break;
            }

            // Block read successfully
            result->num_blocks++;
            result->bytes_scanned += sizeof(hdr) + hdr.comp_len;

            // Iterate entries in this block
            size_t entry_offset = 0;
            while (entry_offset < (size_t)decompressed) {
                wal_entry_t entry;
                ret = wal_block_next_entry(block_buf, decompressed,
                                          &entry_offset, &entry);

                if (ret == LYGUS_ERR_INCOMPLETE) {
                    break;
                }

                if (ret < 0) {
                    result->corruptions++;
                    wal_scanner_truncate_at(scanner, block_start);
                    result->truncated = 1;
                    corrupted = 1;
                    break;
                }

                result->num_entries++;

                // Track highest index/term
                if (entry.index > result->highest_index) {
                    result->highest_index = entry.index;
                }
                if (entry.term > result->highest_term) {
                    result->highest_term = entry.term;
                }

                // Invoke callback
                if (callback) {
                    ret = callback(&entry, user_data);
                    if (ret < 0) {
                        wal_scanner_close(scanner);
                        free(segments);
                        return ret;
                    }
                }
            }

            if (corrupted) {
                break;
            }

            // Check if this was the last block in segment
            if (hdr.flags & WAL_FLAG_LAST_BLOCK) {
                break;
            }
        }

        wal_scanner_close(scanner);

        // If we hit corruption, DELETE and STOP
        if (corrupted) {
            LOG_WARN(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_CORRUPTION,
                     0, seg_num, NULL, 0);

            // Delete all segments after the corrupted one
            for (size_t j = i + 1; j < num_segments; j++) {
                char dead_path[512];
                snprintf(dead_path, sizeof(dead_path), "%s/WAL-%06llu.log",
                         data_dir, segments[j]);

                if (unlink(dead_path) == 0) {
                    LOG_INFO_SIMPLE(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_RECOVERY,
                                   0, segments[j]);
                } else if (errno != ENOENT) {
                    LOG_ERROR(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_CORRUPTION,
                             0, segments[j], NULL, 0);
                    free(segments);
                    return LYGUS_ERR_IO;
                }
            }

            break;  // Stop recovery - exit the for loop
        }
    }  // ← END OF FOR LOOP

    // Now we're outside the loop - cleanup and return
    free(segments);

    uint64_t end_ns = lygus_now_ns();

    // Log recovery stats WITH TIMING
    struct {
        uint64_t highest_index;
        uint64_t num_entries;
        uint64_t num_blocks;
        uint32_t recovery_time_ms;
    } log_data = {
        .highest_index = result->highest_index,
        .num_entries = result->num_entries,
        .num_blocks = result->num_blocks,
        .recovery_time_ms = lygus_ns_to_ms(end_ns - start_ns),
    };
    LOG_INFO(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_RECOVERY,
             result->highest_term, result->highest_index,
             &log_data, sizeof(log_data));

    return LYGUS_OK;
}

