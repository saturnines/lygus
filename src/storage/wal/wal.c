#include "wal.h"
#include "wal_index.h"
#include "writer.h"
#include "recovery.h"
#include "../../util/logging.h"
#include "../compression/zstd_engine.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>

// ============================================================================
// Internal State
// ============================================================================

struct wal {
    // Writer
    wal_writer_t        *writer;
    lygus_zstd_ctx_t    *zctx;

    // In memory index (maps raft index -> file location)
    wal_index_t         *index;

    // Recovery result (from open)
    wal_recovery_result_t recovery;

    // Options
    char                 data_dir[256];
    int                  zstd_level;

    // Current write position tracking (for index)
    uint64_t             current_segment;
    uint64_t             current_block_offset;
    uint64_t             current_entry_offset;

    // Runtime stats
    uint64_t             appends_total;
    uint64_t             flushes_total;
    uint64_t             fsyncs_total;
    uint64_t             bytes_written;
};

// ============================================================================
// Recovery Callback Context
// ============================================================================

typedef struct {
    wal_t               *wal;
    wal_entry_callback_t user_callback;
    void                *user_data;
} recovery_ctx_t;


// Internal recovery callback - builds index and forwards to user callback
static int recovery_index_builder(const wal_entry_t *entry, void *ctx) {
    recovery_ctx_t *rctx = (recovery_ctx_t *)ctx;

    // Add to index using location info from the entry
    int ret = wal_index_append(rctx->wal->index,
                               entry->index,
                               entry->segment_num,
                               entry->block_offset,
                               entry->entry_offset);
    if (ret < 0) {
        // Log but don't fail - index can be rebuilt
        LOG_ERROR(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_RECOVERY,
                  entry->term, entry->index, NULL, 0);
    }

    // Forward to user callback if provided
    if (rctx->user_callback) {
        ret = rctx->user_callback(entry, rctx->user_data);
        if (ret < 0) {
            return ret;
        }
    }

    return LYGUS_OK;
}

// ============================================================================
// Helpers
// ============================================================================

static void get_segment_path(const char *data_dir, uint64_t seg_num,
                             char *out, size_t out_len) {
    snprintf(out, out_len, "%s/WAL-%06llu.log", data_dir,
             (unsigned long long)seg_num);
}

// ============================================================================
// Lifecycle
// ============================================================================

wal_t* wal_open(const wal_opts_t *opts) {
    if (!opts || !opts->data_dir) {
        return NULL;
    }

    // Allocate handle
    wal_t *w = calloc(1, sizeof(wal_t));
    if (!w) {
        return NULL;
    }

    // Copy config
    strncpy(w->data_dir, opts->data_dir, sizeof(w->data_dir) - 1);
    w->zstd_level = (opts->zstd_level > 0) ? opts->zstd_level : 3;

    // Create compression context
    w->zctx = lygus_zstd_create(w->zstd_level);
    if (!w->zctx) {
        free(w);
        return NULL;
    }

    // Create in memory index
    w->index = wal_index_create();
    if (!w->index) {
        lygus_zstd_destroy(w->zctx);
        free(w);
        return NULL;
    }

    // Set up recovery context with index building callback
    LOG_INFO_SIMPLE(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_RECOVERY, 0, 0);

    recovery_ctx_t rctx = {
        .wal = w,
        .user_callback = opts->on_recover,
        .user_data = opts->user_data,
    };

    // Use recovery_index_builder as callback
    // entries to the user's callback if provided
    int ret = wal_recover(opts->data_dir, w->zctx,
                          recovery_index_builder, &rctx,
                          &w->recovery);
    if (ret < 0) {
        LOG_ERROR(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_RECOVERY,
                  0, 0, NULL, 0);
        wal_index_destroy(w->index);
        lygus_zstd_destroy(w->zctx);
        free(w);
        return NULL;
    }

    // Open writer
    wal_writer_opts_t writer_opts = {
        .data_dir = opts->data_dir,
        .zstd_level = w->zstd_level,
        .block_size = 0,
        .fsync_interval_us = opts->fsync_interval_us,
        .fsync_bytes = opts->fsync_bytes,
    };

    w->writer = wal_writer_open(&writer_opts);
    if (!w->writer) {
        wal_index_destroy(w->index);
        lygus_zstd_destroy(w->zctx);
        free(w);
        return NULL;
    }

    // Track current write position
    w->current_segment = wal_writer_segment_num(w->writer);
    w->current_block_offset = wal_writer_offset(w->writer);
    w->current_entry_offset = wal_writer_block_fill(w->writer);

    return w;
}

int wal_close(wal_t *w) {
    if (!w) {
        return LYGUS_ERR_INVALID_ARG;
    }

    LOG_INFO_SIMPLE(LYGUS_MODULE_WAL, LYGUS_EVENT_SHUTDOWN, 0, 0);

    // Close writer (flushes and fsyncs)
    int ret = LYGUS_OK;
    if (w->writer) {
        ret = wal_writer_close(w->writer);
    }

    // Destroy index
    if (w->index) {
        wal_index_destroy(w->index);
    }

    // Destroy compression context
    if (w->zctx) {
        lygus_zstd_destroy(w->zctx);
    }

    free(w);

    return ret;
}

// ============================================================================
// Write Operations (with index tracking)
// ============================================================================

static int wal_append_internal(wal_t *w, wal_entry_type_t type,
                               uint64_t index, uint64_t term,
                               const void *key, size_t klen,
                               const void *val, size_t vlen)
{
    // Track position before append
    uint64_t seg = wal_writer_segment_num(w->writer);
    uint64_t block_off = wal_writer_offset(w->writer);
    uint64_t entry_off = wal_writer_block_fill(w->writer);

    // Append to writer
    int ret = wal_writer_append(w->writer, type, index, term,
                                key, klen, val, vlen);
    if (ret < 0) {
        return ret;
    }

    // Add to index
    ret = wal_index_append(w->index, index, seg, block_off, entry_off);
    if (ret < 0) {
        // Index append failed, this is really bad but entry is written
        // Log error but don't fail the operation
        LOG_ERROR(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_CORRUPTION,
                  term, index, NULL, 0);
    }

    w->appends_total++;
    return LYGUS_OK;
}

int wal_put(wal_t *w, uint64_t index, uint64_t term,
            const void *key, size_t klen,
            const void *val, size_t vlen)
{
    if (!w || !w->writer) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (klen > WAL_ENTRY_MAX_KEY_SIZE) {
        return LYGUS_ERR_KEY_TOO_LARGE;
    }
    if (vlen > WAL_ENTRY_MAX_VALUE_SIZE) {
        return LYGUS_ERR_VAL_TOO_LARGE;
    }

    return wal_append_internal(w, WAL_ENTRY_PUT, index, term,
                               key, klen, val, vlen);
}

int wal_del(wal_t *w, uint64_t index, uint64_t term,
            const void *key, size_t klen)
{
    if (!w || !w->writer) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (klen > WAL_ENTRY_MAX_KEY_SIZE) {
        return LYGUS_ERR_KEY_TOO_LARGE;
    }

    return wal_append_internal(w, WAL_ENTRY_DEL, index, term,
                               key, klen, NULL, 0);
}

int wal_noop_sync(wal_t *w, uint64_t index, uint64_t term) {
    if (!w || !w->writer) {
        return LYGUS_ERR_INVALID_ARG;
    }

    return wal_append_internal(w, WAL_ENTRY_NOOP_SYNC, index, term,
                               NULL, 0, NULL, 0);
}

int wal_snap_mark(wal_t *w, uint64_t index, uint64_t term) {
    if (!w || !w->writer) {
        return LYGUS_ERR_INVALID_ARG;
    }

    return wal_append_internal(w, WAL_ENTRY_SNAP_MARK, index, term,
                               NULL, 0, NULL, 0);
}

// ============================================================================
// Durability Control
// ============================================================================

int wal_flush(wal_t *w, int sync) {
    if (!w || !w->writer) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = wal_writer_flush(w->writer, sync);
    if (ret == LYGUS_OK) {
        w->flushes_total++;
        if (sync) {
            w->fsyncs_total++;
        }
    }

    return ret;
}

int wal_fsync(wal_t *w) {
    if (!w || !w->writer) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = wal_writer_fsync(w->writer);
    if (ret == LYGUS_OK) {
        w->fsyncs_total++;
    }

    return ret;
}

// ============================================================================
// Segment Management
// ============================================================================

int wal_rotate(wal_t *w) {
    if (!w || !w->writer) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = wal_writer_rotate(w->writer);
    if (ret == LYGUS_OK) {
        w->current_segment = wal_writer_segment_num(w->writer);
    }
    return ret;
}

int wal_should_rotate(const wal_t *w) {
    if (!w || !w->writer) {
        return 0;
    }

    uint64_t offset = wal_writer_offset(w->writer);
    return (offset >= WAL_SEGMENT_MAX_SIZE * 0.9);
}

// ============================================================================
// Log Truncation
// ============================================================================

int wal_truncate_after(wal_t *w, uint64_t index) {
    if (!w) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Check if index exists
    if (wal_index_is_empty(w->index)) {
        return LYGUS_OK;  // Nothing to truncate
    }

    uint64_t last = wal_index_last(w->index);
    if (index >= last) {
        return LYGUS_OK;  // Nothing to truncate
    }

    uint64_t first = wal_index_first(w->index);
    if (index < first) {
        // Truncating before our first entry means clear everything
        return wal_clear(w);
    }

    // Look up the location of the entry AFTER index
    // We need to truncate the file at the start of entry (index + 1)
    wal_entry_loc_t loc;
    int ret = wal_index_lookup(w->index, index + 1, &loc);
    if (ret < 0) {
        // Entry not found, but shouldn't happen if index < last
        return ret;
    }

    // Close current writer
    wal_writer_close(w->writer);
    w->writer = NULL;

    // Truncate the segment file at the block containing (index + 1)
    char path[512];
    get_segment_path(w->data_dir, loc.segment_num, path, sizeof(path));

    int fd = open(path, O_RDWR);
    if (fd < 0) {
        return LYGUS_ERR_IO;
    }

    // Truncate at block_offset (removes the block and everything after)
    // This may remove some entries from the block that are <= index, but raft will resend them
    if (ftruncate(fd, loc.block_offset) < 0) {
        close(fd);
        return LYGUS_ERR_IO;
    }

    // Fsync to ensure truncation is durable
#ifdef _WIN32
    if (_commit(fd) < 0) {
#else
    if (fdatasync(fd) < 0) {
#endif
        close(fd);
        return LYGUS_ERR_FSYNC;
    }

    close(fd);

    // Delete any segments after this one
    uint64_t *segments = NULL;
    size_t num_segments = 0;
    ret = wal_list_segments(w->data_dir, &segments, &num_segments);
    if (ret == LYGUS_OK && segments) {
        for (size_t i = 0; i < num_segments; i++) {
            if (segments[i] > loc.segment_num) {
                char dead_path[512];
                get_segment_path(w->data_dir, segments[i], dead_path, sizeof(dead_path));
                unlink(dead_path);
            }
        }
        free(segments);
    }

    // Update in memory index
    wal_index_truncate_after(w->index, index);

    // Re open writer at truncated segment
    wal_writer_opts_t writer_opts = {
        .data_dir = w->data_dir,
        .zstd_level = w->zstd_level,
        .block_size = 0,
        .fsync_interval_us = 0,
        .fsync_bytes = 0,
    };

    w->writer = wal_writer_open(&writer_opts);
    if (!w->writer) {
        return LYGUS_ERR_IO;
    }

    w->current_segment = wal_writer_segment_num(w->writer);

    LOG_WARN(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_CORRUPTION,
             0, index, NULL, 0);

    return LYGUS_OK;
}

int wal_purge_before(wal_t *w, uint64_t index) {
    if (!w) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (wal_index_is_empty(w->index)) {
        return LYGUS_OK;  // Nothing to purge
    }

    // Find the minimum segment that contains entries >= index
    uint64_t keep_from_segment = wal_index_min_segment_from(w->index, index);
    if (keep_from_segment == 0) {
        return LYGUS_OK;  // All entries are before index, but we can't delete current segment
    }

    // List all segments
    uint64_t *segments = NULL;
    size_t num_segments = 0;
    int ret = wal_list_segments(w->data_dir, &segments, &num_segments);
    if (ret < 0) {
        return ret;
    }

    if (!segments || num_segments == 0) {
        return LYGUS_OK;
    }

    // Delete segments that are entirely before keep_from_segment
    uint64_t current_seg = wal_writer_segment_num(w->writer);

    for (size_t i = 0; i < num_segments; i++) {
        uint64_t seg = segments[i];

        // Don't delete the current segment or segments >= keep_from_segment
        if (seg >= keep_from_segment || seg == current_seg) {
            continue;
        }

        char path[512];
        get_segment_path(w->data_dir, seg, path, sizeof(path));

        if (unlink(path) < 0 && errno != ENOENT) {
            LOG_ERROR(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_CORRUPTION,
                     0, seg, NULL, 0);
            // Continue trying to delete other segments
        }
    }

    free(segments);

    // Update in memory index
    wal_index_truncate_before(w->index, index);

    return LYGUS_OK;
}

int wal_clear(wal_t *w) {
    if (!w) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Close writer
    if (w->writer) {
        wal_writer_close(w->writer);
        w->writer = NULL;
    }

    // List and delete all segments
    uint64_t *segments = NULL;
    size_t num_segments = 0;
    int ret = wal_list_segments(w->data_dir, &segments, &num_segments);

    if (ret == LYGUS_OK && segments) {
        for (size_t i = 0; i < num_segments; i++) {
            char path[512];
            get_segment_path(w->data_dir, segments[i], path, sizeof(path));
            unlink(path);
        }
        free(segments);
    }

    // Clear index
    wal_index_clear(w->index);

    // Re-open writer (creates fresh segment)
    wal_writer_opts_t writer_opts = {
        .data_dir = w->data_dir,
        .zstd_level = w->zstd_level,
        .block_size = 0,
        .fsync_interval_us = 0,
        .fsync_bytes = 0,
    };

    w->writer = wal_writer_open(&writer_opts);
    if (!w->writer) {
        return LYGUS_ERR_IO;
    }

    w->current_segment = wal_writer_segment_num(w->writer);

    // Reset recovery stats
    memset(&w->recovery, 0, sizeof(w->recovery));

    LOG_INFO_SIMPLE(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_RECOVERY, 0, 0);

    return LYGUS_OK;
}

// ============================================================================
// Index Queries
// ============================================================================

uint64_t wal_first_index(const wal_t *w) {
    if (!w || !w->index) return 0;
    return wal_index_first(w->index);
}

uint64_t wal_last_index(const wal_t *w) {
    if (!w || !w->index) return 0;
    return wal_index_last(w->index);
}

int wal_contains_index(const wal_t *w, uint64_t index) {
    if (!w || !w->index) return 0;

    uint64_t first = wal_index_first(w->index);
    uint64_t last = wal_index_last(w->index);

    if (first == 0 && last == 0) return 0;  // Empty

    return (index >= first && index <= last);
}

// ============================================================================
// Observability
// ============================================================================

int wal_get_stats(const wal_t *w, wal_stats_t *stats) {
    if (!w || !stats) {
        return LYGUS_ERR_INVALID_ARG;
    }

    memset(stats, 0, sizeof(*stats));

    // Recovery stats
    stats->recovered_entries = w->recovery.num_entries;
    stats->recovered_blocks = w->recovery.num_blocks;
    stats->highest_index = w->recovery.highest_index;
    stats->highest_term = w->recovery.highest_term;
    stats->had_corruption = (w->recovery.corruptions > 0 || w->recovery.truncated);

    // Current writer state
    if (w->writer) {
        stats->segment_num = wal_writer_segment_num(w->writer);
        stats->write_offset = wal_writer_offset(w->writer);
        stats->block_seq = wal_writer_block_seq(w->writer);
        stats->block_fill = wal_writer_block_fill(w->writer);
    }

    // Index state
    if (w->index) {
        stats->first_index = wal_index_first(w->index);
        stats->last_index = wal_index_last(w->index);
    }

    // Runtime counters
    stats->appends_total = w->appends_total;
    stats->flushes_total = w->flushes_total;
    stats->fsyncs_total = w->fsyncs_total;
    stats->bytes_written = w->bytes_written;

    return LYGUS_OK;
}

int wal_get_recovery_result(const wal_t *w, wal_recovery_result_t *result) {
    if (!w || !result) {
        return LYGUS_ERR_INVALID_ARG;
    }

    memcpy(result, &w->recovery, sizeof(*result));
    return LYGUS_OK;
}