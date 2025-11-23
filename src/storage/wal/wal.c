#include "wal.h"
#include "writer.h"
#include "recovery.h"
#include "../../util/logging.h"
#include "../compression/zstd_engine.h"

#include <stdlib.h>
#include <string.h>

// ============================================================================
// Internal State
// ============================================================================

struct wal {
    // Writer
    wal_writer_t        *writer;
    lygus_zstd_ctx_t    *zctx;
    
    // Recovery result (from open)
    wal_recovery_result_t recovery;
    
    // Options
    char                 data_dir[256];
    int                  zstd_level;
    
    // Runtime stats
    uint64_t             appends_total;
    uint64_t             flushes_total;
    uint64_t             fsyncs_total;
    uint64_t             bytes_written;
};

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

    // Run recovery
    LOG_INFO_SIMPLE(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_RECOVERY, 0, 0);

    int ret = wal_recover(opts->data_dir, w->zctx,
                          opts->on_recover, opts->user_data,
                          &w->recovery);
    if (ret < 0) {
        LOG_ERROR(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_RECOVERY,
                  0, 0, NULL, 0);
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
        lygus_zstd_destroy(w->zctx);
        free(w);
        return NULL;
    }

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

    // Destroy compression context
    if (w->zctx) {
        lygus_zstd_destroy(w->zctx);
    }

    free(w);
    
    return ret;
}

// ============================================================================
// Write Operations
// ============================================================================

int wal_put(wal_t *w, uint64_t index, uint64_t term,
            const void *key, size_t klen,
            const void *val, size_t vlen)
{
    if (!w || !w->writer) {
        return LYGUS_ERR_INVALID_ARG;
    }
    
    // Check size limits before attempting to write
    if (klen > WAL_ENTRY_MAX_KEY_SIZE) {
        return LYGUS_ERR_KEY_TOO_LARGE;
    }
    if (vlen > WAL_ENTRY_MAX_VALUE_SIZE) {
        return LYGUS_ERR_VAL_TOO_LARGE;
    }

    int ret = wal_writer_append(w->writer, WAL_ENTRY_PUT,
                                 index, term,
                                 key, klen, val, vlen);
    if (ret == LYGUS_OK) {
        w->appends_total++;
    }

    return ret;
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

    int ret = wal_writer_append(w->writer, WAL_ENTRY_DEL,
                                 index, term,
                                 key, klen, NULL, 0);
    if (ret == LYGUS_OK) {
        w->appends_total++;
    }
    
    return ret;
}

int wal_noop_sync(wal_t *w, uint64_t index, uint64_t term) {
    if (!w || !w->writer) {
        return LYGUS_ERR_INVALID_ARG;
    }
    
    int ret = wal_writer_append(w->writer, WAL_ENTRY_NOOP_SYNC,
                                 index, term,
                                 NULL, 0, NULL, 0);
    if (ret == LYGUS_OK) {
        w->appends_total++;
    }
    
    return ret;
}

int wal_snap_mark(wal_t *w, uint64_t index, uint64_t term) {
    if (!w || !w->writer) {
        return LYGUS_ERR_INVALID_ARG;
    }
    
    int ret = wal_writer_append(w->writer, WAL_ENTRY_SNAP_MARK,
                                 index, term,
                                 NULL, 0, NULL, 0);
    if (ret == LYGUS_OK) {
        w->appends_total++;
    }
    
    return ret;
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
    
    return wal_writer_rotate(w->writer);
}

int wal_should_rotate(const wal_t *w) {
    if (!w || !w->writer) {
        return 0;
    }
    
    // Check if current segment offset is approaching max size
    uint64_t offset = wal_writer_offset(w->writer);
    return (offset >= WAL_SEGMENT_MAX_SIZE * 0.9);  // 90% threshold
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