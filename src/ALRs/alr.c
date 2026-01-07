/**
 * alr.c - Almost Local Reads
 *
 * Memory model:
 *   - Ring buffer for metadata (fixed slots)
 *   - Linear slab for keys (bump allocator, reset on drain)
 *
 */

#include "alr.h"
#include "raft.h"
#include "../state/kv_store.h"
#include "../util/logging.h"

#include <stdlib.h>
#include <string.h>

// ============================================================================
// Defaults
// ============================================================================

#define ALR_DEFAULT_CAPACITY    4096
#define ALR_DEFAULT_SLAB_SIZE   (16 * 1024 * 1024)  // 16MB
#define ALR_MAX_VALUE_SIZE      (64 * 1024)         // 64KB stack buffer

// ============================================================================
// Internal Types
// ============================================================================

typedef struct {
    void    *conn;
    void    *key;           // pointer into slab
    size_t   klen;
    uint64_t sync_index;
    uint64_t sync_term;
} pending_read_t;

struct alr {
    // Ring buffer
    pending_read_t *reads;
    uint16_t head;
    uint16_t count;
    uint16_t capacity;

    // Slab allocator
    uint8_t *slab;
    size_t   slab_size;
    size_t   slab_cursor;
    size_t   slab_high_water;

    // Sync state
    uint64_t last_issued_sync;
    uint64_t last_applied;

    // Dependencies
    raft_t         *raft;
    lygus_kv_t     *kv;
    alr_respond_fn  respond;
    void           *respond_ctx;

    // Stats
    alr_stats_t stats;
};

// ============================================================================
// Ring Helpers
// ============================================================================

static inline uint16_t ring_idx(alr_t *alr, uint16_t offset) {
    return (alr->head + offset) % alr->capacity;
}

static inline pending_read_t *ring_head(alr_t *alr) {
    return &alr->reads[alr->head];
}

static inline pending_read_t *ring_tail(alr_t *alr) {
    return &alr->reads[ring_idx(alr, alr->count)];
}

// ============================================================================
// Lifecycle
// ============================================================================

alr_t *alr_create(const alr_config_t *cfg) {
    if (!cfg || !cfg->raft || !cfg->kv || !cfg->respond) {
        return NULL;
    }

    alr_t *alr = calloc(1, sizeof(alr_t));
    if (!alr) {
        return NULL;
    }

    alr->capacity = cfg->capacity > 0 ? cfg->capacity : ALR_DEFAULT_CAPACITY;
    alr->slab_size = cfg->slab_size > 0 ? cfg->slab_size : ALR_DEFAULT_SLAB_SIZE;

    alr->reads = calloc(alr->capacity, sizeof(pending_read_t));
    if (!alr->reads) {
        free(alr);
        return NULL;
    }

    alr->slab = malloc(alr->slab_size);
    if (!alr->slab) {
        free(alr->reads);
        free(alr);
        return NULL;
    }

    alr->raft = cfg->raft;
    alr->kv = cfg->kv;
    alr->respond = cfg->respond;
    alr->respond_ctx = cfg->respond_ctx;

    return alr;
}

void alr_destroy(alr_t *alr) {
    if (!alr) return;
    free(alr->slab);
    free(alr->reads);
    free(alr);
}

// ============================================================================
// Core Operations
// ============================================================================

lygus_err_t alr_read(alr_t *alr, const void *key, size_t klen, void *conn) {
    if (!alr || !key || klen == 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Ring full?
    if (alr->count >= alr->capacity) {
        return LYGUS_ERR_BATCH_FULL;
    }

    // Slab full?
    if (alr->slab_cursor + klen > alr->slab_size) {
        if (alr->count == 0) {
            alr->slab_cursor = 0;  // safe to reset
        } else {
            return LYGUS_ERR_BATCH_FULL;  // can't wrap with pending reads
        }
    }

    // Acquire sync point
    if (alr->last_issued_sync == 0 ||
        alr->last_issued_sync <= alr->last_applied) {

        uint64_t pending = raft_get_pending_index(alr->raft);

        if (pending > 0) {
            // Leader or follower can piggyback on uncommitted current-term entry
            alr->last_issued_sync = pending;
            alr->stats.piggybacks++;
        } else if (raft_is_leader(alr->raft)) {
            // Leader can propose NOOP when nothing to piggyback
            uint64_t sync_index;
            if (raft_propose_noop(alr->raft, &sync_index) != 0) {
                return LYGUS_ERR_SYNC_FAILED;
            }
            alr->last_issued_sync = sync_index;
            alr->stats.syncs_issued++;
        } else {
            return LYGUS_ERR_TRY_LEADER;
        }
        }

    // Copy key into slab
    void *key_ptr = alr->slab + alr->slab_cursor;
    memcpy(key_ptr, key, klen);
    alr->slab_cursor += klen;

    // Track high water aka memory
    if (alr->slab_cursor > alr->slab_high_water) {
        alr->slab_high_water = alr->slab_cursor;
    }

    // Queue metadata
    pending_read_t *r = ring_tail(alr);
    r->conn = conn;
    r->key = key_ptr;
    r->klen = klen;
    r->sync_index = alr->last_issued_sync;
    r->sync_term = raft_get_term(alr->raft);

    alr->count++;
    alr->stats.reads_total++;

    return LYGUS_OK;
}

void alr_notify(alr_t *alr, uint64_t applied_index) {
    if (!alr) return;

    if (applied_index > alr->last_applied) {
        alr->last_applied = applied_index;
    }

    if (alr->count == 0) return;

    uint8_t val_buf[ALR_MAX_VALUE_SIZE];
    uint64_t current_term = raft_get_term(alr->raft);

    while (alr->count > 0) {
        pending_read_t *r = ring_head(alr);

        // Skip cancelled
        if (r->conn == NULL) {
            alr->head = ring_idx(alr, 1);
            alr->count--;
            continue;
        }

        // Not ready yet
        if (r->sync_index > alr->last_applied) {
            break;
        }

        // edgecase check, to prevent serving a read that was killed by leader  ( Note to self, may have included wrong error)
        if (r->sync_term != current_term) {
            alr->respond(r->conn, r->key, r->klen,
                         NULL, 0, LYGUS_ERR_STALE_READ, alr->respond_ctx);
            alr->head = ring_idx(alr, 1);
            alr->count--;
            alr->stats.reads_stale++;
            continue;
        }

        // Read
        ssize_t vlen = lygus_kv_get(alr->kv, r->key, r->klen,
                                     val_buf, sizeof(val_buf));

        if (vlen >= 0) {
            alr->respond(r->conn, r->key, r->klen,
                         val_buf, (size_t)vlen, LYGUS_OK, alr->respond_ctx);
        } else {
            alr->respond(r->conn, r->key, r->klen,
                         NULL, 0, (lygus_err_t)vlen, alr->respond_ctx);
        }

        alr->head = ring_idx(alr, 1);
        alr->count--;
        alr->stats.reads_completed++;
    }

    // Reset slab when fully drained
    if (alr->count == 0) {
        alr->slab_cursor = 0;
    }
}

int alr_cancel_conn(alr_t *alr, void *conn) {
    if (!alr || !conn) return 0;

    int cancelled = 0;

    for (uint16_t i = 0; i < alr->count; i++) {
        pending_read_t *r = &alr->reads[ring_idx(alr, i)];

        if (r->conn == conn) {
            alr->respond(conn, r->key, r->klen,
                         NULL, 0, LYGUS_ERR_NET, alr->respond_ctx);
            r->conn = NULL;
            cancelled++;
        }
    }

    return cancelled;
}

// ============================================================================
// Stats
// ============================================================================

void alr_get_stats(const alr_t *alr, alr_stats_t *out) {
    if (!alr || !out) return;

    *out = alr->stats;
    out->pending_count = alr->count;
    out->slab_used = alr->slab_cursor;
    out->slab_high_water = alr->slab_high_water;
}