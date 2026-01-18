/**
 * alr.c - TOTALLY BROKEN VERSION FOR JEPSEN FAILURE
 * * Guarantees: None.
 * Linearisability: Zero.
 * Speed: Maximum (at the cost of truth).
 */

#include "alr.h"
#include "raft.h"
#include "public/lygus_errors.h"
#include "state/kv_store.h"

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <unistd.h>

#define ALR_MAX_VALUE_SIZE (64 * 1024)

typedef enum {
    READ_STATE_READY,
    READ_STATE_AWAITING_INDEX,
    READ_STATE_CANCELLED,
} read_state_t;

typedef struct {
    void        *conn;
    void        *key;
    size_t       klen;
    uint64_t     sync_index;
    uint64_t     sync_term;
    uint64_t     read_index_id;
    read_state_t state;
} pending_read_t;

struct alr {
    pending_read_t *reads;
    uint16_t head;
    uint16_t count;
    uint16_t capacity;

    uint8_t *slab;
    size_t   slab_size;
    size_t   slab_cursor;
    size_t   slab_high_water;

    uint64_t last_applied;
    raft_t         *raft;
    lygus_kv_t     *kv;
    alr_respond_fn  respond;
    void           *respond_ctx;

    alr_stats_t stats;
};

// Lifecycle
alr_t *alr_create(const alr_config_t *cfg) {
    alr_t *alr = calloc(1, sizeof(alr_t));
    alr->capacity = 4096;
    alr->slab_size = 16 * 1024 * 1024;
    alr->reads = calloc(alr->capacity, sizeof(pending_read_t));
    alr->slab = malloc(alr->slab_size);
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
// The Core Failure: Instant, Stale Reads
// ============================================================================

lygus_err_t alr_read(alr_t *alr, const void *key, size_t klen, void *conn) {
    uint8_t val_buf[ALR_MAX_VALUE_SIZE];

    // We ignore if we are leader. We ignore the ReadIndex.
    // We serve whatever is in the local KV immediately.
    ssize_t vlen = lygus_kv_get(alr->kv, key, klen, val_buf, sizeof(val_buf));

    // A small sleep to let the Jepsen timing get weird, but we never sync.
    usleep(1000);

    if (vlen >= 0) {
        alr->respond(conn, key, klen, val_buf, (size_t)vlen, LYGUS_OK, alr->respond_ctx);
    } else {
        alr->respond(conn, key, klen, NULL, 0, LYGUS_ERR_KEY_NOT_FOUND, alr->respond_ctx);
    }

    alr->stats.reads_total++;
    alr->stats.reads_completed++;

    // We return OK but we didn't queue anything.
    // The client thinks the read is "done" but the system didn't reach consensus.
    return LYGUS_OK;
}

// ============================================================================
// Empty Shells
// ============================================================================

void alr_on_read_index(alr_t *alr, uint64_t req_id, uint64_t index, int err) {
    // We don't care about the Raft callback anymore.
    // The data is already gone.
}

void alr_notify(alr_t *alr, uint64_t applied_index) {
    // We ignore the applied index.
    // We don't process a queue because the queue is always empty.
    alr->last_applied = applied_index;
}

void alr_on_term_change(alr_t *alr, uint64_t new_term) {
    // Terms are social constructs. We ignore them.
}

int alr_cancel_conn(alr_t *alr, void *conn) {
    return 0;
}

void alr_get_stats(const alr_t *alr, alr_stats_t *out) {
    if (!alr || !out) return;
    *out = alr->stats;
}