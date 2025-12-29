/**
* alr.h - Almost-Local Reads (Lazy-ALR for Raft)
 * Lazy-ALR for State Machine Replication:
 * Based on: "The LAW Theorem: Local Reads and Linearizable
 * Asynchronous Replication" (Katsarakis et al., VLDB 2025)
 */
#ifndef LYGUS_ALR_H
#define LYGUS_ALR_H

#include <stdint.h>
#include <stddef.h>
#include "../public/lygus_errors.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Types
// ============================================================================

typedef struct alr alr_t;
typedef struct raft raft_t;
typedef struct lygus_kv lygus_kv_t;

/**
 * Response callback - called when read completes or fails
 */
typedef void (*alr_respond_fn)(void *conn,
                               const void *key, size_t klen,
                               const void *val, size_t vlen,
                               lygus_err_t err,
                               void *ctx);

/**
 * Configuration
 */
typedef struct {
    raft_t         *raft;
    lygus_kv_t     *kv;
    alr_respond_fn  respond;
    void           *respond_ctx;
    uint16_t        capacity;       // 0 = default (4096)
    size_t          slab_size;      // 0 = default (16MB)
} alr_config_t;

// ============================================================================
// Lifecycle
// ============================================================================

alr_t *alr_create(const alr_config_t *cfg);
void   alr_destroy(alr_t *alr);

// ============================================================================
// Operations
// ============================================================================

/**
 * Queue a linearizable read
 *
 * @return LYGUS_OK             queued, will respond via callback
 *         LYGUS_ERR_BATCH_FULL ring or slab full, backpressure
 *         LYGUS_ERR_SYNC_FAILED couldn't propose NOOP (not leader?)
 */
lygus_err_t alr_read(alr_t *alr, const void *key, size_t klen, void *conn);

/**
 * Notify that entries have been applied
 *
 * Call from apply_entry callback. Drains reads that are now safe.
 */
void alr_notify(alr_t *alr, uint64_t applied_index);

/**
 * Cancel pending reads for a connection
 *
 * Call when connection closes. Returns number cancelled.
 */
int alr_cancel_conn(alr_t *alr, void *conn);

// ============================================================================
// Stats
// ============================================================================

typedef struct {
    uint64_t reads_total;       // total reads queued
    uint64_t reads_completed;   // successfully executed
    uint64_t reads_stale;       // killed by term change
    uint64_t syncs_issued;      // NOOPs we paid for
    uint64_t piggybacks;        // free rides on writes
    uint16_t pending_count;     // current queue depth
    size_t   slab_used;         // current slab usage
    size_t   slab_high_water;   // peak slab usage
} alr_stats_t;

void alr_get_stats(const alr_t *alr, alr_stats_t *out);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_ALR_H