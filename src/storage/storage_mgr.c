/**
 * storage_mgr.c - Storage Manager Implementation
 *
 * Cross-platform implementation:
 *   - POSIX: Async fork-based snapshots with COW
 *   - Windows: Synchronous snapshots (no fork)
 */

#include "storage_mgr.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#ifdef _WIN32
    #include <windows.h>
    #include <direct.h>
    #define mkdir_p(path) _mkdir(path)
    #define PATH_SEP "\\"
#else
    #include <unistd.h>
    #include <sys/stat.h>
    #include <sys/wait.h>
    #define mkdir_p(path) mkdir(path, 0755)
    #define PATH_SEP "/"
#endif

// Adjust these includes to match your project structure
#include "../storage/wal/wal.h"
#include "../storage/wal/block_format.h"  // for WAL_ENTRY_* types
#include "../storage/snapshot/snapshot.h"
#include "../state/kv_store.h"
#include "../public/lygus_errors.h"
#include "../util/logging.h"

// WAL doesn't expose current size directly - we track it
// WAL doesn't expose last term directly - we track it

// ============================================================================
// Internal Structure
// ============================================================================

struct storage_mgr {
    // Components
    lygus_kv_t *kv;
    wal_t      *wal;

    // Directories
    char *data_dir;
    char *wal_dir;
    char *snapshot_dir;

    // Configuration
    size_t snapshot_threshold;
    int    snapshots_to_keep;

    // State tracking
    uint64_t applied_index;
    uint64_t applied_term;
    uint64_t logged_index;
    uint64_t logged_term;

    // WAL size tracking
    size_t wal_bytes_written;

    // Async snapshot state (POSIX only)
#ifndef _WIN32
    int      snapshot_in_progress;
    pid_t    snapshot_pid;
    uint64_t snapshot_index;
    uint64_t snapshot_term;
    char     snapshot_path[512];
#endif
};

// ============================================================================
// Recovery Context (for WAL replay callback)
// ============================================================================

typedef struct {
    storage_mgr_t *mgr;
    uint64_t       snapshot_index;  // Skip entries <= this
    int            error;
} recovery_ctx_t;

/**
 * WAL recovery callback - called for each entry during replay
 *
 * Matches wal_entry_callback_t signature from recovery.h:
 *   int (*)(const wal_entry_t *entry, void *user_data)
 */
static int recovery_callback(const wal_entry_t *entry, void *user_data)
{
    recovery_ctx_t *ctx = (recovery_ctx_t *)user_data;
    storage_mgr_t *mgr = ctx->mgr;

    uint64_t index = entry->index;
    uint64_t term = entry->term;

    // Skip entries already in snapshot
    if (index <= ctx->snapshot_index) {
        return LYGUS_OK;
    }

    // Enforce ordering
    if (index != mgr->applied_index + 1) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD,
                  index, mgr->applied_index, NULL, 0);
        ctx->error = LYGUS_ERR_CORRUPT;
        return LYGUS_ERR_CORRUPT;
    }

    // Apply to KV store based on entry type
    int ret = LYGUS_OK;

    switch (entry->type) {
        case WAL_ENTRY_PUT:
            ret = lygus_kv_put(mgr->kv, entry->key, entry->klen,
                               entry->val, entry->vlen);
            break;

        case WAL_ENTRY_DEL:
            ret = lygus_kv_del(mgr->kv, entry->key, entry->klen);
            // Key not found is OK during recovery should be idempotent
            if (ret == LYGUS_ERR_KEY_NOT_FOUND) {
                ret = LYGUS_OK;
            }
            break;

        case WAL_ENTRY_NOOP_SYNC:
        case WAL_ENTRY_SNAP_MARK:
            // No KV change
            break;

        default:
            LOG_WARN(LYGUS_MODULE_STORAGE, LYGUS_EVENT_WAL_RECOVERY,
                     index, term, NULL, 0);
            break;
    }

    if (ret != LYGUS_OK) {
        ctx->error = ret;
        return ret;
    }

    // Update applied state
    mgr->applied_index = index;
    mgr->applied_term = term;

    return LYGUS_OK;
}

// ============================================================================
// Configuration
// ============================================================================

void storage_mgr_config_init(storage_mgr_config_t *cfg)
{
    if (!cfg) return;

    memset(cfg, 0, sizeof(*cfg));
    cfg->snapshot_threshold = STORAGE_MGR_DEFAULT_SNAPSHOT_THRESHOLD;
    cfg->snapshots_to_keep = STORAGE_MGR_DEFAULT_SNAPSHOTS_TO_KEEP;
    cfg->wal_segment_size = STORAGE_MGR_DEFAULT_WAL_SEGMENT_SIZE;
    cfg->wal_sync_on_append = 1;
}

// ============================================================================
// Helper Functions
// ============================================================================

static char *build_path(const char *base, const char *suffix)
{
    size_t len = strlen(base) + strlen(PATH_SEP) + strlen(suffix) + 1;
    char *path = malloc(len);
    if (path) {
        snprintf(path, len, "%s%s%s", base, PATH_SEP, suffix);
    }
    return path;
}

static int ensure_directory(const char *path)
{
    // Try to create, ignore if exists
    int ret = mkdir_p(path);
    if (ret != 0 && errno != EEXIST) {
        return LYGUS_ERR_IO;
    }
    return LYGUS_OK;
}

// ============================================================================
// Lifecycle
// ============================================================================

int storage_mgr_open(const storage_mgr_config_t *cfg, storage_mgr_t **out)
{
    if (!cfg || !cfg->data_dir || !out) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret;
    storage_mgr_t *mgr = calloc(1, sizeof(storage_mgr_t));
    if (!mgr) {
        return LYGUS_ERR_NOMEM;
    }

    // Copy configuration
    mgr->snapshot_threshold = cfg->snapshot_threshold;
    mgr->snapshots_to_keep = cfg->snapshots_to_keep;

    // Setup directories
    mgr->data_dir = strdup(cfg->data_dir);
    mgr->wal_dir = cfg->wal_dir ? strdup(cfg->wal_dir)
                                : build_path(cfg->data_dir, "wal");
    mgr->snapshot_dir = cfg->snapshot_dir ? strdup(cfg->snapshot_dir)
                                          : build_path(cfg->data_dir, "snapshots");

    if (!mgr->data_dir || !mgr->wal_dir || !mgr->snapshot_dir) {
        ret = LYGUS_ERR_NOMEM;
        goto fail;
    }

    // Ensure directories exist
    if ((ret = ensure_directory(mgr->data_dir)) != LYGUS_OK ||
        (ret = ensure_directory(mgr->wal_dir)) != LYGUS_OK ||
        (ret = ensure_directory(mgr->snapshot_dir)) != LYGUS_OK) {
        goto fail;
    }

    // Create KV store
    mgr->kv = lygus_kv_create();
    if (!mgr->kv) {
        ret = LYGUS_ERR_NOMEM;
        goto fail;
    }

    // =========================================================================
    // RECOVERY PHASE 1: Load latest snapshot
    // =========================================================================

    uint64_t snapshot_index = 0;
    uint64_t snapshot_term = 0;
    char snapshot_path[512];

    ret = snapshot_find_latest(mgr->snapshot_dir, snapshot_path,
                               &snapshot_index, &snapshot_term);

    if (ret == LYGUS_OK) {
        // Found snapshot, load it
        LOG_INFO(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD,
                 snapshot_index, snapshot_term, NULL, 0);

        ret = snapshot_load(snapshot_path, mgr->kv, &snapshot_index, &snapshot_term);
        if (ret != LYGUS_OK) {
            LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD,
                      snapshot_index, snapshot_term, NULL, 0);
            goto fail;
        }

        mgr->applied_index = snapshot_index;
        mgr->applied_term = snapshot_term;

        LOG_INFO(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD,
                 snapshot_index, snapshot_term, NULL, 0);

    } else if (ret == LYGUS_ERR_KEY_NOT_FOUND) {
        // No snapshot, starting fresh
        LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_INIT, 0, 0);
    } else {
        // Actual error
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD,
                  0, 0, NULL, 0);
        goto fail;
    }

    // =========================================================================
    // RECOVERY PHASE 2: Open WAL and replay
    // =========================================================================

    recovery_ctx_t recovery_ctx = {
        .mgr = mgr,
        .snapshot_index = snapshot_index,
        .error = LYGUS_OK
    };

    wal_opts_t wal_opts = {
        .data_dir = mgr->wal_dir,
        .zstd_level = 0,  // Use default
        .fsync_bytes = 0,  // Manual fsync
        .fsync_interval_us = 0,  // Manual fsync
        .on_recover = recovery_callback,
        .user_data = &recovery_ctx
    };

    mgr->wal = wal_open(&wal_opts);
    if (!mgr->wal) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_WAL_RECOVERY,
                  0, 0, NULL, 0);
        ret = LYGUS_ERR_IO;
        goto fail;
    }

    // Check if recovery callback reported errors
    if (recovery_ctx.error != LYGUS_OK) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_WAL_RECOVERY,
                  mgr->applied_index, mgr->applied_term, NULL, 0);
        ret = recovery_ctx.error;
        goto fail;
    }

    // Update logged state from WAL
    mgr->logged_index = wal_last_index(mgr->wal);

    // Get term from WAL stats (no direct wal_last_term function)
    wal_stats_t wal_stats;
    if (wal_get_stats(mgr->wal, &wal_stats) == LYGUS_OK) {
        mgr->logged_term = wal_stats.highest_term;
        mgr->wal_bytes_written = wal_stats.bytes_written;
    } else {
        // Fallback to applied_term
        mgr->logged_term = mgr->applied_term;
        mgr->wal_bytes_written = 0;
    }

    // If WAL is ahead of applied (shouldn't happen after full replay),
    // something is wrong
    if (mgr->logged_index < mgr->applied_index) {
        LOG_WARN(LYGUS_MODULE_STORAGE, LYGUS_EVENT_INIT,
                 mgr->applied_index, mgr->logged_index, NULL, 0);
    }

    LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_INIT,
             mgr->applied_index, mgr->applied_term);

    *out = mgr;
    return LYGUS_OK;

fail:
    storage_mgr_close(mgr);
    return ret;
}

void storage_mgr_close(storage_mgr_t *mgr)
{
    if (!mgr) return;

#ifndef _WIN32
    // Wait for async snapshot to complete
    if (mgr->snapshot_in_progress && mgr->snapshot_pid > 0) {
        int status;
        waitpid(mgr->snapshot_pid, &status, 0);
        mgr->snapshot_in_progress = 0;
    }
#endif

    if (mgr->wal) {
        wal_close(mgr->wal);
    }

    if (mgr->kv) {
        lygus_kv_destroy(mgr->kv);
    }

    free(mgr->data_dir);
    free(mgr->wal_dir);
    free(mgr->snapshot_dir);
    free(mgr);
}

// ============================================================================
// Moment 1: Log Operations
// ============================================================================

int storage_mgr_log_put(storage_mgr_t *mgr,
                        uint64_t index, uint64_t term,
                        const void *key, size_t key_len,
                        const void *val, size_t val_len)
{
    if (!mgr || !key) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = wal_put(mgr->wal, index, term, key, key_len, val, val_len);
    if (ret != LYGUS_OK) {
        return ret;
    }

    // Sync to ensure durability before ACK
    ret = wal_fsync(mgr->wal);
    if (ret != LYGUS_OK) {
        return ret;
    }

    mgr->logged_index = index;
    mgr->logged_term = term;

    // Estimate bytes written (entry overhead + key + val)
    mgr->wal_bytes_written += 32 + key_len + val_len;

    return LYGUS_OK;
}

int storage_mgr_log_del(storage_mgr_t *mgr,
                        uint64_t index, uint64_t term,
                        const void *key, size_t key_len)
{
    if (!mgr || !key) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = wal_del(mgr->wal, index, term, key, key_len);
    if (ret != LYGUS_OK) {
        return ret;
    }

    ret = wal_fsync(mgr->wal);
    if (ret != LYGUS_OK) {
        return ret;
    }

    mgr->logged_index = index;
    mgr->logged_term = term;

    // Estimate bytes written
    mgr->wal_bytes_written += 24 + key_len;

    return LYGUS_OK;
}

int storage_mgr_log_noop(storage_mgr_t *mgr,
                         uint64_t index, uint64_t term)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = wal_noop_sync(mgr->wal, index, term);
    if (ret != LYGUS_OK) {
        return ret;
    }

    ret = wal_fsync(mgr->wal);
    if (ret != LYGUS_OK) {
        return ret;
    }

    mgr->logged_index = index;
    mgr->logged_term = term;

    // Estimate bytes written (just header, no key/val)
    mgr->wal_bytes_written += 20;

    return LYGUS_OK;
}

// ============================================================================
// Moment 2: Apply Operations
// ============================================================================

int storage_mgr_apply_put(storage_mgr_t *mgr,
                          uint64_t index, uint64_t term,
                          const void *key, size_t key_len,
                          const void *val, size_t val_len)
{
    if (!mgr || !key) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Enforce ordering: must apply in sequence
    if (index != mgr->applied_index + 1) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_APPLY_STALLED,
                  index, mgr->applied_index, NULL, 0);
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = lygus_kv_put(mgr->kv, key, key_len, val, val_len);
    if (ret != LYGUS_OK) {
        return ret;
    }

    mgr->applied_index = index;
    mgr->applied_term = term;

    return LYGUS_OK;
}

int storage_mgr_apply_del(storage_mgr_t *mgr,
                          uint64_t index, uint64_t term,
                          const void *key, size_t key_len)
{
    if (!mgr || !key) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (index != mgr->applied_index + 1) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_APPLY_STALLED,
                  index, mgr->applied_index, NULL, 0);
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = lygus_kv_del(mgr->kv, key, key_len);
    // Key not found is acceptable (idempotent deletes)
    if (ret != LYGUS_OK && ret != LYGUS_ERR_KEY_NOT_FOUND) {
        return ret;
    }

    mgr->applied_index = index;
    mgr->applied_term = term;

    return LYGUS_OK;
}

int storage_mgr_apply_noop(storage_mgr_t *mgr,
                           uint64_t index, uint64_t term)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (index != mgr->applied_index + 1) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_APPLY_STALLED,
                  index, mgr->applied_index, NULL, 0);
        return LYGUS_ERR_INVALID_ARG;
    }

    // NOOP doesn't modify KV, just advances index
    mgr->applied_index = index;
    mgr->applied_term = term;

    return LYGUS_OK;
}

// ============================================================================
// Read Operations
// ============================================================================

ssize_t storage_mgr_get(storage_mgr_t *mgr,
                        const void *key, size_t key_len,
                        void *val_out, size_t val_cap)
{
    if (!mgr || !key) {
        return LYGUS_ERR_INVALID_ARG;
    }

    return lygus_kv_get(mgr->kv, key, key_len, val_out, val_cap);
}

// ============================================================================
// Snapshot Operations - Platform Specific
// ============================================================================

/**
 * Common bookkeeping after successful snapshot
 */
static int snapshot_bookkeeping(storage_mgr_t *mgr,
                                uint64_t snap_index,
                                uint64_t snap_term)
{
    int ret;

    // Write SNAP_MARK to WAL (must be durable before purge)
    ret = wal_snap_mark(mgr->wal, snap_index, snap_term);
    if (ret != LYGUS_OK) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
                  snap_index, snap_term, NULL, 0);
        return ret;
    }

    // Sync WAL
    ret = wal_fsync(mgr->wal);
    if (ret != LYGUS_OK) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
                  snap_index, snap_term, NULL, 0);
        return ret;
    }

    // Now safe to purge old WAL segments
    ret = wal_purge_before(mgr->wal, snap_index);
    if (ret != LYGUS_OK) {
        LOG_WARN(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
                 snap_index, snap_term, NULL, 0);
        // Non-fatal - continue
    }

    // Reset WAL size tracking after purge
    mgr->wal_bytes_written = 0;

    // Purge old snapshots
    ret = snapshot_purge_old(mgr->snapshot_dir, mgr->snapshots_to_keep);
    if (ret != LYGUS_OK) {
        LOG_WARN(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
                 snap_index, snap_term, NULL, 0);
        // Non-fatal - continue
    }

    LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
             snap_index, snap_term);

    return LYGUS_OK;
}

#ifdef _WIN32
// =============================================================================
// Windows Implementation: Synchronous Snapshots
// =============================================================================

int storage_mgr_maybe_snapshot(storage_mgr_t *mgr)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Check threshold using our tracked size
    if (mgr->wal_bytes_written < mgr->snapshot_threshold) {
        return LYGUS_OK;
    }

    // Nothing to snapshot
    if (mgr->applied_index == 0) {
        return LYGUS_OK;
    }

    LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_START,
             mgr->applied_index, mgr->applied_term);

    // Build snapshot path
    char snap_path[512];
    int ret = snapshot_path_from_index(mgr->snapshot_dir,
                                       mgr->applied_index,
                                       mgr->applied_term,
                                       snap_path, sizeof(snap_path));
    if (ret != LYGUS_OK) {
        return ret;
    }

    // Synchronous snapshot (blocks!)
    ret = snapshot_write(mgr->kv, mgr->applied_index, mgr->applied_term, snap_path);
    if (ret != LYGUS_OK) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_START,
                  mgr->applied_index, mgr->applied_term, NULL, 0);
        return ret;
    }

    // Bookkeeping
    return snapshot_bookkeeping(mgr, mgr->applied_index, mgr->applied_term);
}

int storage_mgr_poll_snapshot(storage_mgr_t *mgr)
{
    (void)mgr;  // Unused on Windows
    return LYGUS_OK;  // No-op - snapshots are synchronous
}

int storage_mgr_force_snapshot(storage_mgr_t *mgr)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (mgr->applied_index == 0) {
        return LYGUS_OK;  // Nothing to snapshot
    }

    char snap_path[512];
    int ret = snapshot_path_from_index(mgr->snapshot_dir,
                                       mgr->applied_index,
                                       mgr->applied_term,
                                       snap_path, sizeof(snap_path));
    if (ret != LYGUS_OK) {
        return ret;
    }

    ret = snapshot_write(mgr->kv, mgr->applied_index, mgr->applied_term, snap_path);
    if (ret != LYGUS_OK) {
        return ret;
    }

    return snapshot_bookkeeping(mgr, mgr->applied_index, mgr->applied_term);
}

int storage_mgr_snapshot_in_progress(const storage_mgr_t *mgr)
{
    (void)mgr;
    return 0;  // Never in progress on Windows (synchronous)
}

#else
// =============================================================================
// POSIX Implementation: Async Fork-Based Snapshots
// =============================================================================

int storage_mgr_maybe_snapshot(storage_mgr_t *mgr)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Already have one in progress
    if (mgr->snapshot_in_progress) {
        return LYGUS_OK;
    }

    // Check threshold using our tracked size
    if (mgr->wal_bytes_written < mgr->snapshot_threshold) {
        return LYGUS_OK;
    }

    // Nothing to snapshot
    if (mgr->applied_index == 0) {
        return LYGUS_OK;
    }

    LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_START,
             mgr->applied_index, mgr->applied_term);

    // Record what we're snapshotting
    mgr->snapshot_index = mgr->applied_index;
    mgr->snapshot_term = mgr->applied_term;

    int ret = snapshot_path_from_index(mgr->snapshot_dir,
                                       mgr->snapshot_index,
                                       mgr->snapshot_term,
                                       mgr->snapshot_path,
                                       sizeof(mgr->snapshot_path));
    if (ret != LYGUS_OK) {
        return ret;
    }

    // Fork!
    pid_t pid = fork();

    if (pid < 0) {
        // Fork failed
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_START,
                  mgr->snapshot_index, mgr->snapshot_term, NULL, 0);
        return LYGUS_ERR_IO;
    }

    if (pid == 0) {
        // Child process - write snapshot and exit
        // COW gives us a consistent view of the KV store
        int child_ret = snapshot_write(mgr->kv,
                                       mgr->snapshot_index,
                                       mgr->snapshot_term,
                                       mgr->snapshot_path);
        _exit(child_ret == LYGUS_OK ? 0 : 1);
    }

    // Parent process
    mgr->snapshot_in_progress = 1;
    mgr->snapshot_pid = pid;

    LOG_DEBUG(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_START,
              mgr->snapshot_index, mgr->snapshot_term, NULL, 0);

    return LYGUS_OK;
}

int storage_mgr_poll_snapshot(storage_mgr_t *mgr)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (!mgr->snapshot_in_progress) {
        return LYGUS_OK;
    }

    // Non-blocking wait
    int status;
    pid_t result = waitpid(mgr->snapshot_pid, &status, WNOHANG);

    if (result == 0) {
        // Still running
        return LYGUS_OK;
    }

    if (result < 0) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
                  mgr->snapshot_index, mgr->snapshot_term, NULL, 0);
        mgr->snapshot_in_progress = 0;
        return LYGUS_ERR_IO;
    }

    // Child completed
    mgr->snapshot_in_progress = 0;

    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        // Success! Do bookkeeping
        LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
                 mgr->snapshot_index, mgr->snapshot_term);

        return snapshot_bookkeeping(mgr, mgr->snapshot_index, mgr->snapshot_term);

    } else {
        // Failed
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
                  mgr->snapshot_index, mgr->snapshot_term, NULL, 0);

        // Clean up partial file if it exists
        unlink(mgr->snapshot_path);

        return LYGUS_ERR_IO;
    }
}

int storage_mgr_force_snapshot(storage_mgr_t *mgr)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (mgr->snapshot_in_progress) {
        return LYGUS_ERR_BUSY;
    }

    if (mgr->applied_index == 0) {
        return LYGUS_OK;
    }

    // Record and start
    mgr->snapshot_index = mgr->applied_index;
    mgr->snapshot_term = mgr->applied_term;

    int ret = snapshot_path_from_index(mgr->snapshot_dir,
                                       mgr->snapshot_index,
                                       mgr->snapshot_term,
                                       mgr->snapshot_path,
                                       sizeof(mgr->snapshot_path));
    if (ret != LYGUS_OK) {
        return ret;
    }

    pid_t pid = fork();

    if (pid < 0) {
        return LYGUS_ERR_IO;
    }

    if (pid == 0) {
        int child_ret = snapshot_write(mgr->kv,
                                       mgr->snapshot_index,
                                       mgr->snapshot_term,
                                       mgr->snapshot_path);
        _exit(child_ret == LYGUS_OK ? 0 : 1);
    }

    mgr->snapshot_in_progress = 1;
    mgr->snapshot_pid = pid;

    return LYGUS_OK;
}

int storage_mgr_snapshot_in_progress(const storage_mgr_t *mgr)
{
    return mgr ? mgr->snapshot_in_progress : 0;
}

#endif // _WIN32 vs POSIX

// ============================================================================
// Install Snapshot (from leader)
// ============================================================================

int storage_mgr_install_snapshot(storage_mgr_t *mgr,
                                 const void *data, size_t data_len,
                                 uint64_t index, uint64_t term)
{
    if (!mgr || !data || data_len == 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

#ifndef _WIN32
    // Wait for any in-progress snapshot
    if (mgr->snapshot_in_progress) {
        int status;
        waitpid(mgr->snapshot_pid, &status, 0);
        mgr->snapshot_in_progress = 0;
    }
#endif

    LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD,
             index, term);

    // Write snapshot data to file
    char snap_path[512];
    int ret = snapshot_path_from_index(mgr->snapshot_dir, index, term,
                                       snap_path, sizeof(snap_path));
    if (ret != LYGUS_OK) {
        return ret;
    }

    // Write to temp file first
    char tmp_path[520];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", snap_path);

    FILE *f = fopen(tmp_path, "wb");
    if (!f) {
        return LYGUS_ERR_IO;
    }

    if (fwrite(data, 1, data_len, f) != data_len) {
        fclose(f);
        unlink(tmp_path);
        return LYGUS_ERR_IO;
    }

    fclose(f);

    // Atomic rename
    if (rename(tmp_path, snap_path) != 0) {
        unlink(tmp_path);
        return LYGUS_ERR_IO;
    }

    // Load into KV
    uint64_t loaded_index, loaded_term;
    ret = snapshot_load(snap_path, mgr->kv, &loaded_index, &loaded_term);
    if (ret != LYGUS_OK) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD,
                  index, term, NULL, 0);
        return ret;
    }

    // Update state
    mgr->applied_index = loaded_index;
    mgr->applied_term = loaded_term;
    mgr->logged_index = loaded_index;
    mgr->logged_term = loaded_term;

    // Clear WAL - starting fresh from this snapshot
    ret = wal_clear(mgr->wal);
    if (ret != LYGUS_OK) {
        LOG_WARN(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD,
                 index, term, NULL, 0);
    }

    // Purge old snapshots
    snapshot_purge_old(mgr->snapshot_dir, mgr->snapshots_to_keep);

    LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD,
             index, term);

    return LYGUS_OK;
}

// ============================================================================
// Get Snapshot (for sending to follower)
// ============================================================================

ssize_t storage_mgr_get_snapshot(storage_mgr_t *mgr,
                                 void *data_out, size_t data_cap,
                                 uint64_t *index_out, uint64_t *term_out)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Find latest snapshot
    char snap_path[512];
    uint64_t snap_index, snap_term;

    int ret = snapshot_find_latest(mgr->snapshot_dir, snap_path,
                                   &snap_index, &snap_term);
    if (ret != LYGUS_OK) {
        return ret;
    }

    // Get file size
    FILE *f = fopen(snap_path, "rb");
    if (!f) {
        return LYGUS_ERR_IO;
    }

    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (index_out) *index_out = snap_index;
    if (term_out) *term_out = snap_term;

    // If just querying size
    if (!data_out || data_cap == 0) {
        fclose(f);
        return size;
    }

    // Read data
    size_t to_read = (size_t)size < data_cap ? (size_t)size : data_cap;
    if (fread(data_out, 1, to_read, f) != to_read) {
        fclose(f);
        return LYGUS_ERR_IO;
    }

    fclose(f);
    return size;
}

// ============================================================================
// State Queries
// ============================================================================

uint64_t storage_mgr_applied_index(const storage_mgr_t *mgr)
{
    return mgr ? mgr->applied_index : 0;
}

uint64_t storage_mgr_applied_term(const storage_mgr_t *mgr)
{
    return mgr ? mgr->applied_term : 0;
}

uint64_t storage_mgr_logged_index(const storage_mgr_t *mgr)
{
    return mgr ? mgr->logged_index : 0;
}

uint64_t storage_mgr_logged_term(const storage_mgr_t *mgr)
{
    return mgr ? mgr->logged_term : 0;
}

size_t storage_mgr_wal_size(const storage_mgr_t *mgr)
{
    return mgr ? mgr->wal_bytes_written : 0;
}

// ============================================================================
// Log Truncation
// ============================================================================

int storage_mgr_truncate_after(storage_mgr_t *mgr, uint64_t after_index)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Can't truncate below what we've applied
    if (after_index < mgr->applied_index) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_WAL_CORRUPTION,
                  after_index, mgr->applied_index, NULL, 0);
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = wal_truncate_after(mgr->wal, after_index);
    if (ret != LYGUS_OK) {
        return ret;
    }

    // Update logged state
    mgr->logged_index = after_index;
    if (after_index > 0) {
        // Need to get the term from WAL
        // For now, use applied_term if we truncated down to applied_index
        if (after_index == mgr->applied_index) {
            mgr->logged_term = mgr->applied_term;
        }
        // Otherwise the term should be obtained from WAL lookup
        // (you may need to add wal_get_term(wal, index) function)
    } else {
        mgr->logged_term = 0;
    }

    return LYGUS_OK;
}