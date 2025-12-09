/**
 * storage_mgr.h - Storage Manager for Lygus
 *
 * Coordinates WAL, snapshots, and KV store for Raft integration.
 * Provides the two-moment model:
 *   - Moment 1 (log_*): Persist to WAL, fsync, safe to ACK
 *   - Moment 2 (apply_*): Apply to KV after commit known
 *
 * Cross-platform: Windows uses synchronous snapshots,
 * POSIX uses fork-based async snapshots with COW.
 */

#ifndef LYGUS_STORAGE_MGR_H
#define LYGUS_STORAGE_MGR_H

#include <stdint.h>
#include <stddef.h>

#ifdef _WIN32
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
#else
#include <sys/types.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

// Forward declarations
typedef struct storage_mgr storage_mgr_t;

// ============================================================================
// Configuration
// ============================================================================

#define STORAGE_MGR_DEFAULT_SNAPSHOT_THRESHOLD  (64 * 1024 * 1024)  // 64 MB
#define STORAGE_MGR_DEFAULT_SNAPSHOTS_TO_KEEP   2
#define STORAGE_MGR_DEFAULT_WAL_SEGMENT_SIZE    (64 * 1024 * 1024)  // 64 MB

/**
 * Storage manager configuration
 */
typedef struct storage_mgr_config {
    const char *data_dir;           // Base directory for all storage
    const char *wal_dir;            // WAL directory (NULL = data_dir/wal)
    const char *snapshot_dir;       // Snapshot directory (NULL = data_dir/snapshots)

    size_t snapshot_threshold;      // WAL size to trigger snapshot (default: 64MB)
    int    snapshots_to_keep;       // Number of old snapshots to retain (default: 2)

    size_t wal_segment_size;        // WAL segment size (default: 64MB)
    int    wal_sync_on_append;      // Sync after each append (default: 1)
} storage_mgr_config_t;

/**
 * Initialize config with defaults
 */
void storage_mgr_config_init(storage_mgr_config_t *cfg);

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Open storage manager (runs recovery automatically)
 *
 * Recovery order:
 *   1. Find latest snapshot, load into KV if exists
 *   2. Open WAL with recovery callback
 *   3. Replay WAL entries after snapshot_index
 *
 * @param cfg    Configuration
 * @param out    Output handle
 * @return LYGUS_OK on success
 */
int storage_mgr_open(const storage_mgr_config_t *cfg, storage_mgr_t **out);

/**
 * Close storage manager
 *
 * Waits for any in-progress snapshot to complete.
 */
void storage_mgr_close(storage_mgr_t *mgr);

// ============================================================================
// Moment 1: Log Operations (before ACK)
// ============================================================================

/**
 * Log a PUT operation to WAL
 *
 * Call this when receiving an entry from the leader.
 * Returns after fsync - safe to send ACCEPTED.
 *
 * @return LYGUS_OK on success
 */
int storage_mgr_log_put(storage_mgr_t *mgr,
                        uint64_t index, uint64_t term,
                        const void *key, size_t key_len,
                        const void *val, size_t val_len);

/**
 * Log a DELETE operation to WAL
 */
int storage_mgr_log_del(storage_mgr_t *mgr,
                        uint64_t index, uint64_t term,
                        const void *key, size_t key_len);

/**
 * Log a NOOP operation to WAL
 *
 * Used for:
 *   - Leader establishing commit point after election
 *   - NOOP_SYNC for Lazy-ALRs
 */
int storage_mgr_log_noop(storage_mgr_t *mgr,
                         uint64_t index, uint64_t term);

// ============================================================================
// Moment 2: Apply Operations (after commit)
// ============================================================================

/**
 * Apply a PUT to the KV store
 *
 * Must be called in order: index == applied_index + 1
 * Returns LYGUS_ERR_INVALID_ARG if out of order.
 *
 * @return LYGUS_OK on success
 */
int storage_mgr_apply_put(storage_mgr_t *mgr,
                          uint64_t index, uint64_t term,
                          const void *key, size_t key_len,
                          const void *val, size_t val_len);

/**
 * Apply a DELETE to the KV store
 *
 * Must be called in order: index == applied_index + 1
 */
int storage_mgr_apply_del(storage_mgr_t *mgr,
                          uint64_t index, uint64_t term,
                          const void *key, size_t key_len);

/**
 * Apply a NOOP (advances applied_index without KV change)
 *
 * Must be called in order: index == applied_index + 1
 */
int storage_mgr_apply_noop(storage_mgr_t *mgr,
                           uint64_t index, uint64_t term);

// ============================================================================
// Read Operations (for ALR reads)
// ============================================================================

/**
 * Get a value from the KV store
 *
 * @param key      Key to lookup
 * @param key_len  Key length
 * @param val_out  Buffer for value (NULL to query size)
 * @param val_cap  Buffer capacity
 * @return Value length on success, negative error code on failure
 */
ssize_t storage_mgr_get(storage_mgr_t *mgr,
                        const void *key, size_t key_len,
                        void *val_out, size_t val_cap);

// ============================================================================
// Snapshot Operations
// ============================================================================

/**
 * Check if snapshot should be triggered, start if needed
 *
 * Call this periodically (e.g., after each apply).
 * On POSIX: Starts async fork-based snapshot
 * On Windows: Performs synchronous snapshot
 *
 * @return LYGUS_OK on success or if no action needed
 *         LYGUS_ERR_BUSY if snapshot already in progress (POSIX only)
 */
int storage_mgr_maybe_snapshot(storage_mgr_t *mgr);

/**
 * Poll for async snapshot completion (POSIX only)
 *
 * On completion, performs bookkeeping:
 *   - Write SNAP_MARK to WAL
 *   - Fsync WAL
 *   - Purge old WAL segments
 *   - Purge old snapshots
 *
 * On Windows: No-op (snapshots are synchronous)
 *
 * @return LYGUS_OK on success
 */
int storage_mgr_poll_snapshot(storage_mgr_t *mgr);

/**
 * Force a snapshot now
 *
 * On POSIX: Starts async snapshot regardless of WAL size
 * On Windows: Performs synchronous snapshot
 *
 * @return LYGUS_OK on success
 *         LYGUS_ERR_BUSY if snapshot already in progress
 */
int storage_mgr_force_snapshot(storage_mgr_t *mgr);

/**
 * Install a snapshot received from leader (InstallSnapshot RPC)
 *
 * Used when follower is too far behind for WAL replay.
 *
 * @param data      Snapshot data
 * @param data_len  Snapshot data length
 * @param index     Snapshot index
 * @param term      Snapshot term
 * @return LYGUS_OK on success
 */
int storage_mgr_install_snapshot(storage_mgr_t *mgr,
                                 const void *data, size_t data_len,
                                 uint64_t index, uint64_t term);

/**
 * Get snapshot data for sending to follower
 *
 * @param data_out  Output buffer (NULL to query size)
 * @param data_cap  Buffer capacity
 * @param index_out Output: snapshot index
 * @param term_out  Output: snapshot term
 * @return Data length on success, negative error on failure
 */
ssize_t storage_mgr_get_snapshot(storage_mgr_t *mgr,
                                 void *data_out, size_t data_cap,
                                 uint64_t *index_out, uint64_t *term_out);

// ============================================================================
// State Queries
// ============================================================================

/**
 * Get the last applied index
 */
uint64_t storage_mgr_applied_index(const storage_mgr_t *mgr);

/**
 * Get the last applied term
 */
uint64_t storage_mgr_applied_term(const storage_mgr_t *mgr);

/**
 * Get the last logged index (may be > applied_index)
 */
uint64_t storage_mgr_logged_index(const storage_mgr_t *mgr);

/**
 * Get the last logged term
 */
uint64_t storage_mgr_logged_term(const storage_mgr_t *mgr);

/**
 * Check if async snapshot is in progress (always false on Windows)
 */
int storage_mgr_snapshot_in_progress(const storage_mgr_t *mgr);

/**
 * Get current WAL size in bytes
 */
size_t storage_mgr_wal_size(const storage_mgr_t *mgr);

// ============================================================================
// Log Truncation (for Raft conflicts)
// ============================================================================

/**
 * Truncate log after given index
 *
 * Used when leader sends conflicting entry.
 * Removes entries > index from WAL.
 *
 * @param after_index  Keep entries <= this index
 * @return LYGUS_OK on success
 */
int storage_mgr_truncate_after(storage_mgr_t *mgr, uint64_t after_index);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_STORAGE_MGR_H