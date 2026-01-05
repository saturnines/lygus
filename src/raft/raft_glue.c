/**
 * raft_glue.c - Glue layer between lil-raft and lygus storage
 */

#include "raft_glue.h"
#include "raft_internal.h"
#include "network/wire_format.h"
#include "platform/platform.h"
#include "public/lygus_errors.h"
#include "../state/kv_op.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>


// STATIC SCRATCH BUFFER: Avoids malloc/free on every heartbeat.
// 128KB is enough for a large batch of entries.
static uint8_t scratch_buf[131072];

// ============================================================================
// Lifecycle
// ============================================================================

int glue_ctx_init(raft_glue_ctx_t *ctx, const char *data_dir,
                  int node_id, const char *peers_file)
{
    if (!ctx || !data_dir || !peers_file) {
        return LYGUS_ERR_INVALID_ARG;
    }

    memset(ctx, 0, sizeof(*ctx));
    snprintf(ctx->data_dir, sizeof(ctx->data_dir), "%s", data_dir);

    // Initialize snapshot receive buffer to NULL
    ctx->snapshot_recv_buf = NULL;
    ctx->snapshot_recv_len = 0;
    ctx->snapshot_recv_cap = 0;

    // Configure storage manager
    storage_mgr_config_t cfg;
    storage_mgr_config_init(&cfg);
    cfg.data_dir = data_dir;

    // Open storage (runs recovery automatically)
    int ret = storage_mgr_open(&cfg, &ctx->storage);
    if (ret != LYGUS_OK) {
        return ret;
    }

    // Replay WAL entries to KV store (need to check how safe this is)
    uint64_t logged = storage_mgr_logged_index(ctx->storage);
    uint64_t applied = storage_mgr_applied_index(ctx->storage);
    if (logged > 0) {
        ret = storage_mgr_replay_to(ctx->storage, logged);
        if (ret != LYGUS_OK) {
            storage_mgr_close(ctx->storage);
            ctx->storage = NULL;
            return ret;
        }
    }

    // Load peers and create network
    peer_info_t peers[16];
    int num_peers = network_load_peers(peers_file, peers, 16);
    if (num_peers < 0) {
        storage_mgr_close(ctx->storage);
        ctx->storage = NULL;
        return LYGUS_ERR_IO;
    }

    network_config_t net_cfg = {
        .node_id = node_id,
        .peers = peers,
        .num_peers = num_peers,
        .mailbox_size = 256,
    };

    ctx->network = network_create(&net_cfg);
    if (!ctx->network) {
        storage_mgr_close(ctx->storage);
        ctx->storage = NULL;
        return LYGUS_ERR_IO;
    }

    return LYGUS_OK;
}

void glue_ctx_destroy(raft_glue_ctx_t *ctx)
{
    if (!ctx) return;

    if (ctx->network) {
        network_stop(ctx->network);
        network_destroy(ctx->network);
        ctx->network = NULL;
    }

    if (ctx->storage) {
        storage_mgr_close(ctx->storage);
        ctx->storage = NULL;
    }

    // Free snapshot receive buffer
    free(ctx->snapshot_recv_buf);
    ctx->snapshot_recv_buf = NULL;
    ctx->snapshot_recv_len = 0;
    ctx->snapshot_recv_cap = 0;
}

int glue_ctx_start_network(raft_glue_ctx_t *ctx)
{
    if (!ctx || !ctx->network) return LYGUS_ERR_INVALID_ARG;
    return network_start(ctx->network);
}

void glue_ctx_stop_network(raft_glue_ctx_t *ctx)
{
    if (!ctx || !ctx->network) return;
    network_stop(ctx->network);
}

// ============================================================================
// Entry Serialization
// ============================================================================

ssize_t glue_serialize_put(void *buf, size_t buf_cap,
                           const void *key, size_t klen,
                           const void *val, size_t vlen) {
    size_t needed = GLUE_ENTRY_HEADER_SIZE + klen + vlen;
    if (!buf || buf_cap < needed) {
        return -(ssize_t)needed;  // Return negative of needed size
    }

    uint8_t *p = (uint8_t *)buf;

    // Type
    p[0] = GLUE_ENTRY_PUT;

    // Key length (little-endian)
    uint32_t klen32 = (uint32_t)klen;
    memcpy(p + 1, &klen32, 4);

    // Value length (little-endian)
    uint32_t vlen32 = (uint32_t)vlen;
    memcpy(p + 5, &vlen32, 4);

    // Key
    if (klen > 0) {
        memcpy(p + 9, key, klen);
    }

    // Value
    if (vlen > 0) {
        memcpy(p + 9 + klen, val, vlen);
    }

    return (ssize_t)needed;
}

ssize_t glue_serialize_del(void *buf, size_t buf_cap,
                           const void *key, size_t klen) {
    size_t needed = GLUE_ENTRY_HEADER_SIZE + klen;
    if (!buf || buf_cap < needed) {
        return -(ssize_t)needed;
    }

    uint8_t *p = (uint8_t *)buf;

    p[0] = GLUE_ENTRY_DEL;

    uint32_t klen32 = (uint32_t)klen;
    memcpy(p + 1, &klen32, 4);

    uint32_t vlen32 = 0;
    memcpy(p + 5, &vlen32, 4);

    if (klen > 0) {
        memcpy(p + 9, key, klen);
    }

    return (ssize_t)needed;
}

ssize_t glue_serialize_noop(void *buf, size_t buf_cap) {
    size_t needed = GLUE_ENTRY_HEADER_SIZE;
    if (!buf || buf_cap < needed) {
        return -(ssize_t)needed;
    }

    uint8_t *p = (uint8_t *)buf;

    p[0] = GLUE_ENTRY_NOOP;

    uint32_t zero = 0;
    memcpy(p + 1, &zero, 4);  // klen = 0
    memcpy(p + 5, &zero, 4);  // vlen = 0

    return (ssize_t)needed;
}

int glue_parse_entry(const void *data, size_t len,
                     uint8_t *type_out,
                     uint32_t *klen_out, uint32_t *vlen_out,
                     const void **key_out, const void **val_out) {
    if (!data || len < GLUE_ENTRY_HEADER_SIZE) {
        return LYGUS_ERR_INVALID_ARG;
    }

    const uint8_t *p = (const uint8_t *)data;

    uint8_t type = p[0];
    if (type < GLUE_ENTRY_PUT || type > GLUE_ENTRY_NOOP) {
        return LYGUS_ERR_MALFORMED;
    }

    uint32_t klen, vlen;
    memcpy(&klen, p + 1, 4);
    memcpy(&vlen, p + 5, 4);

    // Bounds check: klen must fit in remaining buffer
    if (klen > len - GLUE_ENTRY_HEADER_SIZE) {
        return LYGUS_ERR_MALFORMED;
    }

    // Bounds check: vlen must fit after klen
    if (vlen > len - GLUE_ENTRY_HEADER_SIZE - klen) {
        return LYGUS_ERR_MALFORMED;
    }

    if (type_out) *type_out = type;
    if (klen_out) *klen_out = klen;
    if (vlen_out) *vlen_out = vlen;
    if (key_out)  *key_out  = (klen > 0) ? (p + 9) : NULL;
    if (val_out)  *val_out  = (vlen > 0) ? (p + 9 + klen) : NULL;

    return LYGUS_OK;
}

// ============================================================================
// Vote Persistence
// ============================================================================

int glue_persist_vote(void *ctx, uint64_t term, int voted_for) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g) return LYGUS_ERR_INVALID_ARG;

    char path[512], tmp_path[512];
    snprintf(path, sizeof(path), "%s/vote.dat", g->data_dir);
    snprintf(tmp_path, sizeof(tmp_path), "%s/vote.dat.tmp", g->data_dir);

    // Format: [term:8][voted_for:4]
    uint8_t buf[12];
    memcpy(buf, &term, 8);
    memcpy(buf + 8, &voted_for, 4);

    // Atomic write: write to tmp, fsync, rename
    lygus_fd_t fd = lygus_file_open(tmp_path,
        LYGUS_O_WRONLY | LYGUS_O_CREAT | LYGUS_O_TRUNC, 0644);
    if (fd == LYGUS_INVALID_FD) {
        return LYGUS_ERR_OPEN_FILE;
    }

    int64_t written = lygus_file_write(fd, buf, sizeof(buf));
    if (written != sizeof(buf)) {
        lygus_file_close(fd);
        lygus_unlink(tmp_path);
        return LYGUS_ERR_WRITE;
    }

    if (lygus_file_sync(fd) < 0) {
        lygus_file_close(fd);
        lygus_unlink(tmp_path);
        return LYGUS_ERR_FSYNC;
    }

    lygus_file_close(fd);

    if (lygus_rename(tmp_path, path) < 0) {
        lygus_unlink(tmp_path);
        return LYGUS_ERR_IO;
    }

    return LYGUS_OK;
}

int glue_load_vote(void *ctx, uint64_t *term, int *voted_for) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !term || !voted_for) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Defaults for first boot
    *term = 0;
    *voted_for = -1;

    char path[512];
    snprintf(path, sizeof(path), "%s/vote.dat", g->data_dir);

    lygus_fd_t fd = lygus_file_open(path, LYGUS_O_RDONLY, 0);
    if (fd == LYGUS_INVALID_FD) {
        // No vote file - first boot, use defaults
        return LYGUS_OK;
    }

    uint8_t buf[12];
    int64_t n = lygus_file_read(fd, buf, sizeof(buf));
    lygus_file_close(fd);

    if (n != sizeof(buf)) {
        // Corrupted file - use defaults (safe)
        return LYGUS_OK;
    }

    memcpy(term, buf, 8);
    memcpy(voted_for, buf + 8, 4);

    return LYGUS_OK;
}

// ============================================================================
// Log Operations
// ============================================================================

int glue_log_append(void *ctx, uint64_t index, uint64_t term,
                    const void *data, size_t len) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !g->storage) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (data == NULL || len == 0) {
        return storage_mgr_log_noop(g->storage, index, term);
    }

    // (serialized kv_op)
    return storage_mgr_log_raw(g->storage, index, term, data, len);
}

int glue_log_get(void *ctx, uint64_t index, void *buf, size_t *len) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !g->storage || !len) {
        return LYGUS_ERR_INVALID_ARG;
    }

    uint64_t term;
    ssize_t n = storage_mgr_log_get(g->storage, index, &term, buf, buf ? *len : 0);

    if (n < 0 && n > -1000) {
        // Negative but small = needed buffer size
        *len = (size_t)(-n);
        return LYGUS_ERR_BUFFER_TOO_SMALL;
    }

    if (n < 0) {
        // Actual error code
        return (int)n;
    }

    *len = (size_t)n;
    return LYGUS_OK;
}

int glue_log_truncate_after(void *ctx, uint64_t index) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !g->storage) {
        return LYGUS_ERR_INVALID_ARG;
    }

    return storage_mgr_truncate_after(g->storage, index);
}

uint64_t glue_log_first_index(void *ctx) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !g->storage) {
        return 0;
    }

    return storage_mgr_first_index(g->storage);
}

uint64_t glue_log_last_index(void *ctx) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !g->storage) {
        return 0;
    }

    return storage_mgr_logged_index(g->storage);
}

uint64_t glue_log_last_term(void *ctx) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !g->storage) {
        return 0;
    }

    return storage_mgr_logged_term(g->storage);
}

// This is suicide will need to refactor.
int glue_restore_raft_log(raft_glue_ctx_t *ctx, raft_t *raft) {
    if (!ctx || !ctx->storage || !raft) return LYGUS_ERR_INVALID_ARG;

    uint64_t first = storage_mgr_first_index(ctx->storage);
    uint64_t last = storage_mgr_logged_index(ctx->storage);

    printf("[RESTORE] first=%lu last=%lu\n", first, last);

    if (last == 0) return LYGUS_OK;  // Empty log, nothing to restore

    // Tell the amnesiac log where it left off
    raft->log.base_index = first - 1;

    printf("[RESTORE] set base_index=%lu\n", first - 1);

    uint8_t buf[65536];

    for (uint64_t i = first; i <= last; i++) {
        uint64_t term;
        ssize_t len = storage_mgr_log_get(ctx->storage, i, &term, buf, sizeof(buf));

        // Fail hard on gaps, a missing entry means corrupted storage
        if (len < 0) {
            return LYGUS_ERR_CORRUPT;
        }

        // Determine entry type from data
        raft_entry_type_t type = RAFT_ENTRY_DATA;
        if (len == 0 || (len >= 1 && buf[0] == GLUE_ENTRY_NOOP)) {
            type = RAFT_ENTRY_NOOP;
        }

        raft_entry_t entry = {
            .index = i,
            .term = term,
            .type = type,
            .data = (len > 0 && type != RAFT_ENTRY_NOOP) ? buf : NULL,
            .len = (type != RAFT_ENTRY_NOOP) ? (size_t)len : 0,
        };
        raft_log_append_entry(&raft->log, &entry);
    }

    uint64_t applied = storage_mgr_applied_index(ctx->storage);
    raft->commit_index = applied;
    raft->last_applied = applied;

    if (raft_log_last_index(&raft->log) != last) {
        return LYGUS_ERR_CORRUPT;
    }

    printf("[RESTORE] done, raft last_index=%lu\n",
           raft_log_last_index(&raft->log));

    return LYGUS_OK;
}

// ============================================================================
// State Machine Application
// ============================================================================

int glue_apply_entry(void *ctx, uint64_t index, uint64_t term,
                     raft_entry_type_t type, const void *data, size_t len) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !g->storage) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Handle Raft NOOP
    if (type == RAFT_ENTRY_NOOP || data == NULL || len == 0) {
        return storage_mgr_apply_noop(g->storage, index, term);
    }

    // Deserialize KV operation
    uint8_t entry_type;
    uint32_t klen, vlen;
    const void *key, *val;

    int ret = glue_parse_entry(data, len, &entry_type, &klen, &vlen, &key, &val);
    if (ret != LYGUS_OK) {
        return ret;
    }

    switch (entry_type) {
        case GLUE_ENTRY_PUT:
            return storage_mgr_apply_put(g->storage, index, term,
                                         key, klen, val, vlen);

        case GLUE_ENTRY_DEL:
            return storage_mgr_apply_del(g->storage, index, term, key, klen);

        case GLUE_ENTRY_NOOP:
            return storage_mgr_apply_noop(g->storage, index, term);

        default:
            return LYGUS_ERR_MALFORMED;
    }
}

// ============================================================================
// Snapshot Callbacks
// ============================================================================

int glue_snapshot_create(void *ctx, uint64_t index, uint64_t term) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !g->storage) return LYGUS_ERR_INVALID_ARG;

    // Kick off async snapshot, returns immediately after fork
    return storage_mgr_force_snapshot(g->storage);
}

int glue_snapshot_poll(void *ctx) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !g->storage) return 1;  // No storage = "done"

    // Poll for completion and handle cleanup
    storage_mgr_poll_snapshot(g->storage);

    // Return 1 if done, 0 if still in progress
    return !storage_mgr_snapshot_in_progress(g->storage);
}

int glue_snapshot_load(void *ctx, uint64_t *out_index, uint64_t *out_term) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !g->storage || !out_index || !out_term) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Query snapshot metadata passing null here for metadata
    ssize_t ret = storage_mgr_get_snapshot(g->storage, NULL, 0, out_index, out_term);

    if (ret < 0 && *out_index == 0) {
        return RAFT_ERR_SNAPSHOT_NOT_FOUND;
    }

    return LYGUS_OK;
}

int glue_snapshot_read(void *ctx, uint64_t offset, void *buf, size_t buf_len,
                       size_t *out_len, int *out_done) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !g->storage || !out_len || !out_done) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Get total snapshot size first
    uint64_t snap_index, snap_term;
    ssize_t total_size = storage_mgr_get_snapshot(g->storage, NULL, 0,
                                                   &snap_index, &snap_term);
    if (total_size < 0) {
        return (int)total_size;
    }

    // Calculate how much to read
    if (offset >= (uint64_t)total_size) {
        *out_len = 0;
        *out_done = 1;
        return LYGUS_OK;
    }

    size_t remaining = (size_t)(total_size - offset);
    size_t to_read = (remaining < buf_len) ? remaining : buf_len;

    // Read whole snapshot into temp buffer, then copy chunk
    // (inefficient but works, could optimize with mmap or streaming later)
    uint8_t *full_snap = malloc(total_size);
    if (!full_snap) {
        return LYGUS_ERR_NOMEM;
    }

    ssize_t got = storage_mgr_get_snapshot(g->storage, full_snap, total_size,
                                           &snap_index, &snap_term);
    if (got < 0) {
        free(full_snap);
        return (int)got;
    }

    memcpy(buf, full_snap + offset, to_read);
    free(full_snap);

    *out_len = to_read;
    *out_done = (offset + to_read >= (uint64_t)total_size) ? 1 : 0;

    return LYGUS_OK;
}

int glue_snapshot_write(void *ctx, uint64_t offset, const void *data,
                        size_t len, int done) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g) return LYGUS_ERR_INVALID_ARG;

    // First chunk, allocate/reset buffer
    if (offset == 0) {
        free(g->snapshot_recv_buf);
        g->snapshot_recv_buf = malloc(len > 0 ? len : 1);
        if (!g->snapshot_recv_buf) {
            return LYGUS_ERR_NOMEM;
        }
        g->snapshot_recv_len = 0;
        g->snapshot_recv_cap = len > 0 ? len : 1;
    }

    // Grow buffer if needed
    size_t needed = offset + len;
    if (needed > g->snapshot_recv_cap) {
        size_t new_cap = g->snapshot_recv_cap * 2;
        if (new_cap < needed) new_cap = needed;

        uint8_t *new_buf = realloc(g->snapshot_recv_buf, new_cap);
        if (!new_buf) {
            return LYGUS_ERR_NOMEM;
        }

        g->snapshot_recv_buf = new_buf;
        g->snapshot_recv_cap = new_cap;
    }

    // Copy chunk
    if (len > 0) {
        memcpy(g->snapshot_recv_buf + offset, data, len);
    }
    if (offset + len > g->snapshot_recv_len) {
        g->snapshot_recv_len = offset + len;
    }

    return LYGUS_OK;
}

int glue_snapshot_restore(void *ctx, uint64_t index, uint64_t term) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !g->storage) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Need to have received snapshot data
    if (!g->snapshot_recv_buf || g->snapshot_recv_len == 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Install the accumulated snapshot
    int ret = storage_mgr_install_snapshot(g->storage,
                                           g->snapshot_recv_buf,
                                           g->snapshot_recv_len,
                                           index, term);

    // Clean up receive buffer
    free(g->snapshot_recv_buf);
    g->snapshot_recv_buf = NULL;
    g->snapshot_recv_len = 0;
    g->snapshot_recv_cap = 0;

    return ret;
}

// ============================================================================
// Network Callbacks
// ============================================================================

int glue_send_requestvote(void *ctx, int peer_id,
                          const raft_requestvote_req_t *req) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !g->network || !req) return LYGUS_ERR_INVALID_ARG;

    return network_send_raft(g->network, peer_id, MSG_REQUESTVOTE_REQ,
                             req, sizeof(*req));
}

int glue_send_appendentries(void *ctx, int peer_id,
                            const raft_appendentries_req_t *req,
                            const raft_entry_t *entries,
                            size_t n_entries) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !g->network || !req) return LYGUS_ERR_INVALID_ARG;

    // Calculate total size needed
    // Format: [ae_req][n_entries:4][entry0_term:8][entry0_type:1][entry0_len:4][entry0_data]..
    size_t total_size = sizeof(*req) + 4;  // Header + entry count

    for (size_t i = 0; i < n_entries; i++) {
        total_size += 8 + 1 + 4;  // term + type + len
        if (entries[i].data && entries[i].len > 0) {
            total_size += entries[i].len;
        }
    }

    // Use STATIC BUFFER to avoid malloc/free thrashing
    if (total_size > sizeof(scratch_buf)) {
        return LYGUS_ERR_NOMEM;
    }

    uint8_t *p = scratch_buf;

    // Copy AE request header
    memcpy(p, req, sizeof(*req));
    p += sizeof(*req);

    // Entry count
    uint32_t count = (uint32_t)n_entries;
    memcpy(p, &count, 4);
    p += 4;

    // Entries
    for (size_t i = 0; i < n_entries; i++) {
        // Term
        memcpy(p, &entries[i].term, 8);
        p += 8;

        // Type
        uint8_t type = (uint8_t)entries[i].type;
        *p++ = type;

        // Length + data
        uint32_t len = (uint32_t)entries[i].len;
        memcpy(p, &len, 4);
        p += 4;

        if (entries[i].data && len > 0) {
            memcpy(p, entries[i].data, len);
            p += len;
        }
    }

    return network_send_raft(g->network, peer_id, MSG_APPENDENTRIES_REQ,
                             scratch_buf, total_size);
}

int glue_send_installsnapshot(void *ctx, int peer_id,
                              const raft_installsnapshot_req_t *req) {
    raft_glue_ctx_t *g = (raft_glue_ctx_t *)ctx;
    if (!g || !g->network || !req) return LYGUS_ERR_INVALID_ARG;

    // Serialize: [term:8][leader_id:4][last_index:8][last_term:8][offset:8][len:4][done:4][data]
    size_t header_size = 8 + 4 + 8 + 8 + 8 + 4 + 4;  // 44 bytes
    size_t total = header_size + req->len;

    if (total > sizeof(scratch_buf)) {
        return LYGUS_ERR_NOMEM;
    }

    uint8_t *p = scratch_buf;

    // term
    memcpy(p, &req->term, 8);
    p += 8;

    // leader_id
    memcpy(p, &req->leader_id, 4);
    p += 4;

    // last_index
    memcpy(p, &req->last_index, 8);
    p += 8;

    // last_term
    memcpy(p, &req->last_term, 8);
    p += 8;

    // offset
    memcpy(p, &req->offset, 8);
    p += 8;

    // len (as uint32_t for wire format)
    uint32_t len32 = (uint32_t)req->len;
    memcpy(p, &len32, 4);
    p += 4;

    // done
    memcpy(p, &req->done, 4);
    p += 4;

    // data
    if (req->len > 0 && req->data) {
        memcpy(p, req->data, req->len);
        p += req->len;
    }

    return network_send_raft(g->network, peer_id, MSG_INSTALLSNAPSHOT_REQ,
                             scratch_buf, total);
}

// ============================================================================
// Network Receive Helpers (call from main loop)
// ============================================================================

int glue_process_network(raft_glue_ctx_t *ctx, raft_t *raft)
{
    if (!ctx || !ctx->network || !raft) return 0;

    int processed = 0;
    uint8_t buf[65536];
    int from_id;
    uint8_t msg_type;

    while (1) {
        int len = network_recv_raft(ctx->network, &from_id, &msg_type, buf, sizeof(buf));
        if (len <= 0) break;

        switch (msg_type) {
            case MSG_REQUESTVOTE_REQ: {
                if (len >= (int)sizeof(raft_requestvote_req_t)) {
                    raft_requestvote_req_t *req = (raft_requestvote_req_t *)buf;
                    raft_requestvote_resp_t resp;
                    raft_recv_requestvote(raft, req, &resp);

                    network_send_raft(ctx->network, from_id, MSG_REQUESTVOTE_RESP,
                                      &resp, sizeof(resp));
                }
                break;
            }

            case MSG_REQUESTVOTE_RESP: {
                if (len >= (int)sizeof(raft_requestvote_resp_t)) {
                    raft_requestvote_resp_t *resp = (raft_requestvote_resp_t *)buf;
                    raft_recv_requestvote_response(raft, from_id, resp);
                }
                break;
            }

            case MSG_APPENDENTRIES_REQ: {
                if (len >= (int)sizeof(raft_appendentries_req_t) + 4) {
                    raft_appendentries_req_t *req = (raft_appendentries_req_t *)buf;
                    uint8_t *p = buf + sizeof(*req);

                    uint32_t n_entries;
                    memcpy(&n_entries, p, 4);
                    p += 4;

                    raft_entry_t *entries = NULL;
                    if (n_entries > 0) {
                        if (n_entries > sizeof(buf) / 13) {
                            break;
                        }

                        entries = calloc(n_entries, sizeof(raft_entry_t));
                        if (!entries) break;

                        int malformed = 0;
                        for (uint32_t i = 0; i < n_entries; i++) {
                            if (p + 13 > buf + len) {
                                malformed = 1;
                                break;
                            }

                            memcpy(&entries[i].term, p, 8);
                            p += 8;

                            entries[i].type = (raft_entry_type_t)*p++;

                            uint32_t data_len;
                            memcpy(&data_len, p, 4);
                            p += 4;

                            entries[i].len = data_len;

                            if (data_len > 0) {
                                if (p + data_len > buf + len) {
                                    malformed = 1;
                                    break;
                                }
                                entries[i].data = p;
                                p += data_len;
                            }
                        }

                        if (!malformed) {
                            raft_appendentries_resp_t resp;
                            raft_recv_appendentries(raft, req, entries, n_entries, &resp);
                            network_send_raft(ctx->network, from_id, MSG_APPENDENTRIES_RESP,
                                              &resp, sizeof(resp));
                        }

                        free(entries);
                    } else {
                        raft_appendentries_resp_t resp;
                        raft_recv_appendentries(raft, req, NULL, 0, &resp);
                        network_send_raft(ctx->network, from_id, MSG_APPENDENTRIES_RESP,
                                          &resp, sizeof(resp));
                    }
                }
                break;
            }

            case MSG_APPENDENTRIES_RESP: {
                if (len >= (int)sizeof(raft_appendentries_resp_t)) {
                    raft_appendentries_resp_t *resp = (raft_appendentries_resp_t *)buf;
                    raft_recv_appendentries_response(raft, from_id, resp);
                }
                break;
            }

            case MSG_INSTALLSNAPSHOT_REQ: {
                // Parse: [term:8][leader_id:4][last_index:8][last_term:8][offset:8][len:4][done:4][data]
                if (len >= 44) {  // Minimum header size
                    uint8_t *p = buf;

                    raft_installsnapshot_req_t req;

                    memcpy(&req.term, p, 8);
                    p += 8;

                    memcpy(&req.leader_id, p, 4);
                    p += 4;

                    memcpy(&req.last_index, p, 8);
                    p += 8;

                    memcpy(&req.last_term, p, 8);
                    p += 8;

                    memcpy(&req.offset, p, 8);
                    p += 8;

                    uint32_t data_len;
                    memcpy(&data_len, p, 4);
                    p += 4;
                    req.len = data_len;

                    memcpy(&req.done, p, 4);
                    p += 4;

                    // Validate data length
                    if ((int)(44 + data_len) <= len) {
                        req.data = (data_len > 0) ? p : NULL;

                        raft_installsnapshot_resp_t resp;
                        raft_recv_installsnapshot(raft, &req, &resp);

                        network_send_raft(ctx->network, from_id, MSG_INSTALLSNAPSHOT_RESP,
                                          &resp, sizeof(resp));
                    }
                }
                break;
            }

            case MSG_INSTALLSNAPSHOT_RESP: {
                if (len >= (int)sizeof(raft_installsnapshot_resp_t)) {
                    raft_installsnapshot_resp_t *resp = (raft_installsnapshot_resp_t *)buf;
                    raft_recv_installsnapshot_response(raft, from_id, resp);
                }
                break;
            }
        }

        processed++;
    }

    return processed;
}

// ============================================================================
// INV Helpers
// ============================================================================

int glue_process_inv(raft_glue_ctx_t *ctx,
                     void (*on_inv)(const void *key, size_t klen, void *user_data),
                     void *user_data)
{
    if (!ctx || !ctx->network) return 0;

    int processed = 0;
    uint8_t key_buf[1024];
    int from_id;

    while (1) {
        int klen = network_recv_inv(ctx->network, &from_id, key_buf, sizeof(key_buf));
        if (klen <= 0) break;

        if (on_inv) {
            on_inv(key_buf, (size_t)klen, user_data);
        }

        processed++;
    }

    return processed;
}

int glue_broadcast_inv(raft_glue_ctx_t *ctx, const void *key, size_t klen)
{
    if (!ctx || !ctx->network) return LYGUS_ERR_INVALID_ARG;
    return network_broadcast_inv(ctx->network, key, klen);
}