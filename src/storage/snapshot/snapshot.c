#include "snapshot.h"
#include "util/varint.h"
#include "util/crc32c.h"
#include "util/logging.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef _WIN32
    #include <windows.h>
    #include <io.h>
    #include <direct.h>
    #define O_BINARY _O_BINARY
    #define open _open
    #define close _close
    #define read _read
    #define write _write
    #define fsync(fd) _commit(fd)
    #define unlink(path) _unlink(path)
    // MinGW defines ssize_t in corecrt.h, MSVC doesn't
    #ifdef _MSC_VER
        typedef int ssize_t;
    #endif
#else
    #include <unistd.h>
    #include <sys/wait.h>
    #include <dirent.h>
    #define O_BINARY 0
#endif

// ============================================================================
// Internal Helpers
// ============================================================================

/**
 * Write all bytes to fd (handles partial writes)
 */
static int write_all(int fd, const void *buf, size_t len) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t remaining = len;

    while (remaining > 0) {
        ssize_t n = write(fd, p, (unsigned int)remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += n;
        remaining -= n;
    }

    return 0;
}

/**
 * Read all bytes from fd (handles partial reads)
 */
static int read_all(int fd, void *buf, size_t len) {
    uint8_t *p = (uint8_t *)buf;
    size_t remaining = len;

    while (remaining > 0) {
        ssize_t n = read(fd, p, (unsigned int)remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) {
            return -1;  // Unexpected EOF
        }
        p += n;
        remaining -= n;
    }

    return 0;
}

// ============================================================================
// Snapshot Write
// ============================================================================

int snapshot_write(lygus_kv_t *kv,
                   uint64_t last_index,
                   uint64_t last_term,
                   const char *path)
{
    if (!kv || !path) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Build temp path
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);

    // Create temp file
    int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC | O_BINARY, 0644);
    if (fd < 0) {
        return LYGUS_ERR_OPEN_FILE;
    }

    int ret = LYGUS_OK;
    uint32_t crc = CRC32C_INIT;

    // Prepare header
    snapshot_hdr_t hdr = {
        .magic = SNAPSHOT_MAGIC,
        .version = SNAPSHOT_VERSION,
        .flags = 0,
        .last_index = last_index,
        .last_term = last_term,
        .entry_count = lygus_kv_count(kv),
    };

    // Write header
    if (write_all(fd, &hdr, sizeof(hdr)) < 0) {
        ret = LYGUS_ERR_WRITE;
        goto cleanup;
    }
    crc = crc32c_update(crc, (const uint8_t *)&hdr, sizeof(hdr));

    // Write entries
    lygus_kv_iter_t it;
    lygus_kv_iter_init(&it, kv);

    const void *key, *val;
    size_t klen, vlen;

    while (lygus_kv_iter_next(&it, &key, &klen, &val, &vlen)) {
        // Encode klen and vlen as varints
        uint8_t varint_buf[VARINT_MAX_BYTES * 2];
        size_t pos = 0;

        pos += varint_encode(klen, &varint_buf[pos]);
        pos += varint_encode(vlen, &varint_buf[pos]);

        // Write varints
        if (write_all(fd, varint_buf, pos) < 0) {
            ret = LYGUS_ERR_WRITE;
            goto cleanup;
        }
        crc = crc32c_update(crc, varint_buf, pos);

        // Write key
        if (klen > 0) {
            if (write_all(fd, key, klen) < 0) {
                ret = LYGUS_ERR_WRITE;
                goto cleanup;
            }
            crc = crc32c_update(crc, (const uint8_t *)key, klen);
        }

        // Write value
        if (vlen > 0) {
            if (write_all(fd, val, vlen) < 0) {
                ret = LYGUS_ERR_WRITE;
                goto cleanup;
            }
            crc = crc32c_update(crc, (const uint8_t *)val, vlen);
        }
    }

    // Finalize CRC
    crc ^= 0xFFFFFFFF;

    // Write CRC footer (little-endian)
    uint8_t crc_bytes[4] = {
        (crc >> 0) & 0xFF,
        (crc >> 8) & 0xFF,
        (crc >> 16) & 0xFF,
        (crc >> 24) & 0xFF,
    };

    if (write_all(fd, crc_bytes, 4) < 0) {
        ret = LYGUS_ERR_WRITE;
        goto cleanup;
    }

    // Fsync before rename
    if (fsync(fd) < 0) {
        ret = LYGUS_ERR_FSYNC;
        goto cleanup;
    }

    close(fd);
    fd = -1;

    // Atomic rename (on Windows, need to delete target first if exists)
#ifdef _WIN32
    // Windows rename fails if target exists
    unlink(path);
#endif
    if (rename(tmp_path, path) < 0) {
        ret = LYGUS_ERR_IO;
        goto cleanup;
    }

    LOG_INFO_SIMPLE(LYGUS_MODULE_SNAPSHOT, LYGUS_EVENT_SNAPSHOT_DONE,
                    last_term, last_index);

    return LYGUS_OK;

cleanup:
    if (fd >= 0) {
        close(fd);
    }
    unlink(tmp_path);
    return ret;
}

// ============================================================================
// Snapshot Load
// ============================================================================

int snapshot_load(const char *path,
                  lygus_kv_t *kv,
                  uint64_t *out_index,
                  uint64_t *out_term)
{
    if (!path || !kv) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int fd = open(path, O_RDONLY | O_BINARY);
    if (fd < 0) {
        return LYGUS_ERR_OPEN_FILE;
    }

    int ret = LYGUS_OK;
    uint32_t crc = CRC32C_INIT;

    // Read header
    snapshot_hdr_t hdr;
    if (read_all(fd, &hdr, sizeof(hdr)) < 0) {
        ret = LYGUS_ERR_READ;
        goto cleanup;
    }
    crc = crc32c_update(crc, (const uint8_t *)&hdr, sizeof(hdr));

    // Validate header
    if (hdr.magic != SNAPSHOT_MAGIC) {
        ret = LYGUS_ERR_BAD_SNAPSHOT;
        goto cleanup;
    }
    if (hdr.version != SNAPSHOT_VERSION) {
        ret = LYGUS_ERR_BAD_SNAPSHOT;
        goto cleanup;
    }

    // Clear existing KV data
    lygus_kv_clear(kv);

    // Read entries
    for (uint64_t i = 0; i < hdr.entry_count; i++) {
        // Read varints for klen and vlen
        // We need to read byte-by-byte for varints
        uint8_t varint_buf[VARINT_MAX_BYTES];
        uint64_t klen = 0, vlen = 0;

        // Read klen varint
        size_t varint_pos = 0;
        while (varint_pos < VARINT_MAX_BYTES) {
            if (read_all(fd, &varint_buf[varint_pos], 1) < 0) {
                ret = LYGUS_ERR_READ;
                goto cleanup;
            }
            varint_pos++;

            ssize_t decoded = varint_decode(varint_buf, varint_pos, &klen);
            if (decoded > 0) {
                crc = crc32c_update(crc, varint_buf, varint_pos);
                break;
            }
            if (decoded < 0 && decoded != LYGUS_ERR_INCOMPLETE) {
                ret = LYGUS_ERR_MALFORMED;
                goto cleanup;
            }
        }

        // Read vlen varint
        varint_pos = 0;
        while (varint_pos < VARINT_MAX_BYTES) {
            if (read_all(fd, &varint_buf[varint_pos], 1) < 0) {
                ret = LYGUS_ERR_READ;
                goto cleanup;
            }
            varint_pos++;

            ssize_t decoded = varint_decode(varint_buf, varint_pos, &vlen);
            if (decoded > 0) {
                crc = crc32c_update(crc, varint_buf, varint_pos);
                break;
            }
            if (decoded < 0 && decoded != LYGUS_ERR_INCOMPLETE) {
                ret = LYGUS_ERR_MALFORMED;
                goto cleanup;
            }
        }

        // Allocate and read key
        uint8_t *key = NULL;
        if (klen > 0) {
            key = malloc(klen);
            if (!key) {
                ret = LYGUS_ERR_NOMEM;
                goto cleanup;
            }
            if (read_all(fd, key, klen) < 0) {
                free(key);
                ret = LYGUS_ERR_READ;
                goto cleanup;
            }
            crc = crc32c_update(crc, key, klen);
        }

        // Allocate and read value
        uint8_t *val = NULL;
        if (vlen > 0) {
            val = malloc(vlen);
            if (!val) {
                free(key);
                ret = LYGUS_ERR_NOMEM;
                goto cleanup;
            }
            if (read_all(fd, val, vlen) < 0) {
                free(key);
                free(val);
                ret = LYGUS_ERR_READ;
                goto cleanup;
            }
            crc = crc32c_update(crc, val, vlen);
        }

        // Insert into KV
        ret = lygus_kv_put(kv, key, klen, val, vlen);

        free(key);
        free(val);

        if (ret < 0) {
            goto cleanup;
        }
    }

    // Read and verify CRC
    uint8_t crc_bytes[4];
    if (read_all(fd, crc_bytes, 4) < 0) {
        ret = LYGUS_ERR_READ;
        goto cleanup;
    }

    uint32_t stored_crc = ((uint32_t)crc_bytes[0] << 0)
                        | ((uint32_t)crc_bytes[1] << 8)
                        | ((uint32_t)crc_bytes[2] << 16)
                        | ((uint32_t)crc_bytes[3] << 24);

    // Finalize computed CRC
    crc ^= 0xFFFFFFFF;

    if (stored_crc != crc) {
        // CRC mismatch - clear KV and return error
        lygus_kv_clear(kv);
        ret = LYGUS_ERR_CORRUPT;
        goto cleanup;
    }

    // Return metadata
    if (out_index) *out_index = hdr.last_index;
    if (out_term) *out_term = hdr.last_term;

    LOG_INFO_SIMPLE(LYGUS_MODULE_SNAPSHOT, LYGUS_EVENT_SNAPSHOT_LOAD,
                    hdr.last_term, hdr.last_index);

    ret = LYGUS_OK;

cleanup:
    close(fd);
    return ret;
}

// ============================================================================
// Snapshot Header Read
// ============================================================================

int snapshot_read_header(const char *path,
                         uint64_t *out_index,
                         uint64_t *out_term,
                         uint64_t *out_count)
{
    if (!path) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int fd = open(path, O_RDONLY | O_BINARY);
    if (fd < 0) {
        return LYGUS_ERR_OPEN_FILE;
    }

    snapshot_hdr_t hdr;
    int ret = LYGUS_OK;

    if (read_all(fd, &hdr, sizeof(hdr)) < 0) {
        ret = LYGUS_ERR_READ;
        goto cleanup;
    }

    if (hdr.magic != SNAPSHOT_MAGIC) {
        ret = LYGUS_ERR_BAD_SNAPSHOT;
        goto cleanup;
    }

    if (hdr.version != SNAPSHOT_VERSION) {
        ret = LYGUS_ERR_BAD_SNAPSHOT;
        goto cleanup;
    }

    if (out_index) *out_index = hdr.last_index;
    if (out_term) *out_term = hdr.last_term;
    if (out_count) *out_count = hdr.entry_count;

cleanup:
    close(fd);
    return ret;
}

// ============================================================================
// Async Snapshot
// On Unix: uses fork() for COW semantics (non-blocking)
// On Windows: falls back to synchronous write (blocking, but still works)
// ============================================================================

#ifdef _WIN32

// Windows: synchronous fallback (no fork available)
int snapshot_create_async(lygus_kv_t *kv,
                          uint64_t last_index,
                          uint64_t last_term,
                          const char *path,
                          snapshot_async_t *out_async)
{
    if (!kv || !path || !out_async) {
        return LYGUS_ERR_INVALID_ARG;
    }

    LOG_INFO_SIMPLE(LYGUS_MODULE_SNAPSHOT, LYGUS_EVENT_SNAPSHOT_START,
                    last_term, last_index);

    // On Windows, we do a synchronous write
    // This blocks but maintains correctness
    int ret = snapshot_write(kv, last_index, last_term, path);

    // Mark as "already complete"
    out_async->completed = 1;
    out_async->success = (ret == LYGUS_OK) ? 1 : 0;
    out_async->index = last_index;
    out_async->term = last_term;
    strncpy(out_async->path, path, sizeof(out_async->path) - 1);
    out_async->path[sizeof(out_async->path) - 1] = '\0';

    return LYGUS_OK;
}

int snapshot_poll(snapshot_async_t *async,
                  int *out_done,
                  int *out_success)
{
    if (!async || !out_done || !out_success) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // On Windows, always complete immediately
    *out_done = async->completed;
    *out_success = async->success;

    return LYGUS_OK;
}

int snapshot_wait(snapshot_async_t *async,
                  int *out_success)
{
    if (!async || !out_success) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // On Windows, already complete
    *out_success = async->success;

    return LYGUS_OK;
}

#else  // Unix

int snapshot_create_async(lygus_kv_t *kv,
                          uint64_t last_index,
                          uint64_t last_term,
                          const char *path,
                          snapshot_async_t *out_async)
{
    if (!kv || !path || !out_async) {
        return LYGUS_ERR_INVALID_ARG;
    }

    LOG_INFO_SIMPLE(LYGUS_MODULE_SNAPSHOT, LYGUS_EVENT_SNAPSHOT_START,
                    last_term, last_index);

    pid_t pid = fork();

    if (pid < 0) {
        // Fork failed
        return LYGUS_ERR_INTERNAL;
    }

    if (pid == 0) {
        // Child process - has COW copy of address space
        int ret = snapshot_write(kv, last_index, last_term, path);
        _exit(ret == LYGUS_OK ? 0 : 1);
    }

    // Parent process - store async state
    out_async->pid = pid;
    out_async->index = last_index;
    out_async->term = last_term;
    strncpy(out_async->path, path, sizeof(out_async->path) - 1);
    out_async->path[sizeof(out_async->path) - 1] = '\0';

    return LYGUS_OK;
}

int snapshot_poll(snapshot_async_t *async,
                  int *out_done,
                  int *out_success)
{
    if (!async || !out_done || !out_success) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (async->pid == 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int status;
    pid_t result = waitpid(async->pid, &status, WNOHANG);

    if (result == 0) {
        // Still running
        *out_done = 0;
        *out_success = 0;
        return LYGUS_OK;
    }

    if (result < 0) {
        // Error
        return LYGUS_ERR_INTERNAL;
    }

    // Child finished
    *out_done = 1;
    *out_success = (WIFEXITED(status) && WEXITSTATUS(status) == 0) ? 1 : 0;

    // Clear pid so we don't wait again
    async->pid = 0;

    return LYGUS_OK;
}

int snapshot_wait(snapshot_async_t *async,
                  int *out_success)
{
    if (!async || !out_success) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (async->pid == 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int status;
    pid_t result = waitpid(async->pid, &status, 0);

    if (result < 0) {
        return LYGUS_ERR_INTERNAL;
    }

    *out_success = (WIFEXITED(status) && WEXITSTATUS(status) == 0) ? 1 : 0;

    // Clear pid
    async->pid = 0;

    return LYGUS_OK;
}

#endif  // _WIN32

// ============================================================================
// Snapshot Path Management
// ============================================================================

int snapshot_path_from_index(const char *dir,
                             uint64_t index,
                             uint64_t term,
                             char *out,
                             size_t out_len)
{
    if (!dir || !out || out_len == 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

#ifdef _WIN32
    int n = snprintf(out, out_len, "%s\\snap-%016llx-%016llx.dat",
                     dir,
                     (unsigned long long)index,
                     (unsigned long long)term);
#else
    int n = snprintf(out, out_len, "%s/snap-%016llx-%016llx.dat",
                     dir,
                     (unsigned long long)index,
                     (unsigned long long)term);
#endif

    if (n < 0 || (size_t)n >= out_len) {
        return LYGUS_ERR_INVALID_ARG;
    }

    return LYGUS_OK;
}

// ============================================================================
// Directory Scanning (platform-specific)
// ============================================================================

#ifdef _WIN32

int snapshot_find_latest(const char *dir,
                         char *out_path,
                         uint64_t *out_index,
                         uint64_t *out_term)
{
    if (!dir || !out_path) {
        return LYGUS_ERR_INVALID_ARG;
    }

    char pattern[512];
    snprintf(pattern, sizeof(pattern), "%s\\snap-*.dat", dir);

    WIN32_FIND_DATAA ffd;
    HANDLE hFind = FindFirstFileA(pattern, &ffd);

    if (hFind == INVALID_HANDLE_VALUE) {
        return LYGUS_ERR_KEY_NOT_FOUND;
    }

    uint64_t best_index = 0;
    uint64_t best_term = 0;
    char best_name[256] = {0};

    do {
        if (!(ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
            uint64_t index, term;
            if (sscanf(ffd.cFileName, "snap-%llx-%llx.dat",
                       (unsigned long long *)&index,
                       (unsigned long long *)&term) == 2) {
                if (index > best_index ||
                    (index == best_index && term > best_term)) {
                    best_index = index;
                    best_term = term;
                    strncpy(best_name, ffd.cFileName, sizeof(best_name) - 1);
                }
            }
        }
    } while (FindNextFileA(hFind, &ffd) != 0);

    FindClose(hFind);

    if (best_name[0] == '\0') {
        return LYGUS_ERR_KEY_NOT_FOUND;
    }

    snprintf(out_path, 256, "%s\\%s", dir, best_name);

    if (out_index) *out_index = best_index;
    if (out_term) *out_term = best_term;

    return LYGUS_OK;
}

int snapshot_purge_old(const char *dir, int keep)
{
    if (!dir || keep < 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    char pattern[512];
    snprintf(pattern, sizeof(pattern), "%s\\snap-*.dat", dir);

    WIN32_FIND_DATAA ffd;
    HANDLE hFind = FindFirstFileA(pattern, &ffd);

    if (hFind == INVALID_HANDLE_VALUE) {
        return LYGUS_OK;  // No snapshots
    }

    // First pass: count snapshots
    int count = 0;
    do {
        if (!(ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
            uint64_t index, term;
            if (sscanf(ffd.cFileName, "snap-%llx-%llx.dat",
                       (unsigned long long *)&index,
                       (unsigned long long *)&term) == 2) {
                count++;
            }
        }
    } while (FindNextFileA(hFind, &ffd) != 0);

    FindClose(hFind);

    if (count <= keep) {
        return LYGUS_OK;
    }

    // Second pass: collect snapshots
    typedef struct {
        uint64_t index;
        uint64_t term;
        char name[256];
    } snap_info_t;

    snap_info_t *snaps = malloc(count * sizeof(snap_info_t));
    if (!snaps) {
        return LYGUS_ERR_NOMEM;
    }

    hFind = FindFirstFileA(pattern, &ffd);
    int idx = 0;

    do {
        if (!(ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
            uint64_t index, term;
            if (sscanf(ffd.cFileName, "snap-%llx-%llx.dat",
                       (unsigned long long *)&index,
                       (unsigned long long *)&term) == 2) {
                snaps[idx].index = index;
                snaps[idx].term = term;
                strncpy(snaps[idx].name, ffd.cFileName, sizeof(snaps[idx].name) - 1);
                idx++;
            }
        }
    } while (FindNextFileA(hFind, &ffd) != 0 && idx < count);

    FindClose(hFind);

    // Sort by index descending
    for (int i = 0; i < idx - 1; i++) {
        for (int j = 0; j < idx - i - 1; j++) {
            if (snaps[j].index < snaps[j + 1].index) {
                snap_info_t tmp = snaps[j];
                snaps[j] = snaps[j + 1];
                snaps[j + 1] = tmp;
            }
        }
    }

    // Delete all but first 'keep'
    for (int i = keep; i < idx; i++) {
        char path[512];
        snprintf(path, sizeof(path), "%s\\%s", dir, snaps[i].name);
        unlink(path);
    }

    free(snaps);
    return LYGUS_OK;
}

#else  // Unix

int snapshot_find_latest(const char *dir,
                         char *out_path,
                         uint64_t *out_index,
                         uint64_t *out_term)
{
    if (!dir || !out_path) {
        return LYGUS_ERR_INVALID_ARG;
    }

    DIR *d = opendir(dir);
    if (!d) {
        return LYGUS_ERR_IO;
    }

    uint64_t best_index = 0;
    uint64_t best_term = 0;
    char best_name[256] = {0};

    struct dirent *entry;
    while ((entry = readdir(d)) != NULL) {
        uint64_t index, term;

        // Parse snap-{index}-{term}.dat
        if (sscanf(entry->d_name, "snap-%llx-%llx.dat",
                   (unsigned long long *)&index,
                   (unsigned long long *)&term) == 2) {
            // Higher index wins, or same index with higher term
            if (index > best_index ||
                (index == best_index && term > best_term)) {
                best_index = index;
                best_term = term;
                strncpy(best_name, entry->d_name, sizeof(best_name) - 1);
            }
        }
    }

    closedir(d);

    if (best_name[0] == '\0') {
        return LYGUS_ERR_KEY_NOT_FOUND;
    }

    snprintf(out_path, 256, "%s/%s", dir, best_name);

    if (out_index) *out_index = best_index;
    if (out_term) *out_term = best_term;

    return LYGUS_OK;
}

int snapshot_purge_old(const char *dir, int keep)
{
    if (!dir || keep < 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    DIR *d = opendir(dir);
    if (!d) {
        return LYGUS_ERR_IO;
    }

    // Count snapshots first
    int count = 0;
    struct dirent *entry;
    while ((entry = readdir(d)) != NULL) {
        uint64_t index, term;
        if (sscanf(entry->d_name, "snap-%llx-%llx.dat",
                   (unsigned long long *)&index,
                   (unsigned long long *)&term) == 2) {
            count++;
        }
    }

    if (count <= keep) {
        closedir(d);
        return LYGUS_OK;  // Nothing to purge
    }

    // Collect all snapshots
    rewinddir(d);

    typedef struct {
        uint64_t index;
        uint64_t term;
        char name[256];
    } snap_info_t;

    snap_info_t *snaps = malloc(count * sizeof(snap_info_t));
    if (!snaps) {
        closedir(d);
        return LYGUS_ERR_NOMEM;
    }

    int idx = 0;
    while ((entry = readdir(d)) != NULL && idx < count) {
        uint64_t index, term;
        if (sscanf(entry->d_name, "snap-%llx-%llx.dat",
                   (unsigned long long *)&index,
                   (unsigned long long *)&term) == 2) {
            snaps[idx].index = index;
            snaps[idx].term = term;
            strncpy(snaps[idx].name, entry->d_name, sizeof(snaps[idx].name) - 1);
            idx++;
        }
    }

    closedir(d);

    // Sort by index descending (simple bubble sort, count is small)
    for (int i = 0; i < idx - 1; i++) {
        for (int j = 0; j < idx - i - 1; j++) {
            if (snaps[j].index < snaps[j + 1].index) {
                snap_info_t tmp = snaps[j];
                snaps[j] = snaps[j + 1];
                snaps[j + 1] = tmp;
            }
        }
    }

    // Delete all but the first 'keep' snapshots
    for (int i = keep; i < idx; i++) {
        char path[512];
        snprintf(path, sizeof(path), "%s/%s", dir, snaps[i].name);
        unlink(path);
    }

    free(snaps);
    return LYGUS_OK;
}

#endif  // _WIN32