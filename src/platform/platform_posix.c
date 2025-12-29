/**
 * platform_posix.c - POSIX implementation of platform abstraction
 *
 * Covers: Linux, macOS, BSD, and other Unix-like systems
 */

#define _GNU_SOURCE  // For pread/pwrite on some systems

#include "platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/file.h>
#include <sys/wait.h>
#include <sys/stat.h>

#ifdef __linux__
#include <sys/syscall.h>
#endif

// ============================================================================
// Error Helpers
// ============================================================================

int lygus_errno_is_disk_full(void) {
    return errno == ENOSPC;
}

// ============================================================================
// File Operations
// ============================================================================

lygus_fd_t lygus_file_open(const char *path, int flags, int mode) {
    if (!path) return LYGUS_INVALID_FD;

    int posix_flags = 0;

    // Convert portable flags to POSIX
    if (flags & LYGUS_O_RDONLY) posix_flags |= O_RDONLY;
    if (flags & LYGUS_O_WRONLY) posix_flags |= O_WRONLY;
    if (flags & LYGUS_O_RDWR)   posix_flags |= O_RDWR;
    if (flags & LYGUS_O_CREAT)  posix_flags |= O_CREAT;
    if (flags & LYGUS_O_EXCL)   posix_flags |= O_EXCL;
    if (flags & LYGUS_O_TRUNC)  posix_flags |= O_TRUNC;
    if (flags & LYGUS_O_APPEND) posix_flags |= O_APPEND;

    return open(path, posix_flags, mode);
}

int lygus_file_close(lygus_fd_t fd) {
    if (fd < 0) return -1;
    return close(fd);
}

int64_t lygus_file_read(lygus_fd_t fd, void *buf, size_t len) {
    if (fd < 0 || !buf) return -1;
    return (int64_t)read(fd, buf, len);
}

int64_t lygus_file_write(lygus_fd_t fd, const void *buf, size_t len) {
    if (fd < 0 || !buf) return -1;
    return (int64_t)write(fd, buf, len);
}

int64_t lygus_file_pread(lygus_fd_t fd, void *buf, size_t len, uint64_t offset) {
    if (fd < 0 || !buf) return -1;
    return (int64_t)pread(fd, buf, len, (off_t)offset);
}

int64_t lygus_file_pwrite(lygus_fd_t fd, const void *buf, size_t len, uint64_t offset) {
    if (fd < 0 || !buf) return -1;
    return (int64_t)pwrite(fd, buf, len, (off_t)offset);
}

int64_t lygus_file_seek(lygus_fd_t fd, int64_t offset, int whence) {
    if (fd < 0) return -1;

    int posix_whence;
    switch (whence) {
        case LYGUS_SEEK_SET: posix_whence = SEEK_SET; break;
        case LYGUS_SEEK_CUR: posix_whence = SEEK_CUR; break;
        case LYGUS_SEEK_END: posix_whence = SEEK_END; break;
        default: return -1;
    }

    return (int64_t)lseek(fd, (off_t)offset, posix_whence);
}

int lygus_file_sync(lygus_fd_t fd) {
    if (fd < 0) return -1;

#if defined(_POSIX_SYNCHRONIZED_IO) && _POSIX_SYNCHRONIZED_IO > 0
    return fdatasync(fd);
#else
    return fsync(fd);
#endif
}

int lygus_file_truncate(lygus_fd_t fd, uint64_t size) {
    if (fd < 0) return -1;
    return ftruncate(fd, (off_t)size);
}

int lygus_file_lock(lygus_fd_t fd, int flags) {
    if (fd < 0) return -1;

    int flock_op = 0;

    if (flags & LYGUS_LOCK_UN) {
        flock_op = LOCK_UN;
    } else if (flags & LYGUS_LOCK_EX) {
        flock_op = LOCK_EX;
    } else if (flags & LYGUS_LOCK_SH) {
        flock_op = LOCK_SH;
    } else {
        return -1;
    }

    if (flags & LYGUS_LOCK_NB) {
        flock_op |= LOCK_NB;
    }

    return flock(fd, flock_op);
}

int lygus_file_unlock(lygus_fd_t fd) {
    return lygus_file_lock(fd, LYGUS_LOCK_UN);
}

int64_t lygus_file_size(lygus_fd_t fd) {
    if (fd < 0) return -1;

    struct stat st;
    if (fstat(fd, &st) < 0) return -1;

    return (int64_t)st.st_size;
}

/**
 * Force a visibility barrier for file metadata on POSIX.
 */
int lygus_file_barrier(lygus_fd_t fd) {
    if (fd < 0) return -1;

    struct stat st;
    // forces the kernel to refresh the internal i_size attribute.
    if (fstat(fd, &st) < 0) {
        return -1;
    }

    return 0;
}

// ============================================================================
// Filesystem Operations
// ============================================================================

int lygus_mkdir(const char *path, int mode) {
    if (!path) return -1;

    int ret = mkdir(path, (mode_t)mode);
    if (ret < 0 && errno == EEXIST) {
        return 0;  // Directory already exists is OK
    }
    return ret;
}

int lygus_unlink(const char *path) {
    if (!path) return -1;
    return unlink(path);
}

int lygus_rename(const char *old_path, const char *new_path) {
    if (!old_path || !new_path) return -1;
    return rename(old_path, new_path);
}

int lygus_path_exists(const char *path) {
    if (!path) return 0;
    return access(path, F_OK) == 0;
}

// ============================================================================
// Directory Iteration
// ============================================================================

struct lygus_dir {
    DIR *dir;
    struct dirent *entry;
};

lygus_dir_t* lygus_dir_open(const char *path) {
    if (!path) return NULL;

    DIR *d = opendir(path);
    if (!d) return NULL;

    lygus_dir_t *dir = malloc(sizeof(lygus_dir_t));
    if (!dir) {
        closedir(d);
        return NULL;
    }

    dir->dir = d;
    dir->entry = NULL;
    return dir;
}

const char* lygus_dir_read(lygus_dir_t *dir) {
    if (!dir || !dir->dir) return NULL;

    while ((dir->entry = readdir(dir->dir)) != NULL) {
        // Skip . and ..
        if (dir->entry->d_name[0] == '.') {
            if (dir->entry->d_name[1] == '\0') continue;
            if (dir->entry->d_name[1] == '.' && dir->entry->d_name[2] == '\0') continue;
        }
        return dir->entry->d_name;
    }

    return NULL;
}

void lygus_dir_close(lygus_dir_t *dir) {
    if (!dir) return;
    if (dir->dir) closedir(dir->dir);
    free(dir);
}

// ============================================================================
// Path Utilities
// ============================================================================

const char* lygus_path_separator(void) {
    return "/";
}

int lygus_path_join(char *out, size_t out_len, const char *dir, const char *file) {
    if (!out || !dir || !file || out_len == 0) return -1;

    int n = snprintf(out, out_len, "%s/%s", dir, file);
    if (n < 0 || (size_t)n >= out_len) return -1;

    return 0;
}

// ============================================================================
// Async Process (fork-based)
// ============================================================================

struct lygus_async_proc {
    pid_t pid;
    int   completed;
    int   success;
};

int lygus_async_fork_supported(void) {
    return 1;
}

lygus_async_proc_t* lygus_async_fork(int (*func)(void *ctx), void *ctx, int *is_child) {
    if (!func || !is_child) return NULL;

    *is_child = 0;

    pid_t pid = fork();

    if (pid < 0) return NULL;

    if (pid == 0) {
        // Child process
        *is_child = 1;
        int ret = func(ctx);
        _exit(ret == 0 ? 0 : 1);
    }

    // Parent process
    lygus_async_proc_t *proc = malloc(sizeof(lygus_async_proc_t));
    if (!proc) return NULL;

    proc->pid = pid;
    proc->completed = 0;
    proc->success = 0;

    return proc;
}

int lygus_async_poll(lygus_async_proc_t *proc, int *done, int *success) {
    if (!proc || !done || !success) return -1;

    if (proc->completed) {
        *done = 1;
        *success = proc->success;
        return 0;
    }

    int status;
    pid_t result = waitpid(proc->pid, &status, WNOHANG);

    if (result == 0) {
        *done = 0;
        *success = 0;
        return 0;
    }

    if (result < 0) return -1;

    proc->completed = 1;
    proc->success = (WIFEXITED(status) && WEXITSTATUS(status) == 0) ? 1 : 0;

    *done = 1;
    *success = proc->success;
    return 0;
}

int lygus_async_wait(lygus_async_proc_t *proc, int *success) {
    if (!proc || !success) return -1;

    if (proc->completed) {
        *success = proc->success;
        return 0;
    }

    int status;
    pid_t result = waitpid(proc->pid, &status, 0);

    if (result < 0) return -1;

    proc->completed = 1;
    proc->success = (WIFEXITED(status) && WEXITSTATUS(status) == 0) ? 1 : 0;

    *success = proc->success;
    return 0;
}

void lygus_async_free(lygus_async_proc_t *proc) {
    free(proc);
}

// ============================================================================
// Threading Utilities
// ============================================================================

uint32_t lygus_thread_id(void) {
#ifdef __linux__
    return (uint32_t)syscall(SYS_gettid);
#elif defined(__APPLE__)
    uint64_t tid;
    pthread_threadid_np(NULL, &tid);
    return (uint32_t)tid;
#else
    return (uint32_t)(uintptr_t)pthread_self();
#endif
}

void lygus_sleep_us(uint64_t us) {
    usleep((useconds_t)us);
}

// ============================================================================
// Event Notification
// ============================================================================

#ifdef __linux__
#include <sys/eventfd.h>
#endif

struct lygus_notify {
#ifdef __linux__
    int efd;
#else
    int read_fd;
    int write_fd;
#endif
};

lygus_notify_t *lygus_notify_create(void) {
    lygus_notify_t *notify = malloc(sizeof(lygus_notify_t));
    if (!notify) return NULL;

#ifdef __linux__
    notify->efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (notify->efd < 0) {
        free(notify);
        return NULL;
    }
#else
    int pipefd[2];
    if (pipe(pipefd) < 0) {
        free(notify);
        return NULL;
    }

    // Set non-blocking on both ends
    fcntl(pipefd[0], F_SETFL, O_NONBLOCK);
    fcntl(pipefd[1], F_SETFL, O_NONBLOCK);
    fcntl(pipefd[0], F_SETFD, FD_CLOEXEC);
    fcntl(pipefd[1], F_SETFD, FD_CLOEXEC);

    notify->read_fd = pipefd[0];
    notify->write_fd = pipefd[1];
#endif

    return notify;
}

void lygus_notify_destroy(lygus_notify_t *notify) {
    if (!notify) return;

#ifdef __linux__
    if (notify->efd >= 0) close(notify->efd);
#else
    if (notify->read_fd >= 0) close(notify->read_fd);
    if (notify->write_fd >= 0) close(notify->write_fd);
#endif

    free(notify);
}

lygus_fd_t lygus_notify_fd(const lygus_notify_t *notify) {
    if (!notify) return LYGUS_INVALID_FD;

#ifdef __linux__
    return notify->efd;
#else
    return notify->read_fd;
#endif
}

int lygus_notify_signal(lygus_notify_t *notify) {
    if (!notify) return -1;

#ifdef __linux__
    uint64_t val = 1;
    // write() to eventfd is async-signal-safe
    if (write(notify->efd, &val, sizeof(val)) < 0) {
        if (errno == EAGAIN) return 0;  // Already signaled, that's fine
        return -1;
    }
#else
    char c = 1;
    if (write(notify->write_fd, &c, 1) < 0) {
        if (errno == EAGAIN) return 0;  // Pipe full, already signaled
        return -1;
    }
#endif

    return 0;
}

int lygus_notify_clear(lygus_notify_t *notify) {
    if (!notify) return -1;

#ifdef __linux__
    uint64_t val;
    while (read(notify->efd, &val, sizeof(val)) > 0) {
        // Drain all pending signals
    }
#else
    char buf[64];
    while (read(notify->read_fd, buf, sizeof(buf)) > 0) {
        // Drain pipe
    }
#endif

    return 0;
}

// ============================================================================
// Time Utilities
// ============================================================================

uint64_t lygus_monotonic_ns(void) {
    struct timespec ts;

#ifdef CLOCK_MONOTONIC
    clock_gettime(CLOCK_MONOTONIC, &ts);
#else
    clock_gettime(CLOCK_REALTIME, &ts);
#endif

    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

uint64_t lygus_realtime_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}