// spoiler_check.c
// "Am I putting a spoiler on a tractor?"

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <time.h>
#include <string.h>

static uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
}

int main(int argc, char **argv) {
    const char *path = argc > 1 ? argv[1] : "./test.bin";
    int iterations = 1000;

    printf("\n=== DISK HONESTY CHECK ===\n\n");
    printf("Path: %s\n", path);
    printf("Iterations: %d\n\n", iterations);

    char buf[4096];
    memset(buf, 'X', sizeof(buf));

    int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0666);
    if (fd < 0) {
        perror("open");
        return 1;
    }

    // Warm up
    for (int i = 0; i < 10; i++) {
        write(fd, buf, sizeof(buf));
        fsync(fd);
    }
    lseek(fd, 0, SEEK_SET);

    // Test 1: write + fsync (your current code)
    uint64_t start = now_us();
    for (int i = 0; i < iterations; i++) {
        write(fd, buf, sizeof(buf));
        fsync(fd);
    }
    uint64_t write_fsync_us = now_us() - start;

    lseek(fd, 0, SEEK_SET);

    // Test 2: write only (no fsync)
    start = now_us();
    for (int i = 0; i < iterations; i++) {
        write(fd, buf, sizeof(buf));
    }
    fsync(fd);  // One final fsync
    uint64_t write_only_us = now_us() - start;

    lseek(fd, 0, SEEK_SET);

    // Test 3: batched (10 writes per fsync)
    start = now_us();
    for (int i = 0; i < iterations; i++) {
        write(fd, buf, sizeof(buf));
        if (i % 10 == 9) fsync(fd);
    }
    uint64_t batched_us = now_us() - start;

    close(fd);
    unlink(path);

    // Results
    printf("┌─────────────────────────────────────────────────┐\n");
    printf("│ TEST                    TOTAL      PER-OP      │\n");
    printf("├─────────────────────────────────────────────────┤\n");
    printf("│ write + fsync (each)    %6.1fms   %6.3fms     │\n",
           write_fsync_us / 1000.0, write_fsync_us / 1000.0 / iterations);
    printf("│ write only (1 fsync)    %6.1fms   %6.3fms     │\n",
           write_only_us / 1000.0, write_only_us / 1000.0 / iterations);
    printf("│ batched (10:1)          %6.1fms   %6.3fms     │\n",
           batched_us / 1000.0, batched_us / 1000.0 / iterations);
    printf("└─────────────────────────────────────────────────┘\n");

    printf("\n");
    printf("fsync cost:    %.3fms (this is your bottleneck)\n",
           (write_fsync_us - write_only_us) / 1000.0 / iterations);
    printf("syscall cost:  ~0.001ms (io_uring saves this)\n");
    printf("\n");

    if ((write_fsync_us / iterations) > 500) {
        printf("VERDICT: 🚜 You are plowing a field. Spoiler won't help.\n");
        printf("         Focus on batching, not io_uring.\n");
    } else {
        printf("VERDICT: 🏎️  NVMe detected. io_uring might help.\n");
        printf("         But benchmark Raft first.\n");
    }

    printf("\n");
    return 0;
}