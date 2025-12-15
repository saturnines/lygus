// tests/test_wal_crash.c
//
// Crash harness for WAL testing.
// Uses fork() + SIGKILL to simulate real crashes at random points.
//
// Usage: ./test_wal_crash [--iterations=N]
//
// This is NOT a unit test.
// the WAL writer at random points and verifies recovery correctness.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <dirent.h>

#include "../storage/wal/wal.h"
#include "../storage/wal/recovery.h"
#include "../util/logging.h"

// ============================================================================
// Configuration
// ============================================================================

#define TEST_DIR "./crash_test_data"
#define DEFAULT_ITERATIONS 100
#define MIN_KILL_DELAY_MS 5
#define MAX_KILL_DELAY_MS 200
#define MAX_ENTRIES_PER_RUN 500

// ============================================================================
// Colors for output
// ============================================================================

#define RED     "\x1b[31m"
#define GREEN   "\x1b[32m"
#define YELLOW  "\x1b[33m"
#define BLUE    "\x1b[34m"
#define RESET   "\x1b[0m"

// ============================================================================
// Shared Memory Witness
//
// This structure lives in shared memory so the parent can see what the
// child was doing when it died. The child updates this BEFORE and AFTER
// each wal_put() call.
// ============================================================================

typedef struct {
    // Updated BEFORE wal_put() - "I'm about to try writing entry N"
    uint64_t attempting_index;

    // Updated AFTER wal_put() returns OK - "Entry N is now durable"
    uint64_t completed_index;

    // The term we're using (constant per run)
    uint64_t term;

    // Track what values we wrote so we can verify content
    // completed_index tells us how many of these are valid
    // We store a simple checksum of each entry's value
    uint32_t entry_checksums[MAX_ENTRIES_PER_RUN];

    // Flag to indicate child started successfully
    int child_started;

    // Any error the child encountered
    int child_error;

} crash_witness_t;

// ============================================================================
// Helpers
// ============================================================================

static void cleanup_test_dir(void) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DIR);
    system(cmd);
    mkdir(TEST_DIR, 0755);
}

static uint32_t simple_checksum(const void *data, size_t len) {
    const uint8_t *bytes = (const uint8_t *)data;
    uint32_t sum = 0;
    for (size_t i = 0; i < len; i++) {
        sum = sum * 31 + bytes[i];
    }
    return sum;
}

static int random_range(int min, int max) {
    return min + (rand() % (max - min + 1));
}

static crash_witness_t* create_shared_witness(void) {
    // Create anonymous shared memory that survives across fork
    crash_witness_t *witness = mmap(
        NULL,
        sizeof(crash_witness_t),
        PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANONYMOUS,
        -1,
        0
    );

    if (witness == MAP_FAILED) {
        perror("mmap failed");
        return NULL;
    }

    memset(witness, 0, sizeof(crash_witness_t));
    return witness;
}

static void destroy_shared_witness(crash_witness_t *witness) {
    if (witness && witness != MAP_FAILED) {
        munmap(witness, sizeof(crash_witness_t));
    }
}

// ============================================================================
// Child Process: Write entries until killed
// ============================================================================

static void child_writer(crash_witness_t *witness) {
    // Open WAL
    wal_opts_t opts = {
        .data_dir = TEST_DIR,
        .zstd_level = 3,
        .fsync_bytes = 0,
        .fsync_interval_us = 0,
        .on_recover = NULL,
        .user_data = NULL,
    };

    wal_t *w = wal_open(&opts);
    if (!w) {
        witness->child_error = errno ? errno : -1;
        _exit(1);
    }

    witness->child_started = 1;
    witness->term = 1;

    // Write entries until we get killed
    for (uint64_t i = 1; i <= MAX_ENTRIES_PER_RUN; i++) {
        // Build a value that we can verify later
        char key[64];
        char val[128];
        snprintf(key, sizeof(key), "key_%llu", (unsigned long long)i);
        snprintf(val, sizeof(val), "value_%llu_padding_for_size", (unsigned long long)i);

        // BEFORE: Record that we're attempting this entry
        // This write is visible to parent via shared memory
        witness->attempting_index = i;

        // Store the checksum so parent can verify content
        witness->entry_checksums[i - 1] = simple_checksum(val, strlen(val));

        // Memory barrier to ensure writes are visible
        __sync_synchronize();

        // THE ACTUAL WRITE - we might get killed anywhere in here
        int ret = wal_put(w, i, witness->term, key, strlen(key), val, strlen(val));

        if (ret != LYGUS_OK) {
            // This is actually fine - might get killed during write
            // But if we're still alive and got an error, note it
            witness->child_error = ret;
            _exit(1);
        }

        // Memory barrier before updating completed
        __sync_synchronize();

        // AFTER: Record that this entry is now durable
        witness->completed_index = i;

        // Memory barrier to ensure parent sees update
        __sync_synchronize();

        // Small random delay to create variety in kill timing
        if (rand() % 10 == 0) {
            usleep(random_range(100, 1000));
        }
    }

    // If we get here, we wrote everything without being killed
    // Close cleanly
    wal_close(w);
    _exit(0);
}

// ============================================================================
// Recovery Verification
// ============================================================================

typedef struct {
    uint64_t count;
    uint64_t highest_index;
    uint64_t highest_term;
    uint64_t *recovered_indices;  // Array of indices we recovered
    uint32_t *recovered_checksums; // Checksums of recovered values
    size_t capacity;
} verify_state_t;

static int verify_callback(const wal_entry_t *entry, void *user_data) {
    verify_state_t *state = (verify_state_t *)user_data;

    // Grow arrays if needed
    if (state->count >= state->capacity) {
        size_t new_cap = state->capacity * 2;
        state->recovered_indices = realloc(state->recovered_indices,
                                            new_cap * sizeof(uint64_t));
        state->recovered_checksums = realloc(state->recovered_checksums,
                                              new_cap * sizeof(uint32_t));
        state->capacity = new_cap;
    }

    state->recovered_indices[state->count] = entry->index;
    state->recovered_checksums[state->count] = simple_checksum(entry->val, entry->vlen);

    if (entry->index > state->highest_index) {
        state->highest_index = entry->index;
    }
    if (entry->term > state->highest_term) {
        state->highest_term = entry->term;
    }

    state->count++;
    return LYGUS_OK;
}

// ============================================================================
// Single Crash Test Iteration
// ============================================================================

typedef struct {
    int iteration;
    int passed;
    const char *failure_reason;
    uint64_t attempted;
    uint64_t completed;
    uint64_t recovered;
} iteration_result_t;

static iteration_result_t run_crash_iteration(int iteration) {
    iteration_result_t result = {
        .iteration = iteration,
        .passed = 0,
        .failure_reason = NULL,
        .attempted = 0,
        .completed = 0,
        .recovered = 0,
    };

    // Clean slate
    cleanup_test_dir();

    // Create shared witness
    crash_witness_t *witness = create_shared_witness();
    if (!witness) {
        result.failure_reason = "failed to create shared memory";
        return result;
    }

    // Fork child
    pid_t pid = fork();

    if (pid < 0) {
        result.failure_reason = "fork failed";
        destroy_shared_witness(witness);
        return result;
    }

    if (pid == 0) {
        // === CHILD PROCESS ===
        child_writer(witness);
        _exit(0);  // Should not reach here
    }

    // === PARENT PROCESS ===

    // Wait for child to start
    int wait_count = 0;
    while (!witness->child_started && wait_count < 100) {
        usleep(1000);  // 1ms
        wait_count++;
    }

    if (!witness->child_started) {
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        result.failure_reason = "child failed to start";
        destroy_shared_witness(witness);
        return result;
    }

    // Random delay before killing
    int kill_delay_ms = random_range(MIN_KILL_DELAY_MS, MAX_KILL_DELAY_MS);
    usleep(kill_delay_ms * 1000);

    // KILL THE CHILD - no mercy, no cleanup
    kill(pid, SIGKILL);

    // Wait for child to actually die
    int status;
    waitpid(pid, &status, 0);

    // Record what the child was doing when it died
    result.attempted = witness->attempting_index;
    result.completed = witness->completed_index;

    // Now run recovery and verify
    verify_state_t verify = {
        .count = 0,
        .highest_index = 0,
        .highest_term = 0,
        .recovered_indices = malloc(MAX_ENTRIES_PER_RUN * sizeof(uint64_t)),
        .recovered_checksums = malloc(MAX_ENTRIES_PER_RUN * sizeof(uint32_t)),
        .capacity = MAX_ENTRIES_PER_RUN,
    };

    wal_opts_t opts = {
        .data_dir = TEST_DIR,
        .zstd_level = 3,
        .fsync_bytes = 0,
        .fsync_interval_us = 0,
        .on_recover = verify_callback,
        .user_data = &verify,
    };

    wal_t *w = wal_open(&opts);
    if (!w) {
        result.failure_reason = "recovery failed to open WAL";
        goto cleanup;
    }

    result.recovered = verify.count;

    // === VERIFICATION ===

    // Rule 1: Everything marked "completed" MUST be recovered
    // (because fsync happened before wal_put returned)
    if (verify.highest_index < witness->completed_index) {
        result.failure_reason = "DURABILITY VIOLATION: completed entries not recovered";
        goto cleanup;
    }

    // Rule 2: Nothing beyond "attempting" should be recovered
    // (can't recover what was never even started)
    if (verify.highest_index > witness->attempting_index) {
        result.failure_reason = "GHOST ENTRY: recovered entry that was never attempted";
        goto cleanup;
    }

    // Rule 3: Recovered entries must be contiguous (no gaps)
    // Indices should be 1, 2, 3, ..., N with no holes
    for (uint64_t i = 0; i < verify.count; i++) {
        if (verify.recovered_indices[i] != i + 1) {
            result.failure_reason = "GAP IN LOG: recovered entries have holes";
            goto cleanup;
        }
    }

    // Rule 4: Recovered entry contents must match what we wrote
    for (uint64_t i = 0; i < verify.count; i++) {
        uint32_t expected = witness->entry_checksums[i];
        uint32_t actual = verify.recovered_checksums[i];
        if (expected != actual) {
            result.failure_reason = "DATA CORRUPTION: entry content mismatch";
            goto cleanup;
        }
    }

    // Rule 5: We should be able to write more after recovery
    {
        uint64_t next_index = verify.highest_index + 1;
        int ret = wal_put(w, next_index, 1, "recovery_test", 13, "value", 5);
        if (ret != LYGUS_OK) {
            result.failure_reason = "POST-RECOVERY WRITE FAILED";
            goto cleanup;
        }
    }

    // If we get here, all checks passed
    result.passed = 1;

cleanup:
    if (w) {
        wal_close(w);
    }
    free(verify.recovered_indices);
    free(verify.recovered_checksums);
    destroy_shared_witness(witness);

    return result;
}

// ============================================================================
// Main
// ============================================================================

static void print_usage(const char *prog) {
    fprintf(stderr, "Usage: %s [--iterations=N] [--seed=N] [--verbose]\n", prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "  --iterations=N   Number of amphoreus cylc.. i mean crash iterations (default: %d)\n",
            DEFAULT_ITERATIONS);
    fprintf(stderr, "  --seed=N         Random seed (default: time-based)\n");
    fprintf(stderr, "  --verbose        Print details for each iteration\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "This test simulates real crashes by forking a child process\n");
    fprintf(stderr, "that writes to the WAL, then killing it with SIGKILL at random\n");
    fprintf(stderr, "points. It then verifies that recovery works correctly.\n");
}

int main(int argc, char **argv) {
    int iterations = DEFAULT_ITERATIONS;
    unsigned int seed = (unsigned int)time(NULL);
    int verbose = 0;

    // Parse args
    for (int i = 1; i < argc; i++) {
        if (strncmp(argv[i], "--iterations=", 13) == 0) {
            iterations = atoi(argv[i] + 13);
        } else if (strncmp(argv[i], "--seed=", 7) == 0) {
            seed = (unsigned int)atoi(argv[i] + 7);
        } else if (strcmp(argv[i], "--verbose") == 0) {
            verbose = 1;
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }

    printf(BLUE);
    printf("╔══════════════════════════════════════════════════════════╗\n");
    printf("║            WAL CRASH TEST HARNESS                       ║\n");
    printf("╚══════════════════════════════════════════════════════════╝\n");
    printf(RESET);
    printf("\n");
    printf("Iterations: %d\n", iterations);
    printf("Seed: %u\n", seed);
    printf("Kill delay: %d-%d ms\n", MIN_KILL_DELAY_MS, MAX_KILL_DELAY_MS);
    printf("\n");

    srand(seed);

    // Initialize logging (captures any WAL issues)
    lygus_log_init("./crash_test.log", 1);
    lygus_log_set_mode(LYGUS_LOG_DETAILED);

    int passed = 0;
    int failed = 0;

    for (int i = 1; i <= iterations; i++) {
        iteration_result_t result = run_crash_iteration(i);

        if (result.passed) {
            passed++;
            if (verbose) {
                printf(GREEN "[%4d] PASS" RESET
                       " attempted=%llu completed=%llu recovered=%llu\n",
                       i,
                       (unsigned long long)result.attempted,
                       (unsigned long long)result.completed,
                       (unsigned long long)result.recovered);
            } else if (i % 10 == 0) {
                printf(".");
                fflush(stdout);
            }
        } else {
            failed++;
            printf(RED "\n[%4d] FAIL: %s\n" RESET, i, result.failure_reason);
            printf("       attempted=%llu completed=%llu recovered=%llu\n",
                   (unsigned long long)result.attempted,
                   (unsigned long long)result.completed,
                   (unsigned long long)result.recovered);

            // On failure, save the WAL state for debugging
            char save_cmd[256];
            snprintf(save_cmd, sizeof(save_cmd),
                     "cp -r %s ./crash_failure_%d", TEST_DIR, i);
            system(save_cmd);
            printf("       Saved WAL state to ./crash_failure_%d/\n", i);
        }
    }

    printf("\n\n");
    printf(BLUE "══════════════════════════════════════════════════════════\n" RESET);

    if (failed == 0) {
        printf(GREEN "✓ ALL %d ITERATIONS PASSED\n" RESET, passed);
        printf("  Your WAL correctly handles crashes at random points.\n");
    } else {
        printf(RED "✗ %d/%d ITERATIONS FAILED\n" RESET, failed, iterations);
        printf("  Check crash_failure_*/ directories for failed WAL states.\n");
        printf("  Check crash_test.log for WAL-level logging.\n");
    }

    printf(BLUE "══════════════════════════════════════════════════════════\n" RESET);

    // Cleanup
    lygus_log_shutdown();
    cleanup_test_dir();
    system("rm -rf " TEST_DIR);

    return (failed == 0) ? 0 : 1;
}