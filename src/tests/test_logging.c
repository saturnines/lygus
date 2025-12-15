#include "../util/logging.h"
#include <stdio.h>
#include <unistd.h>

int main(void) {
    printf("Initializing logging...\n");

    if (lygus_log_init("/tmp/test-lygus.log", 1) != 0) {
        fprintf(stderr, "Failed to init logging\n");
        return 1;
    }

    printf("Logging some events...\n");

    // Simple event
    LOG_INFO_SIMPLE(LYGUS_MODULE_CORE, LYGUS_EVENT_INIT, 0, 0);

    // Event with data
    struct {
        uint64_t seq_no;
        uint32_t raw_len;
        uint32_t comp_len;
        uint32_t latency_us;
    } wal_data = {42, 8192, 6144, 1234};

    LOG_DEBUG(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_BLOCK_DONE,
              7, 150,  // term=7, index=150
              &wal_data, sizeof(wal_data));

    // Give formatter time to drain
    sleep(1);

    printf("Shutting down...\n");
    lygus_log_shutdown();

    printf("Check /tmp/test-lygus.log\n");
    return 0;
}