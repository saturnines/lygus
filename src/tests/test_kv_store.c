// tests/test_kv_store.c
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "../state/kv_store.h"

static void test_basic_ops(void) {
    printf("Testing basic operations...\n");

    lygus_kv_t *kv = lygus_kv_create();
    assert(kv != NULL);

    // Test put + get
    int ret = lygus_kv_put(kv, "foo", 3, "bar", 3);
    assert(ret == LYGUS_OK);

    char buf[64];
    ssize_t n = lygus_kv_get(kv, "foo", 3, buf, sizeof(buf));
    assert(n == 3);
    assert(memcmp(buf, "bar", 3) == 0);
    printf("  ✓ Put + Get works\n");

    // Test update
    ret = lygus_kv_put(kv, "foo", 3, "baz", 3);
    assert(ret == LYGUS_OK);
    n = lygus_kv_get(kv, "foo", 3, buf, sizeof(buf));
    assert(n == 3);
    assert(memcmp(buf, "baz", 3) == 0);
    printf("  ✓ Update works\n");

    // Test delete
    ret = lygus_kv_del(kv, "foo", 3);
    assert(ret == LYGUS_OK);
    n = lygus_kv_get(kv, "foo", 3, buf, sizeof(buf));
    assert(n == LYGUS_ERR_KEY_NOT_FOUND);
    printf("  ✓ Delete works\n");

    lygus_kv_destroy(kv);
}

static void test_edge_cases(void) {
    printf("\nTesting edge cases...\n");

    lygus_kv_t *kv = lygus_kv_create();
    assert(kv != NULL);

    // Empty value
    int ret = lygus_kv_put(kv, "key", 3, "", 0);
    assert(ret == LYGUS_OK);

    char buf[64];
    ssize_t n = lygus_kv_get(kv, "key", 3, buf, sizeof(buf));
    assert(n == 0);  // Zero-length value
    printf("  ✓ Empty value works\n");

    // Buffer too small (should return actual size)
    lygus_kv_put(kv, "big", 3, "large_value", 11);
    n = lygus_kv_get(kv, "big", 3, buf, 5);
    assert(n == 11);  // Returns actual size, not copied size
    assert(memcmp(buf, "large", 5) == 0);  // Partial copy
    printf("  ✓ Buffer too small handled correctly\n");

    // Query size without buffer
    n = lygus_kv_get(kv, "big", 3, NULL, 0);
    assert(n == 11);
    printf("  ✓ Size query works\n");

    // Non-existent key
    n = lygus_kv_get(kv, "nope", 4, buf, sizeof(buf));
    assert(n == LYGUS_ERR_KEY_NOT_FOUND);
    printf("  ✓ Missing key returns error\n");

    // Delete non-existent
    ret = lygus_kv_del(kv, "nope", 4);
    assert(ret == LYGUS_ERR_KEY_NOT_FOUND);
    printf("  ✓ Delete missing key returns error\n");

    lygus_kv_destroy(kv);
}

static void test_binary_keys(void) {
    printf("\nTesting binary keys/values...\n");

    lygus_kv_t *kv = lygus_kv_create();
    assert(kv != NULL);

    // Key with null bytes
    uint8_t key[] = {0x00, 0xFF, 0x42};
    uint8_t val[] = {0xDE, 0xAD, 0xBE, 0xEF};

    int ret = lygus_kv_put(kv, key, sizeof(key), val, sizeof(val));
    assert(ret == LYGUS_OK);

    uint8_t buf[64];
    ssize_t n = lygus_kv_get(kv, key, sizeof(key), buf, sizeof(buf));
    assert(n == 4);
    assert(memcmp(buf, val, 4) == 0);
    printf("  ✓ Binary keys/values work\n");

    lygus_kv_destroy(kv);
}

static void test_many_entries(void) {
    printf("\nTesting many entries (resize)...\n");

    lygus_kv_t *kv = lygus_kv_create();
    assert(kv != NULL);

    // Insert 1000 entries (will trigger resizes)
    for (int i = 0; i < 1000; i++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "key_%d", i);
        snprintf(val, sizeof(val), "value_%d", i);

        int ret = lygus_kv_put(kv, key, strlen(key), val, strlen(val));
        assert(ret == LYGUS_OK);
    }

    // Verify all entries exist
    for (int i = 0; i < 1000; i++) {
        char key[32], expected[32];
        snprintf(key, sizeof(key), "key_%d", i);
        snprintf(expected, sizeof(expected), "value_%d", i);

        char buf[64];
        ssize_t n = lygus_kv_get(kv, key, strlen(key), buf, sizeof(buf));
        assert(n == (ssize_t)strlen(expected));
        assert(memcmp(buf, expected, n) == 0);
    }

    assert(lygus_kv_count(kv) == 1000);
    printf("  ✓ 1000 entries inserted and retrieved\n");
    printf("  ✓ Memory usage: %zu bytes\n", lygus_kv_memory_usage(kv));

    lygus_kv_destroy(kv);
}

static void test_iterator(void) {
    printf("\nTesting iterator...\n");

    lygus_kv_t *kv = lygus_kv_create();
    assert(kv != NULL);

    // Insert a few entries
    lygus_kv_put(kv, "a", 1, "1", 1);
    lygus_kv_put(kv, "b", 1, "2", 1);
    lygus_kv_put(kv, "c", 1, "3", 1);

    // Iterate and count
    lygus_kv_iter_t it;
    lygus_kv_iter_init(&it, kv);

    int count = 0;
    const void *k, *v;
    size_t klen, vlen;

    while (lygus_kv_iter_next(&it, &k, &klen, &v, &vlen)) {
        assert(klen == 1);
        assert(vlen == 1);
        count++;
    }

    assert(count == 3);
    printf("  ✓ Iterator visited all %d entries\n", count);

    lygus_kv_destroy(kv);
}

static void test_clear(void) {
    printf("\nTesting clear...\n");

    lygus_kv_t *kv = lygus_kv_create();
    assert(kv != NULL);

    // Add some entries
    lygus_kv_put(kv, "a", 1, "1", 1);
    lygus_kv_put(kv, "b", 1, "2", 1);
    assert(lygus_kv_count(kv) == 2);

    // Clear
    int ret = lygus_kv_clear(kv);
    assert(ret == LYGUS_OK);
    assert(lygus_kv_count(kv) == 0);

    // Verify entries are gone
    char buf[64];
    ssize_t n = lygus_kv_get(kv, "a", 1, buf, sizeof(buf));
    assert(n == LYGUS_ERR_KEY_NOT_FOUND);
    printf("  ✓ Clear removed all entries\n");

    // Can still insert after clear
    lygus_kv_put(kv, "x", 1, "9", 1);
    assert(lygus_kv_count(kv) == 1);
    printf("  ✓ Can insert after clear\n");

    lygus_kv_destroy(kv);
}

static void test_stats(void) {
    printf("\nTesting stats...\n");

    lygus_kv_t *kv = lygus_kv_create();
    assert(kv != NULL);

    assert(lygus_kv_count(kv) == 0);
    size_t initial_mem = lygus_kv_memory_usage(kv);
    printf("  Initial memory: %zu bytes\n", initial_mem);

    // Add entry
    lygus_kv_put(kv, "test", 4, "value", 5);
    assert(lygus_kv_count(kv) == 1);

    size_t after_mem = lygus_kv_memory_usage(kv);
    assert(after_mem > initial_mem);
    printf("  After 1 insert: %zu bytes (+%zu)\n",
           after_mem, after_mem - initial_mem);

    // Delete entry
    lygus_kv_del(kv, "test", 4);
    assert(lygus_kv_count(kv) == 0);

    size_t final_mem = lygus_kv_memory_usage(kv);
    assert(final_mem < after_mem);
    printf("  After delete: %zu bytes (-%zu)\n",
           final_mem, after_mem - final_mem);

    lygus_kv_destroy(kv);
}

int main(void) {
    printf("=== Lygus KV Store Tests ===\n\n");

    test_basic_ops();
    test_edge_cases();
    test_binary_keys();
    test_many_entries();
    test_iterator();
    test_clear();
    test_stats();

    printf("\n✅ All KV store tests passed!\n");
    return 0;
}