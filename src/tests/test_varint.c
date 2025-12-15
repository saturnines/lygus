#include <stdio.h>
#include <assert.h>
#include <string.h>
#include "../util/varint.h"
#include "../public/lygus_errors.h"

void test_encode_decode(uint64_t value) {
    uint8_t buf[VARINT_MAX_BYTES];
    uint64_t decoded;
    
    // Encode
    size_t encoded_len = varint_encode(value, buf);
    printf("  Encode %llu -> %zu bytes\n", (unsigned long long)value, encoded_len);
    
    // Decode
    ssize_t n = varint_decode(buf, encoded_len, &decoded);
    assert(n > 0);  // Should succeed
    assert((size_t)n == encoded_len);
    assert(decoded == value);
    
    // Verify size helper
    assert(varint_size(value) == encoded_len);
    
    printf("  ✓ Roundtrip OK: %llu\n", (unsigned long long)decoded);
}

void test_errors() {
    printf("\nTesting error cases:\n");
    
    uint8_t buf[VARINT_MAX_BYTES * 2];  // Extra space for overflow test
    uint64_t decoded;
    ssize_t result;
    
    // Test: NULL buffer
    result = varint_decode(NULL, 10, &decoded);
    assert(result == LYGUS_ERR_INVALID_ARG);
    printf("  ✓ NULL buffer -> INVALID_ARG\n");
    
    // Test: NULL output
    result = varint_decode(buf, 10, NULL);
    assert(result == LYGUS_ERR_INVALID_ARG);
    printf("  ✓ NULL output -> INVALID_ARG\n");
    
    // Test: Zero length
    result = varint_decode(buf, 0, &decoded);
    assert(result == LYGUS_ERR_INVALID_ARG);
    printf("  ✓ Zero length -> INVALID_ARG\n");
    
    // Test: Incomplete varint (continuation bit set, buffer ends)
    buf[0] = 0x80;  // Continuation bit set
    result = varint_decode(buf, 1, &decoded);
    assert(result == LYGUS_ERR_INCOMPLETE);
    printf("  ✓ Incomplete varint -> INCOMPLETE\n");
    
    // Test: Overflow (varint exceeds 64 bits)
    // Fill buffer with continuation bits beyond what uint64_t can hold
    for (int i = 0; i < VARINT_MAX_BYTES + 1; i++) {
        buf[i] = 0x80;  // All continuation bits
    }
    result = varint_decode(buf, VARINT_MAX_BYTES + 1, &decoded);
    assert(result == LYGUS_ERR_OVERFLOW);
    printf("  ✓ Overflow varint -> OVERFLOW\n");
}

void test_boundary_values() {
    printf("\nTesting boundary values:\n");
    
    // Single byte boundaries
    test_encode_decode(0);
    test_encode_decode(127);  // Max 1 byte: 0x7F
    
    // Two byte boundaries
    test_encode_decode(128);     // Min 2 bytes: 0x80
    test_encode_decode(16383);   // Max 2 bytes: 0x3FFF
    
    // Three byte boundaries
    test_encode_decode(16384);   // Min 3 bytes
    test_encode_decode(2097151); // Max 3 bytes
    
    // Common values
    test_encode_decode(256);
    test_encode_decode(65535);   // 16-bit max
    test_encode_decode(0xFFFFFFFF);  // 32-bit max
    
    // 64-bit max
    test_encode_decode(0xFFFFFFFFFFFFFFFFULL);
}

void test_specific_encoding() {
    printf("\nTesting specific encodings:\n");
    
    uint8_t buf[VARINT_MAX_BYTES];
    size_t len;
    
    // Test: 0 encodes as [0x00]
    len = varint_encode(0, buf);
    assert(len == 1);
    assert(buf[0] == 0x00);
    printf("  ✓ 0 -> [0x00]\n");
    
    // Test: 127 encodes as [0x7F]
    len = varint_encode(127, buf);
    assert(len == 1);
    assert(buf[0] == 0x7F);
    printf("  ✓ 127 -> [0x7F]\n");
    
    // Test: 128 encodes as [0x80, 0x01]
    len = varint_encode(128, buf);
    assert(len == 2);
    assert(buf[0] == 0x80);  // Lower 7 bits: 0, continuation bit: 1
    assert(buf[1] == 0x01);  // Upper bits: 1
    printf("  ✓ 128 -> [0x80, 0x01]\n");
    
    // Test: 300 encodes as [0xAC, 0x02]
    // 300 = 0b100101100
    // Lower 7 bits: 0b0101100 = 0x2C, with continuation: 0xAC
    // Upper bits:   0b10 = 0x02
    len = varint_encode(300, buf);
    assert(len == 2);
    assert(buf[0] == 0xAC);
    assert(buf[1] == 0x02);
    printf("  ✓ 300 -> [0xAC, 0x02]\n");
}

int main(void) {
    printf("=== Lygus Varint Tests ===\n");
    
    test_boundary_values();
    test_specific_encoding();
    test_errors();
    
    printf("\n✅ All tests passed!\n");
    return 0;
}