#include "varint.h"

// ============================================================================
// Encode
// ============================================================================

size_t varint_encode(uint64_t value, uint8_t *buf) {
  size_t pos = 0;

  // Encode 7 bits at a time, LSB first
  while (value >= 0x80) {
    // Set continuation bit (MSB=1) and write lower 7 bits
    buf[pos++] = (uint8_t)((value & 0x7F) | 0x80); // 0x80 forces MSB = 1
    value >>= 7;
  }

  // Last byte: no continuation bit aka (MSB=0)
  buf[pos++] = (uint8_t)(value & 0x7F);

  return pos;
}

// ============================================================================
// Decoding
// ============================================================================
ssize_t varint_decode(const uint8_t *buf, size_t buf_len, uint64_t *out) {
  if (buf == NULL || out == NULL || buf_len == 0) {
    return LYGUS_ERR_INVALID_ARG;
  }

  uint64_t result = 0;
  size_t shift = 0;
  size_t pos = 0;

  while (pos < buf_len) {
    uint8_t byte = buf[pos++];

    // Extract lower 7 bits
    uint64_t value = byte & 0x7F;

    // Check for overflow: max 10 bytes for uint64_t
    // (10 bytes * 7 bits = 70 bits, but we only use 64)
    if (shift >= 64) {
      return LYGUS_ERR_OVERFLOW;
    }

    result |= (value << shift);

    // We are done if msb is finished
    if ((byte & 0x80) == 0) {
      *out = result;
      return (ssize_t)pos;
    }

    shift += 7;

    // max 10 bytes for uint64_t to prevent loop going forever
    if (pos > VARINT_MAX_BYTES) {
      return LYGUS_ERR_MALFORMED;
    }
  }

  // Buffer ended but continuation bit was set
  return LYGUS_ERR_INCOMPLETE;
}

// ============================================================================
// Helper: Skip varint without decoding (useful for scanning)
// Note: May not use this but it's still nice to have
// ============================================================================

/**
 * Skip past a varint in buffer
 *
 * @param buf      Input buffer
 * @param buf_len  Buffer length
 * @return         Number of bytes to skip (1-10), or negative lygus_err_t
 */
ssize_t varint_skip(const uint8_t *buf, size_t buf_len) {
  if (buf == NULL || buf_len == 0) {
    return LYGUS_ERR_INVALID_ARG;
  }

  size_t pos = 0;

  while (pos < buf_len) {
    uint8_t byte = buf[pos++];

    // If MSB is clear, this is the last byte
    if ((byte & 0x80) == 0) {
      return (ssize_t)pos;
    }

    // this should prevent infinite loops on malformed data
    if (pos > VARINT_MAX_BYTES) {
      return LYGUS_ERR_MALFORMED;
    }
  }

  return LYGUS_ERR_INCOMPLETE;
}