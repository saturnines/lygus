#ifndef LYGUS_VARINT_H
#define LYGUS_VARINT_H

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

#include "../public/lygus_errors.h"

/**
 * Variable-length integer encoding (LEB128-style, unsigned)
 *
 * Format: 7 bits of data per byte, MSB indicates continuation
 * - If MSB=1, more bytes follow
 * - If MSB=0, this is the last byte
 *
 * Examples:
 *   0x00       -> [0x00]
 *   0x7F       -> [0x7F]
 *   0x80       -> [0x80, 0x01]
 *   0x3FFF     -> [0xFF, 0x7F]
 *   0xFFFFFFFF -> [0xFF, 0xFF, 0xFF, 0xFF, 0x0F]
 */

/**
 * Maximum bytes needed to encode a uint64_t
 * 64 bits / 7 bits per byte = 10 bytes max
 */
#define VARINT_MAX_BYTES 10

/**
 * Encode uint64_t to varint
 *
 * @param value  Value to encode
 * @param buf    Output buffer (must have at least VARINT_MAX_BYTES space)
 * @return       Number of bytes written (1-10)
 */
size_t varint_encode(uint64_t value, uint8_t *buf);

/**
 * Decode varint to uint64_t
 *
 * @param buf      Input buffer
 * @param buf_len  Buffer length (to prevent overrun)
 * @param out      Decoded value (output)
 * @return         Number of bytes consumed (1-10), or negative lygus_err_t
 *
 * Possible errors:
 *   LYGUS_ERR_INVALID_ARG  - NULL pointer or zero length
 *   LYGUS_ERR_OVERFLOW     - Varint exceeds 64 bits
 *   LYGUS_ERR_INCOMPLETE   - Buffer too short
 *   LYGUS_ERR_MALFORMED    - Varint exceeds 10 bytes
 */
ssize_t varint_decode(const uint8_t *buf, size_t buf_len, uint64_t *out);

/**
 * Get the encoded size of a value without encoding
 *
 * @param value  Value to measure
 * @return       Number of bytes required (1-10)
 */
static inline size_t varint_size(uint64_t value) {
  size_t bytes = 1;
  while (value >= 0x80) {
    bytes++;
    value >>= 7;
  }
  return bytes;
}

/**
 * Fast peek at varint length from first byte (without full decode)
 *
 * @param first_byte  First byte of varint
 * @return            Minimum bytes in this varint (1 if no continuation, else unknown)
 *
 * Note: Only returns definitive answer for single-byte varints.
 * Multi-byte varints require full scan.
 */
static inline int varint_has_continuation(uint8_t byte) {
  return (byte & 0x80) != 0;
}

#endif // LYGUS_VARINT_H