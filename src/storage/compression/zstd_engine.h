#ifndef LYGUS_ZSTD_ENGINE_H
#define LYGUS_ZSTD_ENGINE_H

#include <stddef.h>
#include <stdint.h>

/**
 * Lygus ZSTD Wrapper
 *
 * Thin wrapper around libzstd for WAL block compression.
 * If ZSTD is not available, provides stub functions that
 * pass data through uncompressed.
 */

#ifdef __cplusplus
extern "C" {
#endif

    // Opaque context
    typedef struct lygus_zstd_ctx lygus_zstd_ctx_t;

    /**
     * Create compression/decompression context
     * @param level  ZSTD compression level (ignored if ZSTD not available)
     * @return       Context handle, or NULL on failure
     */
    lygus_zstd_ctx_t* lygus_zstd_create(int level);

    /**
     * Destroy context and free resources
     */
    void lygus_zstd_destroy(lygus_zstd_ctx_t *ctx);

    /**
     * Compress data
     * @return Compressed size on success, 0 on failure
     *         If ZSTD not available, copies data uncompressed.
     */
    size_t lygus_zstd_compress(lygus_zstd_ctx_t *ctx,
                               uint8_t *dst, size_t dst_cap,
                               const uint8_t *src, size_t src_len);

    /**
     * Decompress data
     * @return Decompressed size on success, 0 on failure
     */
    size_t lygus_zstd_decompress(lygus_zstd_ctx_t *ctx,
                                 uint8_t *dst, size_t dst_cap,
                                 const uint8_t *src, size_t src_len);

    /**
     * Get maximum compressed size for given input
     */
    size_t lygus_zstd_compress_bound(size_t src_len);

    /**
     * Get ZSTD library version string
     * Returns "stub (no zstd)" if ZSTD not available.
     */
    const char* lygus_zstd_version(void);

    /**
     * Check if real ZSTD is available
     * @return 1 if ZSTD linked, 0 if using stub
     */
    int lygus_zstd_available(void);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_ZSTD_ENGINE_H