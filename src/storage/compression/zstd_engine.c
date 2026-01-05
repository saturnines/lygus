/**
 * ZSTD Engine - Stub Implementation
 *
 * This is a passthrough stub for when ZSTD is not available.
 * Data is copied uncompressed. This allows the WAL to function
 * without the ZSTD library installed.
 *
 * To enable real compression, define LYGUS_HAVE_ZSTD and link with -lzstd.
 */

#include "zstd_engine.h"
#include <stdlib.h>
#include <string.h>

#ifdef LYGUS_HAVE_ZSTD
// ============================================================================
// Real ZSTD Implementation
// ============================================================================

#include <zstd.h>

struct lygus_zstd_ctx {
    ZSTD_CCtx *cctx;
    ZSTD_DCtx *dctx;
    int level;
};

lygus_zstd_ctx_t* lygus_zstd_create(int level) {
    lygus_zstd_ctx_t *ctx = malloc(sizeof(lygus_zstd_ctx_t));
    if (!ctx) return NULL;

    ctx->cctx = ZSTD_createCCtx();
    ctx->dctx = ZSTD_createDCtx();
    ctx->level = level;

    if (!ctx->cctx || !ctx->dctx) {
        lygus_zstd_destroy(ctx);
        return NULL;
    }

    return ctx;
}

void lygus_zstd_destroy(lygus_zstd_ctx_t *ctx) {
    if (!ctx) return;
    if (ctx->cctx) ZSTD_freeCCtx(ctx->cctx);
    if (ctx->dctx) ZSTD_freeDCtx(ctx->dctx);
    free(ctx);
}

size_t lygus_zstd_compress(lygus_zstd_ctx_t *ctx,
                           uint8_t *dst, size_t dst_cap,
                           const uint8_t *src, size_t src_len) {
    if (!ctx || !ctx->cctx) return 0;

    size_t result = ZSTD_compressCCtx(ctx->cctx, dst, dst_cap, src, src_len, ctx->level);
    if (ZSTD_isError(result)) return 0;
    return result;
}

size_t lygus_zstd_decompress(lygus_zstd_ctx_t *ctx,
                             uint8_t *dst, size_t dst_cap,
                             const uint8_t *src, size_t src_len) {
    if (!ctx || !ctx->dctx) return 0;

    size_t result = ZSTD_decompressDCtx(ctx->dctx, dst, dst_cap, src, src_len);
    if (ZSTD_isError(result)) return 0;
    return result;
}

size_t lygus_zstd_compress_bound(size_t src_len) {
    return ZSTD_compressBound(src_len);
}

const char* lygus_zstd_version(void) {
    static char version[32];
    unsigned v = ZSTD_versionNumber();
    snprintf(version, sizeof(version), "%u.%u.%u",
             v / 10000, (v / 100) % 100, v % 100);
    return version;
}

int lygus_zstd_available(void) {
    return 1;
}

#else
// ============================================================================
// Stub Implementation (no ZSTD)
// ============================================================================

struct lygus_zstd_ctx {
    int level;  // Unused, just for API compatibility
};

lygus_zstd_ctx_t* lygus_zstd_create(int level) {
    lygus_zstd_ctx_t *ctx = malloc(sizeof(lygus_zstd_ctx_t));
    if (!ctx) return NULL;
    ctx->level = level;
    return ctx;
}

void lygus_zstd_destroy(lygus_zstd_ctx_t *ctx) {
    free(ctx);
}

size_t lygus_zstd_compress(lygus_zstd_ctx_t *ctx,
                           uint8_t *dst, size_t dst_cap,
                           const uint8_t *src, size_t src_len) {
    (void)ctx;  // Unused

    // Stub: just copy data uncompressed
    if (dst_cap < src_len) return 0;
    memcpy(dst, src, src_len);
    return src_len;
}

size_t lygus_zstd_decompress(lygus_zstd_ctx_t *ctx,
                             uint8_t *dst, size_t dst_cap,
                             const uint8_t *src, size_t src_len) {
    (void)ctx;  // Unused

    // Stub: just copy data (assumes it wasn't actually compressed)
    if (dst_cap < src_len) return 0;
    memcpy(dst, src, src_len);
    return src_len;
}

size_t lygus_zstd_compress_bound(size_t src_len) {
    // No compression overhead in stub mode
    return src_len;
}

const char* lygus_zstd_version(void) {
    return "stub (no zstd)";
}

int lygus_zstd_available(void) {
    return 0;
}

#endif // LYGUS_HAVE_ZSTD