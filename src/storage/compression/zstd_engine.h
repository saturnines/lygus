#ifndef ZSTD_ENGINE_H
#define ZSTD_ENGINE_H

#include <stddef.h>
#include <stdint.h>

// This is a stub
static inline size_t zstd_compress(void *dst, size_t dst_cap,
                                   const void *src, size_t src_len,
                                   int level) {
    (void)level;
    if (dst_cap < src_len) return 0;
    memcpy(dst, src, src_len);
    return src_len;
}

static inline size_t zstd_decompress(void *dst, size_t dst_cap,
                                     const void *src, size_t src_len) {
    if (dst_cap < src_len) return 0;
    memcpy(dst, src, src_len);
    return src_len;
}

#endif