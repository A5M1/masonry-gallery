#include "blaze.h"
#include <immintrin.h>
#include <string.h>

#define ROTL64(x,r) ((x << r) | (x >> (64 - r)))

#define P1 11400714785074694791ULL
#define P2 14029467366897019727ULL
#define P3 1609587929392839161ULL
#define P4 9650029242287828579ULL
#define P5 2870177450012600261ULL

static inline uint64_t mix64(uint64_t v) {
    v ^= v >> 33; v *= P2; v ^= v >> 29; v *= P3; v ^= v >> 32;
    return v;
}

void blaze64_init(blaze64_state_t* s) {
    s->h1 = P1; s->h2 = P2; s->h3 = P3; s->h4 = P4;
    s->buffered = 0;
    s->total_len = 0;
}

void blaze64_update(blaze64_state_t* s, const uint8_t* data, size_t len) {
    s->total_len += len;
    size_t i = 0;

    if (s->buffered && s->buffered + len >= 32) {
        size_t fill = 32 - s->buffered;
        memcpy(s->buffer + s->buffered, data, fill);
        uint64_t a = *(uint64_t*)(s->buffer);
        uint64_t b = *(uint64_t*)(s->buffer + 8);
        uint64_t c = *(uint64_t*)(s->buffer + 16);
        uint64_t d = *(uint64_t*)(s->buffer + 24);
        s->h1 = ROTL64(s->h1 + a * P1, 31) * P2;
        s->h2 = ROTL64(s->h2 + b * P2, 29) * P3;
        s->h3 = ROTL64(s->h3 + c * P3, 27) * P4;
        s->h4 = ROTL64(s->h4 + d * P4, 23) * P5;
        i += fill;
        s->buffered = 0;
    }

    for (; i + 32 <= len; i += 32) {
        uint64_t a = *(uint64_t*)(data + i);
        uint64_t b = *(uint64_t*)(data + i + 8);
        uint64_t c = *(uint64_t*)(data + i + 16);
        uint64_t d = *(uint64_t*)(data + i + 24);
        s->h1 = ROTL64(s->h1 + a * P1, 31) * P2;
        s->h2 = ROTL64(s->h2 + b * P2, 29) * P3;
        s->h3 = ROTL64(s->h3 + c * P3, 27) * P4;
        s->h4 = ROTL64(s->h4 + d * P4, 23) * P5;
    }

    s->buffered = len - i;
    if (s->buffered) memcpy(s->buffer, data + i, s->buffered);
}

uint64_t blaze64_final(blaze64_state_t* s) {
    uint64_t tail = 0;
    for (size_t i = 0, shift = 0; i < s->buffered; i++, shift += 8)
        tail |= ((uint64_t)s->buffer[i]) << shift;

    s->h1 ^= mix64(tail);
    uint64_t hash = s->h1 ^ s->h2 ^ s->h3 ^ s->h4;
    hash ^= (s->h1 * 3 + s->h2 * 5 + s->h3 * 7 + s->h4 * 11);
    return mix64(hash);
}

void blaze256_init(blaze256_state_t* s) {
    uint64_t init[4] = { P1,P2,P3,P4 };
    s->h = _mm256_loadu_si256((__m256i*)init);
    s->buffered = 0;
    s->total_len = 0;
}

void blaze256_update(blaze256_state_t* s, const uint8_t* data, size_t len) {
    s->total_len += len;
    size_t i = 0;

    if (s->buffered && s->buffered + len >= 32) {
        size_t fill = 32 - s->buffered;
        memcpy(s->buffer + s->buffered, data, fill);

        __m256i tmp = _mm256_loadu_si256((const __m256i*)s->buffer);
        uint64_t tmp64[4]; _mm256_storeu_si256((__m256i*)tmp64, tmp);
        uint64_t h64[4]; _mm256_storeu_si256((__m256i*)h64, s->h);
        h64[0] = ROTL64(h64[0] + tmp64[0] * P1, 31) * P2;
        h64[1] = ROTL64(h64[1] + tmp64[1] * P2, 29) * P3;
        h64[2] = ROTL64(h64[2] + tmp64[2] * P3, 27) * P4;
        h64[3] = ROTL64(h64[3] + tmp64[3] * P4, 23) * P5;
        s->h = _mm256_loadu_si256((__m256i*)h64);

        i += fill;
        s->buffered = 0;
    }

    for (; i + 32 <= len; i += 32) {
        __m256i tmp = _mm256_loadu_si256((const __m256i*)(data + i));
        uint64_t tmp64[4]; _mm256_storeu_si256((__m256i*)tmp64, tmp);
        uint64_t h64[4]; _mm256_storeu_si256((__m256i*)h64, s->h);
        h64[0] = ROTL64(h64[0] + tmp64[0] * P1, 31) * P2;
        h64[1] = ROTL64(h64[1] + tmp64[1] * P2, 29) * P3;
        h64[2] = ROTL64(h64[2] + tmp64[2] * P3, 27) * P4;
        h64[3] = ROTL64(h64[3] + tmp64[3] * P4, 23) * P5;
        s->h = _mm256_loadu_si256((__m256i*)h64);
    }

    s->buffered = len - i;
    if (s->buffered) memcpy(s->buffer, data + i, s->buffered);
}

void blaze256_final(blaze256_state_t* s, uint64_t out[4]) {
    uint64_t tmp64[4]; _mm256_storeu_si256((__m256i*)tmp64, s->h);

    uint64_t tail = 0;
    for (size_t i = 0, shift = 0; i < s->buffered; i++, shift += 8)
        tail |= ((uint64_t)s->buffer[i]) << shift;

    tmp64[0] ^= mix64(tail);

    tmp64[0] = mix64(tmp64[0] + tmp64[2]);
    tmp64[1] = mix64(tmp64[1] + tmp64[3]);
    tmp64[2] = mix64(tmp64[2] + tmp64[0]);
    tmp64[3] = mix64(tmp64[3] + tmp64[1]);

    memcpy(out, tmp64, 4 * sizeof(uint64_t));
}
