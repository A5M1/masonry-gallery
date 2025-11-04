#ifndef BLAZE_H
#define BLAZE_H

#include <stdint.h>
#include <stddef.h>
#include <immintrin.h>
#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint64_t h1, h2, h3, h4;
    uint8_t buffer[32];
    size_t buffered;
    uint64_t total_len;
} blaze64_state_t;

void blaze64_init(blaze64_state_t* state);
void blaze64_update(blaze64_state_t* state, const uint8_t* data, size_t len);
uint64_t blaze64_final(blaze64_state_t* state);

typedef struct {
    __m256i h;          // h0,h1,h2,h3 packed
    uint8_t buffer[32]; // partial block
    size_t buffered;
    uint64_t total_len;
} blaze256_state_t;

void blaze256_init(blaze256_state_t* state);
void blaze256_update(blaze256_state_t* state, const uint8_t* data, size_t len);
void blaze256_final(blaze256_state_t* state, uint64_t out[4]);

#ifdef __cplusplus
}
#endif

#endif // BLAZE_H
