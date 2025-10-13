#pragma once
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MD5_DIGEST_LENGTH  16
#define SHA1_DIGEST_LENGTH 20

typedef struct {
    uint32_t state[4];
    uint32_t count[2];
    uint8_t buffer[64];
} MD5_CTX;

int crypto_md5(const void *data, size_t len, uint8_t *digest_out);
int crypto_md5_file(const char *path, uint8_t *digest_out);
int crypto_sha1(const void *data, size_t len, uint8_t *digest_out);
size_t crypto_base64_encode_len(size_t in_len);
size_t crypto_base64_decode_maxlen(size_t enc_len);
size_t crypto_base64_encode(const void *data, size_t len, char *encoded_out, size_t out_size);
size_t crypto_base64_decode(const char *encoded_in, size_t len, uint8_t *decoded_out, size_t out_size);

#ifdef __cplusplus
}
#endif