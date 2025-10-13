#pragma once
#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

void url_decode(char* s);
char* query_get(char* qs, const char* key);
int ci_cmp(const void* a, const void* b);
void sb_append(char** buf, size_t* cap, size_t* len, const char* s);
void sb_append_esc(char** buf, size_t* cap, size_t* len, const char* s);
void get_thumbs_root(char* out, size_t outlen);
void make_thumb_path(char* out, size_t outlen, const char* basename);

#ifdef __cplusplus
}
#endif