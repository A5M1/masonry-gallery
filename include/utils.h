#pragma once
#include "common.h"

void url_decode(char* s);
char* query_get(char* qs, const char* key);
int p_strcmp(const void* a, const void* b);
int ascii_stricmp(const char* a, const char* b);
#ifdef DEBUG_DIAGNOSTIC
int debug_ascii_stricmp(const char* a, const char* b);
#endif
void sb_append(char** buf, size_t* cap, size_t* len, const char* s);
void sb_append_esc(char** buf, size_t* cap, size_t* len, const char* s);
void get_thumbs_root(char* out, size_t outlen);
void make_thumb_path(char* out, size_t outlen, const char* basename);
void html_escape(const char* src, char* out, size_t outlen);

