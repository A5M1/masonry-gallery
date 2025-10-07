#pragma once
#include "common.h"

void url_decode(char* s);
char* query_get(char* qs, const char* key);
int ci_cmp(const void* a, const void* b);
void sb_append(char** buf, size_t* cap, size_t* len, const char* s);
void sb_append_esc(char** buf, size_t* cap, size_t* len, const char* s);