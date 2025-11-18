#pragma once
#include "common.h"
typedef struct arena arena_t;
arena_t* arena_create(size_t initial_capacity);
void arena_destroy(arena_t* a);
void* arena_alloc(arena_t* a, size_t n);
char* arena_strdup(arena_t* a, const char* s);