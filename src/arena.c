#include "arena.h"
#include <stdlib.h>
#include <string.h>
struct arena { char* buf; size_t cap; size_t off; };
arena_t* arena_create(size_t initial_capacity) {
    arena_t* a = malloc(sizeof(arena_t));
    if (!a) return NULL;
    if (initial_capacity == 0) initial_capacity = 4096;
    a->buf = malloc(initial_capacity);
    if (!a->buf) { free(a); return NULL; }
    a->cap = initial_capacity; a->off = 0; return a;
}
void arena_destroy(arena_t* a) {
    if (!a) return; free(a->buf); free(a);
}
void* arena_alloc(arena_t* a, size_t n) {
    if (!a || n == 0) return NULL;
    if (a->off + n > a->cap) {
        size_t nc = a->cap * 2;
        while (nc < a->off + n) nc *= 2;
        char* nb = realloc(a->buf, nc);
        if (!nb) return NULL;
        a->buf = nb; a->cap = nc;
    }
    void* p = a->buf + a->off; a->off += n; return p;
}
char* arena_strdup(arena_t* a, const char* s) {
    if (!a || !s) return NULL; size_t l = strlen(s) + 1; char* p = arena_alloc(a, l); if (!p) return NULL; memcpy(p, s, l); return p;
}