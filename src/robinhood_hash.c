#include "robinhood_hash.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <immintrin.h>
struct rh_entry { uint64_t h; char* key; size_t key_len; unsigned char* val; size_t val_len; };
struct rh_table { size_t cap; size_t mask; size_t count; struct rh_entry* entries; };

static inline uint64_t read_u64_le(const void* p) {
    uint64_t v; memcpy(&v, p, sizeof(v)); return v;
}

static uint64_t rh_hash64(const char* data, size_t len) {
    const uint64_t m1 = 0x9ddfea08eb382d69ULL;
    const uint64_t m2 = 0xc3a5c85c97cb3127ULL;
    uint64_t h = 1469598103934665603ULL ^ (uint64_t)len;
    size_t i = 0;
    while (i + 8 <= len) {
        uint64_t k = read_u64_le(data + i);
        k *= m1; k = (k << 31) | (k >> 33); k *= m2;
        h ^= k;
        h = (h ^ (h >> 33)) * 0xff51afd7ed558ccdULL;
        i += 8;
    }
    uint64_t tail = 0;
    size_t rem = len - i;
    if (rem) {
        for (size_t j = 0; j < rem; ++j) tail |= (uint64_t)(unsigned char)data[i + j] << (j * 8);
        tail *= m1; tail = (tail << 31) | (tail >> 33); tail *= m2; h ^= tail;
        h = (h ^ (h >> 33)) * 0xff51afd7ed558ccdULL;
    }
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
}

static int simd_memcmp_equal(const char* a, const char* b, size_t len) {
    if (a == b) return 1;
    size_t i = 0;
#if defined(__SSE2__)
    for (; i + 16 <= len; i += 16) {
        __m128i va = _mm_loadu_si128((const __m128i*)(const void*)(a + i));
        __m128i vb = _mm_loadu_si128((const __m128i*)(const void*)(b + i));
        __m128i cmp = _mm_cmpeq_epi8(va, vb);
        int mask = _mm_movemask_epi8(cmp);
        if (mask != 0xFFFF) return 0;
    }
#endif
    for (; i + 8 <= len; i += 8) {
        uint64_t va; uint64_t vb; memcpy(&va, a + i, 8); memcpy(&vb, b + i, 8);
        if (va != vb) return 0;
    }
    for (; i < len; ++i) if ((unsigned char)a[i] != (unsigned char)b[i]) return 0;
    return 1;
}

uint64_t xxh3_64(const void* data, size_t len) {
    return rh_hash64((const char*)data, len);
}
rh_table_t* rh_create(size_t capacity_power) {
    if (capacity_power < 4) capacity_power = 4;
    size_t cap = (size_t)1 << capacity_power;
    struct rh_table* t = malloc(sizeof(*t)); if (!t) return NULL;
    t->entries = calloc(cap, sizeof(struct rh_entry)); if (!t->entries) { free(t); return NULL; }
    t->cap = cap; t->mask = cap - 1; t->count = 0; return t;
}
void rh_destroy(rh_table_t* t) {
    if (!t) return; for (size_t i = 0; i < t->cap; ++i) { free(t->entries[i].key); free(t->entries[i].val); }
    free(t->entries); free(t);
}
static int rh_probe_distance(size_t slot, size_t ideal, size_t cap) {
    if (slot >= ideal) return (int)(slot - ideal);
    return (int)(cap - (ideal - slot));
}
int rh_insert(rh_table_t* t, const char* key, size_t key_len, const unsigned char* val, size_t val_len) {
    if (!t || !key) return -1;
    uint64_t h = rh_hash64(key, key_len);
    size_t mask = t->mask; size_t pos = (size_t)(h & mask);
    struct rh_entry e = { h, NULL, key_len, NULL, val_len };
    e.key = malloc(key_len + 1); if (!e.key) return -1; memcpy(e.key, key, key_len); e.key[key_len] = '\0';
    if (val && val_len) { e.val = malloc(val_len); if (!e.val) { free(e.key); return -1; } memcpy(e.val, val, val_len); }
    while (1) {
        struct rh_entry* cur = &t->entries[pos];
        if (!cur->key) { *cur = e; t->count++; return 0; }
        int cur_pd = rh_probe_distance(pos, (size_t)(cur->h & mask), t->cap);
        int new_pd = rh_probe_distance(pos, (size_t)(e.h & mask), t->cap);
        if (new_pd > cur_pd) {
            struct rh_entry tmp = *cur; *cur = e; e = tmp; 
        }
        pos = (pos + 1) & mask;
    }
    return 0;
}
int rh_find(rh_table_t* t, const char* key, size_t key_len, unsigned char** out_val, size_t* out_val_len) {
    if (!t || !key) return -1;
    uint64_t h = rh_hash64(key, key_len);
    size_t mask = t->mask; size_t pos = (size_t)(h & mask);
    while (1) {
        struct rh_entry* cur = &t->entries[pos];
        if (!cur->key) return 1;
        if (cur->h == h && cur->key && cur->key_len == key_len && simd_memcmp_equal(cur->key, key, key_len)) {
            if (out_val) *out_val = cur->val; if (out_val_len) *out_val_len = cur->val_len; return 0;
        }
        pos = (pos + 1) & mask;
    }
    return 1;
}
int rh_remove(rh_table_t* t, const char* key, size_t key_len) {
    if (!t || !key) return -1;
    uint64_t h = rh_hash64(key, key_len);
    size_t mask = t->mask; size_t pos = (size_t)(h & mask);
    while (1) {
        struct rh_entry* cur = &t->entries[pos];
        if (!cur->key) return 1;
        if (cur->h == h && cur->key && cur->key_len == key_len && simd_memcmp_equal(cur->key, key, key_len)) {
            free(cur->key); free(cur->val); cur->key = NULL; cur->val = NULL; cur->key_len = 0; cur->val_len = 0; cur->h = 0; t->count--; size_t next = (pos + 1) & mask;
            while (t->entries[next].key) {
                struct rh_entry shift = t->entries[next]; t->entries[next].key = NULL; t->entries[next].val = NULL; t->entries[next].key_len = 0; t->entries[next].val_len = 0; t->entries[next].h = 0; t->count--; rh_insert(t, shift.key, shift.key_len, shift.val, shift.val_len); free(shift.key); free(shift.val); next = (next + 1) & mask;
            }
            return 0;
        }
        pos = (pos + 1) & mask;
    }
    return 1;
}

int rh_iterate(rh_table_t* t, int (*cb)(const char* key, const unsigned char* val, size_t val_len, void* ctx), void* ctx) {
    if (!t || !cb) return 0;
    for (size_t i = 0; i < t->cap; ++i) {
        struct rh_entry* e = &t->entries[i];
        if (e->key) {
            int r = cb(e->key, e->val, e->val_len, ctx);
            if (r) return r;
        }
    }
    return 0;
}
