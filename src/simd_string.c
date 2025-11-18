#include "simd_string.h"
#include "common.h"
int simd_streq(const char* a, const char* b) {
    if (!a || !b) return 0;
    return strcmp(a, b) == 0;
}
size_t simd_strlen(const char* s) { if (!s) return 0; return strlen(s); }