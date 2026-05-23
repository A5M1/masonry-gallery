#include "utils.h"
#include "platform.h"
#include "directory.h"

#if defined(__x86_64__) || defined(_M_X64)
#include <emmintrin.h>
#define USE_SSE2 1
#elif defined(__aarch64__)
#include <arm_neon.h>
#define USE_NEON 1
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Woverride-init"
static const unsigned char CharToHex[256] = {
	[0 ... 255] = 0xFF,
	['0'] = 0,['1'] = 1,['2'] = 2,['3'] = 3,['4'] = 4,
	['5'] = 5,['6'] = 6,['7'] = 7,['8'] = 8,['9'] = 9,
	['A'] = 10,['B'] = 11,['C'] = 12,['D'] = 13,['E'] = 14,['F'] = 15,
	['a'] = 10,['b'] = 11,['c'] = 12,['d'] = 13,['e'] = 14,['f'] = 15,
};
#pragma GCC diagnostic pop

static inline bool tryHexToByte(const char* src, unsigned char* outByte) {
	unsigned char hi = CharToHex[(unsigned char)src[0]];
	unsigned char lo = CharToHex[(unsigned char)src[1]];
	if (hi == 0xFF || lo == 0xFF)
		return false;
	*outByte = (hi << 4) | lo;
	return true;
}

void url_decode(char* s) {
	char* o = s;
	while (*s) {

#if defined(USE_SSE2)
		if (((uintptr_t)s % 16 == 0) && strlen(s) >= 16) {
			__m128i chunk = _mm_loadu_si128((__m128i*)s);
			__m128i percent = _mm_set1_epi8('%');
			__m128i plus = _mm_set1_epi8('+');
			__m128i cmp1 = _mm_cmpeq_epi8(chunk, percent);
			__m128i cmp2 = _mm_cmpeq_epi8(chunk, plus);
			int mask = _mm_movemask_epi8(_mm_or_si128(cmp1, cmp2));
			if (mask == 0) {
				_mm_storeu_si128((__m128i*)o, chunk);
				s += 16;
				o += 16;
				continue;
			}
		}

#elif defined(USE_NEON)
		if (((uintptr_t)s % 16 == 0) && strlen(s) >= 16) {
			uint8x16_t chunk = vld1q_u8((uint8_t*)s);
			uint8x16_t percent = vdupq_n_u8('%');
			uint8x16_t plus = vdupq_n_u8('+');
			uint8x16_t cmp1 = vceqq_u8(chunk, percent);
			uint8x16_t cmp2 = vceqq_u8(chunk, plus);
			uint8x16_t cmp = vorrq_u8(cmp1, cmp2);
			uint64_t mask = vmaxvq_u8(cmp);
			if (mask == 0) {
				vst1q_u8((uint8_t*)o, chunk);
				s += 16;
				o += 16;
				continue;
			}
		}
#endif
		if (*s == '+') {
			*o++ = ' ';
			s++;
		}
		else if (*s == '%' && s[1] && s[2]) {
			unsigned char decoded;
			if (tryHexToByte(s + 1, &decoded)) {
				*o++ = decoded;
				s += 3;
			}
			else {
				*o++ = *s++;
			}
		}
		else {
			*o++ = *s++;
		}
	}
	*o = '\0';
}

char* query_get(char* qs, const char* key) {
	char* p = qs;
	while (p && *p) {
		char* amp = strchr(p, '&');
		if (amp) *amp = '\0';
		char* eq = strchr(p, '=');
		if (eq) {
			*eq = '\0';
			if (strcmp(p, key) == 0) {
				char* val = eq + 1;
				char* decoded_val = strdup(val);
				url_decode(decoded_val);
				if (amp) *amp = '&';
				if (eq) *eq = '=';
				return decoded_val;
			}
			if (eq) *eq = '=';
		}
		if (amp) {
			*amp = '&';
			p = amp + 1;
		}
		else {
			break;
		}
	}
	return NULL;
}

static const unsigned char ascii_tolower_table[256] = {
    0x00,0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0a,0x0b,0x0c,0x0d,0x0e,0x0f,
    0x10,0x11,0x12,0x13,0x14,0x15,0x16,0x17,0x18,0x19,0x1a,0x1b,0x1c,0x1d,0x1e,0x1f,
    0x20,0x21,0x22,0x23,0x24,0x25,0x26,0x27,0x28,0x29,0x2a,0x2b,0x2c,0x2d,0x2e,0x2f,
    0x30,0x31,0x32,0x33,0x34,0x35,0x36,0x37,0x38,0x39,0x3a,0x3b,0x3c,0x3d,0x3e,0x3f,
    0x40,0x61,0x62,0x63,0x64,0x65,0x66,0x67,0x68,0x69,0x6a,0x6b,0x6c,0x6d,0x6e,0x6f,
    0x70,0x71,0x72,0x73,0x74,0x75,0x76,0x77,0x78,0x79,0x7a,0x5b,0x5c,0x5d,0x5e,0x5f,
    0x60,0x61,0x62,0x63,0x64,0x65,0x66,0x67,0x68,0x69,0x6a,0x6b,0x6c,0x6d,0x6e,0x6f,
    0x70,0x71,0x72,0x73,0x74,0x75,0x76,0x77,0x78,0x79,0x7a,0x7b,0x7c,0x7d,0x7e,0x7f,
    0x80,0x81,0x82,0x83,0x84,0x85,0x86,0x87,0x88,0x89,0x8a,0x8b,0x8c,0x8d,0x8e,0x8f,
    0x90,0x91,0x92,0x93,0x94,0x95,0x96,0x97,0x98,0x99,0x9a,0x9b,0x9c,0x9d,0x9e,0x9f,
    0xa0,0xa1,0xa2,0xa3,0xa4,0xa5,0xa6,0xa7,0xa8,0xa9,0xaa,0xab,0xac,0xad,0xae,0xaf,
    0xb0,0xb1,0xb2,0xb3,0xb4,0xb5,0xb6,0xb7,0xb8,0xb9,0xba,0xbb,0xbc,0xbd,0xbe,0xbf,
    0xc0,0xc1,0xc2,0xc3,0xc4,0xc5,0xc6,0xc7,0xc8,0xc9,0xca,0xcb,0xcc,0xcd,0xce,0xcf,
    0xd0,0xd1,0xd2,0xd3,0xd4,0xd5,0xd6,0xd7,0xd8,0xd9,0xda,0xdb,0xdc,0xdd,0xde,0xdf,
    0xe0,0xe1,0xe2,0xe3,0xe4,0xe5,0xe6,0xe7,0xe8,0xe9,0xea,0xeb,0xec,0xed,0xee,0xef,
    0xf0,0xf1,0xf2,0xf3,0xf4,0xf5,0xf6,0xf7,0xf8,0xf9,0xfa,0xfb,0xfc,0xfd,0xfe,0xff
};

static int ascii_stricmp_scalar(const char* a, const char* b) {
    if (a == b) return 0;
    if (!a) return -(int)(unsigned char)*b;
    if (!b) return  (int)(unsigned char)*a;
    while (1) {
        unsigned char ca = ascii_tolower_table[(unsigned char)*a++];
        unsigned char cb = ascii_tolower_table[(unsigned char)*b++];
        if (ca != cb) return (int)ca - (int)cb;
        if (ca == '\0') break;
    }
    return 0;
}

#if defined(__x86_64__) || defined(_M_X64)

static int ascii_stricmp_simd_x86(const char* a, const char* b) {
    if (a == b) return 0;
    if (!a) return -(int)(unsigned char)*b;
    if (!b) return  (int)(unsigned char)*a;
    const __m128i upper_case_mask = _mm_set1_epi8(-32);
    const __m128i lower_case_threshold = _mm_set1_epi8('Z');
    const __m128i lower_bound = _mm_set1_epi8('A');
    size_t i = 0;
    for (;;) {
        __m128i va = _mm_loadu_si128((const __m128i*)(a + i));
        __m128i vb = _mm_loadu_si128((const __m128i*)(b + i));
        __m128i is_upper_a = 
			_mm_and_si128(
				_mm_cmpgt_epi8(
					va, _mm_sub_epi8(
						lower_bound, 
						_mm_set1_epi8(1)
			)), _mm_cmplt_epi8(
					va, _mm_add_epi8(
						lower_case_threshold, 
						_mm_set1_epi8(1)
			))
		);
        __m128i is_upper_b = 
			_mm_and_si128(
				_mm_cmpgt_epi8(
					vb, _mm_sub_epi8(
						lower_bound, 
						_mm_set1_epi8(1)
			)), _mm_cmplt_epi8(
					vb, _mm_add_epi8(
						lower_case_threshold, 
						_mm_set1_epi8(1)
			))
		);
        va = _mm_or_si128(
			_mm_andnot_si128(is_upper_a, va),
				_mm_and_si128(is_upper_a, 
					_mm_add_epi8(va, upper_case_mask))
			);
        vb = _mm_or_si128(
			_mm_andnot_si128(is_upper_b, vb), 
				_mm_and_si128(is_upper_b,
					_mm_add_epi8(vb, upper_case_mask))
			);
        __m128i cmp = _mm_cmpeq_epi8(va, vb);
        int mask = _mm_movemask_epi8(cmp);
        if (mask != 0xFFFF) {
            int diff_index = __builtin_ctz(~mask);
            unsigned char ca = ascii_tolower_table[(unsigned char)a[i + diff_index]];
            unsigned char cb = ascii_tolower_table[(unsigned char)b[i + diff_index]];
            return (int)ca - (int)cb;
        }
        __m128i null_mask = _mm_cmpeq_epi8(va, _mm_setzero_si128());
        if (_mm_movemask_epi8(null_mask)) {
            i += __builtin_ctz(_mm_movemask_epi8(null_mask));
            unsigned char ca = ascii_tolower_table[(unsigned char)a[i]];
            unsigned char cb = ascii_tolower_table[(unsigned char)b[i]];
            return (int)ca - (int)cb;
        }
        i += 16;
    }
}

#endif

#if defined(__aarch64__)

static int ascii_stricmp_simd_neon(const char* a, const char* b) {
    if (a == b) return 0;
    if (!a) return -(int)(unsigned char)*b;
    if (!b) return  (int)(unsigned char)*a;
    const uint8x16_t const_32 = vdupq_n_u8(32);
    const uint8x16_t const_A = vdupq_n_u8('A');
    const uint8x16_t const_Z = vdupq_n_u8('Z');
    size_t i = 0;
    for (;;) {
        uint8x16_t va = vld1q_u8((const uint8_t*)(a + i));
        uint8x16_t vb = vld1q_u8((const uint8_t*)(b + i));
        uint8x16_t ge_A_a = vcgeq_u8(va, const_A);
        uint8x16_t le_Z_a = vcleq_u8(va, const_Z);
        uint8x16_t is_upper_a = vandq_u8(ge_A_a, le_Z_a);
        uint8x16_t ge_A_b = vcgeq_u8(vb, const_A);
        uint8x16_t le_Z_b = vcleq_u8(vb, const_Z);
        uint8x16_t is_upper_b = vandq_u8(ge_A_b, le_Z_b);
        va = vbslq_u8(is_upper_a, vaddq_u8(va, const_32), va);
        vb = vbslq_u8(is_upper_b, vaddq_u8(vb, const_32), vb);
        uint8x16_t cmp_eq = vceqq_u8(va, vb);
        uint64_t mask = vget_lane_u64(vreinterpret_u64_u8(vget_low_u8(cmp_eq)), 0) |
                        (vget_lane_u64(vreinterpret_u64_u8(vget_high_u8(cmp_eq)), 0) << 8);
        if (mask != 0xFFFFFFFFFFFFFFFFULL) {
            int diff_index = __builtin_ctzll(~mask);
            unsigned char ca = ascii_tolower_table[(unsigned char)a[i + diff_index]];
            unsigned char cb = ascii_tolower_table[(unsigned char)b[i + diff_index]];
            return (int)ca - (int)cb;
        }
        uint8x16_t null_mask = vceqq_u8(va, vdupq_n_u8(0));
        uint64_t null_bits = vget_lane_u64(vreinterpret_u64_u8(vget_low_u8(null_mask)), 0) |
                             (vget_lane_u64(vreinterpret_u64_u8(vget_high_u8(null_mask)), 0) << 8);
        if (null_bits) {
            i += __builtin_ctzll(null_bits);
            unsigned char ca = ascii_tolower_table[(unsigned char)a[i]];
            unsigned char cb = ascii_tolower_table[(unsigned char)b[i]];
            return (int)ca - (int)cb;
        }
        i += 16;
    }
}

#endif

int ascii_stricmp(const char* a, const char* b) {
#if defined(__x86_64__) || defined(_M_X64)
    return ascii_stricmp_simd_x86(a, b);
#elif defined(__aarch64__)
    return ascii_stricmp_simd_neon(a, b);
#else
    return ascii_stricmp_scalar(a, b);
#endif
}

int p_strcmp(const void* a, const void* b) {
	const char* s1 = *(const char* const*)a;
	const char* s2 = *(const char* const*)b;
	return ascii_stricmp(s1, s2);
}

void sb_append(char** buf, size_t* cap, size_t* len, const char* s) {
	size_t add = strlen(s);
	if (*len + add + 1 >= *cap) {
		*cap = *cap * 2 + add + 64;
		*buf = realloc(*buf, *cap);
		if (!*buf) {
			return;
		}
	}
	memcpy(*buf + *len, s, add);
	*len += add;
	(*buf)[*len] = '\0';
}

void sb_append_esc(char** buf, size_t* cap, size_t* len, const char* s) {
	while (*s) {
		unsigned char c = (unsigned char)*s++;
		if (c == '"' || c == '\\') {
			char tmp[3] = { '\\', (char)c, '\0' };
			sb_append(buf, cap, len, tmp);
		}
		else if (c < 0x20) {
			char t[7];
			snprintf(t, sizeof(t), "\\u%04x", c);
			sb_append(buf, cap, len, t);
		}
		else {
			char t[2] = { (char)c, '\0' };
			sb_append(buf, cap, len, t);
		}
	}
}

void get_thumbs_root(char* out, size_t outlen) {
	if (!out || outlen == 0) return;
	snprintf(out, outlen, "%s" DIR_SEP_STR "thumbs", BASE_DIR);
	normalize_path(out);
}

void make_thumb_path(char* out, size_t outlen, const char* basename) {
	if (!out || outlen == 0 || !basename) return;
	char root[PATH_MAX]; get_thumbs_root(root, sizeof(root));
	snprintf(out, outlen, "%s" DIR_SEP_STR "%s", root, basename);
}

void html_escape(const char* src, char* out, size_t outlen) {
	if (!src || !out || outlen == 0) return;
	size_t i = 0;
	for (const char* p = src; *p && i + 6 < outlen; ++p) {
		unsigned char c = (unsigned char)*p;
		if (c == '&') { strcpy(out + i, "&amp;"); i += 5; }
		else if (c == '<') { strcpy(out + i, "&lt;"); i += 4; }
		else if (c == '>') { strcpy(out + i, "&gt;"); i += 4; }
		else if (c == '"') { strcpy(out + i, "&quot;"); i += 6; }
		else if (c == '\'') { strcpy(out + i, "&#39;"); i += 5; }
		else { out[i++] = *p; }
	}
	out[i < outlen ? i : outlen - 1] = '\0';
}

#ifdef DEBUG_DIAGNOSTIC
#include "common.h"
#include "logging.h"

static int ascii_tolower(int c) { return (c >= 'A' && c <= 'Z') ? c + 32 : c; }

int debug_ascii_stricmp(const char* a, const char* b) {
	//LOG_DEBUG("debug_ascii_stricmp: probe a=%p b=%p", (void*)a, (void*)b);
#ifdef _WIN32
	int a_readable = 0, b_readable = 0;
	if (a) {
		MEMORY_BASIC_INFORMATION mbi;
		if (VirtualQuery((LPCVOID)a, &mbi, sizeof(mbi)) == sizeof(mbi)) {
			if ((mbi.State & MEM_COMMIT) && !(mbi.Protect & PAGE_NOACCESS) && !(mbi.Protect & PAGE_GUARD)) a_readable = 1;
		}
	}
	if (b) {
		MEMORY_BASIC_INFORMATION mbi2;
		if (VirtualQuery((LPCVOID)b, &mbi2, sizeof(mbi2)) == sizeof(mbi2)) {
			if ((mbi2.State & MEM_COMMIT) && !(mbi2.Protect & PAGE_NOACCESS) && !(mbi2.Protect & PAGE_GUARD)) b_readable = 1;
		}
	}
	if (!a_readable || !b_readable) {
		//LOG_DEBUG("debug_ascii_stricmp: unreadable pointer a=%p readable=%d b=%p readable=%d", (void*)a, a_readable, (void*)b, b_readable);
		if (a == b) return 0;
		return (a < b) ? -1 : 1;
	}
	unsigned char probe_a[9] = { 0 };
	unsigned char probe_b[9] = { 0 };
	memcpy(probe_a, a, 8);
	memcpy(probe_b, b, 8);
	//LOG_DEBUG("debug_ascii_stricmp: a_bytes=%.8s b_bytes=%.8s", (char*)probe_a, (char*)probe_b);
	return ascii_stricmp(a, b);
#else
	unsigned char probe_a[9] = { 0 }, probe_b[9] = { 0 };
	if (a) for (int i = 0; i < 8 && a[i]; ++i) probe_a[i] = (unsigned char)a[i];
	if (b) for (int i = 0; i < 8 && b[i]; ++i) probe_b[i] = (unsigned char)b[i];
	//LOG_DEBUG("debug_ascii_stricmp: a_bytes=%.8s b_bytes=%.8s", (char*)probe_a, (char*)probe_b);
	return ascii_stricmp(a, b);
#endif
}
#endif