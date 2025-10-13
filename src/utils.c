#include "utils.h"
#include "platform.h"

#if defined(__SSE2__) || defined(_M_X64) || defined(_M_IX86)
#include <emmintrin.h>
#define USE_SSE2 1
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#define USE_NEON 1
#else
#define USE_SCALAR 1
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Woverride-init"
static const unsigned char CharToHex[256]={
	[0 ... 255] =0xFF,
	['0']=0,['1']=1,['2']=2,['3']=3,['4']=4,
	['5']=5,['6']=6,['7']=7,['8']=8,['9']=9,
	['A']=10,['B']=11,['C']=12,['D']=13,['E']=14,['F']=15,
	['a']=10,['b']=11,['c']=12,['d']=13,['e']=14,['f']=15,
};
#pragma GCC diagnostic pop

static inline bool tryHexToByte(const char* src, unsigned char* outByte) {
	unsigned char hi=CharToHex[(unsigned char)src[0]];
	unsigned char lo=CharToHex[(unsigned char)src[1]];
	if(hi==0xFF||lo==0xFF)
		return false;
	*outByte=(hi<<4)|lo;
	return true;
}

void url_decode(char* s) {
	char* o=s;
	while(*s) {

#if defined(USE_SSE2)
		if(((uintptr_t)s%16==0)&&strlen(s)>=16) {
			__m128i chunk=_mm_loadu_si128((__m128i*)s);
			__m128i percent=_mm_set1_epi8('%');
			__m128i plus=_mm_set1_epi8('+');
			__m128i cmp1=_mm_cmpeq_epi8(chunk, percent);
			__m128i cmp2=_mm_cmpeq_epi8(chunk, plus);
			int mask=_mm_movemask_epi8(_mm_or_si128(cmp1, cmp2));
			if(mask==0) {
				_mm_storeu_si128((__m128i*)o, chunk);
				s+=16;
				o+=16;
				continue;
			}
		}

#elif defined(USE_NEON)
		if(((uintptr_t)s%16==0)&&strlen(s)>=16) {
			uint8x16_t chunk=vld1q_u8((uint8_t*)s);
			uint8x16_t percent=vdupq_n_u8('%');
			uint8x16_t plus=vdupq_n_u8('+');
			uint8x16_t cmp1=vceqq_u8(chunk, percent);
			uint8x16_t cmp2=vceqq_u8(chunk, plus);
			uint8x16_t cmp=vorrq_u8(cmp1, cmp2);
			uint64_t mask=vmaxvq_u8(cmp);
			if(mask==0) {
				vst1q_u8((uint8_t*)o, chunk);
				s+=16;
				o+=16;
				continue;
			}
		}
#endif
		if(*s=='+') {
			*o++=' ';
			s++;
		}
		else if(*s=='%'&&s[1]&&s[2]) {
			unsigned char decoded;
			if(tryHexToByte(s+1, &decoded)) {
				*o++=decoded;
				s+=3;
			}
			else {
				*o++=*s++;
			}
		}
		else {
			*o++=*s++;
		}
	}
	*o='\0';
}

char* query_get(char* qs, const char* key) {
	char* p=qs;
	while(p&&*p) {
		char* amp=strchr(p, '&');
		if(amp) *amp='\0';
		char* eq=strchr(p, '=');
		if(eq) {
			*eq='\0';
			if(strcmp(p, key)==0) {
				char* val=eq+1;
				char* decoded_val=strdup(val);
				url_decode(decoded_val);
				if(amp) *amp='&';
				if(eq) *eq='=';
				return decoded_val;
			}
			if(eq) *eq='=';
		}
		if(amp) {
			*amp='&';
			p=amp+1;
		}
		else {
			break;
		}
	}
	return NULL;
}

int ci_cmp(const void* a, const void* b) {
	const char* s1=*(const char* const*)a;
	const char* s2=*(const char* const*)b;
#ifdef _WIN32
	return _stricmp(s1, s2);
#else
	return strcasecmp(s1, s2);
#endif
}

void sb_append(char** buf, size_t* cap, size_t* len, const char* s) {
	size_t add=strlen(s);
	if(*len+add+1>=*cap) {
		*cap=*cap*2+add+64;
		*buf=realloc(*buf, *cap);
	}
	memcpy(*buf+*len, s, add);
	*len+=add;
	(*buf)[*len]='\0';
}

void sb_append_esc(char** buf, size_t* cap, size_t* len, const char* s) {
	while(*s) {
		unsigned char c=(unsigned char)*s++;
		if(c=='"'||c=='\\') {
			char tmp[3]={ '\\', (char)c, '\0' };
			sb_append(buf, cap, len, tmp);
		}
		else if(c<0x20) {
			char t[7];
			snprintf(t, sizeof(t), "\\u%04x", c);
			sb_append(buf, cap, len, t);
		}
		else {
			char t[2]={ (char)c, '\0' };
			sb_append(buf, cap, len, t);
		}
	}
}

void get_thumbs_root(char* out, size_t outlen) {
	if (!out || outlen == 0) return;
	snprintf(out, outlen, "%s" DIR_SEP_STR "thumbs", BASE_DIR);
}

void make_thumb_path(char* out, size_t outlen, const char* basename) {
	if (!out || outlen == 0 || !basename) return;
	char root[PATH_MAX]; get_thumbs_root(root, sizeof(root));
	snprintf(out, outlen, "%s" DIR_SEP_STR "%s", root, basename);
}