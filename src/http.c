
#include "http.h"
#include "logging.h"
#include "platform.h"
#include "common.h"

static void fmt_size(long b, char* out, size_t n) {
	const char* units[] = {"B","KB","MB","GB","TB"};
	double v = (double)b; int i = 0;
	while (v >= 1024.0 && i < 4) { v /= 1024.0; i++; }
	if (i == 0) snprintf(out, n, "%ld %s", b, units[i]); else snprintf(out, n, "%.2f %s", v, units[i]);
}


const char* mime_for(const char* p) {
	const char* e=strrchr(p, '.');if(!e)return"application/octet-stream";e++;
	if(!strcasecmp(e, "js"))return"application/javascript";
	if(!strcasecmp(e, "css"))return"text/css";
	if(!strcasecmp(e, "html")||!strcasecmp(e, "htm"))return"text/html; charset=utf-8";
	if(!strcasecmp(e, "json"))return"application/json; charset=utf-8";
	if(!strcasecmp(e, "png"))return"image/png";
	if(!strcasecmp(e, "jpg")||!strcasecmp(e, "jpeg"))return"image/jpeg";
	if(!strcasecmp(e, "gif"))return"image/gif";
	if(!strcasecmp(e, "webp"))return"image/webp";
	if(!strcasecmp(e, "svg"))return"image/svg+xml";
	if(!strcasecmp(e, "mp4"))return"video/mp4";
	if(!strcasecmp(e, "webm"))return"video/webm";
	if(!strcasecmp(e, "ogg"))return"video/ogg";
	return"application/octet-stream";
}

char* get_header_value(char* buf, const char* h) {
	if (!buf) return NULL;
	size_t hl=strlen(h);char* line=strstr(buf, "\r\n");
	while(line) {
		char* next=strstr(line+2, "\r\n");if(!next)break;
		*next='\0';
		if(!strncasecmp(line+2, h, hl)) {
			char* v=line+2+hl;while(isspace(*v))v++;
			char* end=v+strlen(v)-1;while(end>v&&isspace(*end))end--;
			*(end+1)='\0';return v;
		}
		*next='\r';line=next;
	}
	return NULL;
}

static inline char* simd_strchr(const char* s, char c) {
	__m256i set=_mm256_set1_epi8(c);const char* p=s;
	for(;;) {
		__m256i chunk=_mm256_loadu_si256((__m256i*)p);
		__m256i eq=_mm256_cmpeq_epi8(chunk, set);
		int mask=_mm256_movemask_epi8(eq);
		if(mask)return(char*)(p+__builtin_ctz(mask));
		__m256i zero=_mm256_cmpeq_epi8(chunk, _mm256_setzero_si256());
		if(_mm256_movemask_epi8(zero))return NULL;
		p+=32;
	}
}

static inline int parse_int_simd(const char* s, const char* end, long* out) {
	int len=end-s;if(len<=0||len>19)return 0;
	__m256i bytes=_mm256_loadu_si256((__m256i*)s);
	__m256i zero=_mm256_set1_epi8('0'), nine=_mm256_set1_epi8('9');
	__m256i d=_mm256_sub_epi8(bytes, zero);
	__m256i bad=_mm256_or_si256(_mm256_cmpgt_epi8(zero, d), _mm256_cmpgt_epi8(d, _mm256_sub_epi8(nine, zero)));
	if((_mm256_movemask_epi8(bad))&((1<<len)-1))return 0;
	static const unsigned long long pow10[19]={
		1000000000000000000ULL,100000000000000000ULL,10000000000000000ULL,
		1000000000000000ULL,100000000000000ULL,10000000000000ULL,1000000000000ULL,
		100000000000ULL,10000000000ULL,1000000000ULL,100000000ULL,10000000ULL,
		1000000ULL,100000ULL,10000ULL,1000ULL,100ULL,10ULL,1ULL };
	const unsigned long long* mul=pow10+(19-len);
	unsigned long long val=0;
	for(int i=0;i<len;i++)val+=((unsigned long long)(s[i]-'0'))*mul[i];
	*out=(long)val;return 1;
}

static inline int parse_decimal_trimmed(const char* s, const char* e, long* out) {
	if(s>=e) return 0;
	unsigned long long val=0;
	for(const char* p=s;p<e;p++) {
		unsigned char c=(unsigned char)*p;
		if(c<'0'||c>'9') return 0;
		int d=c-'0';
		if(val>(unsigned long long)(LONG_MAX-d)/10) val=(unsigned long long)LONG_MAX;
		else val=val*10+d;
	}
	*out=(long)val; return 1;
}

range_t parse_range_header(const char* header_value, long file_size) {
	range_t range={ 0,0,0 };
	if(!header_value||strncmp(header_value, "bytes=", 6)) {
		LOG_DEBUG("No valid Range header: %s", header_value ? header_value : "(null)");
		return range;
	}
	const char* p=header_value+6;
	const char* dash=strchr(p, '-');
	if(!dash) {
		LOG_DEBUG("Range header missing dash: %s", header_value);
		return range;
	}

	const char* s_begin=p;
	const char* s_end=dash;
	while(s_begin<s_end&&isspace((unsigned char)*s_begin)) s_begin++;
	while(s_end>s_begin&&isspace((unsigned char)*(s_end-1))) s_end--;

	const char* e_begin=dash+1;
	const char* e_end=header_value+strlen(header_value);
	while(e_begin<e_end&&isspace((unsigned char)*e_begin)) e_begin++;
	while(e_end>e_begin&&isspace((unsigned char)*(e_end-1))) e_end--;

	long s_val=-1, e_val=-1;
	if(s_begin!=s_end) {
		if(!parse_decimal_trimmed(s_begin, s_end, &s_val)) {
			LOG_DEBUG("Invalid range start: %.*s", (int)(s_end-s_begin), s_begin);
			return range;
		}
	}

	if(e_begin!=e_end) {
		if(!parse_decimal_trimmed(e_begin, e_end, &e_val)) {
			LOG_DEBUG("Invalid range end: %.*s", (int)(e_end-e_begin), e_begin);
			return range;
		}
	}

	if(s_val!=-1) {
		if(s_val>=file_size) { LOG_DEBUG("Range start %ld >= file size %ld", s_val, file_size); return range; }
		if(e_val==-1||e_val>=file_size) e_val=file_size-1;
		if(s_val>e_val) { LOG_DEBUG("Range start %ld > end %ld", s_val, e_val); return range; }
	}
	else if(e_val!=-1) {
		long start=file_size-e_val;
		if(start<0) start=0;
		s_val=start;
		e_val=file_size-1;
	}
	else {
		LOG_DEBUG("Range header both start and end missing: %s", header_value);
		return range;
	}

	range.is_range=1;
	range.start=s_val;
	range.end=e_val;
	{
		char sfs[32]; fmt_size(file_size, sfs, sizeof(sfs));
		LOG_DEBUG("Parsed range: %ld-%ld/%ld (%s)", range.start, range.end, file_size, sfs);
	}
	return range;
}


void send_header(int c, int status, const char* text, const char* ctype, long len, const range_t* r, long fs, int keep) {
	char hbuf[1024];
	int off=snprintf(hbuf, sizeof(hbuf),
		"HTTP/1.1 %d %s\r\nConnection: %s\r\nContent-Type: %s\r\n",
		status, text, keep ? "keep-alive" : "close", ctype);
	if (ctype && (strstr(ctype, "image/") || strstr(ctype, "video/")))
		off+=snprintf(hbuf+off, sizeof(hbuf)-off, "Content-Disposition: inline\r\n");
	if(keep)off+=snprintf(hbuf+off, sizeof(hbuf)-off, "Keep-Alive: timeout=%d, max=100\r\n", 5);
	if(r&&r->is_range) {
		off+=snprintf(hbuf+off, sizeof(hbuf)-off, "Content-Range: bytes %ld-%ld/%ld\r\n", r->start, r->end, fs);
		off+=snprintf(hbuf+off, sizeof(hbuf)-off, "Content-Length: %ld\r\n", r->end-r->start+1);
	}
	else off+=snprintf(hbuf+off, sizeof(hbuf)-off, "Content-Length: %ld\r\n", len);
	if (g_request_url[0] && strncmp(g_request_url, "/images/", 8) == 0) {
		off += snprintf(hbuf+off, sizeof(hbuf)-off, "Cache-Control: no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0\r\n");
		off += snprintf(hbuf+off, sizeof(hbuf)-off, "Pragma: no-cache\r\n");
		off += snprintf(hbuf+off, sizeof(hbuf)-off, "Expires: 0\r\n");
	}
	snprintf(hbuf+off, sizeof(hbuf)-off, "\r\n");
	send(c, hbuf, (int)strlen(hbuf), 0);
}

void send_text(int c, int status, const char* text, const char* body, int keep) {
	send_header(c, status, text, "text/plain; charset=utf-8", (long)strlen(body), NULL, 0, keep);
	send(c, body, (int)strlen(body), 0);
}

void send_file_stream(int c, const char* path, const char* range, int keep) {
	LOG_DEBUG("Serving file: %s", path);
	struct stat st;
	if (platform_stat(path, &st) < 0) {
		LOG_ERROR("Failed to stat file: %s", path);
		send_text(c, 404, "Not Found", "Not found", keep);
		return;
	}
	long fsz=(long)st.st_size;
	range_t r=parse_range_header(range, fsz);
	const char* ctype=mime_for(path);
	long start=0, sz=fsz;int code=200;const char* txt="OK";
	if(r.is_range) {
		if (r.start < 0) r.start = 0;
		if (fsz <= 0) {
			char hbuf[256];
			int off = snprintf(hbuf, sizeof(hbuf), "HTTP/1.1 416 Range Not Satisfiable\r\nConnection: %s\r\nContent-Range: bytes */%ld\r\nContent-Length: 0\r\n\r\n", keep ? "keep-alive" : "close", fsz);
			send(c, hbuf, (int)strlen(hbuf), 0);
			return;
		}
		if (r.end >= fsz) r.end = fsz - 1;
		if (r.start > r.end) {
			char hbuf[256];
			int off = snprintf(hbuf, sizeof(hbuf), "HTTP/1.1 416 Range Not Satisfiable\r\nConnection: %s\r\nContent-Range: bytes */%ld\r\nContent-Length: 0\r\n\r\n", keep ? "keep-alive" : "close", fsz);
			send(c, hbuf, (int)strlen(hbuf), 0);
			return;
		}
		start=r.start;sz=r.end-r.start+1;code=206;txt="Partial Content";
		{
			char ssz[32]; fmt_size(sz, ssz, sizeof(ssz));
			LOG_INFO("Range request: %ld-%ld (%s)", r.start, r.end, ssz);
		}
	}
	(void)0; 
	send_header(c, code, txt, ctype, sz, r.is_range ? &r : NULL, fsz, keep);
	if (platform_stream_file_payload(c, path, start, sz, r.is_range) != 0) {
		LOG_DEBUG("File transfer incomplete or failed for %s", path);
	}
}
