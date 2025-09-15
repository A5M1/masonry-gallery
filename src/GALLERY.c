/*
Abnormal General Software License v2 (ABGSLV2)
This Abnormal General Software License Version 2 ("License") is a legal agreement between the original author ("Licensor")
and the individual or entity using the software, source code, or executable binary licensed under this agreement ("Licensee").
By using, modifying, or distributing the software, Licensee agrees to abide by the terms set forth herein.
https://xcn.abby0666.xyz/abgslv2.htm
https://github.com/AM51

1.5K LINES OF C TO REPLACE 200MB OF NODE_MODULES
GET REAL
gcc -o ga GALLERY.c -lws2_32 && cls && start ga (win)
*/



//good fucking luck <3
#pragma region INC_PREP_GLBS
#define _CRT_SECURE_NO_WARNINGS
#ifdef _WIN32
#define _WIN32_WINNT 0x0A00
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <mswsock.h>
#include <io.h>
#include <direct.h>
#include <process.h>
#include <sys/types.h>
#include <sys/stat.h>
typedef struct _stat stat_t;
#define stat_fn _stat
#define SOCKET_CLOSE(s) closesocket(s)
#define MUTEX_LOCK(m) WaitForSingleObject(m, INFINITE)
#define MUTEX_UNLOCK(m) ReleaseMutex(m)
#define DIR_SEP '\\'
#define DIR_SEP_STR "\\"
#define STAT_FUNC _stat64
#define FOPEN_READ(path) fopen(path, "rb")
#ifndef PATH_MAX
#define PATH_MAX MAX_PATH
#endif
#define F_OK 0
#else
#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>
#include <time.h>
#include <pthread.h>
#include <sys/sysinfo.h>
typedef struct stat stat_t;
#define stat_fn stat
#define SOCKET_CLOSE(s) close(s)
#define MUTEX_LOCK(m) pthread_mutex_lock(&m)
#define MUTEX_UNLOCK(m) pthread_mutex_unlock(&m)
#define DIR_SEP '/'
#define DIR_SEP_STR "/"
#define STAT_FUNC stat
#define FOPEN_READ(path) fopen(path, "rb")
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>
#include <math.h>
#include <stdarg.h>
#include <time.h>
#include <errno.h>
#if defined(__SSE2__) || defined(_M_X64) || defined(_M_IX86)
#include <emmintrin.h>
#define USE_SSE2 1
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#define USE_NEON 1
#else
#define USE_SCALAR 1
#endif
#define STRCPY(dest, src) do { \
    strncpy(dest, src, sizeof(dest) - 1); \
    dest[sizeof(dest) - 1] = '\0'; \
} while(0)
#define IS_EMPTY_STR(s) (!(s) || !(s)[0])
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define SAFE_FREE(ptr) do { if (ptr) { free(ptr); ptr = NULL; } } while(0)
#define ARRAY_LEN(arr) (sizeof(arr) / sizeof((arr)[0]))
static char BASE_DIR[PATH_MAX]={ 0 };
static char VIEWS_DIR[PATH_MAX]={ 0 };
static char JS_DIR[PATH_MAX]={ 0 };
static char CSS_DIR[PATH_MAX]={ 0 };
static char BUNDLED_FILE[PATH_MAX]={ 0 };
static const int ITEMS_PER_PAGE=25;
static const int KEEP_ALIVE_TIMEOUT_SEC=180;
static const char* IMAGE_EXTS[]={ ".jpg", ".jpeg", ".png", ".gif", ".webp", NULL };
static const char* VIDEO_EXTS[]={ ".mp4", ".webm", ".ogg", NULL };

#pragma endregion

#pragma region Logging
#pragma region ANSI
#define ANSI_COLOR_BLACK		   "\x1b[30m"
#define ANSI_COLOR_RED			   "\x1b[31m"
#define ANSI_COLOR_GREEN		   "\x1b[32m"
#define ANSI_COLOR_YELLOW		   "\x1b[33m"
#define ANSI_COLOR_BLUE			   "\x1b[34m"
#define ANSI_COLOR_MAGENTA		   "\x1b[35m"
#define ANSI_COLOR_CYAN			   "\x1b[36m"
#define ANSI_COLOR_WHITE		   "\x1b[37m"
#define ANSI_COLOR_BRIGHT_BLACK    "\x1b[90m"
#define ANSI_COLOR_BRIGHT_RED      "\x1b[91m"
#define ANSI_COLOR_BRIGHT_GREEN    "\x1b[92m"
#define ANSI_COLOR_BRIGHT_YELLOW   "\x1b[93m"
#define ANSI_COLOR_BRIGHT_BLUE     "\x1b[94m"
#define ANSI_COLOR_BRIGHT_MAGENTA  "\x1b[95m"
#define ANSI_COLOR_BRIGHT_CYAN     "\x1b[96m"
#define ANSI_COLOR_BRIGHT_WHITE    "\x1b[97m"
#define ANSI_BG_BLACK			   "\x1b[40m"
#define ANSI_BG_RED				   "\x1b[41m"
#define ANSI_BG_GREEN			   "\x1b[42m"
#define ANSI_BG_YELLOW			   "\x1b[43m"
#define ANSI_BG_BLUE			   "\x1b[44m"
#define ANSI_BG_MAGENTA			   "\x1b[45m"
#define ANSI_BG_CYAN			   "\x1b[46m"
#define ANSI_BG_WHITE			   "\x1b[47m"
#define ANSI_BG_BRIGHT_BLACK	   "\x1b[100m"
#define ANSI_BG_BRIGHT_RED		   "\x1b[101m"
#define ANSI_BG_BRIGHT_GREEN	   "\x1b[102m"
#define ANSI_BG_BRIGHT_YELLOW	   "\x1b[103m"
#define ANSI_BG_BRIGHT_BLUE		   "\x1b[104m"
#define ANSI_BG_BRIGHT_MAGENTA	   "\x1b[105m"
#define ANSI_BG_BRIGHT_CYAN		   "\x1b[106m"
#define ANSI_BG_BRIGHT_WHITE	   "\x1b[107m"
#define ANSI_COLOR_RESET		   "\x1b[0m"
#define ANSI_COLOR_BOLD			   "\x1b[1m"
#define ANSI_COLOR_DIM			   "\x1b[2m"
#define ANSI_COLOR_UNDERLINE	   "\x1b[4m"
#define ANSI_COLOR_REVERSE		   "\x1b[7m"
#define ANSI_COLOR_HIDDEN		   "\x1b[8m"

//ANSI FUCKS

#pragma endregion				  

#ifdef _WIN32
#define ENABLE_VIRTUAL_TERMINAL_PROCESSING  0x0004
#define PATH_MAX 260
#endif
#define LOG_DIR "logs"
#define MAX_LOG_MESSAGE_LENGTH 256
FILE* log_file=NULL;
#define LOG_DEBUG(format, ...) log_message(LOG_LEVEL_DEBUG, __func__, format, ##__VA_ARGS__)
#define LOG_INFO(format, ...)   log_message(LOG_LEVEL_INFO,  __func__, format, ##__VA_ARGS__)
#define LOG_WARN(format, ...)   log_message(LOG_LEVEL_WARN,  __func__, format, ##__VA_ARGS__)
#define LOG_ERROR(format, ...)  log_message(LOG_LEVEL_ERROR, __func__, format, ##__VA_ARGS__)

typedef enum {
	LOG_LEVEL_DEBUG,
	LOG_LEVEL_INFO,
	LOG_LEVEL_WARN,
	LOG_LEVEL_ERROR
} LogLevel;

static LogLevel current_log_level=LOG_LEVEL_DEBUG;

static
#ifdef _WIN32
HANDLE
#else
pthread_mutex_t
#endif
log_mutex;

static void log_init(void) {
	if(
#ifdef _WIN32
		_mkdir(LOG_DIR)
#else
		mkdir(LOG_DIR, 0755)
#endif
		!=0) {
		if(errno!=EEXIST) {
			fprintf(stderr, "WARN: Could not create log directory %s\n", LOG_DIR);
		}
	}
	char log_path[PATH_MAX];
	time_t t=time(NULL);
	struct tm* tm=localtime(&t);
	snprintf(log_path, PATH_MAX, "%s/%04d-%02d-%02d_%02d-%02d-%02d.log",
		LOG_DIR, tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
		tm->tm_hour, tm->tm_min, tm->tm_sec);
	log_file=fopen(log_path, "a");
	if(!log_file) {
		fprintf(stderr, "WARN: Could not open log file %s\n", log_path);
	}
#ifdef _WIN32
	HANDLE hOut=GetStdHandle(STD_OUTPUT_HANDLE);
	if(hOut!=INVALID_HANDLE_VALUE) {
		DWORD dwMode=0;
		if(GetConsoleMode(hOut, &dwMode)) {
			dwMode|=ENABLE_VIRTUAL_TERMINAL_PROCESSING;
			SetConsoleMode(hOut, dwMode);
		}
	}
	log_mutex=CreateMutex(NULL, FALSE, NULL);
#else
	pthread_mutex_init(&log_mutex, NULL);
#endif
}

static void log_message(LogLevel level, const char* function, const char* format, ...) {
	if(level<current_log_level) return;

#ifdef _WIN32
	WaitForSingleObject(log_mutex, INFINITE);
#else
	pthread_mutex_lock(&log_mutex);
#endif

	time_t t=time(NULL);
	struct tm* tm=localtime(&t);
	char time_str[32];
	strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", tm);

	const char* level_str;
	const char* level_color_str=ANSI_COLOR_RESET;

	switch(level) {
		case LOG_LEVEL_DEBUG:
			level_str="DBG";
			level_color_str=ANSI_COLOR_CYAN;
			break;
		case LOG_LEVEL_INFO:
			level_str="INF";
			level_color_str=ANSI_COLOR_GREEN;
			break;
		case LOG_LEVEL_WARN:
			level_str="WRN";
			level_color_str=ANSI_COLOR_YELLOW;
			break;
		case LOG_LEVEL_ERROR:
			level_str="ERR";
			level_color_str=ANSI_COLOR_RED;
			break;
		default:
			level_str="UNKN ";
	}

	char message_buffer[MAX_LOG_MESSAGE_LENGTH+4];
	va_list args;
	va_start(args, format);
	int required_size=vsnprintf(message_buffer, sizeof(message_buffer), format, args);
	va_end(args);

	if(required_size>=sizeof(message_buffer)-1) {
		message_buffer[sizeof(message_buffer)-4]='.';
		message_buffer[sizeof(message_buffer)-3]='.';
		message_buffer[sizeof(message_buffer)-2]='.';
		message_buffer[sizeof(message_buffer)-1]='\0';
	}

	fprintf(stderr, "%s[%s] [%s]%s %s: %s\n", level_color_str, time_str, level_str, ANSI_COLOR_RESET, function, message_buffer);

	if(log_file) {
		fprintf(log_file, "[%s] [%s] %s: %s\n", time_str, level_str, function, message_buffer);
		fflush(log_file);
	}

#ifdef _WIN32
	ReleaseMutex(log_mutex);
#else
	pthread_mutex_unlock(&log_mutex);
#endif
}
#pragma endregion

#pragma region Directory

static bool has_ext(const char* name, const char* const exts[]) {
	const char* dot=strrchr(name, '.');
	if(!dot) return false;
	for(int i=0; exts[i]; ++i) {
		if(strcasecmp(dot, exts[i])==0) return true;
	}
	return false;
}

static void path_join(char* out, const char* a, const char* b) {
	snprintf(out, PATH_MAX, "%s%s%s", a, DIR_SEP_STR, b);
}

static bool is_file(const char* p) {
#ifdef _WIN32
	DWORD attr=GetFileAttributesA(p);
	return (attr!=INVALID_FILE_ATTRIBUTES)&&!(attr&FILE_ATTRIBUTE_DIRECTORY);
#else
	struct stat st;
	return stat(p, &st)==0&&S_ISREG(st.st_mode);
#endif
}

static bool is_dir(const char* p) {
#ifdef _WIN32
	DWORD attr=GetFileAttributesA(p);
	return (attr!=INVALID_FILE_ATTRIBUTES)&&(attr&FILE_ATTRIBUTE_DIRECTORY);
#else
	struct stat st;
	return stat(p, &st)==0&&S_ISDIR(st.st_mode);
#endif
}

static int mk_dir(const char* p) {
#ifdef _WIN32
	return _mkdir(p);
#else
	return mkdir(p, 0755);
#endif
}

static void normalize_path(char* p) {
	char* ptr=p;
	while(*ptr) {
#ifdef _WIN32
		if(*ptr=='/') *ptr=DIR_SEP;
#else
		if(*ptr=='\\') *ptr=DIR_SEP;
#endif
		ptr++;
	}
}

static bool real_path(const char* in, char* out) {
#ifdef _WIN32
	if(!_fullpath(out, in, PATH_MAX)) return false;
	normalize_path(out);
	return true;
#else
	char* r=realpath(in, out);
	if(!r) return false;
	normalize_path(out);
	return true;
#endif
}

static bool safe_under(const char* base_real, const char* path_real) {
	size_t n=strlen(base_real);
#ifdef _WIN32
	return _strnicmp(base_real, path_real, n)==0&&(path_real[n]==DIR_SEP||path_real[n]=='\0');
#else
	return strncmp(base_real, path_real, n)==0&&(path_real[n]==DIR_SEP||path_real[n]=='\0');
#endif
}

#ifdef _WIN32
typedef struct {
	HANDLE h;
	WIN32_FIND_DATAA ffd;
	char pattern[PATH_MAX];
	bool first;
} diriter;

static bool dir_open(diriter* it, const char* path) {
	snprintf(it->pattern, PATH_MAX, "%s\\*", path);
	it->h=FindFirstFileA(it->pattern, &it->ffd);
	it->first=(it->h!=INVALID_HANDLE_VALUE);
	return it->h!=INVALID_HANDLE_VALUE;
}

static const char* dir_next(diriter* it) {
	if(!it->first) {
		if(!FindNextFileA(it->h, &it->ffd)) return NULL;
	}
	it->first=false;
	return it->ffd.cFileName;
}

static void dir_close(diriter* it) {
	if(it->h!=INVALID_HANDLE_VALUE) FindClose(it->h);
}
#else
typedef struct {
	DIR* d;
	struct dirent* e;
} diriter;

static bool dir_open(diriter* it, const char* path) {
	it->d=opendir(path);
	return it->d!=NULL;
}

static const char* dir_next(diriter* it) {
	it->e=readdir(it->d);
	return it->e ? it->e->d_name : NULL;
}

static void dir_close(diriter* it) {
	if(it->d) closedir(it->d);
}
#endif
#pragma endregion

#pragma region HTTP
static const char* mime_for(const char* p) {
	const char* e=strrchr(p, '.');
	if(!e) return "application/octet-stream";
	e++;
	if(!strcasecmp(e, "js")) return "application/javascript";
	if(!strcasecmp(e, "css")) return "text/css";
	if(!strcasecmp(e, "html")||!strcasecmp(e, "htm")) return "text/html; charset=utf-8";
	if(!strcasecmp(e, "json")) return "application/json; charset=utf-8";
	if(!strcasecmp(e, "png")) return "image/png";
	if(!strcasecmp(e, "jpg")||!strcasecmp(e, "jpeg")) return "image/jpeg";
	if(!strcasecmp(e, "gif")) return "image/gif";
	if(!strcasecmp(e, "webp")) return "image/webp";
	if(!strcasecmp(e, "svg")) return "image/svg+xml";
	if(!strcasecmp(e, "mp4")) return "video/mp4";
	if(!strcasecmp(e, "webm")) return "video/webm";
	if(!strcasecmp(e, "ogg")) return "video/ogg";
	return "application/octet-stream";
}

typedef struct {
	bool is_range;
	long start;
	long end;
} range_t;

static char* get_header_value(char* request_buf, const char* header_name) {
	char* line=strstr(request_buf, "\r\n");
	while(line) {
		char* next_line=strstr(line+2, "\r\n");
		if(!next_line) break;
		*next_line='\0';
		if(strncasecmp(line+2, header_name, strlen(header_name))==0) {
			char* value_start=line+2+strlen(header_name);
			while(isspace(*value_start)) value_start++;
			char* value_end=value_start+strlen(value_start)-1;
			while(value_end>value_start&&isspace(*value_end)) value_end--;
			*(value_end+1)='\0';
			return value_start;
		}
		*next_line='\r';
		line=next_line;
	}
	return NULL;
}

static void send_header(int c, int status, const char* text, const char* ctype, long len, const range_t* range, long file_size, bool keep_alive) {
	char h[1024];
	int offset=snprintf(h, sizeof(h), "HTTP/1.1 %d %s\r\nConnection: %s\r\nContent-Type: %s\r\n",
		status, text, keep_alive ? "keep-alive" : "close", ctype);
	if(strstr(ctype, "image/")||strstr(ctype, "video/")) {
		offset+=snprintf(h+offset, sizeof(h)-offset,
			"Content-Disposition: inline\r\n");
	}
	if(keep_alive) {
		offset+=snprintf(h+offset, sizeof(h)-offset, "Keep-Alive: timeout=%d, max=100\r\n", KEEP_ALIVE_TIMEOUT_SEC);
	}
	if(range&&range->is_range) {
		offset+=snprintf(h+offset, sizeof(h)-offset,
			"Content-Range: bytes %ld-%ld/%ld\r\n", range->start, range->end, file_size);
		offset+=snprintf(h+offset, sizeof(h)-offset,
			"Content-Length: %ld\r\n", range->end-range->start+1);
	}
	else {
		offset+=snprintf(h+offset, sizeof(h)-offset, "Content-Length: %ld\r\n", len);
	}
	snprintf(h+offset, sizeof(h)-offset, "\r\n");
	send(c, h, (int)strlen(h), 0);
	LOG_DEBUG("Sent response header: %d %s, Content-Type: %s", status, text, ctype);
}

static void send_text(int c, int status, const char* text, const char* body, bool keep_alive) {
	LOG_INFO("Sending status %d %s with body: %s", status, text, body);
	send_header(c, status, text, "text/plain; charset=utf-8", (long)strlen(body), NULL, 0, keep_alive);
	send(c, body, (int)strlen(body), 0);
}

static range_t parse_range_header(const char* header_value, long file_size) {
	range_t range={ .is_range=false, .start=0, .end=0 };

	if(!header_value||strncmp(header_value, "bytes=", 6)!=0) {
		return range;
	}
	const char* p=header_value+6;
	char* dash=strchr(p, '-');
	if(!dash) {
		return range;
	}
	if(p!=dash) {
		char* endptr;
		range.start=strtol(p, &endptr, 10);
		if(endptr!=dash||range.start<0) {
			return range;
		}
	}
	else {
		range.start=-1;
	}
	if(*(dash+1)!='\0') {
		char* endptr;
		range.end=strtol(dash+1, &endptr, 10);
		if(*endptr!='\0'||range.end<0) {
			return range;
		}
	}
	else {
		range.end=-1;
	}
	if(range.start!=-1) {
		if(range.start>=file_size) {
			return range;
		}

		if(range.end==-1) {
			range.end=file_size-1;
		}
		else if(range.end>=file_size) {
			range.end=file_size-1;
		}

		if(range.start>range.end) {
			return range;
		}
	}
	else if(range.end!=-1) {
		range.start=file_size-range.end;
		if(range.start<0) range.start=0;
		range.end=file_size-1;
	}
	else {
		return range;
	}

	range.is_range=true;
	return range;
}

static void send_file_stream(int c, const char* fs_path, const char* range_header, bool keep_alive) {
	LOG_INFO("Serving file: %s", fs_path);
#ifdef _WIN32
	struct _stat64 st;
	if(_stat64(fs_path, &st)<0) {
#else
	struct stat st;
	if(stat(fs_path, &st)<0) {
#endif
		LOG_ERROR("Failed to stat file: %s", fs_path);
		send_text(c, 404, "Not Found", "Not found", keep_alive);
		return;
	}

	long file_size=(long)st.st_size;
	range_t range=parse_range_header(range_header, file_size);
	const char* ctype=mime_for(fs_path);

	long start_byte=0;
	long bytes_to_send=file_size;
	int status_code=200;
	const char* status_text="OK";

	if(range.is_range) {
		start_byte=range.start;
		bytes_to_send=range.end-range.start+1;
		status_code=206;
		status_text="Partial Content";
		LOG_INFO("Range request: %ld-%ld (%ld bytes)", range.start, range.end, bytes_to_send);
	}

	FILE* f=FOPEN_READ(fs_path);
	if(!f) {
		LOG_ERROR("Failed to open file: %s", fs_path);
		send_text(c, 404, "Not Found", "Not found", keep_alive);
		return;
	}
	if(range.is_range&&fseek(f, start_byte, SEEK_SET)!=0) {
		LOG_ERROR("fseek failed for range request");
		fclose(f);
		send_text(c, 416, "Range Not Satisfiable", "Range not satisfiable", keep_alive);
		return;
	}
	send_header(c, status_code, status_text, ctype, bytes_to_send,
		range.is_range ? &range : NULL, file_size, keep_alive);
	char buffer[16384];
	long remaining=bytes_to_send;
	size_t total_sent=0;

	while(remaining>0) {
		size_t to_read=(size_t)(remaining<sizeof(buffer) ? remaining : sizeof(buffer));
		size_t read=fread(buffer, 1, to_read, f);

		if(read==0) {
			if(ferror(f)) {
				LOG_ERROR("File read error");
				break;
			}
			break;
		}

		int sent=send(c, buffer, (int)read, 0);
		if(sent<=0) {
#ifdef _WIN32
			int error=WSAGetLastError();
			if(error==WSAEWOULDBLOCK||error==WSAEINTR) {
				continue;
			}
			LOG_ERROR("Send error: %d", error);
#else
			if(errno==EAGAIN||errno==EWOULDBLOCK||errno==EINTR) {
				continue;
			}
			LOG_ERROR("Send error: %s", strerror(errno));
#endif
			break;
		}

		remaining-=sent;
		total_sent+=sent;
	}

	fclose(f);

	if(remaining>0) {
		LOG_WARN("File transfer incomplete: %ld bytes remaining", remaining);
	}
	else {
		LOG_INFO("File transfer completed: %zu bytes sent", total_sent);
	}
	}
#pragma endregion

#pragma region Utils
static const unsigned char CharToHex[256]={
	[0 ... 255] =0xFF,
	['0']=0,['1']=1,['2']=2,['3']=3,['4']=4,
	['5']=5,['6']=6,['7']=7,['8']=8,['9']=9,
	['A']=10,['B']=11,['C']=12,['D']=13,['E']=14,['F']=15,
	['a']=10,['b']=11,['c']=12,['d']=13,['e']=14,['f']=15,
};

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

static char* query_get(char* qs, const char* key) {
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

static int ci_cmp(const void* a, const void* b) {
	const char* s1=*(const char* const*)a;
	const char* s2=*(const char* const*)b;
#ifdef _WIN32
	return _stricmp(s1, s2);
#else
	return strcasecmp(s1, s2);
#endif
}

static void sb_append(char** buf, size_t*cap, size_t*len, const char* s) {
	size_t add=strlen(s);
	if(*len+add+1>=*cap) {
		*cap=*cap*2+add+64;
		*buf=realloc(*buf, *cap);
	}
	memcpy(*buf+*len, s, add);
	*len+=add;
	(*buf)[*len]='\0';
}

static void sb_append_esc(char** buf, size_t*cap, size_t*len, const char* s) {
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
#pragma endregion

#pragma region API_Handlers
static bool has_nogallery(const char* dir) {
	char p[PATH_MAX];
	path_join(p, dir, ".nogallery");
	return is_file(p);
}

static bool has_media_rec(const char* dir) {
	if(has_nogallery(dir)) return false;
	diriter it;
	if(!dir_open(&it, dir)) {
		LOG_WARN("Could not open directory for scan: %s", dir);
		return false;
	}
	const char* name;
	bool found=false;
	while((name=dir_next(&it))&&!found) {
		if(!strcmp(name, ".")||!strcmp(name, "..")) continue;
		char full[PATH_MAX];
		path_join(full, dir, name);
		if(is_dir(full)) {
			found=has_media_rec(full);
		}
		else if(is_file(full)) {
			found=has_ext(name, IMAGE_EXTS)||has_ext(name, VIDEO_EXTS);
		}
	}
	dir_close(&it);
	return found;
}

static void build_folder_tree(const char* dir, const char* root, char** buf, size_t*cap, size_t*len) {
	//LOG_DEBUG ("Building folder tree for %s", dir);
	if(!is_dir(dir)||has_nogallery(dir)||!has_media_rec(dir)) {
		sb_append(buf, cap, len, "null");
		return;
	}
	const char* base=strrchr(dir, DIR_SEP);
	base=base ? base+1 : dir;
	sb_append(buf, cap, len, "{\"name\":\"");
	sb_append_esc(buf, cap, len, base);
	sb_append(buf, cap, len, "\",");
	char rroot[PATH_MAX], rdir[PATH_MAX];
	real_path(root, rroot);
	real_path(dir, rdir);
	const char* r=rdir;
	size_t rl=strlen(rroot);
	if(safe_under(rroot, rdir)) {
		r=rdir+rl+((rdir[rl]==DIR_SEP) ? 1 : 0);
	}
	char relurl[PATH_MAX];
	size_t j=0;
	for(size_t i=0; r[i]&&j<PATH_MAX-1; ++i) {
		char ch=r[i];
		if(ch=='\\') ch='/';
		relurl[j++]=ch;
	}
	relurl[j]='\0';
	sb_append(buf, cap, len, "\"path\":\"");
	sb_append_esc(buf, cap, len, relurl);
	sb_append(buf, cap, len, "\",\"children\":[");
	char** names=NULL;
	size_t n=0, alloc=0;
	diriter it;
	if(dir_open(&it, dir)) {
		const char* name;
		while((name=dir_next(&it))) {
			if(!strcmp(name, ".")||!strcmp(name, "..")) continue;
			char full[PATH_MAX];
			path_join(full, dir, name);
			if(is_dir(full)&&!has_nogallery(full)&&has_media_rec(full)) {
				if(n==alloc) {
					alloc=alloc ? alloc*2 : 16;
					names=realloc(names, alloc*sizeof(char*));
				}
				names[n++]=strdup(name);
			}
		}
		dir_close(&it);
	}
	qsort(names, n, sizeof(char*), ci_cmp);
	for(size_t i=0; i<n; i++) {
		if(i) sb_append(buf, cap, len, ",");
		char child[PATH_MAX];
		path_join(child, dir, names[i]);
		build_folder_tree(child, root, buf, cap, len);
		SAFE_FREE(names[i]);
	}
	SAFE_FREE(names);
	sb_append(buf, cap, len, "]}");
}

static void handle_api_tree(int c, bool keep_alive) {
	LOG_INFO("Handling API request for /api/tree");
	size_t cap=4096, len=0;
	char* out=malloc(cap);
	out[0]='\0';
	build_folder_tree(BASE_DIR, BASE_DIR, &out, &cap, &len);
	send_header(c, 200, "OK", "application/json; charset=utf-8", (long)len, NULL, 0, keep_alive);
	send(c, out, (int)len, 0);
	SAFE_FREE(out);
}

static void handle_api_folders(int c, char* qs, bool keep_alive) {
	LOG_INFO("Handling API request for /api/folders");
	char dirparam[PATH_MAX]={ 0 };
	if(qs) {
		char* v=query_get(qs, "dir");
		if(v) {
			STRCPY(dirparam, v);
			SAFE_FREE(v);
			LOG_DEBUG("Query parameter 'dir' is: %s", dirparam);
		}
	}
	char target[PATH_MAX];
	snprintf(target, sizeof(target), "%s/%s", BASE_DIR, dirparam);
	normalize_path(target);
	char base_real[PATH_MAX], target_real[PATH_MAX];
	if(!real_path(BASE_DIR, base_real)||!real_path(target, target_real)||
		!safe_under(base_real, target_real)||!is_dir(target_real)) {
		LOG_ERROR("Invalid directory access attempt: %s", target);
		const char* msg="{\"error\":\"Invalid directory\"}";
		send_header(c, 400, "Bad Request", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
		send(c, msg, (int)strlen(msg), 0);
		return;
	}
	char** names=NULL;
	size_t n=0, alloc=0;
	diriter it;
	if(dir_open(&it, target_real)) {
		const char* name;
		while((name=dir_next(&it))) {
			if(!strcmp(name, ".")||!strcmp(name, "..")) continue;
			char full[PATH_MAX];
			path_join(full, target_real, name);
			if(is_dir(full)&&!has_nogallery(full)&&has_media_rec(full)) {
				if(n==alloc) {
					alloc=alloc ? alloc*2 : 16;
					names=realloc(names, alloc*sizeof(char*));
				}
				names[n++]=strdup(name);
			}
		}
		dir_close(&it);
	}
	qsort(names, n, sizeof(char*), ci_cmp);
	size_t cap=4096, len=0;
	char* out=malloc(cap);
	out[0]='\0';
	sb_append(&out, &cap, &len, "{\"content\":[");
	for(size_t i=0; i<n; i++) {
		if(i) sb_append(&out, &cap, &len, ",");
		sb_append(&out, &cap, &len, "{\"name\":\"");
		sb_append_esc(&out, &cap, &len, names[i]);
		sb_append(&out, &cap, &len, "\",\"path\":\"");
		char tmp[PATH_MAX];
		path_join(tmp, target_real, names[i]);
		char tr[PATH_MAX], br[PATH_MAX];
		real_path(tmp, tr);
		real_path(BASE_DIR, br);
		const char* r=tr+strlen(br)+((tr[strlen(br)]==DIR_SEP) ? 1 : 0);
		char relurl[PATH_MAX];
		size_t j=0;
		for(size_t k=0; r[k]&&j<PATH_MAX-1; k++) {
			char ch=r[k];
			if(ch=='\\') ch='/';
			relurl[j++]=ch;
		}
		relurl[j]='\0';
		sb_append_esc(&out, &cap, &len, relurl);
		sb_append(&out, &cap, &len, "\"}");
		SAFE_FREE(names[i]);
	}
	SAFE_FREE(names);
	sb_append(&out, &cap, &len, "],");
	sb_append(&out, &cap, &len, "\"currentDir\":\"");
	sb_append_esc(&out, &cap, &len, dirparam);
	sb_append(&out, &cap, &len, "\",\"isRoot\":");
	sb_append(&out, &cap, &len, IS_EMPTY_STR(dirparam) ? "true" : "false");
	sb_append(&out, &cap, &len, "}");
	send_header(c, 200, "OK", "application/json; charset=utf-8", (long)len, NULL, 0, keep_alive);
	send(c, out, (int)len, 0);
	SAFE_FREE(out);
}

static void handle_api_media(int c, char* qs, bool keep_alive) {
	LOG_INFO("Handling API request for /api/media");
	char dirparam[PATH_MAX]={ 0 };
	int page=1;
	if(qs) {
		char* v=query_get(qs, "dir");
		if(v) {
			STRCPY(dirparam, v);
			SAFE_FREE(v);
			LOG_DEBUG("Query parameter 'dir' is: %s", dirparam);
		}
		char* p=query_get(qs, "page");
		if(p) {
			int t=atoi(p);
			if(t>0) page=t;
			SAFE_FREE(p);
			LOG_DEBUG("Query parameter 'page' is: %d", page);
		}
	}
	char target[PATH_MAX];
	snprintf(target, sizeof(target), "%s/%s", BASE_DIR, dirparam);
	normalize_path(target);
	char base_real[PATH_MAX], target_real[PATH_MAX];
	if(!real_path(BASE_DIR, base_real)||!real_path(target, target_real)||
		!safe_under(base_real, target_real)||!is_dir(target_real)) {
		LOG_ERROR("Invalid directory access attempt: %s", target);
		const char* msg="{\"error\":\"Invalid directory\"}";
		send_header(c, 400, "Bad Request", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
		send(c, msg, (int)strlen(msg), 0);
		return;
	}
	char** files=NULL;
	size_t n=0, alloc=0;
	diriter it;
	if(dir_open(&it, target_real)) {
		const char* name;
		while((name=dir_next(&it))) {
			if(!strcmp(name, ".")||!strcmp(name, "..")) continue;
			if(has_ext(name, IMAGE_EXTS)||has_ext(name, VIDEO_EXTS)) {
				if(n==alloc) {
					alloc=alloc ? alloc*2 : 64;
					files=realloc(files, alloc*sizeof(char*));
				}
				files[n++]=strdup(name);
			}
		}
		dir_close(&it);
	}
	qsort(files, n, sizeof(char*), ci_cmp);
	int total=(int)n;
	int totalPages=(total+ITEMS_PER_PAGE-1)/ITEMS_PER_PAGE;
	if(totalPages==0) totalPages=1;
	if(page<1) page=1;
	if(page>totalPages) page=totalPages;
	int start=(page-1)*ITEMS_PER_PAGE;
	if(start>total) start=total;
	int end=MIN(start+ITEMS_PER_PAGE, total);
	size_t cap=4096, len=0;
	char* out=malloc(cap);
	out[0]='\0';
	sb_append(&out, &cap, &len, "{\"items\":[");
	for(int i=start; i<end; i++) {
		if(i>start) sb_append(&out, &cap, &len, ",");
		const char* filename=files[i];
		char full[PATH_MAX];
		path_join(full, target_real, filename);
		char tr[PATH_MAX], br[PATH_MAX];
		real_path(full, tr);
		real_path(BASE_DIR, br);
		const char* r=tr+strlen(br)+((tr[strlen(br)]==DIR_SEP) ? 1 : 0);
		char relurl[PATH_MAX];
		size_t j=0;
		for(size_t k=0; r[k]&&j<PATH_MAX-1; k++) {
			char ch=r[k];
			if(ch=='\\') ch='/';
			relurl[j++]=ch;
		}
		relurl[j]='\0';
		const char* type=has_ext(filename, IMAGE_EXTS) ? "image" : "video";
		sb_append(&out, &cap, &len, "{\"path\":\"");
		sb_append_esc(&out, &cap, &len, relurl);
		sb_append(&out, &cap, &len, "\",\"filename\":\"");
		sb_append_esc(&out, &cap, &len, filename);
		sb_append(&out, &cap, &len, "\",\"type\":\"");
		sb_append(&out, &cap, &len, type);
		sb_append(&out, &cap, &len, "\"}");
	}
	for(size_t i=0; i<n; i++) SAFE_FREE(files[i]);
	SAFE_FREE(files);
	sb_append(&out, &cap, &len, "],");
	char tmp[256];
	snprintf(tmp, sizeof(tmp), "\"total\":%d,\"page\":%d,\"totalPages\":%d,\"hasMore\":%s}",
		total, page, totalPages, (page<totalPages) ? "true" : "false");
	sb_append(&out, &cap, &len, tmp);
	send_header(c, 200, "OK", "application/json; charset=utf-8", (long)len, NULL, 0, keep_alive);
	send(c, out, (int)len, 0);
	SAFE_FREE(out);
}

static void handle_single_request(int c, char* request_buf, bool keep_alive) {
	char method[8]={ 0 }, url[PATH_MAX]={ 0 };
	if(sscanf(request_buf, "%7s %4095s", method, url)!=2) {
		LOG_WARN("Malformed request received.");
		send_text(c, 400, "Bad Request", "Malformed request", keep_alive);
		return;
	}
	LOG_INFO("Request received: %s %s", method, url);
	if(strcmp(method, "GET")!=0) {
		send_text(c, 400, "Bad Request", "Only GET supported", keep_alive);
		LOG_WARN("Unsupported HTTP method: %s", method);
		return;
	}
	char* qs=strchr(url, '?');
	if(qs) {
		*qs='\0';
		qs++;
	}
	url_decode(url);
	char* range_header_value=get_header_value(request_buf, "Range:");

	if(strcmp(url, "/")==0) {
		char p[PATH_MAX];
		snprintf(p, sizeof(p), "%s/index.html", VIEWS_DIR);
		if(is_file(p)) send_file_stream(c, p, NULL, keep_alive);
		else send_text(c, 404, "Not Found", "index.html not found", keep_alive);
		return;
	}
	if(strcmp(url, "/api/folders")==0) {
		handle_api_folders(c, qs, keep_alive);
		return;
	}
	if(strcmp(url, "/api/tree")==0) {
		handle_api_tree(c, keep_alive);
		return;
	}
	if(strcmp(url, "/api/media")==0) {
		handle_api_media(c, qs, keep_alive);
		return;
	}
	if(strncmp(url, "/images/", 8)==0) {
		char rel[PATH_MAX];
		snprintf(rel, sizeof(rel), "%s/%s", BASE_DIR, url+8);
		normalize_path(rel);
		char base_real[PATH_MAX], target_real[PATH_MAX];
		if(real_path(BASE_DIR, base_real)&&real_path(rel, target_real)&&
			safe_under(base_real, target_real)&&is_file(target_real)) {
			send_file_stream(c, target_real, range_header_value, keep_alive);
		}
		else {
			LOG_ERROR("File not found or invalid path: %s", rel);
			send_text(c, 404, "Not Found", "Not found", keep_alive);
		}
		return;
	}
	if(strncmp(url, "/js/", 4)==0) {
		char rel[PATH_MAX];
		snprintf(rel, sizeof(rel), "%s/%s", JS_DIR, url+4);
		normalize_path(rel);
		char base_real[PATH_MAX], target_real[PATH_MAX];
		if(real_path(JS_DIR, base_real)&&real_path(rel, target_real)&&
			safe_under(base_real, target_real)&&is_file(target_real)) {
			send_file_stream(c, target_real, NULL, keep_alive);
		}
		else {
			LOG_ERROR("JS file not found or invalid path: %s", rel);
			send_text(c, 404, "Not Found", "Not found", keep_alive);
		}
		return;
	}
	if(strcmp(url, "/bundled")==0) {
		if(is_file(BUNDLED_FILE)) send_file_stream(c, BUNDLED_FILE, NULL, keep_alive);
		else send_text(c, 404, "Not Found", "Not found", keep_alive);
		return;
	}
	if(strncmp(url, "/css/", 5)==0) {
		char rel[PATH_MAX];
		snprintf(rel, sizeof(rel), "%s/%s", CSS_DIR, url+5);
		normalize_path(rel);
		char base_real[PATH_MAX], target_real[PATH_MAX];
		if(real_path(CSS_DIR, base_real)&&real_path(rel, target_real)&&
			safe_under(base_real, target_real)&&is_file(target_real)) {
			send_file_stream(c, target_real, NULL, keep_alive);
		}
		else {
			LOG_ERROR("CSS file not found or invalid path: %s", rel);
			send_text(c, 404, "Not Found", "Not found", keep_alive);
		}
		return;
	}
	LOG_WARN("Unhandled URL: %s", url);
	send_text(c, 404, "Not Found", "Not found", keep_alive);
}
#pragma endregion

#pragma region SERVER
static int create_listen_socket(int port) {
	int s=socket(AF_INET, SOCK_STREAM, 0);
	if(s<0) {
		LOG_ERROR("Failed to create socket: %s", strerror(errno));
		exit(1);
	}
	int opt=1;
#ifndef _WIN32
	setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#else
	setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));
#endif
	struct sockaddr_in a;
	memset(&a, 0, sizeof(a));
	a.sin_family=AF_INET;
	a.sin_addr.s_addr=htonl(INADDR_ANY);
	a.sin_port=htons((unsigned short)port);
	if(bind(s, (struct sockaddr*)&a, sizeof(a))<0) {
		LOG_ERROR("Failed to bind socket to port %d: %s", port, strerror(errno));
		exit(1);
	}
	if(listen(s, 1024)<0) {
		LOG_ERROR("Failed to listen on socket: %s", strerror(errno));
		exit(1);
	}
	LOG_INFO("Created listening socket on port %d", port);
	return s;
}

static void join_path(char* dest, size_t size, const char* base, const char* sub) {
	size_t len=strlen(base);
	if(len>0&&(base[len-1]=='/'||base[len-1]=='\\')) {
		snprintf(dest, size, "%s%s", base, sub);
	}
	else {
		snprintf(dest, size, "%s%c%s", base, DIR_SEP, sub);
	}
}

static void derive_paths(const char* argv0) {
	char exe_path[PATH_MAX];

#ifdef _WIN32
	if(!GetModuleFileNameA(NULL, exe_path, PATH_MAX)) exit(1);
#else
	if(argv0[0]=='/'||argv0[0]=='.') {
		if(!realpath(argv0, exe_path)) exit(1);
	}
	else {
		char* path_env=getenv("PATH");
		if(!path_env) exit(1);
		char* p=strdup(path_env);
		char* dir=strtok(p, ":");
		int found=0;
		while(dir) {
			snprintf(exe_path, PATH_MAX, "%s/%s", dir, argv0);
			if(access(exe_path, X_OK)==0) {
				found=1; break;
			}
			dir=strtok(NULL, ":");
		}
		free(p);
		if(!found) exit(1);
	}
#endif

	char* slash=strrchr(exe_path, DIR_SEP);
	if(slash) *slash='\0';

	strncpy(BASE_DIR, "E:\\F", PATH_MAX);

	join_path(VIEWS_DIR, PATH_MAX, exe_path, "views");
	join_path(JS_DIR, PATH_MAX, exe_path, "public/js");
	join_path(CSS_DIR, PATH_MAX, exe_path, "public/css");
	join_path(BUNDLED_FILE, PATH_MAX, exe_path, "public/bundle/f.js");

	normalize_path(BASE_DIR);
	normalize_path(VIEWS_DIR);
	normalize_path(JS_DIR);
	normalize_path(CSS_DIR);
	normalize_path(BUNDLED_FILE);

	if(!is_dir(BASE_DIR)) {
		LOG_ERROR("Base directory '%s' does not exist", BASE_DIR);
		LOG_DEBUG("BASE_DIR=%s", BASE_DIR);
	}
	LOG_DEBUG("BASE_DIR=%s", BASE_DIR);
	LOG_DEBUG("VIEWS_DIR=%s", VIEWS_DIR);
	LOG_DEBUG("JS_DIR=%s", JS_DIR);
	LOG_DEBUG("CSS_DIR=%s", CSS_DIR);
	LOG_DEBUG("BUNDLED_FILE=%s", BUNDLED_FILE);
}

#pragma endregion

#pragma region Thread_Pool
#ifdef _WIN32
#define QUEUE_CAP 1024
static int* job_ring;
static int job_head=0;
static int job_tail=0;
static int job_count=0;
static HANDLE job_mutex;
static HANDLE job_not_empty;
static HANDLE job_not_full;

static void enqueue_job(int client_socket) {
	WaitForSingleObject(job_mutex, INFINITE);
	while(job_count==QUEUE_CAP) {
		LOG_WARN("Job queue is full, waiting...");
		ReleaseMutex(job_mutex);
		WaitForSingleObject(job_not_full, INFINITE);
		WaitForSingleObject(job_mutex, INFINITE);
	}
	job_ring[job_tail]=client_socket;
	job_tail=(job_tail+1)%QUEUE_CAP;
	job_count++;
	ReleaseMutex(job_mutex);
	ReleaseSemaphore(job_not_empty, 1, NULL);
	LOG_DEBUG("Enqueued client socket %d", client_socket);
}

static int dequeue_job(void) {
	WaitForSingleObject(job_not_empty, INFINITE);
	WaitForSingleObject(job_mutex, INFINITE);
	int c=job_ring[job_head];
	job_head=(job_head+1)%QUEUE_CAP;
	job_count--;
	ReleaseMutex(job_mutex);
	ReleaseSemaphore(job_not_full, 1, NULL);
	LOG_DEBUG("Dequeued client socket %d", c);
	return c;
}

static unsigned __stdcall worker_thread(void* arg) {
	(void)arg;
	for(;;) {
		int c=dequeue_job();
		char buf[8192];
		while(true) {
			int r=recv(c, buf, sizeof(buf)-1, 0);
			if(r>0) {
				buf[r]='\0';
				LOG_DEBUG("Received %d bytes from client %d", r, c);
				handle_single_request(c, buf, true);
			}
			else {
				LOG_DEBUG("Client %d disconnected or timed out.", c);
				break;
			}
			if(strstr(buf, "Connection: close")||strstr(buf, "connection: close")) {
				LOG_INFO("Client %d requested connection close.", c);
				break;
			}
		}
		SOCKET_CLOSE(c);
		LOG_INFO("Closed connection for client %d", c);
	}
	return 0;
}

static int get_worker_count(void) {
	SYSTEM_INFO si;
	GetSystemInfo(&si);
	int n=(int)si.dwNumberOfProcessors;
	if(n<1) n=1;
	int w=n*2;
	if(w<4) w=4;
	return w;
}

static void start_thread_pool(int nworkers) {
	if(nworkers<=0) nworkers=get_worker_count();
	LOG_INFO("Starting thread pool with %d workers", nworkers);
	job_ring=calloc(QUEUE_CAP, sizeof(int));
	job_mutex=CreateMutex(NULL, FALSE, NULL);
	job_not_empty=CreateSemaphore(NULL, 0, QUEUE_CAP, NULL);
	job_not_full=CreateSemaphore(NULL, QUEUE_CAP, QUEUE_CAP, NULL);
	for(int i=0; i<nworkers; ++i) {
		uintptr_t th=_beginthreadex(NULL, 0, worker_thread, NULL, 0, NULL);
		CloseHandle((HANDLE)th);
	}
}

#else
// POSIX / Linux version
#define DEFAULT_WORKERS 0
#define QUEUE_CAP 4096
static int* job_ring;
static int job_head=0;
static int job_tail=0;
static int job_count=0;
static pthread_mutex_t job_mutex=PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t job_not_empty=PTHREAD_COND_INITIALIZER;
static pthread_cond_t job_not_full=PTHREAD_COND_INITIALIZER;

static void enqueue_job(int client_socket) {
	MUTEX_LOCK(job_mutex);
	while(job_count==QUEUE_CAP) {
		LOG_WARN("Job queue is full, waiting...");
		pthread_cond_wait(&job_not_full, &job_mutex);
	}
	job_ring[job_tail]=client_socket;
	job_tail=(job_tail+1)%QUEUE_CAP;
	job_count++;
	pthread_cond_signal(&job_not_empty);
	MUTEX_UNLOCK(job_mutex);
	LOG_DEBUG("Enqueued client socket %d", client_socket);
}

static int dequeue_job(void) {
	MUTEX_LOCK(job_mutex);
	while(job_count==0) {
		pthread_cond_wait(&job_not_empty, &job_mutex);
	}
	int c=job_ring[job_head];
	job_head=(job_head+1)%QUEUE_CAP;
	job_count--;
	pthread_cond_signal(&job_not_full);
	MUTEX_UNLOCK(job_mutex);
	LOG_DEBUG("Dequeued client socket %d", c);
	return c;
}

static void* worker_thread(void* arg) {
	(void)arg;
	LOG_INFO("Worker thread started.");
	for(;;) {
		int c=dequeue_job();
		char buf[8192];
		while(true) {
			int r=recv(c, buf, sizeof(buf)-1, 0);
			if(r>0) {
				buf[r]='\0';
				LOG_DEBUG("Received %d bytes from client %d", r, c);
				handle_single_request(c, buf, true);
			}
			else {
				LOG_DEBUG("Client %d disconnected or timed out.", c);
				break;
			}
			if(strstr(buf, "Connection: close")||strstr(buf, "connection: close")) {
				LOG_INFO("Client %d requested connection close.", c);
				break;
			}
		}
		SOCKET_CLOSE(c);
		LOG_INFO("Closed connection for client %d", c);
	}
	return NULL;
}

static int get_worker_count(void) {
	int n=(int)sysconf(_SC_NPROCESSORS_ONLN);
	if(n<1) n=1;
	int w=n*2;
	if(w<4) w=4;
	return w;
}

static void start_thread_pool(int nworkers) {
	if(nworkers<=0) nworkers=get_worker_count();
	LOG_INFO("Starting thread pool with %d workers", nworkers);
	job_ring=calloc(QUEUE_CAP, sizeof(int));
	for(int i=0; i<nworkers; ++i) {
		pthread_t th;
		pthread_create(&th, NULL, worker_thread, NULL);
		pthread_detach(th);
	}
}
#endif
#pragma endregion

int main(int argc, char* argv[]) {
	log_init();
#ifdef _WIN32
	WSADATA w;
	WSAStartup(MAKEWORD(2, 2), &w);
#endif
	derive_paths(argv[0]);
	LOG_INFO("Checking for media files in: %s", BASE_DIR);
	if(has_media_rec(BASE_DIR)) {
	}
	else {
		LOG_WARN("No media files found in base directory.");
	}
	int port=3000;
	int s=create_listen_socket(port);
	LOG_INFO("Gallery server running on http://localhost:%d", port);
	start_thread_pool(0);

	for(;;) {
		struct sockaddr_in ca;
		socklen_t calen=sizeof(ca);
		LOG_INFO("Waiting for a new client connection...");
		int c=accept(s, (struct sockaddr*)&ca, &calen);
		if(c<0) {
			LOG_ERROR("Accept failed: %s", strerror(errno));
			continue;
		}

		char ip_str[INET_ADDRSTRLEN];
		inet_ntop(AF_INET, &(ca.sin_addr), ip_str, INET_ADDRSTRLEN);
		LOG_INFO("Accepted connection from %s:%d (socket %d)", ip_str, ntohs(ca.sin_port), c);
#ifdef _WIN32
		DWORD timeout=KEEP_ALIVE_TIMEOUT_SEC*1000;
		setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));
		timeout=30000;
		setsockopt(c, SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeout, sizeof(timeout));
#else
		struct timeval timeout;
		timeout.tv_sec=KEEP_ALIVE_TIMEOUT_SEC;
		timeout.tv_usec=0;
		setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
		timeout.tv_sec=30;
		timeout.tv_usec=0;
		setsockopt(c, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#endif

		int keepalive=1;
		setsockopt(c, SOL_SOCKET, SO_KEEPALIVE,
#ifdef _WIN32
		(const char*)&keepalive,
#else
			&keepalive,
#endif
			sizeof(keepalive));

#ifndef _WIN32
#ifdef __linux__
		int keepidle=60;
		int keepintvl=10;
		int keepcnt=3;
		setsockopt(c, IPPROTO_TCP, TCP_KEEPIDLE, &keepidle, sizeof(keepidle));
		setsockopt(c, IPPROTO_TCP, TCP_KEEPINTVL, &keepintvl, sizeof(keepintvl));
		setsockopt(c, IPPROTO_TCP, TCP_KEEPCNT, &keepcnt, sizeof(keepcnt));
#endif
#endif

		int nodelay=1;
		setsockopt(c, IPPROTO_TCP, TCP_NODELAY,
#ifdef _WIN32
		(const char*)&nodelay,
#else
			&nodelay,
#endif
			sizeof(nodelay));

		int buf_size=256*1024;
		setsockopt(c, SOL_SOCKET, SO_SNDBUF,
#ifdef _WIN32
		(const char*)&buf_size,
#else
			&buf_size,
#endif
			sizeof(buf_size));
		setsockopt(c, SOL_SOCKET, SO_RCVBUF,
#ifdef _WIN32
		(const char*)&buf_size,
#else
			&buf_size,
#endif
			sizeof(buf_size));

#ifdef _WIN32
		enqueue_job(c);
#else
		pthread_mutex_lock(&job_mutex);
		if(job_count==QUEUE_CAP) {
			pthread_mutex_unlock(&job_mutex);
			LOG_WARN("Connection refused, job queue is full.");
			const char* msg=
				"HTTP/1.1 503 Service Unavailable\r\n"
				"Connection: close\r\n"
				"Content-Length: 19\r\n\r\n"
				"Service Unavailable";
			send(c, msg, (int)strlen(msg), 0);
			close(c);
		}
		else {
			pthread_mutex_unlock(&job_mutex);
			enqueue_job(c);
		}
#endif
	}

#ifdef _WIN32
	WSACleanup();
#endif
	return 0;
}
