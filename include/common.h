#pragma once

#ifdef _WIN32

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <mswsock.h>
#include <io.h>
#include <fcntl.h>
#include <direct.h>
#include <process.h>
#include <sys/types.h>
#include <sys/stat.h>
typedef struct _stat stat_t;
#define stat_fn _stat
#define STAT_FUNC _stat64
#define SOCKET_CLOSE(s) closesocket(s)
#define MUTEX_LOCK(m) WaitForSingleObject(m, INFINITE)
#define MUTEX_UNLOCK(m) ReleaseMutex(m)
#define DIR_SEP '\\'
#define DIR_SEP_STR "\\"
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
#define STAT_FUNC stat
#define SOCKET_CLOSE(s) close(s)
#define MUTEX_LOCK(m) pthread_mutex_lock(&m)
#define MUTEX_UNLOCK(m) pthread_mutex_unlock(&m)
#define DIR_SEP '/'
#define DIR_SEP_STR "/"
#define FOPEN_READ(path) fopen(path, "rb")
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#include <signal.h>
#include <assert.h>
#include <strings.h>

#include <ctype.h>
#include <math.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <time.h>
#include <errno.h>

#if defined(__linux__)
#include <execinfo.h>
#include <sys/syscall.h>
#endif

#ifdef _WIN32
#include <dbghelp.h>
#include <psapi.h>
#endif

#if defined(__AVX2__)
#include <immintrin.h>
#elif defined(__SSE2__)
#include <emmintrin.h>
#endif
#if defined(__ARM_NEON)
#include <arm_neon.h>
#endif

#define STRCPY(dest, src) do { strncpy(dest, src, sizeof(dest) - 1); dest[sizeof(dest) - 1] = '\0'; } while(0)
#define IS_EMPTY_STR(s) ((s)[0] == '\0')
#define MIN(a,b) ((a) < (b) ? (a) : (b))
#define MAX(a,b) ((a) > (b) ? (a) : (b))
#define SAFE_FREE(ptr) do { if(ptr) { free(ptr); ptr = NULL; } } while(0)
#define ARRAY_LEN(arr) (sizeof(arr) / sizeof((arr)[0]))

#define ITEMS_PER_PAGE 25
#define KEEP_ALIVE_TIMEOUT_SEC 180

#define ANSI_COLOR_BLACK           "\x1b[30m"
#define ANSI_COLOR_RED             "\x1b[31m"
#define ANSI_COLOR_GREEN           "\x1b[32m"
#define ANSI_COLOR_YELLOW          "\x1b[33m"
#define ANSI_COLOR_BLUE            "\x1b[34m"
#define ANSI_COLOR_MAGENTA         "\x1b[35m"
#define ANSI_COLOR_CYAN            "\x1b[36m"
#define ANSI_COLOR_WHITE           "\x1b[37m"
#define ANSI_COLOR_BRIGHT_BLACK    "\x1b[90m"
#define ANSI_COLOR_BRIGHT_RED      "\x1b[91m"
#define ANSI_COLOR_BRIGHT_GREEN    "\x1b[92m"
#define ANSI_COLOR_BRIGHT_YELLOW   "\x1b[93m"
#define ANSI_COLOR_BRIGHT_BLUE     "\x1b[94m"

#define ANSI_BG_YELLOW             "\x1b[43m"
#define ANSI_BG_BLUE               "\x1b[44m"
#define ANSI_BG_MAGENTA            "\x1b[45m"
#define ANSI_BG_CYAN               "\x1b[46m"
#define ANSI_BG_WHITE              "\x1b[47m"
#define ANSI_BG_BRIGHT_BLACK       "\x1b[100m"
#define ANSI_BG_BRIGHT_RED         "\x1b[101m"
#define ANSI_BG_BRIGHT_GREEN       "\x1b[102m"
#define ANSI_BG_BRIGHT_YELLOW      "\x1b[103m"
#define ANSI_BG_BRIGHT_BLUE        "\x1b[104m"
#define ANSI_BG_BRIGHT_MAGENTA     "\x1b[105m"
#define ANSI_BG_BRIGHT_CYAN        "\x1b[106m"
#define ANSI_BG_BRIGHT_WHITE       "\x1b[107m"

#define ANSI_COLOR_RESET           "\x1b[0m"
#define ANSI_COLOR_BOLD            "\x1b[1m"
#define ANSI_COLOR_DIM             "\x1b[2m"
#define ANSI_COLOR_UNDERLINE       "\x1b[4m"
#define ANSI_COLOR_REVERSE         "\x1b[7m"
#define ANSI_COLOR_HIDDEN          "\x1b[8m"

extern const char* IMAGE_EXTS[];
extern const char* VIDEO_EXTS[];

extern char BASE_DIR[PATH_MAX];
extern char VIEWS_DIR[PATH_MAX];
extern char JS_DIR[PATH_MAX];
extern char CSS_DIR[PATH_MAX];
extern char BUNDLED_FILE[PATH_MAX];

