#include "platform.h"
#include "common.h"
#include <stdio.h>
#ifdef _WIN32
#include <Windows.h>
#else
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#endif

void platform_sleep_ms(int ms) {
#ifdef _WIN32
    Sleep(ms);
#else
    struct timespec ts; ts.tv_sec = ms / 1000; ts.tv_nsec = (ms % 1000) * 1000000; nanosleep(&ts, NULL);
#endif
}

int platform_file_delete(const char* path) {
    if (!path) return -1;
#ifdef _WIN32
    return DeleteFileA(path) ? 0 : -1;
#else
    return unlink(path) == 0 ? 0 : -1;
#endif
}

int platform_make_dir(const char* path) {
    if (!path) return -1;
#ifdef _WIN32
    if (CreateDirectoryA(path, NULL)) return 0;
    DWORD e = GetLastError();
    if (e == ERROR_ALREADY_EXISTS) return 0;
    return -1;
#else
    if (mkdir(path, 0755) == 0) return 0;
    if (errno == EEXIST) return 0;
    return -1;
#endif
}

int platform_file_exists(const char* path) {
    if (!path) return 0;
#ifdef _WIN32
    DWORD attrib = GetFileAttributesA(path);
    return (attrib != INVALID_FILE_ATTRIBUTES && !(attrib & FILE_ATTRIBUTE_DIRECTORY));
#else
    struct stat st; return (stat(path, &st) == 0 && S_ISREG(st.st_mode));
#endif
}