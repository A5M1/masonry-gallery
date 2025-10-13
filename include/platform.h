#pragma once
#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

void platform_sleep_ms(int ms);
int platform_file_delete(const char* path);
int platform_make_dir(const char* path);
int platform_file_exists(const char* path);
FILE* platform_popen(const char* cmd, const char* mode);
int platform_pclose(FILE* f);
int platform_create_lockfile_exclusive(const char* lock_path);
typedef void (*platform_watcher_callback_t)(const char* dir);
int platform_start_dir_watcher(const char* dir, platform_watcher_callback_t cb);
int platform_stream_file_payload(int client_socket, const char* path, long start, long len, int is_range);
int platform_stat(const char* path, struct stat* st);
const char* platform_devnull(void);

#ifdef __cplusplus
}
#endif