#pragma once
#include "common.h"
int platform_maximize_window(void);
void platform_sleep_ms(int ms);
int platform_file_delete(const char* path);
int platform_make_dir(const char* path);
int platform_file_exists(const char* path);
FILE* platform_popen(const char* cmd, const char* mode);
int platform_pclose(FILE* f);
FILE* platform_popen_direct(const char* cmd, const char* mode);
int platform_pclose_direct(FILE* f);
int platform_create_lockfile_exclusive(const char* lock_path);
int platform_pid_is_running(int pid);
int platform_run_command(const char* cmd, int timeout_seconds);
int platform_run_command_redirect(const char* cmd, const char* out_err_path, int timeout_seconds);
typedef struct {
	long long ts_ms;
	int thread_id;
	char cmd[1024];
} platform_recent_cmd_t;

void platform_record_command(const char* cmd);
const platform_recent_cmd_t* platform_get_recent_commands(size_t* out_count);
typedef void (*platform_watcher_callback_t)(const char* dir);
int platform_start_dir_watcher(const char* dir, platform_watcher_callback_t cb);
int platform_stream_file_payload(int client_socket, const char* path, long start, long len, int is_range);
int platform_close_streams_for_path(const char* path);
int platform_stat(const char* path, struct stat* st);
const char* platform_devnull(void);
int platform_fsync(int fd);
void platform_escape_path_for_cmd(const char* src, char* dst, size_t dstlen);
void platform_enable_console_colors(void);
int platform_should_use_colors(void);
int platform_move_file(const char* src, const char* dst);
int platform_localtime(time_t t, struct tm* tm_buf);
unsigned int platform_get_pid(void);
unsigned long platform_get_tid(void);
void platform_init_network(void);
void platform_cleanup_network(void);
void platform_set_socket_options(int sock);
bool platform_is_file(const char* p);
bool platform_is_dir(const char* p);
bool platform_real_path(const char* in, char* out);
bool platform_safe_under(const char* base_real, const char* path_real);
int platform_copy_file(const char* src, const char* dst);
int platform_get_cpu_count(void);
long platform_get_physical_memory_mb(void);