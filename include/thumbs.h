#ifndef THUMBS_H
#define THUMBS_H
#include "common.h"
#include <stdatomic.h>
typedef struct skip_counter {
    char dir[PATH_MAX];
    int count;
    struct skip_counter* next;
} skip_counter_t;
typedef struct progress {
    char thumbs_dir[PATH_MAX];
    skip_counter_t* skip_head;
    size_t processed_files;
    size_t total_files;
} progress_t;
void get_thumb_rel_names(const char* full_path, const char* filename, char* small_rel, size_t small_len, char* large_rel, size_t large_len);
int get_media_dimensions(const char* path, int* width, int* height);
void start_background_thumb_generation(const char* dir_path);
int dir_has_missing_thumbs(const char* dir, int videos_only);
int dir_has_missing_thumbs_shallow(const char* dir, int videos_only);
bool check_thumb_exists(const char* media_path, char* thumb_path, size_t thumb_path_len);
extern atomic_int ffmpeg_active;
void make_safe_dir_name_from(const char* dir, char* out, size_t outlen);
void make_thumb_fs_paths(const char* media_full, const char* filename, char* small_fs_out, size_t small_fs_out_len, char* large_fs_out, size_t large_fs_out_len);
void start_periodic_thumb_maintenance(int interval_seconds);
void start_auto_thumb_watcher(const char* dir_path);
void run_thumb_generation(const char* dir);
#ifndef MAX_FFMPEG
#define MAX_FFMPEG 2
#endif
#define DEBOUNCE_MS 250
#define STALE_LOCK_SECONDS 300
#define MAX_SHALLOW_CHECK 25
#define THUMB_SMALL_SCALE 320
#define THUMB_LARGE_SCALE 1280
#define THUMB_SMALL_QUALITY 75
#define THUMB_LARGE_QUALITY 85
#ifdef _WIN32
static unsigned __stdcall debounce_generation_thread(void* args);
static unsigned __stdcall thumbnail_generation_thread(void* args);
static unsigned __stdcall thumb_job_thread(void* args);
static unsigned __stdcall thumb_maintenance_thread(void* args);
#else
static void* debounce_generation_thread(void* args);
static void* thumbnail_generation_thread(void* args);
static void* thumb_job_thread(void* args);
static void* thumb_maintenance_thread(void* args);
#endif
void count_media_in_dir(const char* dir, progress_t* prog);
void ensure_thumbs_in_dir(const char* dir, progress_t* prog);
void clean_orphan_thumbs(const char* dir, progress_t* prog);
void print_skips(progress_t* prog);
#endif // THUMBS_H