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
bool check_thumb_exists(const char* media_path, char* thumb_path, size_t thumb_path_len);
extern atomic_int ffmpeg_active;
#ifndef MAX_FFMPEG
#define MAX_FFMPEG 2
#endif
#endif

/* Internal helpers exposed for api_handlers.c callers */
void start_auto_thumb_watcher(const char* dir_path);
void run_thumb_generation(const char* dir);
