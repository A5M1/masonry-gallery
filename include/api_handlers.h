#pragma once
#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char dir_path[PATH_MAX];
} thread_args_t;

typedef enum {
    THUMB_READY,
    THUMB_GENERATING,
    THUMB_ERROR
} thumb_status_t;

#ifdef _WIN32
typedef unsigned (__stdcall *thread_func_t)(void *);
#endif

void handle_api_tree(int c, bool keep_alive);
char* generate_media_fragment(const char* base_dir, const char* dirparam, int page, size_t* out_len);
void handle_api_folders(int c, char* qs, bool keep_alive);
void handle_api_media(int c, char* qs, bool keep_alive);
void handle_single_request(int c, char* headers, char* body, size_t headers_len, size_t body_len, bool keep_alive);
bool check_thumb_exists(const char* media_path, char* thumb_path, size_t thumb_path_len);
void ensure_thumbs_for_dir(const char* dir);
void handle_api_add_folder(int c, const char* request_body, bool keep_alive);
void handle_api_list_folders(int c, bool keep_alive);
void handle_api_regenerate_thumbs(int c, char* qs, bool keep_alive);
void start_background_thumb_generation(const char* dir_path);
void create_placeholder_thumbnails(void);
#ifdef __cplusplus
}
#endif
