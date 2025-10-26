#pragma once
#include "common.h"

typedef struct diriter {
#ifdef _WIN32
    HANDLE h;
    WIN32_FIND_DATAA ffd;
    char pattern[PATH_MAX];
    bool first;
#else
    DIR* d;
    struct dirent* e;
#endif
} diriter;

bool has_ext(const char* name, const char* const exts[]);
void path_join(char* out, const char* a, const char* b);
bool is_file(const char* p);
bool is_dir(const char* p);
int mk_dir(const char* p);
void normalize_path(char* p);
bool real_path(const char* in, char* out);
bool safe_under(const char* base_real, const char* path_real);
bool dir_open(diriter* it, const char* path);
const char* dir_next(diriter* it);
void dir_close(diriter* it);
bool has_media_rec(const char* dir);
