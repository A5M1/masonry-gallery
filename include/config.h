#pragma once
#include "common.h"

void load_config(void);
void save_config(void);
void add_gallery_folder(const char* path);
bool is_gallery_folder(const char* path);
char** get_gallery_folders(size_t* count);
extern int log_threads_enabled;
