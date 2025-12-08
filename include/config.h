#pragma once
#include "common.h"

void load_config(void);
void save_config(void);
void add_gallery_folder(const char* path);
bool is_gallery_folder(const char* path);
char** get_gallery_folders(size_t* count);
extern int log_threads_enabled;
extern int server_port;
extern int db_repair_interval;
extern int db_compact_interval;
extern int db_sweep_interval;
extern int exif_extraction_enabled;
extern char exif_tool_path[PATH_MAX];

const char* get_exiftool_path(void);
int is_exif_extraction_enabled(void);
