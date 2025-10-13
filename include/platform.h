#pragma once
#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

void platform_sleep_ms(int ms);
int platform_file_delete(const char* path);
int platform_make_dir(const char* path);
int platform_file_exists(const char* path);

#ifdef __cplusplus
}
#endif