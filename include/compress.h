#pragma once
#include "common.h"
int compress_val(const char* in, size_t in_len, unsigned char** out, size_t* out_len);
int decompress_val(const unsigned char* in, size_t in_len, unsigned char** out, size_t* out_len);