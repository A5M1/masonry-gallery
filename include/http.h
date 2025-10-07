#pragma once
#include "common.h"

typedef struct {
    int is_range;
    long start;
    long end;
} range_t;

const char* mime_for(const char* path);
char* get_header_value(char* request_buf,const char* header_name);
void send_header(int c,int status,const char* text,const char* ctype,long len,const range_t* range,long file_size,int keep_alive);
void send_text(int c,int status,const char* text,const char* body,int keep_alive);
range_t parse_range_header(const char* header_value,long file_size);
void send_file_stream(int c,const char* fs_path,const char* range_header,int keep_alive);
