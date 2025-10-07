#pragma once
#include "common.h"

void handle_api_tree(int c, bool keep_alive);
void handle_api_folders(int c, char* qs, bool keep_alive);
void handle_api_media(int c, char* qs, bool keep_alive);
void handle_single_request(int c, char* headers, char* body, size_t headers_len, size_t body_len, bool keep_alive);

void handle_api_add_folder(int c, const char* request_body, bool keep_alive);
void handle_api_list_folders(int c, bool keep_alive);
