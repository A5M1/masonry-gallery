#pragma once
#include "common.h"

void session_store_init(void);
char* session_create(void);
uint64_t session_get_last(const char* session_id);
void session_set_last(const char* session_id, uint64_t last);
void session_clear(const char* session_id);
void session_store_shutdown(void);
