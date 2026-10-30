#pragma once
#include "common.h"

typedef struct mem_scope mem_scope_t;

typedef struct {
    size_t live_blocks;
    size_t live_bytes;
    size_t total_blocks;
    size_t total_bytes;
    size_t quarantine_blocks;
    size_t quarantine_bytes;
    size_t scopes;
} mem_stats_t;

int mem_init(void);
void mem_shutdown(void);
int mem_is_initialized(void);
mem_scope_t* mem_global_scope(void);
mem_scope_t* mem_scope_create(const char* name, mem_scope_t* parent);
void mem_scope_destroy(mem_scope_t* scope);
void mem_scope_enter(mem_scope_t* scope);
void mem_scope_leave(void);
mem_scope_t* mem_scope_current(void);

void* mem_alloc(size_t size);
void* mem_calloc(size_t count, size_t size);
void* mem_realloc(void* ptr, size_t size);
char* mem_strdup(const char* s);
char* mem_strndup(const char* s, size_t n);
void* mem_alloc_tagged(size_t size, const char* tag);
void* mem_scope_alloc(mem_scope_t* scope, size_t size);
void* mem_scope_alloc_tagged(mem_scope_t* scope, size_t size, const char* tag);
char* mem_scope_strdup(mem_scope_t* scope, const char* s);
int mem_promote(void* ptr, mem_scope_t* target);
void mem_free(void* ptr);

int mem_check(void);
void mem_report_leaks(const char* label);
void mem_purge_quarantine(void);
void mem_crash_snapshot(int fd);
mem_stats_t mem_get_stats(void);
