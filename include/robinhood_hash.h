#pragma once
#include "common.h"
typedef struct rh_table rh_table_t;
rh_table_t* rh_create(size_t capacity_power);
void rh_destroy(rh_table_t* t);
int rh_insert(rh_table_t* t, const char* key, size_t key_len, const unsigned char* val, size_t val_len);
int rh_find(rh_table_t* t, const char* key, size_t key_len, unsigned char** out_val, size_t* out_val_len);
int rh_remove(rh_table_t* t, const char* key, size_t key_len);
int rh_iterate(rh_table_t* t, int (*cb)(const char* key, const unsigned char* val, size_t val_len, void* ctx), void* ctx);