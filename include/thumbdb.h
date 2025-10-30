#pragma once

#include "common.h"

int thumbdb_open(void);
int thumbdb_open_for_dir(const char* gallery_dir);
void thumbdb_close(void);
int thumbdb_set(const char* key, const char* value);
int thumbdb_get(const char* key, char* buf, size_t buflen);
int thumbdb_delete(const char* key);
void thumbdb_iterate(void (*cb)(const char* key, const char* value, void* ctx), void* ctx);
int thumbdb_compact(void);
int thumbdb_sweep_orphans(void);
int thumbdb_tx_begin(void);
int thumbdb_tx_commit(void);
int thumbdb_tx_abort(void);


