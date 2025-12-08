#pragma once

#include "common.h"

int thumbdb_open(void);
int thumbdb_open_for_dir(const char* gallery_dir);
void thumbdb_close(void);
int thumbdb_set(const char* key, const char* value);
int thumbdb_get(const char* key, char* buf, size_t buflen);
int thumbdb_find_for_media(const char* media_path, char* out_key, size_t out_key_len);
int thumbdb_delete(const char* key);
void thumbdb_iterate(void (*cb)(const char* key, const char* value, void* ctx), void* ctx);
int thumbdb_compact(void);
int thumbdb_sweep_orphans(void);
int thumbdb_tx_begin(void);
int thumbdb_tx_commit(void);
int thumbdb_tx_abort(void);
void thumbdb_request_compaction(void);
int thumbdb_perform_requested_compaction(void);
int thumbdb_start_repair_task(int interval_seconds);
int thumbdb_start_async_worker(void);
void thumbdb_stop_async_worker(void);
int thumbdb_set_async(const char* key, const char* value);
int thumbdb_delete_async(const char* key);
int thumbdb_validate(void);
int thumbdb_verify_thumbnails(void);
int thumbdb_verify_records(void);
int thumbdb_seek_to_record(const char* filename, char* buf, size_t buflen);


