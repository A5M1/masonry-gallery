#pragma once

#include "common.h"

typedef struct thumbdb_instance thumbdb_instance_t;

int thumbdb_open(void);
thumbdb_instance_t* thumbdb_open_for_dir(const char* gallery_dir);
void thumbdb_close(thumbdb_instance_t* inst);
int thumbdb_set(thumbdb_instance_t* inst, const char* key, const char* value);
int thumbdb_get(thumbdb_instance_t* inst, const char* key, char* buf, size_t buflen);
int thumbdb_find_for_media(thumbdb_instance_t* inst, const char* media_path, char* out_key, size_t out_key_len);
int thumbdb_delete(thumbdb_instance_t* inst, const char* key);
void thumbdb_iterate(thumbdb_instance_t* inst, void (*cb)(const char* key, const char* value, void* ctx), void* ctx);
int thumbdb_compact(thumbdb_instance_t* inst);
int thumbdb_sweep_orphans(thumbdb_instance_t* inst);
int thumbdb_tx_begin(thumbdb_instance_t* inst);
int thumbdb_tx_commit(thumbdb_instance_t* inst);
int thumbdb_tx_abort(thumbdb_instance_t* inst);
void thumbdb_request_compaction(thumbdb_instance_t* inst);
int thumbdb_perform_requested_compaction(thumbdb_instance_t* inst);
int thumbdb_start_repair_task(thumbdb_instance_t* inst, int interval_seconds);
int thumbdb_start_async_worker(thumbdb_instance_t* inst);
void thumbdb_stop_async_worker(thumbdb_instance_t* inst);
int thumbdb_set_async(thumbdb_instance_t* inst, const char* key, const char* value);
int thumbdb_delete_async(thumbdb_instance_t* inst, const char* key);
int thumbdb_validate(thumbdb_instance_t* inst);
int thumbdb_verify_thumbnails(thumbdb_instance_t* inst);
int thumbdb_verify_records(thumbdb_instance_t* inst);
int thumbdb_seek_to_record(thumbdb_instance_t* inst, const char* filename, char* buf, size_t buflen);
char* thumbdb_get_record_detail(thumbdb_instance_t* inst, const char* key);
thumbdb_instance_t* thumbdb_find_instance(const char* db_path);


