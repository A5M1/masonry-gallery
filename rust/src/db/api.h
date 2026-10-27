#ifndef MEDIAVAULT_DB_API_H
#define MEDIAVAULT_DB_API_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opcodes */
#define OP_BEGIN 0xAA
#define OP_END   0xFF
#define OP_TX_BEGIN  0xBB
#define OP_TX_END    0xCC
#define OP_DELETE    0xDD

/* Hash modes */
#define HASH_MODE_FULL_MD5  0
#define HASH_MODE_HALF_MD5  1
#define HASH_MODE_RIPEMD128 2
#define HASH_MODE_NONE      3

/* Return codes */
#define DB_OK           0
#define DB_ERROR       -1
#define DB_NOT_FOUND   -2
#define DB_EXIST       -3
#define DB_INVALID     -4

/* Maximum filename length */
#define DB_MAX_PATH    4096

typedef struct {
    char filename[DB_MAX_PATH];
    uint8_t  meta_byte;
    int64_t  timestamp_delta;
    uint32_t width;
    uint32_t height;
    uint64_t duration;
    uint8_t  hash_mode;
    size_t   hash_len;
    uint8_t  hash[32]; /* MD5 is 16 bytes, RIPEMD-128 is 16 bytes */
} DbRecord;

/* Database handle - opaque pointer */
typedef struct DbDb DbDb;

DbDb* db_open(const char* path);
void db_close(DbDb* db);

int db_insert(DbDb* db, const char* filename, uint8_t meta_byte,
              uint32_t width, uint32_t height, uint64_t duration,
              uint8_t hash_mode, const uint8_t* hash, size_t hash_len);

int db_insert_path(DbDb* db, const char* key, const char* media_path);

int db_set(DbDb* db, const char* key, const char* media_path);

int db_get(DbDb* db, const char* filename, DbRecord* out_record);

int db_delete(DbDb* db, const char* filename);

int db_begin_tx(DbDb* db);

int db_commit(DbDb* db);

int db_rollback(DbDb* db);

int db_count(DbDb* db);

int db_sync(DbDb* db);

#ifdef __cplusplus
}
#endif

#endif /* MEDIAVAULT_DB_API_H */