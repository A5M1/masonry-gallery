#ifndef MEDIAVAULT_DB_API_H
#define MEDIAVAULT_DB_API_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Database record opcodes used by the on-disk format. */
#define OP_BEGIN 0xAA
#define OP_END   0xFF
#define OP_TX_BEGIN  0xBB
#define OP_TX_END    0xCC
#define OP_DELETE    0xDD

/** Hash algorithm identifiers accepted by db_insert(). */
#define HASH_MODE_FULL_MD5  0
#define HASH_MODE_HALF_MD5  1
#define HASH_MODE_RIPEMD128 2
#define HASH_MODE_NONE      3

/** Successful operation. */
#define DB_OK           0
/** General failure, including invalid handles and I/O errors. */
#define DB_ERROR       -1
/** Requested record does not exist. */
#define DB_NOT_FOUND   -2
/** Reserved return code; current API functions do not return it. */
#define DB_EXIST       -3
/** Invalid argument, such as a null hash pointer with nonzero hash length. */
#define DB_INVALID     -4

/** Maximum number of bytes reserved for a record filename, including NUL. */
#define DB_MAX_PATH    4096

/**
 * Record data returned by db_get().
 *
 * Filename strings are NUL-terminated and truncated if they do not fit.
 * The hash buffer contains up to 32 bytes; hash_len reports the number of
 * valid bytes. Current hash modes use 0, 8, or 16 bytes.
 */
typedef struct {
    char filename[DB_MAX_PATH]; /**< Record filename/key. */
    uint8_t  meta_byte;          /**< Encoded media and record metadata. */
    int64_t  timestamp_delta;    /**< Record timestamp in seconds since Unix epoch. */
    uint32_t width;              /**< Media width, or 0 if unavailable. */
    uint32_t height;             /**< Media height, or 0 if unavailable. */
    uint64_t duration;           /**< Stored media duration. */
    uint8_t  hash_mode;          /**< One of the HASH_MODE_* values. */
    size_t   hash_len;           /**< Number of valid bytes in hash. */
    uint8_t  hash[32];           /**< Hash bytes; only hash_len bytes are valid. */
} DbRecord;

/** Opaque database handle returned by db_open(). */
typedef struct DbDb DbDb;

/**
 * Open or create a database at path.
 *
 * @param path NUL-terminated filesystem path to the database.
 * @return Database handle, or NULL if path is NULL or opening fails.
 */
DbDb* db_open(const char* path);

/**
 * Release a handle returned by db_open().
 *
 * @param db Database handle to release.
 * Passing NULL has no effect.
 */
void db_close(DbDb* db);

/**
 * Insert or replace a record's metadata.
 *
 * If both width and height are nonzero, they are stored; otherwise dimensions
 * are omitted. hash_mode selects the hash format, and hash_len bytes are read
 * from hash when hash_len is nonzero. Hash data is truncated or zero-padded
 * to the selected mode's size (16 bytes for full MD5/RIPEMD-128, 8 for half
 * MD5, and 0 for no hash).
 *
 * @param db Database handle.
 * @param filename NUL-terminated record filename/key.
 * @param meta_byte Encoded media and record metadata.
 * @param width Media width; set to 0 to omit dimensions.
 * @param height Media height; set to 0 to omit dimensions.
 * @param duration Stored media duration.
 * @param hash_mode One of the HASH_MODE_* values.
 * @param hash Hash bytes, required when hash_len is nonzero.
 * @param hash_len Number of bytes available at hash.
 * @return DB_OK on success, DB_INVALID for a null hash with nonzero length,
 *         or DB_ERROR for other failures.
 */
int db_insert(DbDb* db, const char* filename, uint8_t meta_byte,
              uint32_t width, uint32_t height, uint64_t duration,
              uint8_t hash_mode, const uint8_t* hash, size_t hash_len);

/**
 * Associate key with a media path, replacing any existing value for key.
 *
 * @param db Database handle.
 * @param key NUL-terminated record key.
 * @param media_path NUL-terminated media path to associate with the key.
 * @return DB_OK on success, or DB_ERROR on failure.
 */
int db_insert_path(DbDb* db, const char* key, const char* media_path);

/**
 * Set a key-to-media-path association.
 *
 * This is an alias for db_insert_path().
 *
 * @param db Database handle.
 * @param key NUL-terminated record key.
 * @param media_path NUL-terminated media path to associate with the key.
 * @return DB_OK on success, or DB_ERROR on failure.
 */
int db_set(DbDb* db, const char* key, const char* media_path);

/**
 * Retrieve a record by filename/key.
 *
 * On success, out_record is filled with the record data. Its filename is
 * NUL-terminated and may be truncated to fit DB_MAX_PATH.
 *
 * @param db Database handle.
 * @param filename NUL-terminated record filename/key.
 * @param out_record Output storage for the record.
 * @return DB_OK on success, DB_NOT_FOUND if no record exists, or DB_ERROR
 *         for an invalid handle, null argument, or other failure.
 */
int db_get(DbDb* db, const char* filename, DbRecord* out_record);

/**
 * Delete a record by filename/key.
 *
 * @param db Database handle.
 * @param filename NUL-terminated record filename/key.
 * @return DB_OK if a record was deleted, DB_NOT_FOUND if it did not exist,
 *         or DB_ERROR on failure.
 */
int db_delete(DbDb* db, const char* filename);

/**
 * Begin a transaction for db_set() and db_delete() operations.
 *
 * A transaction cannot be nested. db_insert() is not staged by this
 * transaction.
 *
 * @param db Database handle.
 * @return DB_OK on success, or DB_ERROR if a transaction is already active
 *         or the operation fails.
 */
int db_begin_tx(DbDb* db);

/**
 * Commit the active transaction and persist its staged changes.
 *
 * @param db Database handle.
 * @return DB_OK on success, or DB_ERROR if there is no active transaction
 *         or the operation fails.
 */
int db_commit(DbDb* db);

/**
 * Roll back the active transaction, discarding its staged changes.
 *
 * @param db Database handle.
 * @return DB_OK on success, or DB_ERROR if there is no active transaction
 *         or the operation fails.
 */
int db_rollback(DbDb* db);

/**
 * Return the number of records currently in the database.
 *
 * @param db Database handle.
 * @return Record count, or DB_ERROR if db is NULL.
 */
int db_count(DbDb* db);

/**
 * Write the current database contents to disk.
 *
 * @param db Database handle.
 * @return DB_OK on success, or DB_ERROR on failure.
 */
int db_sync(DbDb* db);

#ifdef __cplusplus
}
#endif

#endif /* MEDIAVAULT_DB_API_H */