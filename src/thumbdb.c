#include "thumbdb.h"
#include "platform.h"
#include "logging.h"
#include "utils.h"
#include "directory.h"
#include "thread_pool.h"
#include "crypto.h"
#include "common.h"
#include "arena.h"
#include "config.h"
#include "thumbs.h"
#include "robinhood_hash.h"

#define DB_FILENAME "thumbs.tdb"
#define INITIAL_BUCKETS_BITS 16
#define WAL_DIR_NAME "wal"
#define WAL_CHUNK_FMT "chunk-%lld-%u-%u.wal"
#define MAX_INSTANCES 64

typedef struct {
    uint16_t db_magic;
    uint16_t index_magic;
    uint8_t version;
} mediavault_constants_t;

typedef struct {
    uint8_t begin;
    uint8_t end;
    uint8_t tx_begin;
    uint8_t tx_end;
    uint8_t delete_op;
} mediavault_opcodes_t;

static const mediavault_constants_t MV_CONSTANTS = {
    .db_magic = 0x4D56,
    .index_magic = 0x4958,
    .version = 0x21
};

static const mediavault_opcodes_t MV_OPCODES = {
    .begin = 0xAA,
    .end = 0xFF,
    .tx_begin = 0xBB,
    .tx_end = 0xCC,
    .delete_op = 0xDD
};

typedef enum {
    MEDIA_JPG = 0,
    MEDIA_PNG = 1,
    MEDIA_GIF = 2,
    MEDIA_WEBP = 3,
    MEDIA_MP4 = 4,
    MEDIA_WEBM = 5
} media_type_t;

typedef enum {
    HASH_FULL_MD5 = 0,
    HASH_HALF_MD5 = 1,
    HASH_RIPEMD128 = 2,
    HASH_NONE = 3
} hash_mode_t;

typedef struct {
    uint8_t dimensions_delta;
    uint8_t orientation;
    uint8_t codec_info;
    uint8_t gps_coords;
    uint8_t perceptual_hash;
} mediavault_extension_tags_t;

static const mediavault_extension_tags_t MV_EXT_TAGS = {
    .dimensions_delta = 0x01,
    .orientation = 0x02,
    .codec_info = 0x03,
    .gps_coords = 0x04,
    .perceptual_hash = 0x05
};

typedef struct {
    uint8_t varint_continue_bit;
    uint8_t varint_data_mask;
    uint8_t meta_type_mask;
    uint8_t meta_thumb_mode_mask;
    uint8_t meta_bit_mask;
    uint8_t byte_mask;
    uint8_t flags_init;
} mediavault_bitmasks_t;

static const mediavault_bitmasks_t MV_BITMASKS = {
    .varint_continue_bit = 0x80,
    .varint_data_mask = 0x7F,
    .meta_type_mask = 0x07,
    .meta_thumb_mode_mask = 0x03,
    .meta_bit_mask = 0x01,
    .byte_mask = 0xFF,
    .flags_init = 0x00
};

typedef struct {
    media_type_t type;
    int animated;
    int thumb_mode;
    int hash_override;
    int has_extensions;
    hash_mode_t hash_mode;
} meta_byte_t;

typedef struct {
    uint8_t flags;
    uint64_t base_timestamp;
    hash_mode_t default_hash_mode;
    int timestamp_precision;
    int has_index;
} file_header_t;

typedef struct {
    char** dirs;
    size_t count;
    size_t capacity;
} dir_table_t;

typedef struct {
    uint64_t record_sequence;
    char* filename;
    uint64_t timestamp;
    meta_byte_t meta;
    uint8_t hash[16];
    size_t hash_len;
    size_t dir_count;
    uint32_t* dir_indexes;
    uint32_t width;
    uint32_t height;
    uint32_t duration;
    uint32_t crc32;
    uint8_t orientation;
    char* codec_info;
    double gps_lat;
    double gps_lon;
} record_t;

typedef struct {
    uint64_t file_offset;
    uint64_t filename_hash;
} index_entry_t;

typedef struct {
    index_entry_t* entries;
    size_t count;
    size_t capacity;
} index_table_t;

typedef struct async_op {
    char key[PATH_MAX];
    char value[PATH_MAX];
    int is_delete;
    struct async_op* next;
} async_op_t;

struct thumbdb_instance {
    char db_path[PATH_MAX];
    rh_table_t* rh_tbl;
    dir_table_t dir_table;
    index_table_t index_table;
    file_header_t file_header;
    uint64_t current_record_seq;
    uint64_t last_filename_delta;
    uint64_t last_timestamp_delta;
    int tx_active;
    rh_table_t* tx_snapshot;
    thread_mutex_t mutex;
    int compaction_requested;
    thread_mutex_t compaction_mutex;
    async_op_t* async_queue_head;
    async_op_t* async_queue_tail;
    thread_mutex_t async_queue_mutex;
    int async_worker_running;
    int ref_count;
    int db_inited;
    struct thumbdb_instance* next;
};

static thumbdb_instance_t* g_instances = NULL;
static thread_mutex_t g_registry_mutex;
static int g_registry_mutex_inited = 0;

static atomic_uint wal_chunk_seq = ATOMIC_VAR_INIT(0);
static thread_mutex_t wal_seq_mutex;
static int wal_seq_mutex_inited = 0;

static void registry_lock(void) {
    if (!g_registry_mutex_inited) {
        if (thread_mutex_init(&g_registry_mutex) == 0) g_registry_mutex_inited = 1;
    }
    if (g_registry_mutex_inited) thread_mutex_lock(&g_registry_mutex);
}

static void registry_unlock(void) {
    if (g_registry_mutex_inited) thread_mutex_unlock(&g_registry_mutex);
}

thumbdb_instance_t* thumbdb_find_instance(const char* db_path) {
    if (!db_path) return NULL;
    registry_lock();
    thumbdb_instance_t* inst = g_instances;
    while (inst) {
        if (strcmp(inst->db_path, db_path) == 0) {
            registry_unlock();
            return inst;
        }
        inst = inst->next;
    }
    registry_unlock();
    return NULL;
}

static thumbdb_instance_t* thumbdb_get_or_create_instance(const char* db_path) {
    registry_lock();
    thumbdb_instance_t* inst = g_instances;
    while (inst) {
        if (strcmp(inst->db_path, db_path) == 0) {
            inst->ref_count++;
            registry_unlock();
            return inst;
        }
        inst = inst->next;
    }

    inst = calloc(1, sizeof(thumbdb_instance_t));
    if (!inst) {
        registry_unlock();
        return NULL;
    }
    strncpy(inst->db_path, db_path, sizeof(inst->db_path) - 1);
    inst->db_path[sizeof(inst->db_path) - 1] = '\0';
    inst->ref_count = 1;
    if (thread_mutex_init(&inst->mutex) != 0) {
        free(inst);
        registry_unlock();
        return NULL;
    }
    if (thread_mutex_init(&inst->compaction_mutex) != 0) {
        thread_mutex_destroy(&inst->mutex);
        free(inst);
        registry_unlock();
        return NULL;
    }
    if (thread_mutex_init(&inst->async_queue_mutex) != 0) {
        thread_mutex_destroy(&inst->compaction_mutex);
        thread_mutex_destroy(&inst->mutex);
        free(inst);
        registry_unlock();
        return NULL;
    }
    inst->next = g_instances;
    g_instances = inst;
    registry_unlock();
    return inst;
}

static void thumbdb_release_instance(thumbdb_instance_t* inst) {
    if (!inst) return;
    registry_lock();
    inst->ref_count--;
    if (inst->ref_count > 0) {
        registry_unlock();
        return;
    }
    thumbdb_instance_t** prev = &g_instances;
    thumbdb_instance_t* cur = g_instances;
    while (cur) {
        if (cur == inst) {
            *prev = cur->next;
            break;
        }
        prev = &cur->next;
        cur = cur->next;
    }
    registry_unlock();

    inst->async_worker_running = 0;
    platform_sleep_ms(100);
    if (inst->rh_tbl) rh_destroy(inst->rh_tbl);
    if (inst->tx_snapshot) rh_destroy(inst->tx_snapshot);
    for (size_t i = 0; i < inst->dir_table.count; i++) free(inst->dir_table.dirs[i]);
    free(inst->dir_table.dirs);
    free(inst->index_table.entries);
    async_op_t* op = inst->async_queue_head;
    while (op) { async_op_t* next = op->next; free(op); op = next; }
    thread_mutex_destroy(&inst->mutex);
    thread_mutex_destroy(&inst->compaction_mutex);
    thread_mutex_destroy(&inst->async_queue_mutex);
    memset(inst, 0, sizeof(*inst));
    free(inst);
}

static int write_uint64_le(FILE* f, uint64_t value) {
    uint8_t buf[8];
    for (int i = 0; i < 8; i++) buf[i] = (value >> (i * 8)) & 0xFF;
    return fwrite(buf, 1, 8, f) == 8 ? 0 : -1;
}

static int read_uint64_le(FILE* f, uint64_t* out) {
    uint8_t buf[8];
    if (fread(buf, 1, 8, f) != 8) return -1;
    *out = 0;
    for (int i = 0; i < 8; i++) *out |= ((uint64_t)buf[i]) << (i * 8);
    return 0;
}

static int write_uint32_le(FILE* f, uint32_t value) {
    uint8_t buf[4];
    for (int i = 0; i < 4; i++) buf[i] = (value >> (i * 8)) & 0xFF;
    return fwrite(buf, 1, 4, f) == 4 ? 0 : -1;
}

static int read_uint32_le(FILE* f, uint32_t* out) {
    uint8_t buf[4];
    if (fread(buf, 1, 4, f) != 4) return -1;
    *out = 0;
    for (int i = 0; i < 4; i++) *out |= ((uint32_t)buf[i]) << (i * 8);
    return 0;
}

static int write_float_le(FILE* f, float value) {
    uint32_t bits;
    memcpy(&bits, &value, 4);
    return write_uint32_le(f, bits);
}

static int read_float_le(FILE* f, float* out) {
    uint32_t bits;
    if (read_uint32_le(f, &bits) != 0) return -1;
    memcpy(out, &bits, 4);
    return 0;
}

static int write_varint(FILE* f, uint64_t value) {
    uint8_t buf[10];
    int pos = 0;
    while (value >= MV_BITMASKS.varint_continue_bit) {
        buf[pos++] = (uint8_t)((value & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit);
        value >>= 7;
    }
    buf[pos++] = (uint8_t)(value & MV_BITMASKS.varint_data_mask);
    return fwrite(buf, 1, pos, f) == (size_t)pos ? 0 : -1;
}

static int read_varint(FILE* f, uint64_t* out) {
    uint64_t result = 0;
    int shift = 0;
    uint8_t byte;
    do {
        if (fread(&byte, 1, 1, f) != 1) return -1;
        result |= (uint64_t)(byte & MV_BITMASKS.varint_data_mask) << shift;
        shift += 7;
        if (shift > 63) return -1;
    } while (byte & MV_BITMASKS.varint_continue_bit);
    *out = result;
    return 0;
}

static int read_varint_with_size(FILE* f, uint64_t* out, size_t* bytes_read) {
    uint64_t result = 0;
    int shift = 0;
    uint8_t byte;
    size_t count = 0;
    do {
        if (fread(&byte, 1, 1, f) != 1) return -1;
        count++;
        result |= (uint64_t)(byte & MV_BITMASKS.varint_data_mask) << shift;
        shift += 7;
        if (shift > 63) return -1;
    } while (byte & MV_BITMASKS.varint_continue_bit);
    *out = result;
    *bytes_read = count;
    return 0;
}

static int write_bit_packed_indexes(FILE* f, uint32_t* indexes, size_t count, size_t table_size) {
    if (count == 0) return 0;
    int bits_per_index = 1;
    size_t tmp = table_size;
    while (tmp > 1) { bits_per_index++; tmp >>= 1; }
    uint8_t current_byte = 0;
    int bits_in_byte = 0;
    for (size_t i = 0; i < count; i++) {
        uint32_t idx = indexes[i];
        int bits_remaining = bits_per_index;
        while (bits_remaining > 0) {
            int bits_to_write = bits_remaining;
            if (bits_to_write > 8 - bits_in_byte) bits_to_write = 8 - bits_in_byte;
            uint32_t mask = (1u << bits_to_write) - 1;
            uint8_t bits = (idx >> (bits_remaining - bits_to_write)) & mask;
            current_byte |= bits << (8 - bits_in_byte - bits_to_write);
            bits_in_byte += bits_to_write;
            bits_remaining -= bits_to_write;
            if (bits_in_byte == 8) {
                if (fwrite(&current_byte, 1, 1, f) != 1) return -1;
                current_byte = 0;
                bits_in_byte = 0;
            }
        }
    }
    if (bits_in_byte > 0) {
        if (fwrite(&current_byte, 1, 1, f) != 1) return -1;
    }
    return 0;
}

static int read_bit_packed_indexes(FILE* f, uint32_t** out_indexes, size_t count, size_t table_size) {
    if (count == 0) { *out_indexes = NULL; return 0; }
    int bits_per_index = 1;
    size_t tmp = table_size;
    while (tmp > 1) { bits_per_index++; tmp >>= 1; }
    uint32_t* indexes = calloc(count, sizeof(uint32_t));
    if (!indexes) return -1;
    uint8_t current_byte = 0;
    int bits_available = 0;
    for (size_t i = 0; i < count; i++) {
        uint32_t idx = 0;
        int bits_remaining = bits_per_index;
        while (bits_remaining > 0) {
            if (bits_available == 0) {
                if (fread(&current_byte, 1, 1, f) != 1) { free(indexes); return -1; }
                bits_available = 8;
            }
            int bits_to_read = bits_remaining;
            if (bits_to_read > bits_available) bits_to_read = bits_available;
            uint8_t mask = (1u << bits_to_read) - 1;
            uint8_t bits = (current_byte >> (8 - bits_to_read)) & mask;
            idx = (idx << bits_to_read) | bits;
            current_byte <<= bits_to_read;
            bits_available -= bits_to_read;
            bits_remaining -= bits_to_read;
        }
        indexes[i] = idx;
    }
    *out_indexes = indexes;
    return 0;
}

static uint8_t encode_meta_byte(meta_byte_t meta) {
    uint8_t byte = 0;
    byte |= (meta.type & MV_BITMASKS.meta_type_mask) << 5;
    byte |= (meta.animated ? 1 : 0) << 4;
    byte |= (meta.thumb_mode & 3) << 2;
    byte |= (meta.hash_override ? 1 : 0) << 1;
    byte |= (meta.has_extensions ? 1 : 0);
    return byte;
}

static meta_byte_t decode_meta_byte(uint8_t byte) {
    meta_byte_t meta;
    meta.type = (byte >> 5) & MV_BITMASKS.meta_type_mask;
    meta.animated = (byte >> 4) & MV_BITMASKS.meta_bit_mask;
    meta.thumb_mode = (byte >> 2) & MV_BITMASKS.meta_thumb_mode_mask;
    meta.hash_override = (byte >> 1) & MV_BITMASKS.meta_bit_mask;
    meta.has_extensions = byte & MV_BITMASKS.meta_bit_mask;
    meta.hash_mode = HASH_FULL_MD5;
    return meta;
}

static size_t get_hash_length(hash_mode_t mode) {
    switch (mode) {
        case HASH_HALF_MD5: return 8;
        case HASH_NONE: return 0;
        case HASH_FULL_MD5:
        case HASH_RIPEMD128:
        default: return 16;
    }
}

static media_type_t get_media_type_from_path(const char* path) {
    const char* dot = strrchr(path, '.');
    if (!dot) return MEDIA_JPG;
    if (ascii_stricmp(dot, ".jpg") == 0 || ascii_stricmp(dot, ".jpeg") == 0) return MEDIA_JPG;
    if (ascii_stricmp(dot, ".png") == 0) return MEDIA_PNG;
    if (ascii_stricmp(dot, ".gif") == 0) return MEDIA_GIF;
    if (ascii_stricmp(dot, ".webp") == 0) return MEDIA_WEBP;
    if (ascii_stricmp(dot, ".mp4") == 0) return MEDIA_MP4;
    if (ascii_stricmp(dot, ".webm") == 0) return MEDIA_WEBM;
    return MEDIA_JPG;
}

static int is_jpeg_extension(const char* ext) {
    if (!ext) return 0;
    return (ascii_stricmp(ext, ".jpg") == 0 || ascii_stricmp(ext, ".jpeg") == 0);
}

static int add_dir_to_table(thumbdb_instance_t* inst, const char* dir) {
    for (size_t i = 0; i < inst->dir_table.count; i++) {
        if (strcmp(inst->dir_table.dirs[i], dir) == 0) return i;
    }
    if (inst->dir_table.count >= inst->dir_table.capacity) {
        size_t new_cap = inst->dir_table.capacity == 0 ? 64 : inst->dir_table.capacity * 2;
        char** new_dirs = realloc(inst->dir_table.dirs, new_cap * sizeof(char*));
        if (!new_dirs) return -1;
        inst->dir_table.dirs = new_dirs;
        inst->dir_table.capacity = new_cap;
    }
    inst->dir_table.dirs[inst->dir_table.count] = strdup(dir);
    if (!inst->dir_table.dirs[inst->dir_table.count]) return -1;
    return inst->dir_table.count++;
}

static uint64_t hash_filename(const char* filename) {
    uint64_t hash = 0xcbf29ce484222325ULL;
    const uint64_t prime = 0x100000001b3ULL;
    for (const char* p = filename; *p; p++) {
        hash ^= (uint64_t)(unsigned char)*p;
        hash *= prime;
    }
    return hash;
}

static int add_index_entry(thumbdb_instance_t* inst, uint64_t file_offset, const char* filename) {
    if (inst->index_table.count >= inst->index_table.capacity) {
        size_t new_cap = inst->index_table.capacity == 0 ? 256 : inst->index_table.capacity * 2;
        index_entry_t* new_entries = realloc(inst->index_table.entries, new_cap * sizeof(index_entry_t));
        if (!new_entries) return -1;
        inst->index_table.entries = new_entries;
        inst->index_table.capacity = new_cap;
    }
    inst->index_table.entries[inst->index_table.count].file_offset = file_offset;
    inst->index_table.entries[inst->index_table.count].filename_hash = hash_filename(filename);
    inst->index_table.count++;
    return 0;
}

static int compare_index_entries(const void* a, const void* b) {
    const index_entry_t* ea = (const index_entry_t*)a;
    const index_entry_t* eb = (const index_entry_t*)b;
    if (ea->filename_hash < eb->filename_hash) return -1;
    if (ea->filename_hash > eb->filename_hash) return 1;
    return 0;
}

static int write_index_block(thumbdb_instance_t* inst, FILE* f) {
    if (inst->index_table.count == 0) return 0;
    qsort(inst->index_table.entries, inst->index_table.count, sizeof(index_entry_t), compare_index_entries);
    uint8_t magic_hi = (MV_CONSTANTS.index_magic >> 8) & 0xFF;
    uint8_t magic_lo = MV_CONSTANTS.index_magic & 0xFF;
    if (fwrite(&magic_hi, 1, 1, f) != 1) return -1;
    if (fwrite(&magic_lo, 1, 1, f) != 1) return -1;
    if (write_varint(f, inst->index_table.count) != 0) return -1;
    for (size_t i = 0; i < inst->index_table.count; i++) {
        if (write_varint(f, inst->index_table.entries[i].file_offset) != 0) return -1;
        if (write_varint(f, inst->index_table.entries[i].filename_hash) != 0) return -1;
    }
    return 0;
}

static int parse_path_into_dirs(thumbdb_instance_t* inst, const char* path, uint32_t** out_indexes, size_t* out_count) {
    char tmp[PATH_MAX];
    strncpy(tmp, path, sizeof(tmp) - 1);
    tmp[sizeof(tmp) - 1] = '\0';
    normalize_path(tmp);
    char* components[64];
    size_t comp_count = 0;
    char* scan = tmp;
    while (*scan && comp_count < 64) {
        while (*scan == '/' || *scan == '\\') scan++;
        if (!*scan) break;
        char* end = scan;
        while (*end && *end != '/' && *end != '\\') end++;
        size_t tok_len = (size_t)(end - scan);
        if (tok_len > 0) {
            char saved = *end;
            *end = '\0';
            components[comp_count] = scan;
            comp_count++;
            scan = end;
            if (saved) *scan = saved;
            if (*scan) scan++;
        }
    }
    if (comp_count == 0) { *out_indexes = NULL; *out_count = 0; return 0; }
    uint32_t* indexes = calloc(comp_count, sizeof(uint32_t));
    if (!indexes) return -1;
    for (size_t i = 0; i < comp_count; i++) {
        int idx = add_dir_to_table(inst, components[i]);
        if (idx < 0) { free(indexes); return -1; }
        indexes[i] = (uint32_t)idx;
    }
    *out_indexes = indexes;
    *out_count = comp_count;
    return 0;
}

static int reconstruct_path_from_indexes(thumbdb_instance_t* inst, uint32_t* indexes, size_t count, char* out, size_t outlen) {
    out[0] = '\0';
    for (size_t i = 0; i < count; i++) {
        if (indexes[i] >= inst->dir_table.count) return -1;
        if (i > 0) strncat(out, DIR_SEP_STR, outlen - strlen(out) - 1);
        strncat(out, inst->dir_table.dirs[indexes[i]], outlen - strlen(out) - 1);
    }
    return 0;
}

static int write_record(thumbdb_instance_t* inst, FILE* f, record_t* rec, int is_first) {
    uint8_t record_buf[8192];
    size_t buf_pos = 0;

    record_buf[buf_pos++] = MV_OPCODES.begin;

    uint8_t seq_buf[10];
    size_t seq_len = 0;
    uint64_t seq = rec->record_sequence;
    while (seq >= MV_BITMASKS.varint_continue_bit) {
        seq_buf[seq_len++] = (uint8_t)((seq & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit);
        seq >>= 7;
    }
    seq_buf[seq_len++] = (uint8_t)(seq & MV_BITMASKS.varint_data_mask);
    for (size_t i = 0; i < seq_len; i++) record_buf[buf_pos++] = seq_buf[i];

    uint8_t dir_buf[10];
    size_t dir_len = 0;
    uint64_t dc = rec->dir_count;
    while (dc >= MV_BITMASKS.varint_continue_bit) {
        dir_buf[dir_len++] = (uint8_t)((dc & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit);
        dc >>= 7;
    }
    dir_buf[dir_len++] = (uint8_t)(dc & MV_BITMASKS.varint_data_mask);
    for (size_t i = 0; i < dir_len; i++) record_buf[buf_pos++] = dir_buf[i];

    size_t dir_table_size = inst->dir_table.count > 0 ? inst->dir_table.count : 1;
    int bits_per_index = 1;
    size_t tmp = dir_table_size;
    while (tmp > 1) { bits_per_index++; tmp >>= 1; }

    uint8_t current_byte = 0;
    int bits_in_byte = 0;
    for (size_t i = 0; i < rec->dir_count; i++) {
        uint32_t idx = rec->dir_indexes[i];
        int bits_remaining = bits_per_index;
        while (bits_remaining > 0) {
            int bits_to_write = bits_remaining;
            if (bits_to_write > 8 - bits_in_byte) bits_to_write = 8 - bits_in_byte;
            uint32_t mask = (1u << bits_to_write) - 1;
            uint8_t bits = (idx >> (bits_remaining - bits_to_write)) & mask;
            current_byte |= bits << (8 - bits_in_byte - bits_to_write);
            bits_in_byte += bits_to_write;
            bits_remaining -= bits_to_write;
            if (bits_in_byte == 8) {
                record_buf[buf_pos++] = current_byte;
                current_byte = 0;
                bits_in_byte = 0;
            }
        }
    }
    if (bits_in_byte > 0) record_buf[buf_pos++] = current_byte;

    int is_numeric_key = 1;
    char* endptr = NULL;
    uint64_t filename_val = strtoull(rec->filename, &endptr, 10);
    if (endptr == rec->filename || *endptr != '\0') is_numeric_key = 0;

    if (is_numeric_key) {
        uint64_t filename_delta = is_first ? filename_val : (filename_val - inst->last_filename_delta);
        uint8_t fname_buf[10];
        size_t fname_len = 0;
        uint64_t fd = filename_delta;
        while (fd >= MV_BITMASKS.varint_continue_bit) {
            fname_buf[fname_len++] = (uint8_t)((fd & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit);
            fd >>= 7;
        }
        fname_buf[fname_len++] = (uint8_t)(fd & MV_BITMASKS.varint_data_mask);
        for (size_t i = 0; i < fname_len; i++) record_buf[buf_pos++] = fname_buf[i];
        inst->last_filename_delta = filename_val;
    } else {
        record_buf[buf_pos++] = 0x00;
        size_t key_len = strlen(rec->filename);
        if (key_len > 255) key_len = 255;
        uint64_t kl = (uint64_t)key_len;
        uint8_t len_buf[5];
        size_t len_pos = 0;
        while (kl >= MV_BITMASKS.varint_continue_bit) {
            len_buf[len_pos++] = (uint8_t)((kl & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit);
            kl >>= 7;
        }
        len_buf[len_pos++] = (uint8_t)(kl & MV_BITMASKS.varint_data_mask);
        for (size_t i = 0; i < len_pos; i++) record_buf[buf_pos++] = len_buf[i];
        memcpy(record_buf + buf_pos, rec->filename, key_len);
        buf_pos += key_len;
        inst->last_filename_delta = 0;
    }

    uint8_t meta = encode_meta_byte(rec->meta);
    record_buf[buf_pos++] = meta;

    if (rec->meta.hash_override) {
        uint8_t hash_mode_byte = (uint8_t)(rec->meta.hash_mode & 3);
        record_buf[buf_pos++] = hash_mode_byte;
    }

    if (rec->meta.has_extensions) {
        size_t ext_start = buf_pos;
        buf_pos++;
        if (rec->width > 0 && rec->height > 0) {
            record_buf[buf_pos++] = MV_EXT_TAGS.dimensions_delta;
            uint64_t w = rec->width, h = rec->height;
            uint8_t w_buf[10], h_buf[10];
            size_t w_len = 0, h_len = 0;
            while (w >= MV_BITMASKS.varint_continue_bit) { w_buf[w_len++] = (uint8_t)((w & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit); w >>= 7; }
            w_buf[w_len++] = (uint8_t)(w & MV_BITMASKS.varint_data_mask);
            while (h >= MV_BITMASKS.varint_continue_bit) { h_buf[h_len++] = (uint8_t)((h & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit); h >>= 7; }
            h_buf[h_len++] = (uint8_t)(h & MV_BITMASKS.varint_data_mask);
            size_t total_dim_len = w_len + h_len;
            uint8_t len_buf[10]; size_t len_len = 0;
            uint64_t tl = total_dim_len;
            while (tl >= MV_BITMASKS.varint_continue_bit) { len_buf[len_len++] = (uint8_t)((tl & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit); tl >>= 7; }
            len_buf[len_len++] = (uint8_t)(tl & MV_BITMASKS.varint_data_mask);
            for (size_t i = 0; i < len_len; i++) record_buf[buf_pos++] = len_buf[i];
            for (size_t i = 0; i < w_len; i++) record_buf[buf_pos++] = w_buf[i];
            for (size_t i = 0; i < h_len; i++) record_buf[buf_pos++] = h_buf[i];
        }
        if (rec->orientation > 0) { record_buf[buf_pos++] = MV_EXT_TAGS.orientation; record_buf[buf_pos++] = 1; record_buf[buf_pos++] = rec->orientation; }
        if (rec->codec_info && rec->codec_info[0]) {
            size_t codec_len = strlen(rec->codec_info);
            if (codec_len > 255) codec_len = 255;
            record_buf[buf_pos++] = MV_EXT_TAGS.codec_info;
            record_buf[buf_pos++] = (uint8_t)codec_len;
            memcpy(record_buf + buf_pos, rec->codec_info, codec_len);
            buf_pos += codec_len;
        }
        if (rec->gps_lat != 0.0 || rec->gps_lon != 0.0) {
            record_buf[buf_pos++] = MV_EXT_TAGS.gps_coords;
            record_buf[buf_pos++] = 8;
            float lat_f = (float)rec->gps_lat, lon_f = (float)rec->gps_lon;
            uint32_t lat_bits, lon_bits;
            memcpy(&lat_bits, &lat_f, 4); memcpy(&lon_bits, &lon_f, 4);
            record_buf[buf_pos++] = (lat_bits >> 0) & 0xFF; record_buf[buf_pos++] = (lat_bits >> 8) & 0xFF;
            record_buf[buf_pos++] = (lat_bits >> 16) & 0xFF; record_buf[buf_pos++] = (lat_bits >> 24) & 0xFF;
            record_buf[buf_pos++] = (lon_bits >> 0) & 0xFF; record_buf[buf_pos++] = (lon_bits >> 8) & 0xFF;
            record_buf[buf_pos++] = (lon_bits >> 16) & 0xFF; record_buf[buf_pos++] = (lon_bits >> 24) & 0xFF;
        }
        uint8_t ext_len = (uint8_t)(buf_pos - ext_start - 1);
        record_buf[ext_start] = ext_len;
    }

    uint64_t timestamp_delta = is_first ? rec->timestamp : (rec->timestamp - inst->last_timestamp_delta);
    uint8_t ts_buf[10];
    size_t ts_len = 0;
    uint64_t td = timestamp_delta;
    while (td >= MV_BITMASKS.varint_continue_bit) { ts_buf[ts_len++] = (uint8_t)((td & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit); td >>= 7; }
    ts_buf[ts_len++] = (uint8_t)(td & MV_BITMASKS.varint_data_mask);
    for (size_t i = 0; i < ts_len; i++) record_buf[buf_pos++] = ts_buf[i];
    inst->last_timestamp_delta = rec->timestamp;

    for (size_t i = 0; i < rec->hash_len; i++) record_buf[buf_pos++] = rec->hash[i];

    uint32_t crc;
    crypto_crc32(record_buf, buf_pos, &crc);
    record_buf[buf_pos++] = (crc >> 0) & MV_BITMASKS.byte_mask;
    record_buf[buf_pos++] = (crc >> 8) & MV_BITMASKS.byte_mask;
    record_buf[buf_pos++] = (crc >> 16) & MV_BITMASKS.byte_mask;
    record_buf[buf_pos++] = (crc >> 24) & MV_BITMASKS.byte_mask;

    record_buf[buf_pos++] = MV_OPCODES.end;

    if (fwrite(record_buf, 1, buf_pos, f) != buf_pos) return -1;
    return 0;
}

static int read_record(thumbdb_instance_t* inst, FILE* f, record_t* rec, int is_first) {
    uint8_t record_buf[8192];
    size_t buf_pos = 0;
    long start_pos = ftell(f);

    uint8_t begin;
    if (fread(&begin, 1, 1, f) != 1) return -1;
    if (begin != MV_OPCODES.begin) return -1;
    record_buf[buf_pos++] = begin;

    uint64_t record_seq;
    if (read_varint(f, &record_seq) != 0) return -1;
    rec->record_sequence = record_seq;

    uint64_t dir_count;
    if (read_varint(f, &dir_count) != 0) return -1;
    rec->dir_count = (size_t)dir_count;

    if (read_bit_packed_indexes(f, &rec->dir_indexes, rec->dir_count, inst->dir_table.count > 0 ? inst->dir_table.count : 1) != 0)
        return -1;

    uint64_t filename_delta;
    if (read_varint(f, &filename_delta) != 0) { free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }

    uint64_t filename_val = 0;
    if (filename_delta == 0 && !is_first) {
        long before_strlen = ftell(f);
        uint64_t maybe_strlen;
        size_t varint_bytes;
        if (read_varint_with_size(f, &maybe_strlen, &varint_bytes) == 0 && maybe_strlen > 0 && maybe_strlen <= 255) {
            rec->filename = malloc((size_t)maybe_strlen + 1);
            if (!rec->filename) { free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
            if (fread(rec->filename, 1, (size_t)maybe_strlen, f) != (size_t)maybe_strlen) {
                free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1;
            }
            rec->filename[maybe_strlen] = '\0';
            inst->last_filename_delta = 0;
        } else {
            fseek(f, before_strlen, SEEK_SET);
            filename_val = is_first ? filename_delta : (inst->last_filename_delta + filename_delta);
            rec->filename = malloc(32);
            if (!rec->filename) { free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
            snprintf(rec->filename, 32, "%llu", (unsigned long long)filename_val);
            inst->last_filename_delta = filename_val;
        }
    } else {
        filename_val = is_first ? filename_delta : (inst->last_filename_delta + filename_delta);
        rec->filename = malloc(32);
        if (!rec->filename) { free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
        snprintf(rec->filename, 32, "%llu", (unsigned long long)filename_val);
        inst->last_filename_delta = filename_val;
    }

    uint8_t meta;
    if (fread(&meta, 1, 1, f) != 1) { free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
    rec->meta = decode_meta_byte(meta);

    if (rec->meta.hash_override) {
        uint8_t hash_mode_byte;
        if (fread(&hash_mode_byte, 1, 1, f) != 1) { free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
        rec->meta.hash_mode = (hash_mode_t)(hash_mode_byte & 3);
    } else {
        rec->meta.hash_mode = inst->file_header.default_hash_mode;
    }

    if (rec->meta.has_extensions) {
        uint64_t ext_len;
        if (read_varint(f, &ext_len) != 0) { free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
        size_t ext_bytes_read = 0;
        while (ext_bytes_read < ext_len) {
            uint8_t tag;
            if (fread(&tag, 1, 1, f) != 1) { free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
            ext_bytes_read++;
            uint64_t value_len; size_t varint_size;
            if (read_varint_with_size(f, &value_len, &varint_size) != 0) { free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
            ext_bytes_read += varint_size;
            if (tag == MV_EXT_TAGS.dimensions_delta) {
                uint64_t w, h; size_t w_size, h_size;
                if (read_varint_with_size(f, &w, &w_size) != 0 || read_varint_with_size(f, &h, &h_size) != 0) { free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
                rec->width = (uint32_t)w; rec->height = (uint32_t)h;
                ext_bytes_read += w_size + h_size;
            } else if (tag == MV_EXT_TAGS.orientation && value_len == 1) {
                if (fread(&rec->orientation, 1, 1, f) != 1) { free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
                ext_bytes_read++;
            } else if (tag == MV_EXT_TAGS.codec_info && value_len > 0 && value_len < 256) {
                rec->codec_info = malloc(value_len + 1);
                if (rec->codec_info) {
                    if (fread(rec->codec_info, 1, value_len, f) != value_len) { free(rec->codec_info); rec->codec_info = NULL; free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
                    rec->codec_info[value_len] = '\0'; ext_bytes_read += value_len;
                }
            } else if (tag == MV_EXT_TAGS.gps_coords && value_len == 8) {
                float lat_f, lon_f;
                if (read_float_le(f, &lat_f) != 0 || read_float_le(f, &lon_f) != 0) { free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
                rec->gps_lat = (double)lat_f; rec->gps_lon = (double)lon_f; ext_bytes_read += 8;
            } else {
                for (uint64_t i = 0; i < value_len; i++) {
                    uint8_t dummy;
                    if (fread(&dummy, 1, 1, f) != 1) { free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
                    ext_bytes_read++;
                }
            }
        }
    }

    uint64_t timestamp_delta;
    if (read_varint(f, &timestamp_delta) != 0) { free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
    uint64_t ts_val = is_first ? timestamp_delta : (inst->last_timestamp_delta + timestamp_delta);
    rec->timestamp = ts_val;

    rec->hash_len = get_hash_length(rec->meta.hash_mode);
    if (rec->hash_len > 0) {
        if (fread(rec->hash, 1, rec->hash_len, f) != rec->hash_len) { free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
    }

    uint32_t stored_crc;
    if (read_uint32_le(f, &stored_crc) != 0) { free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }

    uint8_t end;
    if (fread(&end, 1, 1, f) != 1 || end != MV_OPCODES.end) { free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }

    long end_pos = ftell(f);
    fseek(f, start_pos, SEEK_SET);
    size_t record_size = (size_t)(end_pos - start_pos - 5);
    if (record_size > sizeof(record_buf)) { free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
    if (fread(record_buf, 1, record_size, f) != record_size) { free(rec->filename); rec->filename = NULL; free(rec->dir_indexes); rec->dir_indexes = NULL; return -1; }
    fseek(f, end_pos, SEEK_SET);

    uint32_t calc_crc;
    crypto_crc32(record_buf, record_size, &calc_crc);
    if (calc_crc != stored_crc) {
        LOG_ERROR("CRC32 mismatch for record sequence %llu — rejecting corrupt record", (unsigned long long)record_seq);
        free(rec->filename); rec->filename = NULL;
        free(rec->dir_indexes); rec->dir_indexes = NULL;
        return -1;
    }

    inst->last_timestamp_delta = rec->timestamp;
    return 0;
}

static void free_record(record_t* rec) {
    if (!rec) return;
    free(rec->filename);
    free(rec->dir_indexes);
    free(rec->codec_info);
}

static int serialize_record_to_value(thumbdb_instance_t* inst, record_t* rec, char* out, size_t outlen) {
    char path[PATH_MAX];
    if (reconstruct_path_from_indexes(inst, rec->dir_indexes, rec->dir_count, path, sizeof(path)) != 0) return -1;
    const char* ext = "";
    switch (rec->meta.type) {
        case MEDIA_JPG: ext = ".jpg"; break;
        case MEDIA_PNG: ext = ".png"; break;
        case MEDIA_GIF: ext = ".gif"; break;
        case MEDIA_WEBP: ext = ".webp"; break;
        case MEDIA_MP4: ext = ".mp4"; break;
        case MEDIA_WEBM: ext = ".webm"; break;
    }
    snprintf(out, outlen, "%s%s%s%s", path, strlen(path) > 0 ? DIR_SEP_STR : "", rec->filename, ext);
    return 0;
}

static void build_wal_dir_from_dbpath(thumbdb_instance_t* inst, char* wal_dir_out, size_t out_len) {
    wal_dir_out[0] = '\0';
    if (!inst->db_path[0]) return;
    char per_thumbs_root[PATH_MAX];
    strncpy(per_thumbs_root, inst->db_path, sizeof(per_thumbs_root) - 1);
    per_thumbs_root[sizeof(per_thumbs_root) - 1] = '\0';
    char* last = strrchr(per_thumbs_root, DIR_SEP);
    if (!last) return;
    *last = '\0';
    snprintf(wal_dir_out, out_len, "%s" DIR_SEP_STR WAL_DIR_NAME, per_thumbs_root);
}

static int write_wal_entry(thumbdb_instance_t* inst, const char* key, const char* value) {
    if (!wal_seq_mutex_inited) { if (thread_mutex_init(&wal_seq_mutex) == 0) wal_seq_mutex_inited = 1; }
    char wal_dir[PATH_MAX];
    build_wal_dir_from_dbpath(inst, wal_dir, sizeof(wal_dir));
    if (!wal_dir[0]) return -1;
    if (!is_dir(wal_dir)) platform_make_dir(wal_dir);
    unsigned int seq = atomic_fetch_add(&wal_chunk_seq, 1);
    long long ts = (long long)time(NULL);
    char chunk_path[PATH_MAX];
    snprintf(chunk_path, sizeof(chunk_path), "%s" DIR_SEP_STR WAL_CHUNK_FMT, wal_dir, ts, seq, platform_get_pid());
    FILE* f = platform_fopen(chunk_path, "w");
    if (!f) return -1;
    fprintf(f, "%s\n%s\n", key, value ? value : "__DELETE__");
    fflush(f);
    platform_fsync(fileno(f));
    fclose(f);
    return 0;
}

static int process_wal_chunks(thumbdb_instance_t* inst) {
    char wal_dir[PATH_MAX];
    build_wal_dir_from_dbpath(inst, wal_dir, sizeof(wal_dir));
    if (!is_dir(wal_dir)) return 0;
    diriter it;
    if (!dir_open(&it, wal_dir)) return 0;
    const char* entry;
    while ((entry = dir_next(&it))) {
        if (!entry || strcmp(entry, ".") == 0 || strcmp(entry, "..") == 0) continue;
        if (strstr(entry, ".wal") == NULL) continue;
        char chunk_path[PATH_MAX];
        snprintf(chunk_path, sizeof(chunk_path), "%s" DIR_SEP_STR "%s", wal_dir, entry);
        FILE* f = platform_fopen(chunk_path, "r");
        if (!f) continue;
        char key[PATH_MAX], value[PATH_MAX];
        if (fgets(key, sizeof(key), f)) {
            size_t key_len = strcspn(key, "\r\n"); key[key_len] = '\0';
            if (fgets(value, sizeof(value), f)) {
                size_t val_len = strcspn(value, "\r\n"); value[val_len] = '\0';
                if (strcmp(value, "__DELETE__") == 0) rh_remove(inst->rh_tbl, key, strlen(key));
                else rh_insert(inst->rh_tbl, key, strlen(key), (const unsigned char*)value, strlen(value) + 1);
            }
        }
        fclose(f);
        platform_file_delete(chunk_path);
    }
    dir_close(&it);
    return 0;
}

static int thumbdb_recover_from_corruption(thumbdb_instance_t* inst) {
    LOG_WARN("thumbdb: database corrupted, attempting recovery");
    if (!inst->db_path[0]) return -1;
    char backup_path[PATH_MAX];
    snprintf(backup_path, sizeof(backup_path), "%s.corrupt", inst->db_path);
    platform_move_file(inst->db_path, backup_path);
    LOG_INFO("thumbdb: backed up corrupted database to %s", backup_path);
    if (inst->rh_tbl) rh_destroy(inst->rh_tbl);
    inst->rh_tbl = rh_create(INITIAL_BUCKETS_BITS);
    if (!inst->rh_tbl) return -1;
    inst->dir_table.count = 0; inst->dir_table.capacity = 0;
    if (inst->dir_table.dirs) { free(inst->dir_table.dirs); inst->dir_table.dirs = NULL; }
    FILE* f_new = platform_fopen(inst->db_path, "wb");
    if (f_new) {
        uint8_t magic_hi = (MV_CONSTANTS.db_magic >> 8) & 0xFF;
        uint8_t magic_lo = MV_CONSTANTS.db_magic & 0xFF;
        fwrite(&magic_hi, 1, 1, f_new); fwrite(&magic_lo, 1, 1, f_new);
        uint8_t version = MV_CONSTANTS.version; fwrite(&version, 1, 1, f_new);
        uint8_t flags = MV_BITMASKS.flags_init; fwrite(&flags, 1, 1, f_new);
        uint64_t record_count = 0; write_varint(f_new, record_count);
        uint64_t base_timestamp = (uint64_t)time(NULL);
        write_uint64_le(f_new, base_timestamp);
        inst->file_header.base_timestamp = base_timestamp;
        uint64_t dir_table_size = 0; write_varint(f_new, dir_table_size);
        fflush(f_new); platform_fsync(fileno(f_new)); fclose(f_new);
        LOG_INFO("thumbdb: recovered database at %s", inst->db_path);
        return 0;
    }
    return -1;
}

static int load_database(thumbdb_instance_t* inst) {
    FILE* f = platform_fopen(inst->db_path, "rb");
    if (!f) return 0;
    uint8_t magic_hi, magic_lo;
    if (fread(&magic_hi, 1, 1, f) != 1 || fread(&magic_lo, 1, 1, f) != 1) { LOG_ERROR("thumbdb: cannot read magic header in %s", inst->db_path); fclose(f); return -1; }
    uint16_t magic = ((uint16_t)magic_hi << 8) | magic_lo;
    if (magic != MV_CONSTANTS.db_magic) { LOG_ERROR("thumbdb: invalid magic header 0x%04X in %s", magic, inst->db_path); fclose(f); return -1; }
    uint8_t version;
    if (fread(&version, 1, 1, f) != 1 || version != MV_CONSTANTS.version) LOG_WARN("thumbdb: version mismatch in %s", inst->db_path);
    uint8_t flags;
    if (fread(&flags, 1, 1, f) != 1) { fclose(f); return -1; }
    inst->file_header.flags = flags;
    inst->file_header.has_index = (flags >> 7) & 1;
    inst->file_header.timestamp_precision = (flags >> 6) & 1;
    inst->file_header.default_hash_mode = (hash_mode_t)((flags >> 4) & 3);
    uint64_t record_count;
    if (read_varint(f, &record_count) != 0) { fclose(f); return -1; }
    if (read_uint64_le(f, &inst->file_header.base_timestamp) != 0) { fclose(f); return -1; }
    uint64_t dir_table_size;
    if (read_varint(f, &dir_table_size) != 0) { fclose(f); return -1; }
    for (uint64_t i = 0; i < dir_table_size; i++) {
        uint64_t prefix_len, suffix_len;
        if (read_varint(f, &prefix_len) != 0 || read_varint(f, &suffix_len) != 0) { fclose(f); return -1; }
        char suffix[PATH_MAX];
        if (suffix_len > 0) { if (fread(suffix, 1, suffix_len, f) != suffix_len) { fclose(f); return -1; } suffix[suffix_len] = '\0'; }
        else suffix[0] = '\0';
        char full_path[PATH_MAX];
        if (prefix_len > 0 && inst->dir_table.count > 0) {
            size_t copy_len = prefix_len; if (copy_len >= PATH_MAX) copy_len = PATH_MAX - 1;
            strncpy(full_path, inst->dir_table.dirs[inst->dir_table.count - 1], copy_len); full_path[copy_len] = '\0';
            strncat(full_path, suffix, PATH_MAX - strlen(full_path) - 1);
        } else { strncpy(full_path, suffix, PATH_MAX - 1); full_path[PATH_MAX - 1] = '\0'; }
        add_dir_to_table(inst, full_path);
    }
    inst->last_filename_delta = 0; inst->last_timestamp_delta = 0; inst->current_record_seq = 0;
    int is_first = 1; long records_end_pos = 0; int in_transaction = 0;
    while (!feof(f)) {
        uint8_t peek; long pos = ftell(f);
        if (fread(&peek, 1, 1, f) != 1) break;
        fseek(f, pos, SEEK_SET);
        if (inst->file_header.has_index && peek == ((MV_CONSTANTS.index_magic >> 8) & 0xFF)) { records_end_pos = pos; break; }
        if (peek == MV_OPCODES.tx_begin) { fgetc(f); in_transaction = 1; continue; }
        if (peek == MV_OPCODES.tx_end) { fgetc(f); in_transaction = 0; continue; }
        if (peek == MV_OPCODES.delete_op) {
            fgetc(f); uint64_t filename_val; uint64_t ts;
            if (read_varint(f, &filename_val) == 0 && read_varint(f, &ts) == 0) {
                uint8_t end_marker;
                if (fread(&end_marker, 1, 1, f) == 1 && end_marker == MV_OPCODES.end) {
                    char key[64]; snprintf(key, sizeof(key), "%llu", (unsigned long long)filename_val);
                    rh_remove(inst->rh_tbl, key, strlen(key));
                }
            }
            continue;
        }
        record_t rec = {0};
        rec.record_sequence = inst->current_record_seq++;
        if (read_record(inst, f, &rec, is_first) != 0) { if (feof(f)) break; LOG_ERROR("Failed to read record from database"); free_record(&rec); continue; }
        is_first = 0;
        char value[PATH_MAX];
        if (serialize_record_to_value(inst, &rec, value, sizeof(value)) == 0) {
            char key[64]; snprintf(key, sizeof(key), "%s", rec.filename);
            rh_insert(inst->rh_tbl, key, strlen(key), (const unsigned char*)value, strlen(value) + 1);
        }
        free_record(&rec);
    }
    if (inst->file_header.has_index && records_end_pos > 0) {
        fseek(f, records_end_pos, SEEK_SET);
        uint8_t idx_magic_hi, idx_magic_lo;
        if (fread(&idx_magic_hi, 1, 1, f) != 1 || fread(&idx_magic_lo, 1, 1, f) != 1) { LOG_WARN("thumbdb: cannot read index magic"); fclose(f); return 0; }
        if ((((uint16_t)idx_magic_hi << 8) | idx_magic_lo) != MV_CONSTANTS.index_magic) { LOG_WARN("thumbdb: invalid index magic"); fclose(f); return 0; }
        uint64_t index_count;
        if (read_varint(f, &index_count) != 0) { fclose(f); return 0; }
        if (inst->index_table.entries) free(inst->index_table.entries);
        inst->index_table.entries = calloc(index_count, sizeof(index_entry_t));
        if (!inst->index_table.entries) { fclose(f); return 0; }
        inst->index_table.count = index_count; inst->index_table.capacity = index_count;
        for (uint64_t i = 0; i < index_count; i++) {
            if (read_varint(f, &inst->index_table.entries[i].file_offset) != 0) break;
            if (read_varint(f, &inst->index_table.entries[i].filename_hash) != 0) break;
        }
    }
    fclose(f);
    return 0;
}

static void* async_worker_thread(void* arg) {
    thumbdb_instance_t* inst = (thumbdb_instance_t*)arg;
    if (!inst) return NULL;
    while (inst->async_worker_running) {
        async_op_t* op = NULL;
        thread_mutex_lock(&inst->async_queue_mutex);
        if (inst->async_queue_head) {
            op = inst->async_queue_head;
            inst->async_queue_head = op->next;
            if (!inst->async_queue_head) inst->async_queue_tail = NULL;
        }
        thread_mutex_unlock(&inst->async_queue_mutex);
        if (op) {
            thread_mutex_lock(&inst->mutex);
            if (op->is_delete) rh_remove(inst->rh_tbl, op->key, strlen(op->key));
            else rh_insert(inst->rh_tbl, op->key, strlen(op->key), (const unsigned char*)op->value, strlen(op->value) + 1);
            thread_mutex_unlock(&inst->mutex);
            free(op);
        } else {
            platform_sleep_ms(50);
        }
    }
    return NULL;
}

static int enqueue_async_op(thumbdb_instance_t* inst, const char* key, const char* value, int is_delete) {
    async_op_t* op = calloc(1, sizeof(async_op_t));
    if (!op) return -1;
    strncpy(op->key, key, sizeof(op->key) - 1); op->key[sizeof(op->key) - 1] = '\0';
    if (!is_delete && value) { strncpy(op->value, value, sizeof(op->value) - 1); op->value[sizeof(op->value) - 1] = '\0'; }
    op->is_delete = is_delete; op->next = NULL;
    thread_mutex_lock(&inst->async_queue_mutex);
    if (inst->async_queue_tail) { inst->async_queue_tail->next = op; inst->async_queue_tail = op; }
    else { inst->async_queue_head = inst->async_queue_tail = op; }
    thread_mutex_unlock(&inst->async_queue_mutex);
    return 0;
}

int thumbdb_open(void) {
    LOG_WARN("thumbdb_open: global DB disabled; use thumbdb_open_for_dir()");
    return -1;
}

thumbdb_instance_t* thumbdb_open_for_dir(const char* db_full_path) {
    if (!db_full_path || db_full_path[0] == '\0') return NULL;

    thumbdb_instance_t* inst = thumbdb_find_instance(db_full_path);
    if (inst) {
        thread_mutex_lock(&inst->mutex);
        if (inst->db_inited) { thread_mutex_unlock(&inst->mutex); return inst; }
        thread_mutex_unlock(&inst->mutex);
    }

    inst = thumbdb_get_or_create_instance(db_full_path);
    if (!inst) return NULL;

    thread_mutex_lock(&inst->mutex);
    if (inst->db_inited) { thread_mutex_unlock(&inst->mutex); return inst; }

    inst->rh_tbl = rh_create(INITIAL_BUCKETS_BITS);
    if (!inst->rh_tbl) { thread_mutex_unlock(&inst->mutex); thumbdb_release_instance(inst); return NULL; }

    FILE* f_check = platform_fopen(inst->db_path, "rb");
    if (!f_check) {
        FILE* f_new = platform_fopen(inst->db_path, "wb");
        if (f_new) {
            uint8_t magic_hi = (MV_CONSTANTS.db_magic >> 8) & 0xFF, magic_lo = MV_CONSTANTS.db_magic & 0xFF;
            fwrite(&magic_hi, 1, 1, f_new); fwrite(&magic_lo, 1, 1, f_new);
            uint8_t version = MV_CONSTANTS.version; fwrite(&version, 1, 1, f_new);
            uint8_t flags = MV_BITMASKS.flags_init; fwrite(&flags, 1, 1, f_new);
            uint64_t record_count = 0; write_varint(f_new, record_count);
            uint64_t base_timestamp = (uint64_t)time(NULL); write_uint64_le(f_new, base_timestamp);
            inst->file_header.base_timestamp = base_timestamp;
            uint64_t dir_table_size = 0; write_varint(f_new, dir_table_size);
            fflush(f_new); platform_fsync(fileno(f_new)); fclose(f_new);
            LOG_INFO("thumbdb: created new database %s", inst->db_path);
        }
    } else {
        fclose(f_check);
        if (load_database(inst) != 0) {
            LOG_WARN("thumbdb: load_database returned error, attempting recovery");
            thumbdb_recover_from_corruption(inst);
        } else if (thumbdb_validate(inst) != 0) {
            LOG_WARN("thumbdb: validation failed, attempting recovery");
            thumbdb_recover_from_corruption(inst);
        }
        process_wal_chunks(inst);
        LOG_INFO("thumbdb: loaded and validated database %s", inst->db_path);
    }
    inst->db_inited = 1;
    thread_mutex_unlock(&inst->mutex);
    return inst;
}

void thumbdb_close(thumbdb_instance_t* inst) {
    if (!inst) return;
    thread_mutex_lock(&inst->mutex);
    if (inst->rh_tbl) { rh_destroy(inst->rh_tbl); inst->rh_tbl = NULL; }
    for (size_t i = 0; i < inst->dir_table.count; i++) free(inst->dir_table.dirs[i]);
    free(inst->dir_table.dirs); inst->dir_table.dirs = NULL;
    inst->dir_table.count = 0; inst->dir_table.capacity = 0;
    if (inst->index_table.entries) { free(inst->index_table.entries); inst->index_table.entries = NULL; }
    inst->index_table.count = 0; inst->index_table.capacity = 0;
    inst->db_inited = 0;
    thread_mutex_unlock(&inst->mutex);
    thumbdb_release_instance(inst);
}

static int copy_to_snapshot_cb(const char* key, const unsigned char* val, size_t val_len, void* ctx) {
    rh_table_t* snap = (rh_table_t*)ctx;
    rh_insert(snap, key, strlen(key), val, val_len);
    return 0;
}

static int copy_from_snapshot_cb(const char* key, const unsigned char* val, size_t val_len, void* ctx) {
    rh_table_t* tbl = (rh_table_t*)ctx;
    rh_insert(tbl, key, strlen(key), val, val_len);
    return 0;
}

typedef struct { thumbdb_instance_t* inst; FILE* fp; } write_changed_ctx_t;

static int write_changed_cb(const char* key, const unsigned char* val, size_t val_len, void* ctx) {
    write_changed_ctx_t* wctx = (write_changed_ctx_t*)ctx;
    thumbdb_instance_t* inst = wctx->inst;
    FILE* fp = wctx->fp;
    unsigned char* old_val = NULL; size_t old_len = 0; size_t key_len = strlen(key);
    int found = rh_find(inst->tx_snapshot, key, key_len, &old_val, &old_len);
    if (found != 0 || !old_val || old_len != val_len || memcmp(old_val, val, val_len) != 0) {
        char key_str[512]; if (key_len >= sizeof(key_str)) key_len = sizeof(key_str) - 1;
        memcpy(key_str, key, key_len); key_str[key_len] = '\0';
        char val_str[PATH_MAX]; if (val_len >= sizeof(val_str)) val_len = sizeof(val_str) - 1;
        memcpy(val_str, val, val_len); val_str[val_len] = '\0';
        record_t rec = {0};
        rec.record_sequence = inst->current_record_seq++;
        rec.filename = strdup(key_str);
        rec.timestamp = (uint64_t)time(NULL);
        rec.meta.type = get_media_type_from_path(val_str);
        rec.meta.animated = 0; rec.meta.thumb_mode = 0; rec.meta.hash_override = 0; rec.meta.has_extensions = 0;
        rec.meta.hash_mode = inst->file_header.default_hash_mode;
        if (parse_path_into_dirs(inst, val_str, &rec.dir_indexes, &rec.dir_count) == 0) {
            if (is_file(val_str)) { if (crypto_md5_file(val_str, rec.hash) != 0) memset(rec.hash, 0, sizeof(rec.hash)); }
            else { memset(rec.hash, 0, sizeof(rec.hash)); LOG_WARN("thumbdb_tx_commit: media file missing for hash: %s", val_str); }
            rec.hash_len = get_hash_length(rec.meta.hash_mode);
            int is_first = (rec.record_sequence == 0);
            write_record(inst, fp, &rec, is_first);
        }
        free_record(&rec);
    }
    return 0;
}

int thumbdb_tx_begin(thumbdb_instance_t* inst) {
    if (!inst) return -1;
    thread_mutex_lock(&inst->mutex);
    if (inst->tx_active) { thread_mutex_unlock(&inst->mutex); return -1; }
    if (inst->tx_snapshot) rh_destroy(inst->tx_snapshot);
    inst->tx_snapshot = rh_create(INITIAL_BUCKETS_BITS);
    if (!inst->tx_snapshot) { thread_mutex_unlock(&inst->mutex); return -1; }
    rh_iterate(inst->rh_tbl, copy_to_snapshot_cb, inst->tx_snapshot);
    inst->tx_active = 1;
    thread_mutex_unlock(&inst->mutex);
    return 0;
}

int thumbdb_tx_abort(thumbdb_instance_t* inst) {
    if (!inst) return -1;
    thread_mutex_lock(&inst->mutex);
    if (!inst->tx_active) { thread_mutex_unlock(&inst->mutex); return -1; }
    if (inst->tx_snapshot && inst->rh_tbl) {
        rh_destroy(inst->rh_tbl);
        inst->rh_tbl = rh_create(INITIAL_BUCKETS_BITS);
        if (inst->rh_tbl) rh_iterate(inst->tx_snapshot, copy_from_snapshot_cb, inst->rh_tbl);
        rh_destroy(inst->tx_snapshot); inst->tx_snapshot = NULL;
    }
    inst->tx_active = 0;
    thread_mutex_unlock(&inst->mutex);
    return 0;
}

int thumbdb_tx_commit(thumbdb_instance_t* inst) {
    if (!inst) return -1;
    thread_mutex_lock(&inst->mutex);
    if (!inst->tx_active) { thread_mutex_unlock(&inst->mutex); return -1; }
    FILE* f = platform_fopen(inst->db_path, "ab");
    if (f) {
        fputc(MV_OPCODES.tx_begin, f);
        write_changed_ctx_t wctx = {inst, f};
        rh_iterate(inst->rh_tbl, write_changed_cb, &wctx);
        fputc(MV_OPCODES.tx_end, f);
        fflush(f); platform_fsync(fileno(f)); fclose(f);
    }
    if (inst->tx_snapshot) { rh_destroy(inst->tx_snapshot); inst->tx_snapshot = NULL; }
    inst->tx_active = 0;
    thread_mutex_unlock(&inst->mutex);
    return 0;
}

int thumbdb_set(thumbdb_instance_t* inst, const char* key, const char* value) {
    if (!inst || !inst->rh_tbl || !key || !value) return -1;
    thread_mutex_lock(&inst->mutex);
    if (!inst->tx_active) {
        record_t rec = {0};
        rec.record_sequence = inst->current_record_seq++;
        rec.filename = strdup(key);
        rec.timestamp = (uint64_t)time(NULL);
        rec.meta.type = get_media_type_from_path(value);
        rec.meta.animated = 0; rec.meta.thumb_mode = 0; rec.meta.hash_override = 0; rec.meta.has_extensions = 0;
        rec.meta.hash_mode = inst->file_header.default_hash_mode;
        if (parse_path_into_dirs(inst, value, &rec.dir_indexes, &rec.dir_count) != 0) { free(rec.filename); thread_mutex_unlock(&inst->mutex); return -1; }
        if (is_file(value)) { if (crypto_md5_file(value, rec.hash) != 0) memset(rec.hash, 0, sizeof(rec.hash)); }
        else { memset(rec.hash, 0, sizeof(rec.hash)); LOG_WARN("thumbdb_set: media file missing for hash: %s", value); }
        rec.hash_len = get_hash_length(rec.meta.hash_mode);
        FILE* f = platform_fopen(inst->db_path, "ab");
        if (f) { int is_first = (rec.record_sequence == 0); write_record(inst, f, &rec, is_first); fflush(f); platform_fsync(fileno(f)); fclose(f); }
        free_record(&rec);
    }
    int ret = rh_insert(inst->rh_tbl, key, strlen(key), (const unsigned char*)value, strlen(value) + 1);
    thread_mutex_unlock(&inst->mutex);
    return ret;
}

int thumbdb_get(thumbdb_instance_t* inst, const char* key, char* buf, size_t buflen) {
    if (!inst || !inst->rh_tbl || !key || !buf || buflen == 0) return -1;
    thread_mutex_lock(&inst->mutex);
    unsigned char* val = NULL; size_t val_len = 0;
    int ret = rh_find(inst->rh_tbl, key, strlen(key), &val, &val_len);
    if (ret == 0 && val && val_len > 0) { strncpy(buf, (const char*)val, buflen - 1); buf[buflen - 1] = '\0'; thread_mutex_unlock(&inst->mutex); return 0; }
    thread_mutex_unlock(&inst->mutex);
    return -1;
}

int thumbdb_delete(thumbdb_instance_t* inst, const char* key) {
    if (!inst || !inst->rh_tbl || !key) return -1;
    thread_mutex_lock(&inst->mutex);
    int ret = -1;
    if (!inst->tx_active) {
        FILE* f = platform_fopen(inst->db_path, "ab");
        if (f) {
            if (fputc(MV_OPCODES.delete_op, f) != EOF && write_varint(f, strtoull(key, NULL, 10)) == 0 &&
                write_varint(f, (uint64_t)time(NULL)) == 0 && fputc(MV_OPCODES.end, f) != EOF) {
                fflush(f); if (platform_fsync(fileno(f)) == 0) ret = 0;
            }
            fclose(f);
        }
    }
    if (rh_remove(inst->rh_tbl, key, strlen(key)) == 0) ret = 0;
    thread_mutex_unlock(&inst->mutex);
    return ret;
}

void thumbdb_iterate(thumbdb_instance_t* inst, void (*cb)(const char* key, const char* value, void* ctx), void* ctx) {
    if (!inst || !inst->rh_tbl || !cb) return;
    thread_mutex_lock(&inst->mutex);
    struct iter_ctx { void (*cb)(const char*, const char*, void*); void* user; } ic = {cb, ctx};
    rh_iterate(inst->rh_tbl, NULL, &ic);
    thread_mutex_unlock(&inst->mutex);
}

char* thumbdb_get_record_detail(thumbdb_instance_t* inst, const char* key) {
    if (!inst || !key) return NULL;
    thread_mutex_lock(&inst->mutex);
    FILE* f = platform_fopen(inst->db_path, "rb");
    if (!f) { thread_mutex_unlock(&inst->mutex); return NULL; }
    uint8_t magic_hi, magic_lo;
    if (fread(&magic_hi, 1, 1, f) != 1 || fread(&magic_lo, 1, 1, f) != 1) { fclose(f); thread_mutex_unlock(&inst->mutex); return NULL; }
    if ((((uint16_t)magic_hi << 8) | magic_lo) != MV_CONSTANTS.db_magic) { fclose(f); thread_mutex_unlock(&inst->mutex); return NULL; }
    uint8_t version; if (fread(&version, 1, 1, f) != 1) { fclose(f); thread_mutex_unlock(&inst->mutex); return NULL; }
    uint8_t flags; if (fread(&flags, 1, 1, f) != 1) { fclose(f); thread_mutex_unlock(&inst->mutex); return NULL; }
    int has_index = (flags >> 7) & 1;
    uint64_t record_count, base_timestamp, dir_table_size;
    if (read_varint(f, &record_count) != 0 || read_uint64_le(f, &base_timestamp) != 0 || read_varint(f, &dir_table_size) != 0) { fclose(f); thread_mutex_unlock(&inst->mutex); return NULL; }
    for (uint64_t i = 0; i < dir_table_size; i++) { uint64_t prefix_len, suffix_len; if (read_varint(f, &prefix_len) != 0 || read_varint(f, &suffix_len) != 0) { fclose(f); thread_mutex_unlock(&inst->mutex); return NULL; } if (suffix_len > 0) fseek(f, suffix_len, SEEK_CUR); }
    int is_first = 1;
    while (!feof(f)) {
        uint8_t peek; long pos = ftell(f);
        if (fread(&peek, 1, 1, f) != 1) break;
        fseek(f, pos, SEEK_SET);
        if (has_index && peek == ((MV_CONSTANTS.index_magic >> 8) & 0xFF)) break;
        record_t rec = {0};
        if (read_record(inst, f, &rec, is_first) != 0) { if (feof(f)) break; free_record(&rec); continue; }
        is_first = 0;
        if (strcmp(rec.filename, key) == 0) {
            char* result = malloc(8192);
            if (!result) { free_record(&rec); fclose(f); thread_mutex_unlock(&inst->mutex); return NULL; }
            char hash_str[64] = ""; if (rec.hash_len > 0) { char* hp = hash_str; for (size_t i = 0; i < rec.hash_len && i < 16; i++) hp += snprintf(hp, sizeof(hash_str) - (hp - hash_str), "%02x", rec.hash[i]); }
            const char* type_str = "unknown";
            switch (rec.meta.type) { case MEDIA_JPG: type_str = "JPG"; break; case MEDIA_PNG: type_str = "PNG"; break; case MEDIA_GIF: type_str = "GIF"; break; case MEDIA_WEBP: type_str = "WEBP"; break; case MEDIA_MP4: type_str = "MP4"; break; case MEDIA_WEBM: type_str = "WEBM"; break; }
            const char* hash_mode_str = "unknown";
            switch (rec.meta.hash_mode) { case HASH_FULL_MD5: hash_mode_str = "FULL_MD5"; break; case HASH_HALF_MD5: hash_mode_str = "HALF_MD5"; break; case HASH_RIPEMD128: hash_mode_str = "RIPEMD128"; break; case HASH_NONE: hash_mode_str = "NONE"; break; }
            snprintf(result, 8192, "{\"record_seq\":%llu,\"timestamp\":%llu,\"media_type\":\"%s\",\"animated\":%s,\"thumb_mode\":%d,\"hash_mode\":\"%s\",\"hash\":\"%s\",\"dir_count\":%zu,\"width\":%u,\"height\":%u,\"duration\":%u,\"crc32\":%u,\"orientation\":%u,\"gps_lat\":%.6f,\"gps_lon\":%.6f}", (unsigned long long)rec.record_sequence, (unsigned long long)rec.timestamp, type_str, rec.meta.animated ? "true" : "false", rec.meta.thumb_mode, hash_mode_str, hash_str, rec.dir_count, rec.width, rec.height, rec.duration, rec.crc32, rec.orientation, rec.gps_lat, rec.gps_lon);
            free_record(&rec); fclose(f); thread_mutex_unlock(&inst->mutex); return result;
        }
        free_record(&rec);
    }
    fclose(f);
    thread_mutex_unlock(&inst->mutex);
    return NULL;
}

int thumbdb_find_for_media(thumbdb_instance_t* inst, const char* media_path, char* out_key, size_t out_key_len) {
    if (!inst || !inst->rh_tbl || !media_path || !out_key || out_key_len == 0) return -1;
    thread_mutex_lock(&inst->mutex);
    out_key[0] = '\0';
    thread_mutex_unlock(&inst->mutex);
    return -1;
}

typedef struct { char* key; char* value; } compact_entry_t;

static int collect_entry_cb(const char* key, const unsigned char* value, size_t value_len, void* user_data) {
    compact_entry_t** entries = (compact_entry_t**)((void**)user_data)[0];
    size_t* count = (size_t*)((void**)user_data)[1];
    size_t* capacity = (size_t*)((void**)user_data)[2];
    size_t key_len = strlen(key);
    if (*count >= *capacity) {
        size_t new_cap = (*capacity == 0) ? 64 : (*capacity * 2);
        compact_entry_t* new_entries = realloc(*entries, new_cap * sizeof(compact_entry_t));
        if (!new_entries) return -1;
        *entries = new_entries; *capacity = new_cap;
    }
    (*entries)[*count].key = malloc(key_len + 1);
    (*entries)[*count].value = malloc(value_len + 1);
    if ((*entries)[*count].key && (*entries)[*count].value) {
        memcpy((*entries)[*count].key, key, key_len); (*entries)[*count].key[key_len] = '\0';
        memcpy((*entries)[*count].value, value, value_len); (*entries)[*count].value[value_len] = '\0';
        (*count)++;
    } else { free((*entries)[*count].key); free((*entries)[*count].value); return -1; }
    return 0;
}

int thumbdb_compact(thumbdb_instance_t* inst) {
    if (!inst || !inst->rh_tbl) return -1;
    thread_mutex_lock(&inst->mutex);
    compact_entry_t* entries = NULL; size_t entry_count = 0, entry_capacity = 0;
    void* user_data[3] = {&entries, &entry_count, &entry_capacity};
    rh_iterate(inst->rh_tbl, collect_entry_cb, user_data);
    if (inst->index_table.entries) { free(inst->index_table.entries); inst->index_table.entries = NULL; }
    inst->index_table.count = 0; inst->index_table.capacity = 0;
    char tmp_path[PATH_MAX]; snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", inst->db_path);
    FILE* f = platform_fopen(tmp_path, "wb");
    if (!f) { for (size_t i = 0; i < entry_count; i++) { free(entries[i].key); free(entries[i].value); } free(entries); thread_mutex_unlock(&inst->mutex); return -1; }
    uint8_t magic_hi = (MV_CONSTANTS.db_magic >> 8) & 0xFF, magic_lo = MV_CONSTANTS.db_magic & 0xFF;
    fwrite(&magic_hi, 1, 1, f); fwrite(&magic_lo, 1, 1, f);
    uint8_t version = MV_CONSTANTS.version; fwrite(&version, 1, 1, f);
    uint8_t flags = inst->file_header.flags | 0x80; fwrite(&flags, 1, 1, f);
    write_varint(f, (uint64_t)entry_count);
    write_uint64_le(f, inst->file_header.base_timestamp);
    write_varint(f, (uint64_t)inst->dir_table.count);
    for (size_t i = 0; i < inst->dir_table.count; i++) {
        size_t prefix_len = 0;
        if (i > 0) { const char* prev = inst->dir_table.dirs[i - 1]; const char* curr = inst->dir_table.dirs[i]; while (prev[prefix_len] && curr[prefix_len] && prev[prefix_len] == curr[prefix_len]) prefix_len++; }
        size_t suffix_len = strlen(inst->dir_table.dirs[i]) - prefix_len;
        write_varint(f, prefix_len); write_varint(f, suffix_len);
        if (suffix_len > 0) fwrite(inst->dir_table.dirs[i] + prefix_len, 1, suffix_len, f);
    }
    inst->last_filename_delta = 0; inst->last_timestamp_delta = 0; inst->current_record_seq = 0;
    for (size_t i = 0; i < entry_count; i++) {
        long record_offset = ftell(f);
        record_t rec = {0};
        rec.record_sequence = inst->current_record_seq++;
        rec.filename = entries[i].key;
        rec.timestamp = (uint64_t)time(NULL);
        rec.meta.type = get_media_type_from_path(entries[i].value);
        rec.meta.animated = 0; rec.meta.thumb_mode = 0; rec.meta.hash_override = 0; rec.meta.has_extensions = 0;
        rec.meta.hash_mode = inst->file_header.default_hash_mode;
        if (parse_path_into_dirs(inst, entries[i].value, &rec.dir_indexes, &rec.dir_count) == 0) {
            if (is_file(entries[i].value)) { if (crypto_md5_file(entries[i].value, rec.hash) != 0) memset(rec.hash, 0, sizeof(rec.hash)); }
            else memset(rec.hash, 0, sizeof(rec.hash));
            rec.hash_len = get_hash_length(rec.meta.hash_mode);
            int is_first = (i == 0);
            if (write_record(inst, f, &rec, is_first) == 0) add_index_entry(inst, (uint64_t)record_offset, entries[i].key);
            free(rec.dir_indexes);
        }
    }
    if (write_index_block(inst, f) != 0) LOG_WARN("thumbdb_compact: failed to write index block");
    else { inst->file_header.has_index = 1; LOG_DEBUG("thumbdb_compact: wrote index with %zu entries", inst->index_table.count); }
    fflush(f); platform_fsync(fileno(f)); fclose(f);
    for (size_t i = 0; i < entry_count; i++) { free(entries[i].key); free(entries[i].value); }
    free(entries);
    {
        char bak_path[PATH_MAX]; snprintf(bak_path, sizeof(bak_path), "%s.bak", inst->db_path);
        platform_file_delete(bak_path);
        if (rename(inst->db_path, bak_path) != 0) LOG_WARN("thumbdb_compact: could not backup old database %s", inst->db_path);
        if (rename(tmp_path, inst->db_path) != 0) { LOG_ERROR("thumbdb_compact: failed to rename %s to %s", tmp_path, inst->db_path); rename(bak_path, inst->db_path); thread_mutex_unlock(&inst->mutex); return -1; }
        platform_file_delete(bak_path);
    }
    inst->last_filename_delta = 0; inst->last_timestamp_delta = 0; inst->current_record_seq = 0;
    thread_mutex_unlock(&inst->mutex);
    return 0;
}

int thumbdb_sweep_orphans(thumbdb_instance_t* inst) {
    if (!inst || !inst->rh_tbl) return -1;
    thread_mutex_lock(&inst->mutex);
    char per_thumbs_root[PATH_MAX]; per_thumbs_root[0] = '\0';
    if (!inst->db_path[0]) { thread_mutex_unlock(&inst->mutex); return -1; }
    strncpy(per_thumbs_root, inst->db_path, sizeof(per_thumbs_root) - 1); per_thumbs_root[sizeof(per_thumbs_root) - 1] = '\0';
    char* last = strrchr(per_thumbs_root, DIR_SEP); if (last) *last = '\0';
    if (!is_dir(per_thumbs_root)) { thread_mutex_unlock(&inst->mutex); return -1; }
    int deleted_count = 0;
    diriter it; if (!dir_open(&it, per_thumbs_root)) { thread_mutex_unlock(&inst->mutex); return -1; }
    const char* entry;
    while ((entry = dir_next(&it))) {
        if (!entry || strcmp(entry, ".") == 0 || strcmp(entry, "..") == 0) continue;
        if (strcmp(entry, "wal") == 0 || strcmp(entry, "thumbs.tdb") == 0 || strcmp(entry, "thumbs.tdb.tmp") == 0 || strcmp(entry, "thumbs.tdb.bak") == 0 || strcmp(entry, "thumbs.db") == 0 || strcmp(entry, "thumbs.db.tmp") == 0 || strcmp(entry, "thumbs.db.bak") == 0) continue;
        const char* ext = strrchr(entry, '.'); int is_jpeg_thumb = ext && is_jpeg_extension(ext); int is_webp_thumb = ext && ascii_stricmp(ext, ".webp") == 0;
        int has_marker = strstr(entry, "-small.") || strstr(entry, "-large.");
        if (!has_marker || (!is_jpeg_thumb && !is_webp_thumb)) continue;
        char base_key[PATH_MAX]; base_key[0] = '\0';
        const char* small_marker = strstr(entry, "-small."); const char* large_marker = strstr(entry, "-large.");
        if (small_marker) { size_t base_len = (size_t)(small_marker - entry); if (base_len >= sizeof(base_key)) base_len = sizeof(base_key) - 1; memcpy(base_key, entry, base_len); base_key[base_len] = '\0'; }
        else if (large_marker) { size_t base_len = (size_t)(large_marker - entry); if (base_len >= sizeof(base_key)) base_len = sizeof(base_key) - 1; memcpy(base_key, entry, base_len); base_key[base_len] = '\0'; }
        if (!base_key[0]) continue;
        unsigned char* val = NULL; size_t val_len = 0;
        int found = rh_find(inst->rh_tbl, base_key, strlen(base_key), &val, &val_len);
        if (found != 0 || !val || val_len == 0) {
            char thumb_path[PATH_MAX]; snprintf(thumb_path, sizeof(thumb_path), "%s" DIR_SEP_STR "%s", per_thumbs_root, entry);
            if (is_file(thumb_path)) { if (platform_file_delete(thumb_path) == 0) { deleted_count++; LOG_INFO("thumbdb_sweep_orphans: deleted orphan thumbnail %s", thumb_path); } else LOG_WARN("thumbdb_sweep_orphans: failed to delete orphan thumbnail %s", thumb_path); }
        }
    }
    dir_close(&it);
    thread_mutex_unlock(&inst->mutex);
    LOG_INFO("thumbdb_sweep_orphans: deleted %d orphan thumbnails", deleted_count);
    return 0;
}

void thumbdb_request_compaction(thumbdb_instance_t* inst) {
    if (!inst) return;
    thread_mutex_lock(&inst->compaction_mutex);
    inst->compaction_requested = 1;
    thread_mutex_unlock(&inst->compaction_mutex);
}

int thumbdb_perform_requested_compaction(thumbdb_instance_t* inst) {
    if (!inst) return 0;
    thread_mutex_lock(&inst->compaction_mutex);
    if (!inst->compaction_requested) { thread_mutex_unlock(&inst->compaction_mutex); return 0; }
    inst->compaction_requested = 0;
    thread_mutex_unlock(&inst->compaction_mutex);
    return thumbdb_compact(inst);
}

static void* repair_worker_thread(void* arg) {
    thumbdb_instance_t* inst = (thumbdb_instance_t*)arg;
    int interval = 3600;
    while (inst && inst->db_inited) {
        for (int i = 0; i < interval && inst->db_inited; i++) platform_sleep_ms(1000);
        if (!inst->db_inited) break;
        LOG_INFO("thumbdb: running scheduled database repair");
        process_wal_chunks(inst);
        thumbdb_perform_requested_compaction(inst);
        thumbdb_sweep_orphans(inst);
    }
    return NULL;
}

int thumbdb_start_repair_task(thumbdb_instance_t* inst, int interval_seconds) {
    if (!inst || !inst->db_inited) return -1;
    if (thread_create_detached((void* (*)(void*))repair_worker_thread, inst) != 0) return -1;
    LOG_INFO("thumbdb: started repair task with %d second interval", interval_seconds > 0 ? interval_seconds : 3600);
    return 0;
}

int thumbdb_validate(thumbdb_instance_t* inst) {
    if (!inst || !inst->rh_tbl || !inst->db_inited) return -1;
    thread_mutex_lock(&inst->mutex);
    FILE* f = platform_fopen(inst->db_path, "rb");
    if (!f) { thread_mutex_unlock(&inst->mutex); LOG_ERROR("thumbdb_validate: cannot open %s", inst->db_path); return -1; }
    uint8_t magic_hi, magic_lo;
    if (fread(&magic_hi, 1, 1, f) != 1 || fread(&magic_lo, 1, 1, f) != 1) { LOG_ERROR("thumbdb_validate: cannot read magic header"); fclose(f); thread_mutex_unlock(&inst->mutex); return -1; }
    uint16_t magic = ((uint16_t)magic_hi << 8) | magic_lo;
    if (magic != MV_CONSTANTS.db_magic) { LOG_ERROR("thumbdb_validate: invalid magic"); fclose(f); thread_mutex_unlock(&inst->mutex); return -1; }
    uint8_t version; if (fread(&version, 1, 1, f) != 1) { fclose(f); thread_mutex_unlock(&inst->mutex); return -1; }
    uint8_t flags; if (fread(&flags, 1, 1, f) != 1) { fclose(f); thread_mutex_unlock(&inst->mutex); return -1; }
    uint64_t record_count; if (read_varint(f, &record_count) != 0) { fclose(f); thread_mutex_unlock(&inst->mutex); return -1; }
    uint64_t base_timestamp; if (read_uint64_le(f, &base_timestamp) != 0) { fclose(f); thread_mutex_unlock(&inst->mutex); return -1; }
    uint64_t dir_table_size; if (read_varint(f, &dir_table_size) != 0) { fclose(f); thread_mutex_unlock(&inst->mutex); return -1; }
    for (uint64_t i = 0; i < dir_table_size; i++) { uint64_t prefix_len, suffix_len; if (read_varint(f, &prefix_len) != 0 || read_varint(f, &suffix_len) != 0) { fclose(f); thread_mutex_unlock(&inst->mutex); return -1; } if (suffix_len > 0) fseek(f, (long)suffix_len, SEEK_CUR); }
    uint64_t records_validated = 0, crc_errors = 0;
    while (!feof(f)) {
        long record_start = ftell(f);
        uint8_t op; if (fread(&op, 1, 1, f) != 1) { if (feof(f)) break; break; }
        if (op == MV_OPCODES.delete_op) { uint64_t filename_ref; if (read_varint(f, &filename_ref) != 0) break; records_validated++; continue; }
        if (op != MV_OPCODES.begin) break;
        long crc_start = ftell(f);
        uint64_t seq, dir_count; if (read_varint(f, &seq) != 0 || read_varint(f, &dir_count) != 0) break;
        { uint32_t* dir_indexes = NULL; size_t table_size = inst->dir_table.count > 0 ? inst->dir_table.count : 1; if (read_bit_packed_indexes(f, &dir_indexes, (size_t)dir_count, table_size) != 0) break; free(dir_indexes); }
        uint64_t filename_delta; if (read_varint(f, &filename_delta) != 0) break;
        uint8_t meta_byte; if (fread(&meta_byte, 1, 1, f) != 1) break;
        uint64_t ts_delta; if (read_varint(f, &ts_delta) != 0) break;
        int hash_override = (meta_byte >> 1) & 1;
        hash_mode_t hash_mode = hash_override ? HASH_FULL_MD5 : (hash_mode_t)((flags >> 4) & 3);
        if (hash_override) { uint8_t hash_mode_byte; if (fread(&hash_mode_byte, 1, 1, f) != 1) break; hash_mode = (hash_mode_t)(hash_mode_byte & 3); }
        size_t hash_len = 0; switch (hash_mode) { case HASH_FULL_MD5: case HASH_RIPEMD128: hash_len = 16; break; case HASH_HALF_MD5: hash_len = 8; break; default: break; }
        if (hash_len > 0) fseek(f, (long)hash_len, SEEK_CUR);
        int has_extensions = meta_byte & 1;
        if (has_extensions) {
            uint64_t ext_len; if (read_varint(f, &ext_len) != 0) break;
            size_t ext_bytes_read = 0;
            while (ext_bytes_read < ext_len) {
                uint8_t tag; if (fread(&tag, 1, 1, f) != 1) break;
                ext_bytes_read++;
                uint64_t value_len; size_t varint_size; if (read_varint_with_size(f, &value_len, &varint_size) != 0) break;
                ext_bytes_read += varint_size;
                fseek(f, (long)value_len, SEEK_CUR); ext_bytes_read += (size_t)value_len;
            }
        }
        long crc_end = ftell(f);
        uint32_t stored_crc; if (read_uint32_le(f, &stored_crc) != 0) break;
        uint8_t end_op; if (fread(&end_op, 1, 1, f) != 1 || end_op != MV_OPCODES.end) break;
        fseek(f, crc_start, SEEK_SET);
        size_t crc_data_len = (size_t)(crc_end - crc_start);
        uint8_t* crc_data = malloc(crc_data_len);
        if (!crc_data) break;
        if (fread(crc_data, 1, crc_data_len, f) != crc_data_len) { free(crc_data); break; }
        uint32_t computed_crc; crypto_crc32(crc_data, crc_data_len, &computed_crc); free(crc_data);
        if (stored_crc != computed_crc) { LOG_ERROR("thumbdb_validate: CRC32 mismatch at offset %ld", record_start); crc_errors++; }
        fseek(f, crc_end + 4 + 1, SEEK_SET);
        records_validated++;
    }
    fclose(f);
    thread_mutex_unlock(&inst->mutex);
    LOG_INFO("thumbdb_validate: validated %llu records, %llu CRC errors", (unsigned long long)records_validated, (unsigned long long)crc_errors);
    return crc_errors > 0 ? -1 : 0;
}

typedef struct { int total_records; int missing_thumbs; int valid_records; char thumbs_root[PATH_MAX]; } verify_records_ctx_t;

static int verify_record_cb(const char* key, const unsigned char* value, size_t value_len, void* user_data) {
    verify_records_ctx_t* ctx = (verify_records_ctx_t*)user_data;
    ctx->total_records++;
    if (!value || value_len == 0) { LOG_WARN("thumbdb_verify_records: key %s has empty value", key); return 0; }
    const char* media_path = (const char*)value;
    if (!is_file(media_path)) LOG_WARN("thumbdb_verify_records: media file does not exist: %s", media_path);
    char small_thumb[PATH_MAX], large_thumb[PATH_MAX];
    snprintf(small_thumb, sizeof(small_thumb), "%s" DIR_SEP_STR "%s-small.jpg", ctx->thumbs_root, key);
    snprintf(large_thumb, sizeof(large_thumb), "%s" DIR_SEP_STR "%s-large.jpg", ctx->thumbs_root, key);
    int has_small = is_file(small_thumb), has_large = is_file(large_thumb);
    if (!has_small && !has_large) { ctx->missing_thumbs++; LOG_WARN("thumbdb_verify_records: record %s missing both thumbnails (media: %s)", key, media_path); }
    else ctx->valid_records++;
    return 0;
}

int thumbdb_verify_records(thumbdb_instance_t* inst) {
    if (!inst || !inst->rh_tbl || !inst->db_inited) return -1;
    thread_mutex_lock(&inst->mutex);
    verify_records_ctx_t ctx = {0};
    if (!inst->db_path[0]) { thread_mutex_unlock(&inst->mutex); return -1; }
    strncpy(ctx.thumbs_root, inst->db_path, sizeof(ctx.thumbs_root) - 1); ctx.thumbs_root[sizeof(ctx.thumbs_root) - 1] = '\0';
    char* last = strrchr(ctx.thumbs_root, DIR_SEP); if (last) *last = '\0';
    if (!is_dir(ctx.thumbs_root)) { thread_mutex_unlock(&inst->mutex); return -1; }
    rh_iterate(inst->rh_tbl, verify_record_cb, &ctx);
    thread_mutex_unlock(&inst->mutex);
    LOG_INFO("thumbdb_verify_records: total_records=%d valid=%d missing_thumbs=%d", ctx.total_records, ctx.valid_records, ctx.missing_thumbs);
    return ctx.missing_thumbs > 0 ? 1 : 0;
}

int thumbdb_verify_thumbnails(thumbdb_instance_t* inst) {
    if (!inst || !inst->rh_tbl || !inst->db_inited) return -1;
    thread_mutex_lock(&inst->mutex);
    char per_thumbs_root[PATH_MAX]; per_thumbs_root[0] = '\0';
    if (!inst->db_path[0]) { thread_mutex_unlock(&inst->mutex); return -1; }
    strncpy(per_thumbs_root, inst->db_path, sizeof(per_thumbs_root) - 1); per_thumbs_root[sizeof(per_thumbs_root) - 1] = '\0';
    char* last = strrchr(per_thumbs_root, DIR_SEP); if (last) *last = '\0';
    if (!is_dir(per_thumbs_root)) { thread_mutex_unlock(&inst->mutex); return -1; }
    int total_thumbs = 0, orphaned_thumbs = 0, valid_thumbs = 0;
    diriter it; if (!dir_open(&it, per_thumbs_root)) { thread_mutex_unlock(&inst->mutex); return -1; }
    const char* entry;
    while ((entry = dir_next(&it))) {
        if (!entry || strcmp(entry, ".") == 0 || strcmp(entry, "..") == 0) continue;
        if (strcmp(entry, "wal") == 0 || strcmp(entry, "thumbs.tdb") == 0 || strcmp(entry, "thumbs.tdb.tmp") == 0 || strcmp(entry, "thumbs.tdb.bak") == 0 || strcmp(entry, "thumbs.db") == 0 || strcmp(entry, "thumbs.db.tmp") == 0 || strcmp(entry, "thumbs.db.bak") == 0) continue;
        const char* ext = strrchr(entry, '.'); int is_jpeg_thumb = ext && is_jpeg_extension(ext); int is_webp_thumb = ext && ascii_stricmp(ext, ".webp") == 0;
        int has_marker = strstr(entry, "-small.") || strstr(entry, "-large.");
        if (!has_marker || (!is_jpeg_thumb && !is_webp_thumb)) continue;
        total_thumbs++;
        char base_key[PATH_MAX]; base_key[0] = '\0';
        const char* small_marker = strstr(entry, "-small."); const char* large_marker = strstr(entry, "-large.");
        if (small_marker) { size_t base_len = (size_t)(small_marker - entry); if (base_len >= sizeof(base_key)) base_len = sizeof(base_key) - 1; memcpy(base_key, entry, base_len); base_key[base_len] = '\0'; }
        else if (large_marker) { size_t base_len = (size_t)(large_marker - entry); if (base_len >= sizeof(base_key)) base_len = sizeof(base_key) - 1; memcpy(base_key, entry, base_len); base_key[base_len] = '\0'; }
        if (!base_key[0]) continue;
        unsigned char* val = NULL; size_t val_len = 0;
        int found = rh_find(inst->rh_tbl, base_key, strlen(base_key), &val, &val_len);
        if (found != 0 || !val || val_len == 0) { orphaned_thumbs++; LOG_WARN("thumbdb_verify_thumbnails: orphaned thumbnail %s", entry); }
        else valid_thumbs++;
    }
    dir_close(&it);
    thread_mutex_unlock(&inst->mutex);
    LOG_INFO("thumbdb_verify_thumbnails: total_thumbs=%d valid=%d orphaned=%d", total_thumbs, valid_thumbs, orphaned_thumbs);
    return orphaned_thumbs > 0 ? 1 : 0;
}

int thumbdb_seek_to_record(thumbdb_instance_t* inst, const char* filename, char* buf, size_t buflen) {
    if (!inst || !inst->rh_tbl || !inst->db_inited || !filename || !buf) return -1;
    if (inst->index_table.count == 0 || !inst->file_header.has_index) return thumbdb_get(inst, filename, buf, buflen);
    thread_mutex_lock(&inst->mutex);
    uint64_t target_hash = hash_filename(filename);
    size_t left = 0, right = inst->index_table.count; int found_idx = -1;
    while (left < right) {
        size_t mid = left + (right - left) / 2;
        if (inst->index_table.entries[mid].filename_hash == target_hash) { found_idx = (int)mid; break; }
        else if (inst->index_table.entries[mid].filename_hash < target_hash) left = mid + 1;
        else right = mid;
    }
    if (found_idx < 0) { thread_mutex_unlock(&inst->mutex); return -1; }
    uint64_t file_offset = inst->index_table.entries[found_idx].file_offset;
    FILE* f = platform_fopen(inst->db_path, "rb");
    if (!f) { thread_mutex_unlock(&inst->mutex); return -1; }
    if (fseek(f, (long)file_offset, SEEK_SET) != 0) { fclose(f); thread_mutex_unlock(&inst->mutex); return -1; }
    record_t rec = {0};
    if (read_record(inst, f, &rec, 0) != 0) { fclose(f); thread_mutex_unlock(&inst->mutex); return -1; }
    fclose(f);
    if (strcmp(rec.filename, filename) != 0) { free_record(&rec); thread_mutex_unlock(&inst->mutex); return -1; }
    if (serialize_record_to_value(inst, &rec, buf, buflen) != 0) { free_record(&rec); thread_mutex_unlock(&inst->mutex); return -1; }
    free_record(&rec);
    thread_mutex_unlock(&inst->mutex);
    return 0;
}

int thumbdb_start_async_worker(thumbdb_instance_t* inst) {
    if (!inst || inst->async_worker_running) return 0;
    inst->async_worker_running = 1;
    if (thread_create_detached((void* (*)(void*))async_worker_thread, inst) != 0) { inst->async_worker_running = 0; return -1; }
    LOG_INFO("thumbdb: started async worker thread");
    return 0;
}

void thumbdb_stop_async_worker(thumbdb_instance_t* inst) {
    if (inst) inst->async_worker_running = 0;
}

int thumbdb_set_async(thumbdb_instance_t* inst, const char* key, const char* value) {
    if (!inst || !key || !value) return -1;
    int ret = write_wal_entry(inst, key, value);
    if (ret == 0) ret = enqueue_async_op(inst, key, value, 0);
    return ret;
}

int thumbdb_delete_async(thumbdb_instance_t* inst, const char* key) {
    if (!inst || !key) return -1;
    int ret = write_wal_entry(inst, key, NULL);
    if (ret == 0) ret = enqueue_async_op(inst, key, NULL, 1);
    return ret;
}
