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

static rh_table_t* rh_tbl = NULL;
static dir_table_t dir_table = {NULL, 0, 0};
static index_table_t index_table = {NULL, 0, 0};
static thread_mutex_t db_mutex;
static int db_inited = 0;
static char db_path[PATH_MAX];
static int tx_active = 0;
static rh_table_t* tx_snapshot = NULL;
static file_header_t file_header;
static uint64_t current_record_seq = 0;

static thread_mutex_t db_open_mutex;
static int db_open_mutex_inited = 0;
static thread_mutex_t compaction_mutex;
static int compaction_mutex_inited = 0;
static int compaction_requested = 0;

static atomic_uint wal_chunk_seq = ATOMIC_VAR_INIT(0);
static thread_mutex_t wal_seq_mutex;
static int wal_seq_mutex_inited = 0;

static uint64_t last_filename_delta = 0;
static uint64_t last_timestamp_delta = 0;

typedef struct async_op {
    char key[PATH_MAX];
    char value[PATH_MAX];
    int is_delete;
    struct async_op* next;
} async_op_t;

static async_op_t* async_queue_head = NULL;
static async_op_t* async_queue_tail = NULL;
static thread_mutex_t async_queue_mutex;
static int async_queue_mutex_inited = 0;
static int async_worker_running = 0;

static int write_uint64_le(FILE* f, uint64_t value) {
    uint8_t buf[8];
    for (int i = 0; i < 8; i++) {
        buf[i] = (value >> (i * 8)) & 0xFF;
    }
    return fwrite(buf, 1, 8, f) == 8 ? 0 : -1;
}

static int read_uint64_le(FILE* f, uint64_t* out) {
    uint8_t buf[8];
    if (fread(buf, 1, 8, f) != 8) return -1;
    *out = 0;
    for (int i = 0; i < 8; i++) {
        *out |= ((uint64_t)buf[i]) << (i * 8);
    }
    return 0;
}

static int write_uint32_le(FILE* f, uint32_t value) {
    uint8_t buf[4];
    for (int i = 0; i < 4; i++) {
        buf[i] = (value >> (i * 8)) & 0xFF;
    }
    return fwrite(buf, 1, 4, f) == 4 ? 0 : -1;
}

static int read_uint32_le(FILE* f, uint32_t* out) {
    uint8_t buf[4];
    if (fread(buf, 1, 4, f) != 4) return -1;
    *out = 0;
    for (int i = 0; i < 4; i++) {
        *out |= ((uint32_t)buf[i]) << (i * 8);
    }
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
    while (tmp > 1) {
        bits_per_index++;
        tmp >>= 1;
    }
    uint8_t current_byte = 0;
    int bits_in_byte = 0;
    for (size_t i = 0; i < count; i++) {
        uint32_t idx = indexes[i];
        int bits_remaining = bits_per_index;
        while (bits_remaining > 0) {
            int bits_to_write = bits_remaining;
            if (bits_to_write > 8 - bits_in_byte) {
                bits_to_write = 8 - bits_in_byte;
            }
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
    if (count == 0) {
        *out_indexes = NULL;
        return 0;
    }
    int bits_per_index = 1;
    size_t tmp = table_size;
    while (tmp > 1) {
        bits_per_index++;
        tmp >>= 1;
    }
    uint32_t* indexes = calloc(count, sizeof(uint32_t));
    if (!indexes) return -1;
    uint8_t current_byte = 0;
    int bits_available = 0;
    for (size_t i = 0; i < count; i++) {
        uint32_t idx = 0;
        int bits_remaining = bits_per_index;
        while (bits_remaining > 0) {
            if (bits_available == 0) {
                if (fread(&current_byte, 1, 1, f) != 1) {
                    free(indexes);
                    return -1;
                }
                bits_available = 8;
            }
            int bits_to_read = bits_remaining;
            if (bits_to_read > bits_available) {
                bits_to_read = bits_available;
            }
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

static int add_dir_to_table(const char* dir) {
    for (size_t i = 0; i < dir_table.count; i++) {
        if (strcmp(dir_table.dirs[i], dir) == 0) return i;
    }
    if (dir_table.count >= dir_table.capacity) {
        size_t new_cap = dir_table.capacity == 0 ? 64 : dir_table.capacity * 2;
        char** new_dirs = realloc(dir_table.dirs, new_cap * sizeof(char*));
        if (!new_dirs) return -1;
        dir_table.dirs = new_dirs;
        dir_table.capacity = new_cap;
    }
    dir_table.dirs[dir_table.count] = strdup(dir);
    if (!dir_table.dirs[dir_table.count]) return -1;
    return dir_table.count++;
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

static int add_index_entry(uint64_t file_offset, const char* filename) {
    if (index_table.count >= index_table.capacity) {
        size_t new_cap = index_table.capacity == 0 ? 256 : index_table.capacity * 2;
        index_entry_t* new_entries = realloc(index_table.entries, new_cap * sizeof(index_entry_t));
        if (!new_entries) return -1;
        index_table.entries = new_entries;
        index_table.capacity = new_cap;
    }
    index_table.entries[index_table.count].file_offset = file_offset;
    index_table.entries[index_table.count].filename_hash = hash_filename(filename);
    index_table.count++;
    return 0;
}

static int compare_index_entries(const void* a, const void* b) {
    const index_entry_t* ea = (const index_entry_t*)a;
    const index_entry_t* eb = (const index_entry_t*)b;
    if (ea->filename_hash < eb->filename_hash) return -1;
    if (ea->filename_hash > eb->filename_hash) return 1;
    return 0;
}

static int write_index_block(FILE* f) {
    if (index_table.count == 0) return 0;
    qsort(index_table.entries, index_table.count, sizeof(index_entry_t), compare_index_entries);
    uint8_t magic_hi = (MV_CONSTANTS.index_magic >> 8) & 0xFF;
    uint8_t magic_lo = MV_CONSTANTS.index_magic & 0xFF;
    if (fwrite(&magic_hi, 1, 1, f) != 1) return -1;
    if (fwrite(&magic_lo, 1, 1, f) != 1) return -1;
    if (write_varint(f, index_table.count) != 0) return -1;
    for (size_t i = 0; i < index_table.count; i++) {
        if (write_varint(f, index_table.entries[i].file_offset) != 0) return -1;
        if (write_varint(f, index_table.entries[i].filename_hash) != 0) return -1;
    }
    return 0;
}

static int parse_path_into_dirs(const char* path, uint32_t** out_indexes, size_t* out_count) {
    char tmp[PATH_MAX];
    strncpy(tmp, path, sizeof(tmp) - 1);
    tmp[sizeof(tmp) - 1] = '\0';
    normalize_path(tmp);
    char* components[64];
    size_t comp_count = 0;
    char* token = strtok(tmp, "/\\");
    while (token && comp_count < 64) {
        components[comp_count++] = token;
        token = strtok(NULL, "/\\");
    }
    if (comp_count == 0) {
        *out_indexes = NULL;
        *out_count = 0;
        return 0;
    }
    uint32_t* indexes = calloc(comp_count, sizeof(uint32_t));
    if (!indexes) return -1;
    for (size_t i = 0; i < comp_count; i++) {
        int idx = add_dir_to_table(components[i]);
        if (idx < 0) {
            free(indexes);
            return -1;
        }
        indexes[i] = (uint32_t)idx;
    }
    *out_indexes = indexes;
    *out_count = comp_count;
    return 0;
}

static int reconstruct_path_from_indexes(uint32_t* indexes, size_t count, char* out, size_t outlen) {
    out[0] = '\0';
    for (size_t i = 0; i < count; i++) {
        if (indexes[i] >= dir_table.count) return -1;
        if (i > 0) strncat(out, DIR_SEP_STR, outlen - strlen(out) - 1);
        strncat(out, dir_table.dirs[indexes[i]], outlen - strlen(out) - 1);
    }
    return 0;
}

static int write_record(FILE* f, record_t* rec, int is_first) {
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
    for (size_t i = 0; i < seq_len; i++)
        record_buf[buf_pos++] = seq_buf[i];
    
    uint8_t dir_buf[10];
    size_t dir_len = 0;
    uint64_t dc = rec->dir_count;
    while (dc >= MV_BITMASKS.varint_continue_bit) {
        dir_buf[dir_len++] = (uint8_t)((dc & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit);
        dc >>= 7;
    }
    dir_buf[dir_len++] = (uint8_t)(dc & MV_BITMASKS.varint_data_mask);
    for (size_t i = 0; i < dir_len; i++)
        record_buf[buf_pos++] = dir_buf[i];
    
    size_t dir_table_size = dir_table.count > 0 ? dir_table.count : 1;
    int bits_per_index = 1;
    size_t tmp = dir_table_size;
    while (tmp > 1) {
        bits_per_index++;
        tmp >>= 1;
    }
    
    uint8_t current_byte = 0;
    int bits_in_byte = 0;
    for (size_t i = 0; i < rec->dir_count; i++) {
        uint32_t idx = rec->dir_indexes[i];
        int bits_remaining = bits_per_index;
        while (bits_remaining > 0) {
            int bits_to_write = bits_remaining;
            if (bits_to_write > 8 - bits_in_byte)
                bits_to_write = 8 - bits_in_byte;
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
    if (bits_in_byte > 0)
        record_buf[buf_pos++] = current_byte;
    
    uint64_t filename_val = strtoull(rec->filename, NULL, 10);
    uint64_t filename_delta = is_first ? filename_val : (filename_val - last_filename_delta);
    uint8_t fname_buf[10];
    size_t fname_len = 0;
    uint64_t fd = filename_delta;
    while (fd >= MV_BITMASKS.varint_continue_bit) {
        fname_buf[fname_len++] = (uint8_t)((fd & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit);
        fd >>= 7;
    }
    fname_buf[fname_len++] = (uint8_t)(fd & MV_BITMASKS.varint_data_mask);
    for (size_t i = 0; i < fname_len; i++)
        record_buf[buf_pos++] = fname_buf[i];
    last_filename_delta = filename_val;
    
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
            while (w >= MV_BITMASKS.varint_continue_bit) {
                w_buf[w_len++] = (uint8_t)((w & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit);
                w >>= 7;
            }
            w_buf[w_len++] = (uint8_t)(w & MV_BITMASKS.varint_data_mask);
            while (h >= MV_BITMASKS.varint_continue_bit) {
                h_buf[h_len++] = (uint8_t)((h & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit);
                h >>= 7;
            }
            h_buf[h_len++] = (uint8_t)(h & MV_BITMASKS.varint_data_mask);
            size_t total_dim_len = w_len + h_len;
            uint8_t len_buf[10];
            size_t len_len = 0;
            uint64_t tl = total_dim_len;
            while (tl >= MV_BITMASKS.varint_continue_bit) {
                len_buf[len_len++] = (uint8_t)((tl & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit);
                tl >>= 7;
            }
            len_buf[len_len++] = (uint8_t)(tl & MV_BITMASKS.varint_data_mask);
            for (size_t i = 0; i < len_len; i++)
                record_buf[buf_pos++] = len_buf[i];
            for (size_t i = 0; i < w_len; i++)
                record_buf[buf_pos++] = w_buf[i];
            for (size_t i = 0; i < h_len; i++)
                record_buf[buf_pos++] = h_buf[i];
        }
        
        if (rec->orientation > 0) {
            record_buf[buf_pos++] = MV_EXT_TAGS.orientation;
            record_buf[buf_pos++] = 1;
            record_buf[buf_pos++] = rec->orientation;
        }
        
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
            float lat_f = (float)rec->gps_lat;
            float lon_f = (float)rec->gps_lon;
            uint32_t lat_bits, lon_bits;
            memcpy(&lat_bits, &lat_f, 4);
            memcpy(&lon_bits, &lon_f, 4);
            record_buf[buf_pos++] = (lat_bits >> 0) & 0xFF;
            record_buf[buf_pos++] = (lat_bits >> 8) & 0xFF;
            record_buf[buf_pos++] = (lat_bits >> 16) & 0xFF;
            record_buf[buf_pos++] = (lat_bits >> 24) & 0xFF;
            record_buf[buf_pos++] = (lon_bits >> 0) & 0xFF;
            record_buf[buf_pos++] = (lon_bits >> 8) & 0xFF;
            record_buf[buf_pos++] = (lon_bits >> 16) & 0xFF;
            record_buf[buf_pos++] = (lon_bits >> 24) & 0xFF;
        }
        
        uint8_t ext_len = (uint8_t)(buf_pos - ext_start - 1);
        record_buf[ext_start] = ext_len;
    }
    
    uint64_t timestamp_delta = is_first ? rec->timestamp : (rec->timestamp - last_timestamp_delta);
    uint8_t ts_buf[10];
    size_t ts_len = 0;
    uint64_t td = timestamp_delta;
    while (td >= MV_BITMASKS.varint_continue_bit) {
        ts_buf[ts_len++] = (uint8_t)((td & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit);
        td >>= 7;
    }
    ts_buf[ts_len++] = (uint8_t)(td & MV_BITMASKS.varint_data_mask);
    for (size_t i = 0; i < ts_len; i++)
        record_buf[buf_pos++] = ts_buf[i];
    last_timestamp_delta = rec->timestamp;
    
    
    for (size_t i = 0; i < rec->hash_len; i++)
        record_buf[buf_pos++] = rec->hash[i];
    
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

static int read_record(FILE* f, record_t* rec, int is_first) {
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
    
    if (read_bit_packed_indexes(f, &rec->dir_indexes, rec->dir_count, dir_table.count > 0 ? dir_table.count : 1) != 0)
        return -1;
    
    uint64_t filename_delta;
    if (read_varint(f, &filename_delta) != 0) {
        free(rec->dir_indexes);
        return -1;
    }
    
    uint64_t filename_val = is_first ? filename_delta : (last_filename_delta + filename_delta);
    last_filename_delta = filename_val;
    rec->filename = malloc(32);
    if (!rec->filename) {
        free(rec->dir_indexes);
        return -1;
    }
    snprintf(rec->filename, 32, "%llu", (unsigned long long)filename_val);
    
    uint8_t meta;
    if (fread(&meta, 1, 1, f) != 1) {
        free(rec->filename);
        free(rec->dir_indexes);
        return -1;
    }
    rec->meta = decode_meta_byte(meta);
    
    if (rec->meta.hash_override) {
        uint8_t hash_mode_byte;
        if (fread(&hash_mode_byte, 1, 1, f) != 1) {
            free(rec->filename);
            free(rec->dir_indexes);
            return -1;
        }
        rec->meta.hash_mode = (hash_mode_t)(hash_mode_byte & 3);
    } else {
        rec->meta.hash_mode = file_header.default_hash_mode;
    }
    
    if (rec->meta.has_extensions) {
        uint64_t ext_len;
        if (read_varint(f, &ext_len) != 0) {
            free(rec->filename);
            free(rec->dir_indexes);
            return -1;
        }
        size_t ext_bytes_read = 0;
        while (ext_bytes_read < ext_len) {
            uint8_t tag;
            if (fread(&tag, 1, 1, f) != 1) {
                free(rec->filename);
                free(rec->dir_indexes);
                return -1;
            }
            ext_bytes_read++;
            
            uint64_t value_len;
            size_t varint_size;
            if (read_varint_with_size(f, &value_len, &varint_size) != 0) {
                free(rec->filename);
                free(rec->dir_indexes);
                return -1;
            }
            ext_bytes_read += varint_size;
            
            if (tag == MV_EXT_TAGS.dimensions_delta) {
                uint64_t w, h;
                size_t w_size, h_size;
                if (read_varint_with_size(f, &w, &w_size) != 0 || read_varint_with_size(f, &h, &h_size) != 0) {
                    free(rec->filename);
                    free(rec->dir_indexes);
                    return -1;
                }
                rec->width = (uint32_t)w;
                rec->height = (uint32_t)h;
                ext_bytes_read += w_size + h_size;
            }
            else if (tag == MV_EXT_TAGS.orientation && value_len == 1) {
                if (fread(&rec->orientation, 1, 1, f) != 1) {
                    free(rec->filename);
                    free(rec->dir_indexes);
                    return -1;
                }
                ext_bytes_read++;
            }
            else if (tag == MV_EXT_TAGS.codec_info && value_len > 0 && value_len < 256) {
                rec->codec_info = malloc(value_len + 1);
                if (rec->codec_info) {
                    if (fread(rec->codec_info, 1, value_len, f) != value_len) {
                        free(rec->codec_info);
                        rec->codec_info = NULL;
                        free(rec->filename);
                        free(rec->dir_indexes);
                        return -1;
                    }
                    rec->codec_info[value_len] = '\0';
                    ext_bytes_read += value_len;
                }
            }
            else if (tag == MV_EXT_TAGS.gps_coords && value_len == 8) {
                float lat_f, lon_f;
                if (read_float_le(f, &lat_f) != 0 || read_float_le(f, &lon_f) != 0) {
                    free(rec->filename);
                    free(rec->dir_indexes);
                    return -1;
                }
                rec->gps_lat = (double)lat_f;
                rec->gps_lon = (double)lon_f;
                ext_bytes_read += 8;
            }
            else {
                for (uint64_t i = 0; i < value_len; i++) {
                    uint8_t dummy;
                    if (fread(&dummy, 1, 1, f) != 1) {
                        free(rec->filename);
                        free(rec->dir_indexes);
                        return -1;
                    }
                    ext_bytes_read++;
                }
            }
        }
    }
    
    uint64_t timestamp_delta;
    if (read_varint(f, &timestamp_delta) != 0) {
        free(rec->filename);
        free(rec->dir_indexes);
        return -1;
    }
    rec->timestamp = is_first ? timestamp_delta : (last_timestamp_delta + timestamp_delta);
    last_timestamp_delta = rec->timestamp;
    
    
    
    rec->hash_len = get_hash_length(rec->meta.hash_mode);
    if (rec->hash_len > 0) {
        if (fread(rec->hash, 1, rec->hash_len, f) != rec->hash_len) {
            free(rec->filename);
            free(rec->dir_indexes);
            return -1;
        }
    }
    
    uint32_t stored_crc;
    if (read_uint32_le(f, &stored_crc) != 0) {
        free(rec->filename);
        free(rec->dir_indexes);
        return -1;
    }
    
    uint8_t end;
    if (fread(&end, 1, 1, f) != 1) {
        free(rec->filename);
        free(rec->dir_indexes);
        return -1;
    }
    if (end != MV_OPCODES.end) {
        free(rec->filename);
        free(rec->dir_indexes);
        return -1;
    }
    
    long end_pos = ftell(f);
    fseek(f, start_pos, SEEK_SET);
    size_t record_size = (size_t)(end_pos - start_pos - 5);
    if (record_size > sizeof(record_buf)) {
        free(rec->filename);
        free(rec->dir_indexes);
        return -1;
    }
    if (fread(record_buf, 1, record_size, f) != record_size) {
        free(rec->filename);
        free(rec->dir_indexes);
        return -1;
    }
    fseek(f, end_pos, SEEK_SET);
    
    uint32_t calc_crc;
    crypto_crc32(record_buf, record_size, &calc_crc);
    if (calc_crc != stored_crc) {
        LOG_WARN("CRC32 mismatch for record sequence %llu", (unsigned long long)record_seq);
    }
    
    return 0;
}

static void free_record(record_t* rec) {
    if (!rec) return;
    free(rec->filename);
    free(rec->dir_indexes);
    free(rec->codec_info);
}

static int extract_exif_metadata(const char* media_path, record_t* rec) {
    if (!is_exif_extraction_enabled()) return 0;
    const char* exiftool = get_exiftool_path();
    if (!exiftool) return 0;
    
    char esc_media[PATH_MAX * 2];
    char esc_tool[PATH_MAX * 2];
    platform_escape_path_for_cmd(media_path, esc_media, sizeof(esc_media));
    platform_escape_path_for_cmd(exiftool, esc_tool, sizeof(esc_tool));
    
    char cmd[PATH_MAX * 4];
    snprintf(cmd, sizeof(cmd), "%s -s -s -s -ImageWidth -ImageHeight -Orientation -GPSLatitude -GPSLongitude -Duration %s", 
             esc_tool, esc_media);
    
    FILE* p = platform_popen_direct(cmd, "r");
    if (!p) return -1;
    
    char line[512];
    while (fgets(line, sizeof(line), p)) {
        line[strcspn(line, "\r\n")] = '\0';
        
        if (strstr(line, "ImageWidth") && rec->width == 0) {
            char* val = strchr(line, ':');
            if (val) rec->width = (uint32_t)atoi(val + 1);
        }
        else if (strstr(line, "ImageHeight") && rec->height == 0) {
            char* val = strchr(line, ':');
            if (val) rec->height = (uint32_t)atoi(val + 1);
        }
        else if (strstr(line, "Orientation") && rec->orientation == 0) {
            char* val = strchr(line, ':');
            if (val) {
                int orient = atoi(val + 1);
                if (orient >= 0 && orient <= 8) rec->orientation = (uint8_t)orient;
            }
        }
        else if (strstr(line, "GPSLatitude") && rec->gps_lat == 0.0) {
            char* val = strchr(line, ':');
            if (val) rec->gps_lat = atof(val + 1);
        }
        else if (strstr(line, "GPSLongitude") && rec->gps_lon == 0.0) {
            char* val = strchr(line, ':');
            if (val) rec->gps_lon = atof(val + 1);
        }
        else if (strstr(line, "Duration") && rec->duration == 0) {
            char* val = strchr(line, ':');
            if (val) {
                double dur_sec = atof(val + 1);
                rec->duration = (uint32_t)(dur_sec * 1000.0);
            }
        }
    }
    
    platform_pclose_direct(p);
    return 0;
}

typedef struct {
    int total_records;
    int missing_thumbs;
    int valid_records;
    char thumbs_root[PATH_MAX];
} verify_records_ctx_t;

static int verify_record_cb(const char* key, const unsigned char* value, size_t value_len, void* user_data) {
    verify_records_ctx_t* ctx = (verify_records_ctx_t*)user_data;
    ctx->total_records++;
    if (!value || value_len == 0) {
        LOG_WARN("thumbdb_verify_records: key %s has empty value", key);
        return 0;
    }
    const char* media_path = (const char*)value;
    if (!is_file(media_path)) {
        LOG_WARN("thumbdb_verify_records: media file does not exist: %s", media_path);
    }
    char small_thumb[PATH_MAX];
    char large_thumb[PATH_MAX];
    snprintf(small_thumb, sizeof(small_thumb), "%s" DIR_SEP_STR "%s-small.jpg", ctx->thumbs_root, key);
    snprintf(large_thumb, sizeof(large_thumb), "%s" DIR_SEP_STR "%s-large.jpg", ctx->thumbs_root, key);
    int has_small = is_file(small_thumb);
    int has_large = is_file(large_thumb);
    if (!has_small && !has_large) {
        ctx->missing_thumbs++;
        LOG_WARN("thumbdb_verify_records: record %s missing both thumbnails (media: %s)", key, media_path);
    } else {
        ctx->valid_records++;
    }
    return 0;
}

int thumbdb_verify_records(void) {
    if (!rh_tbl || !db_inited) return -1;
    thread_mutex_lock(&db_mutex);
    verify_records_ctx_t ctx = {0};
    if (!db_path || db_path[0] == '\0') {
        thread_mutex_unlock(&db_mutex);
        LOG_ERROR("thumbdb_verify_records: db_path not set");
        return -1;
    }
    strncpy(ctx.thumbs_root, db_path, sizeof(ctx.thumbs_root) - 1);
    ctx.thumbs_root[sizeof(ctx.thumbs_root) - 1] = '\0';
    char* last = strrchr(ctx.thumbs_root, DIR_SEP);
    if (last) *last = '\0';
    if (!is_dir(ctx.thumbs_root)) {
        thread_mutex_unlock(&db_mutex);
        LOG_ERROR("thumbdb_verify_records: thumbs directory %s does not exist", ctx.thumbs_root);
        return -1;
    }
    rh_iterate(rh_tbl, verify_record_cb, &ctx);
    thread_mutex_unlock(&db_mutex);
    LOG_INFO("thumbdb_verify_records: total_records=%d valid=%d missing_thumbs=%d", ctx.total_records, ctx.valid_records, ctx.missing_thumbs);
    return ctx.missing_thumbs > 0 ? 1 : 0;
}

static int serialize_record_to_value(record_t* rec, char* out, size_t outlen) {
    char path[PATH_MAX];
    if (reconstruct_path_from_indexes(rec->dir_indexes, rec->dir_count, path, sizeof(path)) != 0) return -1;
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

static void* async_worker_thread(void* arg) {
    (void)arg;
    while (async_worker_running) {
        async_op_t* op = NULL;
        if (!async_queue_mutex_inited) {
            platform_sleep_ms(100);
            continue;
        }
        thread_mutex_lock(&async_queue_mutex);
        if (async_queue_head) {
            op = async_queue_head;
            async_queue_head = op->next;
            if (!async_queue_head) async_queue_tail = NULL;
        }
        thread_mutex_unlock(&async_queue_mutex);
        if (op) {
            if (op->is_delete) {
                rh_remove(rh_tbl, op->key, strlen(op->key));
            } else {
                rh_insert(rh_tbl, op->key, strlen(op->key), (const unsigned char*)op->value, strlen(op->value) + 1);
            }
            free(op);
        } else {
            platform_sleep_ms(50);
        }
    }
    return NULL;
}

static int enqueue_async_op(const char* key, const char* value, int is_delete) {
    if (!async_queue_mutex_inited) {
        if (thread_mutex_init(&async_queue_mutex) == 0) async_queue_mutex_inited = 1;
    }
    if (!async_queue_mutex_inited) return -1;
    async_op_t* op = calloc(1, sizeof(async_op_t));
    if (!op) return -1;
    strncpy(op->key, key, sizeof(op->key) - 1);
    op->key[sizeof(op->key) - 1] = '\0';
    if (!is_delete && value) {
        strncpy(op->value, value, sizeof(op->value) - 1);
        op->value[sizeof(op->value) - 1] = '\0';
    }
    op->is_delete = is_delete;
    op->next = NULL;
    thread_mutex_lock(&async_queue_mutex);
    if (async_queue_tail) {
        async_queue_tail->next = op;
        async_queue_tail = op;
    } else {
        async_queue_head = async_queue_tail = op;
    }
    thread_mutex_unlock(&async_queue_mutex);
    return 0;
}

static int load_database(void) {
    FILE* f = platform_fopen(db_path, "rb");
    if (!f) return 0;
    uint8_t magic_hi, magic_lo;
    if (fread(&magic_hi, 1, 1, f) != 1 || fread(&magic_lo, 1, 1, f) != 1) {
        LOG_ERROR("thumbdb: cannot read magic header in %s", db_path);
        fclose(f);
        return -1;
    }
    uint16_t magic = ((uint16_t)magic_hi << 8) | magic_lo;
    if (magic != MV_CONSTANTS.db_magic) {
        LOG_ERROR("thumbdb: invalid magic header 0x%04X in %s", magic, db_path);
        fclose(f);
        return -1;
    }
    uint8_t version;
    if (fread(&version, 1, 1, f) != 1 || version != MV_CONSTANTS.version) {
        LOG_WARN("thumbdb: version mismatch in %s (expected 0x%02X, got 0x%02X)", db_path, MV_CONSTANTS.version, version);
    }
    uint8_t flags;
    if (fread(&flags, 1, 1, f) != 1) {
        fclose(f);
        return -1;
    }
    file_header.flags = flags;
    file_header.has_index = (flags >> 7) & 1;
    file_header.timestamp_precision = (flags >> 6) & 1;
    file_header.default_hash_mode = (hash_mode_t)((flags >> 4) & 3);
    
    uint64_t record_count;
    if (read_varint(f, &record_count) != 0) {
        fclose(f);
        return -1;
    }
    
    if (read_uint64_le(f, &file_header.base_timestamp) != 0) {
        fclose(f);
        return -1;
    }
    
    uint64_t dir_table_size;
    if (read_varint(f, &dir_table_size) != 0) {
        fclose(f);
        return -1;
    }
    
    for (uint64_t i = 0; i < dir_table_size; i++) {
        uint64_t prefix_len, suffix_len;
        if (read_varint(f, &prefix_len) != 0 || read_varint(f, &suffix_len) != 0) {
            fclose(f);
            return -1;
        }
        char suffix[PATH_MAX];
        if (suffix_len > 0) {
            if (fread(suffix, 1, suffix_len, f) != suffix_len) {
                fclose(f);
                return -1;
            }
            suffix[suffix_len] = '\0';
        } else {
            suffix[0] = '\0';
        }
        
        char full_path[PATH_MAX];
        if (prefix_len > 0 && dir_table.count > 0) {
            size_t copy_len = prefix_len;
            if (copy_len >= PATH_MAX) copy_len = PATH_MAX - 1;
            strncpy(full_path, dir_table.dirs[dir_table.count - 1], copy_len);
            full_path[copy_len] = '\0';
            strncat(full_path, suffix, PATH_MAX - strlen(full_path) - 1);
        } else {
            strncpy(full_path, suffix, PATH_MAX - 1);
            full_path[PATH_MAX - 1] = '\0';
        }
        add_dir_to_table(full_path);
    }
    
    last_filename_delta = 0;
    last_timestamp_delta = 0;
    current_record_seq = 0;
    int is_first = 1;
    long records_end_pos = 0;
    while (!feof(f)) {
        uint8_t peek;
        long pos = ftell(f);
        if (fread(&peek, 1, 1, f) != 1) break;
        fseek(f, pos, SEEK_SET);
        if (file_header.has_index && peek == ((MV_CONSTANTS.index_magic >> 8) & 0xFF)) {
            records_end_pos = pos;
            break;
        }
        record_t rec = {0};
        rec.record_sequence = current_record_seq++;
        if (read_record(f, &rec, is_first) != 0) {
            if (feof(f)) break;
            LOG_ERROR("Failed to read record from database");
            free_record(&rec);
            continue;
        }
        is_first = 0;
        char value[PATH_MAX];
        if (serialize_record_to_value(&rec, value, sizeof(value)) == 0) {
            char key[64];
            snprintf(key, sizeof(key), "%s", rec.filename);
            rh_insert(rh_tbl, key, strlen(key), (const unsigned char*)value, strlen(value) + 1);
        }
        free_record(&rec);
    }
    
    if (file_header.has_index && records_end_pos > 0) {
        fseek(f, records_end_pos, SEEK_SET);
        uint8_t idx_magic_hi, idx_magic_lo;
        if (fread(&idx_magic_hi, 1, 1, f) != 1 || fread(&idx_magic_lo, 1, 1, f) != 1) {
            LOG_WARN("thumbdb: cannot read index magic");
            fclose(f);
            return 0;
        }
        uint16_t idx_magic = ((uint16_t)idx_magic_hi << 8) | idx_magic_lo;
        if (idx_magic != MV_CONSTANTS.index_magic) {
            LOG_WARN("thumbdb: invalid index magic 0x%04X", idx_magic);
            fclose(f);
            return 0;
        }
        uint64_t index_count;
        if (read_varint(f, &index_count) != 0) {
            LOG_WARN("thumbdb: cannot read index count");
            fclose(f);
            return 0;
        }
        if (index_table.entries) {
            free(index_table.entries);
        }
        index_table.entries = calloc(index_count, sizeof(index_entry_t));
        if (!index_table.entries) {
            LOG_ERROR("thumbdb: out of memory for index");
            fclose(f);
            return 0;
        }
        index_table.count = index_count;
        index_table.capacity = index_count;
        for (uint64_t i = 0; i < index_count; i++) {
            if (read_varint(f, &index_table.entries[i].file_offset) != 0) {
                LOG_WARN("thumbdb: cannot read index entry %llu file_offset", (unsigned long long)i);
                break;
            }
            if (read_varint(f, &index_table.entries[i].filename_hash) != 0) {
                LOG_WARN("thumbdb: cannot read index entry %llu filename_hash", (unsigned long long)i);
                break;
            }
        }
        LOG_INFO("thumbdb: loaded index with %llu entries", (unsigned long long)index_count);
    }
    
    fclose(f);
    return 0;
}

static void build_wal_dir_from_dbpath(char* wal_dir_out, size_t out_len) {
    wal_dir_out[0] = '\0';
    if (!db_path || db_path[0] == '\0') return;
    char per_thumbs_root[PATH_MAX];
    strncpy(per_thumbs_root, db_path, sizeof(per_thumbs_root) - 1);
    per_thumbs_root[sizeof(per_thumbs_root) - 1] = '\0';
    char* last = strrchr(per_thumbs_root, DIR_SEP);
    if (!last) return;
    *last = '\0';
    snprintf(wal_dir_out, out_len, "%s" DIR_SEP_STR WAL_DIR_NAME, per_thumbs_root);
}

static int write_wal_entry(const char* key, const char* value) {
    if (!wal_seq_mutex_inited) {
        if (thread_mutex_init(&wal_seq_mutex) == 0) wal_seq_mutex_inited = 1;
    }
    char wal_dir[PATH_MAX];
    build_wal_dir_from_dbpath(wal_dir, sizeof(wal_dir));
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

static int process_wal_chunks(void) {
    char wal_dir[PATH_MAX];
    build_wal_dir_from_dbpath(wal_dir, sizeof(wal_dir));
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
            size_t key_len = strcspn(key, "\r\n");
            key[key_len] = '\0';
            if (fgets(value, sizeof(value), f)) {
                size_t val_len = strcspn(value, "\r\n");
                value[val_len] = '\0';
                if (strcmp(value, "__DELETE__") == 0) {
                    rh_remove(rh_tbl, key, strlen(key));
                } else {
                    rh_insert(rh_tbl, key, strlen(key), (const unsigned char*)value, strlen(value) + 1);
                }
            }
        }
        fclose(f);
        platform_file_delete(chunk_path);
    }
    dir_close(&it);
    return 0;
}

int thumbdb_open(void) {
    LOG_WARN("thumbdb_open: global DB disabled; use thumbdb_open_for_dir()");
    return -1;
}

int thumbdb_open_for_dir(const char* db_full_path) {
    if (!db_open_mutex_inited) {
        if (thread_mutex_init(&db_open_mutex) == 0) db_open_mutex_inited = 1;
    }
    thread_mutex_lock(&db_open_mutex);
    if (!db_inited) {
        if (thread_mutex_init(&db_mutex) != 0) {
            thread_mutex_unlock(&db_open_mutex);
            return -1;
        }
        db_inited = 1;
    }
    if (!db_full_path || db_full_path[0] == '\0') {
        thread_mutex_unlock(&db_open_mutex);
        return -1;
    }
    if (db_path[0] != '\0' && strcmp(db_path, db_full_path) == 0 && rh_tbl) {
        thread_mutex_unlock(&db_open_mutex);
        return 0;
    }
    strncpy(db_path, db_full_path, sizeof(db_path) - 1);
    db_path[sizeof(db_path) - 1] = '\0';
    if (rh_tbl) rh_destroy(rh_tbl);
    rh_tbl = rh_create(INITIAL_BUCKETS_BITS);
    if (!rh_tbl) {
        thread_mutex_unlock(&db_open_mutex);
        return -1;
    }
    FILE* f_check = platform_fopen(db_path, "rb");
    if (!f_check) {
        FILE* f_new = platform_fopen(db_path, "wb");
        if (f_new) {
            uint8_t magic_hi = (MV_CONSTANTS.db_magic >> 8) & 0xFF;
            uint8_t magic_lo = MV_CONSTANTS.db_magic & 0xFF;
            fwrite(&magic_hi, 1, 1, f_new);
            fwrite(&magic_lo, 1, 1, f_new);
            uint8_t version = MV_CONSTANTS.version;
            fwrite(&version, 1, 1, f_new);
            uint8_t flags = MV_BITMASKS.flags_init;
            fwrite(&flags, 1, 1, f_new);
            uint64_t record_count = 0;
            write_varint(f_new, record_count);
            uint64_t base_timestamp = (uint64_t)time(NULL);
            write_uint64_le(f_new, base_timestamp);
            file_header.base_timestamp = base_timestamp;
            uint64_t dir_table_size = 0;
            write_varint(f_new, dir_table_size);
            fflush(f_new);
            platform_fsync(fileno(f_new));
            fclose(f_new);
            LOG_INFO("thumbdb: created new database %s", db_path);
        }
    } else {
        fclose(f_check);
        load_database();
        process_wal_chunks();
        LOG_INFO("thumbdb: loaded database %s", db_path);
    }
    thread_mutex_unlock(&db_open_mutex);
    return 0;
}

int thumbdb_verify_thumbnails(void) {
    if (!rh_tbl || !db_inited) return -1;
    thread_mutex_lock(&db_mutex);
    char per_thumbs_root[PATH_MAX];
    per_thumbs_root[0] = '\0';
    if (!db_path || db_path[0] == '\0') {
        thread_mutex_unlock(&db_mutex);
        LOG_ERROR("thumbdb_verify_thumbnails: db_path not set");
        return -1;
    }
    strncpy(per_thumbs_root, db_path, sizeof(per_thumbs_root) - 1);
    per_thumbs_root[sizeof(per_thumbs_root) - 1] = '\0';
    char* last = strrchr(per_thumbs_root, DIR_SEP);
    if (last) *last = '\0';
    if (!is_dir(per_thumbs_root)) {
        thread_mutex_unlock(&db_mutex);
        LOG_ERROR("thumbdb_verify_thumbnails: thumbs directory %s does not exist", per_thumbs_root);
        return -1;
    }
    int total_thumbs = 0;
    int orphaned_thumbs = 0;
    int valid_thumbs = 0;
    diriter it;
    if (!dir_open(&it, per_thumbs_root)) {
        thread_mutex_unlock(&db_mutex);
        LOG_ERROR("thumbdb_verify_thumbnails: cannot open thumbs directory %s", per_thumbs_root);
        return -1;
    }
    const char* entry;
    while ((entry = dir_next(&it))) {
        if (!entry || strcmp(entry, ".") == 0 || strcmp(entry, "..") == 0) continue;
        if (strcmp(entry, "wal") == 0) continue;
        if (strcmp(entry, "thumbs.tdb") == 0) continue;
        if (strcmp(entry, "thumbs.tdb.tmp") == 0) continue;
        if (strcmp(entry, "thumbs.tdb.bak") == 0) continue;
        if (strcmp(entry, "thumbs.db") == 0) continue;
        if (strcmp(entry, "thumbs.db.tmp") == 0) continue;
        if (strcmp(entry, "thumbs.db.bak") == 0) continue;
        const char* ext = strrchr(entry, '.');
        int is_jpeg_thumb = ext && is_jpeg_extension(ext);
        int is_webp_thumb = ext && ascii_stricmp(ext, ".webp") == 0;
        int has_marker = strstr(entry, "-small.") || strstr(entry, "-large.");
        if (!has_marker || (!is_jpeg_thumb && !is_webp_thumb)) continue;
        total_thumbs++;
        char base_key[PATH_MAX];
        base_key[0] = '\0';
        const char* small_marker = strstr(entry, "-small.");
        const char* large_marker = strstr(entry, "-large.");
        if (small_marker) {
            size_t base_len = (size_t)(small_marker - entry);
            if (base_len >= sizeof(base_key)) base_len = sizeof(base_key) - 1;
            memcpy(base_key, entry, base_len);
            base_key[base_len] = '\0';
        } else if (large_marker) {
            size_t base_len = (size_t)(large_marker - entry);
            if (base_len >= sizeof(base_key)) base_len = sizeof(base_key) - 1;
            memcpy(base_key, entry, base_len);
            base_key[base_len] = '\0';
        }
        if (!base_key[0]) continue;
        unsigned char* val = NULL;
        size_t val_len = 0;
        int found = rh_find(rh_tbl, base_key, strlen(base_key), &val, &val_len);
        if (found != 0 || !val || val_len == 0) {
            orphaned_thumbs++;
            LOG_WARN("thumbdb_verify_thumbnails: orphaned thumbnail %s (no database record for key %s)", entry, base_key);
        } else {
            valid_thumbs++;
        }
    }
    dir_close(&it);
    thread_mutex_unlock(&db_mutex);
    LOG_INFO("thumbdb_verify_thumbnails: total_thumbs=%d valid=%d orphaned=%d", total_thumbs, valid_thumbs, orphaned_thumbs);
    return orphaned_thumbs > 0 ? 1 : 0;
}

void thumbdb_close(void) {
    thread_mutex_lock(&db_mutex);
    if (rh_tbl) {
        rh_destroy(rh_tbl);
        rh_tbl = NULL;
    }
    for (size_t i = 0; i < dir_table.count; i++) {
        free(dir_table.dirs[i]);
    }
    free(dir_table.dirs);
    dir_table.dirs = NULL;
    dir_table.count = 0;
    dir_table.capacity = 0;
    if (index_table.entries) {
        free(index_table.entries);
        index_table.entries = NULL;
    }
    index_table.count = 0;
    index_table.capacity = 0;
    db_inited = 0;
    thread_mutex_unlock(&db_mutex);
}

int thumbdb_seek_to_record(const char* filename, char* buf, size_t buflen) {
    if (!rh_tbl || !db_inited || !filename || !buf) return -1;
    if (index_table.count == 0 || !file_header.has_index) {
        return thumbdb_get(filename, buf, buflen);
    }
    thread_mutex_lock(&db_mutex);
    uint64_t target_hash = hash_filename(filename);
    size_t left = 0;
    size_t right = index_table.count;
    int found_idx = -1;
    while (left < right) {
        size_t mid = left + (right - left) / 2;
        if (index_table.entries[mid].filename_hash == target_hash) {
            found_idx = (int)mid;
            break;
        } else if (index_table.entries[mid].filename_hash < target_hash) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    if (found_idx < 0) {
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    uint64_t file_offset = index_table.entries[found_idx].file_offset;
    FILE* f = platform_fopen(db_path, "rb");
    if (!f) {
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    if (fseek(f, (long)file_offset, SEEK_SET) != 0) {
        fclose(f);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    record_t rec = {0};
    if (read_record(f, &rec, 0) != 0) {
        fclose(f);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    fclose(f);
    if (strcmp(rec.filename, filename) != 0) {
        free_record(&rec);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    if (serialize_record_to_value(&rec, buf, buflen) != 0) {
        free_record(&rec);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    free_record(&rec);
    thread_mutex_unlock(&db_mutex);
    return 0;
}

int thumbdb_tx_begin(void) {
    thread_mutex_lock(&db_mutex);
    if (tx_active) {
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    if (tx_snapshot) rh_destroy(tx_snapshot);
    tx_snapshot = rh_create(INITIAL_BUCKETS_BITS);
    if (!tx_snapshot) {
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    tx_active = 1;
    thread_mutex_unlock(&db_mutex);
    return 0;
}

int thumbdb_tx_abort(void) {
    thread_mutex_lock(&db_mutex);
    if (!tx_active) {
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    if (tx_snapshot) {
        rh_destroy(tx_snapshot);
        tx_snapshot = NULL;
    }
    tx_active = 0;
    thread_mutex_unlock(&db_mutex);
    return 0;
}

int thumbdb_tx_commit(void) {
    thread_mutex_lock(&db_mutex);
    if (!tx_active) {
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    if (tx_snapshot) {
        rh_destroy(tx_snapshot);
        tx_snapshot = NULL;
    }
    tx_active = 0;
    thread_mutex_unlock(&db_mutex);
    return 0;
}

int thumbdb_set(const char* key, const char* value) {
    if (!rh_tbl || !key || !value) return -1;
    thread_mutex_lock(&db_mutex);
    
    record_t rec = {0};
    rec.record_sequence = current_record_seq++;
    rec.filename = strdup(key);
    rec.timestamp = (uint64_t)time(NULL);
    rec.meta.type = get_media_type_from_path(value);
    rec.meta.animated = 0;
    rec.meta.thumb_mode = 0;
    rec.meta.hash_override = 0;
    rec.meta.has_extensions = 0;
    rec.meta.hash_mode = file_header.default_hash_mode;
    
    if (parse_path_into_dirs(value, &rec.dir_indexes, &rec.dir_count) != 0) {
        free(rec.filename);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    
    if (crypto_md5_file(value, rec.hash) != 0) {
        memset(rec.hash, 0, sizeof(rec.hash));
    }
    rec.hash_len = get_hash_length(rec.meta.hash_mode);
    
    FILE* f = platform_fopen(db_path, "ab");
    if (f) {
        int is_first = (rec.record_sequence == 0);
        write_record(f, &rec, is_first);
        fflush(f);
        platform_fsync(fileno(f));
        fclose(f);
    }
    
    int ret = rh_insert(rh_tbl, key, strlen(key), (const unsigned char*)value, strlen(value) + 1);
    
    free_record(&rec);
    thread_mutex_unlock(&db_mutex);
    return ret;
}

int thumbdb_get(const char* key, char* buf, size_t buflen) {
    if (!rh_tbl || !key || !buf || buflen == 0) return -1;
    thread_mutex_lock(&db_mutex);
    unsigned char* val = NULL;
    size_t val_len = 0;
    int ret = rh_find(rh_tbl, key, strlen(key), &val, &val_len);
    if (ret == 0 && val && val_len > 0) {
        strncpy(buf, (const char*)val, buflen - 1);
        buf[buflen - 1] = '\0';
        thread_mutex_unlock(&db_mutex);
        return 0;
    }
    thread_mutex_unlock(&db_mutex);
    return -1;
}

int thumbdb_delete(const char* key) {
    if (!rh_tbl || !key) return -1;
    thread_mutex_lock(&db_mutex);
    
    FILE* f = platform_fopen(db_path, "ab");
    if (f) {
        fputc(MV_OPCODES.delete_op, f);
        uint64_t filename_val = strtoull(key, NULL, 10);
        write_varint(f, filename_val);
        uint64_t ts = (uint64_t)time(NULL);
        write_varint(f, ts);
        fputc(MV_OPCODES.end, f);
        fflush(f);
        platform_fsync(fileno(f));
        fclose(f);
    }
    
    int ret = rh_remove(rh_tbl, key, strlen(key));
    thread_mutex_unlock(&db_mutex);
    return ret;
}

void thumbdb_iterate(void (*cb)(const char* key, const char* value, void* ctx), void* ctx) {
    if (!rh_tbl || !cb) return;
    thread_mutex_lock(&db_mutex);
    struct iter_ctx { void (*cb)(const char*, const char*, void*); void* user; } ic = {cb, ctx};
    rh_iterate(rh_tbl, NULL, &ic);
    thread_mutex_unlock(&db_mutex);
}

int thumbdb_find_for_media(const char* media_path, char* out_key, size_t out_key_len) {
    if (!rh_tbl || !media_path || !out_key || out_key_len == 0) return -1;
    thread_mutex_lock(&db_mutex);
    out_key[0] = '\0';
    thread_mutex_unlock(&db_mutex);
    return -1;
}

typedef struct {
    char* key;
    char* value;
} compact_entry_t;

static int collect_entry_cb(const char* key, const unsigned char* value, size_t value_len, void* user_data) {
    compact_entry_t** entries = (compact_entry_t**)((void**)user_data)[0];
    size_t* count = (size_t*)((void**)user_data)[1];
    size_t* capacity = (size_t*)((void**)user_data)[2];
    
    size_t key_len = strlen(key);
    
    if (*count >= *capacity) {
        size_t new_cap = (*capacity == 0) ? 64 : (*capacity * 2);
        compact_entry_t* new_entries = realloc(*entries, new_cap * sizeof(compact_entry_t));
        if (!new_entries) return -1;
        *entries = new_entries;
        *capacity = new_cap;
    }
    
    (*entries)[*count].key = malloc(key_len + 1);
    (*entries)[*count].value = malloc(value_len + 1);
    if ((*entries)[*count].key && (*entries)[*count].value) {
        memcpy((*entries)[*count].key, key, key_len);
        (*entries)[*count].key[key_len] = '\0';
        memcpy((*entries)[*count].value, value, value_len);
        (*entries)[*count].value[value_len] = '\0';
        (*count)++;
    } else {
        free((*entries)[*count].key);
        free((*entries)[*count].value);
        return -1;
    }
    
    return 0;
}

int thumbdb_compact(void) {
    if (!rh_tbl) return -1;
    thread_mutex_lock(&db_mutex);
    
    compact_entry_t* entries = NULL;
    size_t entry_count = 0;
    size_t entry_capacity = 0;
    void* user_data[3] = {&entries, &entry_count, &entry_capacity};
    rh_iterate(rh_tbl, collect_entry_cb, user_data);
    
    if (index_table.entries) {
        free(index_table.entries);
        index_table.entries = NULL;
    }
    index_table.count = 0;
    index_table.capacity = 0;
    
    char tmp_path[PATH_MAX];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", db_path);
    FILE* f = platform_fopen(tmp_path, "wb");
    if (!f) {
        for (size_t i = 0; i < entry_count; i++) {
            free(entries[i].key);
            free(entries[i].value);
        }
        free(entries);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    
    uint8_t magic_hi = (MV_CONSTANTS.db_magic >> 8) & 0xFF;
    uint8_t magic_lo = MV_CONSTANTS.db_magic & 0xFF;
    fwrite(&magic_hi, 1, 1, f);
    fwrite(&magic_lo, 1, 1, f);
    uint8_t version = MV_CONSTANTS.version;
    fwrite(&version, 1, 1, f);
    uint8_t flags = file_header.flags | 0x80;
    fwrite(&flags, 1, 1, f);
    write_varint(f, (uint64_t)entry_count);
    write_uint64_le(f, file_header.base_timestamp);
    write_varint(f, (uint64_t)dir_table.count);
    
    for (size_t i = 0; i < dir_table.count; i++) {
        size_t prefix_len = 0;
        if (i > 0) {
            const char* prev = dir_table.dirs[i - 1];
            const char* curr = dir_table.dirs[i];
            while (prev[prefix_len] && curr[prefix_len] && prev[prefix_len] == curr[prefix_len])
                prefix_len++;
        }
        size_t suffix_len = strlen(dir_table.dirs[i]) - prefix_len;
        write_varint(f, prefix_len);
        write_varint(f, suffix_len);
        if (suffix_len > 0)
            fwrite(dir_table.dirs[i] + prefix_len, 1, suffix_len, f);
    }
    
    last_filename_delta = 0;
    last_timestamp_delta = 0;
    current_record_seq = 0;
    
    for (size_t i = 0; i < entry_count; i++) {
        long record_offset = ftell(f);
        record_t rec = {0};
        rec.record_sequence = current_record_seq++;
        rec.filename = entries[i].key;
        rec.timestamp = (uint64_t)time(NULL);
        rec.meta.type = get_media_type_from_path(entries[i].value);
        rec.meta.animated = 0;
        rec.meta.thumb_mode = 0;
        rec.meta.hash_override = 0;
        rec.meta.has_extensions = 0;
        rec.meta.hash_mode = file_header.default_hash_mode;
        
        if (parse_path_into_dirs(entries[i].value, &rec.dir_indexes, &rec.dir_count) == 0) {
            if (crypto_md5_file(entries[i].value, rec.hash) != 0) {
                memset(rec.hash, 0, sizeof(rec.hash));
            }
            rec.hash_len = get_hash_length(rec.meta.hash_mode);
            
            int is_first = (i == 0);
            if (write_record(f, &rec, is_first) == 0) {
                add_index_entry((uint64_t)record_offset, entries[i].key);
            }
            free(rec.dir_indexes);
        }
    }
    
    if (write_index_block(f) != 0) {
        LOG_WARN("thumbdb_compact: failed to write index block");
    } else {
        file_header.has_index = 1;
        LOG_INFO("thumbdb_compact: wrote index with %zu entries", index_table.count);
    }
    
    fflush(f);
    platform_fsync(fileno(f));
    fclose(f);
    
    for (size_t i = 0; i < entry_count; i++) {
        free(entries[i].key);
        free(entries[i].value);
    }
    free(entries);
    
    platform_file_delete(db_path);
    if (rename(tmp_path, db_path) != 0) {
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    
    last_filename_delta = 0;
    last_timestamp_delta = 0;
    current_record_seq = 0;
    
    thread_mutex_unlock(&db_mutex);
    return 0;
}

int thumbdb_sweep_orphans(void) {
    if (!rh_tbl) return -1;
    thread_mutex_lock(&db_mutex);
    char per_thumbs_root[PATH_MAX];
    per_thumbs_root[0] = '\0';
    if (!db_path || db_path[0] == '\0') {
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    strncpy(per_thumbs_root, db_path, sizeof(per_thumbs_root) - 1);
    per_thumbs_root[sizeof(per_thumbs_root) - 1] = '\0';
    char* last = strrchr(per_thumbs_root, DIR_SEP);
    if (last) *last = '\0';
    if (!is_dir(per_thumbs_root)) {
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    int deleted_count = 0;
    diriter it;
    if (!dir_open(&it, per_thumbs_root)) {
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    const char* entry;
    while ((entry = dir_next(&it))) {
        if (!entry || strcmp(entry, ".") == 0 || strcmp(entry, "..") == 0) continue;
        if (strcmp(entry, "wal") == 0) continue;
        if (strcmp(entry, "thumbs.tdb") == 0) continue;
        if (strcmp(entry, "thumbs.tdb.tmp") == 0) continue;
        if (strcmp(entry, "thumbs.tdb.bak") == 0) continue;
        if (strcmp(entry, "thumbs.db") == 0) continue;
        if (strcmp(entry, "thumbs.db.tmp") == 0) continue;
        if (strcmp(entry, "thumbs.db.bak") == 0) continue;
        const char* ext = strrchr(entry, '.');
        int is_jpeg_thumb = ext && is_jpeg_extension(ext);
        int is_webp_thumb = ext && ascii_stricmp(ext, ".webp") == 0;
        int has_marker = strstr(entry, "-small.") || strstr(entry, "-large.");
        if (!has_marker || (!is_jpeg_thumb && !is_webp_thumb)) continue;
        char base_key[PATH_MAX];
        base_key[0] = '\0';
        const char* small_marker = strstr(entry, "-small.");
        const char* large_marker = strstr(entry, "-large.");
        if (small_marker) {
            size_t base_len = (size_t)(small_marker - entry);
            if (base_len >= sizeof(base_key)) base_len = sizeof(base_key) - 1;
            memcpy(base_key, entry, base_len);
            base_key[base_len] = '\0';
        } else if (large_marker) {
            size_t base_len = (size_t)(large_marker - entry);
            if (base_len >= sizeof(base_key)) base_len = sizeof(base_key) - 1;
            memcpy(base_key, entry, base_len);
            base_key[base_len] = '\0';
        }
        if (!base_key[0]) continue;
        unsigned char* val = NULL;
        size_t val_len = 0;
        int found = rh_find(rh_tbl, base_key, strlen(base_key), &val, &val_len);
        if (found != 0 || !val || val_len == 0) {
            char thumb_path[PATH_MAX];
            snprintf(thumb_path, sizeof(thumb_path), "%s" DIR_SEP_STR "%s", per_thumbs_root, entry);
            if (is_file(thumb_path)) {
                if (platform_file_delete(thumb_path) == 0) {
                    deleted_count++;
                    LOG_INFO("thumbdb_sweep_orphans: deleted orphan thumbnail %s", thumb_path);
                } else {
                    LOG_WARN("thumbdb_sweep_orphans: failed to delete orphan thumbnail %s", thumb_path);
                }
            }
        }
    }
    dir_close(&it);
    thread_mutex_unlock(&db_mutex);
    LOG_INFO("thumbdb_sweep_orphans: deleted %d orphan thumbnails", deleted_count);
    return 0;
}

void thumbdb_request_compaction(void) {
    if (!compaction_mutex_inited) {
        if (thread_mutex_init(&compaction_mutex) == 0) compaction_mutex_inited = 1;
    }
    thread_mutex_lock(&compaction_mutex);
    compaction_requested = 1;
    thread_mutex_unlock(&compaction_mutex);
}

int thumbdb_perform_requested_compaction(void) {
    if (!compaction_mutex_inited) return 0;
    thread_mutex_lock(&compaction_mutex);
    if (!compaction_requested) {
        thread_mutex_unlock(&compaction_mutex);
        return 0;
    }
    compaction_requested = 0;
    thread_mutex_unlock(&compaction_mutex);
    return thumbdb_compact();
}

static void* repair_worker_thread(void* arg) {
    int* interval_ptr = (int*)arg;
    int interval = interval_ptr ? *interval_ptr : 3600;
    free(interval_ptr);
    while (db_inited) {
        for (int i = 0; i < interval && db_inited; i++) {
            platform_sleep_ms(1000);
        }
        if (!db_inited) break;
        LOG_INFO("thumbdb: running scheduled database repair");
        process_wal_chunks();
        thumbdb_perform_requested_compaction();
        thumbdb_sweep_orphans();
    }
    return NULL;
}

int thumbdb_start_repair_task(int interval_seconds) {
    if (!db_inited) return -1;
    int* interval_ptr = malloc(sizeof(int));
    if (!interval_ptr) return -1;
    *interval_ptr = interval_seconds > 0 ? interval_seconds : 3600;
    if (thread_create_detached((void* (*)(void*))repair_worker_thread, interval_ptr) != 0) {
        free(interval_ptr);
        return -1;
    }
    LOG_INFO("thumbdb: started repair task with %d second interval", *interval_ptr);
    return 0;
}

int thumbdb_validate(void) {
    if (!rh_tbl || !db_inited) return -1;
    thread_mutex_lock(&db_mutex);
    FILE* f = platform_fopen(db_path, "rb");
    if (!f) {
        thread_mutex_unlock(&db_mutex);
        LOG_ERROR("thumbdb_validate: cannot open %s", db_path);
        return -1;
    }
    uint8_t magic_hi, magic_lo;
    if (fread(&magic_hi, 1, 1, f) != 1 || fread(&magic_lo, 1, 1, f) != 1) {
        LOG_ERROR("thumbdb_validate: cannot read magic header");
        fclose(f);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    uint16_t magic = ((uint16_t)magic_hi << 8) | magic_lo;
    if (magic != MV_CONSTANTS.db_magic) {
        LOG_ERROR("thumbdb_validate: invalid magic 0x%04X (expected 0x%04X)", magic, MV_CONSTANTS.db_magic);
        fclose(f);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    uint8_t version;
    if (fread(&version, 1, 1, f) != 1) {
        LOG_ERROR("thumbdb_validate: cannot read version");
        fclose(f);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    if (version != MV_CONSTANTS.version) {
        LOG_WARN("thumbdb_validate: version mismatch 0x%02X (expected 0x%02X)", version, MV_CONSTANTS.version);
    }
    uint8_t flags;
    if (fread(&flags, 1, 1, f) != 1) {
        LOG_ERROR("thumbdb_validate: cannot read flags");
        fclose(f);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    uint64_t record_count;
    if (read_varint(f, &record_count) != 0) {
        LOG_ERROR("thumbdb_validate: invalid varint for record_count");
        fclose(f);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    uint64_t base_timestamp;
    if (read_uint64_le(f, &base_timestamp) != 0) {
        LOG_ERROR("thumbdb_validate: cannot read base_timestamp");
        fclose(f);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    uint64_t dir_table_size;
    if (read_varint(f, &dir_table_size) != 0) {
        LOG_ERROR("thumbdb_validate: invalid varint for dir_table_size");
        fclose(f);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    for (uint64_t i = 0; i < dir_table_size; i++) {
        uint64_t prefix_len, suffix_len;
        if (read_varint(f, &prefix_len) != 0 || read_varint(f, &suffix_len) != 0) {
            LOG_ERROR("thumbdb_validate: invalid varint in dir_table entry %llu", (unsigned long long)i);
            fclose(f);
            thread_mutex_unlock(&db_mutex);
            return -1;
        }
        if (suffix_len > 0) {
            fseek(f, (long)suffix_len, SEEK_CUR);
        }
    }
    uint64_t records_validated = 0;
    uint64_t crc_errors = 0;
    while (!feof(f)) {
        long record_start = ftell(f);
        uint8_t op;
        if (fread(&op, 1, 1, f) != 1) {
            if (feof(f)) break;
            LOG_ERROR("thumbdb_validate: cannot read opcode at offset %ld", record_start);
            break;
        }
        if (op == MV_OPCODES.delete_op) {
            uint64_t filename_ref;
            if (read_varint(f, &filename_ref) != 0) {
                LOG_ERROR("thumbdb_validate: invalid delete record at offset %ld", record_start);
                break;
            }
            records_validated++;
            continue;
        }
        if (op != MV_OPCODES.begin) {
            LOG_ERROR("thumbdb_validate: invalid opcode 0x%02X at offset %ld", op, record_start);
            break;
        }
        long crc_start = ftell(f);
        uint64_t seq, dir_count;
        if (read_varint(f, &seq) != 0 || read_varint(f, &dir_count) != 0) {
            LOG_ERROR("thumbdb_validate: invalid record header at offset %ld", record_start);
            break;
        }
        for (uint64_t i = 0; i < dir_count; i++) {
            uint64_t dir_idx;
            if (read_varint(f, &dir_idx) != 0) {
                LOG_ERROR("thumbdb_validate: invalid dir_index at offset %ld", ftell(f));
                break;
            }
        }
        uint64_t filename_delta;
        if (read_varint(f, &filename_delta) != 0) {
            LOG_ERROR("thumbdb_validate: invalid filename_delta at offset %ld", ftell(f));
            break;
        }
        uint8_t meta_byte;
        if (fread(&meta_byte, 1, 1, f) != 1) {
            LOG_ERROR("thumbdb_validate: cannot read meta_byte at offset %ld", ftell(f));
            break;
        }
        uint64_t ts_delta;
        if (read_varint(f, &ts_delta) != 0) {
            LOG_ERROR("thumbdb_validate: invalid timestamp_delta at offset %ld", ftell(f));
            break;
        }
        int hash_override = (meta_byte >> 3) & 1;
        hash_mode_t hash_mode = (hash_mode_t)((meta_byte >> 4) & 3);
        if (hash_override) {
            hash_mode_t file_hash_mode = (hash_mode_t)((flags >> 4) & 3);
            hash_mode = file_hash_mode;
        }
        size_t hash_len = 0;
        switch (hash_mode) {
            case HASH_FULL_MD5: hash_len = 16; break;
            case HASH_HALF_MD5: hash_len = 8; break;
            case HASH_RIPEMD128: hash_len = 16; break;
            case HASH_NONE: hash_len = 0; break;
        }
        if (hash_len > 0) {
            fseek(f, (long)hash_len, SEEK_CUR);
        }
        int has_extensions = (meta_byte >> 6) & 1;
        if (has_extensions) {
            uint64_t ext_count;
            if (read_varint(f, &ext_count) != 0) {
                LOG_ERROR("thumbdb_validate: invalid extension_count at offset %ld", ftell(f));
                break;
            }
            for (uint64_t i = 0; i < ext_count; i++) {
                uint8_t tag;
                uint64_t len;
                if (fread(&tag, 1, 1, f) != 1 || read_varint(f, &len) != 0) {
                    LOG_ERROR("thumbdb_validate: invalid extension at offset %ld", ftell(f));
                    break;
                }
                fseek(f, (long)len, SEEK_CUR);
            }
        }
        long crc_end = ftell(f);
        uint32_t stored_crc;
        if (read_uint32_le(f, &stored_crc) != 0) {
            LOG_ERROR("thumbdb_validate: cannot read CRC32 at offset %ld", ftell(f));
            break;
        }
        uint8_t end_op;
        if (fread(&end_op, 1, 1, f) != 1 || end_op != MV_OPCODES.end) {
            LOG_ERROR("thumbdb_validate: missing OP_END at offset %ld", ftell(f));
            break;
        }
        fseek(f, crc_start, SEEK_SET);
        size_t crc_data_len = (size_t)(crc_end - crc_start);
        uint8_t* crc_data = malloc(crc_data_len);
        if (!crc_data) {
            LOG_ERROR("thumbdb_validate: out of memory for CRC validation");
            break;
        }
        if (fread(crc_data, 1, crc_data_len, f) != crc_data_len) {
            LOG_ERROR("thumbdb_validate: cannot read record data for CRC validation");
            free(crc_data);
            break;
        }
        uint32_t computed_crc;
        crypto_crc32(crc_data, crc_data_len, &computed_crc);
        free(crc_data);
        if (stored_crc != computed_crc) {
            LOG_ERROR("thumbdb_validate: CRC32 mismatch at offset %ld (stored 0x%08X, computed 0x%08X)", record_start, stored_crc, computed_crc);
            crc_errors++;
        }
        fseek(f, crc_end + 4 + 1, SEEK_SET);
        records_validated++;
    }
    fclose(f);
    thread_mutex_unlock(&db_mutex);
    LOG_INFO("thumbdb_validate: validated %llu records, %llu CRC errors", (unsigned long long)records_validated, (unsigned long long)crc_errors);
    return crc_errors > 0 ? -1 : 0;
}

int thumbdb_start_async_worker(void) {
    if (async_worker_running) return 0;
    async_worker_running = 1;
    if (thread_create_detached((void* (*)(void*))async_worker_thread, NULL) != 0) {
        async_worker_running = 0;
        return -1;
    }
    LOG_INFO("thumbdb: started async worker thread");
    return 0;
}

void thumbdb_stop_async_worker(void) {
    async_worker_running = 0;
}

int thumbdb_set_async(const char* key, const char* value) {
    if (!key || !value) return -1;
    int ret = write_wal_entry(key, value);
    if (ret == 0) {
        ret = enqueue_async_op(key, value, 0);
    }
    return ret;
}

int thumbdb_delete_async(const char* key) {
    if (!key) return -1;
    int ret = write_wal_entry(key, NULL);
    if (ret == 0) {
        ret = enqueue_async_op(key, NULL, 1);
    }
    return ret;
}
