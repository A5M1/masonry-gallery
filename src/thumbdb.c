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

static rh_table_t* rh_tbl = NULL;
static dir_table_t dir_table = {NULL, 0, 0};
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
    byte |= (meta.thumb_mode & MV_BITMASKS.meta_thumb_mode_mask) << 2;
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
        uint8_t hash_mode_byte = (uint8_t)(rec->meta.hash_mode & MV_BITMASKS.meta_thumb_mode_mask);
        record_buf[buf_pos++] = hash_mode_byte;
    }
    
    if (rec->meta.has_extensions) {
        size_t ext_start = buf_pos;
        buf_pos++;
        
        if (rec->width > 0 && rec->height > 0) {
            record_buf[buf_pos++] = MV_EXT_TAGS.dimensions_delta;
            record_buf[buf_pos++] = 0;
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
            memcpy(record_buf + buf_pos, &lat_f, 4);
            buf_pos += 4;
            memcpy(record_buf + buf_pos, &lon_f, 4);
            buf_pos += 4;
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
    
    if (rec->width > 0 && rec->height > 0) {
        uint8_t w_buf[10], h_buf[10];
        size_t w_len = 0, h_len = 0;
        uint64_t w = rec->width, h = rec->height;
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
        for (size_t i = 0; i < w_len; i++)
            record_buf[buf_pos++] = w_buf[i];
        for (size_t i = 0; i < h_len; i++)
            record_buf[buf_pos++] = h_buf[i];
    }
    
    if (rec->duration > 0) {
        uint8_t d_buf[10];
        size_t d_len = 0;
        uint64_t d = rec->duration;
        while (d >= MV_BITMASKS.varint_continue_bit) {
            d_buf[d_len++] = (uint8_t)((d & MV_BITMASKS.varint_data_mask) | MV_BITMASKS.varint_continue_bit);
            d >>= 7;
        }
        d_buf[d_len++] = (uint8_t)(d & MV_BITMASKS.varint_data_mask);
        for (size_t i = 0; i < d_len; i++)
            record_buf[buf_pos++] = d_buf[i];
    }
    
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
        rec->meta.hash_mode = (hash_mode_t)(hash_mode_byte & MV_BITMASKS.meta_thumb_mode_mask);
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
            if (read_varint(f, &value_len) != 0) {
                free(rec->filename);
                free(rec->dir_indexes);
                return -1;
            }
            
            if (tag == MV_EXT_TAGS.dimensions_delta) {
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
                if (fread(&lat_f, 4, 1, f) != 1 || fread(&lon_f, 4, 1, f) != 1) {
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
    
    if (rec->width > 0 && rec->height > 0) {
        uint64_t w, h;
        if (read_varint(f, &w) != 0 || read_varint(f, &h) != 0) {
            free(rec->filename);
            free(rec->dir_indexes);
            return -1;
        }
        rec->width = (uint32_t)w;
        rec->height = (uint32_t)h;
    }
    
    if (rec->duration > 0) {
        uint64_t dur;
        if (read_varint(f, &dur) != 0) {
            free(rec->filename);
            free(rec->dir_indexes);
            return -1;
        }
        rec->duration = (uint32_t)dur;
    }
    
    rec->hash_len = get_hash_length(rec->meta.hash_mode);
    if (rec->hash_len > 0) {
        if (fread(rec->hash, 1, rec->hash_len, f) != rec->hash_len) {
            free(rec->filename);
            free(rec->dir_indexes);
            return -1;
        }
    }
    
    uint32_t stored_crc;
    if (fread(&stored_crc, 4, 1, f) != 1) {
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
    uint16_t magic;
    if (fread(&magic, 2, 1, f) != 1 || magic != MV_CONSTANTS.db_magic) {
        LOG_ERROR("thumbdb: invalid magic header in %s", db_path);
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
    
    if (fread(&file_header.base_timestamp, 8, 1, f) != 1) {
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
    while (!feof(f)) {
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
            uint16_t magic = MV_CONSTANTS.db_magic;
            fwrite(&magic, 2, 1, f_new);
            uint8_t version = MV_CONSTANTS.version;
            fwrite(&version, 1, 1, f_new);
            uint8_t flags = MV_BITMASKS.flags_init;
            fwrite(&flags, 1, 1, f_new);
            uint64_t record_count = 0;
            write_varint(f_new, record_count);
            uint64_t base_timestamp = (uint64_t)time(NULL);
            fwrite(&base_timestamp, 8, 1, f_new);
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
    db_inited = 0;
    thread_mutex_unlock(&db_mutex);
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
    int ret = write_wal_entry(key, value);
    if (ret == 0) {
        ret = rh_insert(rh_tbl, key, strlen(key), (const unsigned char*)value, strlen(value) + 1);
    }
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
    int ret = write_wal_entry(key, NULL);
    if (ret == 0) {
        ret = rh_remove(rh_tbl, key, strlen(key));
    }
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

int thumbdb_compact(void) {
    if (!rh_tbl) return -1;
    thread_mutex_lock(&db_mutex);
    char tmp_path[PATH_MAX];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", db_path);
    FILE* f = platform_fopen(tmp_path, "wb");
    if (!f) {
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    uint16_t magic = MV_CONSTANTS.db_magic;
    fwrite(&magic, 2, 1, f);
    uint8_t version = MV_CONSTANTS.version;
    fwrite(&version, 1, 1, f);
    fwrite(&file_header.flags, 1, 1, f);
    uint64_t record_count = 0;
    write_varint(f, record_count);
    fwrite(&file_header.base_timestamp, 8, 1, f);
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
    int is_first = 1;
    fflush(f);
    platform_fsync(fileno(f));
    fclose(f);
    platform_file_delete(db_path);
    if (rename(tmp_path, db_path) != 0) {
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
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
