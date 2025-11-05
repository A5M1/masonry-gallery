#include "thumbdb.h"
#include "platform.h"
#include "logging.h"
#include "utils.h"
#include "directory.h"
#include "thread_pool.h"
#include "crypto.h"
#include "common.h"
#include "blaze.h"
#include "arena.h"
#include "config.h"
#include "thumbs.h"
#include "robinhood_hash.h"

#define DB_FILENAME "thumbs.db"
#define LINE_MAX 4096
#define INITIAL_BUCKETS 65536
#define DB_MAGIC "TNDB"
#define DB_MAGIC_LEN 4

// New binary format opcodes
const uint8_t OP_BEGIN_RECORD    = 0xAA;
const uint8_t OP_SEPERATOR       = 0x0A;
const uint8_t OP_MP4            = 0x0B;
const uint8_t OP_JPG            = 0x0C;
const uint8_t OP_WEBM           = 0x0D;
const uint8_t OP_GIF            = 0x0D;
const uint8_t OP_WEBP           = 0x0E;
const uint8_t OP_PNG            = 0x0F;
const uint8_t OP_END_RECORD     = 0xFF;
const uint8_t OP_BASE_DIR       = 0xBB;
const uint8_t OP_NEWLINE        = 0xFA;
const uint8_t OP_BACKSLASH      = 0xFB;
const uint8_t OP_LARGE_JPG      = 0xAD;
const uint8_t OP_SMALL_JPG      = 0xBE;

static const struct { const char* ext; uint8_t code; } ext_map[] = {
    {".jpg", OP_JPG}, {".jpeg", OP_JPG}, {".png", OP_PNG},
    {".gif", OP_GIF}, {".webp", OP_WEBP}, {".mp4", OP_MP4},
    {".webm", OP_WEBM}, {".ogg", OP_GIF}, {NULL, 0}
};

static unsigned char get_ext_opcode(const char* path) {
    const char* dot = strrchr(path, '.');
    if (!dot) return OP_JPG;  // Default to JPG
    for (int i = 0; ext_map[i].ext; i++) {
        if (ascii_stricmp(dot, ext_map[i].ext) == 0) return ext_map[i].code;
    }
    return OP_JPG;  // Default to JPG
}

static const char* get_ext_from_opcode(uint8_t code) {
    for (int i = 0; ext_map[i].ext; i++) {
        if (ext_map[i].code == code) return ext_map[i].ext;
    }
    return ".jpg";  // Default to JPG
}

static int is_valid_media_path(const char* path) {
    if (!path || !path[0]) return 0;
    const char* dot = strrchr(path, '.');
    if (!dot) return 0;
    for (int i = 0; ext_map[i].ext; i++) {
        if (ascii_stricmp(dot, ext_map[i].ext) == 0) return 1;
    }
    return 0;
}
typedef struct ht_entry {
    char* key;
    char* val;
    struct ht_entry* next;
} ht_entry_t;

typedef struct tx_op {
    char* key;
    char* val;
    struct tx_op* next;
} tx_op_t;
static void free_ht_table(ht_entry_t** table, size_t buckets);

static int rh_write_bin_cb(const char* key, const unsigned char* val, size_t val_len, void* ctx);

static ht_entry_t** ht = NULL;
static size_t ht_buckets = 0;
static rh_table_t* rh_tbl = NULL;
static thread_mutex_t db_mutex;
static int db_inited = 0;
static char db_path[PATH_MAX];
static int tx_active = 0;
static tx_op_t* tx_head = NULL;
static thread_mutex_t db_open_mutex;
static int db_open_mutex_inited = 0;

static time_t db_last_mtime = 0;
static off_t db_last_size = 0;
static int db_rebuilding = 0;
static int db_binary_mode = 0;

static uint32_t ht_hash(const char* s) {

    uint32_t h = 2166136261u;
    while (*s) { h ^= (unsigned char)*s++; h *= 16777619u; }
    return h;
}

static int append_line_to_file(FILE* f, const char* key, const char* val) {
    if (!f || !key) return -1;
    
    // Write binary encoded record using new opcode format
    // START RECORD | SEPERATOR | KEY | SEPERATOR | MEDIA PATH | END | NEWLINE
    
    if (fwrite(&OP_BEGIN_RECORD, 1, 1, f) != 1) return -1;
    if (fwrite(&OP_SEPERATOR, 1, 1, f) != 1) return -1;
    
    // Write key (thumbnail filename)
    size_t key_len = strlen(key);
    if (fwrite(key, 1, key_len, f) != key_len) return -1;
    if (fwrite(&OP_SEPERATOR, 1, 1, f) != 1) return -1;
    
    // Write media path
    if (val) {
        size_t val_len = strlen(val);
        if (fwrite(val, 1, val_len, f) != val_len) return -1;
    }
    
    if (fwrite(&OP_END_RECORD, 1, 1, f) != 1) return -1;
    if (fwrite(&OP_NEWLINE, 1, 1, f) != 1) return -1;
    
    return 0;
}

static void thumbname_to_base_and_kind(const char* name, char* base, size_t base_len, int* is_small, int* is_large) {
    base[0] = '\0'; if (is_small) *is_small = 0; if (is_large) *is_large = 0;
    if (!name) return;
    char* p = strstr(name, "-small.");
    if (p) {
        size_t bl = (size_t)(p - name); if (bl >= base_len) bl = base_len - 1; memcpy(base, name, bl); base[bl] = '\0'; if (is_small) *is_small = 1; return;
    }
    p = strstr(name, "-large.");
    if (p) {
        size_t bl = (size_t)(p - name); if (bl >= base_len) bl = base_len - 1; memcpy(base, name, bl); base[bl] = '\0'; if (is_large) *is_large = 1; return;
    }
    strncpy(base, name, base_len - 1); base[base_len - 1] = '\0';
}

static int find_thumb_filename_for_base_in_dir(const char* dir, const char* base, int want_small, char* out, size_t outlen) {
    diriter it;
    if (!dir_open(&it, dir)) return 0;
    const char* tname;
    size_t bl = strlen(base);
    while ((tname = dir_next(&it))) {
        if (!tname) continue;
        if (strncmp(tname, base, bl) != 0) continue;
        if (want_small) {
            if (strstr(tname, "-small.")) { strncpy(out, tname, outlen - 1); out[outlen - 1] = '\0'; dir_close(&it); return 1; }
        }
        else {
            if (strstr(tname, "-large.")) { strncpy(out, tname, outlen - 1); out[outlen - 1] = '\0'; dir_close(&it); return 1; }
        }
    }
    dir_close(&it);
    return 0;
}

static int find_thumb_filename_for_base(const char* base, int want_small, char* out, size_t outlen) {
    char thumb_dir[PATH_MAX]; thumb_dir[0] = '\0';
    if (db_path[0]) {
        strncpy(thumb_dir, db_path, sizeof(thumb_dir) - 1); thumb_dir[sizeof(thumb_dir) - 1] = '\0';
        char* last = strrchr(thumb_dir, DIR_SEP);
        if (last) *last = '\0'; else strncpy(thumb_dir, ".", sizeof(thumb_dir) - 1);
        if (find_thumb_filename_for_base_in_dir(thumb_dir, base, want_small, out, outlen)) return 1;
    }
    char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    if (find_thumb_filename_for_base_in_dir(thumbs_root, base, want_small, out, outlen)) return 1;
    diriter dit;
    if (dir_open(&dit, thumbs_root)) {
        const char* dname;
        while ((dname = dir_next(&dit))) {
            if (!strcmp(dname, ".") || !strcmp(dname, "..")) continue;
            char sub[PATH_MAX]; path_join(sub, thumbs_root, dname);
            if (!is_dir(sub)) continue;
            if (find_thumb_filename_for_base_in_dir(sub, base, want_small, out, outlen)) { dir_close(&dit); return 1; }
        }
        dir_close(&dit);
    }
    return 0;
}

static int ht_ensure(size_t buckets) {
    if (rh_tbl) return 0;
    size_t power = 16;
    if (buckets > 0) {
        size_t b = 1; power = 0;
        while (b < buckets) { b <<= 1; power++; }
        if (power == 0) power = 1;
    }
    rh_tbl = rh_create(power);
    if (!rh_tbl) return -1;
    ht_buckets = (size_t)1 << power;
    if (ht) { free_ht_table(ht, ht_buckets); ht = NULL; }
    return 0;
}

static const char* ht_get(const char* key) {
    if (!rh_tbl || !key) return NULL;
    unsigned char* v = NULL; size_t vlen = 0;
    if (rh_find(rh_tbl, key, strlen(key), &v, &vlen) == 0 && v && vlen > 0) return (const char*)v;
    return NULL;
}

static ht_entry_t* ht_find(const char* key) {
    static ht_entry_t tmp;
    tmp.key = (char*)key;
    tmp.val = NULL;
    if (!rh_tbl || !key) return NULL;
    unsigned char* v = NULL; size_t vlen = 0;
    if (rh_find(rh_tbl, key, strlen(key), &v, &vlen) == 0 && v && vlen > 0) { tmp.val = (char*)v; return &tmp; }
    return NULL;
}

static int ht_set_internal(const char* key, const char* val) {
    if (!rh_tbl || !key) return -1;
    if (!val) return rh_remove(rh_tbl, key, strlen(key));
    return rh_insert(rh_tbl, key, strlen(key), (const unsigned char*)val, strlen(val) + 1);
}

static void ht_free_all(void) {
    if (rh_tbl) { rh_destroy(rh_tbl); rh_tbl = NULL; ht_buckets = 0; }
    if (ht) { free_ht_table(ht, ht_buckets); ht = NULL; ht_buckets = 0; }
}

// Removed duplicate definition - using the one defined below

static int last_text_value_for_key(const char* path, const char* key, char* out, size_t out_len) {
    if (!path || !key || !out || out_len == 0) return -1;
    FILE* f = fopen(path, "rb");
    if (!f) return -1;
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return -1; }
    long pos = ftell(f);
    if (pos <= 0) { fclose(f); return -1; }
    size_t key_len = strlen(key);
    const size_t chunk = 8192;
    char* buf = malloc(chunk + key_len + 4);
    if (!buf) { fclose(f); return -1; }
    long read_pos = pos;
    out[0] = '\0';
    while (read_pos > 0) {
        size_t to_read = (read_pos >= (long)chunk) ? chunk : (size_t)read_pos;
        read_pos -= to_read;
        if (fseek(f, read_pos, SEEK_SET) != 0) break;
        size_t r = fread(buf, 1, to_read, f);
        if (r == 0) break;
        buf[r] = '\0';
        for (long i = (long)r - 1; i >= 0; --i) {
            if (buf[i] == '\n' || i == 0) {
                long line_start = (i == 0) ? read_pos : (read_pos + i + 1);
                if (fseek(f, line_start, SEEK_SET) != 0) continue;
                char line[LINE_MAX]; if (!fgets(line, sizeof(line), f)) continue;
                if (strncmp(line, key, key_len) == 0 && line[key_len] == ';') {
                    char* val = line + key_len + 1;
                    char* nl = strchr(val, '\n'); if (nl) *nl = '\0';
                    strncpy(out, val, out_len - 1); out[out_len - 1] = '\0'; free(buf); fclose(f); return 0;
                }
                if (read_pos == 0 && i == 0) break;
            }
        }
        if (read_pos == 0) break;
    }
    free(buf); fclose(f); return -1;
}

static int backup_file(const char* path) {
    if (!path) return -1;
    char bak[PATH_MAX]; snprintf(bak, sizeof(bak), "%s.bak", path);
    FILE* in = fopen(path, "rb");
    if (!in) return -1;
    FILE* out = fopen(bak, "wb");
    if (!out) { fclose(in); return -1; }
    unsigned char buf[8192]; size_t r;
    while ((r = fread(buf, 1, sizeof(buf), in)) > 0) {
        if (fwrite(buf, 1, r, out) != r) { fclose(in); fclose(out); return -1; }
    }
    fflush(out); platform_fsync(fileno(out)); fclose(in); fclose(out);
    return 0;
}

static int persist_tx_ops(tx_op_t* ops) {
    if (!ops) return 0;
    FILE* f = fopen(db_path, "ab");
    if (!f) return -1;
    tx_op_t* cur = ops;
    while (cur) {
        if (append_line_to_file(f, cur->key, cur->val) != 0) { fclose(f); return -1; }
        cur = cur->next;
    }
    fflush(f);
    platform_fsync(fileno(f));
    fclose(f);
    struct stat st; if (stat(db_path, &st) == 0) { db_last_mtime = st.st_mtime; db_last_size = st.st_size; }
    return 0;
}

int thumbdb_open(void) {
    LOG_WARN("thumbdb_open: opening global top-level DB is disabled; use thumbdb_open_for_dir() with a per-gallery path");
    return -1;
}

static int count_cb(const char* key, const unsigned char* val, size_t val_len, void* ctx) {
    (void)key; (void)val; (void)val_len;
    int* count = (int*)ctx;
    (*count)++;
    return 1; // Stop after first item
}

static int is_table_empty(void) {
    if (!rh_tbl) return 1;
    int count = 0;
    rh_iterate(rh_tbl, count_cb, &count);
    return count == 0;
}

static void populate_db_from_existing_thumbs(void) {
    size_t gf_count = 0;
    char** gfolders = get_gallery_folders(&gf_count);
    if (!gfolders || gf_count == 0) return;
    
    char thumbs_root[PATH_MAX];
    get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    
    for (size_t i = 0; i < gf_count; ++i) {
        char folder_real[PATH_MAX];
        if (!real_path(gfolders[i], folder_real)) continue;
        
        char safe_dir[PATH_MAX];
        make_safe_dir_name_from(folder_real, safe_dir, sizeof(safe_dir));
        
        char per_thumbs_root[PATH_MAX];
        snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir);
        
        if (!is_dir(per_thumbs_root)) continue;
        
        diriter tit;
        if (!dir_open(&tit, per_thumbs_root)) continue;
        
        const char* tname;
        while ((tname = dir_next(&tit))) {
            if (!tname) continue;
            if (!strstr(tname, "-small.") && !strstr(tname, "-large.")) continue;
            
            char base[PATH_MAX];
            int is_small = 0, is_large = 0;
            base[0] = '\0';
            thumbname_to_base_and_kind(tname, base, sizeof(base), &is_small, &is_large);
            if (!base[0]) continue;
            
            // Check if we already have this base in the database
            if (ht_get(base)) continue;
            
            // Find the corresponding media file
            diriter mit;
            if (!dir_open(&mit, folder_real)) continue;
            
            const char* mname;
            while ((mname = dir_next(&mit))) {
                if (!mname) continue;
                if (!has_ext(mname, IMAGE_EXTS) && !has_ext(mname, VIDEO_EXTS)) continue;
                
                char media_full[PATH_MAX];
                path_join(media_full, folder_real, mname);
                
                char small_rel[PATH_MAX], large_rel[PATH_MAX];
                small_rel[0] = large_rel[0] = '\0';
                get_thumb_rel_names(media_full, mname, small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));
                
                if (strcmp(small_rel, tname) == 0 || strcmp(large_rel, tname) == 0) {
                    char small_tok[8] = "null";
                    char large_tok[8] = "null";
                    char media[PATH_MAX] = "";
                    
                    char small_path[PATH_MAX];
                    char large_path[PATH_MAX];
                    snprintf(small_path, sizeof(small_path), "%s" DIR_SEP_STR "%s", per_thumbs_root, small_rel);
                    snprintf(large_path, sizeof(large_path), "%s" DIR_SEP_STR "%s", per_thumbs_root, large_rel);
                    
                    if (is_file(small_path)) strncpy(small_tok, "small", sizeof(small_tok) - 1);
                    if (is_file(large_path)) strncpy(large_tok, "large", sizeof(large_tok) - 1);
                    strncpy(media, media_full, sizeof(media) - 1);
                    media[sizeof(media) - 1] = '\0';
                    
                    // Store directly without legacy format
                    ht_set_internal(base, media);
                    
                    LOG_DEBUG("thumbdb: populated %s -> %s", base, media);
                    break;
                }
            }
            dir_close(&mit);
        }
        dir_close(&tit);
    }
}

int thumbdb_open_for_dir(const char* db_full_path) {
    if (!db_open_mutex_inited) { if (thread_mutex_init(&db_open_mutex) == 0) db_open_mutex_inited = 1; }
    thread_mutex_lock(&db_open_mutex);
    if (db_inited) {
        thumbdb_close();
    }
    if (thread_mutex_init(&db_mutex) != 0) {
        LOG_ERROR("thumbdb: failed to init mutex");
        return -1;
    }
    if (ht_ensure(INITIAL_BUCKETS) != 0) { thread_mutex_destroy(&db_mutex); return -1; }

    if (!db_full_path || db_full_path[0] == '\0') {
        thread_mutex_destroy(&db_mutex);
        return -1;
    }
    strncpy(db_path, db_full_path, sizeof(db_path) - 1); db_path[sizeof(db_path) - 1] = '\0';

    // Ensure database file exists and starts with magic header
    FILE* f_check = fopen(db_path, "rb");
    if (!f_check) {
        // File doesn't exist, create it with magic header
        FILE* f_new = fopen(db_path, "wb");
        if (!f_new) {
            LOG_WARN("thumbdb: failed to create db file %s", db_path);
            ht_free_all(); thread_mutex_destroy(&db_mutex); return -1;
        }
        fwrite(DB_MAGIC, 1, DB_MAGIC_LEN, f_new);
        fclose(f_new);
    } else {
        // File exists, check if it starts with magic header
        char probe[DB_MAGIC_LEN];
        size_t pr = fread(probe, 1, DB_MAGIC_LEN, f_check);
        fclose(f_check);
        if (pr != DB_MAGIC_LEN || memcmp(probe, DB_MAGIC, DB_MAGIC_LEN) != 0) {
            // File doesn't start with magic header, recreate it
            LOG_INFO("thumbdb: recreating database file with new opcode format");
            FILE* f_new = fopen(db_path, "wb");
            if (!f_new) {
                LOG_WARN("thumbdb: failed to recreate db file %s", db_path);
                ht_free_all(); thread_mutex_destroy(&db_mutex); return -1;
            }
            fwrite(DB_MAGIC, 1, DB_MAGIC_LEN, f_new);
            fclose(f_new);
        }
    }

    FILE* f = fopen(db_path, "ab");
    if (!f) {
        LOG_WARN("thumbdb: failed to open db file %s", db_path);
        ht_free_all(); thread_mutex_destroy(&db_mutex); return -1;
    }
    fclose(f);

    // Use new opcode-based binary format
    f = fopen(db_path, "rb");
    if (!f) {
        db_inited = 1;
        thread_mutex_unlock(&db_open_mutex);
        return 0;
    }
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);
    
    if (fsize > DB_MAGIC_LEN) {
        char magic[DB_MAGIC_LEN];
        if (fread(magic, 1, DB_MAGIC_LEN, f) != DB_MAGIC_LEN || memcmp(magic, DB_MAGIC, DB_MAGIC_LEN) != 0) {
            LOG_ERROR("thumbdb: invalid magic header in database file");
            fclose(f);
            ht_free_all(); thread_mutex_destroy(&db_mutex); thread_mutex_unlock(&db_open_mutex); return -1;
        }
        
        // Parse new simplified opcode-based records
        size_t offset = DB_MAGIC_LEN;
        while (offset < (size_t)fsize) {
            uint8_t opcode;
            if (fseek(f, offset, SEEK_SET) != 0) break;
            if (fread(&opcode, 1, 1, f) != 1) break;
            
            if (opcode != OP_BEGIN_RECORD) {
                offset++;
                continue;
            }
            
            offset++; // Move past BEGIN_RECORD
            
            // Skip separator
            uint8_t sep;
            if (fread(&sep, 1, 1, f) != 1 || sep != OP_SEPERATOR) break;
            offset++;
            
            // Read key until separator
            char key[PATH_MAX] = "";
            size_t key_len = 0;
            while (offset < (size_t)fsize && key_len < sizeof(key) - 1) {
                uint8_t ch;
                if (fseek(f, offset, SEEK_SET) != 0) break;
                if (fread(&ch, 1, 1, f) != 1) break;
                if (ch == OP_SEPERATOR) {
                    offset++;
                    break;
                }
                key[key_len++] = ch;
                offset++;
            }
            key[key_len] = '\0';
            
            // Read value until end record
            char value[PATH_MAX] = "";
            size_t val_len = 0;
            while (offset < (size_t)fsize && val_len < sizeof(value) - 1) {
                uint8_t ch;
                if (fseek(f, offset, SEEK_SET) != 0) break;
                if (fread(&ch, 1, 1, f) != 1) break;
                if (ch == OP_END_RECORD || ch == OP_NEWLINE) {
                    offset++;
                    break;
                }
                value[val_len++] = ch;
                offset++;
            }
            value[val_len] = '\0';
            
            // Store in hash table
            if (key_len > 0) {
                ht_set_internal(key, val_len > 0 ? value : NULL);
            }
        }
    }
    fclose(f);
    
    struct stat st;
    if (stat(db_path, &st) == 0) {
        db_last_mtime = st.st_mtime;
        db_last_size = st.st_size;
    } else {
        db_last_mtime = 0;
        db_last_size = 0;
    }
    
    // If database is empty, scan for existing thumbnails and populate
    if (is_table_empty()) {
        LOG_INFO("thumbdb: database is empty, scanning for existing thumbnails...");
        populate_db_from_existing_thumbs();
    }
    
    db_inited = 1;
    thread_mutex_unlock(&db_open_mutex);
    LOG_INFO("thumbdb: opened %s (buckets=%zu)", db_path, ht_buckets);
    return 0;
}

static void free_ht_table(ht_entry_t** table, size_t buckets) {
    if (!table) return;
    for (size_t i = 0; i < buckets; ++i) {
        ht_entry_t* e = table[i];
        while (e) { ht_entry_t* nx = e->next; free(e->key); free(e->val); free(e); e = nx; }
    }
    free(table);
}

struct rh_iter_ctx { void (*cb)(const char*, const char*, void*); void* user; };
static int rh_iter_wrap_cb(const char* key, const unsigned char* val, size_t val_len, void* ctx) {
    struct rh_iter_ctx* rc = (struct rh_iter_ctx*)ctx;
    if (!val || val_len == 0) {
        rc->cb(key, NULL, rc->user);
        return 0;
    }
    rc->cb(key, (const char*)val, rc->user);
    return 0;
}

struct comp_ctx { FILE* f; int restrict_to_base; char allowed_base[PATH_MAX]; int wrote_count; };

static int media_is_under_base(const char* media, const char* base) {
    if (!media || !base || base[0] == '\0') return 0;
    char resolved[PATH_MAX]; resolved[0] = '\0';
    if (strncmp(media, "/images/", 8) == 0) {
        char rel[PATH_MAX]; rel[0] = '\0';
        strncpy(rel, media + 8, sizeof(rel) - 1); rel[sizeof(rel) - 1] = '\0';
        char tmp[PATH_MAX]; snprintf(tmp, sizeof(tmp), "%s" DIR_SEP_STR "%s", BASE_DIR, rel);
        normalize_path(tmp);
        if (!real_path(tmp, resolved)) return 0;
        return safe_under(base, resolved);
    }
    if (real_path(media, resolved)) {
        return safe_under(base, resolved);
    }
    return 0;
}

static int rh_comp_cb(const char* key, const unsigned char* val, size_t val_len, void* ctx) {
    struct comp_ctx* c = (struct comp_ctx*)ctx;
    if (!key) return 0;
    // Store directly without legacy format parsing
    if (c->restrict_to_base && val && val_len > 0) {
        if (!media_is_under_base((const char*)val, c->allowed_base)) return 0;
    }
    if (append_line_to_file(c->f, key, (const char*)val) == 0) {
        c->wrote_count++;
    }
    return 0;
}

static int rh_write_bin_cb(const char* key, const unsigned char* val, size_t val_len, void* ctx) {
    FILE* f = (FILE*)ctx;
    if (!f || !key) return 0;
    
    uint8_t op_code;
    
    if (!val || val_len == 0) {
        op_code = 0x22;
    } else {
        char* p = strstr(key, "-small.");
        if (p) {
            op_code = 0x20;
        } else {
            p = strstr(key, "-large.");
            if (p) {
                op_code = 0x21;
            } else {
                op_code = 0x10;
            }
        }
    }
    
    uint32_t key_len = (uint32_t)strlen(key);
    uint32_t out_val_len = 1;
    uint8_t value_op_code = op_code;
    
    if (fwrite(&op_code, 1, 1, f) != 1) return -1;
    
    uint8_t sep = 0x5E;
    if (fwrite(&sep, 1, 1, f) != 1) return -1;
    
    if (fwrite(&key_len, sizeof(uint32_t), 1, f) != 1) return -1;
    
    if (fwrite(&sep, 1, 1, f) != 1) return -1;
    
    if (fwrite(key, 1, key_len, f) != key_len) return -1;
    
    if (fwrite(&sep, 1, 1, f) != 1) return -1;
    
    if (fwrite(&out_val_len, sizeof(uint32_t), 1, f) != 1) return -1;
    
    if (fwrite(&sep, 1, 1, f) != 1) return -1;
    
    if (fwrite(&value_op_code, 1, 1, f) != 1) return -1;
    
    if (fwrite(&sep, 1, 1, f) != 1) return -1;
    
    return 0;
}

struct find_media_ctx { const char* media; char* out; size_t out_len; char best[PATH_MAX]; };
static int rh_find_media_cb(const char* key, const unsigned char* val, size_t val_len, void* ctx) {
    struct find_media_ctx* fc = (struct find_media_ctx*)ctx;
    if (!val || val_len == 0) return 0;
    // Direct comparison without legacy format parsing
    if (strcmp((const char*)val, fc->media) == 0) {
        char found[PATH_MAX]; found[0] = '\0';
        if (find_thumb_filename_for_base(key, 1, found, sizeof(found))) {
            strncpy(fc->out, found, fc->out_len - 1); fc->out[fc->out_len - 1] = '\0'; return 1;
        }
        if (find_thumb_filename_for_base(key, 0, found, sizeof(found))) {
            strncpy(fc->out, found, fc->out_len - 1); fc->out[fc->out_len - 1] = '\0'; return 1;
        }
        if (fc->best[0] == '\0') strncpy(fc->best, key, sizeof(fc->best) - 1);
    }
    return 0;
}

typedef struct kv_t { char* key; char* val; } kv_t;

struct collect_ctx { kv_t** arrp; size_t* capp; size_t* countp; int err; };
static int rh_collect_kv_cb(const char* key, const unsigned char* val, size_t val_len, void* ctx) {
    struct collect_ctx* c = (struct collect_ctx*)ctx;
    if (c->err) return 1;
    kv_t* arr = *c->arrp;
    size_t cap = *c->capp;
    size_t count = *c->countp;
    if (count + 1 > cap) {
        size_t nc = cap * 2;
        kv_t* tmp = realloc(arr, nc * sizeof(*arr));
        if (!tmp) { c->err = 1; return 1; }
        arr = tmp; *c->arrp = arr; *c->capp = nc; cap = nc;
    }
    arr[count].key = key ? strdup(key) : NULL;
    if (!arr[count].key) { c->err = 1; return 1; }
    if (val && val_len > 0) arr[count].val = strdup((const char*)val); else arr[count].val = NULL;
    if (val && val_len > 0 && !arr[count].val) { free(arr[count].key); c->err = 1; return 1; }
    *c->countp = count + 1;
    return 0;
}
static void* rebuild_worker(void* arg) {
    (void)arg;
    struct stat st;
    if (stat(db_path, &st) != 0) return NULL;

    size_t power = 16;
    rh_table_t* new_tbl = rh_create(power);
    if (!new_tbl) return NULL;

    FILE* f = fopen(db_path, "rb");
    if (!f) { rh_destroy(new_tbl); return NULL; }

    // Verify magic header
    char magic[DB_MAGIC_LEN];
    if (fread(magic, 1, DB_MAGIC_LEN, f) != DB_MAGIC_LEN || memcmp(magic, DB_MAGIC, DB_MAGIC_LEN) != 0) {
        LOG_ERROR("thumbdb: invalid magic header in database file during rebuild");
        fclose(f);
        rh_destroy(new_tbl);
        return NULL;
    }

    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, DB_MAGIC_LEN, SEEK_SET);

    size_t offset = DB_MAGIC_LEN;
    while (offset < (size_t)fsize) {
        // Read operation code (binary)
        fseek(f, offset, SEEK_SET);
        uint8_t op_code = 0;
        if (fread(&op_code, 1, 1, f) != 1) break;
        offset += 1;
        
        // Read separator (0x5E) as binary
        uint8_t sep = 0;
        if (fread(&sep, 1, 1, f) != 1 || sep != 0x5E) break;
        offset += 1;
        
        // Read key length (binary)
        uint32_t key_len = 0;
        if (fread(&key_len, sizeof(uint32_t), 1, f) != 1) break;
        offset += sizeof(uint32_t);
        
        // Read separator (0x5E) as binary
        if (fread(&sep, 1, 1, f) != 1 || sep != 0x5E) break;
        offset += 1;
        
        // Read key data (binary)
        if (key_len == 0 || offset + key_len > (size_t)fsize) break;
        char* key = malloc(key_len + 1);
        if (!key) break;
        if (fread(key, 1, key_len, f) != key_len) { free(key); break; }
        key[key_len] = '\0';
        offset += key_len;
        
        // Read separator (0x5E) as binary
        if (fread(&sep, 1, 1, f) != 1 || sep != 0x5E) { free(key); break; }
        offset += 1;
        
        // Read value length (binary)
        uint32_t val_len = 0;
        if (fread(&val_len, sizeof(uint32_t), 1, f) != 1) { free(key); break; }
        offset += sizeof(uint32_t);
        
        // Read separator (0x5E) as binary
        if (fread(&sep, 1, 1, f) != 1 || sep != 0x5E) { free(key); break; }
        offset += 1;
        
        // Read value data (binary)
        unsigned char* val = NULL;
        if (val_len > 0) {
            if (offset + val_len > (size_t)fsize) { free(key); break; }
            val = malloc(val_len + 1);
            if (!val) { free(key); break; }
            if (fread(val, 1, val_len, f) != val_len) { free(key); free(val); break; }
            val[val_len] = '\0';
            offset += val_len;
        }
        
        // Read end marker (0x5E) as binary
        if (fread(&sep, 1, 1, f) != 1 || sep != 0x5E) {
            if (val) free(val);
            free(key);
            break;
        }
        offset += 1;
        
        // Validate operation codes to prevent invalid combinations
        if ((op_code == 0x20 || op_code == 0x21) && val_len == 0) {
            // Invalid: small/large thumbnail with null value
            if (val) free(val);
            free(key);
            continue;
        }
        if (op_code == 0x22 && val_len > 0) {
            // Invalid: null operation with non-null value
            if (val) free(val);
            free(key);
            continue;
        }
        
        // Process the operation
        if (op_code == 0x10) { // SET operation
            rh_insert(new_tbl, key, strlen(key), val, val_len);
        } else if (op_code == 0x20) { // SMALL_THUMB operation
            rh_insert(new_tbl, key, strlen(key), val, val_len);
        } else if (op_code == 0x21) { // LARGE_THUMB operation
            rh_insert(new_tbl, key, strlen(key), val, val_len);
        } else if (op_code == 0x22) { // NULL operation
            rh_insert(new_tbl, key, strlen(key), NULL, 0);
        }
        
        if (val) free(val);
        free(key);
    }
    fclose(f);

    thread_mutex_lock(&db_mutex);
    rh_table_t* old = rh_tbl;
    rh_tbl = new_tbl;
    ht_buckets = (size_t)1 << power;
    db_last_mtime = st.st_mtime;
    db_last_size = st.st_size;
    db_rebuilding = 0;
    thread_mutex_unlock(&db_mutex);

    if (old) rh_destroy(old);
    return NULL;
}

static int ensure_index_uptodate(void) {
    struct stat st;
    if (stat(db_path, &st) != 0) return -1;
    if (db_last_mtime == st.st_mtime && db_last_size == st.st_size) return 0;
    if (db_rebuilding) return 0;
    db_rebuilding = 1;
    if (thread_create_detached((void* (*)(void*))rebuild_worker, NULL) != 0) {
        db_rebuilding = 0;
        return -1;
    }
    return 0;
}

void thumbdb_close(void) {
    if (!db_inited) return;
    thread_mutex_lock(&db_mutex);
    ht_free_all();
    while (tx_head) { tx_op_t* n = tx_head->next; free(tx_head->key); free(tx_head->val); free(tx_head); tx_head = n; }
    tx_active = 0;
    thread_mutex_unlock(&db_mutex);
    thread_mutex_destroy(&db_mutex);
    db_inited = 0;
    if (db_open_mutex_inited) {
        thread_mutex_unlock(&db_open_mutex);
    }
}

int thumbdb_tx_begin(void) {
    if (!db_inited) return -1;
    thread_mutex_lock(&db_mutex);
    if (tx_active) { thread_mutex_unlock(&db_mutex); return -1; }
    tx_active = 1; tx_head = NULL;
    thread_mutex_unlock(&db_mutex);
    return 0;
}

int thumbdb_tx_abort(void) {
    if (!db_inited) {
        LOG_ERROR("thumbdb_tx_abort: database not initialized");
        return -1;
    }
    
    thread_mutex_lock(&db_mutex);
    if (!tx_active) {
        thread_mutex_unlock(&db_mutex);
        LOG_ERROR("thumbdb_tx_abort: no active transaction to abort");
        return -1;
    }
    
    // Count operations to be cleaned up
    size_t op_count = 0;
    tx_op_t* cur = tx_head;
    while (cur) { op_count++; cur = cur->next; }
    
    // Clean up all transaction operations
    int cleanup_errors = 0;
    cur = tx_head;
    while (cur) {
        tx_op_t* n = cur->next;
        if (cur->key) {
            free(cur->key);
            cur->key = NULL;
        }
        if (cur->val) {
            free(cur->val);
            cur->val = NULL;
        }
        free(cur);
        cur = n;
        op_count--;
    }
    tx_head = NULL;
    tx_active = 0;
    thread_mutex_unlock(&db_mutex);
    
    LOG_DEBUG("thumbdb_tx_abort: successfully aborted transaction (%zu operations discarded)", op_count);
    return cleanup_errors == 0 ? 0 : -1;
}

int thumbdb_tx_commit(void) {
    if (!db_inited) return -1;
    thread_mutex_lock(&db_mutex);
    if (!tx_active) {
        thread_mutex_unlock(&db_mutex);
        LOG_ERROR("thumbdb_tx_commit: no active transaction");
        return -1;
    }
    
    // Check if there are any operations to commit
    if (!tx_head) {
        LOG_DEBUG("thumbdb_tx_commit: no operations to commit");
        tx_active = 0;
        thread_mutex_unlock(&db_mutex);
        return 0;
    }
    
    // First, apply all operations to in-memory hash table
    tx_op_t* cur = tx_head;
    while (cur) {
        if (ht_set_internal(cur->key, cur->val) != 0) {
            thread_mutex_unlock(&db_mutex);
            LOG_ERROR("thumbdb_tx_commit: failed to set internal value for key: %s", cur->key ? cur->key : "(null)");
            return -1;
        }
        cur = cur->next;
    }
    
    // Persist all transaction operations to file using new opcode format
    FILE* f = fopen(db_path, "ab");
    if (!f) {
        LOG_WARN("thumbdb: failed to open db for append during commit");
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    cur = tx_head;
    int committed_ops = 0;
    while (cur) {
        if (append_line_to_file(f, cur->key, cur->val) != 0) {
            fclose(f);
            LOG_WARN("thumbdb: failed to write tx op during commit for key: %s", cur->key ? cur->key : "(null)");
            thread_mutex_unlock(&db_mutex);
            return -1;
        }
        committed_ops++;
        cur = cur->next;
    }
    if (fflush(f) != 0) {
        fclose(f);
        LOG_WARN("thumbdb: failed to flush database during commit");
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    if (platform_fsync(fileno(f)) != 0) {
        fclose(f);
        LOG_WARN("thumbdb: failed to sync database during commit");
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    fclose(f);
    
    struct stat st;
    if (stat(db_path, &st) == 0) {
        db_last_mtime = st.st_mtime;
        db_last_size = st.st_size;
    }

    // Clean up transaction list
    while (tx_head) { tx_op_t* n = tx_head->next; free(tx_head->key); free(tx_head->val); free(tx_head); tx_head = n; }
    tx_active = 0;
    thread_mutex_unlock(&db_mutex);
    LOG_DEBUG("thumbdb_tx_commit: successfully committed %d operations", committed_ops);
    return 0;
}

static int tx_record_op(const char* key, const char* val) {
    if (!key) return -1;
    tx_op_t* cur = tx_head;
    while (cur) {
        if (cur->key && strcmp(cur->key, key) == 0) {
            free(cur->val);
            cur->val = val ? strdup(val) : NULL;
            return 0;
        }
        cur = cur->next;
    }
    tx_op_t* op = calloc(1, sizeof(tx_op_t)); if (!op) return -1;
    op->key = strdup(key);
    op->val = val ? strdup(val) : NULL;
    op->next = NULL;
    if (!tx_head) tx_head = op; else { cur = tx_head; while (cur->next) cur = cur->next; cur->next = op; }
    return 0;
}

static int find_hash_conflict(const char* target_hash, char* conflicting_key, size_t key_len) {
    if (!rh_tbl || !target_hash || target_hash[0] == '\0') return 0;
    
    // Simple implementation - check if hash already exists
    const char* existing = ht_get(target_hash);
    if (existing) {
        // Hash conflict detected - return the target hash as conflicting key
        strncpy(conflicting_key, target_hash, key_len - 1);
        conflicting_key[key_len - 1] = '\0';
        return 1;
    }
    return 0;
}

int thumbdb_set(const char* key, const char* value) {
    if (!db_inited) return -1;
    if (!key) return -1;
    if (value && !is_valid_media_path(value)) {
        LOG_WARN("thumbdb_set: rejected invalid media path: %s", value);
        return -1;
    }
    thread_mutex_lock(&db_mutex);
    
    // Use the thumbnail filename directly as the database key (no hash deduplication)
    const char* db_key = key;
    const char* db_value = value;
    
    // For transaction mode, record the operation directly
    if (tx_active) {
        int r = tx_record_op(db_key, db_value);
        if (r != 0) {
            thread_mutex_unlock(&db_mutex);
            LOG_ERROR("thumbdb_set: failed to record transaction operation for key: %s", db_key);
            return -1;
        }
        thread_mutex_unlock(&db_mutex);
        return 0;
    }
    
    // For direct mode, check if we need to update
    const char* curr = ht_get(db_key);
    if (curr && db_value && strcmp(curr, db_value) == 0) {
        thread_mutex_unlock(&db_mutex);
        return 0;  // No change needed
    }
    if (!curr && !db_value) {
        thread_mutex_unlock(&db_mutex);
        return 0;  // No change needed
    }
    
    // Update the hash table
    int r = ht_set_internal(db_key, db_value);
    
    // Write to file immediately for direct mode
    FILE* f = fopen(db_path, "ab");
    if (f) {
        append_line_to_file(f, db_key, db_value);
        fflush(f);
        platform_fsync(fileno(f));
        fclose(f);
        struct stat st; if (stat(db_path, &st) == 0) { db_last_mtime = st.st_mtime; db_last_size = st.st_size; }
    }
    else {
        LOG_WARN("thumbdb: failed to open db file for append in thumbdb_set");
    }
    
    thread_mutex_unlock(&db_mutex);
    return r;
}

int thumbdb_get(const char* key, char* buf, size_t buflen) {
    if (!db_inited) return -1;
    if (!key || !buf) return -1;
    if (ensure_index_uptodate() != 0) return -1;
    thread_mutex_lock(&db_mutex);
    
    // Use the thumbnail filename directly as the lookup key
    const char* e_val = ht_get(key);
    if (!e_val) {
        thread_mutex_unlock(&db_mutex);
        return 1;
    }
    
    // Simply return the stored value
    strncpy(buf, e_val, buflen - 1);
    buf[buflen - 1] = '\0';
    thread_mutex_unlock(&db_mutex);
    return 0;
}

int thumbdb_delete(const char* key) {
    if (!db_inited) return -1;
    if (!key) return -1;
    thread_mutex_lock(&db_mutex);
    char base[PATH_MAX]; int is_small = 0, is_large = 0; base[0] = '\0';
    thumbname_to_base_and_kind(key, base, sizeof(base), &is_small, &is_large);

    // Extract hash from key or use key directly if it's a hash
    char hash_key[PATH_MAX] = "";
    
    // If key looks like a hash (32 hex chars), use it directly as the database key
    if (strlen(key) == 32) {
        const char* hex_chars = "0123456789abcdef";
        int is_hash = 1;
        for (int i = 0; i < 32; i++) {
            if (!strchr(hex_chars, tolower(key[i]))) {
                is_hash = 0;
                break;
            }
        }
        if (is_hash) {
            strncpy(hash_key, key, sizeof(hash_key) - 1);
        }
    }
    
    // If no hash found yet, use base name as hash (fallback)
    if (hash_key[0] == '\0') {
        strncpy(hash_key, base, sizeof(hash_key) - 1);
    }

    // Use direct value storage without legacy format parsing
    if (tx_active) {
        int r = tx_record_op(hash_key, NULL);
        thread_mutex_unlock(&db_mutex);
        return r;
    }

    ht_set_internal(hash_key, NULL);

    tx_op_t single_op;
    single_op.key = (char*)hash_key;
    single_op.val = NULL;
    single_op.next = NULL;
    int ret = persist_tx_ops(&single_op);
    if (ret == 0) {
        struct stat st; if (stat(db_path, &st) == 0) { db_last_mtime = st.st_mtime; db_last_size = st.st_size; }
    }
    // In binary mode, append delete operation(s) directly
    thread_mutex_unlock(&db_mutex);
    return ret;
}

void thumbdb_iterate(void (*cb)(const char* key, const char* value, void* ctx), void* ctx) {
    if (!db_inited) return;
    if (ensure_index_uptodate() != 0) return;
    thread_mutex_lock(&db_mutex);
    if (!rh_tbl) { thread_mutex_unlock(&db_mutex); return; }
    struct rh_iter_ctx rctx = { cb, ctx };
    rh_iterate(rh_tbl, rh_iter_wrap_cb, &rctx);
    thread_mutex_unlock(&db_mutex);
}

int thumbdb_find_for_media(const char* media_path, char* out_key, size_t out_key_len) {
    if (!media_path || !out_key || out_key_len == 0) return 1;
    if (!db_inited) return 1;
    if (ensure_index_uptodate() != 0) return 1;
    thread_mutex_lock(&db_mutex);
    char best_large[PATH_MAX]; best_large[0] = '\0';
    if (!rh_tbl) { thread_mutex_unlock(&db_mutex); return 1; }
    struct find_media_ctx ctx = { media_path, out_key, out_key_len, {0} };
    int r = rh_iterate(rh_tbl, rh_find_media_cb, &ctx);
    if (r != 0) { thread_mutex_unlock(&db_mutex); return 0; }
    strncpy(best_large, ctx.best, sizeof(best_large) - 1);
    if (best_large[0]) {
        char found[PATH_MAX]; found[0] = '\0';
        if (find_thumb_filename_for_base(best_large, 0, found, sizeof(found))) {
            strncpy(out_key, found, out_key_len - 1); out_key[out_key_len - 1] = '\0'; 
            thread_mutex_unlock(&db_mutex); return 0;
        }
    }
    thread_mutex_unlock(&db_mutex);
    return 1;
}

int thumbdb_compact(void) {
    if (!db_inited) return -1;
    if (ensure_index_uptodate() != 0) return -1;
    thread_mutex_lock(&db_mutex);
    
    if (!rh_tbl) {
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    
    // Create temporary file for safe writing
    char temp_path[PATH_MAX];
    snprintf(temp_path, sizeof(temp_path), "%s.tmp", db_path);
    
    FILE* out = fopen(temp_path, "wb");
    if (!out) {
        LOG_WARN("thumbdb: failed to open temporary database file for binary write");
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    
    if (fwrite(DB_MAGIC, 1, DB_MAGIC_LEN, out) != DB_MAGIC_LEN) {
        LOG_WARN("thumbdb: failed to write magic header");
        fclose(out);
        platform_file_delete(temp_path);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    
    int result = rh_iterate(rh_tbl, rh_write_bin_cb, out);
    if (result != 0) {
        LOG_WARN("thumbdb: iteration failed during processing");
        fclose(out);
        platform_file_delete(temp_path);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    
    if (fflush(out) != 0) {
        LOG_WARN("thumbdb: fflush failed during processing");
        fclose(out);
        platform_file_delete(temp_path);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    
    if (platform_fsync(fileno(out)) != 0) {
        LOG_WARN("thumbdb: fsync failed during processing");
        fclose(out);
        platform_file_delete(temp_path);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    
    fclose(out);
    
    // Atomically replace the old file with the new one
    if (platform_move_file(temp_path, db_path) != 0) {
        LOG_WARN("thumbdb: failed to replace database file");
        platform_file_delete(temp_path);
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    
    // Update file stats
    struct stat st;
    if (stat(db_path, &st) == 0) {
        db_last_mtime = st.st_mtime;
        db_last_size = st.st_size;
    }
    
    LOG_INFO("thumbdb: processing completed");
    thread_mutex_unlock(&db_mutex);
    return 0;
}

static void sweep_cb(const char* key, const char* value, void* ctx) {
    (void)ctx;
    if (!value || value[0] == '\0') return;
    // Direct value comparison without legacy format parsing
    const char* media = value;
    if (!media[0]) return;
    if (!is_file(media)) {
        char thumb_path[PATH_MAX];
        bool removed = false;
        char found[PATH_MAX]; found[0] = '\0';
        if (find_thumb_filename_for_base(key, 1, found, sizeof(found))) {
            char thumb_dir[PATH_MAX]; thumb_dir[0] = '\0';
            if (db_path[0]) {
                strncpy(thumb_dir, db_path, sizeof(thumb_dir) - 1); thumb_dir[sizeof(thumb_dir) - 1] = '\0'; char* last = strrchr(thumb_dir, DIR_SEP); if (last) *last = '\0'; else strncpy(thumb_dir, ".", sizeof(thumb_dir) - 1);
                snprintf(thumb_path, sizeof(thumb_path), "%s" DIR_SEP_STR "%s", thumb_dir, found);
                if (is_file(thumb_path)) { if (platform_file_delete(thumb_path) == 0) { LOG_DEBUG("thumbdb: removed thumb %s because media missing: %s", thumb_path, media); removed = true; } else LOG_WARN("thumbdb: failed to remove thumb %s", thumb_path); }
            }
        }
        if (!removed && find_thumb_filename_for_base(key, 0, found, sizeof(found))) {
            char thumb_dir[PATH_MAX]; thumb_dir[0] = '\0';
            if (db_path[0]) {
                strncpy(thumb_dir, db_path, sizeof(thumb_dir) - 1); thumb_dir[sizeof(thumb_dir) - 1] = '\0'; char* last = strrchr(thumb_dir, DIR_SEP); if (last) *last = '\0'; else strncpy(thumb_dir, ".", sizeof(thumb_dir) - 1);
                snprintf(thumb_path, sizeof(thumb_path), "%s" DIR_SEP_STR "%s", thumb_dir, found);
                if (is_file(thumb_path)) { if (platform_file_delete(thumb_path) == 0) { LOG_DEBUG("thumbdb: removed thumb %s because media missing: %s", thumb_path, media); removed = true; } else LOG_WARN("thumbdb: failed to remove thumb %s", thumb_path); }
            }
        }

        if (!removed) {
            char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
            diriter dit;
            if (dir_open(&dit, thumbs_root)) {
                const char* dname;
                while ((dname = dir_next(&dit))) {
                    if (!strcmp(dname, ".") || !strcmp(dname, "..")) continue;
                    char sub[PATH_MAX]; path_join(sub, thumbs_root, dname);
                    if (!is_dir(sub)) continue;
                    if (find_thumb_filename_for_base_in_dir(sub, key, 1, found, sizeof(found))) {
                        snprintf(thumb_path, sizeof(thumb_path), "%s" DIR_SEP_STR "%s", sub, found);
                        if (is_file(thumb_path) && platform_file_delete(thumb_path) == 0) { LOG_DEBUG("thumbdb: removed thumb %s because media missing: %s", thumb_path, media); removed = true; break; }
                    }
                    if (find_thumb_filename_for_base_in_dir(sub, key, 0, found, sizeof(found))) {
                        snprintf(thumb_path, sizeof(thumb_path), "%s" DIR_SEP_STR "%s", sub, found);
                        if (is_file(thumb_path) && platform_file_delete(thumb_path) == 0) { LOG_DEBUG("thumbdb: removed thumb %s because media missing: %s", thumb_path, media); removed = true; break; }
                    }
                }
                dir_close(&dit);
            }
        }

        thumbdb_delete(key);
    }
}

int thumbdb_sweep_orphans(void) {
    if (!db_inited) return -1;

    if (ensure_index_uptodate() != 0) return -1;
    thread_mutex_lock(&db_mutex);
    size_t cap = 128; size_t count = 0;
    kv_t* arr = malloc(cap * sizeof(*arr));
    if (!arr) { thread_mutex_unlock(&db_mutex); return -1; }
    if (!rh_tbl) { free(arr); thread_mutex_unlock(&db_mutex); return -1; }
    struct collect_ctx cctx = { &arr, &cap, &count, 0 };
    int r = rh_iterate(rh_tbl, rh_collect_kv_cb, &cctx);
    if (r != 0 || cctx.err) {
        for (size_t j = 0; j < count; ++j) { free(arr[j].key); free(arr[j].val); }
        free(arr); thread_mutex_unlock(&db_mutex); return -1;
    }
    thread_mutex_unlock(&db_mutex);

    size_t del_cap = 64; size_t del_count = 0; char** dels = malloc(del_cap * sizeof(char*));
    if (!dels) { for (size_t j = 0; j < count; ++j) { free(arr[j].key); free(arr[j].val); } free(arr); return -1; }

    for (size_t i = 0; i < count; ++i) {
        char* key = arr[i].key; char* value = arr[i].val;

        if (!value || value[0] == '\0') { free(key); free(value); continue; }

        bool removed = false;
        char thumb_dir[PATH_MAX]; thumb_dir[0] = '\0';
        if (db_path[0]) {
            strncpy(thumb_dir, db_path, sizeof(thumb_dir) - 1);
            thumb_dir[sizeof(thumb_dir) - 1] = '\0';
            char* last = strrchr(thumb_dir, DIR_SEP);
            if (last) *last = '\0'; else strncpy(thumb_dir, ".", sizeof(thumb_dir) - 1);
        }
        else {
            get_thumbs_root(thumb_dir, sizeof(thumb_dir));
        }

        // Direct value comparison without legacy format parsing
        const char* media = value;
        char thumb_path[PATH_MAX];
        snprintf(thumb_path, sizeof(thumb_path), "%s" DIR_SEP_STR "%s", thumb_dir, key);
        if (!is_file(media)) {
            if (find_thumb_filename_for_base(key, 1, thumb_path, sizeof(thumb_path))) {
                if (is_file(thumb_path)) {
                    if (platform_file_delete(thumb_path) == 0) { LOG_DEBUG("thumbdb: removed thumb %s because media missing: %s", thumb_path, media); removed = true; }
                    else LOG_WARN("thumbdb: failed to remove thumb %s", thumb_path);
                }
            }

            if (!removed) {
                char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
                if (find_thumb_filename_for_base(key, 1, thumb_path, sizeof(thumb_path))) {
                    if (is_file(thumb_path)) { if (platform_file_delete(thumb_path) == 0) { LOG_DEBUG("thumbdb: removed thumb %s because media missing: %s", thumb_path, media); removed = true; } else LOG_WARN("thumbdb: failed to remove thumb %s", thumb_path); }
                }
                if (!removed && find_thumb_filename_for_base(key, 0, thumb_path, sizeof(thumb_path))) {
                    if (is_file(thumb_path)) { if (platform_file_delete(thumb_path) == 0) { LOG_DEBUG("thumbdb: removed thumb %s because media missing: %s", thumb_path, media); removed = true; } else LOG_WARN("thumbdb: failed to remove thumb %s", thumb_path); }
                }
                if (!removed) {
                    diriter dit; if (dir_open(&dit, thumbs_root)) {
                        const char* dname;
                        while ((dname = dir_next(&dit))) {
                            if (!strcmp(dname, ".") || !strcmp(dname, "..")) continue;
                            char sub[PATH_MAX]; path_join(sub, thumbs_root, dname);
                            if (!is_dir(sub)) continue;
                            if (find_thumb_filename_for_base_in_dir(sub, key, 1, thumb_path, sizeof(thumb_path))) {
                                snprintf(thumb_path, sizeof(thumb_path), "%s" DIR_SEP_STR "%s", sub, thumb_path);
                                if (is_file(thumb_path) && platform_file_delete(thumb_path) == 0) { LOG_DEBUG("thumbdb: removed thumb %s because media missing: %s", thumb_path, media); removed = true; break; }
                            }
                            if (find_thumb_filename_for_base_in_dir(sub, key, 0, thumb_path, sizeof(thumb_path))) {
                                snprintf(thumb_path, sizeof(thumb_path), "%s" DIR_SEP_STR "%s", sub, thumb_path);
                                if (is_file(thumb_path) && platform_file_delete(thumb_path) == 0) { LOG_DEBUG("thumbdb: removed thumb %s because media missing: %s", thumb_path, media); removed = true; break; }
                            }
                        }
                        dir_close(&dit);
                    }
                }
            }

            if (del_count + 1 > del_cap) {
                size_t nc = del_cap * 2; char** tmp = realloc(dels, nc * sizeof(char*)); if (!tmp) {}
                else { dels = tmp; del_cap = nc; }
            }
            dels[del_count++] = strdup(key);
        }

        free(key); free(value);
    }
    free(arr);

    if (del_count == 0) { free(dels); return 0; }

    thread_mutex_lock(&db_mutex);
    for (size_t i = 0; i < del_count; ++i) {
        ht_set_internal(dels[i], NULL);
    }
    // Persist orphan deletions to file
    if (!db_binary_mode) {
        FILE* f = fopen(db_path, "ab");
        if (f) {
            for (size_t i = 0; i < del_count; ++i) {
                append_line_to_file(f, dels[i], NULL);
            }
            fflush(f);
            platform_fsync(fileno(f));
            fclose(f);
            struct stat st; if (stat(db_path, &st) == 0) { db_last_mtime = st.st_mtime; db_last_size = st.st_size; }
        }
        else {
            LOG_WARN("thumbdb: failed to open db for appending orphan deletions");
        }
    }
    else {
        FILE* f = fopen(db_path, "ab");
        if (f) {
            for (size_t i = 0; i < del_count; ++i) {
                append_line_to_file(f, dels[i], NULL);
            }
            fflush(f);
            platform_fsync(fileno(f));
            fclose(f);
            struct stat st; if (stat(db_path, &st) == 0) { db_last_mtime = st.st_mtime; db_last_size = st.st_size; }
        }
        else {
            LOG_WARN("thumbdb: failed to open db for appending orphan deletions");
        }
    }
    thread_mutex_unlock(&db_mutex);

    for (size_t i = 0; i < del_count; ++i) free(dels[i]); free(dels);

    return 0;
}
