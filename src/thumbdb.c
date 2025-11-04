#include "thumbdb.h"
#include "platform.h"
#include "logging.h"
#include "utils.h"
#include "directory.h"
#include "thread_pool.h"
#include "crypto.h"
#include "common.h"
#include "compress.h"
#include "robinhood_hash.h"
#include "arena.h"
#include "config.h"
#include "thumbs.h"

#define DB_FILENAME "thumbs.db"
#define LINE_MAX 4096
#define INITIAL_BUCKETS 65536
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

static void split_combined_val(const char* val, char* small, size_t small_len, char* large, size_t large_len, char* media, size_t media_len) {
    small[0] = '\0'; large[0] = '\0'; media[0] = '\0';
    if (!val) return;
    char* v = strdup(val);
    if (!v) return;
    char* s1 = strchr(v, ';');
    if (!s1) { strncpy(media, v, media_len - 1); media[media_len - 1] = '\0'; free(v); return; }
    *s1 = '\0'; strncpy(small, v, small_len - 1); small[small_len - 1] = '\0';
    char* s2 = s1 + 1;
    char* s3 = strchr(s2, ';');
    if (!s3) { strncpy(large, s2, large_len - 1); large[large_len - 1] = '\0'; free(v); return; }
    *s3 = '\0'; strncpy(large, s2, large_len - 1); large[large_len - 1] = '\0'; strncpy(media, s3 + 1, media_len - 1); media[media_len - 1] = '\0';
    free(v);
}

static void make_combined_val(char* out, size_t out_len, const char* small_tok, const char* large_tok, const char* media) {
    if (!out || out_len == 0) return;
    const char* s = small_tok ? small_tok : "null";
    const char* l = large_tok ? large_tok : "null";
    const char* m = media ? media : "";
    snprintf(out, out_len, "%s;%s;%s", s, l, m);
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

static int append_line_to_file(FILE* f, const char* key, const char* val) {
    if (!f) return -1;
    if (!key) return -1;
    if (db_binary_mode) {
        uint32_t key_len = (uint32_t)strlen(key);
        uint32_t val_len = 0;
        unsigned char* comp = NULL;
        if (val && val[0]) {
            size_t tmp_len = 0;
            if (compress_val(val, strlen(val), &comp, &tmp_len) != 0) { return -1; }
            val_len = (uint32_t)tmp_len;
        }
        else {
            val_len = 0;
        }
        if (fwrite(&key_len, sizeof(uint32_t), 1, f) != 1) { if (comp) free(comp); return -1; }
        if (fwrite(&val_len, sizeof(uint32_t), 1, f) != 1) { if (comp) free(comp); return -1; }
        if (fwrite(key, 1, key_len, f) != key_len) { if (comp) free(comp); return -1; }
        if (val_len > 0) {
            if (fwrite(comp, 1, val_len, f) != val_len) { free(comp); return -1; }
            free(comp);
        }
    }
    else {
        if (val) {
            if (fprintf(f, "%s;%s\n", key, val) < 0) return -1;
        }
        else {
            if (fprintf(f, "%s;\n", key) < 0) return -1;
        }
    }
    return 0;
}

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
    FILE* f = fopen(db_path, db_binary_mode ? "ab" : "a");
    if (!f) return -1;
    tx_op_t* cur = ops;
    while (cur) {
        if (append_line_to_file(f, cur->key, cur->val) != 0) { fclose(f); return -1; }
        cur = cur->next;
    }
    fflush(f);
    platform_fsync(fileno(f));
    fclose(f);
    return 0;
}

int thumbdb_open(void) {
    LOG_WARN("thumbdb_open: opening global top-level DB is disabled; use thumbdb_open_for_dir() with a per-gallery path");
    return -1;
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

    FILE* f = fopen(db_path, "a+");
    if (!f) {
        LOG_WARN("thumbdb: failed to open db file %s", db_path);
        ht_free_all(); thread_mutex_destroy(&db_mutex); return -1;
    }
    fclose(f);

    f = fopen(db_path, "rb");
    if (!f) { db_inited = 1; return 0; }
    char probe[256]; size_t pr = fread(probe, 1, sizeof(probe), f);
    db_binary_mode = 0;
    if (pr > 0) {
        if (probe[0] == '#') {
            if (strncmp(probe, "#version;", 9) == 0) db_binary_mode = 1;
        }
    }
    fseek(f, 0, SEEK_SET);
    if (!db_binary_mode) {
        char line[LINE_MAX];
        while (fgets(line, sizeof(line), f)) {
            char* nl = strchr(line, '\n'); if (nl) *nl = '\0';
            if (line[0] == '#') continue;
            char* sem = strchr(line, ';');
            if (sem) { *sem = '\0'; char* key = line; char* val = sem + 1; if (val[0] == '\0') { ht_set_internal(key, NULL); } else { ht_set_internal(key, val); } continue; }
            char* tab = strchr(line, '\t'); if (!tab) continue; *tab = '\0'; char* key = line; char* val = tab + 1;
            if (val[0] == '\0') {
                char base[PATH_MAX]; base[0] = '\0'; char* p = strstr(key, "-small.");
                if (p) { size_t bl = (size_t)(p - key); if (bl >= sizeof(base)) bl = sizeof(base) - 1; memcpy(base, key, bl); base[bl] = '\0'; }
                else { p = strstr(key, "-large."); if (p) { size_t bl = (size_t)(p - key); if (bl >= sizeof(base)) bl = sizeof(base) - 1; memcpy(base, key, bl); base[bl] = '\0'; } else { strncpy(base, key, sizeof(base) - 1); base[sizeof(base) - 1] = '\0'; } }
                ht_set_internal(base, NULL);
            }
            else {
                char base[PATH_MAX]; base[0] = '\0'; int this_is_small = 0; char* p = strstr(key, "-small.");
                if (p) { size_t bl = (size_t)(p - key); if (bl >= sizeof(base)) bl = sizeof(base) - 1; memcpy(base, key, bl); base[bl] = '\0'; this_is_small = 1; }
                else { p = strstr(key, "-large."); if (p) { size_t bl = (size_t)(p - key); if (bl >= sizeof(base)) bl = sizeof(base) - 1; memcpy(base, key, bl); base[bl] = '\0'; this_is_small = 0; } else { strncpy(base, key, sizeof(base) - 1); base[sizeof(base) - 1] = '\0'; } }
                const char* exist_val = ht_get(base);
                char small_tok[8] = "null"; char large_tok[8] = "null"; char media[PATH_MAX] = "";
                if (exist_val) {
                    char* v = strdup(exist_val);
                    if (v) { char* s1 = strchr(v, ';'); if (s1) { *s1 = '\0'; strncpy(small_tok, v, sizeof(small_tok) - 1); small_tok[sizeof(small_tok) - 1] = '\0'; char* s2 = s1 + 1; char* s3 = strchr(s2, ';'); if (s3) { *s3 = '\0'; strncpy(large_tok, s2, sizeof(large_tok) - 1); large_tok[sizeof(large_tok) - 1] = '\0'; strncpy(media, s3 + 1, sizeof(media) - 1); media[sizeof(media) - 1] = '\0'; } } free(v); }
                }
                if (this_is_small) strncpy(small_tok, "small", sizeof(small_tok) - 1); else strncpy(large_tok, "large", sizeof(large_tok) - 1);
                strncpy(media, val, sizeof(media) - 1); media[sizeof(media) - 1] = '\0';
                char combined[LINE_MAX]; snprintf(combined, sizeof(combined), "%s;%s;%s", small_tok, large_tok, media);
                ht_set_internal(base, combined);
            }
        }
    }
    else {
        char header[LINE_MAX]; if (!fgets(header, sizeof(header), f)) { fclose(f); db_inited = 1; return 0; }
        while (1) {
            uint32_t key_len = 0; uint32_t val_len = 0;
            if (fread(&key_len, sizeof(uint32_t), 1, f) != 1) break;
            if (fread(&val_len, sizeof(uint32_t), 1, f) != 1) break;
            if (key_len == 0) { fseek(f, val_len, SEEK_CUR); continue; }
            char* key = malloc(key_len + 1); if (!key) break;
            if (fread(key, 1, key_len, f) != key_len) { free(key); break; }
            key[key_len] = '\0';
            if (val_len == 0) { ht_set_internal(key, NULL); free(key); continue; }
            unsigned char* comp = malloc(val_len);
            if (!comp) { free(key); break; }
            if (fread(comp, 1, val_len, f) != val_len) { free(key); free(comp); break; }
            unsigned char* decomp = NULL; size_t decomp_len = 0;
            if (decompress_val(comp, val_len, &decomp, &decomp_len) == 0) {
                ht_set_internal(key, (const char*)decomp);
                free(decomp);
            }
            else {
                ht_set_internal(key, NULL);
            }
            free(comp); free(key);
        }
    }
    fclose(f);
    {
        struct stat st;
        if (stat(db_path, &st) == 0) {
            db_last_mtime = st.st_mtime;
            db_last_size = st.st_size;
        }
        else {
            db_last_mtime = 0; db_last_size = 0;
        }
    }
    if (!db_binary_mode) {
        if (backup_file(db_path) == 0) LOG_INFO("thumbdb: backed up legacy db to %s.bak", db_path);
        else LOG_WARN("thumbdb: failed to backup legacy db %s", db_path);

        char tmp_path[PATH_MAX]; snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", db_path);
        FILE* out = fopen(tmp_path, "wb");
        if (out) {
            fprintf(out, "#version;1.0\n");
            if (rh_tbl) {
                rh_iterate(rh_tbl, rh_write_bin_cb, out);
            }
            else if (ht) {
                for (size_t i = 0; i < ht_buckets; ++i) {
                    ht_entry_t* e = ht[i];
                    while (e) {
                        rh_write_bin_cb(e->key, (const unsigned char*)e->val, e->val ? strlen(e->val) + 1 : 0, out);
                        e = e->next;
                    }
                }
            }
            fflush(out); platform_fsync(fileno(out)); fclose(out);
            if (platform_move_file(tmp_path, db_path) == 0) {
                LOG_INFO("thumbdb: converted legacy db to binary format: %s", db_path);
                db_binary_mode = 1;
            }
            else {
                LOG_WARN("thumbdb: failed to replace legacy db with binary conversion");
                platform_file_delete(tmp_path);
            }
        }
        else {
            LOG_WARN("thumbdb: failed to open temp file for binary conversion: %s", tmp_path);
        }
    }
    db_inited = 1;
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
    if (val_len >= 4 && val[0] == 'H' && val[1] == 'H' && val[2] == 'R' && val[3] == '1') {
        unsigned char* decomp = NULL; size_t decomp_len = 0;
        if (decompress_val(val, val_len, &decomp, &decomp_len) == 0 && decomp) {
            rc->cb(key, (const char*)decomp, rc->user);
            free(decomp);
            return 0;
        }
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
    if (!key || !val || val_len == 0) return 0;
    char small_tok[8], large_tok[8], media[PATH_MAX]; small_tok[0] = large_tok[0] = media[0] = '\0';
    split_combined_val((const char*)val, small_tok, sizeof(small_tok), large_tok, sizeof(large_tok), media, sizeof(media));
    if (c->restrict_to_base) {
        if (!media[0]) return 0;
        if (!media_is_under_base(media, c->allowed_base)) return 0;
    }
    if (append_line_to_file(c->f, key, (const char*)val) == 0) {
        c->wrote_count++;
    }
    return 0;
}

static int rh_write_bin_cb(const char* key, const unsigned char* val, size_t val_len, void* ctx) {
    FILE* f = (FILE*)ctx;
    if (!f || !key) return 0;
    uint32_t key_len = (uint32_t)strlen(key);
    uint32_t out_val_len = 0;
    unsigned char* comp = NULL;
    if (val && val_len > 0) {
        size_t in_len = val_len;
        if (((const unsigned char*)val)[val_len - 1] == 0 && val_len > 0) in_len = val_len - 1;
        size_t tmp_len = 0;
        if (compress_val((const char*)val, in_len, &comp, &tmp_len) == 0) {
            out_val_len = (uint32_t)tmp_len;
        }
    }
    if (fwrite(&key_len, sizeof(uint32_t), 1, f) != 1) { if (comp) free(comp); return -1; }
    if (fwrite(&out_val_len, sizeof(uint32_t), 1, f) != 1) { if (comp) free(comp); return -1; }
    if (fwrite(key, 1, key_len, f) != key_len) { if (comp) free(comp); return -1; }
    if (out_val_len > 0) {
        if (fwrite(comp, 1, out_val_len, f) != out_val_len) { free(comp); return -1; }
        free(comp);
    }
    return 0;
}

struct find_media_ctx { const char* media; char* out; size_t out_len; char best[PATH_MAX]; };
static int rh_find_media_cb(const char* key, const unsigned char* val, size_t val_len, void* c) {
    struct find_media_ctx* fc = (struct find_media_ctx*)c;
    if (!val || val_len == 0) return 0;
    char small_tok[8], large_tok[8], media[PATH_MAX]; small_tok[0] = large_tok[0] = media[0] = '\0';
    split_combined_val((const char*)val, small_tok, sizeof(small_tok), large_tok, sizeof(large_tok), media, sizeof(media));
    if (media[0] != '\0' && strcmp(media, fc->media) == 0) {
        char found[PATH_MAX]; found[0] = '\0';
        if (strcmp(small_tok, "small") == 0) {
            if (find_thumb_filename_for_base(key, 1, found, sizeof(found))) {
                strncpy(fc->out, found, fc->out_len - 1); fc->out[fc->out_len - 1] = '\0'; return 1;
            }
        }
        if (strcmp(large_tok, "large") == 0) {
            if (find_thumb_filename_for_base(key, 0, found, sizeof(found))) {
                strncpy(fc->out, found, fc->out_len - 1); fc->out[fc->out_len - 1] = '\0'; return 1;
            }
            if (fc->best[0] == '\0') strncpy(fc->best, key, sizeof(fc->best) - 1);
        }
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

    char probe[256]; size_t pr = fread(probe, 1, sizeof(probe), f);
    int local_binary = 0;
    if (pr > 0) { if (probe[0] == '#') { if (strncmp(probe, "#version;", 9) == 0) local_binary = 1; } }
    fseek(f, 0, SEEK_SET);

    if (!local_binary) {
        char line[LINE_MAX];
        while (fgets(line, sizeof(line), f)) {
            char* nl = strchr(line, '\n'); if (nl) *nl = '\0'; if (line[0] == '#') continue;
            char* sem = strchr(line, ';');
            if (sem) {
                *sem = '\0'; char* key = line; char* val = sem + 1;
                if (val[0] == '\0') rh_insert(new_tbl, key, strlen(key), NULL, 0);
                else rh_insert(new_tbl, key, strlen(key), (const unsigned char*)val, strlen(val) + 1);
                continue;
            }
            char* tab = strchr(line, '\t'); if (!tab) continue; *tab = '\0'; char* key = line; char* val = tab + 1;
            if (val[0] == '\0') { rh_insert(new_tbl, key, strlen(key), NULL, 0); }
            else {
                char base[PATH_MAX]; base[0] = '\0'; char* p = strstr(key, "-small."); int this_is_small = 0;
                if (p) { size_t bl = (size_t)(p - key); if (bl >= sizeof(base)) bl = sizeof(base) - 1; memcpy(base, key, bl); base[bl] = '\0'; this_is_small = 1; }
                else { p = strstr(key, "-large."); if (p) { size_t bl = (size_t)(p - key); if (bl >= sizeof(base)) bl = sizeof(base) - 1; memcpy(base, key, bl); base[bl] = '\0'; this_is_small = 0; } else strncpy(base, key, sizeof(base) - 1); base[sizeof(base) - 1] = '\0'; }
                char small_tok[8] = "null"; char large_tok[8] = "null"; char media[PATH_MAX] = "";
                if (this_is_small) strncpy(small_tok, "small", sizeof(small_tok) - 1); else strncpy(large_tok, "large", sizeof(large_tok) - 1);
                strncpy(media, val, sizeof(media) - 1); media[sizeof(media) - 1] = '\0'; char combined[LINE_MAX]; snprintf(combined, sizeof(combined), "%s;%s;%s", small_tok, large_tok, media);
                rh_insert(new_tbl, base, strlen(base), (const unsigned char*)combined, strlen(combined) + 1);
            }
        }
    } else {
        char header[LINE_MAX]; if (!fgets(header, sizeof(header), f)) { fclose(f); rh_destroy(new_tbl); return NULL; }
        while (1) {
            uint32_t key_len = 0; uint32_t val_len = 0;
            if (fread(&key_len, sizeof(uint32_t), 1, f) != 1) break;
            if (fread(&val_len, sizeof(uint32_t), 1, f) != 1) break;
            if (key_len == 0) { fseek(f, val_len, SEEK_CUR); continue; }
            char* key = malloc(key_len + 1); if (!key) break; if (fread(key, 1, key_len, f) != key_len) { free(key); break; } key[key_len] = '\0';
            if (val_len == 0) { rh_insert(new_tbl, key, strlen(key), NULL, 0); free(key); continue; }
            unsigned char* comp = malloc(val_len); if (!comp) { free(key); break; }
            if (fread(comp, 1, val_len, f) != val_len) { free(key); free(comp); break; }
            unsigned char* decomp = NULL; size_t decomp_len = 0;
            if (decompress_val(comp, val_len, &decomp, &decomp_len) == 0) {
                rh_insert(new_tbl, key, strlen(key), decomp, decomp_len + 1);
                free(decomp);
            } else {
                rh_insert(new_tbl, key, strlen(key), NULL, 0);
            }
            free(comp); free(key);
        }
    }
    fclose(f);

    thread_mutex_lock(&db_mutex);
    rh_table_t* old = rh_tbl;
    rh_tbl = new_tbl;
    ht_buckets = (size_t)1 << power;
    db_last_mtime = st.st_mtime; db_last_size = st.st_size; db_rebuilding = 0;
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
    thread_mutex_lock(&db_mutex);
    if (!tx_active) { thread_mutex_unlock(&db_mutex); return -1; }
    while (tx_head) { tx_op_t* n = tx_head->next; free(tx_head->key); free(tx_head->val); free(tx_head); tx_head = n; }
    tx_active = 0;
    thread_mutex_unlock(&db_mutex);
    return 0;
}

int thumbdb_tx_commit(void) {
    if (!db_inited) return -1;
    thread_mutex_lock(&db_mutex);
    if (!tx_active) { thread_mutex_unlock(&db_mutex); return -1; }
    tx_op_t* cur = tx_head;
    while (cur) { ht_set_internal(cur->key, cur->val); cur = cur->next; }
    FILE* f = fopen(db_path, db_binary_mode ? "ab" : "a");
    if (!f) {
        LOG_WARN("thumbdb: failed to open db for append during commit");
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    cur = tx_head;
    while (cur) {
        int do_write = 1;
        if (!db_binary_mode) {
            char lastval[LINE_MAX]; lastval[0] = '\0';
            if (last_text_value_for_key(db_path, cur->key, lastval, sizeof(lastval)) == 0) {
                if ((cur->val == NULL && lastval[0] == '\0') || (cur->val && strcmp(lastval, cur->val) == 0)) do_write = 0;
            }
        }
        if (do_write) {
            if (append_line_to_file(f, cur->key, cur->val) != 0) {
                fclose(f);
                LOG_WARN("thumbdb: failed to write tx op during commit");
                thread_mutex_unlock(&db_mutex);
                return -1;
            }
        }
        cur = cur->next;
    }
    fflush(f);
    platform_fsync(fileno(f));
    fclose(f);
    {
        struct stat st; if (stat(db_path, &st) == 0) { db_last_mtime = st.st_mtime; db_last_size = st.st_size; }
    }

    while (tx_head) { tx_op_t* n = tx_head->next; free(tx_head->key); free(tx_head->val); free(tx_head); tx_head = n; }
    tx_active = 0;
    thread_mutex_unlock(&db_mutex);
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

int thumbdb_set(const char* key, const char* value) {
    if (!db_inited) return -1;
    if (!key) return -1;
    thread_mutex_lock(&db_mutex);
    char base[PATH_MAX]; int is_small = 0, is_large = 0; base[0] = '\0';
    thumbname_to_base_and_kind(key, base, sizeof(base), &is_small, &is_large);

    const char* exist_val = ht_get(base);
    char small_tok[8] = "null"; char large_tok[8] = "null"; char media[PATH_MAX] = "";
    if (exist_val) split_combined_val(exist_val, small_tok, sizeof(small_tok), large_tok, sizeof(large_tok), media, sizeof(media));
    if (is_small) strncpy(small_tok, "small", sizeof(small_tok) - 1);
    if (is_large) strncpy(large_tok, "large", sizeof(large_tok) - 1);
    if (value) strncpy(media, value, sizeof(media) - 1);
    char combined[LINE_MAX]; make_combined_val(combined, sizeof(combined), small_tok, large_tok, media);

    if (tx_active) {
        tx_op_t* tcur = tx_head;
        while (tcur) {
            if (tcur->key && strcmp(tcur->key, base) == 0) {
                if ((tcur->val == NULL && combined[0] == '\0') || (tcur->val && strcmp(tcur->val, combined) == 0)) {
                    thread_mutex_unlock(&db_mutex);
                    return 0;
                }
                break;
            }
            tcur = tcur->next;
        }
        int r = tx_record_op(base, combined);
        thread_mutex_unlock(&db_mutex);
        return r;
    }
    const char* curr = ht_get(base);
    if (curr && strcmp(curr, combined) == 0) {
        thread_mutex_unlock(&db_mutex);
        return 0;
    }
    int r = ht_set_internal(base, combined);
    FILE* f = fopen(db_path, db_binary_mode ? "ab" : "a");
    if (f) {
        if (!db_binary_mode) {
            char lastval[LINE_MAX]; lastval[0] = '\0';
            if (last_text_value_for_key(db_path, base, lastval, sizeof(lastval)) == 0) {
                if (strcmp(lastval, combined) != 0) append_line_to_file(f, base, combined);
            }
            else {
                append_line_to_file(f, base, combined);
            }
        }
        else {
            append_line_to_file(f, base, combined);
        }
        fflush(f);
        platform_fsync(fileno(f));
        fclose(f);
        struct stat st; if (stat(db_path, &st) == 0) { db_last_mtime = st.st_mtime; db_last_size = st.st_size; }
    }
    thread_mutex_unlock(&db_mutex);
    return r;
}

int thumbdb_get(const char* key, char* buf, size_t buflen) {
    if (!db_inited) return -1;
    if (!key || !buf) return -1;
    if (ensure_index_uptodate() != 0) return -1;
    thread_mutex_lock(&db_mutex);
    char base[PATH_MAX]; int is_small = 0, is_large = 0; base[0] = '\0';
    thumbname_to_base_and_kind(key, base, sizeof(base), &is_small, &is_large);
    const char* e_val = ht_get(base);
    if (!e_val) { thread_mutex_unlock(&db_mutex); return 1; }
    char small_tok[8], large_tok[8], media[PATH_MAX]; small_tok[0] = large_tok[0] = media[0] = '\0';
    split_combined_val(e_val, small_tok, sizeof(small_tok), large_tok, sizeof(large_tok), media, sizeof(media));
    if (media[0] == '\0') { thread_mutex_unlock(&db_mutex); return 1; }
    strncpy(buf, media, buflen - 1); buf[buflen - 1] = '\0';
    thread_mutex_unlock(&db_mutex);
    return 0;
}

int thumbdb_delete(const char* key) {
    if (!db_inited) return -1;
    if (!key) return -1;
    thread_mutex_lock(&db_mutex);
    char base[PATH_MAX]; int is_small = 0, is_large = 0; base[0] = '\0';
    thumbname_to_base_and_kind(key, base, sizeof(base), &is_small, &is_large);

    ht_entry_t* exist = ht_find(base);
    char small_tok[8] = "null"; char large_tok[8] = "null"; char media[PATH_MAX] = "";
    if (exist && exist->val) split_combined_val(exist->val, small_tok, sizeof(small_tok), large_tok, sizeof(large_tok), media, sizeof(media));

    if (is_small) strncpy(small_tok, "null", sizeof(small_tok) - 1);
    if (is_large) strncpy(large_tok, "null", sizeof(large_tok) - 1);

    int both_null = (strcmp(small_tok, "null") == 0 && strcmp(large_tok, "null") == 0);

    if (tx_active) {
        if (both_null) {
            int r = tx_record_op(base, NULL);
            thread_mutex_unlock(&db_mutex);
            return r;
        }
        char combined[LINE_MAX]; make_combined_val(combined, sizeof(combined), small_tok, large_tok, media);
        int r = tx_record_op(base, combined);
        thread_mutex_unlock(&db_mutex);
        return r;
    }

    if (both_null) {
        ht_set_internal(base, NULL);
    }
    else {
        char combined[LINE_MAX]; make_combined_val(combined, sizeof(combined), small_tok, large_tok, media);
        ht_set_internal(base, combined);
    }

    FILE* f = fopen(db_path, db_binary_mode ? "ab" : "a");
    if (f) {
        if (!db_binary_mode) {
            if (both_null) {
                char lastval[LINE_MAX]; lastval[0] = '\0';
                if (last_text_value_for_key(db_path, base, lastval, sizeof(lastval)) != 0 || lastval[0] != '\0') append_line_to_file(f, base, NULL);
            }
            else {
                char combined[LINE_MAX]; make_combined_val(combined, sizeof(combined), small_tok, large_tok, media);
                char lastval[LINE_MAX]; lastval[0] = '\0';
                if (last_text_value_for_key(db_path, base, lastval, sizeof(lastval)) != 0 || strcmp(lastval, combined) != 0) append_line_to_file(f, base, combined);
            }
        }
        else {
            if (both_null) append_line_to_file(f, base, NULL);
            else { char combined[LINE_MAX]; make_combined_val(combined, sizeof(combined), small_tok, large_tok, media); append_line_to_file(f, base, combined); }
        }
        fflush(f);
        platform_fsync(fileno(f));
        fclose(f);
        struct stat st; if (stat(db_path, &st) == 0) { db_last_mtime = st.st_mtime; db_last_size = st.st_size; }
    }
    thread_mutex_unlock(&db_mutex);
    return 0;
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
    FILE* f = fopen(db_path, db_binary_mode ? "wb" : "w");
    if (!f) { LOG_WARN("thumbdb: failed to open db for compaction: %s", db_path); thread_mutex_unlock(&db_mutex); return -1; }
    fprintf(f, "#version;1.0\n");
    if (!rh_tbl) { fclose(f); thread_mutex_unlock(&db_mutex); return -1; }
    struct comp_ctx cc;
    cc.f = f; cc.restrict_to_base = 0; cc.allowed_base[0] = '\0';
    char thumb_dir[PATH_MAX]; strncpy(thumb_dir, db_path, sizeof(thumb_dir) - 1); thumb_dir[sizeof(thumb_dir) - 1] = '\0';
    char* last = strrchr(thumb_dir, DIR_SEP);
    if (last) *last = '\0'; else strncpy(thumb_dir, ".", sizeof(thumb_dir) - 1);
    char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    size_t trl = strlen(thumbs_root);
    if (trl > 0 && strncmp(thumb_dir, thumbs_root, trl) == 0) {
        const char* sub = thumb_dir + trl;
        while (*sub == DIR_SEP) sub++;
        if (*sub) {
            char safe_dir[PATH_MAX]; strncpy(safe_dir, sub, sizeof(safe_dir) - 1); safe_dir[sizeof(safe_dir) - 1] = '\0';
            char* s_last = strrchr(safe_dir, DIR_SEP);
            if (s_last) memmove(safe_dir, s_last + 1, strlen(s_last + 1) + 1);
            size_t gf_count = 0; char** gfolders = get_gallery_folders(&gf_count);
            for (size_t i = 0; i < gf_count; ++i) {
                char folder_real[PATH_MAX]; if (!real_path(gfolders[i], folder_real)) continue;
                char sf[PATH_MAX]; sf[0] = '\0'; make_safe_dir_name_from(folder_real, sf, sizeof(sf));
                if (strcmp(sf, safe_dir) == 0) {
                    strncpy(cc.allowed_base, folder_real, sizeof(cc.allowed_base) - 1);
                    cc.allowed_base[sizeof(cc.allowed_base) - 1] = '\0';
                    cc.restrict_to_base = 1;
                    break;
                }
            }
        }
    }

    rh_iterate(rh_tbl, rh_comp_cb, &cc);

    if (cc.restrict_to_base && cc.wrote_count == 0) {
        char per_thumbs_root[PATH_MAX];
        char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
        char safe_dir[PATH_MAX]; make_safe_dir_name_from(cc.allowed_base, safe_dir, sizeof(safe_dir));
        snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir);
        if (is_dir(per_thumbs_root)) {
            diriter tit;
            if (dir_open(&tit, per_thumbs_root)) {
                const char* tname;
                while ((tname = dir_next(&tit))) {
                    if (!tname) continue;
                    if (!strstr(tname, "-small.") && !strstr(tname, "-large.")) continue;
                    char base[PATH_MAX]; int is_small = 0, is_large = 0; base[0] = '\0';
                    thumbname_to_base_and_kind(tname, base, sizeof(base), &is_small, &is_large);
                    if (!base[0]) continue;
                    diriter mit;
                    if (!dir_open(&mit, cc.allowed_base)) continue;
                    const char* mname;
                    while ((mname = dir_next(&mit))) {
                        if (!mname) continue;
                        if (!has_ext(mname, IMAGE_EXTS) && !has_ext(mname, VIDEO_EXTS)) continue;
                        char media_full[PATH_MAX]; path_join(media_full, cc.allowed_base, mname);
                        char small_rel[PATH_MAX], large_rel[PATH_MAX]; small_rel[0] = large_rel[0] = '\0';
                        get_thumb_rel_names(media_full, mname, small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));
                        if (strcmp(small_rel, tname) == 0 || strcmp(large_rel, tname) == 0) {
                            char small_tok[8] = "null"; char large_tok[8] = "null"; char media[PATH_MAX] = "";
                            char small_path[PATH_MAX]; char large_path[PATH_MAX];
                            snprintf(small_path, sizeof(small_path), "%s" DIR_SEP_STR "%s", per_thumbs_root, small_rel);
                            snprintf(large_path, sizeof(large_path), "%s" DIR_SEP_STR "%s", per_thumbs_root, large_rel);
                            if (is_file(small_path)) strncpy(small_tok, "small", sizeof(small_tok) - 1);
                            if (is_file(large_path)) strncpy(large_tok, "large", sizeof(large_tok) - 1);
                            strncpy(media, media_full, sizeof(media) - 1); media[sizeof(media) - 1] = '\0';
                            char combined[LINE_MAX]; make_combined_val(combined, sizeof(combined), small_tok, large_tok, media);
                            append_line_to_file(f, base, combined);
                            cc.wrote_count++;
                            break;
                        }
                    }
                    dir_close(&mit);
                }
                dir_close(&tit);
            }
        }
    }
    fflush(f);
    platform_fsync(fileno(f));
    fclose(f);
    LOG_INFO("thumbdb: compaction completed");
    thread_mutex_unlock(&db_mutex);
    return 0;
}

static void sweep_cb(const char* key, const char* value, void* ctx) {
    (void)ctx;
    if (!value || value[0] == '\0') return;
    char small_tok[8], large_tok[8], media[PATH_MAX]; small_tok[0] = large_tok[0] = media[0] = '\0';
    split_combined_val(value, small_tok, sizeof(small_tok), large_tok, sizeof(large_tok), media, sizeof(media));
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

        char small_tok[8], large_tok[8], media[PATH_MAX]; small_tok[0] = large_tok[0] = media[0] = '\0';
        split_combined_val(value, small_tok, sizeof(small_tok), large_tok, sizeof(large_tok), media, sizeof(media));
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
    FILE* f = fopen(db_path, db_binary_mode ? "ab" : "a");
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
    thread_mutex_unlock(&db_mutex);

    for (size_t i = 0; i < del_count; ++i) free(dels[i]); free(dels);

    thumbdb_compact();

    return 0;
}
