#include "thumbdb.h"
#include "platform.h"
#include "logging.h"
#include "utils.h"
#include "directory.h"
#include "thread_pool.h"
#include "crypto.h"
#include "common.h"

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

static ht_entry_t** ht = NULL;
static size_t ht_buckets = 0;
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

static uint32_t ht_hash(const char* s) {

    uint32_t h = 2166136261u;
    while (*s) { h ^= (unsigned char)*s++; h *= 16777619u; }
    return h;
}

static int ht_ensure(size_t buckets) {
    if (ht) return 0;
    ht = calloc(buckets, sizeof(ht_entry_t*));
    if (!ht) return -1;
    ht_buckets = buckets;
    return 0;
}

static ht_entry_t* ht_find(const char* key) {
    if (!ht) return NULL;
    uint32_t h = ht_hash(key) % ht_buckets;
    ht_entry_t* e = ht[h];
    while (e) { if (strcmp(e->key, key) == 0) return e; e = e->next; }
    return NULL;
}

static int ht_set_internal(const char* key, const char* val) {
    if (!ht) return -1;
    uint32_t h = ht_hash(key) % ht_buckets;
    ht_entry_t* e = ht[h];
    while (e) {
        if (strcmp(e->key, key) == 0) {
            free(e->val);
            e->val = val ? strdup(val) : NULL;
            return 0;
        }
        e = e->next;
    }
    ht_entry_t* ne = calloc(1, sizeof(ht_entry_t));
    if (!ne) return -1;
    ne->key = strdup(key);
    ne->val = val ? strdup(val) : NULL;
    ne->next = ht[h]; ht[h] = ne;
    return 0;
}

static void ht_free_all(void) {
    if (!ht) return;
    for (size_t i = 0; i < ht_buckets; ++i) {
        ht_entry_t* e = ht[i];
        while (e) { ht_entry_t* nx = e->next; free(e->key); free(e->val); free(e); e = nx; }
        ht[i] = NULL;
    }
    free(ht); ht = NULL; ht_buckets = 0;
}

static int append_line_to_file(FILE* f, const char* key, const char* val) {
    if (!f) return -1;
    if (val) {
        if (fprintf(f, "%s\t%s\n", key, val) < 0) return -1;
    }
    else {
        if (fprintf(f, "%s\t\n", key) < 0) return -1;
    }
    return 0;
}

static int persist_tx_ops(tx_op_t* ops) {
    if (!ops) return 0;
    FILE* f = fopen(db_path, "a");
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
    char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    char global_db[PATH_MAX]; snprintf(global_db, sizeof(global_db), "%s" DIR_SEP_STR DB_FILENAME, thumbs_root);
    return thumbdb_open_for_dir(global_db);
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

    f = fopen(db_path, "r");
    if (!f) { db_inited = 1; return 0; }
    char line[LINE_MAX];
    while (fgets(line, sizeof(line), f)) {
        char* nl = strchr(line, '\n'); if (nl) *nl = '\0';
        char* tab = strchr(line, '\t'); if (!tab) continue; *tab = '\0'; char* key = line; char* val = tab + 1;
        if (val[0] == '\0') {
            ht_set_internal(key, NULL);
        }
        else {
            ht_set_internal(key, val);
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
    db_inited = 1;
    LOG_INFO("thumbdb: opened %s (buckets=%zu)", db_path, ht_buckets);
    thumbdb_sweep_orphans();
    thumbdb_compact();
    return 0;
}

static int rebuild_index_locked(void) {
    ht_free_all();
    if (ht_ensure(INITIAL_BUCKETS) != 0) return -1;
    FILE* f = fopen(db_path, "r");
    if (!f) return -1;
    char line[LINE_MAX];
    while (fgets(line, sizeof(line), f)) {
        char* nl = strchr(line, '\n'); if (nl) *nl = '\0';
        char* tab = strchr(line, '\t'); if (!tab) continue; *tab = '\0'; char* key = line; char* val = tab + 1;
        if (val[0] == '\0') ht_set_internal(key, NULL); else ht_set_internal(key, val);
    }
    fclose(f);
    struct stat st;
    if (stat(db_path, &st) == 0) { db_last_mtime = st.st_mtime; db_last_size = st.st_size; }
    else { db_last_mtime = 0; db_last_size = 0; }
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
static void* rebuild_worker(void* arg) {
    (void)arg;
    struct stat st;
    if (stat(db_path, &st) != 0) {
        return NULL;
    }
    size_t new_buckets = INITIAL_BUCKETS;
    ht_entry_t** new_ht = calloc(new_buckets, sizeof(ht_entry_t*));
    if (!new_ht) {
        return NULL;
    }
    FILE* f = fopen(db_path, "r");
    if (!f) {
        free(new_ht);
        return NULL;
    }
    char line[LINE_MAX];
    while (fgets(line, sizeof(line), f)) {
        char* nl = strchr(line, '\n'); if (nl) *nl = '\0';
        char* tab = strchr(line, '\t'); if (!tab) continue; *tab = '\0'; char* key = line; char* val = tab + 1;
        uint32_t h = ht_hash(key) % new_buckets;
        ht_entry_t* ne = calloc(1, sizeof(ht_entry_t));
        if (!ne) continue;
        ne->key = strdup(key);
        ne->val = val[0] ? strdup(val) : NULL;
        ne->next = new_ht[h]; new_ht[h] = ne;
    }
    fclose(f);

    thread_mutex_lock(&db_mutex);
    ht_entry_t** old_ht = ht;
    size_t old_buckets = ht_buckets;
    ht = new_ht;
    ht_buckets = new_buckets;
    db_last_mtime = st.st_mtime;
    db_last_size = st.st_size;
    db_rebuilding = 0;
    thread_mutex_unlock(&db_mutex);

    free_ht_table(old_ht, old_buckets);
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
    if (!db_inited) {
        if (thumbdb_open() != 0) return -1;
    }
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
    FILE* f = fopen(db_path, "a");
    if (!f) {
        LOG_WARN("thumbdb: failed to open db for append during commit");
        thread_mutex_unlock(&db_mutex);
        return -1;
    }
    cur = tx_head;
    while (cur) {
        if (append_line_to_file(f, cur->key, cur->val) != 0) {
            fclose(f);
            LOG_WARN("thumbdb: failed to write tx op during commit");
            thread_mutex_unlock(&db_mutex);
            return -1;
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
    tx_op_t* op = calloc(1, sizeof(tx_op_t)); if (!op) return -1;
    op->key = strdup(key);
    op->val = val ? strdup(val) : NULL;
    op->next = NULL;
    if (!tx_head) tx_head = op; else { tx_op_t* cur = tx_head; while (cur->next) cur = cur->next; cur->next = op; }
    return 0;
}

int thumbdb_set(const char* key, const char* value) {
    if (!db_inited) { if (thumbdb_open() != 0) return -1; }
    if (!key) return -1;
    thread_mutex_lock(&db_mutex);
    if (tx_active) {
        int r = tx_record_op(key, value);
        thread_mutex_unlock(&db_mutex);
        return r;
    }
    int r = ht_set_internal(key, value);
    FILE* f = fopen(db_path, "a");
    if (f) {
        append_line_to_file(f, key, value); fflush(f);
        platform_fsync(fileno(f));
        fclose(f);
        struct stat st; if (stat(db_path, &st) == 0) { db_last_mtime = st.st_mtime; db_last_size = st.st_size; }
    }
    thread_mutex_unlock(&db_mutex);
    return r;
}

int thumbdb_get(const char* key, char* buf, size_t buflen) {
    if (!db_inited) { if (thumbdb_open() != 0) return -1; }
    if (!key || !buf) return -1;
    if (ensure_index_uptodate() != 0) return -1;
    thread_mutex_lock(&db_mutex);
    ht_entry_t* e = ht_find(key);
    if (!e || !e->val) { thread_mutex_unlock(&db_mutex); return 1; }
    strncpy(buf, e->val, buflen - 1); buf[buflen - 1] = '\0';
    thread_mutex_unlock(&db_mutex);
    return 0;
}

int thumbdb_delete(const char* key) {
    if (!db_inited) { if (thumbdb_open() != 0) return -1; }
    if (!key) return -1;
    thread_mutex_lock(&db_mutex);
    if (tx_active) {
        int r = tx_record_op(key, NULL);
        thread_mutex_unlock(&db_mutex);
        return r;
    }
    ht_set_internal(key, NULL);
    FILE* f = fopen(db_path, "a");
    if (f) {
        append_line_to_file(f, key, NULL); fflush(f);
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
    for (size_t i = 0; i < ht_buckets; ++i) {
        ht_entry_t* e = ht[i];
        while (e) { cb(e->key, e->val, ctx); e = e->next; }
    }
    thread_mutex_unlock(&db_mutex);
}

int thumbdb_compact(void) {
    if (!db_inited) return -1;
    if (ensure_index_uptodate() != 0) return -1;
    thread_mutex_lock(&db_mutex);
    char tmp[PATH_MAX]; snprintf(tmp, sizeof(tmp), "%s.tmp", db_path);
    FILE* f = fopen(tmp, "w");
    if (!f) { thread_mutex_unlock(&db_mutex); return -1; }
    for (size_t i = 0; i < ht_buckets; ++i) {
        ht_entry_t* e = ht[i];
        while (e) {
            if (e->key && e->val) append_line_to_file(f, e->key, e->val);
            e = e->next;
        }
    }
    fflush(f);
    platform_fsync(fileno(f));
    fclose(f);
    platform_file_delete(db_path);
    if (rename(tmp, db_path) != 0) { LOG_WARN("thumbdb: compaction rename failed"); thread_mutex_unlock(&db_mutex); return -1; }
    LOG_INFO("thumbdb: compaction completed");
    thread_mutex_unlock(&db_mutex);
    return 0;
}

static void sweep_cb(const char* key, const char* value, void* ctx) {
    (void)ctx;
    if (!value || value[0] == '\0') return;
    if (!is_file(value)) {
        char thumb_dir[PATH_MAX];
        thumb_dir[0] = '\0';
        if (db_path[0]) {
            strncpy(thumb_dir, db_path, sizeof(thumb_dir) - 1);
            thumb_dir[sizeof(thumb_dir) - 1] = '\0';
            char* last = strrchr(thumb_dir, DIR_SEP);
            if (last) *last = '\0';
            else strncpy(thumb_dir, ".", sizeof(thumb_dir) - 1);
        }
        else {
            get_thumbs_root(thumb_dir, sizeof(thumb_dir));
        }

        char thumb_path[PATH_MAX];
        bool removed = false;

        snprintf(thumb_path, sizeof(thumb_path), "%s" DIR_SEP_STR "%s", thumb_dir, key);
        if (is_file(thumb_path)) {
            if (platform_file_delete(thumb_path) == 0) {
                LOG_DEBUG("thumbdb: removed thumb %s because media missing: %s", thumb_path, value);
                removed = true;
            }
            else {
                LOG_WARN("thumbdb: failed to remove thumb %s", thumb_path);
            }
        }

        if (!removed) {
            char thumbs_root[PATH_MAX];
            get_thumbs_root(thumbs_root, sizeof(thumbs_root));
            snprintf(thumb_path, sizeof(thumb_path), "%s" DIR_SEP_STR "%s", thumbs_root, key);
            if (is_file(thumb_path)) {
                if (platform_file_delete(thumb_path) == 0) {
                    LOG_DEBUG("thumbdb: removed thumb %s because media missing: %s", thumb_path, value);
                    removed = true;
                } else LOG_WARN("thumbdb: failed to remove thumb %s", thumb_path);
            }
            else {
                diriter dit;
                if (dir_open(&dit, thumbs_root)) {
                    const char* dname;
                    while ((dname = dir_next(&dit))) {
                        if (!strcmp(dname, ".") || !strcmp(dname, "..")) continue;
                        char sub[PATH_MAX];
                        path_join(sub, thumbs_root, dname);
                        if (!is_dir(sub)) continue;
                        snprintf(thumb_path, sizeof(thumb_path), "%s" DIR_SEP_STR "%s", sub, key);
                        if (is_file(thumb_path)) {
                            if (platform_file_delete(thumb_path) == 0) {
                                LOG_DEBUG("thumbdb: removed thumb %s because media missing: %s", thumb_path, value);
                                removed = true;
                            } else LOG_WARN("thumbdb: failed to remove thumb %s", thumb_path);
                            break;
                        }
                    }
                    dir_close(&dit);
                }
            }
        }

        thumbdb_delete(key);
    }
}

int thumbdb_sweep_orphans(void) {
    if (!db_inited) { if (thumbdb_open() != 0) return -1; }

    if (ensure_index_uptodate() != 0) return -1;
    thread_mutex_lock(&db_mutex);
    size_t cap = 128; size_t count = 0;
    struct kv { char* key; char* val; } *arr = malloc(cap * sizeof(*arr));
    if (!arr) { thread_mutex_unlock(&db_mutex); return -1; }
    for (size_t i = 0; i < ht_buckets; ++i) {
        ht_entry_t* e = ht[i];
        while (e) {
            if (count + 1 > cap) {
                size_t nc = cap * 2;
                struct kv* tmp = realloc(arr, nc * sizeof(*arr));
                if (!tmp) break;
                arr = tmp; cap = nc;
            }
            arr[count].key = e->key ? strdup(e->key) : NULL;
            arr[count].val = e->val ? strdup(e->val) : NULL;
            if (!arr[count].key) {
                for (size_t j = 0; j < count; ++j) { free(arr[j].key); free(arr[j].val); }
                free(arr); thread_mutex_unlock(&db_mutex); return -1;
            }
            count++;
            e = e->next;
        }
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
        } else {
            get_thumbs_root(thumb_dir, sizeof(thumb_dir));
        }

        char thumb_path[PATH_MAX];
        snprintf(thumb_path, sizeof(thumb_path), "%s" DIR_SEP_STR "%s", thumb_dir, key);
        if (!is_file(value)) {
            if (is_file(thumb_path)) {
                if (platform_file_delete(thumb_path) == 0) {
                    LOG_DEBUG("thumbdb: removed thumb %s because media missing: %s", thumb_path, value);
                    removed = true;
                } else LOG_WARN("thumbdb: failed to remove thumb %s", thumb_path);
            }

            if (!removed) {
                char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
                snprintf(thumb_path, sizeof(thumb_path), "%s" DIR_SEP_STR "%s", thumbs_root, key);
                if (is_file(thumb_path)) {
                    if (platform_file_delete(thumb_path) == 0) {
                        LOG_DEBUG("thumbdb: removed thumb %s because media missing: %s", thumb_path, value);
                        removed = true;
                    } else LOG_WARN("thumbdb: failed to remove thumb %s", thumb_path);
                } else {
                    diriter dit; if (dir_open(&dit, thumbs_root)) {
                        const char* dname;
                        while ((dname = dir_next(&dit))) {
                            if (!strcmp(dname, ".") || !strcmp(dname, "..")) continue;
                            char sub[PATH_MAX]; path_join(sub, thumbs_root, dname);
                            if (!is_dir(sub)) continue;
                            snprintf(thumb_path, sizeof(thumb_path), "%s" DIR_SEP_STR "%s", sub, key);
                            if (is_file(thumb_path)) {
                                if (platform_file_delete(thumb_path) == 0) {
                                    LOG_DEBUG("thumbdb: removed thumb %s because media missing: %s", thumb_path, value);
                                    removed = true; break;
                                } else LOG_WARN("thumbdb: failed to remove thumb %s", thumb_path);
                            }
                        }
                        dir_close(&dit);
                    }
                }
            }

            if (del_count + 1 > del_cap) {
                size_t nc = del_cap * 2; char** tmp = realloc(dels, nc * sizeof(char*)); if (!tmp) { }
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
    FILE* f = fopen(db_path, "a");
    if (f) {
        for (size_t i = 0; i < del_count; ++i) {
            append_line_to_file(f, dels[i], NULL);
        }
        fflush(f);
        platform_fsync(fileno(f));
        fclose(f);
        struct stat st; if (stat(db_path, &st) == 0) { db_last_mtime = st.st_mtime; db_last_size = st.st_size; }
    } else {
        LOG_WARN("thumbdb: failed to open db for appending orphan deletions");
    }
    thread_mutex_unlock(&db_mutex);

    for (size_t i = 0; i < del_count; ++i) free(dels[i]); free(dels);

    thumbdb_compact();

    return 0;
}
