#include "thumbs.h"
#include "thumbdb.h"
#include "directory.h"
#include "logging.h"
#include "utils.h"
#include "platform.h"
#include "crypto.h"
#include "thread_pool.h"
#include "api_handlers.h"
#include "config.h"
#include <time.h>
#ifndef _WIN32
#include <strings.h>
#endif

#ifndef MAX_FFMPEG
#define MAX_FFMPEG 2
#endif

atomic_int ffmpeg_active = ATOMIC_VAR_INIT(0);

static void sleep_ms(int ms) { platform_sleep_ms(ms); }

static void escape_path_for_cmd(const char* src, char* dst, size_t dstlen) {
    if (!src || !dst || dstlen == 0) return;
    size_t di = 0;
#ifdef _WIN32
    if (di < dstlen - 1) dst[di++] = '"';
    for (size_t i = 0; src[i] && di + 2 < dstlen; ++i) {
        char c = src[i];
        if (c == '"') continue;
        if (c == '%') {
            if (di + 1 < dstlen) dst[di++] = '%'; else break;
        }
        dst[di++] = c;
    }
    if (di < dstlen - 1) dst[di++] = '"';
#else
    if (di < dstlen - 1) dst[di++] = '"';
    for (size_t i = 0; src[i] && di + 2 < dstlen; ++i) {
        char c = src[i];
        if (c == '\\' || c == '"' || c == '$' || c == '`') {
            if (di + 1 < dstlen) dst[di++] = '\\'; else break;
        }
        dst[di++] = c;
    }
    if (di < dstlen - 1) dst[di++] = '"';
#endif
    dst[di] = '\0';
}

static int is_path_safe(const char* path) {
    if (!path) return 0;
    for (size_t i = 0; path[i]; ++i) {
        unsigned char c = (unsigned char)path[i];
        if (c == '\n' || c == '\r') return 0;
        if (c < 32) return 0;
    }
    return 1;
}

typedef struct watcher_node { char dir[PATH_MAX]; int scheduled; struct watcher_node* next; } watcher_node_t;
static watcher_node_t* watcher_head = NULL;
static thread_mutex_t watcher_mutex;
static int watcher_mutex_inited = 0;

static void get_parent_dir(const char* path, char* out, size_t outlen) {
    if (!path || !out || outlen == 0) return;
    strncpy(out, path, outlen - 1);
    out[outlen - 1] = '\0';
    char* s1 = strrchr(out, '/');
    char* s2 = strrchr(out, '\\');
    char* last = NULL;
    if (s1 && s2) last = (s1 > s2) ? s1 : s2;
    else if (s1) last = s1;
    else if (s2) last = s2;
    if (last) *last = '\0';
    else { out[0] = '.'; out[1] = '\0'; }
}

static void strip_trailing_sep(char* p) {
    if (!p) return;
    size_t len = strlen(p);
    while (len > 0) {
#ifdef _WIN32
    if (p[len-1] == '\\' || p[len-1] == '/') { p[len-1] = '\0'; len--; continue; }
#else
    if (p[len-1] == '/') { p[len-1] = '\0'; len--; continue; }
#endif
    break;
    }
}

static int is_newer(const char* src, const char* dst);
static void print_skips(progress_t* prog);
static void count_media_in_dir(const char* dir, progress_t* prog);
static void ensure_thumbs_in_dir(const char* dir, progress_t* prog);
static void clean_orphan_thumbs(const char* dir, progress_t* prog);
#ifdef _WIN32
static unsigned __stdcall thumbnail_generation_thread(void* args);
#else
static void* thumbnail_generation_thread(void* args);
#endif

#ifdef _WIN32
static unsigned __stdcall debounce_generation_thread(void* args) {
#else
static void* debounce_generation_thread(void* args) {
#endif
    char* dir = (char*)args;
    if (!dir) return 0;
    platform_sleep_ms(500);
    start_background_thumb_generation(dir);
    thread_mutex_lock(&watcher_mutex);
    watcher_node_t* cur = watcher_head;
    while (cur) { if (strcmp(cur->dir, dir) == 0) { cur->scheduled = 0; break; } cur = cur->next; }
    thread_mutex_unlock(&watcher_mutex);
    free(dir);
#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}

void get_thumb_rel_names(const char* full_path, const char* filename, char* small_rel, size_t small_len, char* large_rel, size_t large_len) {
    char key[64]; key[0] = '\0'; struct stat st;
    uint8_t digest[MD5_DIGEST_LENGTH]; char md5hex[MD5_DIGEST_LENGTH * 2 + 1]; md5hex[0] = '\0';
    if (crypto_md5_file(full_path, digest) == 0) {
        for (size_t di = 0; di < MD5_DIGEST_LENGTH; ++di) snprintf(md5hex + (di * 2), 3, "%02x", digest[di]);
    }
    if (md5hex[0] != '\0') {
        snprintf(small_rel, small_len, "%s-small.jpg", md5hex);
        snprintf(large_rel, large_len, "%s-large.jpg", md5hex);
        return;
    }
    const char* fname = filename ? filename : full_path;
    const char* dot = strrchr(fname, '.'); char base_name[PATH_MAX]; char ext[64];
    if (dot) {
        size_t blen = (size_t)(dot - fname);
        if (blen >= sizeof(base_name)) blen = sizeof(base_name) - 1;
        memcpy(base_name, fname, blen); base_name[blen] = '\0';
        size_t elen = strlen(dot + 1); if (elen >= sizeof(ext)) elen = sizeof(ext) - 1; memcpy(ext, dot + 1, elen); ext[elen] = '\0';
    } else {
        strncpy(base_name, fname, sizeof(base_name) - 1); base_name[sizeof(base_name) - 1] = '\0';
        strncpy(ext, "jpg", sizeof(ext) - 1); ext[sizeof(ext) - 1] = '\0';
    }
    snprintf(small_rel, small_len, "%s-small.%s", base_name, ext);
    snprintf(large_rel, large_len, "%s-large.%s", base_name, ext);
}

void make_thumb_fs_paths(const char* media_full, const char* filename, char* small_fs_out, size_t small_fs_out_len, char* large_fs_out, size_t large_fs_out_len) {
    char small_rel[PATH_MAX]; char large_rel[PATH_MAX];
    get_thumb_rel_names(media_full, filename, small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));
    char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    char parent[PATH_MAX]; get_parent_dir(media_full, parent, sizeof(parent));
    char safe_dir_name[PATH_MAX]; safe_dir_name[0] = '\0'; { size_t si = 0; for (size_t ii = 0; parent[ii] && si < sizeof(safe_dir_name) - 1; ++ii) { char ch = parent[ii]; safe_dir_name[si++] = (ch == '/' || ch == '\\') ? '_' : (isalnum((unsigned char)ch) ? ch : '_'); } safe_dir_name[si] = '\0'; }
    char per_thumbs_root[PATH_MAX]; snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);
    if (!is_dir(per_thumbs_root)) platform_make_dir(per_thumbs_root);
    if (small_fs_out && small_fs_out_len > 0) snprintf(small_fs_out, small_fs_out_len, "%s" DIR_SEP_STR "%s", per_thumbs_root, small_rel);
    if (large_fs_out && large_fs_out_len > 0) snprintf(large_fs_out, large_fs_out_len, "%s" DIR_SEP_STR "%s", per_thumbs_root, large_rel);
}


int dir_has_missing_thumbs(const char* dir, int videos_only) {
    LOG_DEBUG("dir_has_missing_thumbs: scanning %s (videos_only=%d)", dir, videos_only);
    diriter it;
    if (!dir_open(&it, dir)) { LOG_DEBUG("dir_has_missing_thumbs: failed to open %s", dir); return 0; }
    const char* name;
    while ((name = dir_next(&it))) {
        if (!strcmp(name, ".") || !strcmp(name, "..") || !strcmp(name, "thumbs")) continue;
        char full[PATH_MAX]; path_join(full, dir, name);
        if (is_dir(full)) {
            if (strcmp(name, "thumbs") == 0) continue;
            if (dir_has_missing_thumbs(full, videos_only)) { dir_close(&it); return 1; }
            continue;
        }
        if (!(has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS))) continue;
        if (videos_only && !has_ext(name, VIDEO_EXTS)) continue;
        char small_rel[PATH_MAX]; char large_rel[PATH_MAX];
        get_thumb_rel_names(full, name, small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));
        char dirbuf[PATH_MAX]; get_parent_dir(full, dirbuf, sizeof(dirbuf));
    char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    char safe_dir_name[PATH_MAX]; safe_dir_name[0] = '\0';
    { size_t si = 0; for (size_t ii = 0; dirbuf[ii] && si < sizeof(safe_dir_name) - 1; ++ii) { char ch = dirbuf[ii]; safe_dir_name[si++] = (ch == '/' || ch == '\\') ? '_' : (isalnum((unsigned char)ch) ? ch : '_'); } safe_dir_name[si] = '\0'; }
    char per_thumbs_root[PATH_MAX]; snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);
    char small_fs[PATH_MAX]; char large_fs[PATH_MAX];
    snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, small_rel);
    snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, large_rel);
        LOG_DEBUG("dir_has_missing_thumbs: checking media=%s small=%s large=%s", full, small_fs, large_fs);
        int small_exists = is_file(small_fs);
        int large_exists = is_file(large_fs);
        struct stat st_media, st_small, st_large;
        int media_stat = platform_stat(full, &st_media);
        int small_stat = platform_stat(small_fs, &st_small);
        int large_stat = platform_stat(large_fs, &st_large);
        LOG_DEBUG("dir_has_missing_thumbs: media_stat=%d small_exists=%d small_stat=%d large_exists=%d large_stat=%d", media_stat, small_exists, small_stat, large_exists, large_stat);
        if (!small_exists) { LOG_DEBUG("dir_has_missing_thumbs: small thumb missing for %s", full); dir_close(&it); return 1; }
        if (media_stat == 0 && small_stat == 0 && st_small.st_mtime < st_media.st_mtime) { LOG_DEBUG("dir_has_missing_thumbs: small thumb older than media for %s", full); dir_close(&it); return 1; }
        if (!large_exists) { LOG_DEBUG("dir_has_missing_thumbs: large thumb missing for %s", full); dir_close(&it); return 1; }
        if (media_stat == 0 && large_stat == 0 && st_large.st_mtime < st_media.st_mtime) { LOG_DEBUG("dir_has_missing_thumbs: large thumb older than media for %s", full); dir_close(&it); return 1; }
    }
    dir_close(&it);
    LOG_DEBUG("dir_has_missing_thumbs: no missing thumbs found in %s", dir);
    return 0;
}

int dir_has_missing_thumbs_shallow(const char* dir, int videos_only) {
    LOG_DEBUG("dir_has_missing_thumbs_shallow: scanning %s (videos_only=%d)", dir, videos_only);
    diriter it;
    if (!dir_open(&it, dir)) { LOG_DEBUG("dir_has_missing_thumbs_shallow: failed to open %s", dir); return 0; }
    const char* name;
    while ((name = dir_next(&it))) {
        if (!strcmp(name, ".") || !strcmp(name, "..") || !strcmp(name, "thumbs")) continue;
        char full[PATH_MAX]; path_join(full, dir, name);
        if (is_dir(full)) continue; /* shallow - don't recurse */
        if (!(has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS))) continue;
        if (videos_only && !has_ext(name, VIDEO_EXTS)) continue;
        char small_rel[PATH_MAX]; char large_rel[PATH_MAX];
        get_thumb_rel_names(full, name, small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));
    char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    char safe_dir_name[PATH_MAX]; safe_dir_name[0] = '\0';
    { size_t si = 0; for (size_t ii = 0; dir[ii] && si < sizeof(safe_dir_name) - 1; ++ii) { char ch = dir[ii]; safe_dir_name[si++] = (ch == '/' || ch == '\\') ? '_' : (isalnum((unsigned char)ch) ? ch : '_'); } safe_dir_name[si] = '\0'; }
    char per_thumbs_root[PATH_MAX]; snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);
    char small_fs[PATH_MAX]; char large_fs[PATH_MAX];
    snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, small_rel);
    snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, large_rel);
        LOG_DEBUG("dir_has_missing_thumbs_shallow: checking media=%s small=%s large=%s", full, small_fs, large_fs);
        int small_exists = is_file(small_fs);
        int large_exists = is_file(large_fs);
        struct stat st_media, st_small, st_large;
        int media_stat = platform_stat(full, &st_media);
        int small_stat = platform_stat(small_fs, &st_small);
        int large_stat = platform_stat(large_fs, &st_large);
        if (!small_exists) { dir_close(&it); return 1; }
        if (media_stat == 0 && small_stat == 0 && st_small.st_mtime < st_media.st_mtime) { dir_close(&it); return 1; }
        if (!large_exists) { dir_close(&it); return 1; }
        if (media_stat == 0 && large_stat == 0 && st_large.st_mtime < st_media.st_mtime) { dir_close(&it); return 1; }
    }
    dir_close(&it);
    LOG_DEBUG("dir_has_missing_thumbs_shallow: no missing thumbs found in %s", dir);
    return 0;
}

static void remove_watcher_node(const char* dir) {
    thread_mutex_lock(&watcher_mutex);
    watcher_node_t* prev = NULL; watcher_node_t* cur = watcher_head;
    while (cur) {
        if (strcmp(cur->dir, dir) == 0) {
            if (prev) prev->next = cur->next; else watcher_head = cur->next;
            free(cur);
            break;
        }
        prev = cur; cur = cur->next;
    }
    thread_mutex_unlock(&watcher_mutex);
}

static void thumb_watcher_cb(const char* dir) {
    if (!dir) return;
    thread_mutex_lock(&watcher_mutex);
    watcher_node_t* cur = watcher_head;
    while (cur) {
        if (strcmp(cur->dir, dir) == 0) break;
        cur = cur->next;
    }
    if (!cur) {
        thread_mutex_unlock(&watcher_mutex);
        start_background_thumb_generation(dir);
        return;
    }
    if (cur->scheduled) {
        thread_mutex_unlock(&watcher_mutex);
        return;
    }
    cur->scheduled = 1;
    thread_mutex_unlock(&watcher_mutex);

    char* dcopy = strdup(dir);
    if (!dcopy) {
        thread_mutex_lock(&watcher_mutex);
        cur->scheduled = 0;
        thread_mutex_unlock(&watcher_mutex);
        return;
    }
    if (thread_create_detached((void* (*)(void*))debounce_generation_thread, (void*)dcopy) != 0) {
        LOG_ERROR("Failed to spawn debounced generation thread for %s", dir);
        free(dcopy);
        thread_mutex_lock(&watcher_mutex);
        cur->scheduled = 0;
        thread_mutex_unlock(&watcher_mutex);
        return;
    }
}

#ifdef _WIN32
static unsigned __stdcall thumb_maintenance_thread(void* args){
#else
static void* thumb_maintenance_thread(void* args){
#endif
    int interval=args?*((int*)args):300;
    int ival=interval;
    free(args);
    thumbdb_open();
    for(;;){
        platform_sleep_ms(1000*ival);
        LOG_INFO("Periodic thumb maintenance: running migration and orphan cleanup");
        size_t gf_count=0;
        char** gfolders=get_gallery_folders(&gf_count);
        if(gf_count==0)continue;
        for(size_t gi=0;gi<gf_count;++gi){
            char* gallery=gfolders[gi];
            /* Open per-gallery DB and thumbs dir */
            char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root,sizeof(thumbs_root));
            char safe_dir_name[PATH_MAX]; safe_dir_name[0] = '\0'; size_t sdi = 0; for (size_t ii = 0; gallery[ii] && sdi < sizeof(safe_dir_name) - 1; ++ii) { char ch = gallery[ii]; safe_dir_name[sdi++] = (ch == '/' || ch == '\\') ? '_' : (isalnum((unsigned char)ch) ? ch : '_'); } safe_dir_name[sdi] = '\0';
            char per_thumbs_root[PATH_MAX]; snprintf(per_thumbs_root,sizeof(per_thumbs_root),"%s" DIR_SEP_STR "%s",thumbs_root,safe_dir_name);
            if (!is_dir(per_thumbs_root)) platform_make_dir(per_thumbs_root);
            char per_db[PATH_MAX]; snprintf(per_db, sizeof(per_db), "%s" DIR_SEP_STR "thumbs.db", per_thumbs_root);
            thumbdb_open_for_dir(per_db);
            if(thumbdb_tx_begin()!=0)LOG_WARN("thumbs: failed to start tx for gallery %s",gallery);
            diriter it; if(!dir_open(&it,gallery)){thumbdb_tx_abort();continue;}
            const char* mname;
            while((mname=dir_next(&it))){
                if(!strcmp(mname,".")||!strcmp(mname,"..")||!strcmp(mname,"thumbs"))continue;
                char media_full[PATH_MAX]; path_join(media_full,gallery,mname);
                if(!is_file(media_full))continue;
                if(!(has_ext(mname,IMAGE_EXTS)||has_ext(mname,VIDEO_EXTS)))continue;
                char small_rel[PATH_MAX],large_rel[PATH_MAX];
                get_thumb_rel_names(media_full,mname,small_rel,sizeof(small_rel),large_rel,sizeof(large_rel));
                char desired_small[PATH_MAX],desired_large[PATH_MAX];
                snprintf(desired_small,sizeof(desired_small),"%s" DIR_SEP_STR "%s",per_thumbs_root,small_rel);
                snprintf(desired_large,sizeof(desired_large),"%s" DIR_SEP_STR "%s",per_thumbs_root,large_rel);
                (void)0; /* legacy migration removed */
            }
            dir_close(&it);
            ensure_thumbs_in_dir(gallery,NULL);
            clean_orphan_thumbs(gallery,NULL);
            if(thumbdb_tx_commit()!=0){
                LOG_WARN("thumbs: failed to commit tx for gallery %s, aborting",gallery);
                thumbdb_tx_abort();
            } else {
                /* sweep and compact this per-gallery DB */
                thumbdb_sweep_orphans();
                thumbdb_compact();
            }
        }
        thumbdb_sweep_orphans();
        thumbdb_compact();
    }
#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}

void start_periodic_thumb_maintenance(int interval_seconds) {
    int* arg = malloc(sizeof(int));
    if (!arg) return;
    *arg = interval_seconds > 0 ? interval_seconds : 300;
    thread_create_detached((void*(*)(void*))thumb_maintenance_thread, arg);
}

void start_auto_thumb_watcher(const char* dir_path) {
    if (!dir_path) return;
    if (!watcher_mutex_inited) { if (thread_mutex_init(&watcher_mutex) == 0) watcher_mutex_inited = 1; }
    thread_mutex_lock(&watcher_mutex);
    watcher_node_t* cur = watcher_head;
    while (cur) { if (strcmp(cur->dir, dir_path) == 0) { thread_mutex_unlock(&watcher_mutex); return; } cur = cur->next; }
    watcher_node_t* node = malloc(sizeof(watcher_node_t));
    if (!node) { thread_mutex_unlock(&watcher_mutex); LOG_ERROR("Failed to allocate watcher node"); return; }
    strncpy(node->dir, dir_path, PATH_MAX - 1); node->dir[PATH_MAX - 1] = '\0'; node->next = watcher_head; watcher_head = node;
    node->scheduled = 0;
    thread_mutex_unlock(&watcher_mutex);

    if (platform_start_dir_watcher(dir_path, thumb_watcher_cb) != 0) {
        LOG_ERROR("Failed to create watcher for %s", dir_path);
        remove_watcher_node(dir_path);
    }
}

void run_thumb_generation(const char* dir) {
    progress_t prog;
    memset(&prog, 0, sizeof(prog));
    char dir_real[PATH_MAX];
    const char* dir_used = dir;
    if (real_path(dir, dir_real)) dir_used = dir_real;
    char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    if (!is_dir(thumbs_root)) platform_make_dir(thumbs_root);
    char safe_dir_name[PATH_MAX]; { size_t si = 0; for (size_t ii = 0; dir_used[ii] && si < sizeof(safe_dir_name) - 1; ++ii) { char ch = dir_used[ii]; safe_dir_name[si++] = (ch == '/' || ch == '\\') ? '_' : (isalnum((unsigned char)ch) ? ch : '_'); } safe_dir_name[si] = '\0'; }
    char per_thumbs_root[PATH_MAX]; snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);
    if (!is_dir(per_thumbs_root)) platform_make_dir(per_thumbs_root);
    strncpy(prog.thumbs_dir, per_thumbs_root, PATH_MAX - 1); prog.thumbs_dir[PATH_MAX - 1] = '\0';

    char lock_path[PATH_MAX];
    snprintf(lock_path, sizeof(lock_path), "%s" DIR_SEP_STR ".thumbs.lock", per_thumbs_root);
    int lock_ret = platform_create_lockfile_exclusive(lock_path);
    if (lock_ret == 1) {
        /* try to read PID from lockfile and remove if PID not running or file is stale */
        struct stat st; if (platform_stat(lock_path, &st) == 0) {
            time_t now = time(NULL);
            const time_t STALE_SECONDS = 300;
            int owner_pid = 0;
            FILE* lf = fopen(lock_path, "r");
            if (lf) {
                if (fscanf(lf, "%d", &owner_pid) != 1) owner_pid = 0;
                fclose(lf);
            }
            int owner_alive = 0;
            if (owner_pid > 0) owner_alive = platform_pid_is_running(owner_pid);
            if (!owner_alive) {
                LOG_WARN("Lockfile %s owned by dead PID %d or unreadable; removing", lock_path, owner_pid);
                platform_file_delete(lock_path);
                lock_ret = platform_create_lockfile_exclusive(lock_path);
            } else if (now > st.st_mtime && (now - st.st_mtime) > STALE_SECONDS) {
                LOG_WARN("Stale lock file detected %s age=%llds (owner pid %d still alive), removing", lock_path, (long long)(now - st.st_mtime), owner_pid);
                platform_file_delete(lock_path);
                lock_ret = platform_create_lockfile_exclusive(lock_path);
            }
        }
    }
    if (lock_ret == 1) {
        LOG_INFO("Thumbnail generation already running for: %s", dir);
        return;
    }
    if (lock_ret != 0) {
        LOG_WARN("Failed to create lock file %s", lock_path);
        return;
    }
    LOG_DEBUG("run_thumb_generation: acquired lock %s", lock_path);
    char per_db[PATH_MAX]; snprintf(per_db, sizeof(per_db), "%s" DIR_SEP_STR "thumbs.db", per_thumbs_root);
    int tbr = thumbdb_open_for_dir(per_db);
    if (tbr != 0) {
        LOG_WARN("run_thumb_generation: thumbdb_open_for_dir failed for %s (rc=%d)", per_db, tbr);
    } else {
        LOG_DEBUG("run_thumb_generation: opened DB %s", per_db);
    }
    count_media_in_dir(dir_used, &prog);
    LOG_INFO("Found %zu media files in %s", prog.total_files, dir_used);
    LOG_DEBUG("run_thumb_generation: calling ensure_thumbs_in_dir for %s (found %zu)", dir_used, prog.total_files);
    ensure_thumbs_in_dir(dir_used, &prog);
    clean_orphan_thumbs(dir_used, &prog);

    print_skips(&prog);

    platform_file_delete(lock_path);

}


bool check_thumb_exists(const char* media_path, char* thumb_path, size_t thumb_path_len) {
    char small_rel[PATH_MAX]; char large_rel[PATH_MAX];
    char dirbuf[PATH_MAX]; char filename[PATH_MAX];
    strncpy(dirbuf, media_path, PATH_MAX - 1); dirbuf[PATH_MAX - 1] = '\0';
    char* last_slash = strrchr(dirbuf, '/');
    char* last_bslash = strrchr(dirbuf, '\\');
    char* last = NULL;
    if (last_slash && last_bslash) last = (last_slash > last_bslash) ? last_slash : last_bslash;
    else if (last_slash) last = last_slash;
    else if (last_bslash) last = last_bslash;
    if (last) {
        strncpy(filename, last + 1, PATH_MAX - 1); filename[PATH_MAX - 1] = '\0';
        *last = '\0';
    } else {
        strncpy(filename, dirbuf, PATH_MAX - 1); filename[PATH_MAX - 1] = '\0';
        dirbuf[0] = '.'; dirbuf[1] = '\0';
    }
    get_thumb_rel_names(media_path, filename, small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));
    char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    char safe_dir_name[PATH_MAX]; safe_dir_name[0] = '\0';
    { size_t si = 0; for (size_t ii = 0; dirbuf[ii] && si < sizeof(safe_dir_name) - 1; ++ii) { char ch = dirbuf[ii]; safe_dir_name[si++] = (ch == '/' || ch == '\\') ? '_' : (isalnum((unsigned char)ch) ? ch : '_'); } safe_dir_name[si] = '\0'; }
    char per_thumbs_root[PATH_MAX]; snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);
    char small_fs[PATH_MAX]; char large_fs[PATH_MAX];
    snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, small_rel);
    snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, large_rel);
    if (thumb_path_len > 0) {
        snprintf(thumb_path, thumb_path_len, "%s", small_rel);
    }
    if (is_file(small_fs)) {
        if (thumb_path_len > 0) snprintf(thumb_path, thumb_path_len, "%s", small_rel);
        return true;
    }
    if (is_file(large_fs)) {
        if (thumb_path_len > 0) snprintf(thumb_path, thumb_path_len, "%s", large_rel);
        return true;
    }
    return false;
}

void start_background_thumb_generation(const char* dir_path) {
    LOG_DEBUG("start_background_thumb_generation: checking for missing thumbs (shallow) in %s", dir_path);
    if (!dir_has_missing_thumbs_shallow(dir_path, 0)) {
        LOG_INFO("No missing thumbnails (shallow) for: %s", dir_path);
        start_auto_thumb_watcher(dir_path);
        return;
    }

    thread_args_t* args = malloc(sizeof(thread_args_t));
    if (!args) {
        LOG_ERROR("Failed to allocate memory for thread arguments");
        return;
    }
    strncpy(args->dir_path, dir_path, PATH_MAX - 1);
    args->dir_path[PATH_MAX - 1] = '\0';
    if (thread_create_detached((void* (*)(void*))thumbnail_generation_thread, args) != 0) {
        LOG_ERROR("Failed to create thumbnail generation thread for %s", dir_path);
        free(args);
    }
    start_auto_thumb_watcher(dir_path);
}

    #ifdef _WIN32
    static unsigned __stdcall thumbnail_generation_thread(void* args) {
        thread_args_t* thread_args = (thread_args_t*)args;
        char dir_path[PATH_MAX];
        strncpy(dir_path, thread_args->dir_path, PATH_MAX - 1);
        dir_path[PATH_MAX - 1] = '\0';
        free(args);
        LOG_INFO("Background thumbnail generation starting for: %s", dir_path);
        run_thumb_generation(dir_path);
        LOG_INFO("Background thumbnail generation finished for: %s", dir_path);
        return 0;
    }
    #else
    static void* thumbnail_generation_thread(void* args) {
        thread_args_t* thread_args = (thread_args_t*)args;
        char dir_path[PATH_MAX];
        strncpy(dir_path, thread_args->dir_path, PATH_MAX - 1);
        dir_path[PATH_MAX - 1] = '\0';
        free(args);
        LOG_INFO("Background thumbnail generation starting for: %s", dir_path);
        run_thumb_generation(dir_path);
        LOG_INFO("Background thumbnail generation finished for: %s", dir_path);
        return NULL;
    }
    #endif

void add_skip(progress_t* prog, const char* reason, const char* path) {
    char log_path[PATH_MAX]; path_join(log_path, prog->thumbs_dir, "skipped.log");
    FILE* f = fopen(log_path, "a");
    if (f) { fprintf(f, "[%s] %s\n", reason, path);fclose(f); }
    skip_counter_t* curr = prog->skip_head;
    while (curr) { if (strcmp(curr->dir, prog->thumbs_dir) == 0)break;curr = curr->next; }
    if (!curr) {
        curr = malloc(sizeof(skip_counter_t));
        strncpy(curr->dir, prog->thumbs_dir, PATH_MAX - 1);curr->dir[PATH_MAX - 1] = '\0';
        curr->count = 0;curr->next = prog->skip_head;prog->skip_head = curr;
    }
    curr->count++;
}

void print_skips(progress_t* prog) {
    skip_counter_t* curr = prog->skip_head;
    while (curr) {
        if (curr->count > 0)
            printf("[SKIPPED] %d files skipped in thumbs_dir: %s\n", curr->count, curr->dir);
        skip_counter_t* tmp = curr;curr = curr->next;free(tmp);
    }
    prog->skip_head = NULL;
}

int is_newer(const char* src, const char* dst) {
    struct stat s, d;
    if (platform_stat(src, &s) != 0) return 0;
    if (platform_stat(dst, &d) != 0) return 1;
    return s.st_mtime > d.st_mtime;
}

int is_valid_media(const char* path) {
    struct stat st;
    if (platform_stat(path, &st) != 0 || st.st_size < 16) return 0;
    if (!is_path_safe(path)) return 0;
    char esc_path[PATH_MAX*2]; escape_path_for_cmd(path, esc_path, sizeof(esc_path));
    char cmd[PATH_MAX * 3];
    snprintf(cmd, sizeof(cmd), "ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 %s", esc_path);
    FILE* f = platform_popen_direct(cmd, "r");
    if (!f) return 0;
    char buf[128]; int ok = 0;
    if (fgets(buf, sizeof(buf), f)) ok = 1;
    int status = platform_pclose_direct(f);
    return ok && status == 0;
}

int is_decodable(const char* path) {
    if (!is_path_safe(path)) return 0;
    char esc_path2[PATH_MAX*2]; escape_path_for_cmd(path, esc_path2, sizeof(esc_path2));
    char cmd[PATH_MAX * 3];
    snprintf(cmd, sizeof(cmd), "ffprobe -v error -i %s -show_entries format=duration -of default=nw=1:nk=1", esc_path2);
    FILE* f = platform_popen_direct(cmd, "r");
    if (!f) return 0;
    /* We don't need the output; rely on exit status */
    int status = platform_pclose_direct(f);
    return status == 0;
}

void generate_thumb_c(const char* input, const char* output, int scale, int q, int index, int total) {
    if (!is_path_safe(input) || !is_valid_media(input) || !is_decodable(input)) {
        LOG_WARN("[%d/%d] Invalid/undecodable media: %s", index, total, input);
        return;
    }
    LOG_INFO("[%d/%d] Processing: %s", index, total, input);
    
    char in_path[PATH_MAX]; char out_path[PATH_MAX];
    strncpy(in_path, input, PATH_MAX - 1); in_path[PATH_MAX - 1] = '\0';
    strncpy(out_path, output, PATH_MAX - 1); out_path[PATH_MAX - 1] = '\0';
    normalize_path(in_path);
    normalize_path(out_path);
    const char* ext = strrchr(in_path, '.');
    char cmd[PATH_MAX * 3];
    char dbg_log[PATH_MAX]; dbg_log[0] = '\0';
    {
        char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
        time_t now = time(NULL);
        snprintf(dbg_log, sizeof(dbg_log), "%s" DIR_SEP_STR "ffmpeg_%lld_%d.log", thumbs_root, (long long)now, index);
    }
    if (ext && (!strcasecmp(ext, ".gif") || !strcasecmp(ext, ".webp"))) {
#ifdef _WIN32
    snprintf(cmd, sizeof(cmd), "ffmpeg -y -threads 1 -i \"%s\" -vf scale=%d:-1 -vframes 1 -update 1 -q:v %d \"%s\" > nul 2> \"%s\"", in_path, scale, q, out_path, dbg_log);
#else
    snprintf(cmd, sizeof(cmd), "ffmpeg -y -i \"%s\" -vf scale=%d:-1 -vframes 1 -update 1 -q:v %d \"%s\" > /dev/null 2> \"%s\"", in_path, scale, q, out_path, dbg_log);
#endif
    }
    else {
#ifdef _WIN32
        snprintf(cmd, sizeof(cmd), "ffmpeg -y -threads 1 -i \"%s\" -vf scale=%d:-1 -vframes 1 -update 1 -q:v %d \"%s\" > nul 2> \"%s\"", in_path, scale, q, out_path, dbg_log);
#else
    snprintf(cmd, sizeof(cmd), "ffmpeg -y -i \"%s\" -vf scale=%d:-1 -vframes 1 -update 1 -q:v %d \"%s\" > /dev/null 2> \"%s\"", in_path, scale, q, out_path, dbg_log);
#endif
    }
    while (atomic_load(&ffmpeg_active) >= MAX_FFMPEG) sleep_ms(50);
    atomic_fetch_add(&ffmpeg_active, 1);
    int __ret = platform_run_command(cmd, 30);
    atomic_fetch_sub(&ffmpeg_active, 1);
    if (__ret != 0) {
        LOG_WARN("[%d/%d] ffmpeg failed for %s -> %s (rc=%d)", index, total, in_path, out_path, __ret);
        if (dbg_log[0] != '\0') LOG_DEBUG("ffmpeg stderr log: %s", dbg_log);
    }
}

int get_media_dimensions(const char* path, int* width, int* height) {
    if (!path || !width || !height) return 0;
    if (!is_path_safe(path)) return 0;
    char tmp_path[PATH_MAX];
    strncpy(tmp_path, path, PATH_MAX - 1); tmp_path[PATH_MAX - 1] = '\0';
    normalize_path(tmp_path);
    char esc_tmp[PATH_MAX*2]; escape_path_for_cmd(tmp_path, esc_tmp, sizeof(esc_tmp));
    char cmd[PATH_MAX * 3];
    snprintf(cmd, sizeof(cmd), "ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0:s=x %s", esc_tmp);
    FILE* f = platform_popen_direct(cmd, "r");
    if (!f) return 0;
    char buf[128];
    if (!fgets(buf, sizeof(buf), f)) { platform_pclose_direct(f); return 0; }
    platform_pclose_direct(f);
    int w = 0, h = 0;
    if (sscanf(buf, "%dx%d", &w, &h) != 2) return 0;
    if (w <= 0 || h <= 0) return 0;
    *width = w; *height = h;
    return 1;
}

void count_media_in_dir(const char* dir, progress_t* prog) {
    diriter it; if (!dir_open(&it, dir)) return;
    const char* name;
    while ((name = dir_next(&it))) {
        if (!strcmp(name, ".") || !strcmp(name, "..") || !strcmp(name, "thumbs")) continue;
        char full[PATH_MAX]; path_join(full, dir, name);
        if (is_file(full) && (has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS))) {
            prog->total_files++;
        }
    }
    dir_close(&it);
}

void ensure_thumbs_in_dir(const char* dir, progress_t* prog) {
    LOG_DEBUG("ensure_thumbs_in_dir: enter for %s", dir);
    if (prog) LOG_DEBUG("ensure_thumbs_in_dir: prog total_files=%zu processed=%zu", prog->total_files, prog->processed_files);
    diriter it;
    if (!dir_open(&it, dir)) return;

    const char* name;
    while ((name = dir_next(&it))) {
        if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) continue;

        char full[PATH_MAX];
        path_join(full, dir, name);

        if (is_dir(full)) {
            continue;
        }
        const char* ext = strrchr(name, '.');
        if (!ext) continue;
        bool is_media = has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS);
        if (!is_media) continue;
        struct stat st;
        if (stat(full, &st) != 0) {
            add_skip(prog, "STAT_FAIL", full);
            continue;
        }
    char thumb_small_rel[PATH_MAX], thumb_large_rel[PATH_MAX];
    get_thumb_rel_names(full, name, thumb_small_rel, sizeof(thumb_small_rel), thumb_large_rel, sizeof(thumb_large_rel));
    char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    char safe_dir_name[PATH_MAX]; safe_dir_name[0] = '\0'; { size_t si = 0; for (size_t ii = 0; dir[ii] && si < sizeof(safe_dir_name) - 1; ++ii) { char ch = dir[ii]; safe_dir_name[si++] = (ch == '/' || ch == '\\') ? '_' : (isalnum((unsigned char)ch) ? ch : '_'); } safe_dir_name[si] = '\0'; }
    char per_thumbs_root[PATH_MAX]; snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);
    if (!is_dir(per_thumbs_root)) platform_make_dir(per_thumbs_root);
    char thumb_small[PATH_MAX], thumb_large[PATH_MAX];
    snprintf(thumb_small, sizeof(thumb_small), "%s" DIR_SEP_STR "%s", per_thumbs_root, thumb_small_rel);
    snprintf(thumb_large, sizeof(thumb_large), "%s" DIR_SEP_STR "%s", per_thumbs_root, thumb_large_rel);
        struct stat st_media, st_small, st_large;
        int need_small = 0, need_large = 0;
        if (platform_stat(full, &st_media) == 0) {
            if (!is_file(thumb_small) || platform_stat(thumb_small, &st_small) != 0 || st_small.st_mtime < st_media.st_mtime) need_small = 1;
            if (!is_file(thumb_large) || platform_stat(thumb_large, &st_large) != 0 || st_large.st_mtime < st_media.st_mtime) need_large = 1;
        } else {
            need_small = !is_file(thumb_small);
            need_large = !is_file(thumb_large);
        }
        LOG_DEBUG("ensure_thumbs_in_dir: media=%s need_small=%d need_large=%d", full, need_small, need_large);
        if (need_small) {
            prog->processed_files++;
            LOG_INFO("Generating small thumb for %s -> %s", full, thumb_small);
            generate_thumb_c(full, thumb_small, 320, 75, prog->processed_files, prog->total_files);
            {
                char *bn = strrchr(thumb_small, DIR_SEP);
                if (bn) bn = bn + 1; else bn = thumb_small;
                thumbdb_set(bn, full);
            }
        }
        if (need_large) {
            LOG_INFO("Generating large thumb for %s -> %s", full, thumb_large);
            generate_thumb_c(full, thumb_large, 1280, 85, prog->processed_files, prog->total_files);
            {
                char *bn = strrchr(thumb_large, DIR_SEP);
                if (bn) bn = bn + 1; else bn = thumb_large;
                thumbdb_set(bn, full);
            }
        }
    }
    dir_close(&it);
}

void clean_orphan_thumbs(const char* dir, progress_t* prog) {
    if (!dir) return;
    char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    char safe_dir_name[PATH_MAX]; safe_dir_name[0] = '\0'; { size_t si = 0; for (size_t ii = 0; dir[ii] && si < sizeof(safe_dir_name) - 1; ++ii) { char ch = dir[ii]; safe_dir_name[si++] = (ch == '/' || ch == '\\') ? '_' : (isalnum((unsigned char)ch) ? ch : '_'); } safe_dir_name[si] = '\0'; }
    char thumbs_path[PATH_MAX]; snprintf(thumbs_path, sizeof(thumbs_path), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);
    if (!is_dir(thumbs_path)) return;
    diriter mit;
    if (!dir_open(&mit, dir)) return;
    size_t expect_cap = 256;
    size_t expect_count = 0;
    char** expects = malloc(expect_cap * sizeof(char*));
    const char* mname;
    while ((mname = dir_next(&mit))) {
        if (!strcmp(mname, ".") || !strcmp(mname, "..") || !strcmp(mname, "thumbs")) continue;
        char media_full[PATH_MAX]; path_join(media_full, dir, mname);
        if (!is_file(media_full)) continue;
        if (!(has_ext(mname, IMAGE_EXTS) || has_ext(mname, VIDEO_EXTS))) continue;
        char small_rel[PATH_MAX]; char large_rel[PATH_MAX];
        get_thumb_rel_names(media_full, mname, small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));
        if (expect_count + 2 > expect_cap) {
            size_t nc = expect_cap * 2; char** tmp = realloc(expects, nc * sizeof(char*)); if (!tmp) break; expects = tmp; expect_cap = nc;
        }
        expects[expect_count++] = strdup(small_rel);
        expects[expect_count++] = strdup(large_rel);
    }
    dir_close(&mit);
    diriter tit;
    if (!dir_open(&tit, thumbs_path)) {
        for (size_t i = 0; i < expect_count; ++i) free(expects[i]); free(expects); return;
    }
    const char* tname;
    char tname_copy[PATH_MAX];
    while ((tname = dir_next(&tit))) {
        if (!tname) continue;
        strncpy(tname_copy, tname, sizeof(tname_copy) - 1);
        tname_copy[sizeof(tname_copy) - 1] = '\0';
        if (!strcmp(tname_copy, ".") || !strcmp(tname_copy, "..") || !strcmp(tname_copy, "skipped.log") || !strcmp(tname_copy, ".nogallery") || !strcmp(tname_copy, ".thumbs.lock")) continue;
        if (!strstr(tname_copy, "-small.") && !strstr(tname_copy, "-large.")) continue;
        bool found = false;
        for (size_t ei = 0; ei < expect_count; ++ei) {
#ifdef _WIN32
            if (_stricmp(tname, expects[ei]) == 0) { found = true; break; }
#else
            if (strcmp(tname, expects[ei]) == 0) { found = true; break; }
#endif
        }
        if (!found) {
            char thumb_full[PATH_MAX]; path_join(thumb_full, thumbs_path, tname_copy);
            char *bn_del = strrchr(tname_copy, DIR_SEP);
            if (bn_del) bn_del = bn_del + 1; else bn_del = tname_copy;
            /* Only delete thumbs that the thumb DB maps to media inside the provided gallery directory.
               This avoids removing thumbs that belong to other galleries that share the same global thumbs folder. */
            char mapped_media[PATH_MAX];
            /* Ensure we're operating on the per-gallery DB */
            char per_db[PATH_MAX]; snprintf(per_db, sizeof(per_db), "%s" DIR_SEP_STR "thumbs.db", thumbs_path);
            thumbdb_open_for_dir(per_db);
            if (thumbdb_get(bn_del, mapped_media, sizeof(mapped_media)) == 0) {
                size_t dlen = strlen(dir);
                if (strncmp(mapped_media, dir, dlen) == 0 && (mapped_media[dlen] == '\0' || mapped_media[dlen] == '/' || mapped_media[dlen] == '\\')) {
                    thumbdb_delete(bn_del);
                    if (platform_file_delete(thumb_full) != 0) {
                        LOG_WARN("Failed to delete orphan thumb: %s", thumb_full);
                    } else {
                        LOG_INFO("Removed orphan thumb: %s", thumb_full);
                    }
                    add_skip(prog, "ORPHAN_REMOVED", thumb_full);
                } else {
                    LOG_DEBUG("Skipping thumb %s mapped to other gallery media: %s", thumb_full, mapped_media);
                }
            } else {
                LOG_DEBUG("Skipping thumb with no db mapping: %s", thumb_full);
            }
        }
    }
    dir_close(&tit);
    for (size_t i = 0; i < expect_count; ++i) free(expects[i]); free(expects);
}


