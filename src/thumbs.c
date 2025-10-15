#include "thumbs.h"
#include "directory.h"
#include "logging.h"
#include "utils.h"
#include "platform.h"
#include "crypto.h"
#include "thread_pool.h"
#include "api_handlers.h"

#ifndef MAX_FFMPEG
#define MAX_FFMPEG 2
#endif

atomic_int ffmpeg_active = ATOMIC_VAR_INIT(0);

static void sleep_ms(int ms) { platform_sleep_ms(ms); }

typedef struct watcher_node { char dir[PATH_MAX]; struct watcher_node* next; } watcher_node_t;
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

/* has_media_rec is in api_handlers.c */

int dir_has_missing_thumbs(const char* dir, int videos_only) {
    diriter it;
    if (!dir_open(&it, dir)) return 0;
    const char* name;
    while ((name = dir_next(&it))) {
        if (!strcmp(name, ".") || !strcmp(name, "..") || !strcmp(name, "thumbs")) continue;
        char full[PATH_MAX]; path_join(full, dir, name);
        if (is_dir(full)) continue;
        if (!(has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS))) continue;
        if (videos_only && !has_ext(name, VIDEO_EXTS)) continue;
        char small_rel[PATH_MAX]; char large_rel[PATH_MAX];
        get_thumb_rel_names(full, name, small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));
        char dirbuf[PATH_MAX]; get_parent_dir(full, dirbuf, sizeof(dirbuf));
        char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
        char small_fs[PATH_MAX]; char large_fs[PATH_MAX];
        snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", thumbs_root, small_rel);
        snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", thumbs_root, large_rel);
        if (!is_file(small_fs) || is_newer(full, small_fs)) { dir_close(&it); return 1; }
        if (!is_file(large_fs) || is_newer(full, large_fs)) { dir_close(&it); return 1; }
    }
    dir_close(&it);
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
    thread_args_t* targ = malloc(sizeof(thread_args_t));
    if (!targ) return;
    strncpy(targ->dir_path, dir, PATH_MAX - 1);
    targ->dir_path[PATH_MAX - 1] = '\0';
    if (thread_create_detached((void* (*)(void*))thumbnail_generation_thread, targ) != 0) {
        LOG_ERROR("Failed to spawn generation thread from watcher for %s", dir);
        free(targ);
    }
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
    char thumbs_root[PATH_MAX]; snprintf(thumbs_root, sizeof(thumbs_root), "%s" DIR_SEP_STR "thumbs", BASE_DIR);
    if (!is_dir(thumbs_root)) platform_make_dir(thumbs_root);
    strncpy(prog.thumbs_dir, thumbs_root, PATH_MAX - 1); prog.thumbs_dir[PATH_MAX - 1] = '\0';

    char lock_path[PATH_MAX];
    char safe_dir_name[PATH_MAX];
    { size_t si = 0; for (size_t ii = 0; dir_used[ii] && si < sizeof(safe_dir_name) - 1; ++ii) { char ch = dir_used[ii]; safe_dir_name[si++] = (ch == '/' || ch == '\\') ? '_' : (isalnum((unsigned char)ch) ? ch : '_'); } safe_dir_name[si] = '\0'; }
    snprintf(lock_path, sizeof(lock_path), "%s" DIR_SEP_STR ".thumbs_%s.lock", thumbs_root, safe_dir_name);
    int lock_ret = platform_create_lockfile_exclusive(lock_path);
    if (lock_ret == 1) {
        LOG_INFO("Thumbnail generation already running for: %s", dir);
        return;
    }
    if (lock_ret != 0) {
        LOG_WARN("Failed to create lock file %s", lock_path);
        return;
    }
    count_media_in_dir(dir_used, &prog);
    LOG_INFO("Found %zu media files in %s", prog.total_files, dir_used);
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
    char thumbs_root[PATH_MAX]; snprintf(thumbs_root, sizeof(thumbs_root), "%s" DIR_SEP_STR "thumbs", BASE_DIR);
    char small_fs[PATH_MAX]; char large_fs[PATH_MAX];
    snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", thumbs_root, small_rel);
    snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", thumbs_root, large_rel);
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
    if (!dir_has_missing_thumbs(dir_path, 0)) {
        LOG_INFO("No missing thumbnails for: %s", dir_path);
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
    char cmd[PATH_MAX * 4];
    snprintf(cmd, sizeof(cmd), "ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 \"%s\" > %s 2>&1", path, platform_devnull());
    return system(cmd) == 0;
}

int is_decodable(const char* path) {
    char cmd[PATH_MAX * 3];
    snprintf(cmd, sizeof(cmd), "ffprobe -v error -i \"%s\" > %s 2>&1", path, platform_devnull());
    return system(cmd) == 0;
}

void generate_thumb_c(const char* input, const char* output, int scale, int q, int index, int total) {
    if (!is_valid_media(input) || !is_decodable(input)) {
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
    if (ext && (!strcasecmp(ext, ".gif") || !strcasecmp(ext, ".webp"))) {
#ifdef _WIN32
            snprintf(cmd, sizeof(cmd), "ffmpeg -y -threads 1 -i \"%s\" -vf scale=%d:-1 -vframes 1 -q:v %d \"%s\" > nul 2>&1", in_path, scale, q, out_path);
#else
        snprintf(cmd, sizeof(cmd), "ffmpeg -y -i \"%s\" -vf scale=%d:-1 -vframes 1 -q:v %d \"%s\" > /dev/null 2>&1", in_path, scale, q, out_path);
#endif
    }
    else {
#ifdef _WIN32
            snprintf(cmd, sizeof(cmd), "ffmpeg -y -threads 1 -i \"%s\" -vf scale=%d:-1 -vframes 1 -q:v %d \"%s\" > nul 2>&1", in_path, scale, q, out_path);
#else
        snprintf(cmd, sizeof(cmd), "ffmpeg -y -i \"%s\" -vf scale=%d:-1 -vframes 1 -q:v %d \"%s\" > /dev/null 2>&1", in_path, scale, q, out_path);
#endif
    }
    while (atomic_load(&ffmpeg_active) >= MAX_FFMPEG) sleep_ms(50);
    atomic_fetch_add(&ffmpeg_active, 1);
    int __ret = system(cmd);
    atomic_fetch_sub(&ffmpeg_active, 1);
    if (__ret != 0)
        LOG_WARN("[%d/%d] ffmpeg failed for %s -> %s", index, total, in_path, out_path);
}

int get_media_dimensions(const char* path, int* width, int* height) {
    if (!path || !width || !height) return 0;
    char cmd[PATH_MAX * 3];
    char tmp_path[PATH_MAX];
    strncpy(tmp_path, path, PATH_MAX - 1); tmp_path[PATH_MAX - 1] = '\0';
    normalize_path(tmp_path);
    snprintf(cmd, sizeof(cmd), "ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0:s=x \"%s\" 2> %s", tmp_path, platform_devnull());
    FILE* f = platform_popen(cmd, "r");
    if (!f) return 0;
    char buf[128];
    if (!fgets(buf, sizeof(buf), f)) { platform_pclose(f); return 0; }
    platform_pclose(f);
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
        char key[64];
        snprintf(key, sizeof(key), "%08lx%08lx", (unsigned long)st.st_mtime, (unsigned long)st.st_size);
        char thumb_small[PATH_MAX], thumb_large[PATH_MAX];
        char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    if (!is_dir(thumbs_root)) platform_make_dir(thumbs_root);
        snprintf(thumb_small, sizeof(thumb_small), "%s" DIR_SEP_STR "%s-small.jpg", thumbs_root, key);
        snprintf(thumb_large, sizeof(thumb_large), "%s" DIR_SEP_STR "%s-large.jpg", thumbs_root, key);
        struct stat st_media, st_small, st_large;
        int need_small = 0, need_large = 0;
        if (stat(full, &st_media) == 0) {
            if (!is_file(thumb_small) || stat(thumb_small, &st_small) != 0 || st_small.st_mtime < st_media.st_mtime) need_small = 1;
            if (!is_file(thumb_large) || stat(thumb_large, &st_large) != 0 || st_large.st_mtime < st_media.st_mtime) need_large = 1;
        } else {
            need_small = !is_file(thumb_small);
            need_large = !is_file(thumb_large);
        }
        if (need_small) {
            prog->processed_files++;
            generate_thumb_c(full, thumb_small, 320, 75, prog->processed_files, prog->total_files);
        }
        if (need_large) {
            generate_thumb_c(full, thumb_large, 1280, 85, prog->processed_files, prog->total_files);
        }
    }
    dir_close(&it);
}

void clean_orphan_thumbs(const char* dir, progress_t* prog) {
    char thumbs_path[PATH_MAX];
    path_join(thumbs_path, dir, "thumbs");
    if (!is_dir(thumbs_path)) return;
    diriter it;
    if (!dir_open(&it, thumbs_path)) return;
    const char* name;
    while ((name = dir_next(&it))) {
        if (!strcmp(name, ".") || !strcmp(name, "..") || !strcmp(name, "skipped.log") || !strcmp(name, ".nogallery") || !strcmp(name, ".thumbs.lock")) continue;
        if (!strstr(name, "-small.") && !strstr(name, "-large.")) continue;
        char thumb_full[PATH_MAX]; path_join(thumb_full, thumbs_path, name);
        bool found = false;
        diriter it2;
        if (!dir_open(&it2, dir)) continue;
        const char* mname;
        while ((mname = dir_next(&it2)) && !found) {
            if (!strcmp(mname, ".") || !strcmp(mname, "..") || !strcmp(mname, "thumbs")) continue;
            char media_full[PATH_MAX]; path_join(media_full, dir, mname);
            if (!is_file(media_full)) continue;
            if (!(has_ext(mname, IMAGE_EXTS) || has_ext(mname, VIDEO_EXTS))) continue;
            char small_rel[PATH_MAX]; char large_rel[PATH_MAX];
            get_thumb_rel_names(media_full, mname, small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));
            const char* sb = strrchr(small_rel, '/'); const char* lb = strrchr(large_rel, '/');
            const char* sbase = sb ? sb + 1 : small_rel;
            const char* lbase = lb ? lb + 1 : large_rel;
            if (strcmp(sbase, name) == 0 || strcmp(lbase, name) == 0) found = true;
        }
        dir_close(&it2);
        if (!found) {
            if (platform_file_delete(thumb_full) != 0) LOG_WARN("Failed to delete orphan thumb: %s", thumb_full);
            add_skip(prog, "ORPHAN_REMOVED", thumb_full);
        }
    }
    dir_close(&it);
}


