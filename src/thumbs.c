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
#include "common.h"
#include "websocket.h"
atomic_int ffmpeg_active = ATOMIC_VAR_INIT(0);
static atomic_int magick_active = ATOMIC_VAR_INIT(0);
#define MAX_MAGICK 2
#define WAL_DIR_NAME "wal"
#define WAL_CHUNK_FMT "chunk-%lld-%u.wal"
static atomic_uint wal_chunk_seq = ATOMIC_VAR_INIT(0);
typedef struct {
    char input[PATH_MAX];
    char output[PATH_MAX];
    int scale;
    int q;
    int index;
    int total;
} thumb_job_t;
static void record_thumb_job_completion(const thumb_job_t* job);
static void run_thumb_job(thumb_job_t* job);
static void generate_thumb_inline_and_record(const char* input, const char* output, int scale, int q, int index, int total);
static void sleep_ms(int ms) { platform_sleep_ms(ms); }
static atomic_int thumb_workers_active = ATOMIC_VAR_INIT(0);

static void wait_for_thumb_workers(void) {
    while (atomic_load(&thumb_workers_active) > 0)
        sleep_ms(50);
}

static void build_wal_dir_path(const char* per_thumbs_root, char* wal_dir, size_t wal_dir_len) {
    if (!per_thumbs_root || !wal_dir || wal_dir_len == 0) return;
    snprintf(wal_dir, wal_dir_len, "%s" DIR_SEP_STR WAL_DIR_NAME, per_thumbs_root);
}

static int wal_write_entry(const char* per_thumbs_root, const char* key, const char* value) {
    if (!per_thumbs_root || !key) return -1;
    char wal_dir[PATH_MAX];
    wal_dir[0] = '\0';
    build_wal_dir_path(per_thumbs_root, wal_dir, sizeof(wal_dir));
    if (!is_dir(wal_dir)) platform_make_dir(wal_dir);
    unsigned int seq = atomic_fetch_add(&wal_chunk_seq, 1);
    long long ts = (long long)time(NULL);
    char chunk_path[PATH_MAX];
    if (ts < 0) ts = 0;
    snprintf(chunk_path, sizeof(chunk_path), "%s" DIR_SEP_STR WAL_CHUNK_FMT, wal_dir, ts, seq);
    FILE* f = fopen(chunk_path, "w");
    if (!f) return -1;
    fprintf(f, "%s\n%s\n", key, value ? value : "");
    fflush(f);
    platform_fsync(fileno(f));
    fclose(f);
    return 0;
}

static int wal_read_entry(const char* chunk_path, char* key, size_t key_len, char* value, size_t value_len) {
    if (!chunk_path || !key || key_len == 0 || !value || value_len == 0) return -1;
    FILE* f = fopen(chunk_path, "r");
    if (!f) return -1;
    if (!fgets(key, key_len, f)) { fclose(f); return -1; }
    size_t key_end = strcspn(key, "\r\n");
    key[key_end] = '\0';
    if (!fgets(value, value_len, f)) {
        value[0] = '\0';
        fclose(f);
        return 0;
    }
    size_t val_end = strcspn(value, "\r\n");
    value[val_end] = '\0';
    fclose(f);
    return 0;
}

static void process_wal_chunks(const char* per_thumbs_root) {
    if (!per_thumbs_root) return;
    char wal_dir[PATH_MAX];
    wal_dir[0] = '\0';
    build_wal_dir_path(per_thumbs_root, wal_dir, sizeof(wal_dir));
    if (!is_dir(wal_dir)) return;
    diriter it;
    if (!dir_open(&it, wal_dir)) return;
    const char* entry;
    char chunk_path[PATH_MAX];
    char key[PATH_MAX];
    char value[PATH_MAX];
    while ((entry = dir_next(&it))) {
        if (!entry || strcmp(entry, "") == 0) continue;
        if (strcmp(entry, ".") == 0 || strcmp(entry, "..") == 0) continue;
        path_join(chunk_path, wal_dir, entry);
        if (!is_file(chunk_path)) continue;
        if (wal_read_entry(chunk_path, key, sizeof(key), value, sizeof(value)) != 0) continue;
        int committed = 0;
        for (int attempt = 0; attempt < 20; ++attempt) {
            if (thumbdb_tx_begin() != 0) {
                sleep_ms(5);
                continue;
            }
            if (thumbdb_set(key, value) == 0) {
                if (thumbdb_tx_commit() == 0) {
                    platform_file_delete(chunk_path);
                    thumbdb_request_compaction();
                    committed = 1;
                }
                else {
                    thumbdb_tx_abort();
                }
                break;
            }
            thumbdb_tx_abort();
            sleep_ms(5);
        }
        if (!committed) {
            LOG_WARN("process_wal_chunks: failed to commit WAL chunk %s", chunk_path);
        }
    }
    dir_close(&it);
}

static int execute_command_with_limits(const char* cmd, const char* out_log, int timeout, int uses_ffmpeg) {
    int ret;
    if (!cmd) return -1;

    size_t cmdlen = strlen(cmd);
    if (cmdlen == 0 || cmdlen > 4096) {
        LOG_ERROR("execute_command_with_limits: refusing to execute invalid or overly long command (len=%zu)", cmdlen);
        return -1;
    }

    for (size_t i = 0; i < cmdlen; ++i) {
        unsigned char c = (unsigned char)cmd[i];
        if (c < 32 || c > 126) {
            LOG_ERROR("execute_command_with_limits: rejecting command with non-printable char at index %zu", i);
            return -1;
        }
        switch (c) {
        case ';': case '&': case '|': case '`':
        case '$': case '>': case '<': case '!':
        case '{': case '}':
        case '\'':
            LOG_ERROR("execute_command_with_limits: rejecting potentially unsafe command character '%c'", c);
            return -1;
        }
    }

    if (uses_ffmpeg) {
        while (atomic_load(&ffmpeg_active) >= MAX_FFMPEG)
            sleep_ms(50);
        atomic_fetch_add(&ffmpeg_active, 1);
        const char* final_out = out_log ? out_log : platform_devnull();
        platform_record_command(cmd);
        LOG_DEBUG("execute_command_with_limits: executing: %s", cmd);
        ret = platform_run_command_redirect(cmd, final_out, timeout);
        LOG_DEBUG("execute_command_with_limits: command rc=%d cmd=%s", ret, cmd);
        atomic_fetch_sub(&ffmpeg_active, 1);
    }
    else {
        int is_magick = 0;
        if (strstr(cmd, "magick") != NULL)
            is_magick = 1;

        if (is_magick) {
            while (atomic_load(&magick_active) >= MAX_MAGICK)
                sleep_ms(50);
            atomic_fetch_add(&magick_active, 1);
            const char* final_out = out_log ? out_log : platform_devnull();
            platform_record_command(cmd);
            LOG_DEBUG("execute_command_with_limits: executing (magick): %s", cmd);
            ret = platform_run_command_redirect(cmd, final_out, timeout);
            LOG_DEBUG("execute_command_with_limits: magick rc=%d cmd=%s", ret, cmd);
            atomic_fetch_sub(&magick_active, 1);
        }
        else {
            platform_record_command(cmd);
            ret = platform_run_command_redirect(cmd, out_log ? out_log : platform_devnull(), timeout);
            LOG_DEBUG("execute_command_with_limits: command rc=%d cmd=%s", ret, cmd);
        }
    }

    return ret;
}

static void generate_thumb_c(const char* input, const char* output, int scale, int q, int index, int total);

static void build_magick_resize_cmd(char* dst, size_t dstlen, const char* in_esc, int scale, int q, const char* out_esc) {
    if (!dst || dstlen == 0)return;
    int threads = platform_get_cpu_count();
    if (threads < 1) threads = 1;
    long mem_mb = platform_get_physical_memory_mb();
    if (mem_mb <= 0) mem_mb = 512;
    long per_proc_mb = mem_mb / (MAX_MAGICK > 0 ? MAX_MAGICK : 1);
    if (per_proc_mb < 256) per_proc_mb = 256;
    snprintf(dst, dstlen, "magick -limit thread %d -limit memory %ldMB -limit map %ldMB %s -resize %dx -quality %d %s",
        threads, per_proc_mb, per_proc_mb, in_esc, scale, q, out_esc);
    LOG_DEBUG("Magick CMD: %s", dst);
}

static void build_ffmpeg_extract_jpg_cmd(char* dst, size_t dstlen, const char* in_esc, const char* tmp_esc, int scale) {
    if (!dst || dstlen == 0)return;
    int threads = platform_get_cpu_count(); if (threads < 1) threads = 1;
    snprintf(dst, dstlen, "ffmpeg -y -threads %d -i %s -vf \"scale=%d:-1\" -vframes 1 -f image2 -c:v mjpeg %s", threads, in_esc, scale, tmp_esc);
    LOG_DEBUG("FFmpeg Extract CMD: %s", dst);
}

static void build_ffmpeg_thumb_cmd(char* dst, size_t dstlen, const char* in_esc, int scale, int q, int to_webp, int add_format_rgb, const char* out_esc) {
    if (!dst || dstlen == 0)return;
    int threads = platform_get_cpu_count(); if (threads < 1) threads = 1;
    if (to_webp) {
        if (add_format_rgb)
            snprintf(dst, dstlen, "ffmpeg -y -threads %d -i %s -vf \"scale=%d:-1,format=rgb24\" -vframes 1 -q:v %d -c:v libwebp %s", threads, in_esc, scale, q, out_esc);
        else
            snprintf(dst, dstlen, "ffmpeg -y -threads %d -i %s -vf \"scale=%d:-1\" -vframes 1 -q:v %d -c:v libwebp %s", threads, in_esc, scale, q, out_esc);
    }
    else
        snprintf(dst, dstlen, "ffmpeg -y -threads %d -i %s -vf \"scale=%d:-1,format=rgb24\" -vframes 1 -q:v %d %s", threads, in_esc, scale, q, out_esc);
    LOG_DEBUG("FFmpeg Thumb CMD: %s", dst);
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

static void strip_trailing_sep(char* p) {
    if (!p) return;
    size_t len = strlen(p);
    while (len > 0 && (p[len - 1] == '/' || p[len - 1] == '\\')) {
        p[--len] = '\0';
    }
}
static void thumbname_to_base_local(const char* name, char* base, size_t base_len) {
    base[0] = '\0';
    if (!name || !base || base_len == 0) return;
    const char* p = strstr(name, "-small.");
    if (p) {
        size_t bl = (size_t)(p - name);
        if (bl >= base_len) bl = base_len - 1;
        memcpy(base, name, bl);
        base[bl] = '\0';
        return;
    }
    p = strstr(name, "-large.");
    if (p) {
        size_t bl = (size_t)(p - name);
        if (bl >= base_len) bl = base_len - 1;
        memcpy(base, name, bl);
        base[bl] = '\0';
        return;
    }
    strncpy(base, name, base_len - 1);
    base[base_len - 1] = '\0';
}
static void get_parent_dir(const char* path, char* out, size_t outlen) {
    if (!path || !out || outlen == 0) return;
    char tmp[PATH_MAX];
    strncpy(tmp, path, sizeof(tmp) - 1);
    tmp[sizeof(tmp) - 1] = '\0';
    normalize_path(tmp);
    strip_trailing_sep(tmp);
    char* s1 = strrchr(tmp, '/');
    char* s2 = strrchr(tmp, '\\');
    char* last = NULL;
    if (s1 && s2) last = (s1 > s2) ? s1 : s2;
    else if (s1) last = s1;
    else if (s2) last = s2;
    if (last) {
        size_t blen = (size_t)(last - tmp);
        if (blen >= outlen) blen = outlen - 1;
        memcpy(out, tmp, blen);
        out[blen] = '\0';
    }
    else {
        if (outlen > 1) {
            out[0] = '.'; out[1] = '\0';
        } else if (outlen > 0) {
            out[0] = '\0';
        }
    }
}
static int is_animated_webp(const char* path) {
    if (!path) return 0;

    FILE* f = fopen(path, "rb");
    if (!f) return 0;

    unsigned char hdr[30];
    if (fread(hdr, 1, sizeof(hdr), f) < 30) { fclose(f); return 0; }

    if (memcmp(hdr, "RIFF", 4) || memcmp(hdr + 8, "WEBP", 4)) { fclose(f); return 0; }

    unsigned char* chunk = hdr + 12;
    char t[5];
    memcpy(t, chunk, 4);
    t[4] = 0;
    if (!memcmp(t, "VP8X", 4)) {
        unsigned char flags = 0;
        if (fseek(f, 20, SEEK_SET) != 0) { fclose(f); return 0; }
        if (fread(&flags, 1, 1, f) != 1) { fclose(f); return 0; }
        fclose(f);
        return (flags & 0x02) ? 1 : 0;
    }

    fclose(f);
    return 0;
}
static int output_is_webp(const char* path) {
    if (!path) return 0;
    const char* dot = strrchr(path, '.');
    if (!dot) return 0;
    return (ascii_stricmp(dot, ".webp") == 0) ? 1 : 0;
}
void make_safe_dir_name_from(const char* dir, char* out, size_t outlen) {
    if (!dir || !out || outlen == 0) return;

    char tmp[PATH_MAX];
    strncpy(tmp, dir, sizeof(tmp) - 1);
    tmp[sizeof(tmp) - 1] = '\0';
    normalize_path(tmp);
    strip_trailing_sep(tmp);

    bool has_alpha = false;
    bool all_upper = true;
    for (size_t ii = 0; tmp[ii]; ++ii) {
        unsigned char uc = (unsigned char)tmp[ii];
        if (isalpha(uc)) {
            has_alpha = true;
            if (islower(uc)) { all_upper = false; break; }
        }
    }

    int to_lower = 1;
    if (has_alpha && all_upper) to_lower = 0;

    size_t si = 0;
    bool last_was_dash = false;

    for (size_t ii = 0; tmp[ii] && si < outlen - 1; ++ii) {
        unsigned char uc = (unsigned char)tmp[ii];
        char ch = (char)uc;
        if (ch == '/' || ch == '\\' || !isalnum(uc)) {
            if (!last_was_dash && si < outlen - 1) {
                out[si++] = '-';
                last_was_dash = true;
            }
        }
        else {
            if (to_lower) ch = (char)tolower(uc);
            out[si++] = ch;
            last_was_dash = false;
        }
    }
    while (si > 0 && out[si - 1] == '-') si--;
    out[si] = '\0';
}
static void record_thumb_job_completion(const thumb_job_t* job) {
    if (!job) return;
    const char* bn = strrchr(job->output, DIR_SEP);
    if (bn) bn = bn + 1;
    else bn = job->output;
    char base_key[PATH_MAX];
    thumbname_to_base_local(bn, base_key, sizeof(base_key));
    char normalized_input[PATH_MAX];
    strncpy(normalized_input, job->input, sizeof(normalized_input) - 1);
    normalized_input[sizeof(normalized_input) - 1] = '\0';
    normalize_path(normalized_input);
    char per_thumbs_root[PATH_MAX];
    per_thumbs_root[0] = '\0';
    get_parent_dir(job->output, per_thumbs_root, sizeof(per_thumbs_root));
    int wrote_wal = 0;
    if (wal_write_entry(per_thumbs_root, base_key, normalized_input) == 0) {
        wrote_wal = 1;
        LOG_DEBUG("record_thumb_job_completion: queued WAL %s -> %s", base_key, normalized_input);
    } else {
        LOG_WARN("record_thumb_job_completion: failed to write WAL entry for %s", job->input);
    }
    if (wrote_wal)
        thumbdb_request_compaction();
    char parent[PATH_MAX];
    parent[0] = '\0';
    get_parent_dir(job->input, parent, sizeof(parent));
    char msg[1024];
    int r = snprintf(msg, sizeof(msg), "{\"type\":\"thumb_ready\",\"media\":\"%s\",\"thumb\":\"%s\"}", job->input, bn);
    if (r > 0) websocket_broadcast_topic(parent[0] ? parent : NULL, msg);
}
static void run_thumb_job(thumb_job_t* job) {
    if (!job) return;
    generate_thumb_c(job->input, job->output, job->scale, job->q, job->index, job->total);
    record_thumb_job_completion(job);
}
static void generate_thumb_inline_and_record(const char* input, const char* output, int scale, int q, int index, int total) {
    thumb_job_t temp;
    memset(&temp, 0, sizeof(temp));
    strncpy(temp.input, input, PATH_MAX - 1);
    temp.input[PATH_MAX - 1] = '\0';
    strncpy(temp.output, output, PATH_MAX - 1);
    temp.output[PATH_MAX - 1] = '\0';
    temp.scale = scale;
    temp.q = q;
    temp.index = index;
    temp.total = total;
    run_thumb_job(&temp);
}
void get_thumb_rel_names(const char* full_path, const char* filename, char* small_rel, size_t small_len, char* large_rel, size_t large_len) {
    char key[64];
    key[0] = '\0';

    uint8_t digest[MD5_DIGEST_LENGTH];
    char md5hex[MD5_DIGEST_LENGTH * 2 + 1];
    md5hex[0] = '\0';

    if (crypto_md5_file(full_path, digest) == 0) {
        for (size_t di = 0; di < MD5_DIGEST_LENGTH; ++di)
            snprintf(md5hex + (di * 2), 3, "%02x", digest[di]);
    }

    if (md5hex[0] != '\0') {
        snprintf(small_rel, small_len, "%s-small.jpg", md5hex);
        snprintf(large_rel, large_len, "%s-large.jpg", md5hex);
        return;
    }

    const char* fname = filename ? filename : full_path;
    const char* dot = strrchr(fname, '.');
    char base_name[PATH_MAX];
    char ext_in[64];
    char ext_out[64];

    if (dot) {
        size_t blen = (size_t)(dot - fname);
        if (blen >= sizeof(base_name)) blen = sizeof(base_name) - 1;
        memcpy(base_name, fname, blen);
        base_name[blen] = '\0';

        size_t elen = strlen(dot + 1);
        if (elen >= sizeof(ext_in)) elen = sizeof(ext_in) - 1;
        memcpy(ext_in, dot + 1, elen);
        ext_in[elen] = '\0';
    }
    else {
        strncpy(base_name, fname, sizeof(base_name) - 1);
        base_name[sizeof(base_name) - 1] = '\0';
        strncpy(ext_in, "jpg", sizeof(ext_in) - 1);
        ext_in[sizeof(ext_in) - 1] = '\0';
    }

    if (ascii_stricmp(ext_in, "webp") == 0) {
        strncpy(ext_out, "webp", sizeof(ext_out) - 1);
        ext_out[sizeof(ext_out) - 1] = '\0';
    }
    else {
        strncpy(ext_out, "jpg", sizeof(ext_out) - 1);
        ext_out[sizeof(ext_out) - 1] = '\0';
    }

    snprintf(small_rel, small_len, "%s-small.%s", base_name, ext_out);
    snprintf(large_rel, large_len, "%s-large.%s", base_name, ext_out);
}
void get_thumb_rel_names_quick(const char* full_path, const char* filename, char* small_rel, size_t small_len, char* large_rel, size_t large_len) {
    const char* fname = filename ? filename : full_path;
    const char* dot = strrchr(fname, '.');
    char base_name[PATH_MAX];
    char ext_in[64];
    char ext_out[64];
    if (dot) {
        size_t blen = (size_t)(dot - fname);
        if (blen >= sizeof(base_name)) blen = sizeof(base_name) - 1;
        memcpy(base_name, fname, blen);
        base_name[blen] = '\0';
        size_t elen = strlen(dot + 1);
        if (elen >= sizeof(ext_in)) elen = sizeof(ext_in) - 1;
        memcpy(ext_in, dot + 1, elen);
        ext_in[elen] = '\0';
    }
    else {
        strncpy(base_name, fname, sizeof(base_name) - 1);
        base_name[sizeof(base_name) - 1] = '\0';
        strncpy(ext_in, "jpg", sizeof(ext_in) - 1);
        ext_in[sizeof(ext_in) - 1] = '\0';
    }
    if (ascii_stricmp(ext_in, "webp") == 0) strncpy(ext_out, "webp", sizeof(ext_out) - 1);
    else strncpy(ext_out, "jpg", sizeof(ext_out) - 1);
    ext_out[sizeof(ext_out) - 1] = '\0';
    snprintf(small_rel, small_len, "%s-small.%s", base_name, ext_out);
    snprintf(large_rel, large_len, "%s-large.%s", base_name, ext_out);
}
void make_thumb_fs_paths(const char* media_full, const char* filename, char* small_fs_out, size_t small_fs_out_len, char* large_fs_out, size_t large_fs_out_len) {
    char small_rel[PATH_MAX];
    char large_rel[PATH_MAX];

    get_thumb_rel_names(media_full, filename, small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));

    char thumbs_root[PATH_MAX];
    get_thumbs_root(thumbs_root, sizeof(thumbs_root));

    char parent[PATH_MAX];
    get_parent_dir(media_full, parent, sizeof(parent));

    char safe_dir_name[PATH_MAX];
    make_safe_dir_name_from(parent, safe_dir_name, sizeof(safe_dir_name));

    char per_thumbs_root[PATH_MAX];
    snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);

    if (!is_dir(per_thumbs_root))
        platform_make_dir(per_thumbs_root);

    if (small_fs_out && small_fs_out_len > 0)
        snprintf(small_fs_out, small_fs_out_len, "%s" DIR_SEP_STR "%s", per_thumbs_root, small_rel);

    if (large_fs_out && large_fs_out_len > 0)
        snprintf(large_fs_out, large_fs_out_len, "%s" DIR_SEP_STR "%s", per_thumbs_root, large_rel);
}
const char* get_file_ext(const char* path) {
    const char* dot = strrchr(path, '.');
    return dot ? dot + 1 : "";
}
const char* get_ffprobe_cmd_for_ext(const char* ext) {
    if (!ext || ext[0] == '\0') return NULL;
    static const char* video_exts[] = {
        "mp4", "mov", "webm", "mkv", "avi", NULL
    };
    for (size_t i = 0; video_exts[i]; ++i) {
        if (ascii_stricmp(ext, video_exts[i]) == 0) {
            return "ffprobe -nostdin -hwaccel none -v error -select_streams v:0 -show_entries stream=codec_name -of default=noprint_wrappers=1:nokey=1 %s";
        }
    }
    return NULL;
}
int is_decodable(const char* path) {
    if (!is_path_safe(path)) return 0;
    const char* ext = get_file_ext(path);
    const char* format_cmd = get_ffprobe_cmd_for_ext(ext);
    if (!format_cmd) {
        static const char* image_exts[] = {
            "jpg", "jpeg", "png", "gif", "webp", NULL
        };
        for (size_t i = 0; image_exts[i]; ++i) {
            if (ascii_stricmp(ext, image_exts[i]) == 0) return 1;
        }
        return 0;
    }
    char esc_path[PATH_MAX * 2];
    platform_escape_path_for_cmd(path, esc_path, sizeof(esc_path));
    char cmd[PATH_MAX * 3];
    snprintf(cmd, sizeof(cmd), format_cmd, esc_path);
    FILE* f = platform_popen_direct(cmd, "r");
    if (!f) return 0;
    char buf[64];
    int ok = 0;
    if (fgets(buf, sizeof(buf), f)) ok = 1;
    int status = platform_pclose_direct(f);
    return ok && status == 0;
}

int is_valid_media(const char* path) {
    struct stat st;
    if (platform_stat(path, &st) != 0) return 0;
    if (!is_path_safe(path)) return 0;
    const char* name = path;
    const char* dot = strrchr(name, '.');
    if (!dot) return 0;
    if (has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS)) return 1;
    return 0;
}
int is_newer(const char* src, const char* dst) {
    struct stat s, d;
    if (platform_stat(src, &s) != 0) return 0;
    if (platform_stat(dst, &d) != 0) return 1;
    return s.st_mtime > d.st_mtime;
}
static void generate_thumb_c(const char* input, const char* output, int scale, int q, int index, int total) {
    LOG_DEBUG("generate_thumb_c: enter input=%s output=%s scale=%d q=%d index=%d total=%d", input ? input : "(null)", output ? output : "(null)", scale, q, index, total);

    if (!is_path_safe(input)) {
        LOG_WARN("[%d/%d] Invalid path (unsafe): %s", index, total, input);
        return;
    }
    if (!is_valid_media(input)) {
        LOG_WARN("[%d/%d] Invalid media (stat/size) or not present: %s", index, total, input);
        return;
    }
    (void)0;

    LOG_DEBUG("[%d/%d] Processing: %s", index, total, input);

    char in_path[PATH_MAX], out_path[PATH_MAX];
    strncpy(in_path, input, PATH_MAX - 1); in_path[PATH_MAX - 1] = '\0';
    strncpy(out_path, output, PATH_MAX - 1); out_path[PATH_MAX - 1] = '\0';
    normalize_path(in_path); normalize_path(out_path);

    const char* ext = strrchr(in_path, '.');
    bool input_is_animated_webp = false;
    if (ext && ascii_stricmp(ext, ".webp") == 0) {
        input_is_animated_webp = is_animated_webp(in_path);
    }
    char in_path_with_frame[PATH_MAX];
    if (ext && ascii_stricmp(ext, ".gif") == 0) {
        snprintf(in_path_with_frame, sizeof(in_path_with_frame), "%s[0]", in_path);
    }
    else {
        strncpy(in_path_with_frame, in_path, sizeof(in_path_with_frame) - 1);
        in_path_with_frame[sizeof(in_path_with_frame) - 1] = '\0';
    }
    char esc_in[PATH_MAX * 2], esc_out[PATH_MAX * 2];
    esc_in[0] = '\0'; esc_out[0] = '\0';
    platform_escape_path_for_cmd(in_path, esc_in, sizeof(esc_in));
    platform_escape_path_for_cmd(out_path, esc_out, sizeof(esc_out));
    char esc_in_with_frame[PATH_MAX * 2];
    esc_in_with_frame[0] = '\0';
    platform_escape_path_for_cmd(in_path_with_frame, esc_in_with_frame, sizeof(esc_in_with_frame));
    if (ext && ascii_stricmp(ext, ".webp") == 0) {
        if (input_is_animated_webp) {
            LOG_DEBUG("[%d/%d] Animated webp detected, using ffmpeg extraction: %s", index, total, in_path);

            char tmp_jpg[PATH_MAX];
            snprintf(tmp_jpg, sizeof(tmp_jpg), "%s.tmp.jpg", out_path);

            char esc_tmp[PATH_MAX * 2];
            esc_tmp[0] = '\0';
            platform_escape_path_for_cmd(tmp_jpg, esc_tmp, sizeof(esc_tmp));
            char ffcmd[1024];
            build_ffmpeg_extract_jpg_cmd(ffcmd, sizeof(ffcmd), esc_in, esc_tmp, scale);

            LOG_DEBUG("generate_thumb_c: executing webp ffmpeg cmd: %s", ffcmd);
            int ret_png = execute_command_with_limits(ffcmd, NULL, 30, 1);

            if (ret_png == 0) {
                char convert_cmd[PATH_MAX * 3];
                if (output_is_webp(out_path) && input_is_animated_webp) {
                    snprintf(convert_cmd, sizeof(convert_cmd),
                        "ffmpeg -y -threads 1 -i \"%s\" -vf \"scale=%d:-1,format=rgb24\" -vframes 1 -q:v %d -c:v libwebp \"%s\"",
                        esc_tmp, scale, q, esc_out);
                }
                else {
                    build_magick_resize_cmd(convert_cmd, sizeof(convert_cmd), esc_tmp, scale, q, esc_out);
                }

                LOG_DEBUG("generate_thumb_c: executing png conversion cmd: %s", convert_cmd);
                int cret = execute_command_with_limits(convert_cmd, NULL, 20, 0);
                platform_file_delete(tmp_jpg);

                if (cret == 0) return;
                LOG_WARN("[%d/%d] conversion failed rc=%d", index, total, cret);
            }
            else {
                LOG_WARN("[%d/%d] ffmpeg (png) failed rc=%d", index, total, ret_png);
                platform_file_delete(tmp_jpg);
            }
        }
        LOG_DEBUG("[%d/%d] Using CPU/image commands for webp: %s", index, total, in_path);

        char magick_cmd[1024];
        build_magick_resize_cmd(magick_cmd, sizeof(magick_cmd), esc_in_with_frame, scale, q, esc_out);

        int mret = execute_command_with_limits(magick_cmd, NULL, 20, 0);
        if (mret == 0) {
            LOG_INFO("[%d/%d] magick succeeded for %s", index, total, in_path);
            return;
        }
        char magick_log[PATH_MAX];
        snprintf(magick_log, sizeof(magick_log), "%s.magick.log", out_path);
        int mret2 = execute_command_with_limits(magick_cmd, magick_log, 20, 0);

        if (mret2 == 0) {
            LOG_INFO("[%d/%d] magick succeeded on retry for %s", index, total, in_path);
            platform_file_delete(magick_log);
            return;
        }

        LOG_WARN("[%d/%d] magick failed for %s rc=%d (retry rc=%d) log=%s",
            index, total, in_path, mret, mret2, magick_log);
        return;
    }
    bool is_video = false;
    if (ext) {
        static const char* video_exts[] = {
            "mp4", "mov", "webm", "mkv", "avi", "m4v", "mpg", "mpeg", NULL
        };

        for (size_t i = 0; video_exts[i]; ++i) {
            if (ascii_stricmp(ext + 1, video_exts[i]) == 0) {
                is_video = true;
                break;
            }
        }
    }

    LOG_DEBUG("generate_thumb_c: ext=%s input_is_animated_webp=%d is_video=%d", ext ? ext : "(null)", input_is_animated_webp ? 1 : 0, is_video ? 1 : 0);

    char* cmd = NULL;

    if (is_video) {
        char tmp_jpg[PATH_MAX];
        snprintf(tmp_jpg, sizeof(tmp_jpg), "%s.tmp.jpg", out_path);

        char esc_tmp[PATH_MAX * 2];
        esc_tmp[0] = '\0';
        platform_escape_path_for_cmd(tmp_jpg, esc_tmp, sizeof(esc_tmp));

        {
            char extract_cmd[1024];
            build_ffmpeg_extract_jpg_cmd(extract_cmd, sizeof(extract_cmd), esc_in, esc_tmp, scale);
            LOG_DEBUG("generate_thumb_c: executing video extraction cmd: %s", extract_cmd);
            int ret_png = execute_command_with_limits(extract_cmd, NULL, 60, 1);

            if (ret_png == 0) {
                char convert_cmd[1024];
                if (output_is_webp(out_path) && input_is_animated_webp) {
                    build_ffmpeg_thumb_cmd(convert_cmd, sizeof(convert_cmd), esc_tmp, scale, q, 1, 1, esc_out);
                }
        else {
            build_magick_resize_cmd(convert_cmd, sizeof(convert_cmd), esc_tmp, scale, q, esc_out);
        }
        LOG_DEBUG("generate_thumb_c: executing video conversion cmd: %s", convert_cmd);
                int cret = execute_command_with_limits(convert_cmd, NULL, 20, 0);
                platform_file_delete(tmp_jpg);

                if (cret == 0) return;
                LOG_WARN("[%d/%d] conversion failed rc=%d", index, total, cret);
            }
            else {
                LOG_WARN("[%d/%d] ffmpeg jpg extraction failed rc=%d", index, total, ret_png);
                platform_file_delete(tmp_jpg);
            }
        }
    }
    {
        char final_cmd[2048];
        if (input_is_animated_webp) {
            int to_webp = output_is_webp(out_path) ? 1 : 0;
            int add_rgb = (ext && (ascii_stricmp(ext, ".gif") == 0 || ascii_stricmp(ext, ".png") == 0)) ? 1 : 0;
            build_ffmpeg_thumb_cmd(final_cmd, sizeof(final_cmd), esc_in, scale, q, to_webp, add_rgb, esc_out);
            LOG_DEBUG("generate_thumb_c: executing final ffmpeg cmd: %s", final_cmd);
            int ret = execute_command_with_limits(final_cmd, NULL, 30, 1);
            if (ret != 0) LOG_WARN("[%d/%d] ffmpeg failed rc=%d", index, total, ret);
        }
        else {
            build_magick_resize_cmd(final_cmd, sizeof(final_cmd), esc_in_with_frame, scale, q, esc_out);
            LOG_DEBUG("generate_thumb_c: executing final magick cmd: %s", final_cmd);
            int ret = execute_command_with_limits(final_cmd, NULL, 30, 0);
            if (ret != 0) LOG_WARN("[%d/%d] magick/ffmpeg failed rc=%d", index, total, ret);
        }
    }
}
int dir_has_missing_thumbs_common(const char* dir, int videos_only, int shallow) {
    LOG_DEBUG("dir_has_missing_thumbs%s: scanning %s (videos_only=%d)",
        shallow ? "_shallow" : "", dir, videos_only);

    diriter it;
    if (!dir_open(&it, dir)) {
        LOG_DEBUG("dir_has_missing_thumbs%s: failed to open %s",
            shallow ? "_shallow" : "", dir);
        return 0;
    }

    const char* name;
    int checked = 0;

    while ((name = dir_next(&it))) {
        if (!strcmp(name, ".") || !strcmp(name, "..") || !strcmp(name, "thumbs"))
            continue;

        char full[PATH_MAX];
        path_join(full, dir, name);

        if (!shallow && is_dir(full)) {
            if (strcmp(name, "thumbs") == 0) continue;
            if (dir_has_missing_thumbs_common(full, videos_only, shallow)) {
                dir_close(&it);
                return 1;
            }
            continue;
        }

        if (shallow && is_dir(full)) continue;

        if (!(has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS))) continue;
        if (videos_only && !has_ext(name, VIDEO_EXTS)) continue;

        char small_rel[PATH_MAX];
        char large_rel[PATH_MAX];
        get_thumb_rel_names(full, name, small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));

        char thumbs_root[PATH_MAX];
        get_thumbs_root(thumbs_root, sizeof(thumbs_root));

        char safe_dir_name[PATH_MAX];
        make_safe_dir_name_from(dir, safe_dir_name, sizeof(safe_dir_name));

        char per_thumbs_root[PATH_MAX];
        snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);

        char small_fs[PATH_MAX];
        char large_fs[PATH_MAX];
        snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, small_rel);
        snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, large_rel);

        LOG_DEBUG("dir_has_missing_thumbs%s: checking media=%s small=%s large=%s",
            shallow ? "_shallow" : "", full, small_fs, large_fs);

        int small_exists = is_file(small_fs);
        int large_exists = is_file(large_fs);

        struct stat st_media, st_small, st_large;
        int media_stat = platform_stat(full, &st_media);
        int small_stat = platform_stat(small_fs, &st_small);
        int large_stat = platform_stat(large_fs, &st_large);

        if (!small_exists) {
            dir_close(&it);
            return 1;
        }
        if (media_stat == 0 && small_stat == 0 && st_small.st_mtime < st_media.st_mtime) {
            dir_close(&it);
            return 1;
        }
        if (!large_exists) {
            dir_close(&it);
            return 1;
        }
        if (media_stat == 0 && large_stat == 0 && st_large.st_mtime < st_media.st_mtime) {
            dir_close(&it);
            return 1;
        }

        checked++;
        if (shallow && checked >= MAX_SHALLOW_CHECK) {
            LOG_DEBUG("dir_has_missing_thumbs_shallow: reached max checks (%d) for %s",
                MAX_SHALLOW_CHECK, dir);
            dir_close(&it);
            return 1;
        }
    }

    dir_close(&it);
    LOG_DEBUG("dir_has_missing_thumbs%s: no missing thumbs found in %s",
        shallow ? "_shallow" : "", dir);
    return 0;
}
int dir_has_missing_thumbs(const char* dir, int videos_only) {
    return dir_has_missing_thumbs_common(dir, videos_only, 0);
}
int dir_has_missing_thumbs_shallow(const char* dir, int videos_only) {
    return dir_has_missing_thumbs_common(dir, videos_only, 1);
}
typedef struct watcher_node {
    char dir[PATH_MAX];
    int scheduled;
    struct watcher_node* next;
} watcher_node_t;
static watcher_node_t* watcher_head = NULL;
static thread_mutex_t watcher_mutex;
static int watcher_mutex_inited = 0;

typedef struct running_node {
    char dir[PATH_MAX];
    struct running_node* next;
} running_node_t;
static running_node_t* running_head = NULL;
static thread_mutex_t running_mutex;
static int running_mutex_inited = 0;
typedef struct warn_node {
    char dir[PATH_MAX];
    time_t last_log;
    struct warn_node* next;
} warn_node_t;
static warn_node_t* warn_head = NULL;
static thread_mutex_t warn_mutex;
static int warn_mutex_inited = 0;

#ifndef LOG_SUPPRESS_SECONDS
#define LOG_SUPPRESS_SECONDS 60
#endif

static void warn_maybe_log_already_running(const char* dir) {
    if (!dir) return;
    if (!warn_mutex_inited) {
        if (thread_mutex_init(&warn_mutex) == 0) warn_mutex_inited = 1;
    }
    time_t now = time(NULL);
    if (warn_mutex_inited) {
        thread_mutex_lock(&warn_mutex);
        warn_node_t* cur = warn_head;
        while (cur) {
            if (strcmp(cur->dir, dir) == 0) break;
            cur = cur->next;
        }
        if (!cur) {
            cur = malloc(sizeof(warn_node_t));
            if (cur) {
                strncpy(cur->dir, dir, PATH_MAX - 1);
                cur->dir[PATH_MAX - 1] = '\0';
                cur->last_log = 0;
                cur->next = warn_head;
                warn_head = cur;
            }
        }
        if (cur) {
            if (now - cur->last_log >= LOG_SUPPRESS_SECONDS) {
                char tmpd[PATH_MAX]; strncpy(tmpd, dir, sizeof(tmpd) - 1); tmpd[sizeof(tmpd) - 1] = '\0'; strip_trailing_sep(tmpd);
                LOG_INFO("Thumbnail generation already running for: %s", tmpd);
                cur->last_log = now;
            }
        }
        thread_mutex_unlock(&warn_mutex);
    } else {
        char tmpd[PATH_MAX]; strncpy(tmpd, dir, sizeof(tmpd) - 1); tmpd[sizeof(tmpd) - 1] = '\0'; strip_trailing_sep(tmpd);
        LOG_INFO("Thumbnail generation already running for: %s", tmpd);
    }
}
static void remove_watcher_node(const char* dir) {
    thread_mutex_lock(&watcher_mutex);
    watcher_node_t* prev = NULL;
    watcher_node_t* cur = watcher_head;
    while (cur) {
        if (strcmp(cur->dir, dir) == 0) {
            if (prev) prev->next = cur->next;
            else watcher_head = cur->next;
            free(cur);
            break;
        }
        prev = cur;
        cur = cur->next;
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

    {
        progress_t quick_prog;
        memset(&quick_prog, 0, sizeof(quick_prog));

        char thumbs_root[PATH_MAX];
        get_thumbs_root(thumbs_root, sizeof(thumbs_root));
        if (!is_dir(thumbs_root)) platform_make_dir(thumbs_root);

        char safe_dir_name[PATH_MAX];
        make_safe_dir_name_from(dir, safe_dir_name, sizeof(safe_dir_name));
        char per_thumbs_root[PATH_MAX];
        snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);
        if (!is_dir(per_thumbs_root)) platform_make_dir(per_thumbs_root);

        char per_db[PATH_MAX];
        snprintf(per_db, sizeof(per_db), "%s" DIR_SEP_STR "thumbs.db", per_thumbs_root);
        thumbdb_open_for_dir(per_db);

        count_media_in_dir(dir, &quick_prog);

        diriter it;
        if (dir_open(&it, dir)) {
            const char* name;
            while ((name = dir_next(&it))) {
                if (!strcmp(name, ".") || !strcmp(name, "..") || !strcmp(name, "thumbs")) continue;
                char full[PATH_MAX];
                path_join(full, dir, name);
                if (!is_file(full)) continue;
                if (!(has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS))) continue;

                char thumb_small_rel[PATH_MAX], thumb_large_rel[PATH_MAX];
                get_thumb_rel_names(full, name, thumb_small_rel, sizeof(thumb_small_rel), thumb_large_rel, sizeof(thumb_large_rel));
                char thumb_small[PATH_MAX], thumb_large[PATH_MAX];
                snprintf(thumb_small, sizeof(thumb_small), "%s" DIR_SEP_STR "%s", per_thumbs_root, thumb_small_rel);
                snprintf(thumb_large, sizeof(thumb_large), "%s" DIR_SEP_STR "%s", per_thumbs_root, thumb_large_rel);

                struct stat st_media, st_small, st_large;
                int need_small = 0, need_large = 0;
                if (platform_stat(full, &st_media) == 0) {
                    if (!is_file(thumb_small) || platform_stat(thumb_small, &st_small) != 0 || st_small.st_mtime < st_media.st_mtime)
                        need_small = 1;
                    if (!is_file(thumb_large) || platform_stat(thumb_large, &st_large) != 0 || st_large.st_mtime < st_media.st_mtime)
                        need_large = 1;
                } else {
                    need_small = !is_file(thumb_small);
                    need_large = !is_file(thumb_large);
                }

                if (need_small) {
                    schedule_or_generate_thumb(full, thumb_small, &quick_prog, THUMB_SMALL_SCALE, THUMB_SMALL_QUALITY);
                }

                if (need_large) {
                    schedule_or_generate_thumb(full, thumb_large, &quick_prog, THUMB_LARGE_SCALE, THUMB_LARGE_QUALITY);
                }
            }
            dir_close(&it);
        }

    }

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

static void* debounce_generation_thread(void* args) {
    char* dcopy = (char*)args;
    if (!dcopy) {
        return NULL;
    }

    platform_sleep_ms(DEBOUNCE_MS);
    start_background_thumb_generation(dcopy);
    thread_mutex_lock(&watcher_mutex);
    watcher_node_t* cur = watcher_head;
    while (cur) {
        if (strcmp(cur->dir, dcopy) == 0) {
            cur->scheduled = 0;
            break;
        }
        cur = cur->next;
    }
    thread_mutex_unlock(&watcher_mutex);
    free(dcopy);

    return NULL;
}

static void* thumbnail_generation_thread(void* args) {
    thread_args_t* thread_args = (thread_args_t*)args;
    char dir_path[PATH_MAX];
    strncpy(dir_path, thread_args->dir_path, PATH_MAX - 1);
    dir_path[PATH_MAX - 1] = '\0';
    free(args);
    strip_trailing_sep(dir_path);
    LOG_INFO("Background thumbnail generation starting for: %s", dir_path);
    run_thumb_generation(dir_path);
    strip_trailing_sep(dir_path);
    LOG_INFO("Background thumbnail generation finished for: %s", dir_path);
    if (!running_mutex_inited && thread_mutex_init(&running_mutex) == 0) running_mutex_inited = 1;
    if (running_mutex_inited) {
        thread_mutex_lock(&running_mutex);
        running_node_t* prev = NULL;
        running_node_t* cur = running_head;
        while (cur) {
            if (strcmp(cur->dir, dir_path) == 0) {
                if (prev) prev->next = cur->next; else running_head = cur->next;
                free(cur);
                break;
            }
            prev = cur;
            cur = cur->next;
        }
        thread_mutex_unlock(&running_mutex);
    }
    return NULL;
}

static void* thumb_job_thread(void* args) {
    if (!args) return NULL;
    thumb_job_t* job = (thumb_job_t*)args;
    LOG_DEBUG("thumb_job_thread: starting generation for %s -> %s", job->input, job->output);
    run_thumb_job(job);
    LOG_DEBUG("thumb_job_thread: broadcasting and finishing for %s", job->input);
    free(job);
    atomic_fetch_sub(&thumb_workers_active, 1);
    return NULL;
}
static void* thumb_maintenance_thread(void* args) {
    int interval = args ? *((int*)args) : 300;
    int ival = interval;
    free(args);
    for (;;) {
        platform_sleep_ms(1000 * ival);
        LOG_INFO("Periodic thumb maintenance: running migration and orphan cleanup");

        size_t gf_count = 0;
        char** gfolders = get_gallery_folders(&gf_count);
        if (gf_count == 0) continue;

        for (size_t gi = 0; gi < gf_count; ++gi) {
            char* gallery = gfolders[gi];
            char thumbs_root[PATH_MAX];
            get_thumbs_root(thumbs_root, sizeof(thumbs_root));
            char safe_dir_name[PATH_MAX];
            make_safe_dir_name_from(gallery, safe_dir_name, sizeof(safe_dir_name));
            char per_thumbs_root[PATH_MAX];
            snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);
            if (!is_dir(per_thumbs_root)) platform_make_dir(per_thumbs_root);
            char per_db[PATH_MAX];
            snprintf(per_db, sizeof(per_db), "%s" DIR_SEP_STR "thumbs.db", per_thumbs_root);
            thumbdb_open_for_dir(per_db);

            diriter it;
            if (!dir_open(&it, gallery)) {
                continue;
            }

            const char* mname;
            while ((mname = dir_next(&it))) {
                if (!strcmp(mname, ".") || !strcmp(mname, "..") || !strcmp(mname, "thumbs")) continue;

                char media_full[PATH_MAX];
                path_join(media_full, gallery, mname);
                if (!is_file(media_full)) continue;
                if (!(has_ext(mname, IMAGE_EXTS) || has_ext(mname, VIDEO_EXTS))) continue;

                char small_rel[PATH_MAX], large_rel[PATH_MAX];
                get_thumb_rel_names(media_full, mname, small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));

                char desired_small[PATH_MAX], desired_large[PATH_MAX];
                snprintf(desired_small, sizeof(desired_small), "%s" DIR_SEP_STR "%s", per_thumbs_root, small_rel);
                snprintf(desired_large, sizeof(desired_large), "%s" DIR_SEP_STR "%s", per_thumbs_root, large_rel);
            }

            dir_close(&it);
            ensure_thumbs_in_dir(gallery, NULL);
            clean_orphan_thumbs(gallery, NULL);

            if (thumbdb_tx_begin() == 0) {
                thumbdb_sweep_orphans();
                if (thumbdb_tx_commit() != 0) {
                    LOG_WARN("thumbs: failed to commit tx for gallery %s, aborting", gallery);
                    thumbdb_tx_abort();
                }
            } else {
                LOG_WARN("thumbs: failed to start tx for database maintenance in gallery %s", gallery);
            }
        }

        thumbdb_sweep_orphans();
        if (!thumbdb_perform_requested_compaction())
            thumbdb_compact();
    }
    return NULL;
}
void start_periodic_thumb_maintenance(int interval_seconds) {
    int* arg = malloc(sizeof(int));
    if (!arg) {
        LOG_ERROR("Failed to allocate interval argument for thumb maintenance thread");
        return;
    }
    *arg = interval_seconds > 0 ? interval_seconds : 300;
    thread_create_detached((void* (*)(void*))thumb_maintenance_thread, arg);
}
void start_auto_thumb_watcher(const char* dir_path) {
    if (!dir_path) return;
    if (!watcher_mutex_inited && thread_mutex_init(&watcher_mutex) == 0) watcher_mutex_inited = 1;
    
    thread_mutex_lock(&watcher_mutex);
    watcher_node_t* cur = watcher_head;
    while (cur) {
        if (strcmp(cur->dir, dir_path) == 0) {
            thread_mutex_unlock(&watcher_mutex);
            return;
        }
        cur = cur->next;
    }

    watcher_node_t* node = malloc(sizeof(watcher_node_t));
    if (!node) {
        thread_mutex_unlock(&watcher_mutex);
        LOG_ERROR("Failed to allocate watcher node for directory %s", dir_path);
        return;
    }

    strncpy(node->dir, dir_path, PATH_MAX - 1);
    node->dir[PATH_MAX - 1] = '\0';
    node->next = watcher_head;
    watcher_head = node;
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
    if (real_path(dir, dir_real))
        dir_used = dir_real;

    char thumbs_root[PATH_MAX];
    get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    if (!is_dir(thumbs_root))
        platform_make_dir(thumbs_root);

    char safe_dir_name[PATH_MAX];
    make_safe_dir_name_from(dir_used, safe_dir_name, sizeof(safe_dir_name));

    char per_thumbs_root[PATH_MAX];
    snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);
    if (!is_dir(per_thumbs_root))
        platform_make_dir(per_thumbs_root);

    strncpy(prog.thumbs_dir, per_thumbs_root, PATH_MAX - 1);
    prog.thumbs_dir[PATH_MAX - 1] = '\0';

    char lock_path[PATH_MAX];
    snprintf(lock_path, sizeof(lock_path), "%s" DIR_SEP_STR ".thumbs.lock", per_thumbs_root);
    
    if (is_file(lock_path)) {
        FILE* lf = fopen(lock_path, "r");
        if (lf) {
            int owner_pid = 0;
            if (fscanf(lf, "%d", &owner_pid) == 1 && owner_pid == platform_get_pid()) {
                fclose(lf);
                LOG_DEBUG("run_thumb_generation: lock already acquired by us %s", lock_path);
                goto skip_lock_creation;
            }
            fclose(lf);
        }
    }
    
    int lock_ret = platform_create_lockfile_exclusive(lock_path);

    if (lock_ret == 1) {
        struct stat st;
        if (platform_stat(lock_path, &st) == 0) {
            time_t now = time(NULL);
            int owner_pid = 0;
            FILE* lf = fopen(lock_path, "r");
            if (lf) {
                if (fscanf(lf, "%d", &owner_pid) != 1)
                    owner_pid = 0;
                fclose(lf);
            }

            int owner_alive = 0;
            if (owner_pid > 0)
                owner_alive = platform_pid_is_running(owner_pid);

            if (!owner_alive) {
                LOG_WARN("Lockfile %s owned by dead PID %d or unreadable; removing", lock_path, owner_pid);
                platform_file_delete(lock_path);
                lock_ret = platform_create_lockfile_exclusive(lock_path);
            }
            else if (now > st.st_mtime && (now - st.st_mtime) > STALE_LOCK_SECONDS) {
                LOG_WARN("Stale lock file detected %s age=%llds (owner pid %d still alive), removing",
                    lock_path, (long long)(now - st.st_mtime), owner_pid);
                platform_file_delete(lock_path);
                lock_ret = platform_create_lockfile_exclusive(lock_path);
            }
        }
    }

    if (lock_ret == 1) {
        LOG_DEBUG("Thumbnail generation already running for: %s", dir);
        return;
    }

    if (lock_ret != 0) {
        LOG_WARN("Failed to create lock file %s", lock_path);
        return;
    }

    LOG_DEBUG("run_thumb_generation: acquired lock %s", lock_path);
    
    skip_lock_creation:

    char per_db[PATH_MAX];
    snprintf(per_db, sizeof(per_db), "%s" DIR_SEP_STR "thumbs.db", per_thumbs_root);
    int tbr = thumbdb_open_for_dir(per_db);
    if (tbr != 0) {
        LOG_WARN("run_thumb_generation: thumbdb_open_for_dir failed for %s (rc=%d)", per_db, tbr);
    }
    else {
        LOG_DEBUG("run_thumb_generation: opened DB %s", per_db);
        process_wal_chunks(per_thumbs_root);
    }

    count_media_in_dir(dir_used, &prog);
    {
        char dir_used_clean[PATH_MAX]; strncpy(dir_used_clean, dir_used, sizeof(dir_used_clean) - 1); dir_used_clean[sizeof(dir_used_clean) - 1] = '\0';
        strip_trailing_sep(dir_used_clean);
        LOG_INFO("Found %zu media files in %s", prog.total_files, dir_used_clean);
    }
    LOG_DEBUG("run_thumb_generation: calling ensure_thumbs_in_dir for %s (found %zu)", dir_used, prog.total_files);

    ensure_thumbs_in_dir(dir_used, &prog);
    wait_for_thumb_workers();
    process_wal_chunks(per_thumbs_root);
    LOG_DEBUG("run_thumb_generation: ensure_thumbs_in_dir completed, processed %zu files", prog.processed_files);

    clean_orphan_thumbs(dir_used, &prog);
    LOG_DEBUG("run_thumb_generation: clean_orphan_thumbs completed");

    print_skips(&prog);
    LOG_DEBUG("run_thumb_generation: print_skips completed");

    LOG_DEBUG("run_thumb_generation: starting final database processing");
    thumbdb_sweep_orphans();
    if (!thumbdb_perform_requested_compaction())
        thumbdb_compact();
    LOG_DEBUG("run_thumb_generation: final database processing completed");

    platform_file_delete(lock_path);
    LOG_DEBUG("run_thumb_generation: released lock %s", lock_path);
}
bool check_thumb_exists(const char* media_path, char* thumb_path, size_t thumb_path_len) {
    char small_rel[PATH_MAX];
    char large_rel[PATH_MAX];
    char dirbuf[PATH_MAX];
    char filename[PATH_MAX];

    strncpy(dirbuf, media_path, PATH_MAX - 1);
    dirbuf[PATH_MAX - 1] = '\0';

    char* last_slash = strrchr(dirbuf, '/');
    char* last_bslash = strrchr(dirbuf, '\\');
    char* last = NULL;

    if (last_slash && last_bslash)
        last = (last_slash > last_bslash) ? last_slash : last_bslash;
    else if (last_slash)
        last = last_slash;
    else if (last_bslash)
        last = last_bslash;

    if (last) {
        strncpy(filename, last + 1, PATH_MAX - 1);
        filename[PATH_MAX - 1] = '\0';
        *last = '\0';
    }
    else {
        strncpy(filename, dirbuf, PATH_MAX - 1);
        filename[PATH_MAX - 1] = '\0';
        dirbuf[0] = '.';
        dirbuf[1] = '\0';
    }

    char found_key[PATH_MAX]; found_key[0] = '\0';
    if (thumbdb_find_for_media(media_path, found_key, sizeof(found_key)) == 0) {
        if (thumb_path_len > 0) {
            snprintf(thumb_path, thumb_path_len, "%s", found_key);
        }
        return true;
    }

    get_thumb_rel_names(media_path, filename, small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));

    char thumbs_root[PATH_MAX];
    get_thumbs_root(thumbs_root, sizeof(thumbs_root));

    char safe_dir_name[PATH_MAX];
    safe_dir_name[0] = '\0';
    make_safe_dir_name_from(dirbuf, safe_dir_name, sizeof(safe_dir_name));

    char per_thumbs_root[PATH_MAX];
    snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);

    char small_fs[PATH_MAX];
    char large_fs[PATH_MAX];
    snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, small_rel);
    snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, large_rel);

    if (thumb_path_len > 0) {
        snprintf(thumb_path, thumb_path_len, "%s", small_rel);
    }

    if (is_file(small_fs)) {
        if (thumb_path_len > 0)
            snprintf(thumb_path, thumb_path_len, "%s", small_rel);
        return true;
    }

    if (is_file(large_fs)) {
        if (thumb_path_len > 0)
            snprintf(thumb_path, thumb_path_len, "%s", large_rel);
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

    char thumbs_root[PATH_MAX];
    get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    if (!is_dir(thumbs_root)) platform_make_dir(thumbs_root);

    char safe_dir_name[PATH_MAX];
    make_safe_dir_name_from(dir_path, safe_dir_name, sizeof(safe_dir_name));

    char per_thumbs_root[PATH_MAX];
    snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);
    if (!is_dir(per_thumbs_root)) platform_make_dir(per_thumbs_root);

    char lock_path[PATH_MAX];
    snprintf(lock_path, sizeof(lock_path), "%s" DIR_SEP_STR ".thumbs.lock", per_thumbs_root);

    int lock_ret = platform_create_lockfile_exclusive(lock_path);
    if (lock_ret == 1) {
        struct stat st;
        if (platform_stat(lock_path, &st) == 0) {
            time_t now = time(NULL);
            int owner_pid = 0;
            FILE* lf = fopen(lock_path, "r");
            if (lf) {
                if (fscanf(lf, "%d", &owner_pid) != 1)
                    owner_pid = 0;
                fclose(lf);
            }

            int owner_alive = 0;
            if (owner_pid > 0) owner_alive = platform_pid_is_running(owner_pid);

            if (owner_alive) {
                if (now > st.st_mtime && (now - st.st_mtime) > STALE_LOCK_SECONDS) {
                    LOG_WARN("Stale lock file detected %s age=%llds (owner pid %d still alive), removing", lock_path, (long long)(now - st.st_mtime), owner_pid);
                    platform_file_delete(lock_path);
                    lock_ret = platform_create_lockfile_exclusive(lock_path);
                }
                else {
                    warn_maybe_log_already_running(dir_path);
                    start_auto_thumb_watcher(dir_path);
                    return;
                }
            }
            else {
                LOG_WARN("Lockfile %s owned by dead PID %d or unreadable; removing", lock_path, owner_pid);
                platform_file_delete(lock_path);
                lock_ret = platform_create_lockfile_exclusive(lock_path);
            }
        }
    }

    if (lock_ret != 0) {
        LOG_WARN("Failed to create lock file %s", lock_path);
        return;
    }

    LOG_DEBUG("start_background_thumb_generation: acquired lock %s", lock_path);

    thread_args_t* args = malloc(sizeof(thread_args_t));
    if (!args) {
        LOG_ERROR("Failed to allocate memory for thread arguments for directory %s", dir_path);
        return;
    }

    if (!running_mutex_inited) {
        if (thread_mutex_init(&running_mutex) == 0) running_mutex_inited = 1;
    }
    if (running_mutex_inited) {
        thread_mutex_lock(&running_mutex);
        running_node_t* cur = running_head;
        while (cur) {
            if (strcmp(cur->dir, dir_path) == 0) {
                thread_mutex_unlock(&running_mutex);
                start_auto_thumb_watcher(dir_path);
                free(args);
                return;
            }
            cur = cur->next;
        }
        running_node_t* rn = malloc(sizeof(running_node_t));
        if (!rn) {
            LOG_ERROR("Failed to allocate running node for directory %s", dir_path);
        } else if (rn) {
            strncpy(rn->dir, dir_path, PATH_MAX - 1);
            rn->dir[PATH_MAX - 1] = '\0';
            rn->next = running_head;
            running_head = rn;
        }
        thread_mutex_unlock(&running_mutex);
    }

    strncpy(args->dir_path, dir_path, PATH_MAX - 1);
    args->dir_path[PATH_MAX - 1] = '\0';

    if (thread_create_detached((void* (*)(void*))thumbnail_generation_thread, args) != 0) {
        LOG_ERROR("Failed to create thumbnail generation thread for %s", dir_path);
        free(args);
    }

    start_auto_thumb_watcher(dir_path);
}
void add_skip(progress_t * prog, const char* reason, const char* path) {
    if (!reason || !path) return;

    char log_path[PATH_MAX];
    if (prog && prog->thumbs_dir[0])
        path_join(log_path, prog->thumbs_dir, "skipped.log");
    else {
        char thumbs_root[PATH_MAX];
        get_thumbs_root(thumbs_root, sizeof(thumbs_root));
        path_join(log_path, thumbs_root, "skipped.log");
    }

    FILE* f = fopen(log_path, "a");
    if (f) {
        fprintf(f, "[%s] %s\n", reason, path);
        fclose(f);
    }

    if (!prog) return;

    skip_counter_t* curr = prog->skip_head;
    while (curr) {
        if (strcmp(curr->dir, prog->thumbs_dir) == 0) break;
        curr = curr->next;
    }

    if (!curr) {
        curr = malloc(sizeof(skip_counter_t));
        if (!curr) {
            LOG_ERROR("Failed to allocate skip counter structure");
            return;
        }
        strncpy(curr->dir, prog->thumbs_dir, PATH_MAX - 1);
        curr->dir[PATH_MAX - 1] = '\0';
        curr->count = 0;
        curr->next = prog->skip_head;
        prog->skip_head = curr;
    }
    curr->count++;
}
void print_skips(progress_t * prog) {
    skip_counter_t* curr = prog->skip_head;
    while (curr) {
        if (curr->count > 0)
            LOG_DEBUG("[SKIPPED] %d files skipped in thumbs_dir: %s\n", curr->count, curr->dir);
        skip_counter_t* tmp = curr;
        curr = curr->next;
        free(tmp);
    }
    prog->skip_head = NULL;
}
int get_media_dimensions(const char* path, int* width, int* height) {
    (void)path; (void)width; (void)height;
    return 0;
}
void count_media_in_dir(const char* dir, progress_t * prog) {
    diriter it;
    if (!dir_open(&it, dir)) return;

    const char* name;
    while ((name = dir_next(&it))) {
        if (!strcmp(name, ".") || !strcmp(name, "..") || !strcmp(name, "thumbs")) continue;

        char full[PATH_MAX];
        path_join(full, dir, name);

        if (is_file(full) && (has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS))) {
            prog->total_files++;
        }
    }
    dir_close(&it);
}
void ensure_thumbs_in_dir(const char* dir, progress_t* prog) {
    if (!dir || !*dir) {
        LOG_ERROR("ensure_thumbs_in_dir: invalid dir");
        return;
    }

    LOG_DEBUG("ensure_thumbs_in_dir: enter for %s", dir);

    if (!prog) {
        int quick = dir_has_missing_thumbs_shallow(dir, 0);
        if (quick)
            start_background_thumb_generation(dir);
        else
            start_auto_thumb_watcher(dir);
        return;
    }

    LOG_DEBUG("ensure_thumbs_in_dir: prog total_files=%zu processed=%zu", prog->total_files, prog->processed_files);

    diriter it;
    if (!dir_open(&it, dir)) {
        LOG_WARN("ensure_thumbs_in_dir: failed to open dir %s", dir);
        return;
    }
    LOG_DEBUG("ensure_thumbs_in_dir: scanning directory %s", dir);
    const char* name;
    while ((name = dir_next(&it))) {
        if (!name || strcmp(name, ".") == 0 || strcmp(name, "..") == 0) continue;
        char full[PATH_MAX];
        path_join(full, dir, name);
        if (is_dir(full)) continue;
        const char* ext_check = strrchr(name, '.');
        if (ext_check && ascii_stricmp(ext_check, ".m4s") == 0) {
            char mp4path[PATH_MAX];
            strncpy(mp4path, full, sizeof(mp4path) - 1);
            mp4path[sizeof(mp4path) - 1] = '\0';
            char* dotp = strrchr(mp4path, '.');
            if (dotp)
                snprintf(dotp, sizeof(mp4path) - (dotp - mp4path), ".mp4");
            else
                strncat(mp4path, ".mp4", sizeof(mp4path) - strlen(mp4path) - 1);
            if (is_newer(full, mp4path)) {
                char esc_in[PATH_MAX * 2] = { 0 };
                char esc_out[PATH_MAX * 2] = { 0 };
                platform_escape_path_for_cmd(full, esc_in, sizeof(esc_in));
                platform_escape_path_for_cmd(mp4path, esc_out, sizeof(esc_out));
                int threads = platform_get_cpu_count();
                if (threads < 1) threads = 1;
                char cmd[PATH_MAX * 3];
                snprintf(cmd, sizeof(cmd), "ffmpeg -y -threads %d -i %s -c copy %s", threads, esc_in, esc_out);
                LOG_INFO("Converting .m4s -> .mp4: %s -> %s", full, mp4path);
                int rc = execute_command_with_limits(cmd, NULL, 120, 1);
                if (rc != 0) {
                    LOG_WARN("Failed to convert %s -> %s (rc=%d)", full, mp4path, rc);
                }
                else {
                    LOG_INFO("Conversion succeeded: %s -> %s", full, mp4path);
                    if (platform_file_delete(full) == 0)
                        LOG_INFO("Deleted original segment file: %s", full);
                    else
                        LOG_WARN("Failed to delete original segment file: %s", full);
                }
            }
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
        char thumb_small_rel[PATH_MAX];
        char thumb_large_rel[PATH_MAX];
        get_thumb_rel_names(full, name, thumb_small_rel, sizeof(thumb_small_rel), thumb_large_rel, sizeof(thumb_large_rel));
        char thumbs_root[PATH_MAX];
        get_thumbs_root(thumbs_root, sizeof(thumbs_root));
        char safe_dir_name[PATH_MAX];
        make_safe_dir_name_from(dir, safe_dir_name, sizeof(safe_dir_name));
        char per_thumbs_root[PATH_MAX];
        snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);
        if (!is_dir(per_thumbs_root)) platform_make_dir(per_thumbs_root);
        char thumb_small[PATH_MAX];
        char thumb_large[PATH_MAX];
        snprintf(thumb_small, sizeof(thumb_small), "%s" DIR_SEP_STR "%s", per_thumbs_root, thumb_small_rel);
        snprintf(thumb_large, sizeof(thumb_large), "%s" DIR_SEP_STR "%s", per_thumbs_root, thumb_large_rel);
        struct stat st_media, st_small, st_large;
        int need_small = 0, need_large = 0;
        if (platform_stat(full, &st_media) == 0) {
            if (!is_file(thumb_small) || platform_stat(thumb_small, &st_small) != 0 || st_small.st_mtime < st_media.st_mtime)
                need_small = 1;
            if (!is_file(thumb_large) || platform_stat(thumb_large, &st_large) != 0 || st_large.st_mtime < st_media.st_mtime)
                need_large = 1;
        }
        else {
            need_small = !is_file(thumb_small);
            need_large = !is_file(thumb_large);
        }
        LOG_DEBUG("ensure_thumbs_in_dir: media=%s need_small=%d need_large=%d", full, need_small, need_large);
        if (need_small)
            schedule_or_generate_thumb(full, thumb_small, prog, THUMB_SMALL_SCALE, THUMB_SMALL_QUALITY);
        if (need_large)
            schedule_or_generate_thumb(full, thumb_large, prog, THUMB_LARGE_SCALE, THUMB_LARGE_QUALITY);
    }
    dir_close(&it);
    LOG_DEBUG("ensure_thumbs_in_dir: completed scanning %s", dir);
}

void schedule_or_generate_thumb(const char* input, const char* output, progress_t* prog, int scale, int q) {
    if (!input || !output || !prog) return;
    
    prog->processed_files++;
    
    thumb_job_t* job = calloc(1, sizeof(thumb_job_t));
    if (!job) {
        LOG_ERROR("Failed to allocate thumb job structure for %s", input);
        generate_thumb_inline_and_record(input, output, scale, q, (int)prog->processed_files, (int)prog->total_files);
        return;
    }
    
    strncpy(job->input, input, PATH_MAX - 1);
    job->input[PATH_MAX - 1] = '\0';
    strncpy(job->output, output, PATH_MAX - 1);
    job->output[PATH_MAX - 1] = '\0';
    job->scale = scale;
    job->q = q;
    job->index = (int)prog->processed_files;
    job->total = (int)prog->total_files;
    
    while (atomic_load(&thumb_workers_active) >= MAX_THUMB_WORKERS) sleep_ms(50);
    atomic_fetch_add(&thumb_workers_active, 1);
    
    if (thread_create_detached((void* (*)(void*))thumb_job_thread, job) != 0) {
        LOG_ERROR("Failed to spawn thumb worker thread, generating inline");
        atomic_fetch_sub(&thumb_workers_active, 1);
        generate_thumb_inline_and_record(input, output, scale, q, job->index, job->total);
        free(job);
    }
}

void clean_orphan_thumbs(const char* dir, progress_t * prog) {
    if (!dir) return;
    char thumbs_root[PATH_MAX];
    get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    char safe_dir_name[PATH_MAX];
    make_safe_dir_name_from(dir, safe_dir_name, sizeof(safe_dir_name));
    char thumbs_path[PATH_MAX];
    snprintf(thumbs_path, sizeof(thumbs_path), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);
    if (!is_dir(thumbs_path)) return;
    diriter mit;
    if (!dir_open(&mit, dir)) return;
    size_t expect_cap = 256;
    size_t expect_count = 0;
    char** expects = malloc(expect_cap * sizeof(char*));
    if (!expects) {
        LOG_ERROR("Failed to allocate expects array for orphan cleanup");
        dir_close(&mit);
        return;
    }
    const char* mname;
    while ((mname = dir_next(&mit))) {
        if (!strcmp(mname, ".") || !strcmp(mname, "..") || !strcmp(mname, "thumbs")) continue;
        char media_full[PATH_MAX];
        path_join(media_full, dir, mname);
        if (!is_file(media_full)) continue;
        if (!(has_ext(mname, IMAGE_EXTS) || has_ext(mname, VIDEO_EXTS))) continue;
        char small_rel[PATH_MAX];
        char large_rel[PATH_MAX];
        get_thumb_rel_names(media_full, mname, small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));
        if (expect_count + 2 > expect_cap) {
            size_t nc = expect_cap * 2;
            char** tmp = realloc(expects, nc * sizeof(char*));
            if (!tmp) {
                LOG_ERROR("Failed to realloc expects array, breaking early");
                break;
            }
            expects = tmp;
            expect_cap = nc;
        }
        expects[expect_count++] = strdup(small_rel);
        expects[expect_count++] = strdup(large_rel);
    }
    dir_close(&mit);
    diriter tit;
    if (!dir_open(&tit, thumbs_path)) {
        for (size_t i = 0; i < expect_count; ++i) free(expects[i]);
        free(expects);
        return;
    }
    char per_db[PATH_MAX];
    snprintf(per_db, sizeof(per_db), "%s" DIR_SEP_STR "thumbs.db", thumbs_path);
    thumbdb_open_for_dir(per_db);
    const char* tname;
    char tname_copy[PATH_MAX];
    while ((tname = dir_next(&tit))) {
        if (!tname) continue;
        strncpy(tname_copy, tname, sizeof(tname_copy) - 1);
        tname_copy[sizeof(tname_copy) - 1] = '\0';
        if (ascii_stricmp(tname_copy, ".") == 0 || ascii_stricmp(tname_copy, "..") == 0 ||
            ascii_stricmp(tname_copy, "skipped.log") == 0 || ascii_stricmp(tname_copy, ".nogallery") == 0 ||
            ascii_stricmp(tname_copy, ".thumbs.lock") == 0) continue;
        if (strstr(tname_copy, "-small-") || strstr(tname_copy, "-large-")) {
            char thumb_full_m[PATH_MAX];
            path_join(thumb_full_m, thumbs_path, tname_copy);
            if (platform_file_delete(thumb_full_m) != 0) LOG_WARN("Failed to delete malformed thumb: %s", thumb_full_m);
            else {
                LOG_DEBUG("Removed malformed thumb: %s", thumb_full_m);
                add_skip(prog, "MALFORMED_REMOVED", thumb_full_m);
            }
            continue;
        }
        if (!strstr(tname_copy, "-small.") && !strstr(tname_copy, "-large.")) continue;
        bool found = false;
        for (size_t ei = 0; ei < expect_count; ++ei) if (ascii_stricmp(tname, expects[ei]) == 0) { found = true; break; }
        if (!found) {
            char thumb_full[PATH_MAX];
            path_join(thumb_full, thumbs_path, tname_copy);
            char* bn_del = tname_copy;
            char mapped_media[PATH_MAX];
            int r = thumbdb_get(bn_del, mapped_media, sizeof(mapped_media));
            if (r != 0) {
                if (platform_file_delete(thumb_full) != 0) LOG_WARN("Failed to delete orphan thumb: %s", thumb_full);
                else LOG_INFO("Removed orphan thumb (no DB entry): %s", thumb_full);
                add_skip(prog, "ORPHAN_REMOVED", thumb_full);
            }
            else {
                size_t dlen = strlen(dir);
                if (strncmp(mapped_media, dir, dlen) == 0 &&
                    (mapped_media[dlen] == '\0' || mapped_media[dlen] == '/' || mapped_media[dlen] == '\\')) {
                    if (!is_file(mapped_media)) {
                        thumbdb_delete(bn_del);
                        if (platform_file_delete(thumb_full) != 0) LOG_WARN("Failed to delete orphan thumb: %s", thumb_full);
                        else LOG_INFO("Removed orphan thumb (media missing): %s", thumb_full);
                        add_skip(prog, "ORPHAN_REMOVED", thumb_full);
                    }
                    else {
                        LOG_DEBUG("Thumb %s maps to existing media in this gallery, keeping: %s", thumb_full, mapped_media);
                    }
                }
                else {
                    LOG_DEBUG("Skipping thumb %s mapped to other gallery media: %s", thumb_full, mapped_media);
                }
            }
        }
        size_t len = strlen(tname_copy);
        if (len > 10 && strcmp(tname_copy + len - 10, "-small.jpg") != 0 && strcmp(tname_copy + len - 10, "-large.jpg") != 0) {
            char thumb_full[PATH_MAX];
            path_join(thumb_full, thumbs_path, tname_copy);
            if (platform_file_delete(thumb_full) != 0) LOG_WARN("Failed to delete invalid thumb: %s", thumb_full);
            else LOG_INFO("Removed invalid thumb: %s", thumb_full);
            add_skip(prog, "INVALID_REMOVED", thumb_full);
        }
    }
    dir_close(&tit);
    for (size_t i = 0; i < expect_count; ++i) free(expects[i]);
    free(expects);
}
void scan_and_generate_missing_thumbs(void) {
    size_t count = 0;
    char** folders = get_gallery_folders(&count);
    if (!folders || count == 0) return;
    for (size_t i = 0; i < count; ++i) {
        {
            char tmpf[PATH_MAX]; strncpy(tmpf, folders[i], sizeof(tmpf) - 1); tmpf[sizeof(tmpf) - 1] = '\0'; strip_trailing_sep(tmpf);
            LOG_INFO("Scanning and generating missing thumbs for: %s", tmpf);
        }
        ensure_thumbs_in_dir(folders[i], NULL);
    }
}
