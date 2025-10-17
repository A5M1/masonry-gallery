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
atomic_int ffmpeg_active = ATOMIC_VAR_INIT(0);
typedef struct {
    char input[PATH_MAX];
    char output[PATH_MAX];
    int scale;
    int q;
    int index;
    int total;
} thumb_job_t;
static void sleep_ms(int ms) { platform_sleep_ms(ms); }

static int execute_command_with_limits(const char* cmd, const char* out_log, int timeout, int uses_ffmpeg) {
    int ret;
    if (!cmd) return -1;
    size_t cmdlen = strlen(cmd);
    if (cmdlen > 4096) {
        LOG_ERROR("execute_command_with_limits: refusing to execute overly long command (len=%zu)", cmdlen);
        return -1;
    }
    if (uses_ffmpeg) {
        while (atomic_load(&ffmpeg_active) >= MAX_FFMPEG) sleep_ms(50);
        atomic_fetch_add(&ffmpeg_active, 1);
        const char* final_out = out_log ? out_log : platform_devnull();
        platform_record_command(cmd);
        LOG_DEBUG("execute_command_with_limits: executing: %s", cmd);
        ret = platform_run_command_redirect(cmd, final_out, timeout);
        LOG_DEBUG("execute_command_with_limits: command rc=%d cmd=%s", ret, cmd);
        atomic_fetch_sub(&ffmpeg_active, 1);
    }
    else {
        platform_record_command(cmd);
        ret = platform_run_command_redirect(cmd, out_log ? out_log : platform_devnull(), timeout);
        LOG_DEBUG("execute_command_with_limits: command rc=%d cmd=%s", ret, cmd);
    }
    return ret;
}

static void build_magick_resize_cmd(char* dst, size_t dstlen, const char* in_esc, int scale, int q, const char* out_esc) {
    if (!dst || dstlen == 0)return;
    snprintf(dst, dstlen, "magick %s -resize %dx -quality %d %s", in_esc, scale, q, out_esc);
    LOG_DEBUG("Magick CMD: %s", dst);
}

static void build_ffmpeg_extract_png_cmd(char* dst, size_t dstlen, const char* in_esc, const char* tmp_esc, int scale) {
    if (!dst || dstlen == 0)return;
    snprintf(dst, dstlen, "ffmpeg -y -threads 1 -i %s -vf \"scale=%d:-1\" -vframes 1 -f image2 -vcodec png %s", in_esc, scale, tmp_esc);
    LOG_DEBUG("FFmpeg Extract CMD: %s", dst);
}

static void build_ffmpeg_thumb_cmd(char* dst, size_t dstlen, const char* in_esc, int scale, int q, int to_webp, int add_format_rgb, const char* out_esc) {
    if (!dst || dstlen == 0)return;
    if (to_webp) {
        if (add_format_rgb)
            snprintf(dst, dstlen, "ffmpeg -y -threads 1 -i %s -vf \"scale=%d:-1,format=rgb24\" -vframes 1 -q:v %d -c:v libwebp %s", in_esc, scale, q, out_esc);
        else
            snprintf(dst, dstlen, "ffmpeg -y -threads 1 -i %s -vf \"scale=%d:-1\" -vframes 1 -q:v %d -c:v libwebp %s", in_esc, scale, q, out_esc);
    }
    else
        snprintf(dst, dstlen, "ffmpeg -y -threads 1 -i %s -vf \"scale=%d:-1,format=rgb24\" -vframes 1 -q:v %d %s", in_esc, scale, q, out_esc);
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
static void strip_trailing_sep(char* p) {
    if (!p) return;
    size_t len = strlen(p);
    while (len > 0) {
#ifdef _WIN32
        if (p[len - 1] == '\\' || p[len - 1] == '/') { p[len - 1] = '\0'; len--; continue; }
#else
        if (p[len - 1] == '/') { p[len - 1] = '\0'; len--; continue; }
#endif
        break;
    }
}
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
static int get_image_dimensions_from_file(const char* path, int* width, int* height) {
    if (!path || !width || !height) return 0;

    FILE* f = fopen(path, "rb");
    if (!f) return 0;

    unsigned char hdr[32];
    size_t n = fread(hdr, 1, sizeof(hdr), f);
    if (n < 10) { fclose(f); return 0; }
    if (memcmp(hdr, "\x89PNG\r\n\x1a\n", 8) == 0) {
        if (n < 24) { fclose(f); return 0; }
        unsigned char* p = hdr + 16;
        unsigned int w = (p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3];
        unsigned int h = (p[4] << 24) | (p[5] << 16) | (p[6] << 8) | p[7];
        *width = (int)w; *height = (int)h; fclose(f); return 1;
    }
    if (memcmp(hdr, "GIF87a", 6) == 0 || memcmp(hdr, "GIF89a", 6) == 0) {
        unsigned int w = hdr[6] | (hdr[7] << 8);
        unsigned int h = hdr[8] | (hdr[9] << 8);
        *width = (int)w; *height = (int)h; fclose(f); return 1;
    }
    if (hdr[0] == 0xFF && hdr[1] == 0xD8) {
        fseek(f, 2, SEEK_SET);
        for (;;) {
            int c = fgetc(f);
            if (c == EOF) break;
            while (c != 0xFF) { c = fgetc(f); if (c == EOF) break; }
            if (c == EOF) break;
            int marker = fgetc(f);
            if (marker == EOF) break;
            while (marker == 0xFF) marker = fgetc(f);
            if (marker == 0xD9 || marker == 0xDA) break;

            unsigned char lenbuf[2];
            if (fread(lenbuf, 1, 2, f) != 2) break;
            int seglen = (lenbuf[0] << 8) | lenbuf[1];
            if (seglen < 2) break;

            /* SOF markers */
            if ((marker >= 0xC0 && marker <= 0xC3) || (marker >= 0xC5 && marker <= 0xC7) ||
                (marker >= 0xC9 && marker <= 0xCB) || (marker >= 0xCD && marker <= 0xCF)) {
                unsigned char sof[5];
                if (fread(sof, 1, 5, f) != 5) break;
                unsigned int h = (sof[1] << 8) | sof[2];
                unsigned int w = (sof[3] << 8) | sof[4];
                *width = (int)w; *height = (int)h; fclose(f); return 1;
            }

            if (fseek(f, seglen - 2, SEEK_CUR) != 0) break;
        }
        fclose(f); return 0;
    }

    /* BMP */
    if (hdr[0] == 'B' && hdr[1] == 'M') {
        if (n < 26) {
            unsigned char tmp[26];
            size_t got = fread(tmp, 1, 26 - n, f);
            if (got + n < 26) { fclose(f); return 0; }
            memcpy(hdr + n, tmp, got);
        }
        unsigned char* p = hdr + 18;
        unsigned int w = p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24);
        unsigned int h = p[4] | (p[5] << 8) | (p[6] << 16) | (p[7] << 24);
        *width = (int)w; *height = (int)h; fclose(f); return 1;
    }

    fclose(f);
    return 0;
}
static int get_webp_dimensions(const char* path, int* w, int* h) {
    FILE* f = fopen(path, "rb");
    if (!f) return 0;

    unsigned char hdr[30];
    if (fread(hdr, 1, 30, f) < 30) { fclose(f); return 0; }

    if (memcmp(hdr, "RIFF", 4) || memcmp(hdr + 8, "WEBP", 4)) { fclose(f); return 0; }

    unsigned char* chunk = hdr + 12;
    char t[5];
    memcpy(t, chunk, 4);
    t[4] = 0;
    if (!memcmp(t, "VP8 ", 4)) {
        unsigned char buf[10];
        fseek(f, 20, SEEK_SET);
        if (fread(buf, 1, 10, f) < 10) { fclose(f); return 0; }
        int w0 = (buf[7] << 8) | buf[6], h0 = (buf[9] << 8) | buf[8];
        *w = w0 & 0x3FFF; *h = h0 & 0x3FFF; fclose(f); return 1;
    }
    if (!memcmp(t, "VP8L", 4)) {
        unsigned char buf[5];
        fseek(f, 21, SEEK_SET);
        if (fread(buf, 1, 5, f) < 5) { fclose(f); return 0; }
        unsigned int bits = buf[1] | (buf[2] << 8) | (buf[3] << 16) | (buf[4] << 24);
        *w = (bits & 0x3FFF) + 1; *h = ((bits >> 14) & 0x3FFF) + 1; fclose(f); return 1;
    }
    if (!memcmp(t, "VP8X", 4)) {
        unsigned char buf[10];
        fseek(f, 24, SEEK_SET);
        if (fread(buf, 1, 10, f) < 10) { fclose(f); return 0; }
        *w = 1 + (buf[4] | (buf[5] << 8) | (buf[6] << 16));
        *h = 1 + (buf[7] | (buf[8] << 8) | (buf[9] << 16));
        fclose(f); return 1;
    }

    fclose(f);
    return 0;
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
    strip_trailing_sep(tmp);

    size_t si = 0;
    bool last_was_dash = false;

    for (size_t ii = 0; tmp[ii] && si < outlen - 1; ++ii) {
        char ch = tmp[ii];
        if (ch == '/' || ch == '\\' || !isalnum((unsigned char)ch)) {
            if (!last_was_dash && si < outlen - 1) {
                out[si++] = '-';
                last_was_dash = true;
            }
        }
        else {
            out[si++] = ch;
            last_was_dash = false;
        }
    }
    while (si > 0 && out[si - 1] == '-') si--;
    out[si] = '\0';
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
            return "ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of default=noprint_wrappers=1:nokey=1 %s";
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
    escape_path_for_cmd(path, esc_path, sizeof(esc_path));
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
    if (platform_stat(path, &st) != 0 || st.st_size < 16) return 0;
    if (!is_path_safe(path)) return 0;

    const char* ext = strrchr(path, '.');
    if (ext) {
        if (ascii_stricmp(ext, ".jpg") == 0 || ascii_stricmp(ext, ".jpeg") == 0 || ascii_stricmp(ext, ".png") == 0) {
            int w = 0, h = 0;
            if (get_image_dimensions_from_file(path, &w, &h)) return 1;
            return 0;
        }
    }

    return is_decodable(path);
}
int is_newer(const char* src, const char* dst) {
    struct stat s, d;
    if (platform_stat(src, &s) != 0) return 0;
    if (platform_stat(dst, &d) != 0) return 1;
    return s.st_mtime > d.st_mtime;
}
void generate_thumb_c(const char* input, const char* output, int scale, int q, int index, int total) {
    LOG_DEBUG("generate_thumb_c: enter input=%s output=%s scale=%d q=%d index=%d total=%d", input ? input : "(null)", output ? output : "(null)", scale, q, index, total);

    if (!is_path_safe(input)) {
        LOG_WARN("[%d/%d] Invalid path (unsafe): %s", index, total, input);
        return;
    }
    if (!is_valid_media(input)) {
        LOG_WARN("[%d/%d] Invalid media (stat/size) or not present: %s", index, total, input);
        return;
    }
    if (!is_decodable(input)) {
        LOG_WARN("[%d/%d] Not decodable (ffprobe failed): %s", index, total, input);
        return;
    }

    LOG_INFO("[%d/%d] Processing: %s", index, total, input);

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
    escape_path_for_cmd(in_path, esc_in, sizeof(esc_in));
    escape_path_for_cmd(out_path, esc_out, sizeof(esc_out));
    char esc_in_with_frame[PATH_MAX * 2];
    esc_in_with_frame[0] = '\0';
    escape_path_for_cmd(in_path_with_frame, esc_in_with_frame, sizeof(esc_in_with_frame));
    if (ext && ascii_stricmp(ext, ".webp") == 0) {
        if (input_is_animated_webp) {
            LOG_DEBUG("[%d/%d] Animated webp detected, using ffmpeg extraction: %s", index, total, in_path);

            char tmp_png[PATH_MAX];
            snprintf(tmp_png, sizeof(tmp_png), "%s.tmp.png", out_path);

            char esc_tmp[PATH_MAX * 2];
            esc_tmp[0] = '\0';
            escape_path_for_cmd(tmp_png, esc_tmp, sizeof(esc_tmp));
            char ffcmd[1024];
            build_ffmpeg_extract_png_cmd(ffcmd, sizeof(ffcmd), esc_in, esc_tmp, scale);

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
                platform_file_delete(tmp_png);

                if (cret == 0) return;
                LOG_WARN("[%d/%d] conversion failed rc=%d", index, total, cret);
            }
            else {
                LOG_WARN("[%d/%d] ffmpeg (png) failed rc=%d", index, total, ret_png);
                platform_file_delete(tmp_png);
            }
        }
        LOG_DEBUG("[%d/%d] Using magick for static webp or after ffmpeg failure: %s", index, total, in_path);

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
        char tmp_png[PATH_MAX];
        snprintf(tmp_png, sizeof(tmp_png), "%s.tmp.png", out_path);

        char esc_tmp[PATH_MAX * 2];
        esc_tmp[0] = '\0';
        escape_path_for_cmd(tmp_png, esc_tmp, sizeof(esc_tmp));

        {
            char extract_cmd[1024];
            build_ffmpeg_extract_png_cmd(extract_cmd, sizeof(extract_cmd), esc_in, esc_tmp, scale);
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
                platform_file_delete(tmp_png);

                if (cret == 0) return;
                LOG_WARN("[%d/%d] conversion failed rc=%d", index, total, cret);
            }
            else {
                LOG_WARN("[%d/%d] ffmpeg extraction failed rc=%d", index, total, ret_png);
                platform_file_delete(tmp_png);
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
static unsigned __stdcall debounce_generation_thread(void* args) {
#else
static void* debounce_generation_thread(void* args) {
#endif
    char* dcopy = (char*)args;
    if (!dcopy) {
#ifdef _WIN32
        return 0;
#else
        return NULL;
#endif
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

#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}

#ifdef _WIN32
static unsigned __stdcall thumbnail_generation_thread(void* args) {
#else
static void* thumbnail_generation_thread(void* args) {
#endif
    thread_args_t* thread_args = (thread_args_t*)args;
    char dir_path[PATH_MAX];
    strncpy(dir_path, thread_args->dir_path, PATH_MAX - 1);
    dir_path[PATH_MAX - 1] = '\0';
    free(args);

    LOG_INFO("Background thumbnail generation starting for: %s", dir_path);
    run_thumb_generation(dir_path);
    LOG_INFO("Background thumbnail generation finished for: %s", dir_path);

#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}

#ifdef _WIN32
static unsigned __stdcall thumb_job_thread(void* args) {
#else
static void* thumb_job_thread(void* args) {
#endif
    if (!args) {
#ifdef _WIN32
        return 0;
#else
        return NULL;
#endif
    }
    thumb_job_t* job = (thumb_job_t*)args;
    generate_thumb_c(job->input, job->output, job->scale, job->q, job->index, job->total);
    char* bn = strrchr(job->output, DIR_SEP);
    if (bn) bn = bn + 1; else bn = job->output;
    thumbdb_set(bn, job->input);

    free(job);

#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}
#ifdef _WIN32
static unsigned __stdcall thumb_maintenance_thread(void* args) {
#else
static void* thumb_maintenance_thread(void* args) {
#endif
    int interval = args ? *((int*)args) : 300;
    int ival = interval;
    free(args);
    thumbdb_open();
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

            if (!is_dir(per_thumbs_root))
                platform_make_dir(per_thumbs_root);

            char per_db[PATH_MAX];
            snprintf(per_db, sizeof(per_db), "%s" DIR_SEP_STR "thumbs.db", per_thumbs_root);
            thumbdb_open_for_dir(per_db);

            if (thumbdb_tx_begin() != 0) {
                LOG_WARN("thumbs: failed to start tx for gallery %s", gallery);
            }

            diriter it;
            if (!dir_open(&it, gallery)) {
                thumbdb_tx_abort();
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

            if (thumbdb_tx_commit() != 0) {
                LOG_WARN("thumbs: failed to commit tx for gallery %s, aborting", gallery);
                thumbdb_tx_abort();
            }
            else {
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
    thread_create_detached((void* (*)(void*))thumb_maintenance_thread, arg);
}
void start_auto_thumb_watcher(const char* dir_path) {
    if (!dir_path) return;

    if (!watcher_mutex_inited) {
        if (thread_mutex_init(&watcher_mutex) == 0)
            watcher_mutex_inited = 1;
    }

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
        LOG_ERROR("Failed to allocate watcher node");
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
        LOG_INFO("Thumbnail generation already running for: %s", dir);
        return;
    }

    if (lock_ret != 0) {
        LOG_WARN("Failed to create lock file %s", lock_path);
        return;
    }

    LOG_DEBUG("run_thumb_generation: acquired lock %s", lock_path);

    char per_db[PATH_MAX];
    snprintf(per_db, sizeof(per_db), "%s" DIR_SEP_STR "thumbs.db", per_thumbs_root);
    int tbr = thumbdb_open_for_dir(per_db);
    if (tbr != 0) {
        LOG_WARN("run_thumb_generation: thumbdb_open_for_dir failed for %s (rc=%d)", per_db, tbr);
    }
    else {
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
            printf("[SKIPPED] %d files skipped in thumbs_dir: %s\n", curr->count, curr->dir);
        skip_counter_t* tmp = curr;
        curr = curr->next;
        free(tmp);
    }
    prog->skip_head = NULL;
}
int get_media_dimensions(const char* path, int* width, int* height) {
    if (!path || !width || !height) return 0;
    if (!is_path_safe(path)) return 0;

    char norm_path[PATH_MAX];
    strncpy(norm_path, path, PATH_MAX - 1);
    norm_path[PATH_MAX - 1] = '\0';
    normalize_path(norm_path);

    char esc_path[PATH_MAX * 2];
    escape_path_for_cmd(norm_path, esc_path, sizeof(esc_path));

    const char* ext = strrchr(norm_path, '.');
    if (ext) {
        if (!strcasecmp(ext, ".webp")) {
            int w = 0, h = 0;
            if (get_webp_dimensions(norm_path, &w, &h) && w > 0 && h > 0) {
                *width = w; *height = h; return 1;
            }
            return 0;
        }

        if (!strcasecmp(ext, ".jpg") || !strcasecmp(ext, ".jpeg") || !strcasecmp(ext, ".png") ||
            !strcasecmp(ext, ".gif") || !strcasecmp(ext, ".bmp") || !strcasecmp(ext, ".tif") ||
            !strcasecmp(ext, ".tiff")) {
            int w = 0, h = 0;
            if (get_image_dimensions_from_file(norm_path, &w, &h) && w > 0 && h > 0) {
                *width = w; *height = h; return 1;
            }
            return 0;
        }
    }

    char dim_cmd[PATH_MAX * 3];
    snprintf(dim_cmd, sizeof(dim_cmd), "ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0:s=x \"%s\"", esc_path);


    FILE* fp = platform_popen_direct(dim_cmd, "r");
    if (!fp) return 0;

    char buf[256];
    buf[0] = '\0';
    int found = 0, w = 0, h = 0;

    while (fgets(buf, sizeof(buf), fp)) {
        char* line = buf;
        while (*line && (*line == ' ' || *line == '\t' || *line == '\r' || *line == '\n')) line++;
        if (sscanf(line, "%dx%d", &w, &h) == 2 && w > 0 && h > 0) {
            found = 1;
            break;
        }
    }

    platform_pclose_direct(fp);
    if (!found) return 0;

    *width = w;
    *height = h;
    return 1;
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
void ensure_thumbs_in_dir(const char* dir, progress_t * prog) {
    LOG_DEBUG("ensure_thumbs_in_dir: enter for %s", dir);

    if (!prog) {
        int quick = dir_has_missing_thumbs_shallow(dir, 0);
        if (quick)
            start_background_thumb_generation(dir);
        else
            start_auto_thumb_watcher(dir);
        return;
    }

    if (prog)
        LOG_DEBUG("ensure_thumbs_in_dir: prog total_files=%zu processed=%zu", prog->total_files, prog->processed_files);

    diriter it;
    if (!dir_open(&it, dir)) return;

    const char* name;
    while ((name = dir_next(&it))) {
        if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) continue;
        char full[PATH_MAX];
        path_join(full, dir, name);
        if (is_dir(full)) continue;
        const char* ext = strrchr(name, '.');
        if (!ext) continue;
        bool is_media = has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS);
        if (!is_media) continue;
        struct stat st;
        if (stat(full, &st) != 0) { add_skip(prog, "STAT_FAIL", full); continue; }
        char thumb_small_rel[PATH_MAX], thumb_large_rel[PATH_MAX];
        get_thumb_rel_names(full, name, thumb_small_rel, sizeof(thumb_small_rel), thumb_large_rel, sizeof(thumb_large_rel));
        char thumbs_root[PATH_MAX];
        get_thumbs_root(thumbs_root, sizeof(thumbs_root));
        char safe_dir_name[PATH_MAX];
        make_safe_dir_name_from(dir, safe_dir_name, sizeof(safe_dir_name));
        char per_thumbs_root[PATH_MAX];
        snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir_name);
        if (!is_dir(per_thumbs_root))platform_make_dir(per_thumbs_root);
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
        }
        else {
            need_small = !is_file(thumb_small);
            need_large = !is_file(thumb_large);
        }

        LOG_DEBUG("ensure_thumbs_in_dir: media=%s need_small=%d need_large=%d", full, need_small, need_large);

        if (need_small) {
            prog->processed_files++;
            LOG_INFO("Scheduling small thumb for %s -> %s", full, thumb_small);

            thumb_job_t* job = calloc(1, sizeof(thumb_job_t));
            if (job) {
                strncpy(job->input, full, PATH_MAX - 1);
                job->input[PATH_MAX - 1] = '\0';
                strncpy(job->output, thumb_small, PATH_MAX - 1);
                job->output[PATH_MAX - 1] = '\0';
                job->scale = THUMB_SMALL_SCALE;
                job->q = THUMB_SMALL_QUALITY;
                job->index = prog->processed_files;
                job->total = (int)prog->total_files;

                if (thread_create_detached((void* (*)(void*))thumb_job_thread, job) != 0) {
                    LOG_ERROR("Failed to spawn thumb worker thread, falling back to inline generation");
                    generate_thumb_c(full, thumb_small, THUMB_SMALL_SCALE, THUMB_SMALL_QUALITY, prog->processed_files, prog->total_files);
                    char* bn = strrchr(thumb_small, DIR_SEP);
                    if (bn) bn = bn + 1; else bn = thumb_small;
                    thumbdb_set(bn, full);
                    free(job);
                }
            }
            else {
                LOG_ERROR("Failed to allocate thumb job, generating inline");
                generate_thumb_c(full, thumb_small, THUMB_SMALL_SCALE, THUMB_SMALL_QUALITY, prog->processed_files, prog->total_files);
                char* bn = strrchr(thumb_small, DIR_SEP);
                if (bn) bn = bn + 1; else bn = thumb_small;
                thumbdb_set(bn, full);
            }
        }

        if (need_large) {
            LOG_INFO("Scheduling large thumb for %s -> %s", full, thumb_large);

            thumb_job_t* job = calloc(1, sizeof(thumb_job_t));
            if (job) {
                strncpy(job->input, full, PATH_MAX - 1);
                job->input[PATH_MAX - 1] = '\0';
                strncpy(job->output, thumb_large, PATH_MAX - 1);
                job->output[PATH_MAX - 1] = '\0';
                job->scale = THUMB_LARGE_SCALE;
                job->q = THUMB_LARGE_QUALITY;
                job->index = prog->processed_files;
                job->total = (int)prog->total_files;

                if (thread_create_detached((void* (*)(void*))thumb_job_thread, job) != 0) {
                    LOG_ERROR("Failed to spawn thumb worker thread, falling back to inline generation");
                    generate_thumb_c(full, thumb_large, THUMB_LARGE_SCALE, THUMB_LARGE_QUALITY, prog->processed_files, prog->total_files);
                    char* bn = strrchr(thumb_large, DIR_SEP);
                    if (bn) bn = bn + 1; else bn = thumb_large;
                    thumbdb_set(bn, full);
                    free(job);
                }
            }
            else {
                LOG_ERROR("Failed to allocate thumb job, generating inline");
                generate_thumb_c(full, thumb_large, THUMB_LARGE_SCALE, THUMB_LARGE_QUALITY, prog->processed_files, prog->total_files);
                char* bn = strrchr(thumb_large, DIR_SEP);
                if (bn) bn = bn + 1; else bn = thumb_large;
                thumbdb_set(bn, full);
            }
        }
    }
    dir_close(&it);
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
            if (!tmp) break;
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
        if (p_strcmp(tname_copy, ".") == 0 || p_strcmp(tname_copy, "..") == 0 ||
            p_strcmp(tname_copy, "skipped.log") == 0 || p_strcmp(tname_copy, ".nogallery") == 0 ||
            p_strcmp(tname_copy, ".thumbs.lock") == 0) continue;

        if (strstr(tname_copy, "-small-") || strstr(tname_copy, "-large-")) {
            char thumb_full_m[PATH_MAX];
            path_join(thumb_full_m, thumbs_path, tname_copy);
            if (platform_file_delete(thumb_full_m) != 0) LOG_WARN("Failed to delete malformed thumb: %s", thumb_full_m);
            else {
                LOG_INFO("Removed malformed thumb: %s", thumb_full_m);
                add_skip(prog, "MALFORMED_REMOVED", thumb_full_m);
            }
            continue;
        }

        if (!strstr(tname_copy, "-small.") && !strstr(tname_copy, "-large.")) continue;
        bool found = false;
        for (size_t ei = 0; ei < expect_count; ++ei) if (p_strcmp(tname, expects[ei]) == 0) { found = true; break; }
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
    }
    dir_close(&tit);
    for (size_t i = 0; i < expect_count; ++i) free(expects[i]);
    free(expects);
}
