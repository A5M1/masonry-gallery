#include "api_handlers.h"
#include "directory.h"
#include "http.h"
#include "utils.h"
#include "logging.h"
#include "config.h"
#include "tinyjson_combined.h"
#include "common.h"
#include "crypto.h"
#include "thread_pool.h"
#include "platform.h"
#include <fcntl.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <time.h>
#ifdef _WIN32
#include <windows.h>
#endif
#ifndef _WIN32
#include <sys/inotify.h>
#include <unistd.h>
#endif

#define MAX_FFMPEG 2

static atomic_int ffmpeg_active = ATOMIC_VAR_INIT(0);

static void sleep_ms(int ms) { platform_sleep_ms(ms); }

const char* IMAGE_EXTS[] = { ".jpg",".jpeg",".png",".gif",".webp",NULL };
const char* VIDEO_EXTS[] = { ".mp4",".webm",".ogg",".webp",NULL };
static char* g_request_headers = NULL;
static void appendf(char** pbuf, size_t* pcap, size_t* pused, const char* fmt, ...);
static void ensure_json_buf(char** pbuf, size_t* pcap, size_t used, size_t need) {
	if (*pcap - used < need + 1024) { 
		size_t newcap = MAX((*pcap + need) * 2, *pcap + 8192);
		char* new_buf = realloc(*pbuf, newcap);
		if (!new_buf) {
			LOG_ERROR("Failed to reallocate JSON buffer");
			return;
		}
		*pbuf = new_buf;
		*pcap = newcap;
	}
}
static bool has_nogallery(const char* dir) {
	char p[PATH_MAX];
	path_join(p, dir, ".nogallery");
	return is_file(p);
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
static void get_thumb_rel_names(const char* full_path, const char* filename, char* small_rel, size_t small_len, char* large_rel, size_t large_len) {
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
bool has_media_rec(const char* dir) {
	if (has_nogallery(dir)) return false;
	diriter it;
	if (!dir_open(&it, dir)) return false;
	const char* name;
	bool found = false;
	while ((name = dir_next(&it)) && !found) {
		if (!strcmp(name, ".") || !strcmp(name, "..") || !strcmp(name, "thumbs")) continue;
		char full[PATH_MAX];
		path_join(full, dir, name);
		if (is_file(full)) found = has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS);
	}
	if (!found) {
		dir_close(&it);
		if (!dir_open(&it, dir)) return false;
		while ((name = dir_next(&it)) && !found) {
			if (!strcmp(name, ".") || !strcmp(name, "..") || !strcmp(name, "thumbs")) continue;
			char full[PATH_MAX];
			path_join(full, dir, name);
			if (is_dir(full) && !has_nogallery(full)) found = has_media_rec(full);
		}
	}
	dir_close(&it);
	return found;
}
typedef struct skip_counter {
	char dir[PATH_MAX];
	int count;
	struct skip_counter* next;
} skip_counter_t;
typedef struct {
	size_t total_files;
	size_t processed_files;
	char thumbs_dir[PATH_MAX];
	skip_counter_t* skip_head;
} progress_t;
static void add_skip(progress_t* prog, const char* reason, const char* path) {
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
static void print_skips(progress_t* prog) {
	skip_counter_t* curr = prog->skip_head;
	while (curr) {
		if (curr->count > 0)
			printf("[SKIPPED] %d files skipped in thumbs_dir: %s\n", curr->count, curr->dir);
		skip_counter_t* tmp = curr;curr = curr->next;free(tmp);
	}
	prog->skip_head = NULL;
}
static int is_newer(const char* src, const char* dst) {
	struct stat s, d;
	if (stat(src, &s) != 0)return 0;
	if (stat(dst, &d) != 0)return 1;
	return s.st_mtime > d.st_mtime;
}
static int is_valid_media(const char* path) {
	struct stat st;
	if (stat(path, &st) != 0 || st.st_size < 16)return 0;
#ifdef _WIN32
	char cmd[PATH_MAX * 4];
	snprintf(cmd, sizeof(cmd), "ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 \"%s\" > nul 2>&1", path);
#else
	char cmd[PATH_MAX * 4];
	snprintf(cmd, sizeof(cmd), "ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 \"%s\" > /dev/null 2>&1", path);
#endif
	return system(cmd) == 0;
}
static int is_decodable(const char* path) {
	char cmd[PATH_MAX * 3];
#ifdef _WIN32
	snprintf(cmd, sizeof(cmd), "ffprobe -v error -i \"%s\" > nul 2>&1", path);
#else
	snprintf(cmd, sizeof(cmd), "ffprobe -v error -i \"%s\" > /dev/null 2>&1", path);
#endif
	return system(cmd) == 0;
}
static void generate_thumb_c(const char* input, const char* output, int scale, int q, int index, int total) {
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
static int get_media_dimensions(const char* path, int* width, int* height) {
	if (!path || !width || !height) return 0;
	char cmd[PATH_MAX * 3];
	char tmp_path[PATH_MAX];
	strncpy(tmp_path, path, PATH_MAX - 1); tmp_path[PATH_MAX - 1] = '\0';
	normalize_path(tmp_path);
#ifdef _WIN32
	snprintf(cmd, sizeof(cmd), "ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0:s=x \"%s\" 2> nul", tmp_path);
	FILE* f = _popen(cmd, "r");
#else
	snprintf(cmd, sizeof(cmd), "ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0:s=x \"%s\" 2> /dev/null", tmp_path);
	FILE* f = popen(cmd, "r");
#endif
	if (!f) return 0;
	char buf[128];
	if (!fgets(buf, sizeof(buf), f)) {
#ifdef _WIN32
		_pclose(f);
#else
		pclose(f);
#endif
		return 0;
	}
#ifdef _WIN32
	_pclose(f);
#else
	pclose(f);
#endif
	int w = 0, h = 0;
	if (sscanf(buf, "%dx%d", &w, &h) != 2) return 0;
	if (w <= 0 || h <= 0) return 0;
	*width = w; *height = h;
	return 1;
}

static void html_escape(const char* src, char* dst, size_t dst_len) {
	if (!src || !dst || dst_len == 0) return;
	size_t j = 0;
	for (size_t i = 0; src[i] && j + 6 < dst_len; ++i) {
		unsigned char c = (unsigned char)src[i];
		if (c == '&') {
			const char* e = "&amp;"; size_t k = 0; while (e[k] && j + 1 < dst_len) dst[j++] = e[k++];
		}
		else if (c == '<') {
			const char* e = "&lt;"; size_t k = 0; while (e[k] && j + 1 < dst_len) dst[j++] = e[k++];
		}
		else if (c == '>') {
			const char* e = "&gt;"; size_t k = 0; while (e[k] && j + 1 < dst_len) dst[j++] = e[k++];
		}
		else if (c == '"') {
			const char* e = "&quot;"; size_t k = 0; while (e[k] && j + 1 < dst_len) dst[j++] = e[k++];
		}
		else if (c == '\'') {
			const char* e = "&#39;"; size_t k = 0; while (e[k] && j + 1 < dst_len) dst[j++] = e[k++];
		}
		else {
			dst[j++] = c;
		}
	}
	dst[j] = '\0';
}

static int is_under_gallery_root(const char* target_real);
static int resolve_and_validate_target(const char* base_dir, const char* dirparam, char* target_real_out, size_t outlen, char* base_real_out, size_t base_outlen);

char* generate_media_fragment(const char* base_dir, const char* dirparam, int page, size_t* out_len) {
	char target[PATH_MAX];
	snprintf(target, sizeof(target), "%s/%s", base_dir, dirparam ? dirparam : "");
	normalize_path(target);
	char target_real[PATH_MAX]; char base_real[PATH_MAX];
	if (!resolve_and_validate_target(base_dir, dirparam, target_real, sizeof(target_real), base_real, sizeof(base_real))) { if (out_len) *out_len = 0; return NULL; }
	
	char** files = NULL; size_t n = 0, alloc = 0;
	diriter it; if (dir_open(&it, target_real)) {
		const char* name; while ((name = dir_next(&it))) {
			if (!strcmp(name, ".") || !strcmp(name, "..")) continue;
			if (has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS)) {
				if (n == alloc) { alloc = alloc ? alloc * 2 : 64; files = realloc(files, alloc * sizeof(char*)); }
				files[n++] = strdup(name);
			}
		}
		dir_close(&it);
	}
	if (n == 0) { if (files) free(files); if (out_len) *out_len = 0; return NULL; }
	qsort(files, n, sizeof(char*), ci_cmp);
	int total = (int)n; int totalPages = (total + ITEMS_PER_PAGE - 1) / ITEMS_PER_PAGE; if (totalPages == 0) totalPages = 1; if (page < 1) page = 1; if (page > totalPages) page = totalPages;
	int start = (page - 1) * ITEMS_PER_PAGE; int end = start + ITEMS_PER_PAGE; if (end > total) end = total;

	size_t hcap = 8192; char* hbuf = malloc(hcap); if (!hbuf) { for (size_t i = 0;i < n;i++) free(files[i]); free(files); if (out_len) *out_len = 0; return NULL; }
	size_t hused = 0;
	appendf(&hbuf, &hcap, &hused, "<div class=\"masonry-fragment\" data-page=\"%d\" data-hasmore=\"%d\">", page, (page < totalPages) ? 1 : 0);
	for (int i = start;i < end;i++) {
		char full_path[PATH_MAX]; path_join(full_path, target_real, files[i]);
		char relurl[PATH_MAX];
		const char* r = full_path;
		size_t j = 0;
		if (target_real[0]) {
			size_t target_len = strlen(target_real);
			if (target_len > 0 && strncmp(full_path, target_real, target_len) == 0) {
				r = full_path + target_len;
				if (*r == DIR_SEP) r++;
			} else {
				size_t base_len = strlen(base_real);
				if (base_len > 0 && strncmp(full_path, base_real, base_len) == 0) {
					r = full_path + base_len;
					if (*r == DIR_SEP) r++;
				}
			}
		} else {
			size_t base_len = strlen(base_real);
			if (base_len > 0 && strncmp(full_path, base_real, base_len) == 0) {
				r = full_path + base_len;
				if (*r == DIR_SEP) r++;
			}
		}
		for (size_t k = 0; r[k] && j < PATH_MAX - 1; k++) relurl[j++] = (r[k] == '\\') ? '/' : r[k]; relurl[j] = '\0';
	char small_rel[PATH_MAX]; char large_rel[PATH_MAX];
	get_thumb_rel_names(full_path, files[i], small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));
	char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
		if (!is_dir(thumbs_root)) platform_make_dir(thumbs_root);
		char small_fs[PATH_MAX]; char large_fs[PATH_MAX]; snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", thumbs_root, small_rel); snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", thumbs_root, large_rel);
		int small_exists = is_file(small_fs); int large_exists = is_file(large_fs);
		char href[PATH_MAX]; if (dirparam && dirparam[0]) snprintf(href, sizeof(href), "/images/%s/%s", dirparam, relurl); else snprintf(href, sizeof(href), "/images/%s", relurl);
		char href_esc[PATH_MAX]; html_escape(href, href_esc, sizeof(href_esc)); char small_url[PATH_MAX] = ""; char large_url[PATH_MAX] = ""; if (small_exists || large_exists) { snprintf(small_url, sizeof(small_url), "/images/thumbs/%s", small_rel); snprintf(large_url, sizeof(large_url), "/images/thumbs/%s", large_rel); }
		char small_esc[PATH_MAX]; char large_esc[PATH_MAX]; html_escape(small_url, small_esc, sizeof(small_esc)); html_escape(large_url, large_esc, sizeof(large_esc));
		int img_w = 0, img_h = 0; char dim_attr[64]; dim_attr[0] = '\0';
		if (get_media_dimensions(full_path, &img_w, &img_h)) {
			snprintf(dim_attr, sizeof(dim_attr), " width=\"%d\" height=\"%d\"", img_w, img_h);
		}
		int is_video = has_ext(files[i], VIDEO_EXTS);
		int thumb_status = small_exists ? 1 : 0;
		if (is_video) {
			appendf(&hbuf, &hcap, &hused, "<div class=\"masonry-item\" data-type=\"video\"><a data-fancybox=\"gallery\" href=\"%s\" data-thumb-status=\"%d\" data-type=\"video\" data-src=\"%s\">", href_esc, thumb_status, href_esc);
		} else {
			appendf(&hbuf, &hcap, &hused, "<div class=\"masonry-item\" data-type=\"image\"><a data-fancybox=\"gallery\" href=\"%s\" data-thumb-status=\"%d\">", href_esc, thumb_status);
		}
		if (small_exists)
			appendf(&hbuf, &hcap, &hused, "<img src=\"%s\" loading=\"lazy\" data-thumb-small=\"%s\" data-thumb-large=\"%s\"%s class=\"thumb-img\">", small_esc, small_esc, large_esc, dim_attr);
		else
			appendf(&hbuf, &hcap, &hused, "<img src=\"/images/placeholder.jpg\" class=\"thumb-img\"%s>", dim_attr);
		appendf(&hbuf, &hcap, &hused, "</a></div>");
	}
	appendf(&hbuf, &hcap, &hused, "</div>");
	for (size_t ii = 0; ii < n; ++ii) free(files[ii]); free(files);
	if (out_len) *out_len = hused; return hbuf;
}

static void appendf(char** pbuf, size_t* pcap, size_t* pused, const char* fmt, ...) {
	va_list ap;
	va_start(ap, fmt);
	int need = vsnprintf(NULL, 0, fmt, ap);
	va_end(ap);
	if (need < 0) return;
	ensure_json_buf(pbuf, pcap, *pused, (size_t)need + 16);
	va_start(ap, fmt);
	vsnprintf(*pbuf + *pused, *pcap - *pused, fmt, ap);
	va_end(ap);
	*pused += (size_t)need;
}

static void count_media_in_dir(const char* dir, progress_t* prog) {
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
static void ensure_thumbs_in_dir(const char* dir, progress_t* prog) {
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
static void clean_orphan_thumbs(const char* dir, progress_t* prog) {
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
static int dir_has_missing_thumbs(const char* dir, int videos_only) {
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
static void run_thumb_generation(const char* dir);
#ifdef _WIN32
static unsigned __stdcall thumbnail_generation_thread(void* args);
#else
static void* thumbnail_generation_thread(void* args);
#endif
static void run_thumb_generation(const char* dir);

typedef struct watcher_node {
	char dir[PATH_MAX];
	struct watcher_node* next;
} watcher_node_t;

static watcher_node_t* watcher_head = NULL;
static thread_mutex_t watcher_mutex;
static int watcher_mutex_inited = 0;

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

#ifdef _WIN32
static unsigned __stdcall thumbnail_watcher_thread(void* args) {
	char* dir = (char*)args;
	if (!dir) return 0;
	LOG_INFO("Thumbnail watcher (event) started for: %s", dir);
	HANDLE hChange = FindFirstChangeNotificationA(dir, FALSE, FILE_NOTIFY_CHANGE_FILE_NAME | FILE_NOTIFY_CHANGE_SIZE | FILE_NOTIFY_CHANGE_LAST_WRITE);
	if (hChange == INVALID_HANDLE_VALUE) { LOG_WARN("FindFirstChangeNotification failed for %s", dir); free(dir); return 0; }
	for (;;) {
		DWORD wait = WaitForSingleObject(hChange, INFINITE);
		if (wait == WAIT_OBJECT_0) {
			LOG_INFO("Watcher event for: %s", dir);
			thread_args_t* targ = malloc(sizeof(thread_args_t));
			if (targ) { strncpy(targ->dir_path, dir, PATH_MAX - 1); targ->dir_path[PATH_MAX - 1] = '\0'; if (thread_create_detached((void* (*)(void*))thumbnail_generation_thread, targ) != 0) { LOG_ERROR("Failed to spawn generation thread from watcher for %s", dir); free(targ); } }
			if (FindNextChangeNotification(hChange) == FALSE) { LOG_WARN("FindNextChangeNotification failed for %s", dir); break; }
		}
	}
	FindCloseChangeNotification(hChange);
	free(dir);
	return 0;
}
#else
static void* thumbnail_watcher_thread(void* args) {
	char* dir = (char*)args;
	if (!dir) return NULL;
	LOG_INFO("Thumbnail watcher (inotify) started for: %s", dir);
	int fd = inotify_init1(IN_NONBLOCK);
	if (fd < 0) { LOG_WARN("inotify_init1 failed for %s", dir); free(dir); return NULL; }
	int wd = inotify_add_watch(fd, dir, IN_CREATE | IN_MOVED_TO | IN_MODIFY);
	if (wd < 0) { LOG_WARN("inotify_add_watch failed for %s", dir); close(fd); free(dir); return NULL; }
	char buf[4096];
	for (;;) {
		int len = read(fd, buf, sizeof(buf));
		if (len > 0) {
			LOG_INFO("Watcher event for: %s", dir);
			thread_args_t* targ = malloc(sizeof(thread_args_t));
			if (targ) { strncpy(targ->dir_path, dir, PATH_MAX - 1); targ->dir_path[PATH_MAX - 1] = '\0'; if (thread_create_detached((void* (*)(void*))thumbnail_generation_thread, targ) != 0) { LOG_ERROR("Failed to spawn generation thread from watcher for %s", dir); free(targ); } }
		}
		sleep_ms(1000);
	}
	inotify_rm_watch(fd, wd);
	close(fd);
	free(dir);
	return NULL;
}
#endif

static void start_auto_thumb_watcher(const char* dir_path) {
	if (!dir_path) return;
	if (!watcher_mutex_inited) { if (thread_mutex_init(&watcher_mutex) == 0) watcher_mutex_inited = 1; }
	thread_mutex_lock(&watcher_mutex);
	watcher_node_t* cur = watcher_head;
	while (cur) { if (strcmp(cur->dir, dir_path) == 0) { thread_mutex_unlock(&watcher_mutex); return; } cur = cur->next; }
	watcher_node_t* node = malloc(sizeof(watcher_node_t));
	if (!node) { thread_mutex_unlock(&watcher_mutex); LOG_ERROR("Failed to allocate watcher node"); return; }
	strncpy(node->dir, dir_path, PATH_MAX - 1); node->dir[PATH_MAX - 1] = '\0'; node->next = watcher_head; watcher_head = node;
	thread_mutex_unlock(&watcher_mutex);

	char* arg = strdup(dir_path);
	if (!arg) { remove_watcher_node(dir_path); LOG_ERROR("Failed to allocate watcher arg"); return; }
	if (thread_create_detached((void* (*)(void*))thumbnail_watcher_thread, arg) != 0) {
		LOG_ERROR("Failed to create watcher thread for %s", dir_path);
		free(arg);
		remove_watcher_node(dir_path);
	}
}
static void run_thumb_generation(const char* dir) {
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
	int lock_acquired = 0;
#ifdef _WIN32
	{
		HANDLE h = CreateFileA(lock_path, GENERIC_WRITE, 0, NULL, CREATE_NEW, FILE_ATTRIBUTE_NORMAL, NULL);
		if (h == INVALID_HANDLE_VALUE) {
			DWORD err = GetLastError();
			if (err == ERROR_FILE_EXISTS) {
				LOG_INFO("Thumbnail generation already running for: %s", dir);
				return;
			}
			else {
				LOG_WARN("Failed to create lock file %s (err=%lu)", lock_path, err);
				return;
			}
		}
		CloseHandle(h);
		lock_acquired = 1;
	}
#else
	{
		int fd = open(lock_path, O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
		if (fd == -1) {
			if (errno == EEXIST) {
				LOG_INFO("Thumbnail generation already running for: %s", dir);
				return;
			}
			else {
				LOG_WARN("Failed to create lock file %s: %s", lock_path, strerror(errno));
				return;
			}
		}
		close(fd);
		lock_acquired = 1;
	}
#endif
	count_media_in_dir(dir_used, &prog);
	LOG_INFO("Found %zu media files in %s", prog.total_files, dir_used);
	ensure_thumbs_in_dir(dir_used, &prog);

	clean_orphan_thumbs(dir_used, &prog);

	print_skips(&prog);

	
    if (lock_acquired) {
    platform_file_delete(lock_path);
    }
}

static int is_under_gallery_root(const char* target_real) {
	size_t count = 0;
	char** folders = get_gallery_folders(&count);
	if (!folders) return 0;
	char folder_real[PATH_MAX];
	int ok = 0;
	for (size_t i = 0; i < count; ++i) {
		if (!real_path(folders[i], folder_real)) continue;
		if (safe_under(folder_real, target_real)) { ok = 1; break; }
	}
	(void)folders;
	return ok;
}

static int resolve_and_validate_target(const char* base_dir, const char* dirparam, char* target_real_out, size_t outlen, char* base_real_out, size_t base_outlen) {
	if (!target_real_out || outlen == 0) return 0;
	char target[PATH_MAX]; snprintf(target, sizeof(target), "%s/%s", base_dir, dirparam ? dirparam : ""); normalize_path(target);
	if (!real_path(target, target_real_out)) return 0;
	if (!is_dir(target_real_out)) return 0;
	if (!is_under_gallery_root(target_real_out)) return 0;
	if (base_real_out && base_outlen > 0) { real_path(base_dir, base_real_out); }
	return 1;
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

void handle_api_regenerate_thumbs(int c, char* qs, bool keep_alive) {
	char dirparam[PATH_MAX] = { 0 };
	if (qs) { char* v = query_get(qs, "dir");if (v) { strncpy(dirparam, v, PATH_MAX - 1);SAFE_FREE(v); } }
	char target[PATH_MAX]; snprintf(target, sizeof(target), "%s/%s", BASE_DIR, dirparam);
	normalize_path(target);
	char target_real[PATH_MAX];
	char base_real[PATH_MAX];
	if (!resolve_and_validate_target(BASE_DIR, dirparam, target_real, sizeof(target_real), base_real, sizeof(base_real))) { const char* msg = "{\"error\":\"Invalid directory\"}"; send_header(c, 400, "Bad Request", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive); send(c, msg, (int)strlen(msg), 0); return; }
	start_background_thumb_generation(target_real);
	const char* msg = "{\"status\":\"accepted\",\"message\":\"Thumbnail regeneration started.\"}";
	send_header(c, 202, "Accepted", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
	send(c, msg, (int)strlen(msg), 0);
}
static char* json_comma_safe(char* ptr, size_t * remLen) {
	if (*remLen < 10) return ptr;
	*ptr++ = ',';
	(*remLen)--;
	return ptr;
}
static char* build_folder_tree_json(char** pbuf, size_t * cap, size_t * used, const char* dir, const char* root) {
	ensure_json_buf(pbuf, cap, *used, 4096);
	char* ptr = *pbuf + *used;
	if (!is_dir(dir) || has_nogallery(dir) || !has_media_rec(dir)) {
		ptr = json_null(ptr, NULL, cap);
		*used = ptr - *pbuf;
		return ptr;
	}
	ptr = json_objOpen(ptr, NULL, cap);
	*used = ptr - *pbuf;
	const char* base = strrchr(dir, DIR_SEP);
	base = base ? base + 1 : dir;
	ptr = json_str(ptr, "name", base, cap);
	*used = ptr - *pbuf;
	char rroot[PATH_MAX], rdir[PATH_MAX];
	real_path(root, rroot);
	real_path(dir, rdir);
	const char* r = rdir;
	size_t rl = strlen(rroot);
	if (safe_under(rroot, rdir)) r = rdir + rl + ((rdir[rl] == DIR_SEP) ? 1 : 0);
	char relurl[PATH_MAX]; size_t j = 0;
	for (size_t i = 0; r[i] && j < PATH_MAX - 1; i++) {
		char ch = r[i]; if (ch == '\\') ch = '/'; relurl[j++] = ch;
	}
	relurl[j] = '\0';
	ptr = json_str(ptr, "path", relurl, cap);
	*used = ptr - *pbuf;
	ptr = json_arrOpen(ptr, "children", cap);
	*used = ptr - *pbuf;
	char** names = NULL; size_t n = 0, alloc = 0;
	diriter it;
	if (dir_open(&it, dir)) {
		const char* name;
		while ((name = dir_next(&it))) {
			if (!name) continue;
			if (!strcmp(name, ".") || !strcmp(name, "..") || !strcmp(name, "thumbs")) continue;
			if (strlen(name) == 0) continue;
			char full[PATH_MAX] = { 0 };
			path_join(full, dir, name);
			full[PATH_MAX - 1] = '\0';
			if (is_dir(full) && !has_nogallery(full) && has_media_rec(full)) {
				if (n == alloc) { alloc = alloc ? alloc * 2 : 16; names = realloc(names, alloc * sizeof(char*)); }
				names[n++] = strdup(name);
			}
		}
		dir_close(&it);
	}
	qsort(names, n, sizeof(char*), ci_cmp);
	for (size_t i = 0;i < n;i++) {
		if (i > 0) {
			ptr = *pbuf + *used;
			ptr = json_comma_safe(ptr, cap);
			*used = ptr - *pbuf;
		}
		char child_full[PATH_MAX]; path_join(child_full, dir, names[i]);
		ptr = *pbuf + *used;
		ptr = build_folder_tree_json(pbuf, cap, used, child_full, root);
		*used = ptr - *pbuf;
	}
	for (size_t i = 0;i < n;i++) free(names[i]);
	free(names);
	ptr = json_arrClose(ptr, cap);
	ptr = json_objClose(ptr, cap);
	*used = ptr - *pbuf;
	return ptr;
}
void handle_api_tree(int c, bool keep_alive) {
	size_t count;
	char** folders = get_gallery_folders(&count);
	size_t cap = 8192;
	char* buf = malloc(cap);
	size_t used = 0;
	if (count == 1) {
		build_folder_tree_json(&buf, &cap, &used, folders[0], folders[0]);
	}
	else {
		char* ptr = buf;
		ptr = json_objOpen(ptr, NULL, &cap); used = ptr - buf;
		ptr = json_str(ptr, "name", "root", &cap); used = ptr - buf;
		ptr = json_str(ptr, "path", "", &cap); used = ptr - buf;
		ptr = json_arrOpen(ptr, "children", &cap); used = ptr - buf;
		for (size_t i = 0; i < count; i++) {
			if (i > 0) { ptr = json_comma_safe(ptr, &cap); used = ptr - buf; }
			build_folder_tree_json(&buf, &cap, &used, folders[i], folders[i]);
		}
		ptr = json_arrClose(ptr, &cap); used = ptr - buf;
		ptr = json_objClose(ptr, &cap); used = ptr - buf;
	}
	send_header(c, 200, "OK", "application/json; charset=utf-8", (long)used, NULL, 0, keep_alive);
	send(c, buf, (int)used, 0);
	free(buf);
}
void handle_api_folders(int c, char* qs, bool keep_alive) {
	char dirparam[PATH_MAX] = { 0 };
	if (qs) {
		char* v = query_get(qs, "dir");
		if (v) { strncpy(dirparam, v, PATH_MAX - 1); dirparam[PATH_MAX - 1] = '\0'; SAFE_FREE(v); }
	}
	char target[PATH_MAX]; snprintf(target, sizeof(target), "%s/%s", BASE_DIR, dirparam); normalize_path(target);
	char base_real[PATH_MAX], target_real[PATH_MAX];
	if (!real_path(BASE_DIR, base_real) || !real_path(target, target_real)
		|| !safe_under(base_real, target_real) || !is_dir(target_real)) {
		const char* msg = "{\"error\":\"Invalid directory\"}";
		send_header(c, 400, "Bad Request", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
		send(c, msg, (int)strlen(msg), 0);
		return;
	}
	char** names = NULL; size_t n = 0, alloc = 0;
	diriter it;
	if (dir_open(&it, target_real)) {
		const char* name;
		while ((name = dir_next(&it))) {
			if (!strcmp(name, ".") || !strcmp(name, "..")) continue;
			char full[PATH_MAX]; path_join(full, target_real, name);
			if (is_dir(full) && !has_nogallery(full) && has_media_rec(full)) {
				if (n == alloc) { alloc = alloc ? alloc * 2 : 16; names = realloc(names, alloc * sizeof(char*)); }
				names[n++] = strdup(name);
			}
		}
		dir_close(&it);
	}
	qsort(names, n, sizeof(char*), ci_cmp);
	size_t cap = 8192; char* buf = malloc(cap); size_t len = cap; char* ptr = buf;
	ptr = json_objOpen(ptr, NULL, &len);
	ptr = json_arrOpen(ptr, "content", &len);
	for (size_t i = 0; i < n; i++) {
		if (i > 0) ptr = json_comma_safe(ptr, &len);
		char child_full[PATH_MAX]; path_join(child_full, target_real, names[i]);
		ptr = json_objOpen(ptr, NULL, &len);
		ptr = json_str(ptr, "name", names[i], &len);
		char tr[PATH_MAX]; real_path(child_full, tr);
		char relurl[PATH_MAX]; size_t j = 0;
		size_t base_len = strlen(BASE_DIR);
		const char* r = tr + base_len + ((tr[base_len] == DIR_SEP) ? 1 : 0);
		for (size_t k = 0; r[k] && j < PATH_MAX - 1; k++) { char ch = r[k]; if (ch == '\\') ch = '/'; relurl[j++] = ch; }
		relurl[j] = '\0';
		ptr = json_str(ptr, "path", relurl, &len);
		ptr = json_objClose(ptr, &len);
		free(names[i]);
	}
	free(names);
	ptr = json_arrClose(ptr, &len);
	ptr = json_str(ptr, "currentDir", dirparam, &len);
	ptr = json_bool(ptr, "isRoot", dirparam[0] == 0, &len);
	ptr = json_objClose(ptr, &len);
	send_header(c, 200, "OK", "application/json; charset=utf-8", (long)(ptr - buf), NULL, 0, keep_alive);
	send(c, buf, (int)(ptr - buf), 0);
	free(buf);
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
void handle_api_media(int c, char* qs, bool keep_alive) {
	char dirparam[PATH_MAX] = { 0 };
	int page = 1;
	int render_html = 0;
	if (qs) {
		char* v = query_get(qs, "dir");
		if (v) { strncpy(dirparam, v, PATH_MAX - 1); SAFE_FREE(v); }
		char* p = query_get(qs, "page");
		if (p) { int t = atoi(p); if (t > 0) page = t; SAFE_FREE(p); }
		char* r = query_get(qs, "render");
		if (r) { if (strcmp(r, "html") == 0) render_html = 1; SAFE_FREE(r); }
	}
	char target[PATH_MAX];
	snprintf(target, sizeof(target), "%s/%s", BASE_DIR, dirparam);
	normalize_path(target);
	char target_real[PATH_MAX];
	char base_real[PATH_MAX];
	if (!real_path(target, target_real) || !is_dir(target_real) || !is_under_gallery_root(target_real)) {
		const char* msg = "{\"error\":\"Invalid directory\"}";
		send_header(c, 400, "Bad Request", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
		send(c, msg, (int)strlen(msg), 0);
		return;
	}
	if (!real_path(BASE_DIR, base_real)) base_real[0] = '\0';
	start_background_thumb_generation(target_real);
	char** files = NULL;
	size_t n = 0, alloc = 0;
	diriter it;
	if (dir_open(&it, target_real)) {
		const char* name;
		while ((name = dir_next(&it))) {
			if (!strcmp(name, ".") || !strcmp(name, "..")) continue;
			if (has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS)) {
				if (n == alloc) {
					alloc = alloc ? alloc * 2 : 64;
					files = realloc(files, alloc * sizeof(char*));
				}
				files[n++] = strdup(name);
			}
		}
		dir_close(&it);
	}
	qsort(files, n, sizeof(char*), ci_cmp);
	int total = (int)n;
	int totalPages = (total + ITEMS_PER_PAGE - 1) / ITEMS_PER_PAGE;
	if (totalPages == 0) totalPages = 1;
	if (page < 1) page = 1;
	if (page > totalPages) page = totalPages;
	int start = (page - 1) * ITEMS_PER_PAGE;
	int end = start + ITEMS_PER_PAGE;
	if (end > total) end = total;

	if (render_html) {
		char cache_dir[PATH_MAX];
		snprintf(cache_dir, sizeof(cache_dir), "%s" DIR_SEP_STR "cache" DIR_SEP_STR "media", BASE_DIR);
		if (!is_dir(cache_dir)) mk_dir(cache_dir);
		char safe_name[PATH_MAX];
		if (dirparam[0] == 0) strncpy(safe_name, "root", sizeof(safe_name)); else {
			size_t si = 0; for (size_t ii = 0; dirparam[ii] && si < sizeof(safe_name) - 1; ii++) {
				char ch = dirparam[ii]; safe_name[si++] = (ch == '/' || ch == '\\') ? '_' : ch;
			}
			safe_name[si] = '\0';
		}
		char cache_path[PATH_MAX];
		snprintf(cache_path, sizeof(cache_path), "%s" DIR_SEP_STR "%s-%d.html", cache_dir, safe_name, page);
		if (is_file(cache_path)) {
			LOG_INFO("Serving cached media fragment: %s", cache_path);
			struct stat stc; if (stat(cache_path, &stc) == 0) {
				char etag[128]; snprintf(etag, sizeof(etag), "\"%08lx-%08lx\"", (unsigned long)stc.st_mtime, (unsigned long)stc.st_size);
				char lm[128]; struct tm* t = gmtime(&stc.st_mtime); if (t) strftime(lm, sizeof(lm), "%a, %d %b %Y %H:%M:%S GMT", t); else lm[0] = '\0';
				char* if_none = get_header_value(g_request_headers, "If-None-Match:");
				if (if_none && strstr(if_none, etag)) {
					send_header(c, 304, "Not Modified", "text/plain; charset=utf-8", 0, NULL, 0, keep_alive);
					free(files);
					return;
				}
				send_header(c, 200, "OK", "text/html; charset=utf-8", (long)stc.st_size, NULL, 0, keep_alive);
				send_file_stream(c, cache_path, NULL, keep_alive);
				free(files);
				return;
			}
			
			free(files);
			return;
		}
		size_t hcap = 8192;
		char* hbuf = malloc(hcap);
		if (!hbuf) {
			const char* msg = "Out of memory";
			send_header(c, 500, "Internal Server Error", "text/plain; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
			send(c, msg, (int)strlen(msg), 0);
			return;
		}
		size_t hused = 0;
		appendf(&hbuf, &hcap, &hused, "<div class=\"masonry-fragment\" data-page=\"%d\" data-hasmore=\"%d\">", page, (page < totalPages) ? 1 : 0);
		size_t cap = 0; char* buf = NULL; 
		(void)cap; (void)buf;
		for (int i = start; i < end; i++) {
			char full_path[PATH_MAX];
			path_join(full_path, target_real, files[i]);
			char relurl[PATH_MAX];
			const char* r = full_path;
			size_t j = 0;
			if (target_real[0]) {
				size_t target_len = strlen(target_real);
				if (target_len > 0 && strncmp(full_path, target_real, target_len) == 0) {
					r = full_path + target_len;
					if (*r == DIR_SEP) r++;
				} else {
					size_t base_len = strlen(base_real);
					if (base_len > 0 && strncmp(full_path, base_real, base_len) == 0) {
						r = full_path + base_len;
						if (*r == DIR_SEP) r++;
					}
				}
			} else {
				size_t base_len = strlen(base_real);
				if (base_len > 0 && strncmp(full_path, base_real, base_len) == 0) {
					r = full_path + base_len;
					if (*r == DIR_SEP) r++;
				}
			}
			for (size_t k = 0; r[k] && j < PATH_MAX - 1; k++) relurl[j++] = (r[k] == '\\') ? '/' : r[k];
			relurl[j] = '\0';

			char small_rel[PATH_MAX]; char large_rel[PATH_MAX];
			get_thumb_rel_names(full_path, files[i], small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));
			char thumbs_root[PATH_MAX]; snprintf(thumbs_root, sizeof(thumbs_root), "%s" DIR_SEP_STR "thumbs", BASE_DIR);
			if (!is_dir(thumbs_root)) platform_make_dir(thumbs_root);
			char small_fs[PATH_MAX]; char large_fs[PATH_MAX];
			snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", thumbs_root, small_rel);
			snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", thumbs_root, large_rel);
			int small_exists = is_file(small_fs);
			int large_exists = is_file(large_fs);

			char href[PATH_MAX];
			if (dirparam[0]) snprintf(href, sizeof(href), "/images/%s/%s", dirparam, relurl);
			else snprintf(href, sizeof(href), "/images/%s", relurl);
			char href_esc[PATH_MAX]; html_escape(href, href_esc, sizeof(href_esc));

			char small_url[PATH_MAX] = ""; char large_url[PATH_MAX] = "";
			if (small_exists || large_exists) {
				snprintf(small_url, sizeof(small_url), "/images/thumbs/%s", small_rel);
				snprintf(large_url, sizeof(large_url), "/images/thumbs/%s", large_rel);
			}
			char small_esc[PATH_MAX]; char large_esc[PATH_MAX]; html_escape(small_url, small_esc, sizeof(small_esc)); html_escape(large_url, large_esc, sizeof(large_esc));
			appendf(&hbuf, &hcap, &hused, "<div class=\"masonry-item\"><a data-fancybox=\"gallery\" href=\"%s\">", href_esc);
			if (small_exists) {
				appendf(&hbuf, &hcap, &hused, "<img src=\"/images/placeholder.jpg\" loading=\"lazy\" data-thumb-small=\"%s\" data-thumb-large=\"%s\" class=\"thumb-img\">", small_esc, large_esc);
			}
			else {
				appendf(&hbuf, &hcap, &hused, "<img src=\"/images/placeholder.jpg\" class=\"thumb-img\">");
			}
			appendf(&hbuf, &hcap, &hused, "</a></div>");
		}

		
		appendf(&hbuf, &hcap, &hused, "</div>");
		
		
		char tmp_path[PATH_MAX];
		snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", cache_path);
		FILE* wf = fopen(tmp_path, "wb");
		if (wf) {
			fwrite(hbuf, 1, hused, wf);
			fclose(wf);
			
#ifdef _WIN32
			MoveFileExA(tmp_path, cache_path, MOVEFILE_REPLACE_EXISTING);
#else
			rename(tmp_path, cache_path);
#endif
		}
		else {
			LOG_WARN("Failed to write cache file: %s", tmp_path);
		}
		send_header(c, 200, "OK", "text/html; charset=utf-8", (long)hused, NULL, 0, keep_alive);
		send(c, hbuf, (int)hused, 0);
		free(hbuf);
		free(files);
		return;
	}

	size_t cap = 8192;
	char* buf = malloc(cap);
	if (!buf) {
		const char* msg = "{\"error\":\"Out of memory\"}";
		send_header(c, 500, "Internal Server Error", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
		send(c, msg, (int)strlen(msg), 0);
		return;
	}
	size_t len = cap; char* ptr = buf; size_t used = 0;
	ptr = json_objOpen(ptr, NULL, &len);
	ptr = json_arrOpen(ptr, "items", &len);
	used = ptr - buf;
	ensure_json_buf(&buf, &cap, used, 4096);
	ptr = buf + used; len = cap - used;
	for (int i = start; i < end; i++) {
		
		used = ptr - buf;
		ensure_json_buf(&buf, &cap, used, 4096);
		ptr = buf + used; len = cap - used;

		if (i > start) ptr = json_comma_safe(ptr, &len);
		char full_path[PATH_MAX];
		path_join(full_path, target_real, files[i]);
		char relurl[PATH_MAX];
		const char* r = full_path;
		size_t j = 0;
		if (target_real[0]) {
			size_t target_len = strlen(target_real);
			if (target_len > 0 && strncmp(full_path, target_real, target_len) == 0) {
				r = full_path + target_len;
				if (*r == DIR_SEP) r++;
			} else {
				size_t base_len = strlen(base_real);
				if (base_len > 0 && strncmp(full_path, base_real, base_len) == 0) {
					r = full_path + base_len;
					if (*r == DIR_SEP) r++;
				}
			}
		} else {
			size_t base_len = strlen(base_real);
			if (base_len > 0 && strncmp(full_path, base_real, base_len) == 0) {
				r = full_path + base_len;
				if (*r == DIR_SEP) r++;
			}
		}
		for (size_t k = 0; r[k] && j < PATH_MAX - 1; k++) {
			relurl[j++] = (r[k] == '\\') ? '/' : r[k];
		}
		relurl[j] = '\0';
		char thumb_path_out[PATH_MAX];
		thumb_status_t thumb_status = check_thumb_exists(full_path, thumb_path_out, sizeof(thumb_path_out))
			? THUMB_READY
			: THUMB_GENERATING;
		ptr = json_objOpen(ptr, NULL, &len);
		ptr = json_str(ptr, "path", relurl, &len);
		ptr = json_str(ptr, "filename", files[i], &len);
		if (has_ext(files[i], IMAGE_EXTS)) {
			ptr = json_str(ptr, "type", "image", &len);
		}
		else if (has_ext(files[i], VIDEO_EXTS)) {
			ptr = json_str(ptr, "type", "video", &len);
		}
		else {
			ptr = json_str(ptr, "type", "unknown", &len);
		}

		
		char small_rel[PATH_MAX]; char large_rel[PATH_MAX];
		get_thumb_rel_names(full_path, files[i], small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));
		char thumbs_root[PATH_MAX]; snprintf(thumbs_root, sizeof(thumbs_root), "%s" DIR_SEP_STR "thumbs", BASE_DIR);
		char small_fs[PATH_MAX]; char large_fs[PATH_MAX];
		snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", thumbs_root, small_rel);
		snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", thumbs_root, large_rel);
		int small_exists = is_file(small_fs);
		int large_exists = is_file(large_fs);

		if (small_exists || large_exists) {
			char small_url[PATH_MAX]; char large_url[PATH_MAX];
			snprintf(small_url, sizeof(small_url), "/images/thumbs/%s", small_rel);
			snprintf(large_url, sizeof(large_url), "/images/thumbs/%s", large_rel);
			ptr = json_str(ptr, "thumb", small_url, &len);
			ptr = json_str(ptr, "thumb_small", small_url, &len);
			ptr = json_str(ptr, "thumb_large", large_url, &len);
		}
		else {
			ptr = json_str(ptr, "thumb", "", &len);
			ptr = json_str(ptr, "thumb_small", "", &len);
			ptr = json_str(ptr, "thumb_large", "", &len);
		}
		
		ptr = json_int(ptr, "thumb_small_status", small_exists ? 1 : 0, &len);
		ptr = json_int(ptr, "thumbStatus", thumb_status, &len);

		
		int w = 0, h = 0;
		char full_copy[PATH_MAX]; strncpy(full_copy, full_path, PATH_MAX - 1); full_copy[PATH_MAX - 1] = '\0';
		if (get_media_dimensions(full_copy, &w, &h)) {
			ptr = json_int(ptr, "width", w, &len);
			ptr = json_int(ptr, "height", h, &len);
		}
		ptr = json_objClose(ptr, &len);
		free(files[i]);
	}
	free(files);
	used = ptr - buf;
	ensure_json_buf(&buf, &cap, used, 512);
	ptr = buf + used; len = cap - used;
	ptr = json_arrClose(ptr, &len);
	ptr = json_comma_safe(ptr, &len);
	ptr = json_int(ptr, "total", total, &len);
	ptr = json_int(ptr, "page", page, &len);
	ptr = json_int(ptr, "totalPages", totalPages, &len);
	ptr = json_bool(ptr, "hasMore", page < totalPages, &len);
	ptr = json_objClose(ptr, &len);
	send_header(c, 200, "OK", "application/json; charset=utf-8", (long)(ptr - buf), NULL, 0, keep_alive);
	send(c, buf, (int)(ptr - buf), 0);
	free(buf);
}


void handle_api_add_folder(int c, const char* body, bool keep_alive) {
	LOG_INFO("Add folder request: %s", body);
	const char* path_start = strstr(body, "\"path\"");
	if (!path_start) { send_text(c, 400, "Bad Request", "Missing path", keep_alive); return; }
	path_start += 6; while (*path_start && (isspace(*path_start) || *path_start == ':')) path_start++;
	if (*path_start != '"') { send_text(c, 400, "Bad Request", "Invalid path", keep_alive); return; }
	path_start++;
	const char* path_end = strchr(path_start, '"');
	if (!path_end) { send_text(c, 400, "Bad Request", "Invalid path", keep_alive); return; }
	char path[PATH_MAX]; size_t len = path_end - path_start; if (len >= PATH_MAX) len = PATH_MAX - 1;
	strncpy(path, path_start, len); path[len] = '\0';
	url_decode(path);
	if (!is_dir(path)) { send_text(c, 400, "Bad Request", "Not a directory", keep_alive); return; }
	if (is_gallery_folder(path)) { send_text(c, 400, "Bad Request", "Folder already in gallery", keep_alive); return; }
	add_gallery_folder(path);
	size_t cap = 512; char* buf = malloc(cap); size_t rlen = cap; char* ptr = buf;
	ptr = json_objOpen(ptr, NULL, &rlen);
	ptr = json_str(ptr, "status", "success", &rlen);
	ptr = json_str(ptr, "message", path, &rlen);
	ptr = json_objClose(ptr, &rlen);
	send_header(c, 200, "OK", "application/json; charset=utf-8", (long)(ptr - buf), NULL, 0, keep_alive);
	send(c, buf, (int)(ptr - buf), 0);
	free(buf);
}
void handle_api_list_folders(int c, bool keep_alive) {
	size_t count; char** folders = get_gallery_folders(&count);
	size_t cap = 8192; char* buf = malloc(cap); size_t len = cap; char* ptr = buf;
	ptr = json_objOpen(ptr, NULL, &len);
	ptr = json_arrOpen(ptr, "folders", &len);
	for (size_t i = 0; i < count; i++) {
		if (i > 0) ptr = json_comma_safe(ptr, &len);
		ptr = json_objOpen(ptr, NULL, &len);
		ptr = json_str(ptr, "path", folders[i], &len);
		ptr = json_objClose(ptr, &len);
	}
	ptr = json_arrClose(ptr, &len);
	ptr = json_objClose(ptr, &len);
	send_header(c, 200, "OK", "application/json; charset=utf-8", (long)(ptr - buf), NULL, 0, keep_alive);
	send(c, buf, (int)(ptr - buf), 0);
	free(buf);
}
typedef enum {
	GET_SIMPLE, GET_QS, POST_BODY
} handler_type_t;
typedef struct {
	const char* path;
	handler_type_t type;
	void* handler;
} route_t;
typedef struct {
	const char* prefix;
	const char* base_dir;
	bool allow_range;
} static_route_t;
static void serve_file(int c, const char* base_dir, const char* sub_path, const char* range, bool keep_alive) {
	char rel[1024], base_real[1024], target_real[1024];
	snprintf(rel, sizeof(rel), "%s/%s", base_dir, sub_path);
	normalize_path(rel);
	if (real_path(base_dir, base_real) && real_path(rel, target_real) && safe_under(base_real, target_real) && is_file(target_real))
		send_file_stream(c, target_real, range, keep_alive);
	else
		send_text(c, 404, "Not Found", "Not found", keep_alive);
}
void handle_single_request(int c, char* headers, char* body, size_t headers_len, size_t body_len, bool keep_alive) {
	(void)headers_len;
	
	g_request_headers = headers;
	char method[8] = { 0 }, url[PATH_MAX] = { 0 };
	if (sscanf(headers, "%7s %4095s", method, url) != 2) {
		send_text(c, 400, "Bad Request", "Malformed request", false);
		return;
	}
	
	g_request_headers = NULL;
	char* qs = strchr(url, '?');
	if (qs) { *qs = '\0'; qs++; }
	url_decode(url);
	char* range = get_header_value(headers, "Range:");
	static const route_t routes[] = {
		{ "/api/tree", GET_SIMPLE, handle_api_tree },
		{ "/api/folders/list", GET_SIMPLE, handle_api_list_folders },
		{ "/api/folders", GET_QS, handle_api_folders },
		{ "/api/media", GET_QS, handle_api_media },
		{ "/api/folders/add", POST_BODY, handle_api_add_folder },
		{ "/api/regenerate-thumbs", GET_QS, handle_api_regenerate_thumbs },
	};
	static const static_route_t static_routes[] = {
		{ "/images/", BASE_DIR, true },
		{ "/js/", JS_DIR, false },
		{ "/css/", CSS_DIR, false },
	};
	for (size_t i = 0; i < sizeof(routes) / sizeof(routes[0]); i++) {
		if (strcmp(url, routes[i].path) == 0) {
			if (strcmp(method, "GET") == 0 && (routes[i].type == GET_SIMPLE || routes[i].type == GET_QS)) {
				if (routes[i].type == GET_SIMPLE) ((void (*)(int, bool))routes[i].handler)(c, keep_alive);
				else ((void (*)(int, char*, bool))routes[i].handler)(c, qs, keep_alive);
				return;
			}
			if (strcmp(method, "POST") == 0 && routes[i].type == POST_BODY) {
				if (!body || body_len == 0) { send_text(c, 400, "Bad Request", "Empty POST body", false); return; }
				char* body_copy = malloc(body_len + 1);
				memcpy(body_copy, body, body_len); body_copy[body_len] = '\0';
				((void (*)(int, const char*, bool))routes[i].handler)(c, body_copy, keep_alive);
				free(body_copy);
				return;
			}
			send_text(c, 405, "Method Not Allowed", "Method not supported for this endpoint", false);
			return;
		}
	}
	if (strcmp(method, "GET") != 0) {
		send_text(c, 405, "Method Not Allowed", "Only GET and POST supported", false);
		return;
	}
	if (strcmp(url, "/") == 0) {
		char path[1024];
		snprintf(path, sizeof(path), "%s/index.html", VIEWS_DIR);
		if (!is_file(path)) { send_text(c, 404, "Not Found", "index.html not found", keep_alive); return; }
		FILE* f = fopen(path, "rb");
		if (!f) { send_text(c, 500, "Internal Server Error", "failed to open index.html", keep_alive); return; }
		fseek(f, 0, SEEK_END); long fsz = ftell(f); fseek(f, 0, SEEK_SET);
		char* buf = malloc(fsz + 1); if (!buf) { fclose(f); send_text(c, 500, "Internal Server Error", "oom", keep_alive); return; }
		fread(buf, 1, fsz, f); buf[fsz] = '\0'; fclose(f);
		char dirparam[PATH_MAX] = { 0 }; int page = 1;
		if (qs) {
			char* v = query_get(qs, "dir"); if (v) { strncpy(dirparam, v, PATH_MAX - 1); SAFE_FREE(v); }
			char* p = query_get(qs, "page"); if (p) { int t = atoi(p); if (t > 0) page = t; SAFE_FREE(p); }
		}
		size_t frag_len = 0; char* frag = generate_media_fragment(BASE_DIR, dirparam, page, &frag_len);
		if (frag) {
			char* ph = strstr(buf, "<!-- MEDIA_FRAGMENT -->");
			if (ph) {
				size_t newlen = fsz + frag_len + 1024;
				char* out = malloc(newlen);
				if (out) {
					size_t pre = (size_t)(ph - buf);
					memcpy(out, buf, pre);
					memcpy(out + pre, frag, frag_len);
					memcpy(out + pre + frag_len, ph + 21, fsz - pre - 21); 
					send_header(c, 200, "OK", "text/html; charset=utf-8", (long)(pre + frag_len + (fsz - pre - 21)), NULL, 0, keep_alive);
					send(c, out, (int)(pre + frag_len + (fsz - pre - 21)), 0);
					free(out);
				}
				else {
					send_header(c, 200, "OK", "text/html; charset=utf-8", (long)fsz, NULL, 0, keep_alive);
					send(c, buf, (int)fsz, 0);
				}
			}
			else {
				send_header(c, 200, "OK", "text/html; charset=utf-8", (long)fsz, NULL, 0, keep_alive);
				send(c, buf, (int)fsz, 0);
			}
			free(frag);
		}
		else {
			send_header(c, 200, "OK", "text/html; charset=utf-8", (long)fsz, NULL, 0, keep_alive);
			send(c, buf, (int)fsz, 0);
		}
		free(buf);
		return;
	}
	for (size_t i = 0; i < sizeof(static_routes) / sizeof(static_routes[0]); i++) {
		size_t len = strlen(static_routes[i].prefix);
		if (strncmp(url, static_routes[i].prefix, len) == 0) {
			serve_file(c, static_routes[i].base_dir, url + len, static_routes[i].allow_range ? range : NULL, keep_alive);
			return;
		}
	}
	if (strcmp(url, "/bundled") == 0) {
		if (is_file(BUNDLED_FILE)) send_file_stream(c, BUNDLED_FILE, NULL, keep_alive);
		else send_text(c, 404, "Not Found", "Not found", keep_alive);
		return;
	}


	send_text(c, 404, "Not Found", "Not found", keep_alive);
}
