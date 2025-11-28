#include "api_handlers.h"
#include "directory.h"
#include "http.h"
#include "utils.h"
#include "logging.h"
#include "config.h"
#include "tinyjson_combined.h"
#include "common.h"
#include "crypto.h"
#include "thumbs.h"
#include "thumbdb.h"
#include "thread_pool.h"
#include "platform.h"
#include "websocket.h"

static void* start_background_wrapper(void* arg) {
	char* dir = (char*)arg;
	if (dir) {
		start_background_thumb_generation(dir);
		free(dir);
	}
	return NULL;
}

typedef struct { 
	char* key; 
	char* val; 
} api_kv_t;
typedef struct { 
	api_kv_t* arr; 
	size_t cap; 
	size_t count; 
	int err; 
} api_collect_ctx_t;

static void api_thumbdb_collect_cb(const char* key, const char* value, void* uctx) {
	api_collect_ctx_t* c = (api_collect_ctx_t*)uctx;
	if (!c) 
		return;
	if (!key) 
		return;
	if (c->count + 1 > c->cap) {
		size_t nc = c->cap ? c->cap * 2 : 256;
		api_kv_t* tmp = realloc(c->arr, nc * sizeof(*tmp));
		if (!tmp) {
			LOG_ERROR("Failed to realloc API KV array to size %zu", nc);
			c->err = 1;
			return;
		}
		c->arr = tmp; c->cap = nc;
	}
	c->arr[c->count].key = key ? strdup(key) : NULL;
	c->arr[c->count].val = value ? strdup(value) : NULL;
	if ((c->arr[c->count].key && !c->arr[c->count].key) || (value && !c->arr[c->count].val)) { 
		c->err = 1; 
		return; 
	}
	c->count++;
}

typedef struct {
	char* media;
	char* thumb;
} thumb_map_t;

static int thumb_map_cmp(const void* a, const void* b) {
	const thumb_map_t* A = (const thumb_map_t*)a;
	const thumb_map_t* B = (const thumb_map_t*)b;
	if (!A || !B) return 0;
	if (!A->media) return (B->media ? -1 : 0);
	if (!B->media) return 1;
	return strcmp(A->media, B->media);
}

static void get_parent_dir_local(const char* path, char* out, size_t outlen) {
	if (!path || !out || outlen == 0) return;
	char tmp[PATH_MAX];
	strncpy(tmp, path, sizeof(tmp) - 1);
	tmp[sizeof(tmp) - 1] = '\0';
	normalize_path(tmp);
	size_t len = strlen(tmp);
	while (len > 0 && (tmp[len - 1] == '/' || tmp[len - 1] == '\\')) { tmp[--len] = '\0'; }
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
		if (outlen > 1) { out[0] = '.'; out[1] = '\0'; }
		else if (outlen > 0) out[0] = '\0';
	}
}



const char* IMAGE_EXTS[] = { ".jpg",".jpeg",".png",".gif",".webp",NULL };
const char* VIDEO_EXTS[] = { ".mp4",".webm",".webp",NULL };
static char* g_request_headers = NULL;
char g_request_url[PATH_MAX] = { 0 };
static char* g_request_qs = NULL;
static char* legacy_folders_cache = NULL;
static size_t legacy_folders_cache_len = 0;
static time_t legacy_folders_cache_time = 0;
static thread_mutex_t legacy_folders_mutex;
static int legacy_folders_mutex_inited = 0;
static void appendf(char** pbuf, size_t* pcap, size_t* pused, const char* fmt, ...);
static void ensure_json_buf(char** pbuf, size_t* pcap, size_t used, size_t need) {
	if (*pcap - used < need + 1024) {
		size_t newcap = MAX((*pcap + need) * 2, *pcap + 8192);
		char* new_buf = realloc(*pbuf, newcap);
		if (!new_buf) {
			LOG_ERROR("Failed to reallocate JSON buffer to size %zu", newcap);
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

static void sanitize_dirparam(char* dirparam) {
	if (!dirparam || dirparam[0] == '\0') return;
	while (*dirparam == '/' || *dirparam == '\\') {
		size_t l = strlen(dirparam);
		if (l == 0) break;
		memmove(dirparam, dirparam + 1, l);
	}
	if (strcmp(dirparam, ".") == 0 || strcmp(dirparam, "/") == 0) dirparam[0] = '\0';
}

static int resolve_and_validate_target(const char* base_dir, const char* dirparam, char* target_real_out, size_t outlen, char* base_real_out, size_t base_outlen);

bool has_media_rec(const char* dir) {
	if (has_nogallery(dir)) return false;
	{
		char fgpath[PATH_MAX];
		path_join(fgpath, dir, ".fg");
		if (is_file(fgpath)) return true;
	}
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


char* generate_media_fragment(const char* base_dir, const char* dirparam, int page, size_t* out_len) {
	char dircopy[PATH_MAX] = { 0 };
	if (dirparam && dirparam[0]) {
		strncpy(dircopy, dirparam, sizeof(dircopy) - 1);
		dircopy[sizeof(dircopy) - 1] = '\0';
		sanitize_dirparam(dircopy);
	}
	const char* dir_to_use = (dirparam && dirparam[0]) ? dircopy : dirparam;
	const char* used_base = base_dir;
	size_t gf_count = 0; char** gfolders = get_gallery_folders(&gf_count);
	if ((!dirparam || dirparam[0] == '\0') && gf_count > 0) used_base = gfolders[0];
	char target[PATH_MAX];
	snprintf(target, sizeof(target), "%s/%s", used_base, dir_to_use ? dir_to_use : "");
	normalize_path(target);
	char target_real[PATH_MAX]; char base_real[PATH_MAX];
	if (!resolve_and_validate_target(used_base, dir_to_use, target_real, sizeof(target_real), base_real, sizeof(base_real))) { 
		if (out_len) *out_len = 0;
			return NULL; 
	}
	{
		if (page <= 1) {
			char* trg = strdup(target_real);
			if (trg) thread_create_detached(start_background_wrapper, trg);
		}
	}

	char** files = NULL; size_t n = 0, alloc = 0;
	diriter it; if (dir_open(&it, target_real)) {
		const char* name; while ((name = dir_next(&it))) {
			if (!strcmp(name, ".") || !strcmp(name, "..")) continue;
			if (has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS)) {
				if (n == alloc) {
					alloc = alloc ? alloc * 2 : 64;
					files = realloc(files, alloc * sizeof(char*));
					if (!files) {
						LOG_ERROR("Failed to realloc files array to size %zu", alloc);
						alloc = 0;
						break;
					}
				}
				files[n++] = strdup(name);
			}
		}
		dir_close(&it);
	}
	if (n == 0) { 
		if (files) free(files); 
		if (out_len) *out_len = 0; 
		return NULL; 
	}
	qsort(files, n, sizeof(char*), p_strcmp);
	int total = (int)n; 
	int totalPages = (total + ITEMS_PER_PAGE - 1) / ITEMS_PER_PAGE; 
	if (totalPages == 0) totalPages = 1; 
	if (page < 1) page = 1; 
	if (page > totalPages) page = totalPages;
	int start = (page - 1) * ITEMS_PER_PAGE; int end = start + ITEMS_PER_PAGE; if (end > total) end = total;

	size_t hcap = 8192;
	char* hbuf = malloc(hcap);
	if (!hbuf) {
		size_t i;
		for (i = 0;i < n;i++) free(files[i]);
		if (out_len) *out_len = 0;
		return NULL;
	}
	size_t hused = 0;
	appendf(&hbuf, &hcap, &hused, "<div class=\"masonry-fragment\" data-page=\"%d\" data-hasmore=\"%d\">", page, (page < totalPages) ? 1 : 0);

	thumb_map_t* thumb_map = NULL; size_t thumb_map_count = 0;
	{
		char parent[PATH_MAX]; parent[0] = '\0';
		get_parent_dir_local(target_real, parent, sizeof(parent));
		char safe_dir[PATH_MAX]; safe_dir[0] = '\0';
		make_safe_dir_name_from(parent, safe_dir, sizeof(safe_dir));
		char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
		char per_thumbs_root[PATH_MAX]; snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir);
		if (is_dir(per_thumbs_root)) {
			diriter tit;
			if (dir_open(&tit, per_thumbs_root)) {
				const char* tname; size_t cap = 128;
				thumb_map = calloc(cap, sizeof(*thumb_map));
				if (!thumb_map) {
					LOG_ERROR("Failed to allocate thumb map array of size %zu", cap);
					cap = 0;
				}
				while ((tname = dir_next(&tit))) {
					if (!tname) continue;
					if (!strstr(tname, "-small.") && !strstr(tname, "-large.")) continue;
					char media_val[PATH_MAX]; media_val[0] = '\0';
					if (thumbdb_get(tname, media_val, sizeof(media_val)) != 0) continue;
					if (thumb_map_count + 1 >= cap) {
						size_t nc = cap * 2;
						thumb_map = realloc(thumb_map, nc * sizeof(*thumb_map));
						if (!thumb_map) {
							LOG_ERROR("Failed to realloc thumb map array to size %zu", nc);
							cap = 0;
							break;
						}
						cap = nc;
					}
					thumb_map[thumb_map_count].media = strdup(media_val);
					thumb_map[thumb_map_count].thumb = strdup(tname);
					thumb_map_count++;
				}
				dir_close(&tit);
				if (thumb_map_count > 1) {
					qsort(thumb_map, thumb_map_count, sizeof(*thumb_map), thumb_map_cmp);
				}
			}
		}
	}
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
			}
			else {
				size_t base_len = strlen(base_real);
				if (base_len > 0 && strncmp(full_path, base_real, base_len) == 0) {
					r = full_path + base_len;
					if (*r == DIR_SEP) r++;
				}
			}
		}
		else {
			size_t base_len = strlen(base_real);
			if (base_len > 0 && strncmp(full_path, base_real, base_len) == 0) {
				r = full_path + base_len;
				if (*r == DIR_SEP) r++;
			}
		}
		for (size_t k = 0; r[k] && j < PATH_MAX - 1; k++) relurl[j++] = (r[k] == '\\') ? '/' : r[k]; relurl[j] = '\0';
		char small_rel[PATH_MAX]; char large_rel[PATH_MAX];
		char found_thumb[PATH_MAX]; found_thumb[0] = '\0';
		int small_exists = 0; int large_exists = 0;
		if (check_thumb_exists(full_path, found_thumb, sizeof(found_thumb))) {
			if (strstr(found_thumb, "-small.")) {
				strncpy(small_rel, found_thumb, sizeof(small_rel) - 1); small_rel[sizeof(small_rel) - 1] = '\0';
				strncpy(large_rel, small_rel, sizeof(large_rel) - 1); large_rel[sizeof(large_rel) - 1] = '\0';
				char* p = strstr(large_rel, "-small."); if (p) memcpy(p, "-large.", 7);
				small_exists = 1;
			}
			else if (strstr(found_thumb, "-large.")) {
				strncpy(large_rel, found_thumb, sizeof(large_rel) - 1); large_rel[sizeof(large_rel) - 1] = '\0';
				strncpy(small_rel, large_rel, sizeof(small_rel) - 1); small_rel[sizeof(small_rel) - 1] = '\0';
				char* p = strstr(small_rel, "-large."); if (p) memcpy(p, "-small.", 7);
				large_exists = 1;
			}
			else {
				strncpy(small_rel, found_thumb, sizeof(small_rel) - 1); small_rel[sizeof(small_rel) - 1] = '\0';
			}
		}
		char small_fs[PATH_MAX]; char large_fs[PATH_MAX];
		char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
		if (dirparam[0]) {
			char safe_dir[PATH_MAX]; make_safe_dir_name_from(target_real, safe_dir, sizeof(safe_dir));
			char per_thumbs_root[PATH_MAX]; snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir);
			snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, small_rel);
			snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, large_rel);
		}
		else {
			char dirpart[PATH_MAX] = "";
			const char* first_slash = strchr(relurl, '/');
			if (first_slash && first_slash != relurl) {
				size_t dlen = (size_t)(first_slash - relurl);
				if (dlen >= sizeof(dirpart)) dlen = sizeof(dirpart) - 1;
				memcpy(dirpart, relurl, dlen);
				dirpart[dlen] = '\0';
			}
			if (!dirpart[0]) {
				size_t gf_count = 0; char** gfolders = get_gallery_folders(&gf_count);
				char folder_real[PATH_MAX]; char base_real_local[PATH_MAX];
				if (real_path(BASE_DIR, base_real_local)) {
					size_t base_len = strlen(base_real_local);
					for (size_t gi = 0; gi < gf_count; ++gi) {
						if (!real_path(gfolders[gi], folder_real)) continue;
						if (!safe_under(folder_real, full_path)) continue;
						const char* r = folder_real + base_len + ((folder_real[base_len] == DIR_SEP) ? 1 : 0);
						make_safe_dir_name_from(r, dirpart, sizeof(dirpart));
						break;
					}
				}
				if (!dirpart[0]) {
					char parent[PATH_MAX];
					strncpy(parent, full_path, sizeof(parent) - 1);
					parent[sizeof(parent) - 1] = '\0';
					char* s1 = strrchr(parent, '/');
					char* s2 = strrchr(parent, '\\');
					char* last = NULL;
					if (s1 && s2) last = (s1 > s2) ? s1 : s2;
					else if (s1) last = s1;
					else if (s2) last = s2;
					if (last) *last = '\0'; else { parent[0] = '.'; parent[1] = '\0'; }
					make_safe_dir_name_from(parent, dirpart, sizeof(dirpart));
				}
			}
			char per_thumbs_root[PATH_MAX]; snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, dirpart[0] ? dirpart : "");
			if (dirpart[0]) {
				snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, small_rel);
				snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, large_rel);
			}
			else {
				snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", thumbs_root, small_rel);
				snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", thumbs_root, large_rel);
			}
		}
		if (!small_exists) small_exists = is_file(small_fs);
		if (!large_exists) large_exists = is_file(large_fs);
		char href[PATH_MAX]; if (dirparam && dirparam[0]) snprintf(href, sizeof(href), "/images/%s/%s", dirparam, relurl); else snprintf(href, sizeof(href), "/images/%s", relurl);
		char href_esc[PATH_MAX]; html_escape(href, href_esc, sizeof(href_esc));
		char small_url[PATH_MAX] = ""; char large_url[PATH_MAX] = "";
		if (small_exists || large_exists) {
			if (dirparam && dirparam[0]) {
				char safe_dir[PATH_MAX]; make_safe_dir_name_from(target_real, safe_dir, sizeof(safe_dir));
				snprintf(small_url, sizeof(small_url), "/images/thumbs/%s/%s", safe_dir, small_rel);
				snprintf(large_url, sizeof(large_url), "/images/thumbs/%s/%s", safe_dir, large_rel);
			}
			else {
				char dirpart[PATH_MAX] = "";
				const char* first_slash = strchr(relurl, '/');
				if (first_slash && first_slash != relurl) {
					size_t dlen = (size_t)(first_slash - relurl);
					if (dlen >= sizeof(dirpart)) dlen = sizeof(dirpart) - 1;
					memcpy(dirpart, relurl, dlen);
					dirpart[dlen] = '\0';
				}
				if (!dirpart[0]) {
					size_t gf_count = 0; char** gfolders = get_gallery_folders(&gf_count);
					char folder_real[PATH_MAX];
					for (size_t gi = 0; gi < gf_count; ++gi) {
						if (!real_path(gfolders[gi], folder_real)) continue;
						if (!safe_under(folder_real, full_path)) continue;
						size_t si = 0;
						for (size_t ii = 0; folder_real[ii] && si < sizeof(dirpart) - 1; ++ii) {
							char tmp_safe[PATH_MAX]; make_safe_dir_name_from(folder_real, tmp_safe, sizeof(tmp_safe));
							size_t tlen = strlen(tmp_safe);
							if (tlen >= sizeof(dirpart)) tlen = sizeof(dirpart) - 1;
							memcpy(dirpart, tmp_safe, tlen);
							dirpart[tlen] = '\0';
							si = tlen;
							break;
						}
						dirpart[si] = '\0';
						break;
					}
					if (!dirpart[0]) {
						char parent[PATH_MAX];
						strncpy(parent, full_path, sizeof(parent) - 1);
						parent[sizeof(parent) - 1] = '\0';
						char* s1 = strrchr(parent, '/');
						char* s2 = strrchr(parent, '\\');
						char* last = NULL;
						if (s1 && s2) last = (s1 > s2) ? s1 : s2;
						else if (s1) last = s1;
						else if (s2) last = s2;
						if (last) *last = '\0'; else { parent[0] = '.'; parent[1] = '\0'; }
						make_safe_dir_name_from(parent, dirpart, sizeof(dirpart));
					}
				}
				if (dirpart[0]) {
					snprintf(small_url, sizeof(small_url), "/images/thumbs/%s/%s", dirpart, small_rel);
					snprintf(large_url, sizeof(large_url), "/images/thumbs/%s/%s", dirpart, large_rel);
				}
				else {
					snprintf(small_url, sizeof(small_url), "/images/thumbs/%s", small_rel);
					snprintf(large_url, sizeof(large_url), "/images/thumbs/%s", large_rel);
				}
			}
		}
		char small_esc[PATH_MAX]; char large_esc[PATH_MAX]; html_escape(small_url, small_esc, sizeof(small_esc)); html_escape(large_url, large_esc, sizeof(large_esc));

		char dim_attr[64]; dim_attr[0] = '\0';
		int is_video = has_ext(files[i], VIDEO_EXTS);
		int thumb_status = small_exists ? 1 : 0;
		if (is_video) {
			appendf(&hbuf, &hcap, &hused, "<div class=\"masonry-item\" data-type=\"video\"><a data-fancybox=\"gallery\" href=\"%s\" data-thumb-status=\"%d\" data-type=\"video\" data-src=\"%s\">", href_esc, thumb_status, href_esc);
		}
		else {
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


void handle_api_regenerate_thumbs(int c, char* qs, bool keep_alive) {
	char dirparam[PATH_MAX] = { 0 };
	if (qs) { char* v = query_get(qs, "dir");if (v) { strncpy(dirparam, v, PATH_MAX - 1);SAFE_FREE(v); } }
	sanitize_dirparam(dirparam);
	char target[PATH_MAX]; snprintf(target, sizeof(target), "%s/%s", BASE_DIR, dirparam);
	normalize_path(target);
	char target_real[PATH_MAX];
	char base_real[PATH_MAX];
	if (!resolve_and_validate_target(BASE_DIR, dirparam, target_real, sizeof(target_real), base_real, sizeof(base_real))) {
		const char* msg = "{\"error\":\"Invalid directory\"}";
		send_header(c, 400, "Bad Request", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
		send(c, msg, (int)strlen(msg), 0);
		return;
	}
	if (dir_has_missing_thumbs_shallow(target_real, 0)) start_background_thumb_generation(target_real);
	const char* msg = "{\"status\":\"accepted\",\"message\":\"Thumbnail regeneration started.\"}";
	send_header(c, 202, "Accepted", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
	send(c, msg, (int)strlen(msg), 0);
}
static char* json_comma_safe(char* ptr, size_t* remLen) {
	if (*remLen < 10) return ptr;
	*ptr++ = ',';
	(*remLen)--;
	return ptr;
}
static char* build_folder_tree_json(char** pbuf, size_t* cap, size_t* used, const char* dir, const char* root) {
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
				if (n == alloc) {
					alloc = alloc ? alloc * 2 : 16;
					names = realloc(names, alloc * sizeof(char*));
					if (!names) {
						size_t i;
						for (i = 0; i < n; i++) free(names[i]);
						alloc = 0;
						break;
					}
				}
				names[n++] = strdup(name);
			}
		}
		dir_close(&it);
	}
	qsort(names, n, sizeof(char*), p_strcmp);
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
		size_t i;
		for (i = 0; i < count; i++) {
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
	sanitize_dirparam(dirparam);
	char target[PATH_MAX]; 
	snprintf(target, sizeof(target), "%s/%s", BASE_DIR, dirparam); 
	normalize_path(target);
	char base_real[PATH_MAX], target_real[PATH_MAX];
	if (!real_path(BASE_DIR, base_real) || !real_path(target, target_real)
		|| !safe_under(base_real, target_real) || !is_dir(target_real)) {
		const char* msg = "{\"error\":\"Invalid directory\"}";
		send_header(c, 400, 
			"Bad Request", 
			"application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
		send(c, msg, (int)strlen(msg), 0);
		return;
	}
	char** names = NULL; size_t n = 0, alloc = 0;
	diriter it;
	if (dir_open(&it, target_real)) {
		const char* name;
		while ((name = dir_next(&it))) {
			if (!strcmp(name, ".") || !strcmp(name, "..")) 
				continue;
			char full[PATH_MAX]; 
			path_join(full, target_real, name);
			if (is_dir(full) && !has_nogallery(full) && has_media_rec(full)) {
				if (n == alloc) { 
					alloc = alloc ? alloc * 2 : 16; 
					names = realloc(names, alloc * sizeof(char*)); 
				}
				names[n++] = strdup(name);
			}
		}
		dir_close(&it);
	}
	qsort(names, n, sizeof(char*), p_strcmp);
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
	sanitize_dirparam(dirparam);
	char target[PATH_MAX];
	if (dirparam[0] == '\0') {
		size_t gf_count = 0; char** gfolders = get_gallery_folders(&gf_count);
		if (gf_count > 0) {
			strncpy(target, gfolders[0], sizeof(target) - 1);
			target[sizeof(target) - 1] = '\0';
			normalize_path(target);
		}
		else {
			snprintf(target, sizeof(target), "%s/%s", BASE_DIR, dirparam);
			normalize_path(target);
		}
	}
	else {
		snprintf(target, sizeof(target), "%s/%s", BASE_DIR, dirparam);
		normalize_path(target);
	}
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
	{
		if (page <= 1) {
			char* trg = strdup(target_real);
			if (trg) thread_create_detached(start_background_wrapper, trg);
		}
	}
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
	qsort(files, n, sizeof(char*), p_strcmp);
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
			make_safe_dir_name_from(dirparam, safe_name, sizeof(safe_name));
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
					if (files) {
						for (size_t ii = 0; ii < n; ++ii) free(files[ii]);
						free(files);
						files = NULL;
					}
					SAFE_FREE(if_none);
					return;
				}
				SAFE_FREE(if_none);
				send_header(c, 200, "OK", "text/html; charset=utf-8", (long)stc.st_size, NULL, 0, keep_alive);
				send_file_stream(c, cache_path, NULL, keep_alive);
				if (files) {
					for (size_t ii = 0; ii < n; ++ii) free(files[ii]);
					free(files);
					files = NULL;
				}
				return;
			}

			if (files) {
				for (size_t ii = 0; ii < n; ++ii) free(files[ii]);
				free(files);
				files = NULL;
			}
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
				}
				else {
					size_t base_len = strlen(base_real);
					if (base_len > 0 && strncmp(full_path, base_real, base_len) == 0) {
						r = full_path + base_len;
						if (*r == DIR_SEP) r++;
					}
				}
			}
			else {
				size_t base_len = strlen(base_real);
				if (base_len > 0 && strncmp(full_path, base_real, base_len) == 0) {
					r = full_path + base_len;
					if (*r == DIR_SEP) r++;
				}
			}
			for (size_t k = 0; r[k] && j < PATH_MAX - 1; k++) relurl[j++] = (r[k] == '\\') ? '/' : r[k];
			relurl[j] = '\0';
			char small_rel[PATH_MAX]; char large_rel[PATH_MAX];
			char found_thumb[PATH_MAX]; found_thumb[0] = '\0';
			int small_exists = 0; int large_exists = 0;
			if (check_thumb_exists(full_path, found_thumb, sizeof(found_thumb))) {
				if (strstr(found_thumb, "-small.")) {
					strncpy(small_rel, found_thumb, sizeof(small_rel) - 1); small_rel[sizeof(small_rel) - 1] = '\0';
					strncpy(large_rel, small_rel, sizeof(large_rel) - 1); large_rel[sizeof(large_rel) - 1] = '\0';
					char* p = strstr(large_rel, "-small."); if (p) memcpy(p, "-large.", 7);
					small_exists = 1;
				}
				else if (strstr(found_thumb, "-large.")) {
					strncpy(large_rel, found_thumb, sizeof(large_rel) - 1); large_rel[sizeof(large_rel) - 1] = '\0';
					strncpy(small_rel, large_rel, sizeof(small_rel) - 1); small_rel[sizeof(small_rel) - 1] = '\0';
					char* p = strstr(small_rel, "-large."); if (p) memcpy(p, "-small.", 7);
					large_exists = 1;
				}
				else {
					strncpy(small_rel, found_thumb, sizeof(small_rel) - 1); small_rel[sizeof(small_rel) - 1] = '\0';
				}
			}
			char small_fs[PATH_MAX]; char large_fs[PATH_MAX];
			char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
			if (dirparam[0]) {
				char safe_dir[PATH_MAX]; make_safe_dir_name_from(target_real, safe_dir, sizeof(safe_dir));
				char per_thumbs_root[PATH_MAX]; snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir);
				snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, small_rel);
				snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, large_rel);
			}
			else {
				char dirpart[PATH_MAX] = "";
				const char* first_slash = strchr(relurl, '/');
				if (first_slash && first_slash != relurl) {
					size_t dlen = (size_t)(first_slash - relurl);
					if (dlen >= sizeof(dirpart)) dlen = sizeof(dirpart) - 1;
					memcpy(dirpart, relurl, dlen);
					dirpart[dlen] = '\0';
				}
				if (!dirpart[0]) {
					size_t gf_count = 0; char** gfolders = get_gallery_folders(&gf_count);
					char folder_real[PATH_MAX]; char base_real_local[PATH_MAX];
					if (real_path(BASE_DIR, base_real_local)) {
						size_t base_len = strlen(base_real_local);
						for (size_t gi = 0; gi < gf_count; ++gi) {
							if (!real_path(gfolders[gi], folder_real)) continue;
							if (!safe_under(folder_real, full_path)) continue;
							const char* r = folder_real + base_len + ((folder_real[base_len] == DIR_SEP) ? 1 : 0);
							make_safe_dir_name_from(r, dirpart, sizeof(dirpart));
							break;
						}
					}
					if (!dirpart[0]) {
						char parent[PATH_MAX];
						strncpy(parent, full_path, sizeof(parent) - 1);
						parent[sizeof(parent) - 1] = '\0';
						char* s1 = strrchr(parent, '/');
						char* s2 = strrchr(parent, '\\');
						char* last = NULL;
						if (s1 && s2) last = (s1 > s2) ? s1 : s2;
						else if (s1) last = s1;
						else if (s2) last = s2;
						if (last) *last = '\0'; else { parent[0] = '.'; parent[1] = '\0'; }
						make_safe_dir_name_from(parent, dirpart, sizeof(dirpart));
					}
				}
				char per_thumbs_root[PATH_MAX]; snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, dirpart[0] ? dirpart : "");
				if (dirpart[0]) {
					snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, small_rel);
					snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", per_thumbs_root, large_rel);
				}
				else {
					snprintf(small_fs, sizeof(small_fs), "%s" DIR_SEP_STR "%s", thumbs_root, small_rel);
					snprintf(large_fs, sizeof(large_fs), "%s" DIR_SEP_STR "%s", thumbs_root, large_rel);
				}
			}
			if (!small_exists) small_exists = is_file(small_fs);
			if (!large_exists) large_exists = is_file(large_fs);
			char href[PATH_MAX];
			if (dirparam[0]) snprintf(href, sizeof(href), "/images/%s/%s", dirparam, relurl);
			else snprintf(href, sizeof(href), "/images/%s", relurl);
			char href_esc[PATH_MAX]; html_escape(href, href_esc, sizeof(href_esc));
			char small_url[PATH_MAX] = ""; char large_url[PATH_MAX] = "";
			if (small_exists || large_exists) {
				if (dirparam[0]) {
					char safe_dir[PATH_MAX]; make_safe_dir_name_from(target_real, safe_dir, sizeof(safe_dir));
					snprintf(small_url, sizeof(small_url), "/images/thumbs/%s/%s", safe_dir, small_rel);
					snprintf(large_url, sizeof(large_url), "/images/thumbs/%s/%s", safe_dir, large_rel);
				}
				else {
					char dirpart[PATH_MAX] = "";
					const char* first_slash = strchr(relurl, '/');
					if (first_slash && first_slash != relurl) {
						size_t dlen = (size_t)(first_slash - relurl);
						if (dlen >= sizeof(dirpart)) dlen = sizeof(dirpart) - 1;
						memcpy(dirpart, relurl, dlen);
						dirpart[dlen] = '\0';
					}
					if (!dirpart[0]) {
						size_t gf_count = 0; char** gfolders = get_gallery_folders(&gf_count);
						char folder_real[PATH_MAX];
						for (size_t gi = 0; gi < gf_count; ++gi) {
							if (!real_path(gfolders[gi], folder_real)) continue;
							if (!safe_under(folder_real, full_path)) continue;
							make_safe_dir_name_from(folder_real, dirpart, sizeof(dirpart));
							break;
						}
					}
					if (dirpart[0]) {
						snprintf(small_url, sizeof(small_url), "/images/thumbs/%s/%s", dirpart, small_rel);
						snprintf(large_url, sizeof(large_url), "/images/thumbs/%s/%s", dirpart, large_rel);
					}
					else {
						snprintf(small_url, sizeof(small_url), "/images/thumbs/%s", small_rel);
						snprintf(large_url, sizeof(large_url), "/images/thumbs/%s", large_rel);
					}
				}
			}
			char small_esc[PATH_MAX]; char large_esc[PATH_MAX]; html_escape(small_url, small_esc, sizeof(small_esc)); html_escape(large_url, large_esc, sizeof(large_esc));
			appendf(&hbuf, &hcap, &hused, "<div class=\"masonry-item\"><a data-fancybox=\"gallery\" href=\"%s\">", href_esc);
			if (small_exists)
				appendf(&hbuf, &hcap, &hused, "<img src=\"%s\" loading=\"lazy\" data-thumb-small=\"%s\" data-thumb-large=\"%s\" class=\"thumb-img\">", small_esc, small_esc, large_esc);
			else if (large_exists)
				appendf(&hbuf, &hcap, &hused, "<img src=\"%s\" loading=\"lazy\" data-thumb-large=\"%s\" class=\"thumb-img\">", large_esc, large_esc);
			else
				appendf(&hbuf, &hcap, &hused, "<img src=\"/images/placeholder.jpg\" class=\"thumb-img\">");
			appendf(&hbuf, &hcap, &hused, "</a></div>");
		}
		appendf(&hbuf, &hcap, &hused, "</div>");
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
			}
			else {
				size_t base_len = strlen(base_real);
				if (base_len > 0 && strncmp(full_path, base_real, base_len) == 0) {
					r = full_path + base_len;
					if (*r == DIR_SEP) r++;
				}
			}
		}
		else {
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
		char small_fs[PATH_MAX]; char large_fs[PATH_MAX];
		make_thumb_fs_paths(full_path, files[i], small_fs, sizeof(small_fs), large_fs, sizeof(large_fs));
		int small_exists = is_file(small_fs);
		int large_exists = is_file(large_fs);

		if (small_exists || large_exists) {
			char small_url[PATH_MAX]; char large_url[PATH_MAX];
			if (dirparam[0]) {
				char safe_dir[PATH_MAX]; make_safe_dir_name_from(target_real, safe_dir, sizeof(safe_dir));
				snprintf(small_url, sizeof(small_url), "/images/thumbs/%s/%s", safe_dir, small_rel);
				snprintf(large_url, sizeof(large_url), "/images/thumbs/%s/%s", safe_dir, large_rel);
			}
			else {
				char dirpart[PATH_MAX] = "";
				const char* first_slash = strchr(relurl, '/');
				if (first_slash && first_slash != relurl) {
					size_t dlen = (size_t)(first_slash - relurl);
					if (dlen >= sizeof(dirpart)) dlen = sizeof(dirpart) - 1;
					memcpy(dirpart, relurl, dlen);
					dirpart[dlen] = '\0';
				}
				if (!dirpart[0]) {
					size_t gf_count = 0; char** gfolders = get_gallery_folders(&gf_count);
					char folder_real[PATH_MAX]; char base_real_local[PATH_MAX];
					if (real_path(BASE_DIR, base_real_local)) {
						size_t base_len = strlen(base_real_local);
						for (size_t gi = 0; gi < gf_count; ++gi) {
							if (!real_path(gfolders[gi], folder_real)) continue;
							if (!safe_under(folder_real, full_path)) continue;
							const char* r = folder_real + base_len + ((folder_real[base_len] == DIR_SEP) ? 1 : 0);
							make_safe_dir_name_from(r, dirpart, sizeof(dirpart));
							break;
						}
					}
					if (!dirpart[0]) {
						char parent[PATH_MAX];
						strncpy(parent, full_path, sizeof(parent) - 1);
						parent[sizeof(parent) - 1] = '\0';
						char* s1 = strrchr(parent, '/');
						char* s2 = strrchr(parent, '\\');
						char* last = NULL;
						if (s1 && s2) last = (s1 > s2) ? s1 : s2;
						else if (s1) last = s1;
						else if (s2) last = s2;
						if (last) *last = '\0'; else { parent[0] = '.'; parent[1] = '\0'; }
						make_safe_dir_name_from(parent, dirpart, sizeof(dirpart));
					}
				}
				if (dirpart[0]) {
					snprintf(small_url, sizeof(small_url), "/images/thumbs/%s/%s", dirpart, small_rel);
					snprintf(large_url, sizeof(large_url), "/images/thumbs/%s/%s", dirpart, large_rel);
				}
				else {
					snprintf(small_url, sizeof(small_url), "/images/thumbs/%s", small_rel);
					snprintf(large_url, sizeof(large_url), "/images/thumbs/%s", large_rel);
				}
			}
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
	if (!body) { send_text(c, 400, "Bad Request", "Missing body", keep_alive); return; }
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
	size_t cap = 512; char* buf = malloc(cap);
	if (!buf) {
		const char* msg = "{\"error\":\"Out of memory\"}";
		send_header(c, 500, "Internal Server Error", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
		send(c, msg, (int)strlen(msg), 0);
		return;
	}
	size_t rlen = cap; char* ptr = buf;
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
	size_t cap = 8192; char* buf = malloc(cap);
	if (!buf) {
		const char* msg = "{\"error\":\"Out of memory\"}";
		send_header(c, 500, "Internal Server Error", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
		send(c, msg, (int)strlen(msg), 0);
		return;
	}
	size_t len = cap; char* ptr = buf;
	ptr = json_objOpen(ptr, NULL, &len);
	ptr = json_arrOpen(ptr, "folders", &len);
	size_t i;
	for (i = 0; i < count; i++) {
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
void handle_legacy_folders(int c, bool keep_alive) {
	LOG_DEBUG("handle_legacy_folders requested");
	const int STACK_INIT = 64; const int SUBDIR_INIT = 32; const int STACK_GROW = 64;
	if (!legacy_folders_mutex_inited) { thread_mutex_init(&legacy_folders_mutex); legacy_folders_mutex_inited = 1; }
	time_t now = time(NULL);
	thread_mutex_lock(&legacy_folders_mutex);
	if (legacy_folders_cache && (now - legacy_folders_cache_time) < 5) {
		size_t used = legacy_folders_cache_len;
		send_header(c, 200, "OK", "application/json; charset=utf-8", (long)used, NULL, 0, keep_alive);
		send(c, legacy_folders_cache, (int)used, 0);
		thread_mutex_unlock(&legacy_folders_mutex);
		return;
	}
	thread_mutex_unlock(&legacy_folders_mutex);
	char** stack = malloc(STACK_INIT * sizeof(char*));
	if (!stack) { send_text(c, 500, "Internal Server Error", "Memory error", keep_alive); return; }
	int sp = 0, stack_cap = STACK_INIT;
	stack[sp++] = strdup(BASE_DIR);

	size_t cap = 1024; char* out = malloc(cap); if (!out) { free(stack); send_text(c, 500, "Internal Server Error", "Memory error", keep_alive); return; }
	size_t used = 0; out[used++] = '[';
	int first = 1;

	while (sp > 0) {
		char* d = stack[--sp];
		char nog[PATH_MAX]; path_join(nog, d, ".nogallery"); if (is_file(nog)) { free(d); continue; }

		diriter it; if (!dir_open(&it, d)) { free(d); continue; }
		const char* name; int has_media = 0;
		char** subdirs = malloc(SUBDIR_INIT * sizeof(char*));
		if (!subdirs) {
			LOG_ERROR("Failed to allocate subdirs array");
			free(d);
			continue;
		}
		int subcap = SUBDIR_INIT, subcnt = 0;
		while ((name = dir_next(&it))) {
			if (!strcmp(name, ".") || !strcmp(name, "..")) continue;
			char full[PATH_MAX]; path_join(full, d, name);
			if (is_file(full)) {
				if (has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS) || strcmp(name, ".fg") == 0) has_media = 1;
			}
			else if (is_dir(full)) {
				if (subcnt >= subcap) {
					subcap += STACK_GROW;
					char** tmp = realloc(subdirs, subcap * sizeof(char*));
					if (!tmp) break;
					subdirs = tmp;
				}
				subdirs[subcnt++] = strdup(name);
			}
		}
		dir_close(&it);

		if (has_media) {
			const char* rel = d + strlen(BASE_DIR);
			while (*rel == '/' || *rel == '\\') rel++;
			if (*rel) {
				if (!first) { ensure_json_buf(&out, &cap, used, 2); out[used++] = ','; }
				first = 0;
				ensure_json_buf(&out, &cap, used, strlen(rel) + 4);
				out[used++] = '"';
				for (const char* p = rel; *p; ++p) { out[used++] = (*p == '\\') ? '/' : *p; }
				out[used++] = '"';
			}
		}

		for (int i = subcnt - 1; i >= 0; --i) {
			if (sp >= stack_cap) {
				stack_cap += STACK_GROW;
				char** tmp = realloc(stack, stack_cap * sizeof(char*));
				if (!tmp) break;
				stack = tmp;
			}
			char* child = malloc(PATH_MAX);
			if (!child) {
				LOG_ERROR("Failed to allocate child path buffer");
				break;
			}
			snprintf(child, PATH_MAX, "%s%s%s", d, DIR_SEP_STR, subdirs[i]);
			normalize_path(child);
			stack[sp++] = child;
			free(subdirs[i]);
		}
		free(subdirs);
		free(d);
	}

	ensure_json_buf(&out, &cap, used, 2);
	out[used++] = ']';
	thread_mutex_lock(&legacy_folders_mutex);
	if (legacy_folders_cache) free(legacy_folders_cache);
	legacy_folders_cache = malloc(used);
	if (legacy_folders_cache) {
		memcpy(legacy_folders_cache, out, used);
		legacy_folders_cache_len = used;
		legacy_folders_cache_time = time(NULL);
	}
	thread_mutex_unlock(&legacy_folders_mutex);
	send_header(c, 200, "OK", "application/json; charset=utf-8", (long)used, NULL, 0, keep_alive);
	send(c, legacy_folders_cache ? legacy_folders_cache : out, (int)used, 0);
	free(out); free(stack);
}
void handle_legacy_files(int c, char* qs, bool keep_alive) {
	char dirparam[PATH_MAX] = { 0 };

	if (qs) {
		char* v = query_get(qs, "dir");
		if (v) {
			strncpy(dirparam, v, PATH_MAX - 1);
			dirparam[PATH_MAX - 1] = '\0';
			SAFE_FREE(v);
		}
	}

	sanitize_dirparam(dirparam);
	normalize_path(dirparam);
	while (*dirparam == DIR_SEP) memmove(dirparam, dirparam + 1, strlen(dirparam));

	if (strstr(dirparam, "..") || dirparam[0] == DIR_SEP) {
		send_text(c, 400, "Bad Request", "Invalid directory path", keep_alive);
		return;
	}

	LOG_DEBUG("handle_legacy_files requested dir=%s", dirparam[0] ? dirparam : "/");

	char search[PATH_MAX];
	if (dirparam[0]) {
		snprintf(search, sizeof(search), "%s%s%s", BASE_DIR, DIR_SEP_STR, dirparam);
	}
	else {
		snprintf(search, sizeof(search), "%s", BASE_DIR);
	}

	normalize_path(search);

	if (!is_dir(search)) {
		send_text(c, 400, "Bad Request", "Invalid directory", keep_alive);
		return;
	}

	size_t cap = 1024;
	char* out = malloc(cap);
	if (!out) {
		send_text(c, 500, "Internal Server Error", "Out of memory", keep_alive);
		return;
	}

	size_t used = 0;
	out[used++] = '[';
	int first = 1;

	diriter it;
	if (!dir_open(&it, search)) {
		free(out);
		send_text(c, 500, "Internal Server Error", "opendir failed", keep_alive);
		return;
	}

	const char* name;
	while ((name = dir_next(&it))) {
		if (!strcmp(name, ".") || !strcmp(name, ".."))
			continue;

		char full[PATH_MAX];
		path_join(full, search, name);
		if (!is_file(full))
			continue;
		if (!(has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS)))
			continue;

		if (!first) {
			ensure_json_buf(&out, &cap, used, 2);
			out[used++] = ',';
		}
		first = 0;

		ensure_json_buf(&out, &cap, used, strlen(name) + strlen(dirparam) + 12);
		out[used++] = '"';
		out[used++] = '/'; out[used++] = 'm'; out[used++] = 'e';
		out[used++] = 'd'; out[used++] = 'i'; out[used++] = 'a';
		out[used++] = '/';

		if (dirparam[0]) {
			char clean[PATH_MAX];
			size_t wi = 0;
			for (size_t i = 0; dirparam[i] && wi + 1 < sizeof(clean); ++i) {
				char ch = dirparam[i];
				if (ch == '\\') ch = '/';
				if (ch == '/' && wi > 0 && clean[wi - 1] == '/') continue;
				clean[wi++] = ch;
			}
			if (wi > 0 && clean[wi - 1] == '/') wi--;
			clean[wi] = '\0';
			if (wi > 0) {
				for (size_t i = 0; clean[i]; ++i) {
					if (used + 2 >= cap) ensure_json_buf(&out, &cap, used, 16);
					out[used++] = clean[i];
				}
				out[used++] = '/';
			}
		}

		for (const char* p = name; *p; ++p) {
			if (used + 2 >= cap) ensure_json_buf(&out, &cap, used, 16);
			out[used++] = *p;
		}
		out[used++] = '"';
	}

	dir_close(&it);

	ensure_json_buf(&out, &cap, used, 2);
	out[used++] = ']';

	send_header(c, 200, "OK", "application/json; charset=utf-8",
		(long)used, NULL, 0, keep_alive);
	send(c, out, (int)used, 0);
	free(out);
}


void handle_legacy_move(int c, const char* body, bool keep_alive) {
	if (!body) { send_text(c, 400, "Bad Request", "Missing body", keep_alive); return; }
	const char* f_start = strstr(body, "\"fromPath\":\"");
	const char* t_start = strstr(body, "\"targetFolder\":\"");

	{
		const char* maybe_from = strstr(body, "\"fromPath\":\"");
		const char* maybe_to = strstr(body, "\"targetFolder\":\"");
		if (maybe_from && maybe_to) {
			const char* mf = maybe_from + strlen("\"fromPath\":\"");
			const char* mt = maybe_to + strlen("\"targetFolder\":\"");
			const char* mfe = strchr(mf, '"');
			const char* mte = strchr(mt, '"');
			char lf[256] = { 0 }, lt[256] = { 0 };
			if (mfe && (size_t)(mfe - mf) < sizeof(lf)) strncpy(lf, mf, (size_t)(mfe - mf));
			if (mte && (size_t)(mte - mt) < sizeof(lt)) strncpy(lt, mt, (size_t)(mte - mt));
			char* p, * q; char prev = 0;
			p = lf; q = lf; prev = 0;
			while (*p) { char c = *p++; if (c == '\\') c = '/'; if (c == '/' && prev == '/') continue; *q++ = c; prev = c; }
			*q = '\0';
			p = lt; q = lt; prev = 0;
			while (*p) { char c = *p++; if (c == '\\') c = '/'; if (c == '/' && prev == '/') continue; *q++ = c; prev = c; }
			*q = '\0';
			LOG_INFO("handle_legacy_move requested from=%s target=%s", lf, lt);
		}
	}
	if (!f_start || !t_start) { send_text(c, 400, "Bad Request", "missing fields", keep_alive); return; }
	f_start += strlen("\"fromPath\":\""); t_start += strlen("\"targetFolder\":\"");
	const char* f_end = strchr(f_start, '"'); const char* t_end = strchr(t_start, '"');
	if (!f_end || !t_end) { send_text(c, 400, "Bad Request", "invalid data", keep_alive); return; }
	char from[MAX_PATH]; char target[MAX_PATH]; size_t fl = (size_t)(f_end - f_start); size_t tl = (size_t)(t_end - t_start);
	if (fl >= sizeof(from)) fl = sizeof(from) - 1; if (tl >= sizeof(target)) tl = sizeof(target) - 1;
	strncpy(from, f_start, fl); from[fl] = '\0'; strncpy(target, t_start, tl); target[tl] = '\0'; url_decode(from); url_decode(target);

	const char* rel = from;
	if (strncmp(from, "/media/", 7) == 0) rel = from + 7;
	char rel_copy[PATH_MAX];
	strncpy(rel_copy, rel, sizeof(rel_copy) - 1);
	rel_copy[sizeof(rel_copy) - 1] = '\0';
	if (strstr(rel_copy, "..") || rel_copy[0] == '/' || rel_copy[0] == '\\') {
		LOG_WARN("Rejected path traversal attempt: %s", rel_copy);
		return;
	}
	rel_copy[sizeof(rel_copy) - 1] = '\0';
	while (*rel_copy == '/' || *rel_copy == '\\') memmove(rel_copy, rel_copy + 1, strlen(rel_copy));
	char src[PATH_MAX]; snprintf(src, sizeof(src), "%s%s%s", BASE_DIR, DIR_SEP_STR, rel_copy); normalize_path(src);
	char* fname = strrchr(src, DIR_SEP);
	if (!fname || *(fname + 1) == '\0') { send_text(c, 400, "Bad Request", "invalid fromPath", keep_alive); return; }
	fname++;
	char target_copy[PATH_MAX]; strncpy(target_copy, target, sizeof(target_copy) - 1); target_copy[sizeof(target_copy) - 1] = 0;
	while (*target_copy == '/' || *target_copy == '\\') memmove(target_copy, target_copy + 1, strlen(target_copy));
	char destFolder[PATH_MAX]; if (strlen(target_copy) > 0) snprintf(destFolder, sizeof(destFolder), "%s%s%s", BASE_DIR, DIR_SEP_STR, target_copy); else snprintf(destFolder, sizeof(destFolder), "%s", BASE_DIR);
	normalize_path(destFolder); mk_dir(destFolder);
	char dest[PATH_MAX]; path_join(dest, destFolder, fname);

	platform_close_streams_for_path(src);
	if (platform_move_file(src, dest) == 0) {
		LOG_INFO("handle_legacy_move: renamed %s -> %s", src, dest);
		const char* ok = "{\"status\":\"ok\"}";
		send_header(c, 200, "OK", "application/json; charset=utf-8", (long)strlen(ok), NULL, 0, keep_alive);
		send(c, ok, (int)strlen(ok), 0);
		return;
	}
	platform_close_streams_for_path(src);
	if (platform_copy_file(src, dest) == 0) {
		LOG_INFO("handle_legacy_move: copied %s -> %s", src, dest);
		if (platform_file_delete(src) == 0) {
			LOG_INFO("handle_legacy_move: deleted original %s", src);
			const char* ok = "{\"status\":\"ok\"}";
			send_header(c, 200, "OK", "application/json; charset=utf-8", (long)strlen(ok), NULL, 0, keep_alive);
			send(c, ok, (int)strlen(ok), 0);
			return;
		}
		LOG_ERROR("handle_legacy_move: copied but failed to delete original %s", src);
		const char* msg = "{\"error\":\"copied but delete failed\"}";
		send_header(c, 500, "Internal Server Error", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
		send(c, msg, (int)strlen(msg), 0);
		return;
	}
	LOG_ERROR("handle_legacy_move: failed to move or copy %s -> %s", src, dest);
	char emsg[128]; snprintf(emsg, sizeof(emsg), "{\"error\":\"move failed\"}");
	send_header(c, 500, "Internal Server Error", "application/json; charset=utf-8", (long)strlen(emsg), NULL, 0, keep_alive);
	send(c, emsg, (int)strlen(emsg), 0);
}

void handle_legacy_addfolder(int c, const char* body, bool keep_alive) {
	if (!body) { send_text(c, 400, "Bad Request", "Missing body", keep_alive); return; }
	LOG_INFO("handle_legacy_addfolder request body=%s", body);
	const char* n_start = strstr(body, "\"name\":\"");
	if (!n_start) { send_text(c, 400, "Bad Request", "missing name", keep_alive); return; }
	n_start += strlen("\"name\":\""); const char* n_end = strchr(n_start, '"'); if (!n_end) { send_text(c, 400, "Bad Request", "invalid name", keep_alive); return; }
	char folder[PATH_MAX] = { 0 }; size_t nlen = (size_t)(n_end - n_start); if (nlen >= sizeof(folder)) nlen = sizeof(folder) - 1; strncpy(folder, n_start, nlen); folder[nlen] = '\0'; url_decode(folder);
	char target[PATH_MAX] = { 0 }; const char* t_start = strstr(body, "\"target\":\""); if (t_start) { t_start += strlen("\"target\":\""); const char* t_end = strchr(t_start, '"'); if (t_end) { size_t tlen = (size_t)(t_end - t_start); if (tlen >= sizeof(target)) tlen = sizeof(target) - 1; strncpy(target, t_start, tlen); target[tlen] = '\0'; url_decode(target); } }
	char target_copy[PATH_MAX]; strncpy(target_copy, target, sizeof(target_copy) - 1); target_copy[sizeof(target_copy) - 1] = 0; while (*target_copy == '/' || *target_copy == '\\') memmove(target_copy, target_copy + 1, strlen(target_copy));
	char dest[PATH_MAX]; if (strlen(target_copy) > 0) snprintf(dest, sizeof(dest), "%s%s%s%s%s", BASE_DIR, DIR_SEP_STR, target_copy, DIR_SEP_STR, folder); else snprintf(dest, sizeof(dest), "%s%s%s", BASE_DIR, DIR_SEP_STR, folder);
	normalize_path(dest); mk_dir(dest);
	if (is_dir(dest)) {
		LOG_INFO("handle_legacy_addfolder created folder: %s", dest);
		char fg_path[PATH_MAX];
		path_join(fg_path, dest, ".fg");
		FILE* fgf = fopen(fg_path, "wb");
		if (fgf) fclose(fgf);
		char msg[1024];
		int rr = snprintf(msg, sizeof(msg), "{\"type\":\"folderAdded\",\"path\":\"%s\"}", dest);
		if (rr > 0) websocket_broadcast_topic(dest, msg);
		const char* ok = "{\"status\":\"ok\"}";
		send_header(c, 200, "OK", "application/json; charset=utf-8", (long)strlen(ok), NULL, 0, keep_alive);
		send(c, ok, (int)strlen(ok), 0);
		return;
	}
	LOG_ERROR("handle_legacy_addfolder mkdir failed for: %s", dest);
	const char msg[128] = "{\"error\":\"mkdir failed\"}";
	send_header(c, 500, "Internal Server Error", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
	send(c, msg, (int)strlen(msg), 0);
}
typedef struct {
	char* buf;
	size_t cap;
	size_t used;
	int first;
	int filter_enabled;
	char per_thumbs_root[PATH_MAX];
	char base_real[PATH_MAX];
} tdb_list_ctx_t;

static void normalize_value_inplace(char* v) {
	if (!v) return;
	size_t i = 0, j = 0; while (v[i]) { char ch = v[i++]; if (ch == '\\') ch = '/'; v[j++] = ch; }
	while (j > 0 && (v[j - 1] == ' ' || v[j - 1] == '\t' || v[j - 1] == '\r' || v[j - 1] == '\n' || v[j - 1] == '/')) j--; v[j] = '\0';
}

static void format_thumbdb_value(const char* raw, const char* base_real, char* out, size_t outlen) {
	if (!out || outlen == 0) return;
	out[0] = '\0';
	if (!raw || raw[0] == '\0') return;
	char tmp[PATH_MAX];
	size_t len = strlen(raw);
	if (len >= sizeof(tmp)) len = sizeof(tmp) - 1;
	memcpy(tmp, raw, len);
	tmp[len] = '\0';
	normalize_value_inplace(tmp);
	if (tmp[0] == '\0') return;
	if (base_real && base_real[0] != '\0' && safe_under(base_real, tmp)) {
		size_t bl = strlen(base_real);
		const char* rel = tmp + bl + ((base_real[bl] == DIR_SEP) ? 1 : 0);
		char outp[PATH_MAX];
		size_t ri = 0;
		while (rel[ri] && ri + 1 < sizeof(outp)) {
			outp[ri] = (rel[ri] == '\\') ? '/' : rel[ri];
			ri++;
		}
		outp[ri] = '\0';
		snprintf(out, outlen, "/images/%s", outp);
		return;
	}
	size_t copy_len = strlen(tmp);
	if (copy_len >= outlen) copy_len = outlen - 1;
	memcpy(out, tmp, copy_len);
	out[copy_len] = '\0';
}

static void thumbdb_list_cb(const char* key, const char* value, void* ctx) {
	tdb_list_ctx_t* c = (tdb_list_ctx_t*)ctx;
	if (!key) return;
	if (c->filter_enabled) {
		if (strchr(key, '/') || strchr(key, '\\')) return;
		char full[PATH_MAX]; path_join(full, c->per_thumbs_root, key);
		if (!is_file(full)) return;
	}
	char formatted[PATH_MAX] = "";
	format_thumbdb_value(value, c->base_real, formatted, sizeof(formatted));
	size_t used = c->used;
	ensure_json_buf(&c->buf, &c->cap, used, 256 + strlen(key) + strlen(formatted));
	char* ptr = c->buf + used;
	size_t rem = c->cap - used;
	if (!c->first) {
		ptr = json_comma_safe(ptr, &rem);
	}
	else {
		c->first = 0;
	}
	ptr = json_objOpen(ptr, NULL, &rem);
	ptr = json_str(ptr, "key", key, &rem);
	ptr = json_str(ptr, "value", formatted, &rem);
	ptr = json_objClose(ptr, &rem);
	c->used = ptr - c->buf;
}

void handle_api_thumbdb_list(int c, char* qs, bool keep_alive) {
	size_t cap = 8192;
	char* buf = malloc(cap);
	if (!buf) { send_text(c, 500, "Internal Server Error", "Out of memory", keep_alive); return; }
	size_t used = 0;
	char* ptr = buf;
	size_t rem = cap;
	ptr = json_objOpen(ptr, NULL, &rem);
	ptr = json_arrOpen(ptr, "items", &rem);
	used = ptr - buf;
	tdb_list_ctx_t ctx;
	ctx.buf = buf; ctx.cap = cap; ctx.used = used; ctx.first = 1; ctx.filter_enabled = 0; ctx.per_thumbs_root[0] = '\0'; ctx.base_real[0] = '\0';
	if (qs) {
		char* dir = query_get(qs, "dir");
		if (dir) {
			char dircopy[PATH_MAX]; strncpy(dircopy, dir, sizeof(dircopy) - 1); dircopy[sizeof(dircopy) - 1] = '\0'; sanitize_dirparam(dircopy);
			char target_real[PATH_MAX]; char base_real[PATH_MAX];
			if (!resolve_and_validate_target(BASE_DIR, dircopy, target_real, sizeof(target_real), base_real, sizeof(base_real))) {
				SAFE_FREE(dir);
				free(buf);
				const char* msg = "{\"error\":\"Invalid directory\"}";
				send_header(c, 400, "Bad Request", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
				send(c, msg, (int)strlen(msg), 0);
				return;
			}
			{
				char safe_dir[PATH_MAX]; safe_dir[0] = '\0';
				make_safe_dir_name_from(target_real, safe_dir, sizeof(safe_dir));
				char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
				if (!safe_dir[0]) {
					size_t gf_count = 0; char** gfolders = get_gallery_folders(&gf_count);
					if (gf_count > 0 && gfolders[0]) {
						char first_real[PATH_MAX]; if (real_path(gfolders[0], first_real)) make_safe_dir_name_from(first_real, safe_dir, sizeof(safe_dir));
					}
				}
				if (safe_dir[0]) snprintf(ctx.per_thumbs_root, sizeof(ctx.per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir);
				else snprintf(ctx.per_thumbs_root, sizeof(ctx.per_thumbs_root), "%s", thumbs_root);
				strncpy(ctx.base_real, base_real, sizeof(ctx.base_real) - 1);
				ctx.base_real[sizeof(ctx.base_real) - 1] = '\0';
				ctx.filter_enabled = 1;
				char per_db[PATH_MAX]; char per_thumbs_root[PATH_MAX]; strncpy(per_thumbs_root, ctx.per_thumbs_root, sizeof(per_thumbs_root) - 1); per_thumbs_root[sizeof(per_thumbs_root) - 1] = '\0'; mk_dir(per_thumbs_root);
				snprintf(per_db, sizeof(per_db), "%s" DIR_SEP_STR "thumbs.db", per_thumbs_root);
				thumbdb_open_for_dir(per_db);
			}
			SAFE_FREE(dir);
		}
	}
	char* plain_flag = NULL;
	if (qs) plain_flag = query_get(qs, "plain");
	if (plain_flag) {
		SAFE_FREE(plain_flag);
		{
			api_collect_ctx_t cctx = { NULL, 0, 0, 0 };
			thumbdb_iterate(api_thumbdb_collect_cb, &cctx);
			if (cctx.err) {
				for (size_t i = 0; i < cctx.count; ++i) { free(cctx.arr[i].key); free(cctx.arr[i].val); }
				free(cctx.arr);
				free(buf);
				send_text(c, 500, "Internal Server Error", "Memory error", keep_alive);
				return;
			}
			for (size_t i = 0; i < cctx.count; ++i) {
				if (!cctx.arr[i].key) continue;
				for (size_t j = i + 1; j < cctx.count; ++j) {
					if (!cctx.arr[j].key) continue;
					if (strcmp(cctx.arr[i].key, cctx.arr[j].key) == 0) {
						char media_i[PATH_MAX] = ""; char media_j[PATH_MAX] = "";
						format_thumbdb_value(cctx.arr[i].val, ctx.base_real, media_i, sizeof(media_i));
						format_thumbdb_value(cctx.arr[j].val, ctx.base_real, media_j, sizeof(media_j));
						int keep_j = 0;
						if ((media_i[0] == '\0') && (media_j[0] != '\0')) keep_j = 1;
						if (keep_j) {
							free(cctx.arr[i].val); cctx.arr[i].val = cctx.arr[j].val; cctx.arr[j].val = NULL;
							free(cctx.arr[j].key); cctx.arr[j].key = NULL;
						}
						else {
							free(cctx.arr[j].val); cctx.arr[j].val = NULL; free(cctx.arr[j].key); cctx.arr[j].key = NULL;
						}
					}
				}
			}
			char encbuf[PATH_MAX];
			size_t outcap = 4096; char* outbuf = malloc(outcap); if (!outbuf) { for (size_t i = 0;i < cctx.count;i++) { free(cctx.arr[i].key); free(cctx.arr[i].val); } free(cctx.arr); free(buf); send_text(c, 500, "Internal Server Error", "Memory error", keep_alive); return; }
			size_t outs = 0;
			for (size_t i = 0; i < cctx.count; ++i) {
				if (!cctx.arr[i].key) continue;
				char small_tok[64] = "null"; char large_tok[64] = "null"; char media_display[PATH_MAX] = "";
				if (cctx.arr[i].val) {
					format_thumbdb_value(cctx.arr[i].val, ctx.base_real, media_display, sizeof(media_display));
				}
				{
					size_t oi = 0;
					for (size_t pi = 0; media_display[pi] && oi + 4 < sizeof(encbuf); ++pi) {
						unsigned char ch = (unsigned char)media_display[pi];
						if (ch == '\\') ch = '/';
						if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' || ch == '/' || ch == ':') { encbuf[oi++] = ch; }
						else {
							snprintf(encbuf + oi, sizeof(encbuf) - oi, "\\x%02X", ch); oi += 4;
						}
					}
					encbuf[oi] = '\0';
				}
				size_t need = strlen(cctx.arr[i].key) + 1 + strlen(small_tok) + 1 + strlen(large_tok) + 1 + strlen(encbuf) + 2;
				if (outs + need > outcap) {
					size_t nc = (outs + need) * 2;
					char* tmp = realloc(outbuf, nc);
					if (!tmp) break;
					outbuf = tmp; outcap = nc;
				}
				outs += snprintf(outbuf + outs, outcap - outs, "%s;%s;%s;%s\n", cctx.arr[i].key, small_tok, large_tok, encbuf);
			}
			for (size_t i = 0; i < cctx.count; ++i) { free(cctx.arr[i].key); free(cctx.arr[i].val); }
			free(cctx.arr);
			free(buf);
			send_header(c, 200, "OK", "text/plain; charset=utf-8", (long)outs, NULL, 0, keep_alive);
			send(c, outbuf, (int)outs, 0);
			free(outbuf);
			return;
		}
	}
	{
		api_collect_ctx_t cctx = { NULL, 0, 0, 0 };
		thumbdb_iterate(api_thumbdb_collect_cb, &cctx);
		if (cctx.err) {
			for (size_t i = 0; i < cctx.count; ++i) { free(cctx.arr[i].key); free(cctx.arr[i].val); }
			free(cctx.arr);
			free(buf);
			send_text(c, 500, "Internal Server Error", "Memory error", keep_alive);
			return;
		}
		for (size_t i = 0; i < cctx.count; ++i) {
			if (!cctx.arr[i].key) continue;
			for (size_t j = i + 1; j < cctx.count; ++j) {
				if (!cctx.arr[j].key) continue;
				if (strcmp(cctx.arr[i].key, cctx.arr[j].key) == 0) {
					char media_i[PATH_MAX] = ""; char media_j[PATH_MAX] = "";
					if (cctx.arr[i].val) {
						const char* v = cctx.arr[i].val;
						const char* s1 = strchr(v, ';');
						if (s1) { const char* s2 = strchr(s1 + 1, ';'); if (s2) strncpy(media_i, s2 + 1, sizeof(media_i) - 1); }
					}
					if (cctx.arr[j].val) {
						const char* v = cctx.arr[j].val;
						const char* s1 = strchr(v, ';');
						if (s1) { const char* s2 = strchr(s1 + 1, ';'); if (s2) strncpy(media_j, s2 + 1, sizeof(media_j) - 1); }
					}
					int keep_j = 0;
					if ((media_i[0] == '\0') && (media_j[0] != '\0')) keep_j = 1;
					if (keep_j) {
						free(cctx.arr[i].val);
						cctx.arr[i].val = cctx.arr[j].val;
						cctx.arr[j].val = NULL;
						free(cctx.arr[j].key);
						cctx.arr[j].key = NULL;
					}
					else {
						free(cctx.arr[j].val);
						cctx.arr[j].val = NULL;
						free(cctx.arr[j].key);
						cctx.arr[j].key = NULL;
					}
				}
			}
		}

		for (size_t i = 0; i < cctx.count; ++i) {
			if (!cctx.arr[i].key) continue;
			char small_tok[64] = "null"; char large_tok[64] = "null"; char media[PATH_MAX] = "";
			if (cctx.arr[i].val) {
				const char* v = cctx.arr[i].val;
				const char* p1 = strchr(v, ';');
				if (p1) {
					size_t l1 = (size_t)(p1 - v);
					size_t l = l1 < sizeof(small_tok) - 1 ? l1 : sizeof(small_tok) - 1;
					memcpy(small_tok, v, l); small_tok[l] = '\0';
					const char* p2 = strchr(p1 + 1, ';');
					if (p2) {
						size_t l2 = (size_t)(p2 - (p1 + 1)); size_t ll = l2 < sizeof(large_tok) - 1 ? l2 : sizeof(large_tok) - 1;
						memcpy(large_tok, p1 + 1, ll); large_tok[ll] = '\0';
						strncpy(media, p2 + 1, sizeof(media) - 1); media[sizeof(media) - 1] = '\0';
					}
					else {
						size_t l2 = strlen(p1 + 1); size_t ll = l2 < sizeof(large_tok) - 1 ? l2 : sizeof(large_tok) - 1;
						memcpy(large_tok, p1 + 1, ll); large_tok[ll] = '\0';
					}
				}
				else {
					strncpy(media, v, sizeof(media) - 1); media[sizeof(media) - 1] = '\0';
				}
			}
			if (media[0]) normalize_value_inplace(media);
			char out_media[PATH_MAX] = "";
			if (media[0] && ctx.base_real[0] && safe_under(ctx.base_real, media)) {
				size_t bl = strlen(ctx.base_real);
				const char* rel = media + bl + ((ctx.base_real[bl] == DIR_SEP) ? 1 : 0);
				char outp[PATH_MAX]; size_t ri = 0;
				for (size_t ii = 0; rel[ii] && ri + 1 < sizeof(outp); ++ii) outp[ri++] = (rel[ii] == '\\') ? '/' : rel[ii]; outp[ri] = '\0';
				snprintf(out_media, sizeof(out_media), "/images/%s", outp);
			}
			else if (media[0]) {
				strncpy(out_media, media, sizeof(out_media) - 1); out_media[sizeof(out_media) - 1] = '\0';
			}
			size_t used_local = ctx.used;
			ensure_json_buf(&ctx.buf, &ctx.cap, used_local, 512 + strlen(cctx.arr[i].key) + strlen(small_tok) + strlen(large_tok) + strlen(out_media));
			char* p = ctx.buf + ctx.used; size_t rem_local = ctx.cap - ctx.used;
			if (!ctx.first) p = json_comma_safe(p, &rem_local); else ctx.first = 0;
			p = json_objOpen(p, NULL, &rem_local);
			p = json_str(p, "key", cctx.arr[i].key, &rem_local);
			p = json_str(p, "small", small_tok, &rem_local);
			p = json_str(p, "large", large_tok, &rem_local);
			p = json_str(p, "value", out_media, &rem_local);
			p = json_objClose(p, &rem_local);
			ctx.used = p - ctx.buf;
		}

		for (size_t i = 0; i < cctx.count; ++i) { free(cctx.arr[i].key); free(cctx.arr[i].val); }
		free(cctx.arr);

		buf = ctx.buf; cap = ctx.cap; used = ctx.used;
		ptr = buf + used; rem = cap - used;
		ptr = json_arrClose(ptr, &rem);
		ptr = json_objClose(ptr, &rem);
		used = ptr - buf;
		send_header(c, 200, "OK", "application/json; charset=utf-8", (long)used, NULL, 0, keep_alive);
		send(c, buf, (int)used, 0);
		free(buf);
	}
}

void handle_api_thumbdb_get(int c, char* qs, bool keep_alive) {
	if (!qs) { send_text(c, 400, "Bad Request", "Missing query", keep_alive); return; }
	char* k = query_get(qs, "key");
	if (!k) { send_text(c, 400, "Bad Request", "Missing key", keep_alive); return; }
	if (qs) {
		char* dirq = query_get(qs, "dir");
		if (dirq) {
			char dircopy[PATH_MAX]; strncpy(dircopy, dirq, sizeof(dircopy) - 1); dircopy[sizeof(dircopy) - 1] = '\0'; sanitize_dirparam(dircopy);
			char target_real[PATH_MAX]; char base_real[PATH_MAX];
			if (resolve_and_validate_target(BASE_DIR, dircopy, target_real, sizeof(target_real), base_real, sizeof(base_real))) {
				char safe_dir[PATH_MAX]; safe_dir[0] = '\0'; make_safe_dir_name_from(target_real, safe_dir, sizeof(safe_dir));
				if (!safe_dir[0]) {
					size_t gf_count = 0; char** gfolders = get_gallery_folders(&gf_count);
					if (gf_count > 0 && gfolders[0]) { char first_real[PATH_MAX]; if (real_path(gfolders[0], first_real)) make_safe_dir_name_from(first_real, safe_dir, sizeof(safe_dir)); }
				}
				char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
				char per_thumbs_root[PATH_MAX]; if (safe_dir[0]) snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir); else snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s", thumbs_root);
				mk_dir(per_thumbs_root);
				char per_db[PATH_MAX]; snprintf(per_db, sizeof(per_db), "%s" DIR_SEP_STR "thumbs.db", per_thumbs_root);
				thumbdb_open_for_dir(per_db);
			}
			SAFE_FREE(dirq);
		}
	}

	char val[65536]; val[0] = '\0';
	int r = thumbdb_get(k, val, sizeof(val));
	if (r != 0) {
		SAFE_FREE(k);
		send_text(c, 404, "Not Found", "Key not found", keep_alive);
		return;
	}
	normalize_value_inplace(val);
	char smalltok[256] = { 0 }; char largetok[256] = { 0 }; char* media = val;
	char* s1 = strchr(val, ';');
	if (s1) {
		size_t sl = (size_t)(s1 - val);
		if (sl >= sizeof(smalltok)) sl = sizeof(smalltok) - 1;
		memcpy(smalltok, val, sl); smalltok[sl] = '\0';
		char* s2 = strchr(s1 + 1, ';');
		if (s2) {
			size_t ll = (size_t)(s2 - (s1 + 1)); if (ll >= sizeof(largetok)) ll = sizeof(largetok) - 1;
			memcpy(largetok, s1 + 1, ll); largetok[ll] = '\0';
			media = s2 + 1;
		}
		else {
			media = s1 + 1;
		}
	}
	size_t val_len = strlen(media);
	size_t cap = val_len + 1024;
	if (cap < 1024) cap = 1024;
	char* buf = malloc(cap);
	if (!buf) { SAFE_FREE(k); send_text(c, 500, "Internal Server Error", "Out of memory", keep_alive); return; }
	size_t rem = cap; char* ptr = buf;
	ptr = json_objOpen(ptr, NULL, &rem);
	ptr = json_str(ptr, "key", k, &rem);
	ptr = json_str(ptr, "small", smalltok, &rem);
	ptr = json_str(ptr, "large", largetok, &rem);
	ptr = json_str(ptr, "value", media, &rem);
	ptr = json_objClose(ptr, &rem);
	size_t used = ptr - buf;
	send_header(c, 200, "OK", "application/json; charset=utf-8", (long)used, NULL, 0, keep_alive);
	send(c, buf, (int)used, 0);
	free(buf); SAFE_FREE(k);
}

void handle_api_thumbdb_set(int c, const char* body, bool keep_alive) {
	if (!body) { send_text(c, 400, "Bad Request", "Missing body", keep_alive); return; }
	if (g_request_qs) {
		char* dirq = query_get(g_request_qs, "dir");
		if (dirq) {
			char dircopy[PATH_MAX]; strncpy(dircopy, dirq, sizeof(dircopy) - 1); dircopy[sizeof(dircopy) - 1] = '\0'; sanitize_dirparam(dircopy);
			char target_real[PATH_MAX]; char base_real[PATH_MAX];
			if (resolve_and_validate_target(BASE_DIR, dircopy, target_real, sizeof(target_real), base_real, sizeof(base_real))) {
				char safe_dir[PATH_MAX]; safe_dir[0] = '\0'; make_safe_dir_name_from(target_real, safe_dir, sizeof(safe_dir));
				if (!safe_dir[0]) {
					size_t gf_count = 0; char** gfolders = get_gallery_folders(&gf_count);
					if (gf_count > 0 && gfolders[0]) { char first_real[PATH_MAX]; if (real_path(gfolders[0], first_real)) make_safe_dir_name_from(first_real, safe_dir, sizeof(safe_dir)); }
				}
				char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
				char per_thumbs_root[PATH_MAX]; if (safe_dir[0]) snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir); else snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s", thumbs_root);
				mk_dir(per_thumbs_root);
				char per_db[PATH_MAX]; snprintf(per_db, sizeof(per_db), "%s" DIR_SEP_STR "thumbs.db", per_thumbs_root);
				thumbdb_open_for_dir(per_db);
			}
			SAFE_FREE(dirq);
		}
	}
	const char* kstart = strstr(body, "\"key\":\"");
	const char* vstart = strstr(body, "\"value\":\"");
	if (!kstart || !vstart) { send_text(c, 400, "Bad Request", "Missing fields", keep_alive); return; }
	kstart += strlen("\"key\":\""); vstart += strlen("\"value\":\"");
	const char* kend = strchr(kstart, '"'); const char* vend = strchr(vstart, '"');
	if (!kend || !vend) { send_text(c, 400, "Bad Request", "Invalid data", keep_alive); return; }
	size_t klen = (size_t)(kend - kstart); size_t vlen = (size_t)(vend - vstart);
	char* key = malloc(klen + 1); char* val = malloc(vlen + 1);
	if (!key || !val) { free(key); free(val); send_text(c, 500, "Internal Server Error", "Out of memory", keep_alive); return; }
	memcpy(key, kstart, klen); key[klen] = '\0'; memcpy(val, vstart, vlen); val[vlen] = '\0';
	url_decode(key); url_decode(val);
	normalize_path(val);
	int r = thumbdb_set(key, val);
	if (r == 0)
		thumbdb_request_compaction();
	free(key); free(val);
	if (r == 0) send_text(c, 200, "OK", "{\"status\":\"ok\"}", keep_alive); else send_text(c, 500, "Internal Server Error", "set failed", keep_alive);
}

void handle_api_thumbdb_delete(int c, const char* body, bool keep_alive) {
	if (!body) { send_text(c, 400, "Bad Request", "Missing body", keep_alive); return; }
	if (g_request_qs) {
		char* dirq = query_get(g_request_qs, "dir");
		if (dirq) {
			char dircopy[PATH_MAX]; strncpy(dircopy, dirq, sizeof(dircopy) - 1); dircopy[sizeof(dircopy) - 1] = '\0'; sanitize_dirparam(dircopy);
			char target_real[PATH_MAX]; char base_real[PATH_MAX];
			if (resolve_and_validate_target(BASE_DIR, dircopy, target_real, sizeof(target_real), base_real, sizeof(base_real))) {
				char safe_dir[PATH_MAX]; safe_dir[0] = '\0'; make_safe_dir_name_from(target_real, safe_dir, sizeof(safe_dir));
				if (!safe_dir[0]) {
					size_t gf_count = 0; char** gfolders = get_gallery_folders(&gf_count);
					if (gf_count > 0 && gfolders[0]) { char first_real[PATH_MAX]; if (real_path(gfolders[0], first_real)) make_safe_dir_name_from(first_real, safe_dir, sizeof(safe_dir)); }
				}
				char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
				char per_thumbs_root[PATH_MAX]; if (safe_dir[0]) snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s" DIR_SEP_STR "%s", thumbs_root, safe_dir); else snprintf(per_thumbs_root, sizeof(per_thumbs_root), "%s", thumbs_root);
				mk_dir(per_thumbs_root);
				char per_db[PATH_MAX]; snprintf(per_db, sizeof(per_db), "%s" DIR_SEP_STR "thumbs.db", per_thumbs_root);
				thumbdb_open_for_dir(per_db);
			}
			SAFE_FREE(dirq);
		}
	}
	const char* kstart = strstr(body, "\"key\":\"");
	if (!kstart) { send_text(c, 400, "Bad Request", "Missing key", keep_alive); return; }
	kstart += strlen("\"key\":\"");
	const char* kend = strchr(kstart, '"'); if (!kend) { send_text(c, 400, "Bad Request", "Invalid data", keep_alive); return; }
	size_t klen = (size_t)(kend - kstart);
	char* key = malloc(klen + 1); if (!key) { send_text(c, 500, "Internal Server Error", "Out of memory", keep_alive); return; }
	memcpy(key, kstart, klen); key[klen] = '\0'; url_decode(key);
	int r = thumbdb_delete(key);
	free(key);
	if (r == 0) send_text(c, 200, "OK", "{\"status\":\"ok\"}", keep_alive); else send_text(c, 500, "Internal Server Error", "delete failed", keep_alive);
}

void handle_api_thumbdb_thumbs_for_dir(int c, char* qs, bool keep_alive) {
	if (!qs) { send_text(c, 400, "Bad Request", "Missing query", keep_alive); return; }
	char* dir = query_get(qs, "dir");
	if (!dir) { send_text(c, 400, "Bad Request", "Missing dir", keep_alive); return; }
	char dircopy[PATH_MAX]; strncpy(dircopy, dir, sizeof(dircopy) - 1); dircopy[sizeof(dircopy) - 1] = '\0'; sanitize_dirparam(dircopy);
	char target_real[PATH_MAX]; char base_real[PATH_MAX];
	if (!resolve_and_validate_target(BASE_DIR, dircopy, target_real, sizeof(target_real), base_real, sizeof(base_real))) {
		SAFE_FREE(dir);
		send_text(c, 400, "Bad Request", "Invalid directory", keep_alive);
		return;
	}
	char safe_dir[PATH_MAX]; safe_dir[0] = '\0'; make_safe_dir_name_from(target_real, safe_dir, sizeof(safe_dir));
	char thumbs_root[PATH_MAX]; get_thumbs_root(thumbs_root, sizeof(thumbs_root));
	char url[PATH_MAX]; if (safe_dir[0]) snprintf(url, sizeof(url), "/images/thumbs/%s", safe_dir); else snprintf(url, sizeof(url), "/images/thumbs");
	size_t cap = 512; char* buf = malloc(cap); size_t rem = cap; char* ptr = buf;
	ptr = json_objOpen(ptr, NULL, &rem);
	ptr = json_str(ptr, "url", url, &rem);
	ptr = json_objClose(ptr, &rem);
	size_t used = ptr - buf;
	send_header(c, 200, "OK", "application/json; charset=utf-8", (long)used, NULL, 0, keep_alive);
	send(c, buf, (int)used, 0);
	free(buf); SAFE_FREE(dir);
}

void handle_api_delete_file(int c, const char* body, bool keep_alive) {
	if (!body) { send_text(c, 400, "Bad Request", "Missing body", keep_alive); return; }
	const char* f_start = strstr(body, "\"fromPath\"");
	if (!f_start) { send_text(c, 400, "Bad Request", "Missing fromPath", keep_alive); return; }
	f_start = strchr(f_start, ':');
	if (!f_start) { send_text(c, 400, "Bad Request", "Invalid fromPath", keep_alive); return; }
	f_start++;
	while (*f_start && (isspace((unsigned char)*f_start) || *f_start == ':')) f_start++;
	if (*f_start != '"') { send_text(c, 400, "Bad Request", "Invalid fromPath", keep_alive); return; }
	f_start++;
	const char* f_end = strchr(f_start, '"');
	if (!f_end) { send_text(c, 400, "Bad Request", "Invalid fromPath", keep_alive); return; }
	char from[MAX_PATH]; size_t fl = (size_t)(f_end - f_start); if (fl >= sizeof(from)) fl = sizeof(from) - 1;
	strncpy(from, f_start, fl); from[fl] = '\0'; url_decode(from);
	const char* rel = from;
	if (strncmp(from, "/media/", 7) == 0) rel = from + 7;
	char rel_copy[PATH_MAX]; strncpy(rel_copy, rel, sizeof(rel_copy) - 1); rel_copy[sizeof(rel_copy) - 1] = '\0';
	while (*rel_copy == '/' || *rel_copy == '\\') memmove(rel_copy, rel_copy + 1, strlen(rel_copy));
	char src[PATH_MAX]; snprintf(src, sizeof(src), "%s%s%s", BASE_DIR, DIR_SEP_STR, rel_copy); normalize_path(src);
	char* fname = strrchr(src, DIR_SEP);
	if (!fname || *(fname + 1) == '\0') { send_text(c, 400, "Bad Request", "invalid fromPath", keep_alive); return; }
	fname++;
	char trash_root[PATH_MAX]; snprintf(trash_root, sizeof(trash_root), "%s" DIR_SEP_STR "trash", BASE_DIR); normalize_path(trash_root); mk_dir(trash_root);
	char destFolder[PATH_MAX]; strncpy(destFolder, trash_root, sizeof(destFolder) - 1); destFolder[sizeof(destFolder) - 1] = '\0';
	char rel_dir[PATH_MAX]; strncpy(rel_dir, rel_copy, sizeof(rel_dir) - 1); rel_dir[sizeof(rel_dir) - 1] = '\0';
	char* l = strrchr(rel_dir, '/');
	if (l) {
		*l = '\0';
		char tmp[PATH_MAX]; snprintf(tmp, sizeof(tmp), "%s" DIR_SEP_STR "%s", trash_root, rel_dir); normalize_path(tmp); strncpy(destFolder, tmp, sizeof(destFolder) - 1); destFolder[sizeof(destFolder) - 1] = '\0';
	}
	normalize_path(destFolder); mk_dir(destFolder);
	char dest[PATH_MAX]; path_join(dest, destFolder, fname);
	if (platform_move_file(src, dest) == 0) {
		const char* ok = "{\"status\":\"ok\"}";
		send_header(c, 200, "OK", "application/json; charset=utf-8", (long)strlen(ok), NULL, 0, keep_alive);
		send(c, ok, (int)strlen(ok), 0);
		return;
	}
	if (platform_copy_file(src, dest) == 0) {
		if (platform_file_delete(src) == 0) {
			const char* ok = "{\"status\":\"ok\"}";
			send_header(c, 200, "OK", "application/json; charset=utf-8", (long)strlen(ok), NULL, 0, keep_alive);
			send(c, ok, (int)strlen(ok), 0);
			return;
		}
		const char* msg = "{\"error\":\"copied but delete failed\"}";
		send_header(c, 500, "Internal Server Error", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
		send(c, msg, (int)strlen(msg), 0);
		return;
	}
	char emsg[128]; snprintf(emsg, sizeof(emsg), "{\"error\":\"delete failed\"}");
	send_header(c, 500, "Internal Server Error", "application/json; charset=utf-8", (long)strlen(emsg), NULL, 0, keep_alive);
	send(c, emsg, (int)strlen(emsg), 0);
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
int handle_single_request(int c, char* headers, char* body, size_t headers_len, size_t body_len, bool keep_alive) {
	(void)headers_len;

	g_request_headers = headers;
	char method[8] = { 0 }, url[PATH_MAX] = { 0 };
	{
		char* p = headers;
		char* sp = strchr(p, ' ');
		if (!sp) { send_text(c, 400, "Bad Request", "Malformed request", false); return 0; }
		size_t mlen = (size_t)(sp - p);
		if (mlen >= sizeof(method)) mlen = sizeof(method) - 1;
		memcpy(method, p, mlen); method[mlen] = '\0';
		char* p2 = sp + 1;
		char* sp2 = strchr(p2, ' ');
		size_t ulen = sp2 ? (size_t)(sp2 - p2) : strlen(p2);
		if (ulen >= sizeof(url)) ulen = sizeof(url) - 1;
		memcpy(url, p2, ulen); url[ulen] = '\0';
	}

	char* qs = strchr(url, '?');
	if (qs) { *qs = '\0'; qs++; }
	g_request_qs = NULL;
	url_decode(url);
	STRCPY(g_request_url, url);
	char* range = get_header_value(headers, "Range:");

	char* upgrade = get_header_value(headers, "Upgrade:");

	char* connection_hdr = get_header_value(headers, "Connection:");

	if (upgrade || connection_hdr) {
		LOG_DEBUG("Incoming request headers: Upgrade=%s Connection=%s", upgrade ? upgrade : "(null)", connection_hdr ? connection_hdr : "(null)");
	}
	if (upgrade && connection_hdr && strcasecmp(upgrade, "websocket") == 0) {
		const char* p = connection_hdr; int found = 0;
		while (*p) { if (tolower((unsigned char)*p) == 'u' && strncasecmp(p, "upgrade", 7) == 0) { found = 1; break; } p++; }
		if (found) {
			if (websocket_register_socket(c, headers)) {
				SAFE_FREE(upgrade);
				SAFE_FREE(connection_hdr);
				return 1;
			}
		}
	}
	SAFE_FREE(upgrade);
	SAFE_FREE(connection_hdr);


	static const route_t routes[] = {
		{ "/api/thumbdb/thumbs_for_dir", GET_QS, handle_api_thumbdb_thumbs_for_dir },
		{ "/api/thumbdb/list", GET_QS, handle_api_thumbdb_list },
		{ "/api/thumbdb/get", GET_QS, handle_api_thumbdb_get },
		{ "/api/thumbdb/set", POST_BODY, handle_api_thumbdb_set },
		{ "/api/thumbdb/delete", POST_BODY, handle_api_thumbdb_delete },
		{ "/folders", GET_SIMPLE, handle_legacy_folders },
		{ "/files", GET_QS, handle_legacy_files },
		{ "/move", POST_BODY, handle_legacy_move },
		{ "/addfolder", POST_BODY, handle_legacy_addfolder },
		{ "/api/delete-file", POST_BODY, handle_api_delete_file },
		{ "/api/tree", GET_SIMPLE, handle_api_tree },
		{ "/api/folders/list", GET_SIMPLE, handle_api_list_folders },
		{ "/api/folders", GET_QS, handle_api_folders },
		{ "/api/media", GET_QS, handle_api_media },
		{ "/api/folders/add", POST_BODY, handle_api_add_folder },
		{ "/api/regenerate-thumbs", GET_QS, handle_api_regenerate_thumbs },
	};
	static const static_route_t static_routes[] = {
		{ "/images/", BASE_DIR, true },
		{ "/media/", BASE_DIR, true },
		{ "/js/", JS_DIR, false },
		{ "/css/", CSS_DIR, false },
	};

	if (strcmp(url, "/mover") == 0 || strcmp(url, "/mover/") == 0) {
		char path[1024];
		snprintf(path, sizeof(path), "%s" DIR_SEP_STR "mover.html", VIEWS_DIR);
		LOG_DEBUG("Serving mover page: %s", path);
		if (!is_file(path)) { send_text(c, 404, "Not Found", "mover.html not found", keep_alive); SAFE_FREE(range); return 0; }
		FILE* f = fopen(path, "rb");
		if (!f) { LOG_ERROR("Failed to open mover.html: %s", path); send_text(c, 500, "Internal Server Error", "failed to open mover.html", keep_alive); SAFE_FREE(range); return 0; }
		fseek(f, 0, SEEK_END); long fsz = ftell(f); fseek(f, 0, SEEK_SET);
		char* buf = malloc(fsz + 1); if (!buf) { fclose(f); send_text(c, 500, "Internal Server Error", "oom", keep_alive); SAFE_FREE(range); return 0; }
		fread(buf, 1, fsz, f); buf[fsz] = '\0'; fclose(f);
		send_header(c, 200, "OK", "text/html; charset=utf-8", (long)fsz, NULL, 0, keep_alive);
		send(c, buf, (int)fsz, 0);
		free(buf);
		SAFE_FREE(range);
		return 0;
	}
	if (strcmp(url, "/thumbdb") == 0 || strcmp(url, "/thumbdb/") == 0) {
		char path[1024];
		snprintf(path, sizeof(path), "%s" DIR_SEP_STR "thumbdb.html", VIEWS_DIR);
		if (!is_file(path)) { send_text(c, 404, "Not Found", "thumbdb.html not found", keep_alive); SAFE_FREE(range); return 0; }
		FILE* f = fopen(path, "rb");
		if (!f) { send_text(c, 500, "Internal Server Error", "failed to open thumbdb.html", keep_alive); SAFE_FREE(range); return 0; }
		fseek(f, 0, SEEK_END); long fsz = ftell(f); fseek(f, 0, SEEK_SET);
		char* buf = malloc(fsz + 1); if (!buf) { fclose(f); send_text(c, 500, "Internal Server Error", "oom", keep_alive); SAFE_FREE(range); return 0; }
		fread(buf, 1, fsz, f); buf[fsz] = '\0'; fclose(f);
		send_header(c, 200, "OK", "text/html; charset=utf-8", (long)fsz, NULL, 0, keep_alive);
		send(c, buf, (int)fsz, 0);
		free(buf);
		SAFE_FREE(range);
		return 0;
	}
	for (size_t i = 0; i < sizeof(routes) / sizeof(routes[0]); i++) {
		if (strcmp(url, routes[i].path) == 0) {
			if (strcmp(method, "GET") == 0 && (routes[i].type == GET_SIMPLE || routes[i].type == GET_QS)) {
				if (routes[i].type == GET_SIMPLE) ((void (*)(int, bool))routes[i].handler)(c, keep_alive);
				else ((void (*)(int, char*, bool))routes[i].handler)(c, qs, keep_alive);
				SAFE_FREE(range);
				return 0;
			}
			if (strcmp(method, "POST") == 0 && routes[i].type == POST_BODY) {
				if (!body || body_len == 0) { send_text(c, 400, "Bad Request", "Empty POST body", false); SAFE_FREE(range); return 0; }
				char* body_copy = malloc(body_len + 1);
				memcpy(body_copy, body, body_len); body_copy[body_len] = '\0';
				char* qs_copy = NULL;
				if (qs) qs_copy = strdup(qs);
				g_request_qs = qs_copy;
				((void (*)(int, const char*, bool))routes[i].handler)(c, body_copy, keep_alive);
				free(body_copy);
				SAFE_FREE(g_request_qs);
				return 0;
			}
			send_text(c, 405, "Method Not Allowed", "Method not supported for this endpoint", false);
			SAFE_FREE(range);
			return 0;
		}
	}
	if (strcmp(method, "GET") != 0) {
		send_text(c, 405, "Method Not Allowed", "Only GET and POST supported", false);
		SAFE_FREE(range);
		return 0;
	}
	if (strcmp(url, "/") == 0) {
		char path[1024];
		snprintf(path, sizeof(path), "%s/index.html", VIEWS_DIR);
		if (!is_file(path)) { send_text(c, 404, "Not Found", "index.html not found", keep_alive); SAFE_FREE(range); return 0; }
		FILE* f = fopen(path, "rb");
		if (!f) { send_text(c, 500, "Internal Server Error", "failed to open index.html", keep_alive); SAFE_FREE(range); return 0; }
		fseek(f, 0, SEEK_END); long fsz = ftell(f); fseek(f, 0, SEEK_SET);
		char* buf = malloc(fsz + 1); if (!buf) { fclose(f); send_text(c, 500, "Internal Server Error", "oom", keep_alive); SAFE_FREE(range); return 0; }
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
			SAFE_FREE(range);
			return 0;
		}
		else {
			send_header(c, 200, "OK", "text/html; charset=utf-8", (long)fsz, NULL, 0, keep_alive);
			send(c, buf, (int)fsz, 0);
		}
		free(buf);
		SAFE_FREE(range);
		return 0;
	}
	for (size_t i = 0; i < sizeof(static_routes) / sizeof(static_routes[0]); i++) {
		size_t len = strlen(static_routes[i].prefix);
		if (strncmp(url, static_routes[i].prefix, len) == 0) {
			const char* base_for_route = static_routes[i].base_dir;
			const char* sub_path = url + len;
			if (strcmp(static_routes[i].base_dir, BASE_DIR) == 0) {
				if (*sub_path != '\0') {
					const char* p = sub_path;
					const char* slash = strchr(p, '/');
					if (slash) {
						char first_comp[PATH_MAX]; size_t fi = 0;
						while (p && *p && *p != '/' && fi + 1 < sizeof(first_comp)) first_comp[fi++] = *p++;
						first_comp[fi] = '\0';
						if (fi > 0) {
							char base_buf[PATH_MAX];
							snprintf(base_buf, sizeof(base_buf), "%s" DIR_SEP_STR "%s", BASE_DIR, first_comp);
							normalize_path(base_buf);
							base_for_route = strdup(base_buf);
							sub_path = slash + 1;
							while (*sub_path == '/') sub_path++;
							serve_file(c, base_for_route, sub_path, static_routes[i].allow_range ? range : NULL, keep_alive);
							free((void*)base_for_route);
							SAFE_FREE(range);
							return 0;
						}
					}
					else {
						size_t gf_count = 0; char** gfolders = get_gallery_folders(&gf_count);
						if (gf_count > 0 && gfolders[0] && gfolders[0][0]) {
							base_for_route = gfolders[0];
							serve_file(c, base_for_route, sub_path, static_routes[i].allow_range ? range : NULL, keep_alive);
							SAFE_FREE(range);
							return 0;
						}
					}
				}
			}
			serve_file(c, base_for_route, sub_path, static_routes[i].allow_range ? range : NULL, keep_alive);
			SAFE_FREE(range);
			return 0;
		}
	}
	if (strcmp(url, "/bundled") == 0) {
		const char* base_for_bundle = (BASE_DIR[0]) ? BASE_DIR : ".";
		char path[PATH_MAX];
		snprintf(path, sizeof(path), "%s" DIR_SEP_STR "public" DIR_SEP_STR "bundle" DIR_SEP_STR "libs.bundle.js", base_for_bundle);
		normalize_path(path);
		if (is_file(BUNDLED_FILE)) {
			send_file_stream(c, BUNDLED_FILE, NULL, keep_alive);
		}
		else if (is_file(path)) {
			send_file_stream(c, path, NULL, keep_alive);
		}
		else {
			char alt[PATH_MAX];
			snprintf(alt, sizeof(alt), "." DIR_SEP_STR "public" DIR_SEP_STR "bundle" DIR_SEP_STR "libs.bundle.js");
			normalize_path(alt);
			if (is_file(alt)) send_file_stream(c, alt, NULL, keep_alive);
			else send_text(c, 404, "Not Found", "Not found", keep_alive);
		}
		SAFE_FREE(range);
		return 0;
	}
	if (strncmp(url, "/bundled/", 9) == 0) {
		const char* sub = url + 9;
		const char* base_for_bundle = (BASE_DIR[0]) ? BASE_DIR : ".";
		char path[PATH_MAX];
		snprintf(path, sizeof(path), "%s" DIR_SEP_STR "public" DIR_SEP_STR "bundle" DIR_SEP_STR "%s", base_for_bundle, sub);
		normalize_path(path);
		if (is_file(path)) {
			send_file_stream(c, path, NULL, keep_alive);
		}
		else {
			char alt[PATH_MAX];
			snprintf(alt, sizeof(alt), "." DIR_SEP_STR "public" DIR_SEP_STR "bundle" DIR_SEP_STR "%s", sub);
			normalize_path(alt);
			if (is_file(alt)) send_file_stream(c, alt, NULL, keep_alive);
			else send_text(c, 404, "Not Found", "Not found", keep_alive);
		}
		SAFE_FREE(range);
		return 0;
	}


	send_text(c, 404, "Not Found", "Not found", keep_alive);
	SAFE_FREE(range);
	return 0;
}
