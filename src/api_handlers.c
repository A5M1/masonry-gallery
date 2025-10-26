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
#include "thread_pool.h"
#include "platform.h"



const char* IMAGE_EXTS[] = { ".jpg",".jpeg",".png",".gif",".webp",NULL };
const char* VIDEO_EXTS[] = { ".mp4",".webm",".ogg",".webp",NULL };
static char* g_request_headers = NULL;
char g_request_url[PATH_MAX] = {0};
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

static int resolve_and_validate_target(const char* base_dir, const char* dirparam, char* target_real_out, size_t outlen, char* base_real_out, size_t base_outlen);

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
	char target[PATH_MAX];
	snprintf(target, sizeof(target), "%s/%s", base_dir, dirparam ? dirparam : "");
	normalize_path(target);
	char target_real[PATH_MAX]; char base_real[PATH_MAX];
	if (!resolve_and_validate_target(base_dir, dirparam, target_real, sizeof(target_real), base_real, sizeof(base_real))) { if (out_len) *out_len = 0; return NULL; }
	start_background_thumb_generation(target_real);

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
	qsort(files, n, sizeof(char*), p_strcmp);
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
		get_thumb_rel_names(full_path, files[i], small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));
		char small_fs[PATH_MAX]; char large_fs[PATH_MAX];
		make_thumb_fs_paths(full_path, files[i], small_fs, sizeof(small_fs), large_fs, sizeof(large_fs));
		int small_exists = is_file(small_fs); int large_exists = is_file(large_fs);
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
							strncpy(parent, full_path, sizeof(parent)-1);
							parent[sizeof(parent)-1] = '\0';
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
// static void* thumbnail_generation_thread(void* args) {
// 	thread_args_t* thread_args = (thread_args_t*)args;
// 	char dir_path[PATH_MAX];
// 	strncpy(dir_path, thread_args->dir_path, PATH_MAX - 1);
// 	dir_path[PATH_MAX - 1] = '\0';
// 	free(args);
// 	LOG_INFO("Background thumbnail generation starting for: %s", dir_path);
// 	run_thumb_generation(dir_path);
// 	LOG_INFO("Background thumbnail generation finished for: %s", dir_path);
// 	return NULL;
// }

void handle_api_regenerate_thumbs(int c, char* qs, bool keep_alive) {
	char dirparam[PATH_MAX] = { 0 };
	if (qs) { char* v = query_get(qs, "dir");if (v) { strncpy(dirparam, v, PATH_MAX - 1);SAFE_FREE(v); } }
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
			get_thumb_rel_names(full_path, files[i], small_rel, sizeof(small_rel), large_rel, sizeof(large_rel));
			char small_fs[PATH_MAX]; char large_fs[PATH_MAX];
			make_thumb_fs_paths(full_path, files[i], small_fs, sizeof(small_fs), large_fs, sizeof(large_fs));
			int small_exists = is_file(small_fs);
			int large_exists = is_file(large_fs);
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
			if (small_exists) appendf(&hbuf, &hcap, &hused, "<img src=\"/images/placeholder.jpg\" loading=\"lazy\" data-thumb-small=\"%s\" data-thumb-large=\"%s\" class=\"thumb-img\">", small_esc, large_esc);
			else appendf(&hbuf, &hcap, &hused, "<img src=\"/https://i.postimg.cc/kGNSRbFX/loader.webp\" class=\"thumb-img\">");
			appendf(&hbuf, &hcap, &hused, "</a></div>");
		}
		appendf(&hbuf, &hcap, &hused, "</div>");
		// unsigned int pid = platform_get_pid();
		// unsigned long tid = platform_get_tid();
		// char tmp_path[PATH_MAX];
		// snprintf(tmp_path, sizeof(tmp_path), "%s.%u.%lu.tmp", cache_path, pid, tid);
		// FILE* wf = fopen(tmp_path, "wb");
		// if (wf) {
		// 	fwrite(hbuf, 1, hused, wf);
		// 	fclose(wf);
		// 	platform_move_file(tmp_path, cache_path);
		// }
		// else {
		// 	LOG_WARN("Failed to write cache file: %s", tmp_path);
		// }
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
							strncpy(parent, full_path, sizeof(parent)-1);
							parent[sizeof(parent)-1] = '\0';
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

	char* qs = strchr(url, '?');
	if (qs) { *qs = '\0'; qs++; }
	url_decode(url);
	STRCPY(g_request_url, url);
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
