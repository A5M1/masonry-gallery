#include "api_handlers.h"
#include "directory.h"
#include "http.h"
#include "utils.h"
#include "logging.h"
#include "config.h"
#include "tinyjson_combined.h"
#include "common.h"

const char* IMAGE_EXTS[] = { ".jpg", ".jpeg", ".png", ".gif", ".webp", NULL };
const char* VIDEO_EXTS[] = { ".mp4", ".webm", ".ogg", NULL };

// ----------------- JSON buffer management -----------------
#define MIN_JSON_BUF 8192
static void ensure_json_buf(char** pbuf, size_t* pcap, size_t used, size_t need) {
	if (*pcap - used < need) {
		size_t newcap = (*pcap + need) * 2;
		*pbuf = realloc(*pbuf, newcap);
		*pcap = newcap;
	}
}
static char* json_comma_safe(char* ptr, size_t* remLen) {
	if (*remLen < 2) return ptr;
	*ptr++ = ','; (*remLen)--;
	return ptr;
}

// ----------------- Directory scanning -----------------
static bool has_nogallery(const char* dir) {
	char p[PATH_MAX];
	path_join(p, dir, ".nogallery");
	return is_file(p);
}
bool has_media_rec(const char* dir) {
	if (has_nogallery(dir)) return false;
	diriter it;
	if (!dir_open(&it, dir)) return false;
	const char* name;
	bool found = false;
	while ((name = dir_next(&it)) && !found) {
		if (!strcmp(name, ".") || !strcmp(name, "..")) continue;
		char full[PATH_MAX];
		path_join(full, dir, name);
		if (is_file(full)) found = has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS);
	}
	if (!found) {
		dir_close(&it);
		if (!dir_open(&it, dir)) return false;
		while ((name = dir_next(&it)) && !found) {
			if (!strcmp(name, ".") || !strcmp(name, "..")) continue;
			char full[PATH_MAX];
			path_join(full, dir, name);
			if (is_dir(full) && !has_nogallery(full)) found = has_media_rec(full);
		}
	}
	dir_close(&it);
	return found;
}

// ----------------- API handler -----------------
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

	// Collect subfolders
	char** names = NULL; size_t n = 0, alloc = 0;
	diriter it;
	if (dir_open(&it, dir)) {
		const char* name;
		while ((name = dir_next(&it))) {
			if (!strcmp(name, ".") || !strcmp(name, "..")) continue;
			char full[PATH_MAX]; path_join(full, dir, name);
			if (is_dir(full) && !has_nogallery(full) && has_media_rec(full)) {
				if (n == alloc) { alloc = alloc ? alloc * 2 : 16; names = realloc(names, alloc * sizeof(char*)); }
				names[n++] = strdup(name);
			}
		}
		dir_close(&it);
	}
	qsort(names, n, sizeof(char*), ci_cmp);

	for (size_t i = 0;i < n;i++) {
		if (i > 0) { ptr = json_comma_safe(ptr, cap); *used = ptr - *pbuf; }
		char child_full[PATH_MAX]; path_join(child_full, dir, names[i]);
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
		ptr = json_objOpen(ptr, NULL, &cap);
		used = ptr - buf;
		ptr = json_str(ptr, "name", "root", &cap);
		used = ptr - buf;
		ptr = json_str(ptr, "path", "", &cap);
		used = ptr - buf;
		ptr = json_arrOpen(ptr, "children", &cap);
		used = ptr - buf;
		for (size_t i = 0;i < count;i++) {
			if (i > 0) { ptr = json_comma_safe(ptr, &cap); used = ptr - buf; }
			build_folder_tree_json(&buf, &cap, &used, folders[i], folders[i]);
		}
		ptr = json_arrClose(buf, &cap);
		used = ptr - buf;
		ptr = json_objClose(buf, &cap);
		used = ptr - buf;
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
	if (!real_path(BASE_DIR, base_real) || !real_path(target, target_real) || !safe_under(base_real, target_real) || !is_dir(target_real)) {
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
void handle_api_media(int c, char* qs, bool keep_alive) {
	char dirparam[PATH_MAX] = { 0 };
	int page = 1;
	if (qs) {
		char* v = query_get(qs, "dir"); if (v) { strncpy(dirparam, v, PATH_MAX - 1); dirparam[PATH_MAX - 1] = 0; SAFE_FREE(v); }
		char* p = query_get(qs, "page"); if (p) { int t = atoi(p); if (t > 0) page = t; SAFE_FREE(p); }
	}

	char target[PATH_MAX]; snprintf(target, sizeof(target), "%s/%s", BASE_DIR, dirparam); normalize_path(target);
	char base_real[PATH_MAX], target_real[PATH_MAX];
	if (!real_path(BASE_DIR, base_real) || !real_path(target, target_real) || !safe_under(base_real, target_real) || !is_dir(target_real)) {
		const char* msg = "{\"error\":\"Invalid directory\"}";
		send_header(c, 400, "Bad Request", "application/json; charset=utf-8", (long)strlen(msg), NULL, 0, keep_alive);
		send(c, msg, (int)strlen(msg), 0);
		return;
	}

	char** files = NULL; size_t n = 0, alloc = 0;
	diriter it;
	if (dir_open(&it, target_real)) {
		const char* name;
		while ((name = dir_next(&it))) {
			if (!strcmp(name, ".") || !strcmp(name, "..")) continue;
			if (has_ext(name, IMAGE_EXTS) || has_ext(name, VIDEO_EXTS)) {
				if (n == alloc) { alloc = alloc ? alloc * 2 : 64; files = realloc(files, alloc * sizeof(char*)); }
				files[n++] = strdup(name);
			}
		}
		dir_close(&it);
	}
	qsort(files, n, sizeof(char*), ci_cmp);

	int total = (int)n;
	int totalPages = (total + ITEMS_PER_PAGE - 1) / ITEMS_PER_PAGE;
	if (totalPages == 0) totalPages = 1;
	if (page < 1) page = 1; if (page > totalPages) page = totalPages;
	int start = (page - 1) * ITEMS_PER_PAGE;
	int end = start + ITEMS_PER_PAGE; if (end > total) end = total;

	size_t cap = 8192; char* buf = malloc(cap); size_t len = cap; char* ptr = buf;
	ptr = json_objOpen(ptr, NULL, &len);

	ptr = json_arrOpen(ptr, "items", &len);
	for (int i = start;i < end;i++) {
		if (i > start) ptr = json_comma_safe(ptr, &len);
		char full[PATH_MAX]; path_join(full, target_real, files[i]);
		char tr[PATH_MAX]; real_path(full, tr);
		char relurl[PATH_MAX]; size_t j = 0;
		size_t base_len = strlen(BASE_DIR);
		const char* r = tr + base_len + ((tr[base_len] == DIR_SEP) ? 1 : 0);
		for (size_t k = 0;r[k] && j < PATH_MAX - 1;k++) { char ch = r[k]; if (ch == '\\') ch = '/'; relurl[j++] = ch; }
		relurl[j] = '\0';
		ptr = json_objOpen(ptr, NULL, &len);
		ptr = json_str(ptr, "path", relurl, &len);
		ptr = json_str(ptr, "filename", files[i], &len);
		ptr = json_str(ptr, "type", has_ext(files[i], IMAGE_EXTS) ? "image" : "video", &len);
		ptr = json_objClose(ptr, &len);
		free(files[i]);
	}
	free(files);

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
	for (size_t i = 0;i < count;i++) {
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

//todo: move to http.c
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
	char method[8] = { 0 }, url[PATH_MAX] = { 0 };
	if (sscanf(headers, "%7s %4095s", method, url) != 2) {
		send_text(c, 400, "Bad Request", "Malformed request", false);
		return;
	}

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
	};
	static const static_route_t static_routes[] = {
		{ "/images/", BASE_DIR, true },
		{ "/js/", JS_DIR, false },
		{ "/css/", CSS_DIR, false },
	};
	for (size_t i = 0; i < sizeof(routes) / sizeof(routes[0]); i++) {
		if (strcmp(url, routes[i].path) == 0) {
			if (strcmp(method, "GET") == 0 && (routes[i].type == GET_SIMPLE || routes[i].type == GET_QS)) {
				if (routes[i].type == GET_SIMPLE) {
					void (*h)(int, bool) = routes[i].handler;
					h(c, keep_alive);
				}
				else {
					void (*h)(int, char*, bool) = routes[i].handler;
					h(c, qs, keep_alive);
				}
				return;
			}
			if (strcmp(method, "POST") == 0 && routes[i].type == POST_BODY) {
				if (!body || body_len == 0) {
					send_text(c, 400, "Bad Request", "Empty POST body", false);
					return;
				}
				char* body_copy = malloc(body_len + 1);
				memcpy(body_copy, body, body_len);
				body_copy[body_len] = '\0';
				void (*h)(int, const char*, bool) = routes[i].handler;
				h(c, body_copy, keep_alive);
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
		if (is_file(path)) send_file_stream(c, path, NULL, keep_alive);
		else send_text(c, 404, "Not Found", "index.html not found", keep_alive);
		return;
	}
	for (size_t i = 0; i < sizeof(static_routes) / sizeof(static_routes[0]); i++) {
		size_t len = strlen(static_routes[i].prefix);
		if (strncmp(url, static_routes[i].prefix, len) == 0) {
			const char* sub_path = url + len;
			serve_file(c, static_routes[i].base_dir, sub_path,
				static_routes[i].allow_range ? range : NULL, keep_alive);
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
