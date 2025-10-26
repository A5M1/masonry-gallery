#include "directory.h"
#include "common.h"
#include "platform.h"
#include "utils.h"
bool has_ext(const char* name, const char* const exts[]) {
	const char* dot = strrchr(name, '.');
	if (!dot) return false;
	for (int i = 0; exts[i]; ++i) {
		if (ascii_stricmp(dot, exts[i]) == 0) return true;
	}
	return false;
}

void path_join(char* out, const char* a, const char* b) {
	if (!a || !b) { out[0] = '\0'; return; }
	size_t al = strlen(a);
	if (al == 0) {
		snprintf(out, PATH_MAX, "%s", b);
		return;
	}
	char last = a[al - 1];
	if (last == '/' || last == '\\') {
		snprintf(out, PATH_MAX, "%s%s", a, b);
	} else {
		snprintf(out, PATH_MAX, "%s%s%s", a, DIR_SEP_STR, b);
	}
}

bool is_file(const char* p) {
	return platform_is_file(p);
}

bool is_dir(const char* p) {
	return platform_is_dir(p);
}

int mk_dir(const char* p) {
	return platform_make_dir(p);
}

void normalize_path(char* p) {
	if (!p) return;
	for (char* ptr = p; *ptr; ++ptr) {
#ifdef _WIN32
		if (*ptr == '/') *ptr = DIR_SEP;
#else
		if (*ptr == '\\') *ptr = DIR_SEP;
#endif
	}
	char* src = p; char* dst = p; char prev = '\0';
	while (*src) {
		char c = *src++;
		if (c == DIR_SEP && prev == DIR_SEP) continue;
		*dst++ = c; prev = c;
	}
	*dst = '\0';
}

bool real_path(const char* in, char* out) {
	return platform_real_path(in, out);
}

bool safe_under(const char* base_real, const char* path_real) {
	return platform_safe_under(base_real, path_real);
}

#ifdef _WIN32
bool dir_open(diriter* it, const char* path) {
	snprintf(it->pattern, PATH_MAX, "%s\\*", path);
	it->h = FindFirstFileA(it->pattern, &it->ffd);
	it->first = (it->h != INVALID_HANDLE_VALUE);
	return it->h != INVALID_HANDLE_VALUE;
}

const char* dir_next(diriter* it) {
	if (!it->first) {
		if (!FindNextFileA(it->h, &it->ffd)) return NULL;
	}
	it->first = false;
	return it->ffd.cFileName;
}

void dir_close(diriter* it) {
	if (it->h != INVALID_HANDLE_VALUE) FindClose(it->h);
}
#else
bool dir_open(diriter* it, const char* path) {
	it->d = opendir(path);
	return it->d != NULL;
}

const char* dir_next(diriter* it) {
	it->e = readdir(it->d);
	return it->e ? it->e->d_name : NULL;
}

void dir_close(diriter* it) {
	if (it->d) closedir(it->d);
}
#endif
