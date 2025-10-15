#include "directory.h"
#include "common.h"
#ifndef _WIN32
#include <strings.h>
#include <stdlib.h>
#endif

bool has_ext(const char* name, const char* const exts[]) {
	const char* dot = strrchr(name, '.');
	if (!dot) return false;
	for (int i = 0; exts[i]; ++i) {
#ifdef _WIN32
		if (_stricmp(dot, exts[i]) == 0) return true;
#else
		if (strcasecmp(dot, exts[i]) == 0) return true;
#endif
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
#ifdef _WIN32
	DWORD attr = GetFileAttributesA(p);
	return (attr != INVALID_FILE_ATTRIBUTES) && !(attr & FILE_ATTRIBUTE_DIRECTORY);
#else
	struct stat st;
	return stat(p, &st) == 0 && S_ISREG(st.st_mode);
#endif
}

bool is_dir(const char* p) {
#ifdef _WIN32
	DWORD attr = GetFileAttributesA(p);
	return (attr != INVALID_FILE_ATTRIBUTES) && (attr & FILE_ATTRIBUTE_DIRECTORY);
#else
	struct stat st;
	return stat(p, &st) == 0 && S_ISDIR(st.st_mode);
#endif
}

int mk_dir(const char* p) {
#ifdef _WIN32
	return _mkdir(p);
#else
	return mkdir(p, 0755);
#endif
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
#ifdef _WIN32
	if (!_fullpath(out, in, PATH_MAX)) return false;
#else
	if (!realpath(in, out)) return false;
#endif
	normalize_path(out);
	return true;
}

bool safe_under(const char* base_real, const char* path_real) {
	size_t n = strlen(base_real);
#ifdef _WIN32
	return _strnicmp(base_real, path_real, n) == 0 && (path_real[n] == DIR_SEP || path_real[n] == '\0');
#else
	return strncmp(base_real, path_real, n) == 0 && (path_real[n] == DIR_SEP || path_real[n] == '\0');
#endif
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
