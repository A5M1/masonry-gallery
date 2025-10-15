#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "thumbdb.h"
#include <stdint.h>
#include <errno.h>
#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <unistd.h>
#endif

static void print_kv(const char* key, const char* value, void* ctx) {
    (void)ctx;
    if (value && value[0]) printf("%s\t%s\n", key, value);
    else printf("%s\t(removed)\n", key);
}

int main(int argc, char** argv) {
    if (argc < 2) {
        printf("thumbs_tool usage:\n");
        printf("  %s list\n", argv[0]);
        printf("  %s get <key>\n", argv[0]);
        printf("  %s count\n", argv[0]);
        printf("  %s add <key> <value>\n", argv[0]);
        printf("  %s delete <key>\n", argv[0]);
        printf("  optionally: use --db <dbfile> before command to operate on a specific db file\n");
        printf("  %s sweep\n", argv[0]);
        printf("  %s compact\n", argv[0]);
        return 1;
    }
    int argi = 1;
    const char* dbpath = NULL;
    while (argi < argc && strcmp(argv[argi], "--db") == 0) {
        if (argi + 1 >= argc) { fprintf(stderr, "--db requires a path\n"); return 2; }
        dbpath = argv[argi + 1];
        argi += 2;
    }
    if (argi >= argc) { fprintf(stderr, "no command provided\n"); return 2; }
    const char* cmd = argv[argi++];
    if (dbpath) {
        if (strcmp(cmd, "list") == 0) {
            FILE* f = fopen(dbpath, "r"); if (!f) { fprintf(stderr, "failed to open %s: %s\n", dbpath, strerror(errno)); return 2; }
            typedef struct { char* k; char* v; } ent_t; size_t cap = 128, n = 0; ent_t* arr = calloc(cap, sizeof(ent_t)); char line[4096];
            while (fgets(line, sizeof(line), f)) {
                char* nl = strchr(line, '\n'); if (nl) *nl = '\0'; char* tab = strchr(line, '\t'); if (!tab) continue; *tab = '\0'; char* key = line; char* val = tab + 1;
                size_t i; for (i = 0; i < n; ++i) if (strcmp(arr[i].k, key) == 0) break;
                if (i < n) { free(arr[i].v); arr[i].v = val[0] ? strdup(val) : NULL; }
                else { if (n + 1 > cap) { cap *= 2; arr = realloc(arr, cap * sizeof(ent_t)); } arr[n].k = strdup(key); arr[n].v = val[0] ? strdup(val) : NULL; n++; }
            }
            fclose(f);
            for (size_t i = 0; i < n; ++i) { if (arr[i].v && arr[i].v[0]) printf("%s\t%s\n", arr[i].k, arr[i].v); else printf("%s\t(removed)\n", arr[i].k); free(arr[i].k); free(arr[i].v); }
            free(arr);
        } else if (strcmp(cmd, "get") == 0) {
            if (argi >= argc) { fprintf(stderr, "get requires a key\n"); return 2; }
            const char* keyq = argv[argi++]; FILE* f = fopen(dbpath, "r"); if (!f) { fprintf(stderr, "failed to open %s: %s\n", dbpath, strerror(errno)); return 2; }
            char line[4096]; char* found = NULL; while (fgets(line, sizeof(line), f)) { char* nl = strchr(line, '\n'); if (nl) *nl = '\0'; char* tab = strchr(line, '\t'); if (!tab) continue; *tab = '\0'; char* key = line; char* val = tab + 1; if (strcmp(key, keyq) == 0) { free(found); found = val[0] ? strdup(val) : NULL; } }
            fclose(f); if (found) { printf("%s\n", found); free(found); } else printf("(not found)\n");
        } else if (strcmp(cmd, "count") == 0) {
            FILE* f = fopen(dbpath, "r"); if (!f) { fprintf(stderr, "failed to open %s: %s\n", dbpath, strerror(errno)); return 2; }
            typedef struct { char* k; char* v; } ent_t; size_t cap = 128, n = 0; ent_t* arr = calloc(cap, sizeof(ent_t)); char line[4096];
            while (fgets(line, sizeof(line), f)) {
                char* nl = strchr(line, '\n'); if (nl) *nl = '\0'; char* tab = strchr(line, '\t'); if (!tab) continue; *tab = '\0'; char* key = line; char* val = tab + 1; size_t i; for (i = 0; i < n; ++i) if (strcmp(arr[i].k, key) == 0) break; if (i < n) { free(arr[i].v); arr[i].v = val[0] ? strdup(val) : NULL; } else { if (n + 1 > cap) { cap *= 2; arr = realloc(arr, cap * sizeof(ent_t)); } arr[n].k = strdup(key); arr[n].v = val[0] ? strdup(val) : NULL; n++; }
            }
            fclose(f); printf("%zu\n", n); for (size_t i = 0; i < n; ++i) { free(arr[i].k); free(arr[i].v); } free(arr);
        } else if (strcmp(cmd, "add") == 0) {
            if (argi + 1 >= argc) { fprintf(stderr, "add requires key and value\n"); return 2; }
            const char* keyq = argv[argi++]; const char* valq = argv[argi++]; FILE* f = fopen(dbpath, "a"); if (!f) { fprintf(stderr, "failed to open %s for append: %s\n", dbpath, strerror(errno)); return 2; } if (fprintf(f, "%s\t%s\n", keyq, valq) < 0) { fclose(f); fprintf(stderr, "write failed\n"); return 2; } fflush(f);
#ifdef _WIN32
            { int fd = _fileno(f); if (fd >= 0) { intptr_t h = _get_osfhandle(fd); if (h != -1) FlushFileBuffers((HANDLE)h); } }
#else
            { int fd = fileno(f); if (fd >= 0) fsync(fd); }
#endif
            fclose(f);
        } else if (strcmp(cmd, "delete") == 0) {
            if (argi >= argc) { fprintf(stderr, "delete requires a key\n"); return 2; }
            const char* keyq = argv[argi++]; FILE* f = fopen(dbpath, "a"); if (!f) { fprintf(stderr, "failed to open %s for append: %s\n", dbpath, strerror(errno)); return 2; } if (fprintf(f, "%s\t\n", keyq) < 0) { fclose(f); fprintf(stderr, "write failed\n"); return 2; } fflush(f);
#ifdef _WIN32
            { int fd = _fileno(f); if (fd >= 0) { intptr_t h = _get_osfhandle(fd); if (h != -1) FlushFileBuffers((HANDLE)h); } }
#else
            { int fd = fileno(f); if (fd >= 0) fsync(fd); }
#endif
            fclose(f);
        } else if (strcmp(cmd, "sweep") == 0) {
            if (thumbdb_open() != 0) { fprintf(stderr, "failed to open thumbdb\n"); return 2; } if (thumbdb_sweep_orphans() != 0) fprintf(stderr, "sweep failed\n"); thumbdb_close();
        } else if (strcmp(cmd, "compact") == 0) {
            if (thumbdb_open() != 0) { fprintf(stderr, "failed to open thumbdb\n"); return 2; } if (thumbdb_compact() != 0) fprintf(stderr, "compact failed\n"); thumbdb_close();
        } else { fprintf(stderr, "unknown command for db file mode: %s\n", cmd); return 2; }
        return 0;
    }
    if (thumbdb_open() != 0) { fprintf(stderr, "failed to open thumbdb\n"); return 2; }
    if (strcmp(cmd, "list") == 0) {
        thumbdb_iterate(print_kv, NULL);
    } else if (strcmp(cmd, "get") == 0) {
        if (argi >= argc) { fprintf(stderr, "get requires a key\n"); thumbdb_close(); return 2; }
        char buf[PATH_MAX]; int r = thumbdb_get(argv[argi], buf, sizeof(buf)); if (r == 0) printf("%s\n", buf); else printf("(not found)\n");
    } else if (strcmp(cmd, "count") == 0) {
        size_t count = 0; void cb_count(const char* k, const char* v, void* ctx) { (void)k; (void)v; size_t* p = (size_t*)ctx; (*p)++; };
        thumbdb_iterate(cb_count, &count); printf("%zu\n", count);
    } else if (strcmp(cmd, "add") == 0) {
        if (argi + 1 >= argc) { fprintf(stderr, "add requires key and value\n"); thumbdb_close(); return 2; }
        if (thumbdb_set(argv[argi], argv[argi+1]) != 0) fprintf(stderr, "add failed\n");
    } else if (strcmp(cmd, "delete") == 0) {
        if (argi >= argc) { fprintf(stderr, "delete requires a key\n"); thumbdb_close(); return 2; }
        if (thumbdb_delete(argv[argi]) != 0) fprintf(stderr, "delete failed\n");
    } else if (strcmp(cmd, "sweep") == 0) {
        if (thumbdb_sweep_orphans() != 0) fprintf(stderr, "sweep failed\n");
    } else if (strcmp(cmd, "compact") == 0) {
        if (thumbdb_compact() != 0) fprintf(stderr, "compact failed\n");
    } else {
        fprintf(stderr, "unknown command: %s\n", cmd);
        thumbdb_close();
        return 2;
    }
    thumbdb_close();
    return 0;
}
