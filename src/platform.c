#include "platform.h"
#include "common.h"
#include "thread_pool.h"
#ifdef _WIN32
#else
#endif

void platform_sleep_ms(int ms) {
#ifdef _WIN32
    Sleep(ms);
#else
    struct timespec ts; ts.tv_sec = ms / 1000; ts.tv_nsec = (ms % 1000) * 1000000; nanosleep(&ts, NULL);
#endif
}

int platform_file_delete(const char* path) {
    if (!path) return -1;
#ifdef _WIN32
    return DeleteFileA(path) ? 0 : -1;
#else
    return unlink(path) == 0 ? 0 : -1;
#endif
}

int platform_make_dir(const char* path) {
    if (!path) return -1;
#ifdef _WIN32
    if (CreateDirectoryA(path, NULL)) return 0;
    DWORD e = GetLastError();
    if (e == ERROR_ALREADY_EXISTS) return 0;
    return -1;
#else
    if (mkdir(path, 0755) == 0) return 0;
    if (errno == EEXIST) return 0;
    return -1;
#endif
}

int platform_file_exists(const char* path) {
    if (!path) return 0;
#ifdef _WIN32
    DWORD attrib = GetFileAttributesA(path);
    return (attrib != INVALID_FILE_ATTRIBUTES && !(attrib & FILE_ATTRIBUTE_DIRECTORY));
#else
    struct stat st; return (stat(path, &st) == 0 && S_ISREG(st.st_mode));
#endif
}

FILE* platform_popen(const char* cmd, const char* mode) {
#ifdef _WIN32
    return _popen(cmd, mode);
#else
    return popen(cmd, mode);
#endif
}

int platform_pclose(FILE* f) {
#ifdef _WIN32
    return _pclose(f);
#else
    return pclose(f);
#endif
}

int platform_create_lockfile_exclusive(const char* lock_path) {
    if (!lock_path) return -1;
#ifdef _WIN32
    HANDLE h = CreateFileA(lock_path, GENERIC_WRITE, 0, NULL, CREATE_NEW, FILE_ATTRIBUTE_NORMAL, NULL);
    if (h == INVALID_HANDLE_VALUE) {
        DWORD err = GetLastError();
        if (err == ERROR_FILE_EXISTS) return 1;
        return -1;
    }
    CloseHandle(h);
    return 0;
#else
    int fd = open(lock_path, O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        if (errno == EEXIST) return 1;
        return -1;
    }
    close(fd);
    return 0;
#endif
}

static void watcher_trampoline(void* arg) {
    void** a = (void**)arg;
    char* dir = (char*)a[0];
    platform_watcher_callback_t cb = (platform_watcher_callback_t)a[1];
    free(arg);
#ifdef _WIN32
    HANDLE hChange = FindFirstChangeNotificationA(dir, FALSE, FILE_NOTIFY_CHANGE_FILE_NAME | FILE_NOTIFY_CHANGE_SIZE | FILE_NOTIFY_CHANGE_LAST_WRITE);
    if (hChange == INVALID_HANDLE_VALUE) { cb(dir); free(dir); return; }
    for (;;) {
        DWORD wait = WaitForSingleObject(hChange, INFINITE);
        if (wait == WAIT_OBJECT_0) {
            cb(dir);
            if (FindNextChangeNotification(hChange) == FALSE) break;
        }
    }
    FindCloseChangeNotification(hChange);
    free(dir);
#else
    int fd = inotify_init1(IN_NONBLOCK);
    if (fd < 0) { cb(dir); free(dir); return; }
    int wd = inotify_add_watch(fd, dir, IN_CREATE | IN_MOVED_TO | IN_MODIFY);
    if (wd < 0) { close(fd); cb(dir); free(dir); return; }
    char buf[4096];
    for (;;) {
        int len = read(fd, buf, sizeof(buf));
        if (len > 0) cb(dir);
        platform_sleep_ms(1000);
    }
    inotify_rm_watch(fd, wd);
    close(fd);
    free(dir);
#endif
}

int platform_start_dir_watcher(const char* dir, platform_watcher_callback_t cb) {
    if (!dir || !cb) return -1;
    char* d = strdup(dir);
    if (!d) return -1;
    void** arg = malloc(sizeof(void*) * 2);
    if (!arg) { free(d); return -1; }
    arg[0] = d; arg[1] = (void*)cb;
    if (thread_create_detached((void*(*)(void*))watcher_trampoline, arg) != 0) { free(d); free(arg); return -1; }
    return 0;
}

int platform_stat(const char* path, struct stat* st) {
    if (!path || !st) return -1;
    return stat(path, st);
}

const char* platform_devnull(void) {
#ifdef _WIN32
    return "nul";
#else
    return "/dev/null";
#endif
}

int platform_stream_file_payload(int client_socket, const char* path, long start, long len, int is_range) {
    (void)start; (void)len; (void)is_range;
#ifdef _WIN32
    HANDLE hFile = CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hFile == INVALID_HANDLE_VALUE) return -1;
    WSAPROTOCOL_INFO pi; int pi_len = sizeof(pi);
    if (getsockopt(client_socket, SOL_SOCKET, SO_PROTOCOL_INFO, (char*)&pi, &pi_len) == 0) {
        if (TransmitFile((SOCKET)client_socket, hFile, 0, 0, NULL, NULL, 0)) { CloseHandle(hFile); return 0; }
    }
    char buf[65536]; long rem = len; if (rem <= 0) rem = (long)GetFileSize(hFile, NULL);
    size_t total = 0;
    SetFilePointer(hFile, (LONG)start, NULL, FILE_BEGIN);
    while (rem > 0) {
        DWORD toread = (rem < (long)sizeof(buf) ? (DWORD)rem : (DWORD)sizeof(buf));
        DWORD rd = 0; if (!ReadFile(hFile, buf, toread, &rd, NULL) || rd == 0) break;
        int snt = send(client_socket, buf, (int)rd, 0);
        if (snt <= 0) break;
        rem -= snt; total += snt;
    }
    CloseHandle(hFile);
    return (rem > 0) ? -1 : 0;
#else
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    off_t offset = start;
    long remain = len; if (remain <= 0) {
        struct stat st; if (fstat(fd, &st) == 0) remain = (long)st.st_size - start; else remain = 0;
    }
    while (remain > 0) {
        ssize_t sent = sendfile(client_socket, fd, &offset, (size_t)remain);
        if (sent <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) continue;
            close(fd); return -1;
        }
        remain -= sent;
    }
    close(fd);
    return 0;
#endif
}