#include "platform.h"
#include "common.h"
#include "thread_pool.h"
#include "logging.h"
#include "directory.h"
#include "utils.h"

static thread_mutex_t g_streams_mutex;
typedef struct { char path[PATH_MAX]; int sock; } active_stream_t;
static active_stream_t g_active_streams[128];
static int g_active_streams_count = 0;

static void init_streams(void) {
    static int inited = 0;
    if (inited) return;
    inited = 1;
    thread_mutex_init(&g_streams_mutex);
    for (int i = 0; i < (int)(sizeof(g_active_streams)/sizeof(g_active_streams[0])); ++i) g_active_streams[i].sock = -1;
}

static void register_stream(const char* path, int sock) {
    if (!path) return;
    init_streams();
    thread_mutex_lock(&g_streams_mutex);
    for (int i = 0; i < (int)(sizeof(g_active_streams)/sizeof(g_active_streams[0])); ++i) {
        if (g_active_streams[i].sock == -1) {
            strncpy(g_active_streams[i].path, path, PATH_MAX - 1);
            g_active_streams[i].path[PATH_MAX - 1] = '\0';
            g_active_streams[i].sock = sock;
            thread_mutex_unlock(&g_streams_mutex);
            return;
        }
    }
    thread_mutex_unlock(&g_streams_mutex);
}

static void unregister_stream_by_sock(int sock) {
    init_streams();
    thread_mutex_lock(&g_streams_mutex);
    for (int i = 0; i < (int)(sizeof(g_active_streams)/sizeof(g_active_streams[0])); ++i) {
        if (g_active_streams[i].sock == sock) {
            g_active_streams[i].sock = -1;
            g_active_streams[i].path[0] = '\0';
            break;
        }
    }
    thread_mutex_unlock(&g_streams_mutex);
}

int platform_close_streams_for_path(const char* path) {
    if (!path) return 0;
    init_streams();
    char thumbs_root[PATH_MAX];
    get_thumbs_root(thumbs_root, sizeof(thumbs_root));
    normalize_path((char*)path);
    normalize_path(thumbs_root);
    if (strncmp(path, thumbs_root, strlen(thumbs_root)) == 0) {
        LOG_WARN("platform_close_streams_for_path: refusing to close streams for thumbs path: %s", path);
        return 0;
    }
    int closed = 0;
    thread_mutex_lock(&g_streams_mutex);
    for (int i = 0; i < (int)(sizeof(g_active_streams)/sizeof(g_active_streams[0])); ++i) {
        if (g_active_streams[i].sock != -1 && g_active_streams[i].path[0] && strcmp(g_active_streams[i].path, path) == 0) {
            int s = g_active_streams[i].sock;
            g_active_streams[i].sock = -1;
            g_active_streams[i].path[0] = '\0';
            thread_mutex_unlock(&g_streams_mutex);
            SOCKET_CLOSE(s);
            closed++;
            thread_mutex_lock(&g_streams_mutex);
        }
    }
    thread_mutex_unlock(&g_streams_mutex);
    return closed;
}

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
    int attempts = 0;
    const int max_attempts = 20;
    while (attempts < max_attempts) {
        if (DeleteFileA(path)) return 0;
        DWORD err = GetLastError();
        if (err == ERROR_FILE_NOT_FOUND) return 0;
        LOG_WARN("platform_file_delete: attempt=%d DeleteFileA failed for %s err=%lu", attempts + 1, path, (unsigned long)err);
        if (err == ERROR_ACCESS_DENIED || err == ERROR_SHARING_VIOLATION) {
            SetFileAttributesA(path, FILE_ATTRIBUTE_NORMAL);
            if (DeleteFileA(path)) return 0;
            DWORD err2 = GetLastError();
            LOG_WARN("platform_file_delete: attempt=%d DeleteFileA retry after SetFileAttributesA failed for %s err=%lu", attempts + 1, path, (unsigned long)err2);
        }
        if (err == ERROR_SHARING_VIOLATION) {
            int backoff = 50 * (attempts + 1);
            if (backoff < 100) backoff = 100;
            if (backoff > 2000) backoff = 2000;
            platform_sleep_ms(backoff);
        }
        else {
            platform_sleep_ms(50);
        }
        attempts++;
    }
    LOG_ERROR("platform_file_delete: giving up deleting %s after %d attempts", path, max_attempts);
    return -1;
#else
    int attempts = 0;
    while (attempts < 5) {
        if (unlink(path) == 0) return 0;
        if (errno == ENOENT) return 0;
        if (errno == EACCES || errno == EPERM) {
            LOG_WARN("platform_file_delete: unlink permission error for %s errno=%d", path, errno);
            chmod(path, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
            if (unlink(path) == 0) return 0;
            LOG_WARN("platform_file_delete: unlink after chmod failed for %s errno=%d", path, errno);
        }
        if (errno == EINTR) { attempts++; continue; }
        platform_sleep_ms(50);
        attempts++;
    }
    LOG_ERROR("platform_file_delete: giving up deleting %s after attempts errno=%d", path, errno);
    return -1;
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
#if 0
#include <errno.h>
#include <string.h>
#ifdef _WIN32
#include <io.h>
#include <fcntl.h>
#endif
#endif

int platform_copy_file(const char* src,const char* dst){
#ifdef _WIN32
    if(!CopyFileA(src,dst,FALSE))return -1;
    return 0;
#else
    int in=open(src,O_RDONLY);
    if(in<0)return -1;
    int out=open(dst,O_WRONLY|O_CREAT|O_TRUNC,0666);
    if(out<0){close(in);return -1;}
    char buf[8192];
    ssize_t n;
    while((n=read(in,buf,sizeof(buf)))>0){
        if(write(out,buf,n)!=n){close(in);close(out);return -1;}
    }
    close(in);close(out);
    return n<0?-1:0;
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
#if 1
    if (cmd) { platform_record_command(cmd); LOG_DEBUG("platform_popen: %s", cmd); }
#endif
#ifdef _WIN32
    FILE* f = _popen(cmd, mode);
    if (!f) {
        DWORD err = GetLastError();
        LOG_WARN("platform_popen: _popen failed for '%s' err=%lu", cmd ? cmd : "(null)", err);
    }
    return f;
#else
    FILE* f = popen(cmd, mode);
    if (!f) LOG_WARN("platform_popen: popen failed for '%s' err=%s", cmd ? cmd : "(null)", strerror(errno));
    return f;
#endif
}

int platform_pclose(FILE* f) {
#ifdef _WIN32
    return _pclose(f);
#else
    return pclose(f);
#endif
}

typedef struct {FILE* f; HANDLE proc; int is_ffprobe;} PHandleMap;
static PHandleMap g_ph[32];
static int g_phc=0;
static atomic_int ffprobe_active = ATOMIC_VAR_INIT(0);
#define MAX_FFPROBE 4

FILE* platform_popen_direct(const char* cmd,const char* mode){
    if (cmd) { platform_record_command(cmd); LOG_DEBUG("platform_popen_direct: %s", cmd); }
#ifdef _WIN32
    if(!cmd||!mode)return NULL;
    SECURITY_ATTRIBUTES sa;ZeroMemory(&sa,sizeof(sa));sa.nLength=sizeof(sa);sa.bInheritHandle=TRUE;
    HANDLE hR=NULL,hW=NULL;
    if(!CreatePipe(&hR,&hW,&sa,0)){
        DWORD err = GetLastError();
        LOG_ERROR("platform_popen_direct: CreatePipe failed err=%lu", err);
        return NULL;
    }
    if(!SetHandleInformation(hR,HANDLE_FLAG_INHERIT,0)){CloseHandle(hR);CloseHandle(hW);return NULL;}
    STARTUPINFOA si;PROCESS_INFORMATION pi;
    ZeroMemory(&si,sizeof(si));ZeroMemory(&pi,sizeof(pi));
    si.cb=sizeof(si);si.dwFlags=STARTF_USESTDHANDLES;
    si.hStdOutput=hW;si.hStdError=hW;si.hStdInput=GetStdHandle(STD_INPUT_HANDLE);
    char* c=_strdup(cmd);
    BOOL ok=CreateProcessA(NULL,c,NULL,NULL,TRUE,CREATE_NO_WINDOW,NULL,NULL,&si,&pi);
    free(c);
    CloseHandle(hW);
    if(!ok){
        DWORD err = GetLastError();
        LOG_ERROR("platform_popen_direct: CreateProcessA failed for '%s' err=%lu", cmd ? cmd : "(null)", err);
        CloseHandle(hR);
        return NULL;
    }
    int fd=_open_osfhandle((intptr_t)hR,_O_RDONLY|_O_BINARY);
    if(fd==-1){
        DWORD err = GetLastError();
        LOG_ERROR("platform_popen_direct: _open_osfhandle failed err=%lu", err);
        CloseHandle(hR);CloseHandle(pi.hProcess);CloseHandle(pi.hThread);return NULL;}
    FILE* f=_fdopen(fd,mode);
    if(!f){
        LOG_ERROR("platform_popen_direct: _fdopen failed");
        _close(fd);CloseHandle(pi.hProcess);CloseHandle(pi.hThread);return NULL;}
    CloseHandle(pi.hThread);
    {
        int is_ff = (strstr(cmd, "ffprobe") != NULL) ? 1 : 0;
        while (g_phc >= 32) platform_sleep_ms(10);
        if (g_phc < 32) {
            g_ph[g_phc].f = f;
            g_ph[g_phc].proc = pi.hProcess;
            g_ph[g_phc].is_ffprobe = is_ff ? 1 : 0;
            g_phc++;
            if (is_ff) atomic_fetch_add(&ffprobe_active, 1);
        } else {
            CloseHandle(pi.hProcess); CloseHandle(pi.hThread); CloseHandle(hR);
            fclose(f); return NULL;
        }
    }
    setvbuf(f,NULL,_IONBF,0);
    return f;
#else
    int is_ff = (cmd && strstr(cmd, "ffprobe") != NULL) ? 1 : 0;
    if (is_ff) {
        while (atomic_load(&ffprobe_active) >= MAX_FFPROBE) platform_sleep_ms(50);
        atomic_fetch_add(&ffprobe_active, 1);
    }
    FILE* f = popen(cmd,mode);
    if (!f) {
        LOG_WARN("platform_popen_direct: popen failed for '%s' err=%s", cmd ? cmd : "(null)", strerror(errno));
        if (is_ff) atomic_fetch_sub(&ffprobe_active, 1);
    }
    return f;
#endif
}

int platform_pclose_direct(FILE* f){
#ifdef _WIN32
    if(!f)return-1;
    HANDLE hProc=NULL;
    for(int i=0;i<g_phc;i++){
        if(g_ph[i].f==f){
            hProc=g_ph[i].proc;
            int was_ff = g_ph[i].is_ffprobe;
            g_ph[i]=g_ph[--g_phc];
            if (was_ff) atomic_fetch_sub(&ffprobe_active, 1);
            break;
        }
    }
    fclose(f);
    if(hProc){
        WaitForSingleObject(hProc,INFINITE);
        DWORD code=0;GetExitCodeProcess(hProc,&code);
        CloseHandle(hProc);
        return (int)code;
    }
    return 0;
#else
    return pclose(f);
#endif
}
int platform_run_command(const char* cmd, int timeout_seconds) {
    if (!cmd) return -1;
    platform_record_command(cmd);
    LOG_DEBUG("platform_run_command: %s", cmd);
#ifdef _WIN32
    STARTUPINFOA si; PROCESS_INFORMATION pi; ZeroMemory(&si, sizeof(si)); si.cb = sizeof(si); si.dwFlags = STARTF_USESHOWWINDOW; si.wShowWindow = SW_HIDE; ZeroMemory(&pi, sizeof(pi));
    char cmdline[4096];
    if (snprintf(cmdline, sizeof(cmdline), "cmd.exe /C %s", cmd) >= (int)sizeof(cmdline)) return -1;
    BOOL ok = CreateProcessA(NULL, cmdline, NULL, NULL, FALSE, CREATE_NO_WINDOW, NULL, NULL, &si, &pi);
    if (!ok) {
        DWORD err = GetLastError();
        LOG_ERROR("platform_run_command: CreateProcessA failed for '%s' err=%lu", cmd ? cmd : "(null)", err);
        return -1;
    }
    DWORD wait = WaitForSingleObject(pi.hProcess, (timeout_seconds > 0) ? (DWORD)timeout_seconds * 1000 : INFINITE);
    DWORD exit_code = -1;
    if (wait == WAIT_OBJECT_0) {
        GetExitCodeProcess(pi.hProcess, &exit_code);
    } else {
        TerminateProcess(pi.hProcess, 1);
        exit_code = -1;
    }
    CloseHandle(pi.hProcess); CloseHandle(pi.hThread);
    return (int)exit_code;
#else
    pid_t pid = fork();
    if (pid < 0) {
        LOG_ERROR("platform_run_command: fork failed: %s", strerror(errno));
        return -1;
    }
    if (pid == 0) {
        execl("/bin/sh", "sh", "-c", cmd, (char*)NULL);
        _exit(127);
    }
    int status = -1;
    if (timeout_seconds > 0) {
        int elapsed = 0; int rc;
        while (elapsed < timeout_seconds) {
            rc = waitpid(pid, &status, WNOHANG);
            if (rc == pid) break;
            sleep(1);
            elapsed++;
        }
        if (elapsed >= timeout_seconds) {
            kill(pid, SIGKILL);
            waitpid(pid, &status, 0);
        } else if (rc == 0) {
            waitpid(pid, &status, 0);
        }
    } else {
        waitpid(pid, &status, 0);
    }
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    return -1;
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
    DWORD pid = GetCurrentProcessId();
    char buf[64]; int bl = snprintf(buf, sizeof(buf), "%u\n", (unsigned int)pid);
    DWORD written = 0; WriteFile(h, buf, (DWORD)bl, &written, NULL);
    CloseHandle(h);
    return 0;
#else
    int fd = open(lock_path, O_CREAT | O_EXCL | O_WRONLY, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        if (errno == EEXIST) return 1;
        return -1;
    }
    pid_t pid = getpid();
    char buf[64]; int bl = snprintf(buf, sizeof(buf), "%d\n", (int)pid);
    ssize_t w = write(fd, buf, (size_t)bl);
    (void)w;
    close(fd);
    return 0;
#endif
}

int platform_pid_is_running(int pid) {
#ifdef _WIN32
    if (pid <= 0) return 0;
    HANDLE h = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, (DWORD)pid);
    if (!h) return 0;
    DWORD code = 0;
    BOOL ok = GetExitCodeProcess(h, &code);
    CloseHandle(h);
    if (!ok) return 0;
    return code == STILL_ACTIVE;
#else
    if (pid <= 0) return 0;
    if (kill((pid_t)pid, 0) == 0) return 1;
    return errno != ESRCH;
#endif
}

static void watcher_trampoline(void* arg) {
    void** a = (void**)arg;
    char* dir = (char*)a[0];
    platform_watcher_callback_t cb = (platform_watcher_callback_t)a[1];
    free(arg);
#ifdef _WIN32
    HANDLE hDir = CreateFileA(dir, FILE_LIST_DIRECTORY, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, NULL);
    if (hDir == INVALID_HANDLE_VALUE) { LOG_ERROR("CreateFileA failed for watcher on %s", dir); cb(dir); free(dir); return; }
    for (;;) {
        BYTE buffer[8192]; DWORD bytesReturned = 0;
        BOOL ok = ReadDirectoryChangesW(hDir, buffer, sizeof(buffer), FALSE, FILE_NOTIFY_CHANGE_FILE_NAME | FILE_NOTIFY_CHANGE_SIZE | FILE_NOTIFY_CHANGE_LAST_WRITE | FILE_NOTIFY_CHANGE_DIR_NAME | FILE_NOTIFY_CHANGE_ATTRIBUTES | FILE_NOTIFY_CHANGE_SECURITY, &bytesReturned, NULL, NULL);
        if (!ok) {
            DWORD err = GetLastError();
            LOG_WARN("ReadDirectoryChangesW failed for %s with %lu", dir, err);
            break;
        }
        if (bytesReturned == 0) { platform_sleep_ms(50); continue; }
        FILE_NOTIFY_INFORMATION* fni = (FILE_NOTIFY_INFORMATION*)buffer;
        for (;;) {
            int action = (int)fni->Action;
            int fname_len = (int)(fni->FileNameLength / sizeof(WCHAR));
            char fname_utf8[1024]; fname_utf8[0] = '\0';
            if (fname_len > 0) {
                int conv = WideCharToMultiByte(CP_UTF8, 0, fni->FileName, fname_len, fname_utf8, (int)sizeof(fname_utf8) - 1, NULL, NULL);
                if (conv > 0) fname_utf8[conv] = '\0'; else fname_utf8[0] = '\0';
            }
            LOG_DEBUG("Watcher event action=%d name=%s dir=%s", action, fname_utf8[0] ? fname_utf8 : "(nil)", dir);
            if (fname_utf8[0] != '\0' && strcmp(fname_utf8, "thumbs") == 0) {
                LOG_DEBUG("Ignoring watcher event for thumbs directory: %s", fname_utf8);
            } else if (action == FILE_ACTION_ADDED || action == FILE_ACTION_RENAMED_NEW_NAME || action == FILE_ACTION_MODIFIED || action == FILE_ACTION_REMOVED) {
                cb(dir);
            }
            if (fni->NextEntryOffset == 0) break;
            fni = (FILE_NOTIFY_INFORMATION*)((char*)fni + fni->NextEntryOffset);
        }
    }
    CloseHandle(hDir);
    free(dir);
#else
#if defined(__linux__)
    int fd = inotify_init1(IN_NONBLOCK);
    if (fd < 0) { cb(dir); free(dir); return; }
    int wd = inotify_add_watch(fd, dir, IN_CREATE | IN_MOVED_TO | IN_MODIFY | IN_DELETE | IN_MOVED_FROM | IN_CLOSE_WRITE | IN_ATTRIB);
    if (wd < 0) { close(fd); cb(dir); free(dir); return; }
    char buf[4096];
    for (;;) {
        int len = read(fd, buf, sizeof(buf));
        if (len > 0) {
            int off = 0;
            while (off < len) {
                struct inotify_event* ev = (struct inotify_event*)(buf + off);
                const char* name = (ev->len > 0) ? ev->name : NULL;
                LOG_DEBUG("inotify event mask=%u name=%s dir=%s", ev->mask, name ? name : "(nil)", dir);
                if ((ev->mask & (IN_CREATE | IN_MOVED_TO | IN_MODIFY | IN_DELETE | IN_MOVED_FROM | IN_CLOSE_WRITE | IN_ATTRIB))) {
                    if (name && strcmp(name, "thumbs") == 0) {
                        LOG_DEBUG("Ignoring inotify event for thumbs directory: %s", name);
                    } else {
                        cb(dir);
                        break;
                    }
                }
                off += sizeof(struct inotify_event) + ev->len;
            }
        }
        platform_sleep_ms(1000);
    }
    inotify_rm_watch(fd, wd);
    close(fd);
    free(dir);
#else
    for (;;) {
        platform_sleep_ms(1000);
        cb(dir);
    }
#endif
#endif
}

int platform_start_dir_watcher(const char* dir, platform_watcher_callback_t cb) {
    if (!dir || !cb) return -1;
    char* d = strdup(dir);
    if (!d) return -1;
    void** arg = malloc(sizeof(void*) * 2);
    if (!arg) {
        LOG_ERROR("Failed to allocate argument array for directory watcher");
        free(d);
        return -1;
    }
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
    register_stream(path, client_socket);
    WSAPROTOCOL_INFO pi; int pi_len = sizeof(pi);
    DWORD file_size = GetFileSize(hFile, NULL);
    long rem = len;
    if (rem <= 0) {
        if (file_size > (DWORD)start) rem = (long)(file_size - (DWORD)start);
        else rem = 0;
    }
    if ((long)start >= (long)file_size) { CloseHandle(hFile); return -1; }
    SetFilePointer(hFile, (LONG)start, NULL, FILE_BEGIN);
    WSAPROTOCOL_INFO pi2; int pi2_len = sizeof(pi2);
    if (getsockopt(client_socket, SOL_SOCKET, SO_PROTOCOL_INFO, (char*)&pi2, &pi2_len) == 0) {
        DWORD toWrite = (rem > 0 && rem <= (long)0xFFFFFFFF) ? (DWORD)rem : 0;
        if (toWrite > 0) {
            if (TransmitFile((SOCKET)client_socket, hFile, toWrite, 0, NULL, NULL, 0)) { CloseHandle(hFile); unregister_stream_by_sock(client_socket); return 0; }
        } else {
            if (TransmitFile((SOCKET)client_socket, hFile, 0, 0, NULL, NULL, 0)) { CloseHandle(hFile); unregister_stream_by_sock(client_socket); return 0; }
        }
    }
    char buf[65536];
    size_t total = 0;
    while (rem > 0) {
        DWORD toread = (rem < (long)sizeof(buf) ? (DWORD)rem : (DWORD)sizeof(buf));
        DWORD rd = 0; if (!ReadFile(hFile, buf, toread, &rd, NULL) || rd == 0) break;
        int snt = send(client_socket, buf, (int)rd, 0);
        if (snt <= 0) break;
        rem -= snt; total += snt;
    }
    CloseHandle(hFile);
    unregister_stream_by_sock(client_socket);
    return (rem > 0) ? -1 : 0;
#else
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    register_stream(path, client_socket);
    off_t offset = start;
    long remain = len; if (remain <= 0) {
        struct stat st; if (fstat(fd, &st) == 0) remain = (long)st.st_size - start; else remain = 0;
    }
    char buf[65536];
    while (remain > 0) {
        size_t toread = (remain < (long)sizeof(buf)) ? (size_t)remain : sizeof(buf);
        if (lseek(fd, offset, SEEK_SET) == (off_t)-1) { close(fd); return -1; }
        ssize_t rd = read(fd, buf, toread);
        if (rd <= 0) { if (errno == EINTR) continue; close(fd); return -1; }
        ssize_t sent_total = 0;
        while (sent_total < rd) {
            ssize_t snt = send(client_socket, buf + sent_total, (int)(rd - sent_total), 0);
            if (snt <= 0) {
                if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
                close(fd); return -1;
            }
            sent_total += snt;
            offset += snt;
            remain -= snt;
        }
    }
    close(fd);
    unregister_stream_by_sock(client_socket);
    return 0;
#endif
}

int platform_run_command_redirect(const char* cmd, const char* out_err_path, int timeout_seconds) {
    if (!cmd) return -1;
    platform_record_command(cmd);
    LOG_DEBUG("platform_run_command_redirect: %s -> %s", cmd, out_err_path ? out_err_path : "(null)");
#ifdef _WIN32
    STARTUPINFOA si; PROCESS_INFORMATION pi; ZeroMemory(&si, sizeof(si)); si.cb = sizeof(si); si.dwFlags = STARTF_USESHOWWINDOW | STARTF_USESTDHANDLES; si.wShowWindow = SW_HIDE; ZeroMemory(&pi, sizeof(pi));
    HANDLE hOut = CreateFileA(out_err_path, GENERIC_WRITE, FILE_SHARE_READ, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hOut == INVALID_HANDLE_VALUE) { DWORD err = GetLastError(); LOG_ERROR("platform_run_command_redirect: CreateFileA failed for '%s' err=%lu", out_err_path ? out_err_path : "(null)", err); return -1; }
    si.hStdOutput = hOut; si.hStdError = hOut; si.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
    char cmdline[4096]; if (snprintf(cmdline, sizeof(cmdline), "%s", cmd) >= (int)sizeof(cmdline)) { CloseHandle(hOut); return -1; }
    BOOL ok = CreateProcessA(NULL, cmdline, NULL, NULL, TRUE, CREATE_NO_WINDOW, NULL, NULL, &si, &pi);
    if (!ok) { DWORD err = GetLastError(); LOG_ERROR("platform_run_command_redirect: CreateProcessA failed for '%s' err=%lu", cmd ? cmd : "(null)", err); CloseHandle(hOut); return -1; }
    DWORD wait = WaitForSingleObject(pi.hProcess, (timeout_seconds > 0) ? (DWORD)timeout_seconds * 1000 : INFINITE);
    DWORD exit_code = -1;
    if (wait == WAIT_OBJECT_0) { GetExitCodeProcess(pi.hProcess, &exit_code); } else { TerminateProcess(pi.hProcess, 1); exit_code = -1; }
    CloseHandle(pi.hProcess); CloseHandle(pi.hThread); CloseHandle(hOut);
    LOG_DEBUG("platform_run_command_redirect: exit_code=%d", (int)exit_code);
    return (int)exit_code;
#else
    pid_t pid = fork();
    if (pid < 0) { LOG_ERROR("platform_run_command_redirect: fork failed: %s", strerror(errno)); return -1; }
    if (pid == 0) {
        int fd = open(out_err_path, O_CREAT | O_TRUNC | O_WRONLY, 0666);
        if (fd >= 0) {
            dup2(fd, STDOUT_FILENO); dup2(fd, STDERR_FILENO); close(fd);
        } else {
            LOG_WARN("platform_run_command_redirect: open failed for '%s' err=%s", out_err_path ? out_err_path : "(null)", strerror(errno));
        }
        execl("/bin/sh", "sh", "-c", cmd, (char*)NULL);
        _exit(127);
    }
    int status = -1;
    if (timeout_seconds > 0) {
        int elapsed = 0; int rc;
        while (elapsed < timeout_seconds) {
            rc = waitpid(pid, &status, WNOHANG);
            if (rc == pid) break;
            sleep(1);
            elapsed++;
        }
        if (elapsed >= timeout_seconds) { kill(pid, SIGKILL); waitpid(pid, &status, 0); }
        else if (rc == 0) { waitpid(pid, &status, 0); }
    } else {
        waitpid(pid, &status, 0);
    }
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    LOG_DEBUG("platform_run_command_redirect: child status not exited WIFEXITED(status)=%d", WIFEXITED(status));
    return -1;
#endif
}

#define PLATFORM_RECENT_CMDS_CAP 16
static platform_recent_cmd_t g_recent_cmds[PLATFORM_RECENT_CMDS_CAP];
static size_t g_recent_cmds_head = 0;
static size_t g_recent_cmds_count = 0;

static long long now_ms(void){
#if defined(_WIN32) || defined(_WIN64)
    FILETIME ft; GetSystemTimeAsFileTime(&ft);
    unsigned long long t=((unsigned long long)ft.dwHighDateTime<<32)|ft.dwLowDateTime;
    return (t-116444736000000000ULL)/10000ULL;
#else
    struct timespec ts; clock_gettime(CLOCK_REALTIME,&ts);
    return (long long)ts.tv_sec*1000LL+ts.tv_nsec/1000000LL;
#endif
}

void platform_record_command(const char* cmd) {
    if (!cmd) return;
    size_t idx = (g_recent_cmds_head + g_recent_cmds_count) % PLATFORM_RECENT_CMDS_CAP;
    if (g_recent_cmds_count == PLATFORM_RECENT_CMDS_CAP) {
        idx = g_recent_cmds_head;
        g_recent_cmds_head = (g_recent_cmds_head + 1) % PLATFORM_RECENT_CMDS_CAP;
    } else {
        g_recent_cmds_count++;
    }
    platform_recent_cmd_t* rc = &g_recent_cmds[idx];
    rc->ts_ms = now_ms();
#ifdef _WIN32
    rc->thread_id = (int)GetCurrentThreadId();
#else
    rc->thread_id = (int)(uintptr_t)pthread_self();
#endif
    strncpy(rc->cmd, cmd, sizeof(rc->cmd) - 1);
    rc->cmd[sizeof(rc->cmd) - 1] = '\0';
}

const platform_recent_cmd_t* platform_get_recent_commands(size_t* out_count) {
    if (out_count) *out_count = g_recent_cmds_count;
    return g_recent_cmds_count ? g_recent_cmds : NULL;
}

int platform_fsync(int fd) {
#ifdef _WIN32
    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (h != INVALID_HANDLE_VALUE) {
        return FlushFileBuffers(h) ? 0 : -1;
    }
    return -1;
#else
    return fsync(fd);
#endif
}

void platform_escape_path_for_cmd(const char* src, char* dst, size_t dstlen) {
    if (!src || !dst || dstlen == 0) return;
    for (size_t i = 0; src[i]; ++i) {
        unsigned char c = (unsigned char)src[i];
        if (!(isalnum(c) || c == '/' || c == '\\' ||
            c == '_' || c == '-' || c == '.' || c == ':' || c == ' ')) {
            dst[0] = '\0';
            return;
        }
    }

    size_t di = 0;

#ifdef _WIN32
    if (di < dstlen - 1) dst[di++] = '"';
    for (size_t i = 0; src[i] && di + 2 < dstlen; ++i) {
        char c = src[i];
        if (c == '"') continue;         
        if (c == '%') {                
            if (di + 1 < dstlen) dst[di++] = '%';
            else break;
}
        dst[di++] = c;
    }
    if (di < dstlen - 1) dst[di++] = '"';
#else
    if (di < dstlen - 1) dst[di++] = '"';
    for (size_t i = 0; src[i] && di + 2 < dstlen; ++i) {
        char c = src[i];
        if (c == '\\' || c == '"' || c == '$' || c == '`') {
            if (di + 1 < dstlen) dst[di++] = '\\';
            else break;
        }
        dst[di++] = c;
    }
    if (di < dstlen - 1) dst[di++] = '"';
#endif
    dst[di] = '\0';
}

int platform_maximize_window(void) {
#ifdef _WIN32
    HWND h = GetConsoleWindow();
    if (h) {
        if (IsIconic(h)) ShowWindow(h, SW_RESTORE);
        ShowWindow(h, SW_MAXIMIZE);
        return 0;
    }
    return -1;
#else
#if defined(__APPLE__)
    const char* cmd = "osascript -e 'tell application \"System Events\" to tell (first process whose frontmost is true) to keystroke \"m\" using {command down, control down}'";
    return platform_run_command(cmd, 2) == 0 ? 0 : -1;
#else
    if (platform_run_command("wmctrl -r :ACTIVE: -b add,maximized_vert,maximized_horz", 2) == 0) return 0;
    if (platform_run_command("xdotool getactivewindow windowsize 100% 100%", 2) == 0) return 0;
    if (platform_run_command("xdotool getactivewindow windowactivate --sync && xdotool getactivewindow windowstate --sync maximize", 2) == 0) return 0;
    return -1;
#endif
#endif
}

void platform_enable_console_colors(void) {
#ifdef _WIN32
    HANDLE hOut = GetStdHandle(STD_OUTPUT_HANDLE);
    if (hOut != INVALID_HANDLE_VALUE) {
        DWORD dwMode = 0;
        if (GetConsoleMode(hOut, &dwMode)) {
            dwMode |= ENABLE_VIRTUAL_TERMINAL_PROCESSING;
            SetConsoleMode(hOut, dwMode);
        }
    }
    HANDLE hErr = GetStdHandle(STD_ERROR_HANDLE);
    if (hErr != INVALID_HANDLE_VALUE) {
        DWORD dwModeErr = 0;
        if (GetConsoleMode(hErr, &dwModeErr)) {
            dwModeErr |= ENABLE_VIRTUAL_TERMINAL_PROCESSING;
            SetConsoleMode(hErr, dwModeErr);
        }
    }
#endif
}

int platform_should_use_colors(void) {
#ifdef _WIN32
    HANDLE hErr = GetStdHandle(STD_ERROR_HANDLE);
    DWORD dw = 0;
    if (hErr != INVALID_HANDLE_VALUE && GetConsoleMode(hErr, &dw)) return 1;
    return 0;
#else
    return isatty(fileno(stderr));
#endif
}

int platform_move_file(const char* src, const char* dst) {
#ifdef _WIN32
    return MoveFileExA(src, dst, MOVEFILE_REPLACE_EXISTING) ? 0 : -1;
#else
    return rename(src, dst);
#endif
}

int platform_localtime(time_t t, struct tm* tm_buf) {
#ifdef _WIN32
    return localtime_s(tm_buf, &t) == 0 ? 0 : -1;
#else
    return localtime_r(&t, tm_buf) != NULL ? 0 : -1;
#endif
}

unsigned int platform_get_pid(void) {
#ifdef _WIN32
    return (unsigned int)GetCurrentProcessId();
#else
    return (unsigned int)getpid();
#endif
}

unsigned long platform_get_tid(void) {
#ifdef _WIN32
    return (unsigned long)GetCurrentThreadId();
#else
#if defined(__linux__)
    return (unsigned long)syscall(SYS_gettid);
#else
    return (unsigned long)pthread_self();
#endif
#endif
}

void platform_init_network(void) {
#ifdef _WIN32
    WSADATA w;
    WSAStartup(MAKEWORD(2, 2), &w);
#endif
}

void platform_cleanup_network(void) {
#ifdef _WIN32
    WSACleanup();
#endif
}

int platform_get_cpu_count(void) {
#ifdef _WIN32
    SYSTEM_INFO si; GetSystemInfo(&si); return (int)si.dwNumberOfProcessors;
#else
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    return (n > 0) ? (int)n : 1;
#endif
}

long platform_get_physical_memory_mb(void) {
#ifdef _WIN32
    MEMORYSTATUSEX st; st.dwLength = sizeof(st); if (GlobalMemoryStatusEx(&st)) return (long)(st.ullTotalPhys / (1024 * 1024)); return -1;
#else
    long pages = sysconf(_SC_PHYS_PAGES); long page_size = sysconf(_SC_PAGESIZE);
    if (pages > 0 && page_size > 0) return (long)((pages * page_size) / (1024 * 1024));
    return -1;
#endif
}

void platform_set_socket_options(int sock) {
#ifdef _WIN32
    DWORD rcv = 30000, snd = 30000;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&rcv, sizeof(rcv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (const char*)&snd, sizeof(snd));
    int ka = 1;
    setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (const char*)&ka, sizeof(ka));
    int nd = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (const char*)&nd, sizeof(nd));
    int sz = 256 * 1024;
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, (const char*)&sz, sizeof(sz));
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF, (const char*)&sz, sizeof(sz));
#else
    struct timeval rcv, snd;
    rcv.tv_sec = 30; rcv.tv_usec = 0;
    snd.tv_sec = 30; snd.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &rcv, sizeof(rcv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &snd, sizeof(snd));
    int ka = 1;
    setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &ka, sizeof(ka));
    int idle = 60, intvl = 10, cnt = 3;
    setsockopt(sock, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
    setsockopt(sock, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl));
    setsockopt(sock, IPPROTO_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt));
    int nd = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd));
    int sz = 256 * 1024;
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &sz, sizeof(sz));
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &sz, sizeof(sz));
#endif
}

bool platform_is_file(const char* p) {
#ifdef _WIN32
    DWORD attr = GetFileAttributesA(p);
    return (attr != INVALID_FILE_ATTRIBUTES) && !(attr & FILE_ATTRIBUTE_DIRECTORY);
#else
    struct stat st;
    return stat(p, &st) == 0 && S_ISREG(st.st_mode);
#endif
}

bool platform_is_dir(const char* p) {
#ifdef _WIN32
    DWORD attr = GetFileAttributesA(p);
    return (attr != INVALID_FILE_ATTRIBUTES) && (attr & FILE_ATTRIBUTE_DIRECTORY);
#else
    struct stat st;
    return stat(p, &st) == 0 && S_ISDIR(st.st_mode);
#endif
}

bool platform_real_path(const char* in, char* out) {
#ifdef _WIN32
    if (!_fullpath(out, in, PATH_MAX)) return false;
#else
    if (!realpath(in, out)) return false;
#endif
    normalize_path(out);
#ifdef _WIN32
    if (in && isalpha((unsigned char)in[0]) && in[1] == ':') {
        out[0] = in[0];
    }
#endif
    return true;
}

bool platform_safe_under(const char* base_real, const char* path_real) {
	size_t n = strlen(base_real);
#ifdef _WIN32
	return _strnicmp(base_real, path_real, n) == 0 && (path_real[n] == DIR_SEP || path_real[n] == '\0');
#else
	return strncmp(base_real, path_real, n) == 0 && (path_real[n] == DIR_SEP || path_real[n] == '\0');
#endif
}