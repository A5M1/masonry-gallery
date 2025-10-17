#include "platform.h"
#include "common.h"
#include "thread_pool.h"
#include "logging.h"

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

typedef struct {FILE* f; HANDLE proc;} PHandleMap;
static PHandleMap g_ph[32];
static int g_phc=0;

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
    if(g_phc<32){g_ph[g_phc].f=f;g_ph[g_phc].proc=pi.hProcess;g_phc++;}
    setvbuf(f,NULL,_IONBF,0);
    return f;
#else
    FILE* f = popen(cmd,mode);
    if (!f) LOG_WARN("platform_popen_direct: popen failed for '%s' err=%s", cmd ? cmd : "(null)", strerror(errno));
    return f;
#endif
}

int platform_pclose_direct(FILE* f){
#ifdef _WIN32
    if(!f)return-1;
    HANDLE hProc=NULL;
    for(int i=0;i<g_phc;i++){
        if(g_ph[i].f==f){hProc=g_ph[i].proc;
            g_ph[i]=g_ph[--g_phc];break;}
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
        BOOL ok = ReadDirectoryChangesW(hDir, buffer, sizeof(buffer), FALSE, FILE_NOTIFY_CHANGE_FILE_NAME | FILE_NOTIFY_CHANGE_SIZE | FILE_NOTIFY_CHANGE_LAST_WRITE, &bytesReturned, NULL, NULL);
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
            } else if (action == FILE_ACTION_ADDED || action == FILE_ACTION_RENAMED_NEW_NAME || action == FILE_ACTION_MODIFIED) {
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
    int wd = inotify_add_watch(fd, dir, IN_CREATE | IN_MOVED_TO | IN_MODIFY);
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
                if ((ev->mask & (IN_CREATE | IN_MOVED_TO | IN_MODIFY))) {
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
            if (TransmitFile((SOCKET)client_socket, hFile, toWrite, 0, NULL, NULL, 0)) { CloseHandle(hFile); return 0; }
        } else {
            if (TransmitFile((SOCKET)client_socket, hFile, 0, 0, NULL, NULL, 0)) { CloseHandle(hFile); return 0; }
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
    return (rem > 0) ? -1 : 0;
#else
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
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
    return 0;
#endif
}

#include <time.h>

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