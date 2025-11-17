#include "exception_handler.h"
#include "logging.h"
#include "platform.h"
#include "common.h"
#include "thread_pool.h"

#ifdef _WIN32
#include <windows.h>
#include <dbghelp.h>
#include <psapi.h>

static void write_backtrace(void) {
    void* frames[128];
    USHORT fcount = CaptureStackBackTrace(0, 128, frames, NULL);
    LOG_ERROR("Captured %u stack frames:", (unsigned)fcount);
    for (USHORT i = 0; i < fcount; ++i)
        LOG_ERROR("  [%02u] %p", (unsigned)i, frames[i]);
}

static void write_process_memory_info(void) {
    PROCESS_MEMORY_COUNTERS pmc;
    if (GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc))) {
        LOG_ERROR("Memory: PageFaultCount=%lu WorkingSetSize=%zu PeakWorkingSetSize=%zu PagefileUsage=%zu",
            pmc.PageFaultCount, (size_t)pmc.WorkingSetSize, (size_t)pmc.PeakWorkingSetSize, (size_t)pmc.PagefileUsage);
    }
}

static void write_handles_and_modules(void) {
    HMODULE mods[1024];
    DWORD needed = 0;
    if (EnumProcessModules(GetCurrentProcess(), mods, sizeof(mods), &needed)) {
        int mcount = needed / sizeof(HMODULE);
        LOG_ERROR("Loaded modules (%d):", mcount);
        for (int i = 0; i < mcount; ++i) {
            char name[512];
            if (GetModuleFileNameA(mods[i], name, sizeof(name)))
                LOG_ERROR("  %s", name);
        }
    }
}

static void write_minidump_with_filename(EXCEPTION_POINTERS* ep) {
    SYSTEMTIME st;
    GetLocalTime(&st);
    char dir[MAX_PATH] = "dmp";
    CreateDirectoryA(dir, NULL);

    char fname[MAX_PATH];
    snprintf(fname, sizeof(fname), "%s\\crashdump_%04d%02d%02d_%02d%02d%02d.dmp",
        dir, st.wYear, st.wMonth, st.wDay, st.wHour, st.wMinute, st.wSecond);

    HANDLE hFile = CreateFileA(fname, GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hFile == INVALID_HANDLE_VALUE) {
        LOG_ERROR("Failed to create minidump file %s (err=%lu)", fname, GetLastError());
        return;
    }

    MINIDUMP_EXCEPTION_INFORMATION mei;
    mei.ThreadId = GetCurrentThreadId();
    mei.ExceptionPointers = ep;
    mei.ClientPointers = FALSE;

    BOOL ok = MiniDumpWriteDump(GetCurrentProcess(), GetCurrentProcessId(), hFile,
        MiniDumpWithFullMemory | MiniDumpWithHandleData, &mei, NULL, NULL);
    if (!ok)
        LOG_ERROR("MiniDumpWriteDump failed (err=%lu)", GetLastError());
    else
        LOG_ERROR("Minidump written: %s", fname);

    CloseHandle(hFile);
}

static LONG WINAPI exception_filter(EXCEPTION_POINTERS* ep) {
    LOG_ERROR("=== UNHANDLED EXCEPTION ===");
    LOG_ERROR("Code=0x%08x Address=%p",
        (unsigned)ep->ExceptionRecord->ExceptionCode,
        ep->ExceptionRecord->ExceptionAddress);

    DWORD pid = GetCurrentProcessId();
    DWORD tid = GetCurrentThreadId();
    LOG_ERROR("Process=%u Thread=%u", (unsigned)pid, (unsigned)tid);

    size_t rcnt = 0;
    const platform_recent_cmd_t* recent = platform_get_recent_commands(&rcnt);
    if (recent && rcnt > 0) {
        LOG_ERROR("Recent commands (most recent first):");
        for (size_t i = 0; i < rcnt; ++i) {
            const platform_recent_cmd_t* r = &recent[i];
            LOG_ERROR("  ts=%lld thread=%d cmd=%s", r->ts_ms, r->thread_id, r->cmd);
        }
    }

    write_process_memory_info();
    write_handles_and_modules();
    stop_thread_pool();
    write_backtrace();
    write_minidump_with_filename(ep);

    LOG_ERROR("Press Enter to exit the process due to unhandled exception...");
    fflush(stderr);
    getchar();

    LOG_ERROR("Exiting process due to unhandled exception");
    ExitProcess(1);
}

void install_exception_handlers(void) {
    SetUnhandledExceptionFilter(exception_filter);
}

#else // POSIX
#if defined(__linux__)
#include <sys/syscall.h>
#endif

static const char* signal_name(int sig) {
    switch (sig) {
    case SIGSEGV: return "SIGSEGV (Segmentation Fault)";
    case SIGABRT: return "SIGABRT (Abort)";
    case SIGFPE:  return "SIGFPE (Floating Point Error)";
    case SIGILL:  return "SIGILL (Illegal Instruction)";
    case SIGBUS:  return "SIGBUS (Bus Error)";
    default:      return "Unknown Signal";
    }
}

static void write_backtrace(void) {
    void* array[128];
    int size = backtrace(array, 128);
    char** symbols = backtrace_symbols(array, size);
    if (!symbols) {
        LOG_ERROR("Failed to get backtrace symbols");
        return;
    }
    LOG_ERROR("Captured %d stack frames:", size);
    for (int i = 0; i < size; i++)
        LOG_ERROR("  [%02d] %s", i, symbols[i]);
    free(symbols);
}

static void signal_handler(int sig) {
    LOG_ERROR("=== SIGNAL HANDLER TRIGGERED ===");
    LOG_ERROR("Received signal %d (%s)", sig, signal_name(sig));

    const char* last = platform_get_last_command();
    if (last) LOG_ERROR("Last attempted command: %s", last);

    unsigned int pid = (unsigned int)getpid();
    unsigned long tid = 0;
#if defined(__linux__)
    tid = (unsigned long)syscall(SYS_gettid);
#else
    tid = (unsigned long)pthread_self();
#endif
    LOG_ERROR("Process=%u Thread=%lu", pid, tid);

    write_backtrace();
    LOG_ERROR("Press Enter to exit..."); fflush(stderr); getchar();
    _exit(1);
}

void install_exception_handlers(void) {
    signal(SIGSEGV, signal_handler);
    signal(SIGABRT, signal_handler);
    signal(SIGFPE, signal_handler);
    signal(SIGILL, signal_handler);
    signal(SIGBUS, signal_handler);
}
#endif
