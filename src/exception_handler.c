#include "exception_handler.h"
#include "memory.h"
#include "logging.h"
#include "platform.h"
#include "common.h"
#include "thread_pool.h"

#define EXC_THREAD_SLOTS 128
#define EXC_ALTSTACK_SIZE (256 * 1024)

#if defined(_MSC_VER)
#define EXC_THREAD_LOCAL __declspec(thread)
#else
#define EXC_THREAD_LOCAL _Thread_local
#endif

typedef struct {
    _Atomic int active;
    unsigned long tid;
    char name[48];
    char role[24];
} exc_thread_slot_t;

static exc_thread_slot_t g_thread_slots[EXC_THREAD_SLOTS];
static EXC_THREAD_LOCAL int g_thread_slot = -1;
static _Atomic int g_crash_active;
static _Atomic int g_shutdown_requested;
static int g_crash_fd = -1;

#ifdef _WIN32
static FILE* g_crash_file;
#else
static void* g_altstack;
#include <sys/wait.h>
#endif

static void exception_handler_store_slot(int slot, const char* name, const char* role) {
    if (slot < 0 || slot >= EXC_THREAD_SLOTS) return;
    g_thread_slots[slot].tid = platform_get_tid();
    if (name && name[0]) {
        strncpy(g_thread_slots[slot].name, name, sizeof(g_thread_slots[slot].name) - 1);
    } else {
        strncpy(g_thread_slots[slot].name, "thread", sizeof(g_thread_slots[slot].name) - 1);
    }
    if (role && role[0]) {
        strncpy(g_thread_slots[slot].role, role, sizeof(g_thread_slots[slot].role) - 1);
    } else {
        strncpy(g_thread_slots[slot].role, "worker", sizeof(g_thread_slots[slot].role) - 1);
    }
}

void exception_handler_register_thread(const char* name, const char* role) {
    if (g_thread_slot >= 0) {
        exception_handler_store_slot(g_thread_slot, name, role);
    } else {
        for (int i = 0; i < EXC_THREAD_SLOTS; ++i) {
            int expected = 0;
            if (atomic_compare_exchange_strong(&g_thread_slots[i].active, &expected, 1)) {
                g_thread_slot = i;
                exception_handler_store_slot(i, name, role);
                break;
            }
        }
    }
#ifdef _WIN32
    ULONG guarantee = 65536;
    SetThreadStackGuarantee(&guarantee);
#else
    if (!g_altstack) {
        void* stack = malloc(EXC_ALTSTACK_SIZE);
        if (stack) {
            stack_t ss;
            memset(&ss, 0, sizeof(ss));
            ss.ss_sp = stack;
            ss.ss_size = EXC_ALTSTACK_SIZE;
            ss.ss_flags = 0;
            if (sigaltstack(&ss, NULL) == 0) {
                g_altstack = stack;
            } else {
                free(stack);
            }
        }
    }
#endif
}

void exception_handler_unregister_thread(void) {
    if (g_thread_slot >= 0) {
        atomic_store(&g_thread_slots[g_thread_slot].active, 0);
        g_thread_slot = -1;
    }
#ifndef _WIN32
    if (g_altstack) {
        stack_t ss;
        memset(&ss, 0, sizeof(ss));
        ss.ss_flags = SS_DISABLE;
        sigaltstack(&ss, NULL);
        free(g_altstack);
        g_altstack = NULL;
    }
#endif
}

int exception_handler_crash_pending(void) {
    return atomic_load(&g_crash_active);
}

int exception_handler_shutdown_requested(void) {
    return atomic_load(&g_shutdown_requested);
}

void exception_handler_request_shutdown(void) {
    atomic_store(&g_shutdown_requested, 1);
}

static void crash_printf(const char* format, ...) {
    char buffer[1024];
    va_list args;
    va_start(args, format);
    int length = vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);
    if (length <= 0) return;
    if ((size_t)length >= sizeof(buffer)) length = (int)sizeof(buffer) - 1;
#ifdef _WIN32
    fwrite(buffer, 1, (size_t)length, stderr);
    if (g_crash_file) {
        fwrite(buffer, 1, (size_t)length, g_crash_file);
        fflush(g_crash_file);
    }
#else
    ssize_t ignored = write(2, buffer, (size_t)length);
    (void)ignored;
    if (g_crash_fd >= 0) {
        ignored = write(g_crash_fd, buffer, (size_t)length);
        (void)ignored;
    }
#endif
}

static void crash_begin(void) {
#ifdef _WIN32
    _mkdir("dmp");
    char path[256];
    snprintf(path, sizeof(path), "dmp\\crash_report_%lld_%u.txt",
        (long long)time(NULL), (unsigned int)platform_get_pid());
    g_crash_file = fopen(path, "w");
    if (g_crash_file) {
        crash_printf("crash report: %s\n", path);
    }
#else
    mkdir("dmp", 0755);
    char path[256];
    snprintf(path, sizeof(path), "dmp/crash_report_%lld_%u.txt",
        (long long)time(NULL), (unsigned int)platform_get_pid());
    g_crash_fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (g_crash_fd >= 0) {
        crash_printf("crash report: %s\n", path);
    }
#endif
}

static void crash_end(void) {
#ifdef _WIN32
    if (g_crash_file) {
        fflush(g_crash_file);
        fclose(g_crash_file);
        g_crash_file = NULL;
    }
#else
    if (g_crash_fd >= 0) {
        close(g_crash_fd);
        g_crash_fd = -1;
    }
#endif
}

static void crash_wait(void) {
    const char* enabled = getenv("GALLERIA_CRASH_WAIT");
    if (!enabled || !enabled[0] || strcmp(enabled, "0") == 0) return;
    int seconds = 30;
    const char* seconds_text = getenv("GALLERIA_CRASH_WAIT_SECONDS");
    if (seconds_text && seconds_text[0]) seconds = atoi(seconds_text);
    if (seconds <= 0) return;
    if (seconds > 300) seconds = 300;
    crash_printf("crash: waiting %d second(s) before exit; attach a debugger now\n", seconds);
    for (int i = 0; i < seconds; ++i) {
        platform_sleep_ms(1000);
    }
}

static void write_thread_list(void) {
    crash_printf("Registered threads:\n");
    unsigned long current = platform_get_tid();
    for (int i = 0; i < EXC_THREAD_SLOTS; ++i) {
        if (!atomic_load(&g_thread_slots[i].active)) continue;
        crash_printf("  tid=%lu role=%s name=%s%s\n",
            g_thread_slots[i].tid,
            g_thread_slots[i].role,
            g_thread_slots[i].name,
            g_thread_slots[i].tid == current ? " current" : "");
    }
}

static void write_recent_commands(void) {
    size_t count = 0;
    const platform_recent_cmd_t* recent = platform_get_recent_commands(&count);
    if (!recent || count == 0) return;
    crash_printf("Recent commands (most recent first):\n");
    for (size_t i = 0; i < count; ++i) {
        crash_printf("  ts=%lld thread=%d cmd=%s\n", recent[i].ts_ms, recent[i].thread_id, recent[i].cmd);
    }
}

#ifdef _WIN32

static const char* exception_code_description(DWORD code) {
    switch (code) {
    case EXCEPTION_ACCESS_VIOLATION:         return "Access Violation";
    case EXCEPTION_ARRAY_BOUNDS_EXCEEDED:    return "Array Bounds Exceeded";
    case EXCEPTION_BREAKPOINT:               return "Breakpoint";
    case EXCEPTION_DATATYPE_MISALIGNMENT:    return "Datatype Misalignment";
    case EXCEPTION_FLT_DENORMAL_OPERAND:     return "Float Denormal Operand";
    case EXCEPTION_FLT_DIVIDE_BY_ZERO:       return "Float Divide by Zero";
    case EXCEPTION_FLT_INEXACT_RESULT:       return "Float Inexact Result";
    case EXCEPTION_FLT_INVALID_OPERATION:    return "Float Invalid Operation";
    case EXCEPTION_FLT_OVERFLOW:             return "Float Overflow";
    case EXCEPTION_FLT_STACK_CHECK:          return "Float Stack Check";
    case EXCEPTION_FLT_UNDERFLOW:            return "Float Underflow";
    case EXCEPTION_ILLEGAL_INSTRUCTION:      return "Illegal Instruction";
    case EXCEPTION_IN_PAGE_ERROR:            return "In Page Error";
    case EXCEPTION_INT_DIVIDE_BY_ZERO:       return "Integer Divide by Zero";
    case EXCEPTION_INT_OVERFLOW:             return "Integer Overflow";
    case EXCEPTION_INVALID_DISPOSITION:      return "Invalid Disposition";
    case EXCEPTION_NONCONTINUABLE_EXCEPTION: return "Noncontinuable Exception";
    case EXCEPTION_PRIV_INSTRUCTION:         return "Privileged Instruction";
    case EXCEPTION_SINGLE_STEP:              return "Single Step";
    case EXCEPTION_STACK_OVERFLOW:           return "Stack Overflow";
    case 0xC000041D:                         return "Fatal App Exit (Unhandled Exception)";
    case 0xE06D7363:                         return "C++ Exception";
    default:                                  return "Unknown Exception";
    }
}

static void write_backtrace_with_symbols(void) {
    void* frames[128];
    USHORT fcount = CaptureStackBackTrace(0, 128, frames, NULL);
    HANDLE process = GetCurrentProcess();
    SymSetOptions(SYMOPT_UNDNAME | SYMOPT_DEFERRED_LOADS | SYMOPT_LOAD_LINES);
    BOOL sym_init = SymInitialize(process, NULL, TRUE);

    crash_printf("Captured %u stack frames:\n", (unsigned)fcount);

    for (USHORT i = 0; i < fcount; ++i) {
        DWORD64 addr = (DWORD64)frames[i];
        char buffer[sizeof(SYMBOL_INFO) + MAX_SYM_NAME * sizeof(TCHAR)];
        PSYMBOL_INFO symbol = (PSYMBOL_INFO)buffer;
        symbol->SizeOfStruct = sizeof(SYMBOL_INFO);
        symbol->MaxNameLen = MAX_SYM_NAME;

        DWORD64 displacement = 0;
        if (sym_init && SymFromAddr(process, addr, &displacement, symbol)) {
            IMAGEHLP_LINE64 line;
            line.SizeOfStruct = sizeof(IMAGEHLP_LINE64);
            DWORD line_displacement = 0;
            if (SymGetLineFromAddr64(process, addr, &line_displacement, &line)) {
                crash_printf("  [%02u] %p: %s+0x%llx (%s:%lu)\n",
                    (unsigned)i, frames[i], symbol->Name, displacement,
                    line.FileName, line.LineNumber);
            } else {
                crash_printf("  [%02u] %p: %s+0x%llx\n",
                    (unsigned)i, frames[i], symbol->Name, displacement);
            }
        } else {
            crash_printf("  [%02u] %p\n", (unsigned)i, frames[i]);
        }
    }
    if (sym_init) SymCleanup(process);
}

static void write_process_memory_info(void) {
    PROCESS_MEMORY_COUNTERS pmc;
    if (GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc))) {
        crash_printf("Memory: PageFaultCount=%lu WorkingSetSize=%zu PeakWorkingSetSize=%zu PagefileUsage=%zu\n",
            pmc.PageFaultCount, (size_t)pmc.WorkingSetSize, (size_t)pmc.PeakWorkingSetSize, (size_t)pmc.PagefileUsage);
    }
}

static void write_handles_and_modules(void) {
    HMODULE mods[1024];
    DWORD needed = 0;
    if (EnumProcessModules(GetCurrentProcess(), mods, sizeof(mods), &needed)) {
        int mcount = needed / sizeof(HMODULE);
        crash_printf("Loaded modules (%d):\n", mcount);
        for (int i = 0; i < mcount; ++i) {
            char name[512];
            if (GetModuleFileNameA(mods[i], name, sizeof(name))) {
                MODULEINFO mod_info;
                if (GetModuleInformation(GetCurrentProcess(), mods[i], &mod_info, sizeof(mod_info))) {
                    crash_printf("  %s (Base: %p, Size: %zu)\n", name, mod_info.lpBaseOfDll, mod_info.SizeOfImage);
                } else {
                    crash_printf("  %s\n", name);
                }
            }
        }
    }
}

static void write_minidump_with_filename(EXCEPTION_POINTERS* ep) {
    SYSTEMTIME st;
    GetLocalTime(&st);
    CreateDirectoryA("dmp", NULL);
    char fname[MAX_PATH];
    snprintf(fname, sizeof(fname), "dmp\\crashdump_%04d%02d%02d_%02d%02d%02d.dmp",
        st.wYear, st.wMonth, st.wDay, st.wHour, st.wMinute, st.wSecond);
    HANDLE hFile = CreateFileA(fname, GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hFile == INVALID_HANDLE_VALUE) {
        crash_printf("Failed to create minidump file %s (err=%lu)\n", fname, GetLastError());
        return;
    }
    MINIDUMP_EXCEPTION_INFORMATION mei;
    mei.ThreadId = GetCurrentThreadId();
    mei.ExceptionPointers = ep;
    mei.ClientPointers = FALSE;
    MINIDUMP_TYPE dump_type = (MINIDUMP_TYPE)(
        MiniDumpWithDataSegs |
        MiniDumpWithHandleData |
        MiniDumpWithFullMemoryInfo |
        MiniDumpWithThreadInfo |
        MiniDumpWithUnloadedModules
    );
    BOOL ok = MiniDumpWriteDump(GetCurrentProcess(), GetCurrentProcessId(), hFile,
        dump_type, &mei, NULL, NULL);
    if (!ok) crash_printf("MiniDumpWriteDump failed (err=%lu)\n", GetLastError());
    else crash_printf("Minidump written: %s\n", fname);
    CloseHandle(hFile);
}

static void write_register_state(EXCEPTION_POINTERS* ep) {
    CONTEXT* ctx = ep->ContextRecord;
    crash_printf("Register State:\n");
#if defined(_M_X64) || defined(__x86_64__)
    crash_printf("  RAX=%016llx RBX=%016llx RCX=%016llx RDX=%016llx\n",
        ctx->Rax, ctx->Rbx, ctx->Rcx, ctx->Rdx);
    crash_printf("  RSI=%016llx RDI=%016llx RBP=%016llx RSP=%016llx\n",
        ctx->Rsi, ctx->Rdi, ctx->Rbp, ctx->Rsp);
    crash_printf("  R8 =%016llx R9 =%016llx R10=%016llx R11=%016llx\n",
        ctx->R8, ctx->R9, ctx->R10, ctx->R11);
    crash_printf("  R12=%016llx R13=%016llx R14=%016llx R15=%016llx\n",
        ctx->R12, ctx->R13, ctx->R14, ctx->R15);
    crash_printf("  RIP=%016llx EFLAGS=%08lx\n", ctx->Rip, ctx->EFlags);
#elif defined(_M_IX86) || defined(__i386__)
    crash_printf("  EAX=%08lx EBX=%08lx ECX=%08lx EDX=%08lx\n",
        ctx->Eax, ctx->Ebx, ctx->Ecx, ctx->Edx);
    crash_printf("  ESI=%08lx EDI=%08lx EBP=%08lx ESP=%08lx\n",
        ctx->Esi, ctx->Edi, ctx->Ebp, ctx->Esp);
    crash_printf("  EIP=%08lx EFLAGS=%08lx\n", ctx->Eip, ctx->EFlags);
#elif defined(_M_ARM64) || defined(__aarch64__)
    crash_printf("  X0 =%016llx X1 =%016llx X2 =%016llx X3 =%016llx\n",
        ctx->X0, ctx->X1, ctx->X2, ctx->X3);
    crash_printf("  X4 =%016llx X5 =%016llx X6 =%016llx X7 =%016llx\n",
        ctx->X4, ctx->X5, ctx->X6, ctx->X7);
    crash_printf("  X8 =%016llx X9 =%016llx X10=%016llx X11=%016llx\n",
        ctx->X8, ctx->X9, ctx->X10, ctx->X11);
    crash_printf("  X12=%016llx X13=%016llx X14=%016llx X15=%016llx\n",
        ctx->X12, ctx->X13, ctx->X14, ctx->X15);
    crash_printf("  X16=%016llx X17=%016llx X18=%016llx X19=%016llx\n",
        ctx->X16, ctx->X17, ctx->X18, ctx->X19);
    crash_printf("  X20=%016llx X21=%016llx X22=%016llx X23=%016llx\n",
        ctx->X20, ctx->X21, ctx->X22, ctx->X23);
    crash_printf("  X24=%016llx X25=%016llx X26=%016llx X27=%016llx\n",
        ctx->X24, ctx->X25, ctx->X26, ctx->X27);
    crash_printf("  X28=%016llx FP =%016llx LR =%016llx SP =%016llx\n",
        ctx->X28, ctx->Fp, ctx->Lr, ctx->Sp);
    crash_printf("  PC =%016llx\n", ctx->Pc);
#endif
}

static LONG WINAPI exception_filter(EXCEPTION_POINTERS* ep) {
    int expected = 0;
    if (!atomic_compare_exchange_strong(&g_crash_active, &expected, 1)) {
        ExitProcess(1);
    }
    crash_begin();
    crash_printf("=== UNHANDLED EXCEPTION ===\n");
    DWORD code = ep->ExceptionRecord->ExceptionCode;
    crash_printf("Exception Code: 0x%08x (%s)\n", (unsigned)code, exception_code_description(code));
    crash_printf("Exception Address: %p\n", ep->ExceptionRecord->ExceptionAddress);
    if (code == EXCEPTION_ACCESS_VIOLATION && ep->ExceptionRecord->NumberParameters >= 2) {
        ULONG_PTR access_type = ep->ExceptionRecord->ExceptionInformation[0];
        ULONG_PTR address = ep->ExceptionRecord->ExceptionInformation[1];
        crash_printf("Access Violation: %s at address %p\n",
            access_type == 0 ? "Read" : (access_type == 1 ? "Write" : "DEP"),
            (void*)address);
    }
    crash_printf("Process ID: %u\n", (unsigned)GetCurrentProcessId());
    crash_printf("Thread ID: %u\n", (unsigned)GetCurrentThreadId());
    write_register_state(ep);
    write_thread_list();
    write_recent_commands();
    write_process_memory_info();
    write_handles_and_modules();
    write_backtrace_with_symbols();
    write_minidump_with_filename(ep);
    mem_crash_snapshot(g_crash_file ? _fileno(g_crash_file) : 2);
    crash_wait();
    crash_printf("Exiting process due to unhandled exception\n");
    crash_end();
    ExitProcess(1);
    return EXCEPTION_EXECUTE_HANDLER;
}

static BOOL WINAPI console_ctrl_handler(DWORD type) {
    if (type == CTRL_C_EVENT || type == CTRL_BREAK_EVENT) {
        exception_handler_request_shutdown();
        return TRUE;
    }
    return FALSE;
}

void install_exception_handlers(void) {
    SetUnhandledExceptionFilter(exception_filter);
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX);
    SetConsoleCtrlHandler(console_ctrl_handler, TRUE);
}

void install_thread_exception_handler(void) {
    exception_handler_register_thread("worker", "worker");
}

#else

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
    crash_printf("Captured %d stack frames:\n", size);
    backtrace_symbols_fd(array, size, g_crash_fd >= 0 ? g_crash_fd : 2);
    if (g_crash_fd >= 0) {
        backtrace_symbols_fd(array, size, 2);
    }
}

static void crash_child_report(int sig, siginfo_t* info, void* context) {
    (void)context;
    crash_begin();
    crash_printf("=== FATAL SIGNAL ===\n");
    crash_printf("Received signal %d (%s)\n", sig, signal_name(sig));
    if (info) {
        crash_printf("Signal code=%d address=%p\n", info->si_code, info->si_addr);
    }
    crash_printf("Process ID: %u\n", (unsigned)getpid());
    crash_printf("Thread ID: %lu\n", (unsigned long)platform_get_tid());
    write_thread_list();
    write_recent_commands();
    mem_crash_snapshot(g_crash_fd >= 0 ? g_crash_fd : 2);
    write_backtrace();
    crash_wait();
    crash_printf("Exiting process due to fatal signal\n");
    crash_end();
    _exit(1);
}

static void fatal_signal_handler(int sig, siginfo_t* info, void* context) {
    int expected = 0;
    if (!atomic_compare_exchange_strong(&g_crash_active, &expected, 1)) {
        _exit(128 + sig);
    }
    pid_t child = fork();
    if (child == 0) {
        crash_child_report(sig, info, context);
    }
    if (child > 0) {
        int status = 0;
        while (waitpid(child, &status, 0) < 0 && errno == EINTR) {
        }
    } else {
        crash_printf("=== FATAL SIGNAL ===\nReceived signal %d (%s)\n", sig, signal_name(sig));
        mem_crash_snapshot(2);
        crash_wait();
    }
    signal(sig, SIG_DFL);
    raise(sig);
}

static void graceful_signal_handler(int sig) {
    (void)sig;
    exception_handler_request_shutdown();
}

void install_exception_handlers(void) {
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_sigaction = fatal_signal_handler;
    sigemptyset(&action.sa_mask);
    action.sa_flags = SA_SIGINFO | SA_ONSTACK | SA_RESETHAND;
    sigaction(SIGSEGV, &action, NULL);
    sigaction(SIGABRT, &action, NULL);
    sigaction(SIGFPE, &action, NULL);
    sigaction(SIGILL, &action, NULL);
    sigaction(SIGBUS, &action, NULL);
    struct sigaction graceful;
    memset(&graceful, 0, sizeof(graceful));
    graceful.sa_handler = graceful_signal_handler;
    sigemptyset(&graceful.sa_mask);
    graceful.sa_flags = 0;
    sigaction(SIGINT, &graceful, NULL);
    sigaction(SIGTERM, &graceful, NULL);
}

void install_thread_exception_handler(void) {
    exception_handler_register_thread("worker", "worker");
}

#endif
