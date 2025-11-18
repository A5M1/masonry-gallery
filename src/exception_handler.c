#include "exception_handler.h"
#include "logging.h"
#include "platform.h"
#include "common.h"
#include "thread_pool.h"

#ifdef _WIN32
#include <windows.h>
#include <dbghelp.h>
#include <psapi.h>

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
    
    // Initialize symbol handler for better stack traces
    HANDLE process = GetCurrentProcess();
    SymSetOptions(SYMOPT_UNDNAME | SYMOPT_DEFERRED_LOADS | SYMOPT_LOAD_LINES);
    BOOL sym_init = SymInitialize(process, NULL, TRUE);
    
    LOG_ERROR("Captured %u stack frames:", (unsigned)fcount);
    
    for (USHORT i = 0; i < fcount; ++i) {
        DWORD64 addr = (DWORD64)frames[i];
        char buffer[sizeof(SYMBOL_INFO) + MAX_SYM_NAME * sizeof(TCHAR)];
        PSYMBOL_INFO symbol = (PSYMBOL_INFO)buffer;
        symbol->SizeOfStruct = sizeof(SYMBOL_INFO);
        symbol->MaxNameLen = MAX_SYM_NAME;
        
        DWORD64 displacement = 0;
        if (sym_init && SymFromAddr(process, addr, &displacement, symbol)) {
            // Try to get line information
            IMAGEHLP_LINE64 line;
            line.SizeOfStruct = sizeof(IMAGEHLP_LINE64);
            DWORD line_displacement = 0;
            
            if (SymGetLineFromAddr64(process, addr, &line_displacement, &line)) {
                LOG_ERROR("  [%02u] %p: %s+0x%llx (%s:%lu)", 
                    (unsigned)i, frames[i], symbol->Name, displacement, 
                    line.FileName, line.LineNumber);
            } else {
                LOG_ERROR("  [%02u] %p: %s+0x%llx", 
                    (unsigned)i, frames[i], symbol->Name, displacement);
            }
        } else {
            LOG_ERROR("  [%02u] %p", (unsigned)i, frames[i]);
        }
    }
    
    if (sym_init) {
        SymCleanup(process);
    }
}

static void write_backtrace(void) {
    write_backtrace_with_symbols();
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
            if (GetModuleFileNameA(mods[i], name, sizeof(name))) {
                MODULEINFO mod_info;
                if (GetModuleInformation(GetCurrentProcess(), mods[i], &mod_info, sizeof(mod_info))) {
                    LOG_ERROR("  %s (Base: %p, Size: %zu)", name, mod_info.lpBaseOfDll, mod_info.SizeOfImage);
                } else {
                    LOG_ERROR("  %s", name);
                }
            }
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

    // Use enhanced minidump options for better debugging
    // MiniDumpWithDataSegs: Include data segments (global and static variables)
    // MiniDumpWithHandleData: Include handle information
    // MiniDumpWithFullMemoryInfo: Include memory region information
    // MiniDumpWithThreadInfo: Include thread state information
    // MiniDumpWithUnloadedModules: Include recently unloaded modules
    MINIDUMP_TYPE dump_type = (MINIDUMP_TYPE)(
        MiniDumpWithDataSegs | 
        MiniDumpWithHandleData | 
        MiniDumpWithFullMemoryInfo | 
        MiniDumpWithThreadInfo | 
        MiniDumpWithUnloadedModules
    );

    BOOL ok = MiniDumpWriteDump(GetCurrentProcess(), GetCurrentProcessId(), hFile,
        dump_type, &mei, NULL, NULL);
    if (!ok)
        LOG_ERROR("MiniDumpWriteDump failed (err=%lu)", GetLastError());
    else
        LOG_ERROR("Minidump written: %s", fname);

    CloseHandle(hFile);
}

static void write_register_state(EXCEPTION_POINTERS* ep) {
    CONTEXT* ctx = ep->ContextRecord;
    LOG_ERROR("Register State:");
    
#if defined(_M_X64) || defined(__x86_64__)
    LOG_ERROR("  RAX=%016llx RBX=%016llx RCX=%016llx RDX=%016llx",
        ctx->Rax, ctx->Rbx, ctx->Rcx, ctx->Rdx);
    LOG_ERROR("  RSI=%016llx RDI=%016llx RBP=%016llx RSP=%016llx",
        ctx->Rsi, ctx->Rdi, ctx->Rbp, ctx->Rsp);
    LOG_ERROR("  R8 =%016llx R9 =%016llx R10=%016llx R11=%016llx",
        ctx->R8, ctx->R9, ctx->R10, ctx->R11);
    LOG_ERROR("  R12=%016llx R13=%016llx R14=%016llx R15=%016llx",
        ctx->R12, ctx->R13, ctx->R14, ctx->R15);
    LOG_ERROR("  RIP=%016llx EFLAGS=%08lx",
        ctx->Rip, ctx->EFlags);
#elif defined(_M_IX86) || defined(__i386__)
    LOG_ERROR("  EAX=%08lx EBX=%08lx ECX=%08lx EDX=%08lx",
        ctx->Eax, ctx->Ebx, ctx->Ecx, ctx->Edx);
    LOG_ERROR("  ESI=%08lx EDI=%08lx EBP=%08lx ESP=%08lx",
        ctx->Esi, ctx->Edi, ctx->Ebp, ctx->Esp);
    LOG_ERROR("  EIP=%08lx EFLAGS=%08lx",
        ctx->Eip, ctx->EFlags);
#elif defined(_M_ARM64) || defined(__aarch64__)
    LOG_ERROR("  X0 =%016llx X1 =%016llx X2 =%016llx X3 =%016llx",
        ctx->X0, ctx->X1, ctx->X2, ctx->X3);
    LOG_ERROR("  X4 =%016llx X5 =%016llx X6 =%016llx X7 =%016llx",
        ctx->X4, ctx->X5, ctx->X6, ctx->X7);
    LOG_ERROR("  X8 =%016llx X9 =%016llx X10=%016llx X11=%016llx",
        ctx->X8, ctx->X9, ctx->X10, ctx->X11);
    LOG_ERROR("  X12=%016llx X13=%016llx X14=%016llx X15=%016llx",
        ctx->X12, ctx->X13, ctx->X14, ctx->X15);
    LOG_ERROR("  X16=%016llx X17=%016llx X18=%016llx X19=%016llx",
        ctx->X16, ctx->X17, ctx->X18, ctx->X19);
    LOG_ERROR("  X20=%016llx X21=%016llx X22=%016llx X23=%016llx",
        ctx->X20, ctx->X21, ctx->X22, ctx->X23);
    LOG_ERROR("  X24=%016llx X25=%016llx X26=%016llx X27=%016llx",
        ctx->X24, ctx->X25, ctx->X26, ctx->X27);
    LOG_ERROR("  X28=%016llx FP =%016llx LR =%016llx SP =%016llx",
        ctx->X28, ctx->Fp, ctx->Lr, ctx->Sp);
    LOG_ERROR("  PC =%016llx", ctx->Pc);
#endif
}

static LONG WINAPI exception_filter(EXCEPTION_POINTERS* ep) {
    LOG_ERROR("=== UNHANDLED EXCEPTION ===");
    
    DWORD code = ep->ExceptionRecord->ExceptionCode;
    const char* desc = exception_code_description(code);
    
    LOG_ERROR("Exception Code: 0x%08x (%s)", (unsigned)code, desc);
    LOG_ERROR("Exception Address: %p", ep->ExceptionRecord->ExceptionAddress);
    
    // Additional information for specific exception types
    if (code == EXCEPTION_ACCESS_VIOLATION && ep->ExceptionRecord->NumberParameters >= 2) {
        ULONG_PTR access_type = ep->ExceptionRecord->ExceptionInformation[0];
        ULONG_PTR address = ep->ExceptionRecord->ExceptionInformation[1];
        LOG_ERROR("Access Violation: %s at address %p",
            access_type == 0 ? "Read" : (access_type == 1 ? "Write" : "DEP"),
            (void*)address);
    }
    
    DWORD pid = GetCurrentProcessId();
    DWORD tid = GetCurrentThreadId();
    LOG_ERROR("Process ID: %u", (unsigned)pid);
    LOG_ERROR("Thread ID: %u", (unsigned)tid);
    
    write_register_state(ep);

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

    size_t rcnt = 0;
    const platform_recent_cmd_t* recent = platform_get_recent_commands(&rcnt);
    if (recent && rcnt > 0) {
        LOG_ERROR("Recent commands (most recent first):");
        for (size_t i = 0; i < rcnt && i < 5; ++i) {
            const platform_recent_cmd_t* r = &recent[i];
            LOG_ERROR("  ts=%lld thread=%d cmd=%s", r->ts_ms, r->thread_id, r->cmd);
        }
    }

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
