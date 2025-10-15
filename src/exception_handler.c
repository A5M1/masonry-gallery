#include "exception_handler.h"
#include "logging.h"
#include <signal.h>
#ifdef _WIN32
#include <windows.h>
#include <dbghelp.h>
static void write_backtrace(void) {
    void* frames[64];
    USHORT frames_c = CaptureStackBackTrace(0, 64, frames, NULL);
    for (USHORT i = 0; i < frames_c; ++i) {
        LOG_ERROR("backtrace: frame[%u]=%p", (unsigned)i, frames[i]);
    }
}
static LONG WINAPI exception_filter(EXCEPTION_POINTERS* ep) {
    LOG_ERROR("Unhandled exception code=0x%08x at address=%p", (unsigned)ep->ExceptionRecord->ExceptionCode, ep->ExceptionRecord->ExceptionAddress);
    write_backtrace();
    return EXCEPTION_EXECUTE_HANDLER;
}
void install_exception_handlers(void) {
    SetUnhandledExceptionFilter(exception_filter);
}
#else
static void signal_handler(int sig) {
    LOG_ERROR("Received signal %d", sig);
    /* no portable backtrace here without extra deps */
}
void install_exception_handlers(void) {
    signal(SIGSEGV, signal_handler);
    signal(SIGABRT, signal_handler);
    signal(SIGFPE, signal_handler);
    signal(SIGILL, signal_handler);
}
#endif
