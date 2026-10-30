#pragma once

void install_exception_handlers(void);
void install_thread_exception_handler(void);
void exception_handler_register_thread(const char* name, const char* role);
void exception_handler_unregister_thread(void);
int exception_handler_crash_pending(void);
int exception_handler_shutdown_requested(void);
void exception_handler_request_shutdown(void);
