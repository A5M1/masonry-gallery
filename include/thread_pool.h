#pragma once

#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

void start_thread_pool(int nworkers);
void enqueue_job(int client_socket);

#ifdef _WIN32
typedef HANDLE thread_mutex_t;
#else
typedef pthread_mutex_t thread_mutex_t;
#endif

int thread_mutex_init(thread_mutex_t* m);
int thread_mutex_destroy(thread_mutex_t* m);
void thread_mutex_lock(thread_mutex_t* m);
void thread_mutex_unlock(thread_mutex_t* m);

int thread_create_detached(void *(*start_routine)(void*), void* arg);

#ifdef __cplusplus
}
#endif