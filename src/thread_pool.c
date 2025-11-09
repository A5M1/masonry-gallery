#include "thread_pool.h"
#include "api_handlers.h"
#include "logging.h"
#include "http.h"
#include "common.h"
#define QUEUE_CAP 1024

static int* job_ring;
static int job_head=0;
static int job_tail=0;
static int job_count=0;
static int shutting_down = 0;

#ifdef _WIN32
static thread_mutex_t job_mutex;
static HANDLE job_not_empty;
static HANDLE job_not_full;
static HANDLE shutdown_event = NULL;
static HANDLE *worker_handles = NULL;
static int worker_count = 0;
#else
static thread_mutex_t job_mutex;
static pthread_cond_t job_not_empty=PTHREAD_COND_INITIALIZER;
static pthread_cond_t job_not_full=PTHREAD_COND_INITIALIZER;
static pthread_t *worker_handles = NULL;
static int worker_count = 0;
#endif

static int dequeue_job(void);
int thread_mutex_init(thread_mutex_t* m) {
#ifdef _WIN32
    *m = CreateMutex(NULL, FALSE, NULL);
    return (*m != NULL) ? 0 : -1;
#else
    return pthread_mutex_init(m, NULL);
#endif
}

int thread_mutex_destroy(thread_mutex_t* m) {
#ifdef _WIN32
    return CloseHandle(*m) ? 0 : -1;
#else
    return pthread_mutex_destroy(m);
#endif
}

void thread_mutex_lock(thread_mutex_t* m) {
#ifdef _WIN32
    WaitForSingleObject(*m, INFINITE);
#else
    pthread_mutex_lock(m);
#endif
}

void thread_mutex_unlock(thread_mutex_t* m) {
#ifdef _WIN32
    ReleaseMutex(*m);
#else
    pthread_mutex_unlock(m);
#endif
}

int thread_create_detached(void *(*start_routine)(void*), void* arg) {
#ifdef _WIN32
    uintptr_t th = _beginthreadex(NULL, 0, (unsigned (__stdcall *)(void *))start_routine, arg, 0, NULL);
    if (th == 0) return -1;
    CloseHandle((HANDLE)th);
    return 0;
#else
    pthread_t th;
    if (pthread_create(&th, NULL, start_routine, arg) != 0) return -1;
    pthread_detach(th);
    return 0;
#endif
}

#ifdef _WIN32
static unsigned __stdcall worker_thread(void* arg) {
#else
static void* worker_thread(void* arg) {
#endif
    (void)arg;
    size_t buf_size = 8192;
    char* buffer = malloc(buf_size);
    if (!buffer) {
        LOG_ERROR("Failed to allocate thread buffer of size %zu", buf_size);
#ifdef _WIN32
        return 1;
#else
        return NULL;
#endif
    }
    for (;;) {
        int c = dequeue_job();
        if (c < 0) break;
        size_t total_read = 0;
        int content_length = 0;
        int keep_socket = 0;
        bool headers_done = false;
        char* headers_end = NULL;
        int broken = 0;
        struct timeval timeout;
        timeout.tv_sec = 30;  
        timeout.tv_usec = 0;
        setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));

        while (1) {
            if (total_read >= buf_size - 1) {
                buf_size *= 2;
                char* new_buf = realloc(buffer, buf_size);
                if (!new_buf) {
                    LOG_ERROR("Failed to realloc request buffer to size %zu", buf_size);
                    broken = 1;
                    break;
                }
                buffer = new_buf;
            }

            int r = recv(c, buffer + total_read, buf_size - total_read - 1, 0);
            if (r <= 0) {
                if (r == 0) {
                    LOG_DEBUG("Client disconnected");
                } else {
#ifdef _WIN32
                    int err = WSAGetLastError();
                    if (err == WSAETIMEDOUT) {
                        LOG_DEBUG("Socket timeout on connection %d", c);
                    } else {
                        LOG_ERROR("recv error: %d", err);
                    }
#else
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        LOG_DEBUG("Socket timeout on connection %d", c);
                    } else {
                        LOG_ERROR("recv error: %s", strerror(errno));
                    }
#endif
                }
                broken = 1;
                break;
            }

            total_read += r;
            buffer[total_read] = '\0';
            if (!headers_done) {
                headers_end = strstr(buffer, "\r\n\r\n");
                if (headers_end) {
                    headers_done = true;
                    char method[16] = {0};
                    if (sscanf(buffer, "%15s", method) == 1) { 
                        if (strcmp(method, "POST") == 0) {
                            char* cl = get_header_value(buffer, "Content-Length:");
                            if (cl) {
                                content_length = atoi(cl);
                                if (content_length < 0 || content_length > 10*1024*1024) {
                                    LOG_WARN("Invalid content length: %d", content_length);
                                    SAFE_FREE(cl);
                                    broken = 1;
                                    break;
                                }
                                SAFE_FREE(cl);
                            }
                        }
                    }
                }
            }

            if (headers_done) {
                size_t body_received = total_read - (headers_end - buffer + 4);
                if (body_received >= (size_t)content_length) break;
            }
        }

        if (!broken && total_read > 0 && headers_done) {
            size_t headers_len = headers_end - buffer + 4;
            if (headers_len >= 2) buffer[headers_len - 2] = '\0';

            char* headers_copy = NULL;
            char stack_headers[4096];
            if (headers_len < sizeof(stack_headers)) {
                memcpy(stack_headers, buffer, headers_len);
                stack_headers[headers_len] = '\0';
                headers_copy = stack_headers;
            } else {
                headers_copy = malloc(headers_len + 1);
                if (!headers_copy) {
                    LOG_ERROR("Failed to allocate headers copy of size %zu", headers_len + 1);
                    SOCKET_CLOSE(c);
                    continue;
                }
                memcpy(headers_copy, buffer, headers_len);
                headers_copy[headers_len] = '\0';
            }

            char* body = NULL;
            char stack_body[4096];
            if (content_length > 0) {
                if (content_length < sizeof(stack_body)) {
                    memcpy(stack_body, buffer + headers_len, content_length);
                    stack_body[content_length] = '\0';
                    body = stack_body;
                } else {
                    body = malloc(content_length + 1);
                    if (!body) {
                        LOG_ERROR("Failed to allocate body buffer of size %d", content_length + 1);
                        if (headers_copy != stack_headers) free(headers_copy);
                        SOCKET_CLOSE(c);
                        continue;
                    }
                    memcpy(body, buffer + headers_len, content_length);
                    body[content_length] = '\0';
                }
            }

            int keep_socket = handle_single_request(c, headers_copy, body, headers_len, content_length, true);

            if (headers_copy != stack_headers) free(headers_copy);
            if (body != stack_body && body != NULL) free(body);
        }

        if (!keep_socket) SOCKET_CLOSE(c);
    }

    free(buffer);
#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}

#ifdef _WIN32
static int get_worker_count(void) {
	SYSTEM_INFO si;
	GetSystemInfo(&si);
	int n=(int)si.dwNumberOfProcessors;
	if(n<1) n=1;
	int w=n*2;
	if(w<4) w=4;
	return w;
}
#else
static int get_worker_count(void) {
	int n=(int)sysconf(_SC_NPROCESSORS_ONLN);
	if(n<1) n=1;
	int w=n*2;
	if(w<4) w=4;
	return w;
}
#endif

void start_thread_pool(int nworkers) {
	if(nworkers<=0) nworkers=get_worker_count();
	LOG_INFO("Starting thread pool with %d workers", nworkers);
    job_ring=calloc(QUEUE_CAP, sizeof(int));
    if (!job_ring) {
        LOG_ERROR("Failed to allocate job ring buffer of size %d", QUEUE_CAP);
        return;
    }
    for (int i = 0; i < QUEUE_CAP; ++i) job_ring[i] = -1;
    thread_mutex_init(&job_mutex);
#ifdef _WIN32
    job_not_empty=CreateSemaphore(NULL, 0, QUEUE_CAP, NULL);
    job_not_full=CreateSemaphore(NULL, QUEUE_CAP, QUEUE_CAP, NULL);
    if (!job_not_empty || !job_not_full) {
        LOG_ERROR("Failed to create thread pool synchronization objects");
        return;
    }
    shutdown_event = CreateEvent(NULL, TRUE, FALSE, NULL);
    if (!shutdown_event) {
        LOG_ERROR("Failed to create shutdown event");
        return;
    }
    worker_handles = calloc(nworkers, sizeof(HANDLE));
    if (!worker_handles) {
        LOG_ERROR("Failed to allocate worker handles array for %d workers", nworkers);
        return;
    }
    worker_count = nworkers;
    for(int i=0;i<nworkers;++i) {
        uintptr_t th = _beginthreadex(NULL, 0, worker_thread, NULL, 0, NULL);
        if (th == 0) {
            LOG_ERROR("Failed to create worker thread %d", i);
            worker_handles[i] = NULL;
            continue;
        }
        worker_handles[i] = (HANDLE)th;
    }
#else
    worker_handles = calloc(nworkers, sizeof(pthread_t));
    if (!worker_handles) {
        LOG_ERROR("Failed to allocate worker handles array for %d workers", nworkers);
        return;
    }
    worker_count = nworkers;
    for(int i=0;i<nworkers;++i) {
        if (pthread_create(&worker_handles[i], NULL, worker_thread, NULL) != 0) {
            LOG_ERROR("Failed to create worker thread %d", i);
            worker_handles[i] = 0;
            continue;
        }
    }
#endif
}

void enqueue_job(int client_socket) {
    if (shutting_down) {
        LOG_WARN("Attempt to enqueue while shutting down, closing socket %d", client_socket);
        SOCKET_CLOSE(client_socket);
        return;
    }
    thread_mutex_lock(&job_mutex);
    if (!job_ring) {
        thread_mutex_unlock(&job_mutex);
        LOG_WARN("enqueue_job called but job_ring is NULL, closing socket %d", client_socket);
        SOCKET_CLOSE(client_socket);
        return;
    }
    if (job_count == QUEUE_CAP) {
        LOG_WARN("Job queue is full, dropping connection %d", client_socket);
        thread_mutex_unlock(&job_mutex);
        SOCKET_CLOSE(client_socket);
        return;
    }
    job_ring[job_tail] = client_socket;
    job_tail = (job_tail + 1) % QUEUE_CAP;
    job_count++;
#ifdef _WIN32
    ReleaseSemaphore(job_not_empty, 1, NULL);
#else
    pthread_cond_signal(&job_not_empty);
#endif
    thread_mutex_unlock(&job_mutex);
    LOG_DEBUG("Enqueued client socket %d", client_socket);
}

static int dequeue_job(void) {
    int c;
    thread_mutex_lock(&job_mutex);
#ifdef _WIN32
    while (job_count == 0 && !shutting_down) {
        thread_mutex_unlock(&job_mutex);
        HANDLE hs[2] = { job_not_empty, shutdown_event };
        DWORD idx = WaitForMultipleObjects(2, hs, FALSE, INFINITE);
        thread_mutex_lock(&job_mutex);
        if (idx == WAIT_OBJECT_0 + 1) {
            break;
        }
    }
#else
    while(job_count==0 && !shutting_down) {
        pthread_cond_wait(&job_not_empty, &job_mutex);
    }
#endif
    if (job_ring == NULL || job_count == 0) {
        thread_mutex_unlock(&job_mutex);
        return -1;
    }
    c = job_ring[job_head];
    job_ring[job_head] = -1;
    job_head=(job_head+1)%QUEUE_CAP;
    job_count--;
#ifdef _WIN32
    ReleaseSemaphore(job_not_full, 1, NULL);
#else
    pthread_cond_signal(&job_not_full);
#endif
    thread_mutex_unlock(&job_mutex);
	LOG_DEBUG("Dequeued client socket %d", c);
	return c;
}

void stop_thread_pool(void) {
    LOG_INFO("Stopping thread pool");
    shutting_down = 1;
#ifdef _WIN32
    if (shutdown_event) SetEvent(shutdown_event);
    if (job_not_empty) ReleaseSemaphore(job_not_empty, worker_count, NULL);
    for (int i = 0; i < worker_count; ++i) {
        if (worker_handles && worker_handles[i]) {
            DWORD wait = WaitForSingleObject(worker_handles[i], 5000);
            if (wait != WAIT_OBJECT_0) {
                LOG_WARN("Worker thread %d did not exit in time, forcing termination", i);
                TerminateThread(worker_handles[i], 1);
            }
            CloseHandle(worker_handles[i]);
            worker_handles[i] = NULL;
        }
    }
    if (worker_handles) { free(worker_handles); worker_handles = NULL; }
    if (shutdown_event) { CloseHandle(shutdown_event); shutdown_event = NULL; }
    if (job_not_empty) { CloseHandle(job_not_empty); job_not_empty = NULL; }
    if (job_not_full) { CloseHandle(job_not_full); job_not_full = NULL; }
#else
    pthread_cond_broadcast(&job_not_empty);
    thread_mutex_lock(&job_mutex);
    thread_mutex_unlock(&job_mutex);
    if (worker_handles) {
        for (int i = 0; i < worker_count; ++i) {
            if (worker_handles[i]) {
                pthread_join(worker_handles[i], NULL);
                worker_handles[i] = 0;
            }
        }
        free(worker_handles);
        worker_handles = NULL;
        worker_count = 0;
    }
#endif
    if (job_ring) { free(job_ring); job_ring = NULL; }
    thread_mutex_destroy(&job_mutex);
    LOG_INFO("Thread pool stopped");
}
