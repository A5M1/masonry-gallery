#include "thread_pool.h"
#include "api_handlers.h"
#include "logging.h"
#include "http.h"
#define QUEUE_CAP 1024

static int* job_ring;
static int job_head=0;
static int job_tail=0;
static int job_count=0;

#ifdef _WIN32
static HANDLE job_mutex;
static HANDLE job_not_empty;
static HANDLE job_not_full;
#else
static pthread_mutex_t job_mutex=PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t job_not_empty=PTHREAD_COND_INITIALIZER;
static pthread_cond_t job_not_full=PTHREAD_COND_INITIALIZER;
#endif

static int dequeue_job(void);

#ifdef _WIN32
static unsigned __stdcall worker_thread(void* arg) {
#else
static void* worker_thread(void* arg) {
#endif
	(void)arg;
	size_t buf_size=8192;
	char* buffer=malloc(buf_size);
	if(!buffer) {
		LOG_ERROR("Failed to allocate thread buffer");
#ifdef _WIN32
		return 0;
#else
		return NULL;
#endif
	}

	for(;;) {
		int c=dequeue_job();

		size_t total_read=0;
		int content_length=0;
		bool headers_done=false;
		char* headers_end=NULL;
		int broken=0;

		while(1) {
			if(total_read>=buf_size-1) {
				buf_size*=2;
				char* new_buf=realloc(buffer, buf_size);
				if(!new_buf) {
					LOG_ERROR("Failed to realloc request buffer");
					broken=1;
					break;
				}
				buffer=new_buf;
			}

			int r=recv(c, buffer+total_read, buf_size-total_read-1, 0);
			if(r<=0) {
				if(r==0) LOG_DEBUG("Client disconnected");
				else LOG_ERROR("recv error: %d", errno);
				broken=1;
				break;
			}

			total_read+=r;
			buffer[total_read]='\0';

			if(!headers_done) {
				headers_end=strstr(buffer, "\r\n\r\n");
				if(headers_end) {
					headers_done=true;
					char method[8]={ 0 };
					sscanf(buffer, "%7s", method);
					if(strcmp(method, "POST")==0) {
						char* cl=get_header_value(buffer, "Content-Length:");
						if(cl) content_length=atoi(cl);
					}
				}
			}

			if(headers_done) {
				size_t body_received=total_read-(headers_end-buffer+4);
				if(body_received>=(size_t)content_length) break;
			}
		}

		if(!broken&&total_read>0&&headers_done) {
			size_t headers_len=headers_end-buffer+4;
			if(headers_len>=2) buffer[headers_len-2]='\0';

			char* headers_copy=malloc(headers_len+1);
			if(!headers_copy) {
				LOG_ERROR("Failed to allocate headers copy");
				SOCKET_CLOSE(c);
				continue;
			}
			memcpy(headers_copy, buffer, headers_len);
			headers_copy[headers_len]='\0';

			char* body=NULL;
			if(content_length>0) {
				body=malloc(content_length+1);
				if(!body) {
					LOG_ERROR("Failed to allocate body buffer");
					free(headers_copy);
					SOCKET_CLOSE(c);
					continue;
				}
				memcpy(body, buffer+headers_len, content_length);
				body[content_length]='\0';
			}

			handle_single_request(c, headers_copy, body, headers_len, content_length, true);

			free(headers_copy);
			if(body) free(body);
		}

		SOCKET_CLOSE(c);
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
#ifdef _WIN32
	job_mutex=CreateMutex(NULL, FALSE, NULL);
	job_not_empty=CreateSemaphore(NULL, 0, QUEUE_CAP, NULL);
	job_not_full=CreateSemaphore(NULL, QUEUE_CAP, QUEUE_CAP, NULL);
	for(int i=0;i<nworkers;++i) {
		uintptr_t th=_beginthreadex(NULL, 0, worker_thread, NULL, 0, NULL);
		CloseHandle((HANDLE)th);
	}
#else
	for(int i=0;i<nworkers;++i) {
		pthread_t th;
		pthread_create(&th, NULL, worker_thread, NULL);
		pthread_detach(th);
	}
#endif
}

void enqueue_job(int client_socket) {
#ifdef _WIN32
	WaitForSingleObject(job_mutex, INFINITE);
	while(job_count==QUEUE_CAP) {
		LOG_WARN("Job queue is full, waiting...");
		ReleaseMutex(job_mutex);
		WaitForSingleObject(job_not_full, INFINITE);
		WaitForSingleObject(job_mutex, INFINITE);
	}
	job_ring[job_tail]=client_socket;
	job_tail=(job_tail+1)%QUEUE_CAP;
	job_count++;
	ReleaseMutex(job_mutex);
	ReleaseSemaphore(job_not_empty, 1, NULL);
#else
	pthread_mutex_lock(&job_mutex);
	while(job_count==QUEUE_CAP) {
		LOG_WARN("Job queue is full, waiting...");
		pthread_cond_wait(&job_not_full, &job_mutex);
	}
	job_ring[job_tail]=client_socket;
	job_tail=(job_tail+1)%QUEUE_CAP;
	job_count++;
	pthread_cond_signal(&job_not_empty);
	pthread_mutex_unlock(&job_mutex);
#endif
	LOG_DEBUG("Enqueued client socket %d", client_socket);
}

static int dequeue_job(void) {
#ifdef _WIN32
	WaitForSingleObject(job_not_empty, INFINITE);
	WaitForSingleObject(job_mutex, INFINITE);
	int c=job_ring[job_head];
	job_head=(job_head+1)%QUEUE_CAP;
	job_count--;
	ReleaseMutex(job_mutex);
	ReleaseSemaphore(job_not_full, 1, NULL);
#else
	pthread_mutex_lock(&job_mutex);
	while(job_count==0) {
		pthread_cond_wait(&job_not_empty, &job_mutex);
	}
	int c=job_ring[job_head];
	job_head=(job_head+1)%QUEUE_CAP;
	job_count--;
	pthread_cond_signal(&job_not_full);
	pthread_mutex_unlock(&job_mutex);
#endif
	LOG_DEBUG("Dequeued client socket %d", c);
	return c;
}
