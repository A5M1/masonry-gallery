#include "common.h"
#include "logging.h"
#include "directory.h"
#include "http.h"
#include "utils.h"
#include "api_handlers.h"
#include "server.h"
#include "thread_pool.h"
#include "config.h" 

int main() {
	log_init();
#ifdef _WIN32
	WSADATA w;
	WSAStartup(MAKEWORD(2, 2), &w);
#endif
	derive_paths();
	load_config();

	LOG_INFO("Checking for media files in: %s", BASE_DIR);
	if(has_media_rec(BASE_DIR)) {
	}
	else {
		LOG_WARN("No media files found in base directory.");
	}
	int port=3000;
	int s=create_listen_socket(port);
	LOG_INFO("Gallery server running on http://localhost:%d", port);
	start_thread_pool(0);
	int connection_wait_counter=0;

	for(;;) {
		struct sockaddr_in ca;
		socklen_t calen=sizeof(ca);
		if(connection_wait_counter%10==0) {
			LOG_INFO("Waiting for a new client connection...");
		}
		connection_wait_counter++;
		int c=accept(s, (struct sockaddr*)&ca, &calen);
		if(c<0) {
			if(errno!=EAGAIN&&errno!=EWOULDBLOCK) {
				LOG_ERROR("Accept failed: %s", strerror(errno));
			}
			continue;
		}

		char ip_str[INET_ADDRSTRLEN];
		inet_ntop(AF_INET, &(ca.sin_addr), ip_str, INET_ADDRSTRLEN);
		LOG_INFO("Accepted connection from %s:%d (socket %d)", ip_str, ntohs(ca.sin_port), c);
#ifdef _WIN32
		DWORD timeout=KEEP_ALIVE_TIMEOUT_SEC*1000;
		setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));
		timeout=30000;
		setsockopt(c, SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeout, sizeof(timeout));
#else
		struct timeval timeout;
		timeout.tv_sec=KEEP_ALIVE_TIMEOUT_SEC;
		timeout.tv_usec=0;
		setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
		timeout.tv_sec=30;
		timeout.tv_usec=0;
		setsockopt(c, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#endif

		int keepalive=1;
		setsockopt(c, SOL_SOCKET, SO_KEEPALIVE,
#ifdef _WIN32
		(const char*)&keepalive,
#else
			&keepalive,
#endif
			sizeof(keepalive));

#ifndef _WIN32
#ifdef __linux__
		int keepidle=60;
		int keepintvl=10;
		int keepcnt=3;
		setsockopt(c, IPPROTO_TCP, TCP_KEEPIDLE, &keepidle, sizeof(keepidle));
		setsockopt(c, IPPROTO_TCP, TCP_KEEPINTVL, &keepintvl, sizeof(keepintvl));
		setsockopt(c, IPPROTO_TCP, TCP_KEEPCNT, &keepcnt, sizeof(keepcnt));
#endif
#endif

		int nodelay=1;
		setsockopt(c, IPPROTO_TCP, TCP_NODELAY,
#ifdef _WIN32
		(const char*)&nodelay,
#else
			&nodelay,
#endif
			sizeof(nodelay));

		int buf_size=256*1024;
		setsockopt(c, SOL_SOCKET, SO_SNDBUF,
#ifdef _WIN32
		(const char*)&buf_size,
#else
			&buf_size,
#endif
			sizeof(buf_size));
		setsockopt(c, SOL_SOCKET, SO_RCVBUF,
#ifdef _WIN32
		(const char*)&buf_size,
#else
			&buf_size,
#endif
			sizeof(buf_size));

#ifdef _WIN32
		enqueue_job(c);
#else
		pthread_mutex_lock(&job_mutex);
		if(job_count==QUEUE_CAP) {
			pthread_mutex_unlock(&job_mutex);
			LOG_WARN("Connection refused, job queue is full.");
			const char* msg=
				"HTTP/1.1 503 Service Unavailable\r\n"
				"Connection: close\r\n"
				"Content-Length: 19\r\n\r\n"
				"Service Unavailable";
			send(c, msg, (int)strlen(msg), 0);
			close(c);
		}
		else {
			pthread_mutex_unlock(&job_mutex);
			enqueue_job(c);
		}
#endif
	}

#ifdef _WIN32
	WSACleanup();
#endif
	return 0;
}