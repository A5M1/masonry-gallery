#include "common.h"
#include "logging.h"
#include "directory.h"
#include "http.h"
#include "utils.h"
#include "api_handlers.h"
#include "server.h"
#include "thread_pool.h"
#include "config.h"
#include "thumbs.h"
#include "exception_handler.h"



#ifdef _WIN32
#define INIT_NETWORK() do { WSADATA w; WSAStartup(MAKEWORD(2, 2), &w); } while (0)
#define CLEANUP_NETWORK() do { WSACleanup(); } while (0)
#define SET_SOCKET_TIMEOUTS(sock) do { \
    DWORD rcv = KEEP_ALIVE_TIMEOUT_SEC * 1000, snd = 30000; \
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&rcv, sizeof(rcv)); \
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (const char*)&snd, sizeof(snd)); \
} while (0)
#define SET_KEEPALIVE(sock) do { \
    int ka = 1; \
    setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (const char*)&ka, sizeof(ka)); \
} while (0)
#define SET_NODELAY(sock) do { \
    int nd = 1; \
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (const char*)&nd, sizeof(nd)); \
} while (0)
#define SET_BUFFERS(sock) do { \
    int sz = 256 * 1024; \
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, (const char*)&sz, sizeof(sz)); \
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF, (const char*)&sz, sizeof(sz)); \
} while (0)
#else
#define INIT_NETWORK() do {} while (0)
#define CLEANUP_NETWORK() do {} while (0)
#define SET_SOCKET_TIMEOUTS(sock) do { \
    struct timeval rcv, snd; \
    rcv.tv_sec = KEEP_ALIVE_TIMEOUT_SEC; rcv.tv_usec = 0; \
    snd.tv_sec = 30; snd.tv_usec = 0; \
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &rcv, sizeof(rcv)); \
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &snd, sizeof(snd)); \
} while (0)
#define SET_KEEPALIVE(sock) do { \
    int ka = 1; \
    setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &ka, sizeof(ka)); \
    int idle = 60, intvl = 10, cnt = 3; \
    setsockopt(sock, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle)); \
    setsockopt(sock, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl)); \
    setsockopt(sock, IPPROTO_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt)); \
} while (0)
#define SET_NODELAY(sock) do { \
    int nd = 1; \
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd)); \
} while (0)
#define SET_BUFFERS(sock) do { \
    int sz = 256 * 1024; \
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &sz, sizeof(sz)); \
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &sz, sizeof(sz)); \
} while (0)
#endif

int main(int argc, char** argv) {
    log_init();
    install_exception_handlers();
    LOG_DEBUG("startup: installed exception handlers");
    LOG_DEBUG("startup: after log_init");
    INIT_NETWORK();
    LOG_DEBUG("startup: after INIT_NETWORK");
    derive_paths(argc > 0 ? argv[0] : NULL);
    LOG_DEBUG("startup: after derive_paths");
    load_config();
    LOG_DEBUG("startup: after load_config");
   
    LOG_INFO("Registering gallery folder watchers and starting thumbnail maintenance on startup...");
    LOG_DEBUG("startup: about to get_gallery_folders");
    {
        size_t count = 0;
        char** folders = get_gallery_folders(&count);
        LOG_INFO("startup: get_gallery_folders returned count=%zu", count);
        if (count == 0) {
            LOG_WARN("No gallery folders configured.");
        } else {
            LOG_INFO("Registering directory watcher for %zu folder(s)", count);
            for (size_t i = 0; i < count; ++i) {
                LOG_DEBUG("startup: registering watcher for folder[%zu]=%s", i, folders[i]);
                start_auto_thumb_watcher(folders[i]);
                LOG_DEBUG("startup: watcher registered for folder[%zu]", i);
            }
        }
    }

    LOG_DEBUG("startup: about to create_listen_socket");
    int port = 3000;
    int s = create_listen_socket(port);
    LOG_DEBUG("startup: create_listen_socket returned %d", s);
    if (s < 0) {
        LOG_ERROR("Failed to create listening socket on port %d", port);
        CLEANUP_NETWORK();
        return 1;
    }
    LOG_INFO("Gallery server running on http://localhost:%d", port);
    LOG_DEBUG("startup: about to start_thread_pool");
    start_thread_pool(0);
    LOG_DEBUG("startup: after start_thread_pool");
    int wait_ct = 0;
    for (;;) {
        struct sockaddr_in ca;
        socklen_t calen = sizeof(ca);
        if (wait_ct % 10 == 0)
            LOG_DEBUG("Waiting for a new client connection...");
        wait_ct++;
        int c = accept(s, (struct sockaddr*)&ca, &calen);
        if (c < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK)
                LOG_ERROR("Accept failed: %s", strerror(errno));
            continue;
        }
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(ca.sin_addr), ip, INET_ADDRSTRLEN);
        LOG_DEBUG("Accepted connection from %s:%d (socket %d)", ip, ntohs(ca.sin_port), c);
        SET_SOCKET_TIMEOUTS(c);
        SET_KEEPALIVE(c);
        SET_NODELAY(c);
        SET_BUFFERS(c);

        enqueue_job(c);
    }

    CLEANUP_NETWORK();
    return 0;
}
