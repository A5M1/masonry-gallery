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
#include "platform.h"
#include "websocket.h"

int main(int argc, char** argv) {
    log_init();
    install_exception_handlers();
    LOG_DEBUG("startup: installed exception handlers");
    LOG_DEBUG("startup: after log_init");
    platform_init_network();
    LOG_DEBUG("startup: after INIT_NETWORK");
    websocket_init();
    derive_paths(argc > 0 ? argv[0] : NULL);
    LOG_DEBUG("startup: after derive_paths");
    load_config();
    LOG_DEBUG("startup: after load_config");
    if (platform_maximize_window() == 0) {
        LOG_DEBUG("startup: platform_maximize_window succeeded");
    } else {
        LOG_DEBUG("startup: platform_maximize_window not available or failed");
    }
    LOG_DEBUG("Registering gallery folder watchers and starting thumbnail maintenance on startup...");
    LOG_DEBUG("startup: about to get_gallery_folders");
    {
        size_t count = 0;
        char** folders = get_gallery_folders(&count);
        LOG_DEBUG("startup: get_gallery_folders returned count=%zu", count);
        if (count == 0) {
            LOG_WARN("No gallery folders configured.");
        } else {
            LOG_DEBUG("Registering directory watcher for %zu folder(s)", count);
            for (size_t i = 0; i < count; ++i) {
                LOG_DEBUG("startup: registering watcher for folder[%zu]=%s", i, folders[i]);
                start_auto_thumb_watcher(folders[i]);
                LOG_DEBUG("startup: watcher registered for folder[%zu]", i);
            }
        }
    }
    LOG_DEBUG("Scanning for missing thumbnails on startup...");
    scan_and_generate_missing_thumbs();
    LOG_DEBUG("startup: about to create_listen_socket");
    int port = 3000;
    int s = create_listen_socket(port);
    LOG_DEBUG("startup: create_listen_socket returned %d", s);
    if (s < 0) {
        LOG_ERROR("Failed to create listening socket on port %d", port);
        platform_cleanup_network();
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
        platform_set_socket_options(c);
        enqueue_job(c);
    }
    platform_cleanup_network();
    return 0;
}
