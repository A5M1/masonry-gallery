#include "websocket.h"
#include "thread_pool.h"
#include "crypto.h"
#include "http.h"
#include "logging.h"
#include "tinyjson_combined.h"
#include "session_store.h"
#include "utils.h"
#include "directory.h"
#include "platform.h"

#define MAX_WS_CLIENTS 256

typedef struct {
    int sock;
    char topic[PATH_MAX];
    char session_id[64];
    uint64_t last_sent_id;
} ws_client_t;

static ws_client_t ws_clients[MAX_WS_CLIENTS];
static int ws_count = 0;
static thread_mutex_t ws_mutex;

static inline void ws_lock(void) { thread_mutex_lock(&ws_mutex); }
static inline void ws_unlock(void) { thread_mutex_unlock(&ws_mutex); }

static int add_client(int s) {
    ws_lock();
    if (ws_count >= MAX_WS_CLIENTS) {
        ws_unlock();
        return -1;
    }

    for (int i = 0; i < MAX_WS_CLIENTS; ++i) {
        if (ws_clients[i].sock == -1) {
            ws_clients[i].sock = s;
            ws_clients[i].topic[0] = '\0';
            ws_clients[i].session_id[0] = '\0';
            ws_clients[i].last_sent_id = 0;
            ws_count++;
            ws_unlock();
            return 0;
        }
    }
    ws_unlock();
    return -1;
}

static void remove_client_socket(int s) {
    ws_lock();
    for (int i = 0; i < MAX_WS_CLIENTS; ++i) {
        if (ws_clients[i].sock == s) {
            ws_clients[i].sock = -1;
            ws_clients[i].topic[0] = '\0';
            ws_count--;
            break;
        }
    }
    ws_unlock();
}

static int send_ws_frame_opcode(int s, int opcode, const unsigned char* data, size_t len) {
    unsigned char header[10];
    size_t header_len;

    header[0] = 0x80 | (opcode & 0x0F);
    if (len <= 125) {
        header[1] = (unsigned char)len;
        header_len = 2;
    }
    else if (len <= 65535) {
        header[1] = 126;
        header[2] = (len >> 8) & 0xFF;
        header[3] = len & 0xFF;
        header_len = 4;
    }
    else {
        header[1] = 127;
        for (int i = 0; i < 8; ++i)
            header[2 + i] = (len >> (56 - 8 * i)) & 0xFF;
        header_len = 10;
    }

    if (send(s, (const char*)header, (int)header_len, 0) != (int)header_len)
        return -1;
    if (len && send(s, (const char*)data, (int)len, 0) != (int)len)
        return -1;

    return 0;
}

static void ws_update_topic(int c, const char* msg) {
    const char* pathp = strstr(msg, "\"path\"");
    if (!pathp) return;

    const char* colon = strchr(pathp, ':');
    if (!colon) return;

    const char* val = colon + 1;
    while (*val && isspace((unsigned char)*val)) val++;

    size_t l = 0;
    const char* start = val;
    if (*val == '"') {
        start = val + 1;
        const char* end = strchr(start, '"');
        if (!end) return;
        l = (size_t)(end - start);
    }
    else {
        const char* end = start;
        while (*end && *end != ',' && *end != '}' && !isspace((unsigned char)*end)) end++;
        l = (size_t)(end - start);
    }
    if (l >= PATH_MAX) l = PATH_MAX - 1;

    char topic[PATH_MAX];
    if (l > 0) memcpy(topic, start, l);
    topic[l] = '\0';

    ws_lock();
    for (int i = 0; i < MAX_WS_CLIENTS; ++i) {
        if (ws_clients[i].sock == c) {
            strncpy(ws_clients[i].topic, topic, PATH_MAX - 1);
            ws_clients[i].topic[PATH_MAX - 1] = '\0';
            break;
        }
    }
    ws_unlock();
}

static void* websocket_client_thread(void* arg) {
    int c = *(int*)arg;
    free(arg);

    unsigned char* accum = NULL;
    size_t accum_len = 0;
    int expect_opcode = -1;
    unsigned char small_buf[4096];

    for (;;) {
        unsigned char hdr[2];
        int r = recv(c, (char*)hdr, 2, 0);
        if (r <= 0) break;

        int fin = (hdr[0] & 0x80) != 0;
        int opcode = hdr[0] & 0x0F;
        int masked = (hdr[1] & 0x80) != 0;
        uint64_t payload_len = hdr[1] & 0x7F;

        if (payload_len == 126) {
            unsigned char ext[2];
            if (recv(c, (char*)ext, 2, 0) != 2) break;
            payload_len = ((uint64_t)ext[0] << 8) | ext[1];
        }
        else if (payload_len == 127) {
            unsigned char ext[8];
            if (recv(c, (char*)ext, 8, 0) != 8) break;
            payload_len = 0;
            for (int i = 0; i < 8; ++i)
                payload_len = (payload_len << 8) | ext[i];
        }

        unsigned char mask[4] = { 0 };
        if (masked && recv(c, (char*)mask, 4, 0) != 4) break;

        unsigned char* payload = (payload_len > sizeof(small_buf))
            ? malloc(payload_len + 1)
            : small_buf;
        if (!payload) break;

        size_t got = 0;
        while (got < payload_len) {
            int rc = recv(c, (char*)payload + got, (int)(payload_len - got), 0);
            if (rc <= 0) { if (payload != small_buf) free(payload); goto exit_loop; }
            got += rc;
        }

        if (masked) {
            for (size_t i = 0; i < payload_len; ++i)
                payload[i] ^= mask[i % 4];
        }

        if (opcode == 0x8) { // close
            if (payload != small_buf) free(payload);
            break;
        }

        if (opcode == 0x9) { // ping
            send_ws_frame_opcode(c, 0xA, payload, payload_len);
            if (payload != small_buf) free(payload);
            continue;
        }

        if (opcode == 0x1 || (opcode == 0x0 && expect_opcode == 0x1)) {
            unsigned char* newbuf = realloc(accum, accum_len + payload_len + 1);
            if (!newbuf) { if (payload != small_buf) free(payload); break; }

            accum = newbuf;
            memcpy(accum + accum_len, payload, payload_len);
            accum_len += payload_len;
            accum[accum_len] = '\0';

            if (payload != small_buf) free(payload);

            if (fin) {
                if (strstr((char*)accum, "subscribe")) {
                    ws_update_topic(c, (char*)accum);
                    const char* sstart = strstr((char*)accum, "\"session\"");
                    if (!sstart) sstart = strstr((char*)accum, "\"session_id\"");
                    if (sstart) {
                        const char* colon = strchr(sstart, ':');
                        if (colon) {
                            const char* v = colon + 1;
                            while (*v && isspace((unsigned char)*v)) v++;
                            if (*v == '"') {
                                v++;
                                const char* e = strchr(v, '"');
                                if (e) {
                                    size_t L = (size_t)(e - v);
                                    char sid[64]; if (L >= sizeof(sid)) L = sizeof(sid) - 1; memcpy(sid, v, L); sid[L] = '\0';
                                    ws_lock();
                                    for (int i = 0; i < MAX_WS_CLIENTS; ++i) {
                                        if (ws_clients[i].sock == c) {
                                            strncpy(ws_clients[i].session_id, sid, sizeof(ws_clients[i].session_id) - 1);
                                            ws_clients[i].session_id[sizeof(ws_clients[i].session_id) - 1] = '\0';
                                            ws_clients[i].last_sent_id = session_get_last(sid);
                                            break;
                                        }
                                    }
                                    ws_unlock();
                                }
                            }
                        }
                    }
                }

                if (strstr((char*)accum, "\"action\"") && strstr((char*)accum, "addFolder")) {
                    const char* body = (char*)accum;
                    const char* n_start = strstr(body, "\"name\":");
                    if (n_start) {
                        n_start = strchr(n_start, ':');
                        if (n_start) {
                            n_start++;
                            while (*n_start && isspace((unsigned char)*n_start)) n_start++;
                            if (*n_start == '"') {
                                n_start++;
                                const char* n_end = strchr(n_start, '"');
                                if (n_end) {
                                    char folder[PATH_MAX] = { 0 };
                                    size_t nlen = (size_t)(n_end - n_start);
                                    if (nlen >= sizeof(folder)) nlen = sizeof(folder) - 1;
                                    memcpy(folder, n_start, nlen);
                                    folder[nlen] = '\0';
                                    url_decode(folder);

                                    char target[PATH_MAX] = { 0 };
                                    const char* t_start = strstr(body, "\"target\":");
                                    if (t_start) {
                                        t_start = strchr(t_start, ':');
                                        if (t_start) {
                                            t_start++;
                                            while (*t_start && isspace((unsigned char)*t_start)) t_start++;
                                            if (*t_start == '"') {
                                                t_start++;
                                                const char* t_end = strchr(t_start, '"');
                                                if (t_end) {
                                                    size_t tlen = (size_t)(t_end - t_start);
                                                    if (tlen >= sizeof(target)) tlen = sizeof(target) - 1;
                                                    memcpy(target, t_start, tlen);
                                                    target[tlen] = '\0';
                                                    url_decode(target);
                                                }
                                            }
                                        }
                                    }

                                    char target_copy[PATH_MAX]; strncpy(target_copy, target, sizeof(target_copy) - 1); target_copy[sizeof(target_copy) - 1] = '\0';
                                    while (*target_copy == '/' || *target_copy == '\\') memmove(target_copy, target_copy + 1, strlen(target_copy));
                                    char dest[PATH_MAX];
                                    if (strlen(target_copy) > 0) snprintf(dest, sizeof(dest), "%s%s%s%s%s", BASE_DIR, DIR_SEP_STR, target_copy, DIR_SEP_STR, folder);
                                    else snprintf(dest, sizeof(dest), "%s%s%s", BASE_DIR, DIR_SEP_STR, folder);
                                    normalize_path(dest);
                                    mk_dir(dest);
                                    if (is_dir(dest)) {
                                        char fg_path[PATH_MAX]; path_join(fg_path, dest, ".fg");
                                        FILE* fgf = fopen(fg_path, "wb"); if (fgf) fclose(fgf);
                                        char msg[1024]; int rr = snprintf(msg, sizeof(msg), "{\"type\":\"folderAdded\",\"path\":\"%s\"}", dest);
                                        if (rr > 0) websocket_broadcast(msg);
                                    }
                                    else {
                                        char emsg[256]; int rr = snprintf(emsg, sizeof(emsg), "{\"type\":\"folderAdded\",\"error\":\"mkdir failed\"}"); (void)rr;
                                        websocket_broadcast(emsg);
                                    }
                                }
                            }
                        }
                    }
                }

                free(accum);
                accum = NULL;
                accum_len = 0;
                expect_opcode = -1;
            }
            else {
                expect_opcode = opcode;
            }
            continue;
        }

        if (payload != small_buf) free(payload);
    }

exit_loop:
    if (accum) free(accum);
    remove_client_socket(c);
    SOCKET_CLOSE(c);
    return NULL;
}
int websocket_init(void) {
    for (int i = 0; i < MAX_WS_CLIENTS; ++i) {
        ws_clients[i].sock = -1;
        ws_clients[i].topic[0] = '\0';
    }
    ws_count = 0;
    thread_mutex_init(&ws_mutex);
    session_store_init();
    return 0;
}

int websocket_register_socket(int client_socket, char* request_headers) {
    if (!request_headers) {
        LOG_WARN("websocket_register_socket called without headers for client_socket=%d", client_socket);
        return 0;
    }

    char* key = get_header_value(request_headers, "Sec-WebSocket-Key:");
    int key_alloc = 0;
    if (key) key_alloc = 1;
    if (!key) {
        char* p = request_headers;
        while (*p) {
            if (!strncasecmp(p, "Sec-WebSocket-Key:", 18)) {
                char* v = p + 18;
                while (*v && isspace((unsigned char)*v)) v++;
                char* e = v;
                while (*e && *e != '\r' && *e != '\n') e++;
                static char inline_key[256];
                size_t len = (size_t)(e - v);
                if (len >= sizeof(inline_key)) len = sizeof(inline_key) - 1;
                memcpy(inline_key, v, len);
                inline_key[len] = '\0';
                key = inline_key;
                key_alloc = 0;
                break;
            }
            char* nl = strchr(p, '\n');
            if (!nl) break;
            p = nl + 1;
        }
    }

    if (!key || !*key) {
        LOG_WARN("WebSocket register: Sec-WebSocket-Key not found in provided headers");
        if (key_alloc) free(key);
        return 0;
    }

    LOG_DEBUG("WebSocket handshake received, Sec-WebSocket-Key=%.64s", key);

    const char* guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    size_t total = strlen(key) + strlen(guid);

    char* combined = malloc(total + 1);
    if (!combined) {
        if (key_alloc) free(key);
        return 0;
    }

    sprintf(combined, "%s%s", key, guid);
    if (key_alloc) { free(key); key = NULL; key_alloc = 0; }

    uint8_t sha[20];
    if (crypto_sha1(combined, total, sha) != 0) {
        free(combined);
        return 0;
    }
    free(combined);

    size_t enc_len = crypto_base64_encode_len(20);
    char* accept = malloc(enc_len + 1);
    if (!accept) return 0;
    crypto_base64_encode(sha, 20, accept, enc_len + 1);

    char resp[512];
    int n = snprintf(resp, sizeof(resp),
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: %s\r\n\r\n",
        accept);
    free(accept);

    if (send(client_socket, resp, n, 0) != n) return 0;
    if (add_client(client_socket) != 0) {
        SOCKET_CLOSE(client_socket);
        return 0;
    }

    LOG_DEBUG("WebSocket handshake accepted, client_socket=%d", client_socket);

    int* arg = malloc(sizeof(int));
    if (!arg) { remove_client_socket(client_socket); SOCKET_CLOSE(client_socket); return 0; }
    *arg = client_socket;
    thread_create_detached(websocket_client_thread, arg);

    char* session = session_create();
    if (!session) session = strdup("");
    char jb[256];
    size_t rem = sizeof(jb);
    char* p = jb;
    p = json_objOpen(p, NULL, &rem);
    p = json_str(p, "type", "welcome", &rem);
    p = json_str(p, "message", "connected", &rem);
    if (session && session[0]) p = json_str(p, "session", session, &rem);
    p = json_objClose(p, &rem);
    send_ws_frame_opcode(client_socket, 0x1, (const unsigned char*)jb, p - jb);

    if (session) free(session);

    return 1;
}

void websocket_broadcast_topic(const char* topic, const char* msg) {
    if (!msg) return;

    static _Atomic(uint64_t)g_msg_id = 0;
    uint64_t id = atomic_fetch_add(&g_msg_id, 1) + 1;

    char wrapped[4096];
    const char* s = msg;
    size_t len = 0;
    while (*s && isspace((unsigned char)*s)) s++;

    if (*s == '{') {
        const char* e = msg + strlen(msg) - 1;
        while (e > s && isspace((unsigned char)*e)) e--;

        if (*e == '}') {
            size_t inner_len = (size_t)(e - s - 0);
            if (inner_len + 128 < sizeof(wrapped)) {
                size_t off = snprintf(wrapped, sizeof(wrapped), "{\"id\":%llu,", (unsigned long long)id);
                size_t copy_len = (size_t)(e - s - 0);
                if (copy_len > 1) {
                    memcpy(wrapped + off, s + 1, copy_len - 1);
                    off += copy_len - 1;
                }
                wrapped[off++] = '}';
                wrapped[off] = '\0';
                len = off;
                s = wrapped;
            }
        }
    }

    if (len == 0) len = strlen(s);

    ws_lock();

    for (int i = 0; i < MAX_WS_CLIENTS; ++i) {
        int sock = ws_clients[i].sock;
        if (sock == -1) continue;
        if (topic && ws_clients[i].topic[0] && !strstr(ws_clients[i].topic, topic) && !strstr(topic, ws_clients[i].topic)) continue;


        if (ws_clients[i].last_sent_id >= id) continue;

        if (send_ws_frame_opcode(sock, 0x1, (const unsigned char*)s, len) != 0) {
            SOCKET_CLOSE(sock);
            ws_clients[i].sock = -1;
            ws_clients[i].topic[0] = '\0';
            ws_clients[i].session_id[0] = '\0';
            ws_clients[i].last_sent_id = 0;
            ws_count--;
        }
        else {
            ws_clients[i].last_sent_id = id;
            if (ws_clients[i].session_id[0]) session_set_last(ws_clients[i].session_id, id);
        }
    }

    ws_unlock();
}
void websocket_broadcast(const char* msg) {
    websocket_broadcast_topic(NULL, msg);
}

void websocket_shutdown(void) {
    ws_lock();
    for (int i = 0; i < MAX_WS_CLIENTS; ++i) {
        int s = ws_clients[i].sock;
        if (s != -1) {
            SOCKET_CLOSE(s);
            ws_clients[i].sock = -1;
            ws_clients[i].topic[0] = '\0';
        }
    }
    ws_count = 0;
    ws_unlock();
    thread_mutex_destroy(&ws_mutex);
}
