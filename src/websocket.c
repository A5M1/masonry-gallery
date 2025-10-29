#include "websocket.h"
#include "thread_pool.h"
#include "crypto.h"
#include "http.h"
#include "logging.h"
#include "tinyjson_combined.h"

#define MAX_WS_CLIENTS 256

typedef struct {
    int sock;
    char topic[PATH_MAX];
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

    const char* q = strchr(pathp, '"');
    if (!q || !(q = strchr(q + 1, '"'))) return;
    const char* q2 = strchr(q + 1, '"');
    if (!q2) return;

    size_t l = (size_t)(q2 - (q + 1));
    if (l >= PATH_MAX) l = PATH_MAX - 1;

    char topic[PATH_MAX];
    memcpy(topic, q + 1, l);
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
                if (strstr((char*)accum, "subscribe"))
                    ws_update_topic(c, (char*)accum);

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
    return 0;
}

int websocket_register_socket(int client_socket, char* request_headers) {
    char* key = get_header_value(request_headers, "Sec-WebSocket-Key:");
    if (!key) return 0;

    const char* guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    size_t total = strlen(key) + strlen(guid);

    char* combined = malloc(total + 1);
    if (!combined) return 0;

    sprintf(combined, "%s%s", key, guid);

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

    int* arg = malloc(sizeof(int));
    if (!arg) { remove_client_socket(client_socket); SOCKET_CLOSE(client_socket); return 0; }
    *arg = client_socket;
    thread_create_detached(websocket_client_thread, arg);

    char jb[128];
    size_t rem = sizeof(jb);
    char* p = jb;
    p = json_objOpen(p, NULL, &rem);
    p = json_str(p, "type", "welcome", &rem);
    p = json_str(p, "message", "connected", &rem);
    p = json_objClose(p, &rem);
    send_ws_frame_opcode(client_socket, 0x1, (const unsigned char*)jb, p - jb);

    return 1;
}

void websocket_broadcast_topic(const char* topic, const char* msg) {
    if (!msg) return;

    size_t len = strlen(msg);
    ws_lock();

    for (int i = 0; i < MAX_WS_CLIENTS; ++i) {
        int s = ws_clients[i].sock;
        if (s == -1) continue;

        if (topic && ws_clients[i].topic[0]) {
            if (!strstr(ws_clients[i].topic, topic) && !strstr(topic, ws_clients[i].topic))
                continue;
        }

        if (send_ws_frame_opcode(s, 0x1, (const unsigned char*)msg, len) != 0) {
            SOCKET_CLOSE(s);
            ws_clients[i].sock = -1;
            ws_clients[i].topic[0] = '\0';
            ws_count--;
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
