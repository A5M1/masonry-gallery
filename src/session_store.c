#include "session_store.h"
#include "logging.h"
#include "common.h"
#include "thread_pool.h"
typedef struct session_node {
    char id[64];
    uint64_t last;
    struct session_node* next;
} session_node_t;

static session_node_t* sessions = NULL;
static thread_mutex_t sessions_mutex;

void session_store_init(void) {
    thread_mutex_init(&sessions_mutex);
}

static char* make_session_id(void) {
    static uint64_t counter = 0;
    uint64_t v = (uint64_t)time(NULL);
    v ^= (uint64_t)((uintptr_t)&v);
    v ^= (uint64_t)++counter;
    char* s = malloc(64);
    if (!s) {
        LOG_ERROR("Failed to allocate session ID string");
        return NULL;
    }
    snprintf(s, 64, "s%016llx%08x", (unsigned long long)v, (unsigned int)rand());
    return s;
}

char* session_create(void) {
    char* id = make_session_id();
    if (!id) return NULL;
    session_node_t* n = malloc(sizeof(session_node_t));
    if (!n) {
        LOG_ERROR("Failed to allocate session node structure");
        free(id);
        return NULL;
    }
    strncpy(n->id, id, sizeof(n->id)-1); n->id[sizeof(n->id)-1] = '\0';
    n->last = 0;
    thread_mutex_lock(&sessions_mutex);
    n->next = sessions; sessions = n;
    thread_mutex_unlock(&sessions_mutex);
    return id;
}

uint64_t session_get_last(const char* session_id) {
    if (!session_id) return 0;
    uint64_t out = 0;
    thread_mutex_lock(&sessions_mutex);
    session_node_t* cur = sessions;
    while (cur) {
        if (strcmp(cur->id, session_id) == 0) { out = cur->last; break; }
        cur = cur->next;
    }
    thread_mutex_unlock(&sessions_mutex);
    return out;
}

void session_set_last(const char* session_id, uint64_t last) {
    if (!session_id) return;
    thread_mutex_lock(&sessions_mutex);
    session_node_t* cur = sessions;
    while (cur) {
        if (strcmp(cur->id, session_id) == 0) { cur->last = last; break; }
        cur = cur->next;
    }
    thread_mutex_unlock(&sessions_mutex);
}

void session_clear(const char* session_id) {
    if (!session_id) return;
    thread_mutex_lock(&sessions_mutex);
    session_node_t* cur = sessions;
    while (cur) {
        if (strcmp(cur->id, session_id) == 0) { cur->last = 0; break; }
        cur = cur->next;
    }
    thread_mutex_unlock(&sessions_mutex);
}

void session_store_shutdown(void) {
    thread_mutex_lock(&sessions_mutex);
    session_node_t* cur = sessions;
    while (cur) {
        session_node_t* nxt = cur->next;
        free(cur);
        cur = nxt;
    }
    sessions = NULL;
    thread_mutex_unlock(&sessions_mutex);
    thread_mutex_destroy(&sessions_mutex);
}
