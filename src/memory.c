#include "memory.h"
#include "logging.h"
#include "platform.h"
#include "thread_pool.h"

#define MEM_BLOCK_MAGIC 0x4D454D424C4F434BULL
#define MEM_TAIL_MAGIC 0x5441494C4D454D4DULL
#define MEM_BLOCK_LIVE 0x4C495645U
#define MEM_BLOCK_QUARANTINED 0x51554E54U
#define MEM_MAX_QUARANTINE_BLOCKS 1024U
#define MEM_MAX_QUARANTINE_BYTES (16U * 1024U * 1024U)
#define MEM_TLS_DEPTH 16
#define MEM_POISON_BYTE 0xDD
#define MEM_DEFAULT_TAG "untagged"

#if defined(_MSC_VER)
#define MEM_THREAD_LOCAL __declspec(thread)
#else
#define MEM_THREAD_LOCAL _Thread_local
#endif

typedef struct mem_block mem_block_t;

struct mem_block {
    uint64_t magic;
    uint64_t tail;
    size_t size;
    mem_scope_t* scope;
    const char* tag;
    unsigned long thread_id;
    mem_block_t* prev;
    mem_block_t* next;
    uint32_t state;
    uint32_t canary;
};

struct mem_scope {
    uint64_t id;
    char name[64];
    mem_scope_t* parent;
    mem_scope_t* first_child;
    mem_scope_t* next_sibling;
    mem_scope_t* registry_next;
    mem_block_t* blocks;
    size_t live_blocks;
    size_t live_bytes;
    thread_mutex_t lock;
    int lock_ready;
    int destroyed;
};

typedef struct {
    mem_scope_t* stack[MEM_TLS_DEPTH];
    int depth;
} mem_thread_state_t;

static MEM_THREAD_LOCAL mem_thread_state_t g_thread_state;

static thread_mutex_t g_registry_lock;
static int g_registry_ready;
static int g_inited;
static mem_scope_t* g_global_scope;
static mem_scope_t* g_scopes;
static uint64_t g_next_scope_id;

static mem_block_t* g_quarantine_head;
static mem_block_t* g_quarantine_tail;
static size_t g_quarantine_blocks;
static size_t g_quarantine_bytes;

static _Atomic size_t g_live_blocks;
static _Atomic size_t g_live_bytes;
static _Atomic size_t g_total_blocks;
static _Atomic size_t g_total_bytes;
static _Atomic size_t g_scope_count;

#define MEM_ALIGNMENT _Alignof(max_align_t)
#define MEM_HEADER_SIZE ((sizeof(mem_block_t) + MEM_ALIGNMENT - 1U) & ~(MEM_ALIGNMENT - 1U))
#define MEM_TRAILER_SIZE sizeof(uint64_t)

static uint32_t mem_canary(const mem_block_t* block) {
    uintptr_t value = (uintptr_t)block ^ (uintptr_t)block->size ^ (uintptr_t)MEM_BLOCK_MAGIC;
    return (uint32_t)(value ^ (value >> 32));
}

static void mem_write_tail(mem_block_t* block) {
    uint64_t tail = MEM_TAIL_MAGIC ^ (uint64_t)block->size;
    memcpy((char*)block + MEM_HEADER_SIZE + block->size, &tail, sizeof(tail));
}

static int mem_tail_ok(const mem_block_t* block) {
    uint64_t tail = 0;
    memcpy(&tail, (const char*)block + MEM_HEADER_SIZE + block->size, sizeof(tail));
    return tail == (MEM_TAIL_MAGIC ^ (uint64_t)block->size);
}

static int mem_block_ok(const mem_block_t* block, const char* op) {
    if (!block || block->magic != MEM_BLOCK_MAGIC) {
        fprintf(stderr, "memory: %s received an untracked pointer\n", op);
        return 0;
    }
    if (block->canary != mem_canary(block)) {
        fprintf(stderr, "memory: %s detected a corrupted block header\n", op);
        return 0;
    }
    if (!mem_tail_ok(block)) {
        fprintf(stderr, "memory: %s detected a buffer overrun for block %p (%zu bytes)\n", op, (void*)block, block->size);
        return 0;
    }
    return 1;
}

static void mem_stats_add_block(size_t size) {
    atomic_fetch_add_explicit(&g_live_blocks, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_live_bytes, size, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_total_blocks, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_total_bytes, size, memory_order_relaxed);
}

static void mem_stats_remove_block(size_t size) {
    atomic_fetch_sub_explicit(&g_live_blocks, 1, memory_order_relaxed);
    atomic_fetch_sub_explicit(&g_live_bytes, size, memory_order_relaxed);
}

static void mem_tls_forget_scope(mem_scope_t* scope) {
    for (int i = 0; i < g_thread_state.depth; ++i) {
        if (g_thread_state.stack[i] == scope) {
            for (int j = i; j < g_thread_state.depth - 1; ++j) {
                g_thread_state.stack[j] = g_thread_state.stack[j + 1];
            }
            g_thread_state.depth--;
            i--;
        }
    }
}

int mem_init(void) {
    if (g_inited) return 0;
    if (thread_mutex_init(&g_registry_lock) != 0) {
        fprintf(stderr, "memory: failed to initialize registry lock\n");
        return -1;
    }
    g_registry_ready = 1;
    g_inited = 1;
    g_global_scope = NULL;
    g_scopes = NULL;
    g_quarantine_head = NULL;
    g_quarantine_tail = NULL;
    g_quarantine_blocks = 0;
    g_quarantine_bytes = 0;
    g_next_scope_id = 0;
    atomic_store_explicit(&g_live_blocks, 0, memory_order_relaxed);
    atomic_store_explicit(&g_live_bytes, 0, memory_order_relaxed);
    atomic_store_explicit(&g_total_blocks, 0, memory_order_relaxed);
    atomic_store_explicit(&g_total_bytes, 0, memory_order_relaxed);
    atomic_store_explicit(&g_scope_count, 0, memory_order_relaxed);
    g_thread_state.depth = 0;

    mem_scope_t* global = mem_scope_create("global", NULL);
    if (!global) {
        g_inited = 0;
        g_registry_ready = 0;
        thread_mutex_destroy(&g_registry_lock);
        return -1;
    }
    g_global_scope = global;
    return 0;
}

void mem_shutdown(void) {
    if (!g_inited) return;
    mem_report_leaks("shutdown");
    for (;;) {
        thread_mutex_lock(&g_registry_lock);
        mem_scope_t* scope = g_scopes;
        thread_mutex_unlock(&g_registry_lock);
        if (!scope) break;
        mem_scope_destroy(scope);
    }
    mem_purge_quarantine();
    if (g_registry_ready) {
        thread_mutex_destroy(&g_registry_lock);
        g_registry_ready = 0;
    }
    g_global_scope = NULL;
    g_inited = 0;
    g_thread_state.depth = 0;
}

int mem_is_initialized(void) {
    return g_inited;
}

mem_scope_t* mem_global_scope(void) {
    return g_global_scope;
}

mem_scope_t* mem_scope_create(const char* name, mem_scope_t* parent) {
    if (!g_inited) return NULL;
    mem_scope_t* scope = calloc(1, sizeof(mem_scope_t));
    if (!scope) {
        fprintf(stderr, "memory: failed to allocate scope\n");
        return NULL;
    }
    if (thread_mutex_init(&scope->lock) != 0) {
        fprintf(stderr, "memory: failed to initialize scope lock\n");
        free(scope);
        return NULL;
    }
    scope->lock_ready = 1;
    if (name && name[0]) {
        strncpy(scope->name, name, sizeof(scope->name) - 1);
    } else {
        strncpy(scope->name, "scope", sizeof(scope->name) - 1);
    }
    if (!parent) parent = g_global_scope;

    thread_mutex_lock(&g_registry_lock);
    if (parent && parent->destroyed) {
        thread_mutex_unlock(&g_registry_lock);
        thread_mutex_destroy(&scope->lock);
        free(scope);
        return NULL;
    }
    scope->id = ++g_next_scope_id;
    scope->parent = parent;
    if (parent) {
        scope->next_sibling = parent->first_child;
        parent->first_child = scope;
    }
    if (g_scopes) {
        mem_scope_t* tail = g_scopes;
        while (tail->registry_next) tail = tail->registry_next;
        tail->registry_next = scope;
    } else {
        g_scopes = scope;
    }
    thread_mutex_unlock(&g_registry_lock);
    atomic_fetch_add_explicit(&g_scope_count, 1, memory_order_relaxed);
    return scope;
}

void mem_scope_destroy(mem_scope_t* scope) {
    if (!scope || !g_inited) return;
    thread_mutex_lock(&g_registry_lock);
    if (scope->destroyed) {
        thread_mutex_unlock(&g_registry_lock);
        return;
    }
    mem_scope_t** link = &g_scopes;
    while (*link && *link != scope) link = &(*link)->registry_next;
    if (*link == scope) *link = scope->registry_next;
    if (scope->parent) {
        mem_scope_t** child_link = &scope->parent->first_child;
        while (*child_link && *child_link != scope) child_link = &(*child_link)->next_sibling;
        if (*child_link == scope) *child_link = scope->next_sibling;
    }
    scope->destroyed = 1;
    if (g_global_scope == scope) g_global_scope = NULL;
    scope->next_sibling = NULL;
    scope->parent = NULL;
    thread_mutex_unlock(&g_registry_lock);
    atomic_fetch_sub_explicit(&g_scope_count, 1, memory_order_relaxed);

    for (;;) {
        thread_mutex_lock(&g_registry_lock);
        mem_scope_t* child = scope->first_child;
        if (child) {
            scope->first_child = child->next_sibling;
            child->next_sibling = NULL;
            child->parent = NULL;
        }
        thread_mutex_unlock(&g_registry_lock);
        if (!child) break;
        mem_scope_destroy(child);
    }

    mem_tls_forget_scope(scope);

    mem_block_t* block = NULL;
    if (scope->lock_ready) {
        thread_mutex_lock(&scope->lock);
        block = scope->blocks;
        scope->blocks = NULL;
        scope->live_blocks = 0;
        scope->live_bytes = 0;
        thread_mutex_unlock(&scope->lock);
    }
    while (block) {
        mem_block_t* next = block->next;
        mem_stats_remove_block(block->size);
        block->state = MEM_BLOCK_QUARANTINED;
        memset((char*)block + MEM_HEADER_SIZE, MEM_POISON_BYTE, block->size);
        free(block);
        block = next;
    }
    if (scope->lock_ready) {
        thread_mutex_destroy(&scope->lock);
        scope->lock_ready = 0;
    }
    free(scope);
}

void mem_scope_enter(mem_scope_t* scope) {
    if (!scope) return;
    if (g_thread_state.depth >= MEM_TLS_DEPTH) {
        fprintf(stderr, "memory: scope stack overflow\n");
        return;
    }
    g_thread_state.stack[g_thread_state.depth++] = scope;
}

void mem_scope_leave(void) {
    if (g_thread_state.depth <= 0) {
        fprintf(stderr, "memory: scope stack underflow\n");
        return;
    }
    g_thread_state.depth--;
}

mem_scope_t* mem_scope_current(void) {
    if (g_thread_state.depth > 0) {
        return g_thread_state.stack[g_thread_state.depth - 1];
    }
    return g_global_scope;
}

static void* mem_alloc_in_scope(mem_scope_t* scope, size_t size, const char* tag) {
    if (!g_inited) return NULL;
    if (!scope) scope = mem_scope_current();
    if (!scope) return NULL;
    if (size == 0) size = 1;
    if (size > SIZE_MAX - MEM_HEADER_SIZE - MEM_TRAILER_SIZE) {
        fprintf(stderr, "memory: allocation size overflow\n");
        return NULL;
    }
    size_t total = MEM_HEADER_SIZE + size + MEM_TRAILER_SIZE;
    mem_block_t* block = malloc(total);
    if (!block) {
        fprintf(stderr, "memory: out of memory allocating %zu bytes\n", size);
        return NULL;
    }
    block->magic = MEM_BLOCK_MAGIC;
    block->tail = MEM_TAIL_MAGIC;
    block->size = size;
    block->scope = scope;
    block->tag = (tag && tag[0]) ? tag : MEM_DEFAULT_TAG;
    block->thread_id = platform_get_tid();
    block->prev = NULL;
    block->next = NULL;
    block->state = MEM_BLOCK_LIVE;
    block->canary = 0;
    block->canary = mem_canary(block);
    mem_write_tail(block);

    thread_mutex_lock(&scope->lock);
    if (scope->destroyed) {
        thread_mutex_unlock(&scope->lock);
        free(block);
        return NULL;
    }
    block->next = scope->blocks;
    if (scope->blocks) scope->blocks->prev = block;
    scope->blocks = block;
    scope->live_blocks++;
    scope->live_bytes += size;
    thread_mutex_unlock(&scope->lock);
    mem_stats_add_block(size);
    return (char*)block + MEM_HEADER_SIZE;
}

void* mem_alloc(size_t size) {
    return mem_alloc_in_scope(NULL, size, MEM_DEFAULT_TAG);
}

void* mem_alloc_tagged(size_t size, const char* tag) {
    return mem_alloc_in_scope(NULL, size, tag);
}

void* mem_scope_alloc(mem_scope_t* scope, size_t size) {
    return mem_alloc_in_scope(scope, size, MEM_DEFAULT_TAG);
}

void* mem_scope_alloc_tagged(mem_scope_t* scope, size_t size, const char* tag) {
    return mem_alloc_in_scope(scope, size, tag);
}

void* mem_calloc(size_t count, size_t size) {
    if (count != 0 && size > SIZE_MAX / count) {
        fprintf(stderr, "memory: calloc size overflow\n");
        return NULL;
    }
    size_t total = count * size;
    void* ptr = mem_alloc(total);
    if (ptr) memset(ptr, 0, total);
    return ptr;
}

void mem_free(void* ptr) {
    if (!ptr) return;
    mem_block_t* block = (mem_block_t*)((char*)ptr - MEM_HEADER_SIZE);
    if (!mem_block_ok(block, "free")) return;
    if (block->state == MEM_BLOCK_QUARANTINED) {
        fprintf(stderr, "memory: double free detected for %p\n", ptr);
        return;
    }
    if (block->state != MEM_BLOCK_LIVE) {
        fprintf(stderr, "memory: free received a non-live block %p\n", ptr);
        return;
    }
    mem_scope_t* scope = block->scope;
    if (!scope) {
        fprintf(stderr, "memory: free received a block without a scope\n");
        return;
    }
    thread_mutex_lock(&scope->lock);
    if (block->prev) block->prev->next = block->next;
    if (block->next) block->next->prev = block->prev;
    if (scope->blocks == block) scope->blocks = block->next;
    block->prev = NULL;
    block->next = NULL;
    if (scope->live_blocks > 0) scope->live_blocks--;
    if (scope->live_bytes >= block->size) scope->live_bytes -= block->size;
    thread_mutex_unlock(&scope->lock);
    mem_stats_remove_block(block->size);
    block->state = MEM_BLOCK_QUARANTINED;
    block->scope = NULL;
    memset(ptr, MEM_POISON_BYTE, block->size);

    int keep = 0;
    thread_mutex_lock(&g_registry_lock);
    if (g_quarantine_blocks < MEM_MAX_QUARANTINE_BLOCKS &&
        g_quarantine_bytes + block->size <= MEM_MAX_QUARANTINE_BYTES) {
        block->prev = g_quarantine_tail;
        block->next = NULL;
        if (g_quarantine_tail) g_quarantine_tail->next = block;
        else g_quarantine_head = block;
        g_quarantine_tail = block;
        g_quarantine_blocks++;
        g_quarantine_bytes += block->size;
        keep = 1;
    }
    thread_mutex_unlock(&g_registry_lock);
    if (!keep) free(block);
}

void* mem_realloc(void* ptr, size_t size) {
    if (!ptr) return mem_alloc(size);
    if (size == 0) {
        mem_free(ptr);
        return NULL;
    }
    mem_block_t* block = (mem_block_t*)((char*)ptr - MEM_HEADER_SIZE);
    if (!mem_block_ok(block, "realloc")) return NULL;
    if (block->state == MEM_BLOCK_QUARANTINED) {
        fprintf(stderr, "memory: realloc on freed pointer %p\n", ptr);
        return NULL;
    }
    mem_scope_t* scope = block->scope;
    const char* tag = block->tag;
    size_t old_size = block->size;
    void* replacement = mem_alloc_in_scope(scope, size, tag);
    if (!replacement) return NULL;
    memcpy(replacement, ptr, old_size < size ? old_size : size);
    mem_free(ptr);
    return replacement;
}

char* mem_strndup(const char* s, size_t n) {
    if (!s) return NULL;
    size_t len = 0;
    while (len < n && s[len]) len++;
    char* result = mem_alloc_tagged(len + 1, "strdup");
    if (!result) return NULL;
    memcpy(result, s, len);
    result[len] = '\0';
    return result;
}

char* mem_strdup(const char* s) {
    if (!s) return NULL;
    return mem_strndup(s, strlen(s));
}

char* mem_scope_strdup(mem_scope_t* scope, const char* s) {
    if (!s) return NULL;
    size_t len = strlen(s);
    char* result = mem_alloc_in_scope(scope, len + 1, "strdup");
    if (!result) return NULL;
    memcpy(result, s, len + 1);
    return result;
}

int mem_promote(void* ptr, mem_scope_t* target) {
    if (!ptr) return -1;
    if (!target) target = g_global_scope;
    if (!target) return -1;
    mem_block_t* block = (mem_block_t*)((char*)ptr - MEM_HEADER_SIZE);
    if (!mem_block_ok(block, "promote")) return -1;
    if (block->state != MEM_BLOCK_LIVE) return -1;
    mem_scope_t* source = block->scope;
    if (!source || source == target) return 0;

    mem_scope_t* first = (uintptr_t)source < (uintptr_t)target ? source : target;
    mem_scope_t* second = (uintptr_t)source < (uintptr_t)target ? target : source;
    thread_mutex_lock(&first->lock);
    thread_mutex_lock(&second->lock);
    if (target->destroyed || source->destroyed) {
        thread_mutex_unlock(&second->lock);
        thread_mutex_unlock(&first->lock);
        return -1;
    }
    if (block->prev) block->prev->next = block->next;
    if (block->next) block->next->prev = block->prev;
    if (source->blocks == block) source->blocks = block->next;
    block->prev = NULL;
    block->next = NULL;
    if (source->live_blocks > 0) source->live_blocks--;
    if (source->live_bytes >= block->size) source->live_bytes -= block->size;
    block->next = target->blocks;
    if (target->blocks) target->blocks->prev = block;
    target->blocks = block;
    block->scope = target;
    target->live_blocks++;
    target->live_bytes += block->size;
    thread_mutex_unlock(&second->lock);
    thread_mutex_unlock(&first->lock);
    return 0;
}

void mem_purge_quarantine(void) {
    if (!g_registry_ready) return;
    mem_block_t* block = NULL;
    thread_mutex_lock(&g_registry_lock);
    block = g_quarantine_head;
    g_quarantine_head = NULL;
    g_quarantine_tail = NULL;
    g_quarantine_blocks = 0;
    g_quarantine_bytes = 0;
    thread_mutex_unlock(&g_registry_lock);
    while (block) {
        mem_block_t* next = block->next;
        free(block);
        block = next;
    }
}

int mem_check(void) {
    if (!g_registry_ready) return 0;
    int errors = 0;
    thread_mutex_lock(&g_registry_lock);
    for (mem_scope_t* scope = g_scopes; scope; scope = scope->registry_next) {
        if (!scope->lock_ready) continue;
        thread_mutex_lock(&scope->lock);
        for (mem_block_t* block = scope->blocks; block; block = block->next) {
            if (!mem_block_ok(block, "check")) {
                errors++;
                continue;
            }
            if (block->scope != scope) {
                fprintf(stderr, "memory: check found block %p in the wrong scope\n", (void*)block);
                errors++;
            }
            if (block->state != MEM_BLOCK_LIVE) {
                fprintf(stderr, "memory: check found a non-live tracked block %p\n", (void*)block);
                errors++;
            }
        }
        thread_mutex_unlock(&scope->lock);
    }
    for (mem_block_t* block = g_quarantine_head; block; block = block->next) {
        if (block->magic != MEM_BLOCK_MAGIC || !mem_tail_ok(block)) {
            errors++;
        }
    }
    thread_mutex_unlock(&g_registry_lock);
    return errors;
}

void mem_report_leaks(const char* label) {
    if (!g_registry_ready) return;
    mem_stats_t stats = mem_get_stats();
    if (stats.live_blocks == 0) {
        LOG_INFO("memory: no live scoped allocations at %s", label ? label : "report");
        return;
    }
    LOG_WARN("memory: %zu live blocks, %zu live bytes, %zu total blocks, %zu total bytes at %s",
        stats.live_blocks, stats.live_bytes, stats.total_blocks, stats.total_bytes,
        label ? label : "report");
    thread_mutex_lock(&g_registry_lock);
    for (mem_scope_t* scope = g_scopes; scope; scope = scope->registry_next) {
        if (!scope->lock_ready) continue;
        thread_mutex_lock(&scope->lock);
        if (scope->live_blocks > 0) {
            LOG_WARN("memory: scope '%s' id=%llu blocks=%zu bytes=%zu",
                scope->name, (unsigned long long)scope->id, scope->live_blocks, scope->live_bytes);
            size_t shown = 0;
            for (mem_block_t* block = scope->blocks; block && shown < 8; block = block->next, ++shown) {
                LOG_WARN("memory:   block %p size=%zu tag=%s thread=%lu",
                    (void*)block, block->size, block->tag ? block->tag : MEM_DEFAULT_TAG, block->thread_id);
            }
        }
        thread_mutex_unlock(&scope->lock);
    }
    thread_mutex_unlock(&g_registry_lock);
}

void mem_crash_snapshot(int fd) {
    if (fd < 0) return;
    char buffer[256];
    int len = snprintf(buffer, sizeof(buffer),
        "memory: live_blocks=%zu live_bytes=%zu total_blocks=%zu total_bytes=%zu quarantine_blocks=%zu quarantine_bytes=%zu scopes=%zu\n",
        atomic_load_explicit(&g_live_blocks, memory_order_relaxed),
        atomic_load_explicit(&g_live_bytes, memory_order_relaxed),
        atomic_load_explicit(&g_total_blocks, memory_order_relaxed),
        atomic_load_explicit(&g_total_bytes, memory_order_relaxed),
        g_quarantine_blocks, g_quarantine_bytes,
        atomic_load_explicit(&g_scope_count, memory_order_relaxed));
    if (len <= 0) return;
    if ((size_t)len >= sizeof(buffer)) len = (int)sizeof(buffer) - 1;
#ifdef _WIN32
    _write(fd, buffer, (unsigned int)len);
#else
    ssize_t written = write(fd, buffer, (size_t)len);
    (void)written;
#endif
}

mem_stats_t mem_get_stats(void) {
    mem_stats_t stats;
    stats.live_blocks = atomic_load_explicit(&g_live_blocks, memory_order_relaxed);
    stats.live_bytes = atomic_load_explicit(&g_live_bytes, memory_order_relaxed);
    stats.total_blocks = atomic_load_explicit(&g_total_blocks, memory_order_relaxed);
    stats.total_bytes = atomic_load_explicit(&g_total_bytes, memory_order_relaxed);
    if (g_registry_ready) {
        thread_mutex_lock(&g_registry_lock);
        stats.quarantine_blocks = g_quarantine_blocks;
        stats.quarantine_bytes = g_quarantine_bytes;
        thread_mutex_unlock(&g_registry_lock);
    } else {
        stats.quarantine_blocks = 0;
        stats.quarantine_bytes = 0;
    }
    stats.scopes = atomic_load_explicit(&g_scope_count, memory_order_relaxed);
    return stats;
}
