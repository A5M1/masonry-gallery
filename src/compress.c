#include "compress.h"
#include "logging.h"
#include "common.h"

static void write_u16(unsigned char* p, uint16_t v) { p[0] = (unsigned char)(v & 0xFF); p[1] = (unsigned char)((v >> 8) & 0xFF); }
static void write_u32(unsigned char* p, uint32_t v) { p[0] = (unsigned char)(v & 0xFF); p[1] = (unsigned char)((v >> 8) & 0xFF); p[2] = (unsigned char)((v >> 16) & 0xFF); p[3] = (unsigned char)((v >> 24) & 0xFF); }
static uint16_t read_u16(const unsigned char* p) { return (uint16_t)p[0] | ((uint16_t)p[1] << 8); }
static uint32_t read_u32(const unsigned char* p) { return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24); }

static unsigned char* do_rle(const char* in, size_t in_len, size_t* out_len) {
    if (!in || in_len == 0) { *out_len = 0; return NULL; }
    size_t cap = in_len + 16;
    unsigned char* ob = malloc(cap);
    if (!ob) {
        LOG_ERROR("Failed to allocate RLE output buffer of size %zu", cap);
        return NULL;
    }
    size_t oi = 0; size_t i = 0;
    while (i < in_len) {
        size_t j = i + 1; while (j < in_len && in[j] == in[i] && j - i < 255) j++;
        size_t run = j - i;
        if (run > 3) {
            if (oi + 3 > cap) {
                cap = (cap + 3) * 2;
                unsigned char* nb = realloc(ob, cap);
                if (!nb) {
                    LOG_ERROR("Failed to realloc RLE output buffer to size %zu", cap);
                    free(ob);
                    return NULL;
                }
                ob = nb;
            }
            ob[oi++] = 0xFF; ob[oi++] = (unsigned char)run; ob[oi++] = (unsigned char)in[i];
        } else {
            if (oi + run > cap) {
                cap = (cap + run) * 2;
                unsigned char* nb = realloc(ob, cap);
                if (!nb) {
                    LOG_ERROR("Failed to realloc RLE output buffer to size %zu", cap);
                    free(ob);
                    return NULL;
                }
                ob = nb;
            }
            for (size_t k = 0; k < run; ++k) ob[oi++] = (unsigned char)in[i+k];
        }
        i = j;
    }
    unsigned char* tb = realloc(ob, oi);
    if (tb) ob = tb;
    *out_len = oi; return ob;
}

typedef struct HNode { uint64_t weight; int symbol; int left, right; } HNode;

static int build_huffman_lengths(const uint64_t freq[256], uint8_t code_len[256]) {
    memset(code_len, 0, 256);
    int symbols = 0; for (int i = 0; i < 256; ++i) if (freq[i]) symbols++;
    if (symbols == 0) return 0;
    if (symbols == 1) { for (int i=0;i<256;++i) if (freq[i]) { code_len[i] = 1; return 1; } }
    int max_nodes = 2 * symbols - 1;
    HNode* nodes = malloc(sizeof(HNode) * max_nodes);
    if (!nodes) {
        LOG_ERROR("Failed to allocate Huffman nodes array of size %d", max_nodes);
        return -1;
    }
    int n = 0;
    for (int i = 0; i < 256; ++i) if (freq[i]) { nodes[n].weight = freq[i]; nodes[n].symbol = i; nodes[n].left = nodes[n].right = -1; n++; }
    int node_count = n;
    while (n > 1) {
        int a = -1, b = -1;
        for (int i = 0; i < node_count; ++i) if (nodes[i].weight > 0) {
            if (a == -1 || nodes[i].weight < nodes[a].weight) { b = a; a = i; }
            else if (b == -1 || nodes[i].weight < nodes[b].weight) { b = i; }
        }
        if (a == -1 || b == -1) break;
        nodes[node_count].weight = nodes[a].weight + nodes[b].weight;
        nodes[node_count].symbol = -1;
        nodes[node_count].left = a; nodes[node_count].right = b;
        nodes[a].weight = 0; nodes[b].weight = 0; node_count++; n--;
    }
    int root = node_count - 1;
    int stack[512]; int sp = 0; int depth[512];
    stack[sp] = root; depth[sp] = 0; sp++;
    while (sp > 0) {
        sp--; int idx = stack[sp]; int d = depth[sp];
        if (nodes[idx].symbol >= 0) {
            int sym = nodes[idx].symbol; code_len[sym] = (uint8_t)d;
        } else {
            if (nodes[idx].right >= 0) { stack[sp] = nodes[idx].right; depth[sp] = d+1; sp++; }
            if (nodes[idx].left >= 0) { stack[sp] = nodes[idx].left; depth[sp] = d+1; sp++; }
        }
    }
    free(nodes);
    return symbols;
}

typedef struct SymLen { uint8_t sym; uint8_t len; } SymLen;
static int make_canonical(uint8_t code_len[256], uint32_t codes[256]) {
    SymLen arr[256]; int ac = 0;
    for (int i = 0; i < 256; ++i) if (code_len[i]) { arr[ac].sym = (uint8_t)i; arr[ac].len = code_len[i]; ac++; }
    if (ac == 0) return 0;
    for (int i = 0; i < ac - 1; ++i) for (int j = i+1; j < ac; ++j) {
        if (arr[i].len > arr[j].len || (arr[i].len == arr[j].len && arr[i].sym > arr[j].sym)) { SymLen t = arr[i]; arr[i] = arr[j]; arr[j] = t; }
    }
    memset(codes, 0, sizeof(uint32_t) * 256);
    uint32_t code = 0; uint8_t prev_len = arr[0].len;
    for (int i = 0; i < ac; ++i) {
        uint8_t len = arr[i].len;
        if (i == 0) {
            code = 0;
        } else {
            if (len > prev_len) code <<= (len - prev_len);
            code++;
        }
        codes[arr[i].sym] = code;
        prev_len = len;
    }
    return ac;
}

typedef struct BitWriter { unsigned char* buf; size_t cap; size_t byte; int bitpos; } BitWriter;
static int bw_init(BitWriter* bw, size_t est) {
    bw->cap = est + 16;
    bw->buf = malloc(bw->cap);
    if (!bw->buf) {
        LOG_ERROR("Failed to allocate BitWriter buffer of size %zu", bw->cap);
        return -1;
    }
    bw->byte = 0; bw->bitpos = 0; return 0;
}
static void bw_ensure(BitWriter* bw, size_t more) {
    if (bw->byte + more + 4 > bw->cap) {
        bw->cap = (bw->cap + more) * 2;
        unsigned char* nb = realloc(bw->buf, bw->cap);
        if (nb) bw->buf = nb;
    }
}
static void bw_write_bits(BitWriter* bw, uint32_t code, uint8_t len) {
    for (int i = len - 1; i >= 0; --i) {
        int bit = (code >> i) & 1;
        if (bw->bitpos == 0) { bw_ensure(bw, 1); bw->buf[bw->byte] = 0; }
        bw->buf[bw->byte] = (unsigned char)((bw->buf[bw->byte] << 1) | bit);
        bw->bitpos++;
        if (bw->bitpos == 8) { bw->bitpos = 0; bw->byte++; }
    }
}
static unsigned char* bw_finish(BitWriter* bw, size_t* out_bits, size_t* out_bytes) {
    if (bw->bitpos != 0) { bw->buf[bw->byte] <<= (8 - bw->bitpos); bw->byte++; }
    *out_bytes = bw->byte; *out_bits = (bw->byte - 1) * 8 + (bw->bitpos == 0 ? 8 : bw->bitpos);
    return bw->buf;
}

int compress_val(const char* in, size_t in_len, unsigned char** out, size_t* out_len) {
    if (!in || in_len == 0 || !out || !out_len) return -1;
    size_t rle_len = 0; unsigned char* rle = do_rle(in, in_len, &rle_len);
    if (!rle) return -1;
    uint64_t freq[256]; memset(freq, 0, sizeof(freq)); for (size_t i=0;i<rle_len;++i) freq[rle[i]]++;
    uint8_t code_len[256]; if (build_huffman_lengths(freq, code_len) < 0) { free(rle); return -1; }
    uint32_t codes[256]; memset(codes,0,sizeof(codes)); int symcount = make_canonical(code_len, codes);
    size_t header_cap = 4 + 2 + symcount * 2 + 4;
    BitWriter bw; if (bw_init(&bw, rle_len) != 0) { free(rle); return -1; }
    for (size_t i=0;i<rle_len;++i) {
        uint8_t s = rle[i]; uint8_t len = code_len[s]; uint32_t code = codes[s]; bw_write_bits(&bw, code, len);
    }
    size_t bitlen=0, bytelenc=0; unsigned char* bitbuf = bw_finish(&bw, &bitlen, &bytelenc);
    size_t total = header_cap + bytelenc;
    unsigned char* outb = malloc(total);
    if (!outb) {
        LOG_ERROR("Failed to allocate compressed output buffer of size %zu", total);
        free(rle); free(bitbuf); return -1;
    }
    outb[0]='H'; outb[1]='H'; outb[2]='R'; outb[3]='1';
    write_u16(outb+4, (uint16_t)symcount);
    size_t p = 6;
    for (int i=0;i<256;++i) if (code_len[i]) { outb[p++] = (unsigned char)i; outb[p++] = code_len[i]; }
    write_u32(outb + p, (uint32_t)bitlen); p += 4;
    memcpy(outb + p, bitbuf, bytelenc); p += bytelenc;
    free(bitbuf); free(rle);
    *out = outb; *out_len = p; return 0;
}

int decompress_val(const unsigned char* in, size_t in_len, unsigned char** out, size_t* out_len) {
    if (!in || in_len < 9 || !out || !out_len) return -1;
    if (!(in[0]=='H' && in[1]=='H' && in[2]=='R' && in[3]=='1')) return -1;
    uint16_t symcount = read_u16(in+4);
    size_t p = 6;
    uint8_t code_len[256]; memset(code_len,0,sizeof(code_len));
    for (uint16_t i=0;i<symcount;++i) {
        if (p + 2 > in_len) return -1;
        uint8_t sym = in[p++]; uint8_t len = in[p++]; code_len[sym] = len;
    }
    if (p + 4 > in_len) return -1;
    uint32_t bitlen = read_u32(in + p); p += 4;
    size_t bytelenc = in_len - p;
    const unsigned char* bitbuf = in + p;
    uint32_t codes[256]; memset(codes,0,sizeof(codes)); make_canonical(code_len, codes);
    struct Pair { uint32_t code; uint8_t sym; } pairs[256]; int pair_counts[257]; memset(pair_counts,0,sizeof(pair_counts));
    int pc = 0;
    for (int i=0;i<256;++i) if (code_len[i]) { pairs[pc].code = codes[i]; pairs[pc].sym = (uint8_t)i; pc++; }
    for (int i=0;i<pc-1;++i) for (int j=i+1;j<pc;++j) {
        if (code_len[pairs[i].sym] > code_len[pairs[j].sym] || (code_len[pairs[i].sym]==code_len[pairs[j].sym] && pairs[i].code > pairs[j].code)) { struct Pair t = pairs[i]; pairs[i]=pairs[j]; pairs[j]=t; }
    }
    int idx_by_len[257]; memset(idx_by_len,-1,sizeof(idx_by_len));
    int idx = 0; for (int l=1;l<=256;++l) { idx_by_len[l] = idx; while (idx < pc && code_len[pairs[idx].sym] == l) idx++; }
    size_t bitpos = 0; uint32_t cur = 0; int cur_len = 0;
    unsigned char* out_rle = malloc(256);
    if (!out_rle) {
        LOG_ERROR("Failed to allocate RLE output buffer of size 256");
        return -1;
    }
    size_t out_cap = 256; size_t outi = 0;
    for (size_t b = 0; b < bitlen; ++b) {
        size_t byte_idx = b / 8; int bit_idx = 7 - (b % 8);
        int bit = (bitbuf[byte_idx] >> bit_idx) & 1;
        cur = (cur << 1) | bit; cur_len++;
        int start = (cur_len <= 256) ? idx_by_len[cur_len] : -1;
        if (start >= 0) {
            int j = start;
            while (j < pc && code_len[pairs[j].sym] == cur_len) {
                if (pairs[j].code == cur) {
                    uint8_t sym = pairs[j].sym;
                    if (outi + 1 > out_cap) {
                        out_cap *= 2;
                        unsigned char* nb = realloc(out_rle, out_cap);
                        if (!nb) {
                            LOG_ERROR("Failed to realloc RLE output buffer to size %zu", out_cap);
                            free(out_rle);
                            return -1;
                        }
                        out_rle = nb;
                    }
                    out_rle[outi++] = sym;
                    cur = 0; cur_len = 0;
                    break;
                }
                j++;
            }
        }
    }
    size_t exp_cap = outi * 2 + 16;
    unsigned char* exp = malloc(exp_cap);
    if (!exp) {
        LOG_ERROR("Failed to allocate expansion buffer of size %zu", exp_cap);
        free(out_rle);
        return -1;
    }
    size_t expi = 0; size_t ri = 0;
    while (ri < outi) {
        unsigned char c = out_rle[ri++];
        if (c == 0xFF) {
            if (ri + 1 > outi) { free(out_rle); free(exp); return -1; }
            unsigned char run = out_rle[ri++]; unsigned char ch = out_rle[ri++];
            if (expi + run > exp_cap) {
                while (expi + run > exp_cap) exp_cap *= 2;
                unsigned char* nb = realloc(exp, exp_cap);
                if (!nb) {
                    LOG_ERROR("Failed to realloc expansion buffer to size %zu", exp_cap);
                    free(out_rle); free(exp);
                    return -1;
                }
                exp = nb;
            }
            for (unsigned int k=0;k<run;++k) exp[expi++] = ch;
        } else {
            if (expi + 1 > exp_cap) {
                exp_cap *= 2;
                unsigned char* nb = realloc(exp, exp_cap);
                if (!nb) {
                    LOG_ERROR("Failed to realloc expansion buffer to size %zu", exp_cap);
                    free(out_rle); free(exp);
                    return -1;
                }
                exp = nb;
            }
            exp[expi++] = c;
        }
    }
    free(out_rle);
    unsigned char* tb = realloc(exp, expi + 1);
    if (tb) exp = tb;
    exp[expi] = '\0';
    *out = exp; *out_len = expi; return 0;
}