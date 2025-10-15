#include "crypto.h"
#include "common.h"


#define F(x,y,z) ((x & y) | (~x & z))
#define G(x,y,z) ((x & z) | (y & ~z))
#define H(x,y,z) (x ^ y ^ z)
#define I(x,y,z) (y ^ (x | ~z))
#define ROTATE_LEFT(x,n) (((x) << (n)) | ((x) >> (32-(n))))
#define FF(a,b,c,d,x,s,ac) { a += F(b,c,d) + x + ac; a = ROTATE_LEFT(a,s) + b; }
#define GG(a,b,c,d,x,s,ac) { a += G(b,c,d) + x + ac; a = ROTATE_LEFT(a,s) + b; }
#define HH(a,b,c,d,x,s,ac) { a += H(b,c,d) + x + ac; a = ROTATE_LEFT(a,s) + b; }
#define II(a,b,c,d,x,s,ac) { a += I(b,c,d) + x + ac; a = ROTATE_LEFT(a,s) + b; }
static void MD5_Transform(uint32_t state[4], const uint8_t block[64]) {
	uint32_t a = state[0], b = state[1], c = state[2], d = state[3], x[16];
	for (int i = 0;i < 16;i++)
		x[i] = (uint32_t)block[i * 4] | ((uint32_t)block[i * 4 + 1] << 8) |
		((uint32_t)block[i * 4 + 2] << 16) | ((uint32_t)block[i * 4 + 3] << 24);
	FF(a, b, c, d, x[0], 7, 0xd76aa478); FF(d, a, b, c, x[1], 12, 0xe8c7b756);
	FF(c, d, a, b, x[2], 17, 0x242070db); FF(b, c, d, a, x[3], 22, 0xc1bdceee);
	FF(a, b, c, d, x[4], 7, 0xf57c0faf); FF(d, a, b, c, x[5], 12, 0x4787c62a);
	FF(c, d, a, b, x[6], 17, 0xa8304613); FF(b, c, d, a, x[7], 22, 0xfd469501);
	FF(a, b, c, d, x[8], 7, 0x698098d8); FF(d, a, b, c, x[9], 12, 0x8b44f7af);
	FF(c, d, a, b, x[10], 17, 0xffff5bb1); FF(b, c, d, a, x[11], 22, 0x895cd7be);
	FF(a, b, c, d, x[12], 7, 0x6b901122); FF(d, a, b, c, x[13], 12, 0xfd987193);
	FF(c, d, a, b, x[14], 17, 0xa679438e); FF(b, c, d, a, x[15], 22, 0x49b40821);
	GG(a, b, c, d, x[1], 5, 0xf61e2562); GG(d, a, b, c, x[6], 9, 0xc040b340);
	GG(c, d, a, b, x[11], 14, 0x265e5a51); GG(b, c, d, a, x[0], 20, 0xe9b6c7aa);
	GG(a, b, c, d, x[5], 5, 0xd62f105d); GG(d, a, b, c, x[10], 9, 0x02441453);
	GG(c, d, a, b, x[15], 14, 0xd8a1e681); GG(b, c, d, a, x[4], 20, 0xe7d3fbc8);
	GG(a, b, c, d, x[9], 5, 0x21e1cde6); GG(d, a, b, c, x[14], 9, 0xc33707d6);
	GG(c, d, a, b, x[3], 14, 0xf4d50d87); GG(b, c, d, a, x[8], 20, 0x455a14ed);
	GG(a, b, c, d, x[13], 5, 0xa9e3e905); GG(d, a, b, c, x[2], 9, 0xfcefa3f8);
	GG(c, d, a, b, x[7], 14, 0x676f02d9); GG(b, c, d, a, x[12], 20, 0x8d2a4c8a);
	HH(a, b, c, d, x[5], 4, 0xfffa3942); HH(d, a, b, c, x[8], 11, 0x8771f681);
	HH(c, d, a, b, x[11], 16, 0x6d9d6122); HH(b, c, d, a, x[14], 23, 0xfde5380c);
	HH(a, b, c, d, x[1], 4, 0xa4beea44); HH(d, a, b, c, x[4], 11, 0x4bdecfa9);
	HH(c, d, a, b, x[7], 16, 0xf6bb4b60); HH(b, c, d, a, x[10], 23, 0xbebfbc70);
	HH(a, b, c, d, x[13], 4, 0x289b7ec6); HH(d, a, b, c, x[0], 11, 0xeaa127fa);
	HH(c, d, a, b, x[3], 16, 0xd4ef3085); HH(b, c, d, a, x[6], 23, 0x04881d05);
	HH(a, b, c, d, x[9], 4, 0xd9d4d039); HH(d, a, b, c, x[12], 11, 0xe6db99e5);
	HH(c, d, a, b, x[15], 16, 0x1fa27cf8); HH(b, c, d, a, x[2], 23, 0xc4ac5665);
	II(a, b, c, d, x[0], 6, 0xf4292244); II(d, a, b, c, x[7], 10, 0x432aff97);
	II(c, d, a, b, x[14], 15, 0xab9423a7); II(b, c, d, a, x[5], 21, 0xfc93a039);
	II(a, b, c, d, x[12], 6, 0x655b59c3); II(d, a, b, c, x[3], 10, 0x8f0ccc92);
	II(c, d, a, b, x[10], 15, 0xffeff47d); II(b, c, d, a, x[1], 21, 0x85845dd1);
	II(a, b, c, d, x[8], 6, 0x6fa87e4f); II(d, a, b, c, x[15], 10, 0xfe2ce6e0);
	II(c, d, a, b, x[6], 15, 0xa3014314); II(b, c, d, a, x[13], 21, 0x4e0811a1);
	II(a, b, c, d, x[4], 6, 0xf7537e82); II(d, a, b, c, x[11], 10, 0xbd3af235);
	II(c, d, a, b, x[2], 15, 0x2ad7d2bb); II(b, c, d, a, x[9], 21, 0xeb86d391);
	state[0] += a; state[1] += b; state[2] += c; state[3] += d;
}
static void MD5_Init(MD5_CTX* ctx) {
	ctx->count[0] = ctx->count[1] = 0;
	ctx->state[0] = 0x67452301; ctx->state[1] = 0xefcdab89;
	ctx->state[2] = 0x98badcfe; ctx->state[3] = 0x10325476;
}
static void MD5_Update(MD5_CTX* ctx,const uint8_t* input,size_t len){
    size_t i,index,partlen;uint8_t block[64];
    index=(ctx->count[0]>>3)&0x3F;
    if((ctx->count[0]+=((uint32_t)len<<3))<((uint32_t)len<<3))ctx->count[1]++;
    ctx->count[1]+=((uint32_t)len>>29);
    partlen=64-index;
    if(len>=partlen){
        memcpy(&ctx->buffer[index],input,partlen);
        MD5_Transform(ctx->state,ctx->buffer);
        i=partlen;
        while(i+64<=len){
            memcpy(block,input+i,64);
            MD5_Transform(ctx->state,block);
            i+=64;
        }
        index=0;
    }else i=0;
    if(len>i)memcpy(&ctx->buffer[index],input+i,len-i);
}
static void MD5_Final(uint8_t digest[16], MD5_CTX* ctx) {
	uint8_t bits[8]; size_t index, padlen;
	for (int i = 0;i < 4;i++) {
		bits[i] = ctx->count[0] >> (i * 8);
		bits[i + 4] = ctx->count[1] >> (i * 8);
	}
	index = (ctx->count[0] >> 3) & 0x3F;
	padlen = (index < 56) ? (56 - index) : (120 - index); 
	static const uint8_t PADDING[64] = { 0x80 };
	MD5_Update(ctx, PADDING, padlen); 
	MD5_Update(ctx, bits, 8);
	for (int i = 0;i < 16;i++) digest[i] = (ctx->state[i >> 2] >> (i % 4 * 8)) & 0xFF;
}
int crypto_md5(const void* data, size_t len, uint8_t* digest_out) {
	if (!data || !digest_out) return -1;
	MD5_CTX ctx;
	MD5_Init(&ctx);
	MD5_Update(&ctx, (const uint8_t*)data, len);
	MD5_Final(digest_out, &ctx);
	return 0;
}


typedef struct {
	uint32_t state[5];
	uint32_t count[2];
	uint8_t buffer[64];
} SHA1_CTX;
#define SHA1_ROTL(a,b) (((a) << (b)) | ((a) >> (32-(b))))
static void SHA1_Transform(uint32_t state[5], const uint8_t buffer[64]) {
	uint32_t a, b, c, d, e, t, W[80];
	for (int i = 0;i < 16;i++)
		W[i] = (buffer[i * 4] << 24) | (buffer[i * 4 + 1] << 16) | (buffer[i * 4 + 2] << 8) | (buffer[i * 4 + 3]);
	for (int i = 16;i < 80;i++) W[i] = SHA1_ROTL(W[i - 3] ^ W[i - 8] ^ W[i - 14] ^ W[i - 16], 1);
	a = state[0]; b = state[1]; c = state[2]; d = state[3]; e = state[4];
	for (int i = 0;i < 80;i++) {
		uint32_t f, k;
		if (i < 20) { f = (b & c) | (~b & d); k = 0x5A827999; }
		else if (i < 40) { f = b ^ c ^ d; k = 0x6ED9EBA1; }
		else if (i < 60) { f = (b & c) | (b & d) | (c & d); k = 0x8F1BBCDC; }
		else { f = b ^ c ^ d; k = 0xCA62C1D6; }

		t = SHA1_ROTL(a, 5) + f + e + k + W[i];
		e = d; d = c; c = SHA1_ROTL(b, 30); b = a; a = t;
	}
	state[0] += a; state[1] += b; state[2] += c; state[3] += d; state[4] += e;
}
static void SHA1_Init(SHA1_CTX* ctx) {
	ctx->state[0] = 0x67452301; ctx->state[1] = 0xEFCDAB89;
	ctx->state[2] = 0x98BADCFE; ctx->state[3] = 0x10325476; ctx->state[4] = 0xC3D2E1F0;
	ctx->count[0] = ctx->count[1] = 0;
}
static void SHA1_Update(SHA1_CTX* ctx,const uint8_t* data,size_t len){
    size_t i,index,partlen;uint8_t block[64];
    index=(ctx->count[0]>>3)&0x3F;
    if((ctx->count[0]+=((uint32_t)len<<3))<((uint32_t)len<<3))ctx->count[1]++;
    ctx->count[1]+=((uint32_t)len>>29);
    partlen=64-index;
    if(len>=partlen){
        memcpy(&ctx->buffer[index],data,partlen);
        SHA1_Transform(ctx->state,ctx->buffer);
        i=partlen;
        while(i+64<=len){
            memcpy(block,data+i,64);
            SHA1_Transform(ctx->state,block);
            i+=64;
        }
        index=0;
    }else i=0;
    if(len>i)memcpy(&ctx->buffer[index],data+i,len-i);
}
static void SHA1_Final(uint8_t digest[20], SHA1_CTX* ctx) {
	uint8_t finalcount[8]; size_t i, index, padlen;
	for (i = 0;i < 4;i++) {
		finalcount[i] = (ctx->count[1] >> ((3 - i) * 8)) & 0xFF;
		finalcount[i + 4] = (ctx->count[0] >> ((3 - i) * 8)) & 0xFF;
	}
	index = (ctx->count[0] >> 3) & 0x3F;
	padlen = (index < 56) ? (56 - index) : (120 - index);
	static const uint8_t PADDING[64] = { 0x80 };
	SHA1_Update(ctx, PADDING, padlen);
	SHA1_Update(ctx, finalcount, 8);
	for (i = 0;i < 20;i++) digest[i] = (ctx->state[i >> 2] >> ((3 - (i & 3)) * 8)) & 0xFF;
}
int crypto_sha1(const void* data, size_t len, uint8_t* digest_out) {
	if (!data || !digest_out) return -1;
	SHA1_CTX ctx;
	SHA1_Init(&ctx);
	SHA1_Update(&ctx, (const uint8_t*)data, len);
	SHA1_Final(digest_out, &ctx);
	return 0;
}

static const char base64_chars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
size_t crypto_base64_encode_len(size_t in_len) {
	return ((in_len + 2) / 3) * 4; 
}
size_t crypto_base64_decode_maxlen(size_t enc_len) {
	if (enc_len % 4 != 0) return (size_t)-1;
	return (enc_len / 4) * 3;
}
size_t crypto_base64_encode(const void* data, size_t len, char* encoded_out, size_t out_size) {
	const uint8_t* input = (const uint8_t*)data;
	size_t i = 0, j = 0;
	uint32_t octet_a, octet_b, octet_c;
	uint32_t triple;
	if (!data || !encoded_out) return (size_t)-1;
	size_t output_len = crypto_base64_encode_len(len);
	if (out_size < output_len + 1) return (size_t)-1;
	while (i < len) {
		octet_a = input[i++];
		bool has_octet_b = (i < len);
		octet_b = has_octet_b ? input[i++] : 0;
		bool has_octet_c = (i < len);
		octet_c = has_octet_c ? input[i++] : 0;
		triple = (octet_a << 16) | (octet_b << 8) | octet_c;
		encoded_out[j++] = base64_chars[(triple >> 18) & 0x3F];
		encoded_out[j++] = base64_chars[(triple >> 12) & 0x3F];
		if (!has_octet_b) {
			encoded_out[j++] = '=';
			encoded_out[j++] = '=';
		}
		else if (!has_octet_c) {
			encoded_out[j++] = base64_chars[(triple >> 6) & 0x3F];
			encoded_out[j++] = '=';
		}
		else {
			encoded_out[j++] = base64_chars[(triple >> 6) & 0x3F];
			encoded_out[j++] = base64_chars[triple & 0x3F];
		}
	}
	encoded_out[output_len] = '\0';
	return output_len;
}
static inline int base64_val(unsigned char c) {
	if (c >= 'A' && c <= 'Z') return c - 'A';
	if (c >= 'a' && c <= 'z') return c - 'a' + 26;
	if (c >= '0' && c <= '9') return c - '0' + 52;
	if (c == '+') return 62;
	if (c == '/') return 63;
	return -1;
}
size_t crypto_base64_decode(const char* encoded_in, size_t len, uint8_t* decoded_out, size_t out_size) {
	if (!encoded_in || !decoded_out) return (size_t)-1;
	if (len % 4 != 0) return (size_t)-1;
	size_t max_dec = crypto_base64_decode_maxlen(len);
	if (max_dec == (size_t)-1) return (size_t)-1;
	if (out_size < max_dec) return (size_t)-1; /* ensure caller buffer big enough */

	size_t i = 0, j = 0;
	while (i < len) {
		int a = base64_val((unsigned char)encoded_in[i]);
		int b = base64_val((unsigned char)encoded_in[i + 1]);
		int c = encoded_in[i + 2] == '=' ? 0 : base64_val((unsigned char)encoded_in[i + 2]);
		int d = encoded_in[i + 3] == '=' ? 0 : base64_val((unsigned char)encoded_in[i + 3]);
		if (a < 0 || b < 0) return (size_t)-1; /* invalid input */
		if (encoded_in[i + 2] != '=' && c < 0) return (size_t)-1;
		if (encoded_in[i + 3] != '=' && d < 0) return (size_t)-1;

		uint32_t triple = ((uint32_t)a << 18) | ((uint32_t)b << 12) | ((uint32_t)c << 6) | (uint32_t)d;
		decoded_out[j++] = (triple >> 16) & 0xFF;
		if (encoded_in[i + 2] != '=') {
			decoded_out[j++] = (triple >> 8) & 0xFF;
		}
		if (encoded_in[i + 3] != '=') {
			decoded_out[j++] = triple & 0xFF;
		}
		i += 4;
	}
	return j;
}

int crypto_md5_file(const char *path, uint8_t *digest_out) {
	if (!path || !digest_out) return -1;
	FILE *f = fopen(path, "rb");
	if (!f) return -1;
	MD5_CTX ctx; MD5_Init(&ctx);
	unsigned char buf[4096]; size_t r;
	while ((r = fread(buf, 1, sizeof(buf), f)) > 0) {
		MD5_Update(&ctx, buf, r);
	}
	MD5_Final(digest_out, &ctx);
	fclose(f);
	return 0;
}