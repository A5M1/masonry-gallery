#include "video2png.h"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/hwcontext.h>
#include <libswscale/swscale.h>
}
#include <cuda_runtime.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#define PNG_FILTER_SUB 1


__global__ void png_filter_row_kernel(unsigned char* output, const unsigned char* input,
                                      int width, int filter_type) {
    int x = blockIdx.x * blockDim.x + threadIdx.x;
    if (x >= width) return;
    int bpp = 4;
    unsigned char a = (x >= bpp) ? input[x - bpp] : 0;
    unsigned char b = 0;
    unsigned char c = 0;
    int p = a + b - c;
    int pa = abs(p - a);
    int pb = abs(p - b);
    int pc = abs(p - c);
    unsigned char pr = (pa <= pb && pa <= pc) ? a : (pb <= pc ? b : c);
    output[x] = input[x] - pr;
}

__global__ void deflate_compress_kernel(const unsigned char* input, size_t input_size,
                                        unsigned char* output, size_t* output_size) {
    if (threadIdx.x == 0 && blockIdx.x == 0) {
        output[0] = 0x78;
        output[1] = 0x01;
        size_t out_idx = 2;
        size_t in_idx = 0;
        while (in_idx < input_size) {
            size_t chunk = input_size - in_idx;
            if (chunk > 65535) chunk = 65535;
            unsigned char bfinal = (in_idx + chunk >= input_size) ? 1 : 0;
            output[out_idx++] = bfinal;
            output[out_idx++] = chunk & 0xFF;
            output[out_idx++] = (chunk >> 8) & 0xFF;
            output[out_idx++] = (~chunk) & 0xFF;
            output[out_idx++] = (~(chunk >> 8)) & 0xFF;
            for (size_t i = 0; i < chunk; ++i) {
                output[out_idx++] = input[in_idx++];
            }
        }
        uint32_t s1 = 1, s2 = 0;
        for (size_t i = 0; i < input_size; ++i) {
            s1 = (s1 + input[i]) % 65521;
            s2 = (s2 + s1) % 65521;
        }
        uint32_t adler = (s2 << 16) | s1;
        output[out_idx++] = (adler >> 24) & 0xFF;
        output[out_idx++] = (adler >> 16) & 0xFF;
        output[out_idx++] = (adler >> 8) & 0xFF;
        output[out_idx++] = adler & 0xFF;
        *output_size = out_idx;
    }
}

uint32_t bswap_32(uint32_t x) {
    return ((x >> 24) & 0xff) | ((x << 8) & 0xff0000) | ((x >> 8) & 0xff00) |
           ((x << 24) & 0xff000000);
}

void write_png_chunk(FILE *f, const char* type, const unsigned char* data, uint32_t len) {
    uint32_t len_be = bswap_32(len);
    fwrite(&len_be, 1, 4, f);
    fwrite(type, 1, 4, f);
    if (len > 0) fwrite(data, 1, len, f);
    uint32_t crc = 0xFFFFFFFF;
    for (int i = 0; i < 4; i++) {
        crc ^= type[i];
        for (int j = 0; j < 8; j++)
            crc = (crc & 1) ? (crc >> 1) ^ 0xEDB88320 : crc >> 1;
    }
    for (uint32_t i = 0; i < len; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++)
            crc = (crc & 1) ? (crc >> 1) ^ 0xEDB88320 : crc >> 1;
    }
    crc ^= 0xFFFFFFFF;
    uint32_t crc_be = bswap_32(crc);
    fwrite(&crc_be, 1, 4, f);
}


static int encode_frame_to_png(AVFrame *frame, const char *output_filename) {
    int image_width = frame->width;
    int image_height = frame->height;
    int bpp = 4;
    size_t image_size = image_width * image_height * bpp;
    size_t filtered_size = image_height * (image_width * bpp + 1);

    AVFrame *rgb_frame = av_frame_alloc();
    rgb_frame->format = AV_PIX_FMT_RGBA;
    rgb_frame->width = image_width;
    rgb_frame->height = image_height;
    av_frame_get_buffer(rgb_frame, 32);

    struct SwsContext *sws_ctx = sws_getContext(
        image_width, image_height, (AVPixelFormat)frame->format,
        image_width, image_height, AV_PIX_FMT_RGBA,
        SWS_BILINEAR, NULL, NULL, NULL
    );

    sws_scale(sws_ctx, frame->data, frame->linesize, 0, image_height,
              rgb_frame->data, rgb_frame->linesize);

    unsigned char *d_raw_pixels, *d_filtered_data, *d_compressed_data;
    size_t *d_compressed_size;
    cudaError_t err;
    err = cudaMalloc(&d_raw_pixels, image_size);
    if (err != cudaSuccess) return -6;
    err = cudaMalloc(&d_filtered_data, filtered_size);
    if (err != cudaSuccess) return -6;
    err = cudaMalloc(&d_compressed_data, filtered_size * 2 + 1024);
    if (err != cudaSuccess) return -6;
    err = cudaMalloc(&d_compressed_size, sizeof(size_t));
    if (err != cudaSuccess) return -6;

    unsigned char* host_raw = (unsigned char*)malloc(image_size);
    for (int i = 0; i < image_height; i++) {
        memcpy(host_raw + (i * image_width * bpp),
               rgb_frame->data[0] + (i * rgb_frame->linesize[0]),
               image_width * bpp);
    }

    sws_freeContext(sws_ctx);
    av_frame_free(&rgb_frame);

    cudaMemcpy(d_raw_pixels, host_raw, image_size, cudaMemcpyHostToDevice);

    int threads_per_block = 256;
    int num_blocks = (image_width * bpp + threads_per_block - 1) / threads_per_block;

    for (int y = 0; y < image_height; y++) {
        unsigned char* d_in_row = d_raw_pixels + y * image_width * bpp;
        unsigned char* d_out_row = d_filtered_data + y * (image_width * bpp + 1);
        unsigned char filter_type = PNG_FILTER_SUB;
        cudaMemcpy(d_out_row, &filter_type, 1, cudaMemcpyHostToDevice);
        png_filter_row_kernel<<<num_blocks, threads_per_block>>>(
            d_out_row + 1, d_in_row, image_width * bpp, PNG_FILTER_SUB);
    }

    deflate_compress_kernel<<<1, 1>>>(d_filtered_data, filtered_size,
                                      d_compressed_data, d_compressed_size);
    cudaDeviceSynchronize();

    size_t h_compressed_size = 0;
    cudaMemcpy(&h_compressed_size, d_compressed_size, sizeof(size_t),
               cudaMemcpyDeviceToHost);
    unsigned char *h_compressed_data = (unsigned char*)malloc(h_compressed_size);
    cudaMemcpy(h_compressed_data, d_compressed_data, h_compressed_size,
               cudaMemcpyDeviceToHost);

    FILE *f = fopen(output_filename, "wb");
    if (!f) return -5;
    unsigned char sig[8] = {137, 80, 78, 71, 13, 10, 26, 10};
    fwrite(sig, 1, 8, f);
    unsigned char ihdr[13];
    uint32_t w_be = bswap_32(image_width);
    uint32_t h_be = bswap_32(image_height);
    memcpy(ihdr, &w_be, 4);
    memcpy(ihdr+4, &h_be, 4);
    ihdr[8] = 8; ihdr[9] = 6; ihdr[10] = 0; ihdr[11] = 0; ihdr[12] = 0;
    write_png_chunk(f, "IHDR", ihdr, 13);
    write_png_chunk(f, "IDAT", h_compressed_data, h_compressed_size);
    write_png_chunk(f, "IEND", NULL, 0);

    fclose(f);
    free(host_raw);
    free(h_compressed_data);
    cudaFree(d_raw_pixels);
    cudaFree(d_filtered_data);
    cudaFree(d_compressed_data);
    cudaFree(d_compressed_size);
    return 0;
}

int v2p_init(void) {
    cudaError_t err = cudaFree(0);
    if (err != cudaSuccess) {
        return -6;
    }
    return 0;
}

void v2p_cleanup(void) {
    cudaDeviceReset();
}
int v2p_extract_frame_to_png(const char *video_path, const char *output_png_path,
                             double seek_time_seconds) {
    if (seek_time_seconds < 0) return -3;

    av_log_set_level(AV_LOG_DEBUG);

    AVFormatContext *fmt_ctx = NULL;
    if (avformat_open_input(&fmt_ctx, video_path, NULL, NULL) < 0)
        return -1;
    if (avformat_find_stream_info(fmt_ctx, NULL) < 0) {
        avformat_close_input(&fmt_ctx);
        return -1;
    }

    int stream_idx = av_find_best_stream(fmt_ctx, AVMEDIA_TYPE_VIDEO, -1, -1, NULL, 0);
    if (stream_idx < 0) {
        avformat_close_input(&fmt_ctx);
        return -2;
    }

    int64_t seek_target = (int64_t)(seek_time_seconds * AV_TIME_BASE);
    if (av_seek_frame(fmt_ctx, -1, seek_target, AVSEEK_FLAG_BACKWARD) < 0) {
        avformat_close_input(&fmt_ctx);
        return -3;
    }

    AVCodecContext *dec_ctx = avcodec_alloc_context3(NULL);
    avcodec_parameters_to_context(dec_ctx, fmt_ctx->streams[stream_idx]->codecpar);
    const AVCodec *codec = avcodec_find_decoder(dec_ctx->codec_id);
    if (!codec || avcodec_open2(dec_ctx, codec, NULL) < 0) {
        avcodec_free_context(&dec_ctx);
        avformat_close_input(&fmt_ctx);
        return -4;
    }

    AVPacket *pkt = av_packet_alloc();
    AVFrame *frame = av_frame_alloc();
    int ret = -4;   
    while (av_read_frame(fmt_ctx, pkt) >= 0) {
        if (pkt->stream_index == stream_idx) {
            if (avcodec_send_packet(dec_ctx, pkt) == 0) {
                if (avcodec_receive_frame(dec_ctx, frame) == 0) {
                    ret = encode_frame_to_png(frame, output_png_path);
                    break;
                }
            }
        }
        av_packet_unref(pkt);
    }

    av_frame_free(&frame);
    av_packet_free(&pkt);
    avcodec_free_context(&dec_ctx);
    avformat_close_input(&fmt_ctx);
    return ret;
}