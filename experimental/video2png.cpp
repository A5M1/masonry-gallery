#include "video2png.h"
extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/hwcontext.h>
#include <libswscale/swscale.h>
}
#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#define PNG_FILTER_SUB 1

static cl_platform_id platform = NULL;
static cl_device_id device = NULL;
static cl_context context = NULL;
static cl_command_queue queue = NULL;
static cl_program program = NULL;
static cl_kernel krn_png_filter = NULL;
static cl_kernel krn_deflate = NULL;


static const char* ocl_kernel_source = R"CLC(
__kernel void png_filter_row_kernel(__global unsigned char* output, __global const unsigned char* input,
                                    int width, int filter_type) {
    int x = get_global_id(0);
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

__kernel void deflate_compress_kernel(__global const unsigned char* input, ulong input_size,
                                      __global unsigned char* output, __global ulong* output_size) {
    if (get_global_id(0) == 0) {
        output[0] = 0x78;
        output[1] = 0x01;
        ulong out_idx = 2;
        ulong in_idx = 0;

        while (in_idx < input_size) {
            ulong chunk = input_size - in_idx;
            if (chunk > 65535) chunk = 65535;

            unsigned char bfinal = (in_idx + chunk >= input_size) ? 1 : 0;
            output[out_idx++] = bfinal;
            output[out_idx++] = chunk & 0xFF;
            output[out_idx++] = (chunk >> 8) & 0xFF;
            output[out_idx++] = (~chunk) & 0xFF;
            output[out_idx++] = (~(chunk >> 8)) & 0xFF;

            for (ulong i = 0; i < chunk; ++i) {
                output[out_idx++] = input[in_idx++];
            }
        }

        uint s1 = 1, s2 = 0;
        for (ulong i = 0; i < input_size; ++i) {
            s1 = (s1 + input[i]) % 65521;
            s2 = (s2 + s1) % 65521;
        }

        uint adler = (s2 << 16) | s1;
        output[out_idx++] = (adler >> 24) & 0xFF;
        output[out_idx++] = (adler >> 16) & 0xFF;
        output[out_idx++] = (adler >> 8) & 0xFF;
        output[out_idx++] = adler & 0xFF;

        *output_size = out_idx;
    }
}
)CLC";

typedef struct {
    const char* name;
    int width;
    int height;
} ResolutionPreset;

static const ResolutionPreset size_map[] = {
    {"1080p", 1920, 1080},
    {"720p", 1280, 720},
    {"480p", 854, 480},
    {"360p", 640, 360}
};

static void get_resolution(const char* preset, int* w, int* h) {
    *w = 0; *h = 0;
    if (!preset) return;
    for (size_t i = 0; i < sizeof(size_map) / sizeof(size_map[0]); i++) {
        if (strcmp(preset, size_map[i].name) == 0) {
            *w = size_map[i].width;
            *h = size_map[i].height;
            return;
        }
    }
    sscanf(preset, "%dx%d", w, h);
}

uint32_t bswap_32(uint32_t x) {
    return ((x >> 24) & 0xff) | ((x << 8) & 0xff0000) | ((x >> 8) & 0xff00) | ((x << 24) & 0xff000000);
}

void write_png_chunk(FILE* f, const char* type, const unsigned char* data, uint32_t len) {
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

static int encode_frame_to_png(AVFrame* frame, const char* output_filename, int target_w, int target_h) {
    int image_width = (target_w > 0) ? target_w : frame->width;
    int image_height = (target_h > 0) ? target_h : frame->height;
    int bpp = 4;
    size_t image_size = image_width * image_height * bpp;
    size_t filtered_size = image_height * (image_width * bpp + 1);

    AVFrame* rgb_frame = av_frame_alloc();
    rgb_frame->format = AV_PIX_FMT_RGBA;
    rgb_frame->width = image_width;
    rgb_frame->height = image_height;
    av_frame_get_buffer(rgb_frame, 32);

    struct SwsContext* sws_ctx = sws_getContext(
        frame->width, frame->height, (AVPixelFormat)frame->format,
        image_width, image_height, AV_PIX_FMT_RGBA,
        SWS_BILINEAR, NULL, NULL, NULL
    );

    sws_scale(sws_ctx, frame->data, frame->linesize, 0, frame->height,
        rgb_frame->data, rgb_frame->linesize);

    unsigned char* host_raw = (unsigned char*)malloc(image_size);
    for (int i = 0; i < image_height; i++) {
        memcpy(host_raw + (i * image_width * bpp),
            rgb_frame->data[0] + (i * rgb_frame->linesize[0]),
            image_width * bpp);
    }

    sws_freeContext(sws_ctx);
    av_frame_free(&rgb_frame);

    cl_int cl_err;
    cl_mem d_raw_pixels = clCreateBuffer(context, CL_MEM_READ_WRITE, image_size, NULL, &cl_err);
    cl_mem d_filtered_data = clCreateBuffer(context, CL_MEM_READ_WRITE, filtered_size, NULL, &cl_err);
    cl_mem d_compressed_data = clCreateBuffer(context, CL_MEM_READ_WRITE, filtered_size * 2 + 1024, NULL, &cl_err);
    cl_mem d_compressed_size = clCreateBuffer(context, CL_MEM_READ_WRITE, sizeof(cl_ulong), NULL, &cl_err);

    if (cl_err != CL_SUCCESS) return -6;

    clEnqueueWriteBuffer(queue, d_raw_pixels, CL_TRUE, 0, image_size, host_raw, 0, NULL, NULL);

    int row_width_bytes = image_width * bpp;
    size_t local_item_size = 256;
    size_t global_item_size = ((row_width_bytes + local_item_size - 1) / local_item_size) * local_item_size;

    for (int y = 0; y < image_height; y++) {
        size_t in_offset = (size_t)y * row_width_bytes;
        size_t out_offset = (size_t)y * (row_width_bytes + 1);
        unsigned char filter_type = PNG_FILTER_SUB;

        clEnqueueWriteBuffer(queue, d_filtered_data, CL_TRUE, out_offset, 1, &filter_type, 0, NULL, NULL);

        cl_buffer_region in_region = { in_offset, (size_t)row_width_bytes };
        cl_buffer_region out_region = { out_offset + 1, (size_t)row_width_bytes };

        cl_mem d_in_row = clCreateSubBuffer(d_raw_pixels, CL_MEM_READ_ONLY, CL_BUFFER_CREATE_TYPE_REGION, &in_region, &cl_err);
        cl_mem d_out_row = clCreateSubBuffer(d_filtered_data, CL_MEM_WRITE_ONLY, CL_BUFFER_CREATE_TYPE_REGION, &out_region, &cl_err);

        clSetKernelArg(krn_png_filter, 0, sizeof(cl_mem), &d_out_row);
        clSetKernelArg(krn_png_filter, 1, sizeof(cl_mem), &d_in_row);
        clSetKernelArg(krn_png_filter, 2, sizeof(int), &row_width_bytes);
        clSetKernelArg(krn_png_filter, 3, sizeof(int), &filter_type);

        clEnqueueNDRangeKernel(queue, krn_png_filter, 1, NULL, &global_item_size, &local_item_size, 0, NULL, NULL);

        clReleaseMemObject(d_in_row);
        clReleaseMemObject(d_out_row);
    }

    size_t global_deflate_size = 1;
    size_t local_deflate_size = 1;
    cl_ulong u_filtered_size = (cl_ulong)filtered_size;

    clSetKernelArg(krn_deflate, 0, sizeof(cl_mem), &d_filtered_data);
    clSetKernelArg(krn_deflate, 1, sizeof(cl_ulong), &u_filtered_size);
    clSetKernelArg(krn_deflate, 2, sizeof(cl_mem), &d_compressed_data);
    clSetKernelArg(krn_deflate, 3, sizeof(cl_mem), &d_compressed_size);

    clEnqueueNDRangeKernel(queue, krn_deflate, 1, NULL, &global_deflate_size, &local_deflate_size, 0, NULL, NULL);
    clFinish(queue);

    cl_ulong h_compressed_size = 0;
    clEnqueueReadBuffer(queue, d_compressed_size, CL_TRUE, 0, sizeof(cl_ulong), &h_compressed_size, 0, NULL, NULL);

    unsigned char* h_compressed_data = (unsigned char*)malloc(h_compressed_size);
    clEnqueueReadBuffer(queue, d_compressed_data, CL_TRUE, 0, h_compressed_size, h_compressed_data, 0, NULL, NULL);

    FILE* f = fopen(output_filename, "wb");
    if (!f) return -5;
    unsigned char sig[8] = { 137, 80, 78, 71, 13, 10, 26, 10 };
    fwrite(sig, 1, 8, f);
    unsigned char ihdr[13];
    uint32_t w_be = bswap_32(image_width);
    uint32_t h_be = bswap_32(image_height);
    memcpy(ihdr, &w_be, 4);
    memcpy(ihdr + 4, &h_be, 4);
    ihdr[8] = 8; ihdr[9] = 6; ihdr[10] = 0; ihdr[11] = 0; ihdr[12] = 0;
    write_png_chunk(f, "IHDR", ihdr, 13);
    write_png_chunk(f, "IDAT", h_compressed_data, (uint32_t)h_compressed_size);
    write_png_chunk(f, "IEND", NULL, 0);

    fclose(f);
    free(host_raw);
    free(h_compressed_data);
    clReleaseMemObject(d_raw_pixels);
    clReleaseMemObject(d_filtered_data);
    clReleaseMemObject(d_compressed_data);
    clReleaseMemObject(d_compressed_size);
    return 0;
}

int v2p_init(void) {
    cl_int err;
    err = clGetPlatformIDs(1, &platform, NULL);
    if (err != CL_SUCCESS) return -6;
    err = clGetDeviceIDs(platform, CL_DEVICE_TYPE_GPU, 1, &device, NULL);
    if (err != CL_SUCCESS) return -6;
    context = clCreateContext(NULL, 1, &device, NULL, NULL, &err);
    if (err != CL_SUCCESS) return -6;
    queue = clCreateCommandQueueWithProperties(context, device, NULL, &err);
    if (err != CL_SUCCESS) return -6;
    size_t source_size = strlen(ocl_kernel_source);
    program = clCreateProgramWithSource(context, 1, &ocl_kernel_source, &source_size, &err);
    err = clBuildProgram(program, 1, &device, NULL, NULL, NULL);
    if (err != CL_SUCCESS) {
        char build_log[4096];
        clGetProgramBuildInfo(program, device, CL_PROGRAM_BUILD_LOG, sizeof(build_log), build_log, NULL);
        printf("OpenCL Build Log:\n%s\n", build_log);
        return -6;
    }
    krn_png_filter = clCreateKernel(program, "png_filter_row_kernel", &err);
    krn_deflate = clCreateKernel(program, "deflate_compress_kernel", &err);
    if (err != CL_SUCCESS) return -6;

    return 0;
}

void v2p_cleanup(void) {
    if (krn_png_filter) clReleaseKernel(krn_png_filter);
    if (krn_deflate) clReleaseKernel(krn_deflate);
    if (program) clReleaseProgram(program);
    if (queue) clReleaseCommandQueue(queue);
    if (context) clReleaseContext(context);
}

int v2p_extract_frame_to_png(const char* video_path, const char* output_png_path,
    double seek_time_seconds, const char* size_preset) {
    if (seek_time_seconds < 0) return -3;
    int target_w = 0;
    int target_h = 0;
    get_resolution(size_preset, &target_w, &target_h);
    av_log_set_level(AV_LOG_DEBUG);
    AVFormatContext* fmt_ctx = NULL;
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

    AVCodecContext* dec_ctx = avcodec_alloc_context3(NULL);
    avcodec_parameters_to_context(dec_ctx, fmt_ctx->streams[stream_idx]->codecpar);
    const AVCodec* codec = avcodec_find_decoder(dec_ctx->codec_id);
    if (!codec || avcodec_open2(dec_ctx, codec, NULL) < 0) {
        avcodec_free_context(&dec_ctx);
        avformat_close_input(&fmt_ctx);
        return -4;
    }

    AVPacket* pkt = av_packet_alloc();
    AVFrame* frame = av_frame_alloc();
    int ret = -4;
    while (av_read_frame(fmt_ctx, pkt) >= 0) {
        if (pkt->stream_index == stream_idx) {
            if (avcodec_send_packet(dec_ctx, pkt) == 0) {
                if (avcodec_receive_frame(dec_ctx, frame) == 0) {
                    ret = encode_frame_to_png(frame, output_png_path, target_w, target_h);
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
