#pragma once
#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_INFO,
    LOG_LEVEL_WARN,
    LOG_LEVEL_ERROR
} LogLevel;

void log_init(void);
void log_message(LogLevel level, const char* function, const char* format, ...);

#define LOG_DEBUG(format, ...) log_message(LOG_LEVEL_DEBUG, __func__, format, ##__VA_ARGS__)
#define LOG_INFO(format, ...)   log_message(LOG_LEVEL_INFO,  __func__, format, ##__VA_ARGS__)
#define LOG_WARN(format, ...)   log_message(LOG_LEVEL_WARN,  __func__, format, ##__VA_ARGS__)
#define LOG_ERROR(format, ...)  log_message(LOG_LEVEL_ERROR, __func__, format, ##__VA_ARGS__)

#ifdef __cplusplus
}
#endif