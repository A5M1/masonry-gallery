#pragma once
#include "common.h"
#include <stdio.h>
#include <stdarg.h>

/**
 * @file logging.h
 * @brief Simple, leveled logging API for C applications.
 *
 * This module provides functions and macros for emitting formatted
 * log messages at various severity levels (DEBUG, INFO, WARN, ERROR).
 *
 * Each log message automatically includes:
 *   - A timestamp (YYYY-MM-DD HH:MM:SS)
 *   - The log level
 *   - The calling function name
 *
 * Example:
 * @code
 *   log_init();
 *   LOG_INFO("Program started");
 *   LOG_DEBUG("Value = %d", 42);
 *   LOG_ERROR("File not found: %s", path);
 * @endcode
 */

 /**
  * @enum LogLevel
  * @brief Represents the severity of a log message.
  */
typedef enum {
    LOG_LEVEL_DEBUG,  /**< Detailed debugging messages. */
    LOG_LEVEL_INFO,   /**< General informational messages. */
    LOG_LEVEL_WARN,   /**< Warnings about potential issues. */
    LOG_LEVEL_ERROR   /**< Errors or failures requiring attention. */
} LogLevel;

/**
 * @brief Initialize the logging system.
 *
 * Opens the default log file ("application.log") for appending.
 * If the file cannot be opened, output falls back to stderr.
 *
 * This function should be called once before using other logging functions.
 * If not called manually, it will be automatically invoked on first use.
 *
 * Example:
 * @code
 *   log_init();
 * @endcode
 */
void log_init(void);

/**
 * @brief Write a formatted log message.
 *
 * Used internally by logging macros (LOG_DEBUG, LOG_INFO, etc.).
 * You may also call it directly if you need custom behavior.
 *
 * @param level    The log level (e.g., LOG_LEVEL_ERROR).
 * @param function Name of the calling function (typically __func__).
 * @param format   printf-style format string.
 * @param ...      Additional arguments corresponding to the format string.
 *
 * Example:
 * @code
 *   log_message(LOG_LEVEL_INFO, __func__, "Loaded %d records", record_count);
 * @endcode
 */
void log_message(LogLevel level, const char* function, const char* format, ...);

/**
 * @def LOG_DEBUG(format, ...)
 * @brief Log a DEBUG-level message.
 *
 * Expands to a call to log_message() with LOG_LEVEL_DEBUG
 * and automatically includes the current function name.
 *
 * @param format printf-style message format string.
 * @param ...    Optional arguments for the format string.
 *
 * Example:
 * @code
 *   LOG_DEBUG("Initializing module with id=%d", id);
 * @endcode
 */
#define LOG_DEBUG(format, ...) log_message(LOG_LEVEL_DEBUG, __func__, format, ##__VA_ARGS__)

 /**
  * @def LOG_INFO(format, ...)
  * @brief Log an INFO-level message.
  *
  * Typically used for general application events or progress updates.
  *
  * Example:
  * @code
  *   LOG_INFO("Server listening on port %d", port);
  * @endcode
  */
#define LOG_INFO(format, ...)  log_message(LOG_LEVEL_INFO,  __func__, format, ##__VA_ARGS__)

  /**
   * @def LOG_WARN(format, ...)
   * @brief Log a WARN-level message.
   *
   * Used for recoverable problems or unexpected states that do not
   * prevent execution but should be noted.
   *
   * Example:
   * @code
   *   LOG_WARN("Configuration key '%s' missing, using default.", key);
   * @endcode
   */
#define LOG_WARN(format, ...)  log_message(LOG_LEVEL_WARN,  __func__, format, ##__VA_ARGS__)

   /**
    * @def LOG_ERROR(format, ...)
    * @brief Log an ERROR-level message.
    *
    * Used for critical issues or failures that affect functionality.
    *
    * Example:
    * @code
    *   LOG_ERROR("Unable to open file: %s", filename);
    * @endcode
    */
#define LOG_ERROR(format, ...) log_message(LOG_LEVEL_ERROR, __func__, format, ##__VA_ARGS__)
