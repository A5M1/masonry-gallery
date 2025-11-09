#include "common.h"
#include "logging.h"
#include "directory.h"
#include "thread_pool.h"
#include "platform.h"
#define LOG_DIR "logs"
#define MAX_LOG_MESSAGE_LENGTH 256
#ifdef DEBUG_DIAGNOSTIC
LogLevel current_log_level=LOG_LEVEL_DEBUG;
#else
LogLevel current_log_level=LOG_LEVEL_INFO;
#endif
FILE* log_file=NULL;

thread_mutex_t log_mutex;

void log_init(void) {
	if(mk_dir(LOG_DIR)!=0) {
		if(errno!=EEXIST) {
			fprintf(stderr, "WARN: Could not create log directory %s\n", LOG_DIR);
		}
	}
	char log_path[PATH_MAX];
	time_t t=time(NULL);
	struct tm* tm=localtime(&t);
	snprintf(log_path, PATH_MAX, "%s/%04d-%02d-%02d_%02d-%02d-%02d.log",
		LOG_DIR, tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
		tm->tm_hour, tm->tm_min, tm->tm_sec);
	log_file=fopen(log_path, "a");
	if(!log_file) {
		fprintf(stderr, "WARN: Could not open log file %s\n", log_path);
	}
	platform_enable_console_colors();
	thread_mutex_init(&log_mutex);
}

void log_message(LogLevel level, const char* function, const char* format, ...) {
	if (level < current_log_level) return;
	thread_mutex_lock(&log_mutex);
	time_t t = time(NULL);
	char time_str[32];
	struct tm tm_buf;
	if (platform_localtime(t, &tm_buf) == 0) {
		strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &tm_buf);
	} else {
		strncpy(time_str, "1970-01-01 00:00:00", sizeof(time_str));
		time_str[sizeof(time_str)-1] = '\0';
	}

	unsigned int pid = platform_get_pid();
	unsigned long tid = platform_get_tid();

	const char* level_str;
	const char* level_color_str=ANSI_COLOR_RESET;

	switch(level) {
		case LOG_LEVEL_DEBUG:
			level_str="DBG";
			level_color_str=ANSI_COLOR_CYAN;
			break;
		case LOG_LEVEL_INFO:
			level_str="INF";
			level_color_str=ANSI_COLOR_GREEN;
			break;
		case LOG_LEVEL_WARN:
			level_str="WRN";
			level_color_str=ANSI_COLOR_YELLOW;
			break;
		case LOG_LEVEL_ERROR:
			level_str="ERR";
			level_color_str=ANSI_COLOR_RED;
			break;
		default:
			level_str="UNKN ";
	}

	char message_buffer[MAX_LOG_MESSAGE_LENGTH+4];
	va_list args;
	va_start(args, format);
	size_t required_size=vsnprintf(message_buffer, sizeof(message_buffer), format, args);
	va_end(args);

	if(required_size>=sizeof(message_buffer)-1) {
		message_buffer[sizeof(message_buffer)-4]='.';
		message_buffer[sizeof(message_buffer)-3]='.';
		message_buffer[sizeof(message_buffer)-2]='.';
		message_buffer[sizeof(message_buffer)-1]='\0';
	}
	int use_color = platform_should_use_colors();
	const char* color_reset = use_color ? ANSI_COLOR_RESET : "";
	const char* ts_color = use_color ? ANSI_COLOR_BRIGHT_BLUE : "";
	const char* func_color = use_color ? ANSI_COLOR_MAGENTA : ""; 
	const char* pid_color = use_color ? ANSI_COLOR_BRIGHT_YELLOW : ""; 
	const char* level_prefix = use_color ? level_color_str : "";
	char outbuf[512 + MAX_LOG_MESSAGE_LENGTH];
	int out_len = 0;
#ifdef DEBUG_DIAGNOSTIC
	if (use_color) {
		out_len = snprintf(outbuf, sizeof(outbuf), "%s[%s]%s %s[%u:%lu]%s %s[%s]%s %s%s%s: %s\n",
			ts_color, time_str, color_reset,
			pid_color, pid, tid, color_reset,
			level_prefix, level_str, color_reset,
			func_color, function, color_reset,
			message_buffer);
	}
	else {
		out_len = snprintf(outbuf, sizeof(outbuf), "[%s] [%u:%lu] [%s] %s: %s\n",
			time_str, pid, tid, level_str, function, message_buffer);
	}
#else
	if (use_color) {
		out_len = snprintf(outbuf, sizeof(outbuf), "%s[%s]%s %s[%s]%s %s\n",
			ts_color, time_str, color_reset,
			level_prefix, level_str, color_reset,
			message_buffer);
	}
	else {
		out_len = snprintf(outbuf, sizeof(outbuf), "[%s] [%s] %s: %s\n",
			time_str, level_str, function, message_buffer);
	}
#endif
	if(out_len < 0) out_len = 0;
	if((size_t)out_len >= sizeof(outbuf)) out_len = sizeof(outbuf) - 1;
	fwrite(outbuf, 1, out_len, stderr);
	if(log_file) {
#ifdef DEBUG_DIAGNOSTIC
		fprintf(log_file, "[%s] [%u:%lu] [%s] %s: %s\n", time_str, pid, tid, level_str, function, message_buffer);
#else
		fprintf(log_file, "[%s] [%s] %s: %s\n", time_str, level_str, function, message_buffer);
#endif
		fflush(log_file);
	}

	thread_mutex_unlock(&log_mutex);
}