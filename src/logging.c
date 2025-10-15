#include "logging.h"
#include "directory.h"
#include "thread_pool.h"
#define LOG_DIR "logs"
#define MAX_LOG_MESSAGE_LENGTH 256

LogLevel current_log_level=LOG_LEVEL_DEBUG;
FILE* log_file=NULL;

#ifdef _WIN32
thread_mutex_t log_mutex;
#else
thread_mutex_t log_mutex;
#endif

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
	#ifdef _WIN32
	HANDLE hOut=GetStdHandle(STD_OUTPUT_HANDLE);
	if(hOut!=INVALID_HANDLE_VALUE) {
		DWORD dwMode=0;
		if(GetConsoleMode(hOut, &dwMode)) {
			dwMode |= ENABLE_VIRTUAL_TERMINAL_PROCESSING;
			SetConsoleMode(hOut, dwMode);
		}
	}
	/* Also enable VT processing for stderr so colored output on stderr is handled */
	HANDLE hErr = GetStdHandle(STD_ERROR_HANDLE);
	if(hErr!=INVALID_HANDLE_VALUE) {
		DWORD dwModeErr = 0;
		if(GetConsoleMode(hErr, &dwModeErr)) {
			dwModeErr |= ENABLE_VIRTUAL_TERMINAL_PROCESSING;
			SetConsoleMode(hErr, dwModeErr);
		}
	}
	thread_mutex_init(&log_mutex);
	#else
	thread_mutex_init(&log_mutex);
	#endif
}

void log_message(LogLevel level, const char* function, const char* format, ...) {
	if(level<current_log_level) return;

	thread_mutex_lock(&log_mutex);

	/* thread-safe localtime */
	time_t t = time(NULL);
	char time_str[32];
#ifdef _WIN32
	struct tm tm_buf;
	if(localtime_s(&tm_buf, &t) == 0) {
		strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &tm_buf);
	} else {
		strncpy(time_str, "1970-01-01 00:00:00", sizeof(time_str));
		time_str[sizeof(time_str)-1] = '\0';
	}
#else
	struct tm tm_buf;
	if(localtime_r(&t, &tm_buf) != NULL) {
		strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &tm_buf);
	} else {
		strncpy(time_str, "1970-01-01 00:00:00", sizeof(time_str));
		time_str[sizeof(time_str)-1] = '\0';
	}
#endif

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
	int use_color = 0;
#ifdef _WIN32
	{
		HANDLE hErr = GetStdHandle(STD_ERROR_HANDLE);
		DWORD dw = 0;
		if(hErr != INVALID_HANDLE_VALUE && GetConsoleMode(hErr, &dw)) use_color = 1;
	}
#else
	if(isatty(fileno(stderr))) use_color = 1;
#endif
	const char* color_prefix = use_color ? level_color_str : "";
	const char* color_reset = use_color ? ANSI_COLOR_RESET : "";
	char outbuf[512 + MAX_LOG_MESSAGE_LENGTH];
	int out_len = snprintf(outbuf, sizeof(outbuf), "%s[%s] [%s]%s %s: %s\n",
		color_prefix, time_str, level_str, color_reset, function, message_buffer);
	if(out_len < 0) out_len = 0;
	if((size_t)out_len >= sizeof(outbuf)) out_len = sizeof(outbuf) - 1;
	fwrite(outbuf, 1, out_len, stderr);
	if(log_file) {
		fprintf(log_file, "[%s] [%s] %s: %s\n", time_str, level_str, function, message_buffer);
		fflush(log_file);
	}

	thread_mutex_unlock(&log_mutex);
}