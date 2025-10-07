#include "logging.h"

#define LOG_DIR "logs"
#define MAX_LOG_MESSAGE_LENGTH 256

LogLevel current_log_level=LOG_LEVEL_DEBUG;
FILE* log_file=NULL;

#ifdef _WIN32
HANDLE log_mutex;
#else
pthread_mutex_t log_mutex;
#endif

void log_init(void) {
	if(
#ifdef _WIN32
		_mkdir(LOG_DIR)
#else
		mkdir(LOG_DIR, 0755)
#endif
		!=0) {
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
			dwMode|=ENABLE_VIRTUAL_TERMINAL_PROCESSING;
			SetConsoleMode(hOut, dwMode);
		}
	}
	log_mutex=CreateMutex(NULL, FALSE, NULL);
#else
	pthread_mutex_init(&log_mutex, NULL);
#endif
}

void log_message(LogLevel level, const char* function, const char* format, ...) {
	if(level<current_log_level) return;

#ifdef _WIN32
	WaitForSingleObject(log_mutex, INFINITE);
#else
	pthread_mutex_lock(&log_mutex);
#endif

	time_t t=time(NULL);
	struct tm* tm=localtime(&t);
	char time_str[32];
	strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", tm);

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

	fprintf(stderr, "%s[%s] [%s]%s %s: %s\n", level_color_str, time_str, level_str, ANSI_COLOR_RESET, function, message_buffer);

	if(log_file) {
		fprintf(log_file, "[%s] [%s] %s: %s\n", time_str, level_str, function, message_buffer);
		fflush(log_file);
	}

#ifdef _WIN32
	ReleaseMutex(log_mutex);
#else
	pthread_mutex_unlock(&log_mutex);
#endif
}