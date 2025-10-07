#include "server.h"
#include "directory.h"

char BASE_DIR[PATH_MAX]={ 0 };
char VIEWS_DIR[PATH_MAX]={ 0 };
char JS_DIR[PATH_MAX]={ 0 };
char CSS_DIR[PATH_MAX]={ 0 };
char BUNDLED_FILE[PATH_MAX]={ 0 };

static void join_path(char* dest, size_t size, const char* base, const char* sub) {
	size_t len=strlen(base);
	if(len>0&&(base[len-1]=='/'||base[len-1]=='\\')) {
		snprintf(dest, size, "%s%s", base, sub);
	}
	else {
		snprintf(dest, size, "%s%c%s", base, DIR_SEP, sub);
	}
}

void derive_paths() {
	char exe_path[PATH_MAX];

#ifdef _WIN32
	if(!GetModuleFileNameA(NULL, exe_path, PATH_MAX)) exit(1);
#else
	if(argv0[0]=='/'||argv0[0]=='.') {
		if(!realpath(argv0, exe_path)) exit(1);
	}
	else {
		char* path_env=getenv("PATH");
		if(!path_env) exit(1);
		char* p=strdup(path_env);
		char* dir=strtok(p, ":");
		int found=0;
		while(dir) {
			snprintf(exe_path, PATH_MAX, "%s/%s", dir, argv0);
			if(access(exe_path, X_OK)==0) {
				found=1; break;
			}
			dir=strtok(NULL, ":");
		}
		free(p);
		if(!found) exit(1);
	}
#endif

	char* slash=strrchr(exe_path, DIR_SEP);
	if(slash) *slash='\0';

	strncpy(BASE_DIR, "E:\\F", PATH_MAX);

	join_path(VIEWS_DIR, PATH_MAX, exe_path, "views");
	join_path(JS_DIR, PATH_MAX, exe_path, "public/js");
	join_path(CSS_DIR, PATH_MAX, exe_path, "public/css");
	join_path(BUNDLED_FILE, PATH_MAX, exe_path, "public/bundle/f.js");

	normalize_path(BASE_DIR);
	normalize_path(VIEWS_DIR);
	normalize_path(JS_DIR);
	normalize_path(CSS_DIR);
	normalize_path(BUNDLED_FILE);
}

int create_listen_socket(int port) {
	int s=socket(AF_INET, SOCK_STREAM, 0);
	if(s<0) {
		exit(1);
	}
	int opt=1;
#ifndef _WIN32
	setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#else
	setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));
#endif
	struct sockaddr_in a;
	memset(&a, 0, sizeof(a));
	a.sin_family=AF_INET;
	a.sin_addr.s_addr=htonl(INADDR_ANY);
	a.sin_port=htons((unsigned short)port);
	if(bind(s, (struct sockaddr*)&a, sizeof(a))<0) {
		exit(1);
	}
	if(listen(s, 1024)<0) {
		exit(1);
	}
	return s;
}