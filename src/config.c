#include "config.h"
#include "common.h"
#include "logging.h"
#include "directory.h"
#include "utils.h"

#define CONFIG_FILE "galleria.conf"

static char** gallery_folders = NULL;
static size_t gallery_folder_count = 0;
int server_port = 3000;

void load_config(void) {
	FILE* f = fopen(CONFIG_FILE, "r");
	if (!f) {
		LOG_INFO("No config file found, using default folder");
		add_gallery_folder(BASE_DIR);
		return;
	}

	char line[PATH_MAX];
	while (fgets(line, sizeof(line), f)) {
		line[strcspn(line, "\n")] = '\0';
		char* s = line;
		while (*s && isspace((unsigned char)*s)) s++;
		if (*s == '#' || *s == '\0') continue;
		char* eq = strchr(s, '=');
		if (eq) {
			*eq = '\0';
			char* key = s; char* val = eq + 1;
			while (*key && isspace((unsigned char)*key)) key++;
			while (*val && isspace((unsigned char)*val)) val++;
			if (ascii_stricmp(key, "port") == 0) {
				int p = atoi(val);
				if (p > 0 && p < 65536) server_port = p;
				LOG_INFO("Loaded server port from config: %d", server_port);
			}
			else {
				LOG_WARN("Unknown config key: %s", key);
			}
			continue;
		}
		if (is_dir(s)) {
			add_gallery_folder(s);
			LOG_INFO("Loaded gallery folder: %s", s);
		}
		else {
			LOG_WARN("Config contains invalid directory: %s", s);
		}
	}

	fclose(f);
	if (gallery_folder_count == 0) {
		add_gallery_folder(BASE_DIR);
	}

	if (gallery_folder_count > 0 && gallery_folders[0] && gallery_folders[0][0]) {
		strncpy(BASE_DIR, gallery_folders[0], PATH_MAX);
		BASE_DIR[PATH_MAX-1] = '\0';
		normalize_path(BASE_DIR);
	}
}

void save_config(void) {
	FILE* f = fopen(CONFIG_FILE, "w");
	if (!f) {
		LOG_ERROR("Failed to save config file: %s", CONFIG_FILE);
		return;
	}

	fprintf(f, "# Galleria configuration file\n");
	fprintf(f, "# Key=value entries supported (e.g. port=3000)\n");
	fprintf(f, "# Each other non-comment line should contain a path to a gallery folder\n\n");

	fprintf(f, "port=%d\n", server_port);

	for (size_t i = 0; i < gallery_folder_count; i++) {
		fprintf(f, "%s\n", gallery_folders[i]);
	}

	fclose(f);
	LOG_DEBUG("Config saved with %zu gallery folders", gallery_folder_count);
}

void add_gallery_folder(const char* path) {
	for (size_t i = 0; i < gallery_folder_count; i++) {
		if (strcmp(gallery_folders[i], path) == 0) {
			LOG_INFO("Folder already in config: %s", path);
			return;
		}
	}
	gallery_folder_count++;
	gallery_folders = realloc(gallery_folders, gallery_folder_count * sizeof(char*));
	if (!gallery_folders) {
		LOG_ERROR("Failed to realloc gallery folders array for %zu folders", gallery_folder_count);
		gallery_folder_count--;
		return;
	}
	gallery_folders[gallery_folder_count - 1] = strdup(path);
	if (!gallery_folders[gallery_folder_count - 1]) {
		LOG_ERROR("Failed to duplicate gallery folder path: %s", path);
		gallery_folder_count--;
		return;
	}
	LOG_DEBUG("Added gallery folder: %s", path);

	save_config();
}

bool is_gallery_folder(const char* path) {
	char path_real[PATH_MAX];
	if (!real_path(path, path_real)) {
		return false;
	}

	for (size_t i = 0; i < gallery_folder_count; i++) {
		char folder_real[PATH_MAX];
		if (real_path(gallery_folders[i], folder_real)) {
			if (safe_under(folder_real, path_real)) {
				return true;
			}
		}
	}

	return false;
}

char** get_gallery_folders(size_t* count) {
	*count = gallery_folder_count;
	return gallery_folders;
}