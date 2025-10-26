#include "config.h"
#include "logging.h"
#include "directory.h"
#include "utils.h"

#define CONFIG_FILE "galleria.conf"

static char** gallery_folders = NULL;
static size_t gallery_folder_count = 0;

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
		if (line[0] != '#' && line[0] != '\0') {
			if (is_dir(line)) {
				add_gallery_folder(line);
				LOG_INFO("Loaded gallery folder: %s", line);
			}
			else {
				LOG_WARN("Config contains invalid directory: %s", line);
			}
		}
	}

	fclose(f);
	if (gallery_folder_count == 0) {
		add_gallery_folder(BASE_DIR);
	}
}

void save_config(void) {
	FILE* f = fopen(CONFIG_FILE, "w");
	if (!f) {
		LOG_ERROR("Failed to save config file: %s", CONFIG_FILE);
		return;
	}

	fprintf(f, "# Galleria configuration file\n");
	fprintf(f, "# Each line should contain a path to a gallery folder\n\n");

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
	gallery_folders[gallery_folder_count - 1] = strdup(path);
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