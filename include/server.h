#pragma once

#include "common.h"
#ifdef __cplusplus
extern "C" {
#endif

int create_listen_socket(int port);
void derive_paths(const char* argv0);

#ifdef __cplusplus
}
#endif