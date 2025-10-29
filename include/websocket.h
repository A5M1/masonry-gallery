#pragma once
#include "common.h"

int websocket_init(void);
int websocket_register_socket(int client_socket, char* request_headers);
void websocket_broadcast(const char* msg);
void websocket_broadcast_topic(const char* topic, const char* msg);
void websocket_shutdown(void);
