#pragma once

void start_thread_pool(int nworkers);
void enqueue_job(int client_socket);