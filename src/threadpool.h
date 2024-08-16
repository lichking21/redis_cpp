#pragma once

#include <iostream>
#include <cstdint>
#include <vector>
#include <deque>
#include <pthread.h>

class Work {
public:
    void(*f)(void*) = nullptr;
    void *arg = nullptr;
};

class Thread_Pool {
public:
    std::vector<pthread_t> threads;
    std::deque<Work> queue;

    pthread_mutex_t mu;
    pthread_cond_t not_empty;
};

void threadpool_init(Thread_Pool* tp, size_t num_threads);
void threadpool_queue(Thread_Pool* tp, void (*f)(void*), void* arg);