#include <iostream>
#include <cassert>
#include "threadpool.h"

static void* worker(void* arg){

    Thread_Pool* tp = (Thread_Pool*)arg;

    while(1){

        pthread_mutex_lock(&tp->mu);

        while(tp->queue.empty())
            pthread_cond_wait(&tp->not_empty, &tp->mu);

        Work w = tp->queue.front();
        tp->queue.pop_front();
        pthread_mutex_unlock(&tp->mu);

        w.f(w.arg);
    }

    return nullptr;
}

void threadpool_init(Thread_Pool* tp, size_t num_threads){

    assert(num_threads > 0);

    int rv = pthread_mutex_init(&tp->mu, nullptr);
    assert(rv == 0);

    rv = pthread_cond_init(&tp->not_empty, nullptr);
    assert(rv == 0);

    tp->threads.resize(num_threads);
    for (size_t i = 0; i < num_threads; ++i){

        int rv = pthread_create(&tp->threads[i], nullptr, &worker, tp);
        assert(rv == 0);
    }
}

void threadpool_queue(Thread_Pool* tp, void (*f)(void*), void* arg){

    Work w;
    w.f = f;
    w.arg = arg;

    pthread_mutex_lock(&tp->mu);
    tp->queue.push_back(w);
    pthread_cond_signal(&tp->not_empty);
    pthread_mutex_unlock(&tp->mu);
}