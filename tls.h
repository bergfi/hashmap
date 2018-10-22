#pragma once

#include <pthread.h>

template<typename T>
class TLS {
public:

    TLS() {
        pthread_key_create(&_key, NULL);
    }

    ~TLS() {
        pthread_key_delete(_key);
    }

    void operator=(T* t) {
        pthread_setspecific(_key, t);
    }

    T& operator*() {
        return *(T*)pthread_getspecific(_key);
    }

    T* operator->() {
        return (T*)pthread_getspecific(_key);
    }

    T* get() {
        return (T*)pthread_getspecific(_key);
    }

private:
    pthread_key_t _key;
};
