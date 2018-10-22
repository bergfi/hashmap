#pragma once

#include <sstream>
#include <iostream>

#include "tls.h"
#include <libfrugi/Settings.h>

using namespace libfrugi;

struct slab {
    char* entries;
    char* nextentry;
    char* end;
    slab* next;

    slab(slab* next, size_t bytesNeeded): next(next) {
        size_t map_page_size = 18 << MAP_HUGE_SHIFT;
        entries = nextentry = (char*)mmap(nullptr, bytesNeeded, PROT_READ|PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE | map_page_size, -1, 0);
        if((intptr_t)entries & 0xFFFF000000000000ULL) {
            std::cout << "Warning: allocated memory slab using upper 16 bits" << std::endl;
        }
        assert(entries);
        end = entries + bytesNeeded;
    }

    ~slab() {
        munmap(entries, (end-entries));
    }

    template<typename T>
    __attribute__((always_inline))
    T* alloc() {
        return (T*)alloc<sizeof(T)>(sizeof(T));
    }

    __attribute__((always_inline))
    char* alloc(size_t size) {
        auto mem = nextentry;
        nextentry += size;
        return mem;
    }

//    __attribute__((always_inline))
//    char* alloc(size_t size, size_t alignBytes) {
//        auto mem = nextentry;
//        auto al = (intptr_t)mem % alignBytes;
//        if(al) {
//            mem += alignBytes-al;
//        }
//        nextentry = mem + size;
//        return mem;
//    }

    template<int alignPowerTwo>
    __attribute__((always_inline))
    char* alloc(size_t size) {
        auto mem = nextentry;
        mem += alignPowerTwo-1;
        mem = (char*)(((uintptr_t)mem) & ~(alignPowerTwo-1));
        nextentry = mem + size;
        return mem;
    }

    template<typename T>
    __attribute__((always_inline))
    void free(T* mem) {
        return free((void*)mem, sizeof(T));
    }

    __attribute__((always_inline))
    void free(void* mem, size_t length) {
        (void)mem;
        nextentry -= length;
        assert((char*)mem == nextentry);
    }
};

template<typename T>
class slaballocator: public slab  {
public:

    using value_type = T;

    slaballocator(): slab(nullptr, 1ULL<<34ULL) {
    }

    __attribute__((always_inline))
    T* allocate(size_t n) {
        printf("allocate %zu\n", n); fflush(stdout);
        return (T*)alloc<sizeof(T)>(n);
    }

    __attribute__((always_inline))
    void deallocate(T* p, size_t n) {
        (void)p;
        (void)n;
        //return free(p);
    }
};

class SlabManager {
public:

    SlabManager()
    : _allSlabs(nullptr)
    {

    }

    ~SlabManager() {
        auto current = _allSlabs.load(std::memory_order_relaxed);
        while(current) {
            auto next = current->next;
            delete current;
            current = next;
        }
    }

    template<typename T>
    __attribute__((always_inline))
    T* alloc() {
        slab* mySlab = _slab;
        if(mySlab->nextentry + sizeof(T) > mySlab->end) {
            mySlab = linkNewSlab();
            _slab = mySlab;
        }
        auto r = mySlab->alloc<T>();
        assert(r);
        return r;
    }

    template<typename T>
    __attribute__((always_inline))
    void free(T* mem) {
        return _slab->free(mem);
    }

    template<int alignPowerTwo>
    __attribute__((always_inline))
    char* alloc(size_t size) {
        slab* mySlab = _slab;
        if(mySlab->nextentry + size > mySlab->end) {
            mySlab = linkNewSlab();
            _slab = mySlab;
        }
        auto r = mySlab->alloc<alignPowerTwo>(size);
        assert(r);
        return r;
    }

    __attribute__((always_inline))
    void free(void* mem, size_t length) {
        return _slab->free(mem, length);
    }

    slab* linkNewSlab() {
        //assert(!_slab.get());
        Settings& settings = Settings::global();
        auto mySlab = new slab(_allSlabs.load(std::memory_order_relaxed), 1ULL << (settings["buckets_scale"].asUnsignedValue()+2));
        _slab = mySlab;
        while(!_allSlabs.compare_exchange_weak(mySlab->next, mySlab, std::memory_order_release, std::memory_order_relaxed)) {
        }
        return mySlab;
    }

    __attribute__((always_inline))
    void thread_init() {
        linkNewSlab();
    }
private:
//    static TLS<slab> _slab;
    static __thread slab* _slab;
    std::atomic<slab*> _allSlabs;
};
