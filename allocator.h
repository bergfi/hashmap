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
//        std::cout << this << " SlabManager " << inUse() << std::endl;
    }

    ~SlabManager() {
//        std::cout << this << " ~SlabManager " << inUse() << std::endl;
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
            mySlab = linkNewSlab(sizeof(T));
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
        slab* mySlab = ensureSlab(size);
        auto r = mySlab->alloc<alignPowerTwo>(size);
        assert(r);
        return r;
    }

    __attribute__((always_inline))
    void free(void* mem, size_t length) {
        return _slab->free(mem, length);
    }

    __attribute__((always_inline))
    slab* ensureSlab(size_t size) {
        slab* mySlab = _slab;
        if(mySlab == nullptr || mySlab->nextentry + size > mySlab->end) {
            slab* oldSlab = mySlab;
            mySlab = linkNewSlab(size);
            _slab = mySlab;
//            std::cout << "new slab " << mySlab << ", old slab " << oldSlab << std::endl;
        }
        return mySlab;
    }

    slab* linkNewSlab(size_t minimum_size) {
        //assert(!_slab.get());
        Settings& settings = Settings::global();
        size_t size = 1ULL << (settings["buckets_scale"].asUnsignedValue()+2);
        auto mySlab = new slab(_allSlabs.load(std::memory_order_relaxed), std::max(size, minimum_size));
        _slab = mySlab;
        while(!_allSlabs.compare_exchange_weak(mySlab->next, mySlab, std::memory_order_release, std::memory_order_relaxed)) {
        }
        return mySlab;
    }

    __attribute__((always_inline))
    void thread_init() {
        linkNewSlab(0);
    }

    __attribute__((always_inline))
    bool inUse() const {
        return _allSlabs != nullptr;
    }

private:
//    static TLS<slab> _slab;
    static __thread slab* _slab;
    std::atomic<slab*> _allSlabs;
};

template<typename T>
class SlabPerThreadAllocator {
public:
    typedef T value_type;
    typedef value_type* pointer;
    typedef const value_type* const_pointer;
    typedef value_type& reference;
    typedef const value_type& const_reference;
    typedef size_t size_type;
    typedef ptrdiff_t difference_type;
    template<class U> struct rebind {
        typedef SlabPerThreadAllocator<U> other;
    };

    SlabPerThreadAllocator() {
    }

    SlabPerThreadAllocator(const SlabPerThreadAllocator<T>& other) throw() {
        assert(!other.inUse());
    }

    template<typename U> SlabPerThreadAllocator(const SlabPerThreadAllocator<U>& other) throw() {
        assert(!other.inUse());
    }

    template<typename U> SlabPerThreadAllocator(SlabPerThreadAllocator<U>&& other) throw() {
        assert(!other.inUse());
    }

    void operator=(const SlabPerThreadAllocator<T>& other) {
        assert(!other.inUse());
    }

    void operator=(SlabPerThreadAllocator<T>&& other) {
        assert(!other.inUse());
    }

    void destroy(pointer p) {
        ((T*)p)->~T();
    }

    void deallocate(pointer p, size_type n) {
        (void)p;
        (void)n;
    }

    pointer allocate(size_type n, const void* /*hint*/ =0 ) {
        size_t bytes = n*sizeof(T);
        char* r = sm.alloc<16>(bytes);
//        std::cout << "allocate << " << bytes << ": " << (void*)r << " - " << (void*)(r+bytes) << std::endl;
        return (pointer)r;
    }

    bool inUse() const {
        return sm.inUse();
    }
private:
    SlabManager sm;
};

template<typename T>
class StatelessSlabPerThreadAllocator {
public:
    static SlabPerThreadAllocator<T> allocator;

    typedef T value_type;
    typedef value_type* pointer;
    typedef const value_type* const_pointer;
    typedef value_type& reference;
    typedef const value_type& const_reference;
    typedef size_t size_type;
    typedef ptrdiff_t difference_type;
    template<class U> struct rebind {
        typedef StatelessSlabPerThreadAllocator<U> other;
    };

    StatelessSlabPerThreadAllocator() {
    }

    StatelessSlabPerThreadAllocator(const StatelessSlabPerThreadAllocator<T>& other) throw() {
        assert(!other.inUse());
    }

    template<typename U> StatelessSlabPerThreadAllocator(const StatelessSlabPerThreadAllocator<U>& other) throw() {
        assert(!other.inUse());
    }

    template<typename U> StatelessSlabPerThreadAllocator(StatelessSlabPerThreadAllocator<U>&& other) throw() {
        assert(!other.inUse());
    }

    void operator=(const StatelessSlabPerThreadAllocator<T>& other) {
        assert(!other.inUse());
    }

    void operator=(StatelessSlabPerThreadAllocator<T>&& other) {
        assert(!other.inUse());
    }

    void destroy(pointer p) {
        allocator.destroy(p);
    }

    void deallocate(pointer p, size_type n) {
        allocator.deallocate(p, n);
    }

    pointer allocate(size_type n, const void* hint =0 ) {
        allocator.allocate(n, hint);
    }

    bool inUse() const {
        return allocator.inUse();
    }
};

template<typename T>
SlabPerThreadAllocator<T> StatelessSlabPerThreadAllocator<T>::allocator;
