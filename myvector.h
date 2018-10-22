#pragma once

#include "allocator.h"
#include "murmurhash.h"
#include <atomic>

struct myvector {

    static const size_t size = 64;

    char data[size];

    bool operator==(myvector const& other) const {
        return memcmp(data, other.data, size) == 0;
    }

    size_t hash() const {
        return MurmurHash64(data, size, 0);
    }

    static int      cmp   (void * a, void * b, void * ctx) {
        myvector* a_ = (myvector*)a;
        myvector* b_ = (myvector*)b;
        bool r = *a_ == *b_;
        return !r;
    }
    static void *   clone (void * a, void * ctx) {
        myvector* a_ = (myvector*)a;
        SlabManager* sm = (SlabManager*)ctx;
        myvector* acopy = new(sm->alloc<4>(sizeof(myvector))) myvector(*a_);
        return (void*)acopy;
    }
    static uint32_t s_hash  (void * a, void * ctx);
    static void     free  (void * a) {
        (void)a;
    }
};

namespace std {

template<>
struct hash<myvector> {
    std::size_t operator()(myvector const& k) const {
        return k.hash();
//        return MurmurHash64(k.s, k.len, 0);
    }
};

std::ostream& operator<<(std::ostream& out, const myvector& obj) {
    out << std::string(obj.data);
    return out;
}

}

uint32_t myvector::s_hash  (void * a, void * ctx) {
    myvector* a_ = (myvector*)a;
    size_t h = a_->hash();
    return (uint32_t)(h ^ (h>>32));
}

namespace hashtables {

template<>
struct key_accessor<myvector> {
    static const char* data(myvector const& key) {
        return (const char*)key.data;
    }
    static size_t size(myvector const& key) {
        return key.size;
    }
};

}

template<>
uint64_t MurmurHash64<myvector> ( myvector const& key ) {
    return MurmurHash64(key.data, key.size, 0);
}

template<>
struct MurmurHasher<myvector> {
    static const size_t significant_digits = 64;

    inline uint64_t operator()(myvector const& k) const
    {
        return k.hash();
    }

    // for DIVINE
    inline std::pair<uint64_t,uint64_t> hash(myvector const& k) const
    {
        return {k.hash(),0};
    }
    inline bool equal(myvector const& one, myvector const& other) {
        return one == other;
    }
};

