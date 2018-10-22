#pragma once

#include "allocator.h"
#include "murmurhash.h"
#include <atomic>

struct my_string {
    char* s;
    size_t len;

//    static std::atomic<int> total_copies;
//    static std::atomic<int> total_moves;

    my_string(): s(nullptr), len(0) {
    }

    my_string(char* s, size_t len): s(s), len(len) {
    }

    my_string(my_string const& other) {
        len = other.len;
        s = strndup(other.s, len);
//        total_copies++;
    }

    my_string(my_string&& other) {
        abort();
        std::swap(s, other.s);
        std::swap(len, other.len);
//        total_moves++;
    }

    ~my_string() {
        if(s) free(s);
    }

    my_string const& operator=(my_string const& other) {
        len = other.len;
        s = strndup(other.s, len);
        *this;
//        total_copies++;
    }

    my_string& operator=(my_string&& other) {
        abort();
        std::swap(s, other.s);
        std::swap(len, other.len);
//        total_moves++;
    }

    bool operator==(my_string const& other) const {
        if(len != other.len) return false;
        return strncmp(s, other.s, len) == 0;
    }

    size_t hash() const {
        return MurmurHash64(s, len, 0);
    }

    static int      cmp   (void * a, void * b, void * ctx) {
        my_string* a_ = (my_string*)a;
        my_string* b_ = (my_string*)b;
        bool r = *a_ == *b_;
        return !r;
    }
    static void *   clone (void * a, void * ctx) {
        my_string* a_ = (my_string*)a;
        SlabManager* sm = (SlabManager*)ctx;
        my_string* acopy = new(sm->alloc<4>(sizeof(my_string))) my_string(*a_);
        return (void*)acopy;
    }
    static uint32_t s_hash  (void * a, void * ctx);
    static void     free  (void * a) {
        (void)a;
    }
};

namespace std {

template <>
struct hash<my_string> {
    std::size_t operator()(my_string const& k) const {
        return k.hash();
//        return MurmurHash64(k.s, k.len, 0);
    }
};

std::ostream& operator<<(std::ostream& out, const my_string& obj) {
    out << std::string(obj.s);
    return out;
}

}

uint32_t my_string::s_hash  (void * a, void * ctx) {
    my_string* a_ = (my_string*)a;
    size_t h = a_->hash();
    return (uint32_t)(h ^ (h>>32));
}

namespace hashtables {

template<>
struct key_accessor<my_string> {
    static const char* data(my_string const& key) {
        return (const char*)key.s;
    }
    static size_t size(my_string const& key) {
        return key.len;
    }
};

}

template<>
uint64_t MurmurHash64<my_string> ( my_string const& key ) {
    return MurmurHash64(key.s, key.len, 0);
}

template<>
struct MurmurHasher<my_string> {
    static const size_t significant_digits = 64;

    inline uint64_t operator()(my_string const& k) const
    {
        return k.hash();
    }
};
