#pragma once

#include <utility>
#include <cstddef>
#include <cstdint>

template<typename K>
uint64_t MurmurHash64 ( K * key, int len, unsigned int seed )
{
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;

    uint64_t h = seed ^ (len * m);

    const uint64_t * data = (const uint64_t *)key;
    const uint64_t * end = data + (len/8);

    while(data != end)
    {
        uint64_t k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const unsigned char * data2 = (const unsigned char*)data;

    switch(len & 7)
    {
    case 7: h ^= ((uint64_t)data2[6]) << 48;
    case 6: h ^= ((uint64_t)data2[5]) << 40;
    case 5: h ^= ((uint64_t)data2[4]) << 32;
    case 4: h ^= ((uint64_t)data2[3]) << 24;
    case 3: h ^= ((uint64_t)data2[2]) << 16;
    case 2: h ^= ((uint64_t)data2[1]) << 8;
    case 1: h ^= ((uint64_t)data2[0]);
            h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

template<typename K>
uint64_t MurmurHash64 ( K const& key );

template<>
uint64_t MurmurHash64<size_t> ( size_t const& key );

template<>
uint64_t MurmurHash64<uint32_t> ( uint32_t const& key );

template<typename K> struct MurmurHasher;

struct murmur_hasher {
    static const size_t significant_digits = 64;

    inline uint64_t operator()(const uint64_t k) const
    {
        auto local = k;
        return MurmurHash64(&local, 8, 0);
    }
};

template<>
struct MurmurHasher<uint64_t> {
    static const size_t significant_digits = 64;

    inline uint64_t operator()(const uint64_t k) const
    {
        auto local = k;
        return MurmurHash64(&local, 8, 0);
    }

    // for DIVINE
    inline std::pair<uint64_t,uint64_t> hash(uint64_t const& k) const
    {
        auto local = k;
        return {MurmurHash64(&local, 8, 0),0};
    }
    inline bool equal(uint64_t const& one, uint64_t const& other) {
        return one == other;
    }
};

