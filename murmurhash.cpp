#include "murmurhash.h"

template<typename K>
uint64_t MurmurHash64 ( K const& key );

template<>
uint64_t MurmurHash64<size_t> ( size_t const& key ) {
    return key;
    //return MurmurHash64(&key, sizeof(size_t), 0);
}

template<>
uint64_t MurmurHash64<uint32_t> ( uint32_t const& key ) {
    return key;
    //return MurmurHash64(&key, sizeof(size_t), 0);
}
