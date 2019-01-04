#pragma once

namespace hashtables {

template<typename KEY>
struct key_accessor {

    __attribute__((always_inline))
    static const char* data(KEY const& key) {
        return (const char*)&key;
    }

    __attribute__((always_inline))
    static size_t size(KEY const& key) {
        return sizeof(KEY);
    }
};

}
