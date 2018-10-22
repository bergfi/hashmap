#pragma once

namespace hashtables {

template<typename KEY>
struct key_accessor {
    static const char* data(KEY const& key) {
        return (const char*)&key;
    }
    static size_t size(KEY const& key) {
        return sizeof(KEY);
    }
};

}
