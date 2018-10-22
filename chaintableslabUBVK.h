#pragma once

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include <atomic>
#include <new>

#include "mmapper.h"
#include "key_accessor.h"

namespace chaintablegenericUBVK {

template<typename HTE>
struct slab {
    char* entries;
    char* nextentry;
    char* end;
    slab* next;

    slab(slab* next): next(next) {
        size_t bytesNeeded = sizeof(HTE) << 29;
        size_t map_page_size = 18 << MAP_HUGE_SHIFT;
        entries = nextentry = (char*)mmap(nullptr, bytesNeeded, PROT_READ|PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE | map_page_size, -1, 0);
        end = entries + bytesNeeded/sizeof(HTE);
        //printf("entries @ %p: %zu bytes, %zu entries\n", (void*)entries, bytesNeeded, bytesNeeded/sizeof(HTE));
        assert(entries);
    }

    ~slab() {
        //printf("unmapped entries @ %p\n", (void*)entries);
        munmap(entries, (end-entries)*sizeof(HTE));
    }

    HTE* create(size_t length) {
        auto mem = nextentry;
        mem += 64-1;
        mem = (char*)(((uintptr_t)mem) & ~(64-1));
        size_t size = sizeof(HTE) + length;
        size += 64-1;
        size &= ~(64-1);
        nextentry = mem + size;
        if(nextentry > end) {
            std::cerr << "slab ran out of memory, tried to allocate more than " << ((end-entries)>>20) << "MiB" << std::endl;
            abort();
        }
        return (HTE*)mem;
//        char* mem = nextentry;
//        nextentry += sizeof(HTE) + length;
//        return (HTE*)mem;
    }
};

template<typename K, typename V>
class HashTableEntry {
public:

    HashTableEntry(size_t length, const char* keyData, V const& value, HashTableEntry* next): _value(value), _length(length), _next(next) {
        memmove(_keyData, keyData, length);
    }

    HashTableEntry* getNext() {
        return _next.load(std::memory_order_relaxed);
    }
    void setNext(HashTableEntry* next) {
        _next.store(next, std::memory_order_relaxed);
    }

    size_t size() const {
        return sizeof(HashTableEntry) + _length;
    }

    bool matches(size_t length, const char* keyData) const {
        if( _length != length) return false;
        return !memcmp(_keyData, keyData, length);
    }

public:
    V _value;
    size_t _length;
    std::atomic<HashTableEntry*> _next;
    char _keyData[0];
};

template<typename K, typename V>
class HashTable {
public:
    static constexpr size_t PAGE_SIZE_P2 = 20;
public:
    HashTable(size_t bucketsScale)
        : _buckets(1ULL << bucketsScale)
        , _map(nullptr)
        , _allSlabs(nullptr)
        {
        _map = (decltype(_map))MMapper::mmapForMap(_buckets * sizeof(std::atomic<HashTableEntry<K,V>*>));
    }
public:
    size_t insert(K const& key, V const& value) {
//        printf("key:   %zx\n", key);
        size_t h = hash(key);
        size_t h16l = hash16LeftFromHash(h);
        size_t e = entryFromhash(h);
//        printf("entry: %zx\n", e);
        HashTableEntry<K,V>* current = _map[e].load(std::memory_order_relaxed);
//        printf("cur:   %p\n", current);

        size_t length = hashtables::key_accessor<K>::size(key);

        std::atomic<HashTableEntry<K,V>*>* parentLink = &_map[e];
        while(current) {
//            printf("checking existing entry: %zx -> %zx\n", current->_key, current->_value);
            size_t currentHash = ((intptr_t)current & 0xFFFF000000000000ULL);
            current = (HashTableEntry<K,V>*)((intptr_t)current & 0x0000FFFFFFFFFFFFULL);
            if(currentHash == h16l) {
                if(current->matches(length, hashtables::key_accessor<K>::data(key))) return current->_value;
            }
            parentLink = &(current->_next);
            current = current->getNext();
        }

        HashTableEntry<K,V>* hte = createHTE(length, hashtables::key_accessor<K>::data(key), value, nullptr);
        HashTableEntry<K,V>* hteWithHash = (HashTableEntry<K,V>*)(((intptr_t)hte)|h16l);
        while(!parentLink->compare_exchange_weak(current, hteWithHash, std::memory_order_release, std::memory_order_relaxed)) {
            while(current) {
                size_t currentHash = ((intptr_t)current & 0xFFFF000000000000ULL);
                current = (HashTableEntry<K,V>*)((intptr_t)current & 0x0000FFFFFFFFFFFFULL);
                if(currentHash == h16l) {
                    if(current->matches(length, hashtables::key_accessor<K>::data(key))) {
                        //delete hte;
                        return current->_value;
                    }
                }
                parentLink = &current->_next;
                current = current->getNext();
            }
        }
        return value;
    }

    bool get(K const& key, V& value) {
        size_t h = hash(key);
        size_t h16l = hash16LeftFromHash(h);
        size_t e = entryFromhash(h);
//        printf("entry: %zx\n", e);
        HashTableEntry<K,V>* current = _map[e].load(std::memory_order_relaxed);
//        printf("cur:   %p\n", current);
        size_t length = hashtables::key_accessor<K>::size(key);

        while(current) {
//            printf("checking existing entry: %zx -> %zx\n", current->_key, current->_value);
            size_t currentHash = ((intptr_t)current & 0xFFFF000000000000ULL);
            current = (HashTableEntry<K,V>*)((intptr_t)current & 0x0000FFFFFFFFFFFFULL);
            if(currentHash == h16l) {
                if(current->matches(length, hashtables::key_accessor<K>::data(key))) {
                    value = current->_value;
                    return true;
                }
            }
            current = current->getNext();
        }

        return false;
    }

    size_t hash16LeftFromHash(size_t h) const {
//        h ^= h << 32ULL;
//        h ^= h << 16ULL;
        return h & 0xFFFF000000000000ULL;
    }

    size_t entryFromhash(size_t const& h) {
        return h & (_buckets-1);
    }

    size_t hash(K const& key) {
        return MurmurHash64(key);
    }

    size_t bucketSize(std::atomic<HashTableEntry<K,V>*>* bucket) {
        size_t s = 0;
        HashTableEntry<K,V>* current = bucket->load(std::memory_order_relaxed);
        while(current) {
            s++;
            current = (HashTableEntry<K,V>*)((intptr_t)current & 0x0000FFFFFFFFFFFFULL);
            current = current->getNext();
        }
        return s;
    }

    size_t size() {
        size_t s = 0;
        std::atomic<HashTableEntry<K,V>*>* bucket = _map;
        std::atomic<HashTableEntry<K,V>*>* end = _map + _buckets;
        while(bucket < end) {
            s += bucketSize(bucket);
            bucket++;
        }
        return s;
    }

    void printStatistics() {
        printf("ht stats\n");
        printf("size = %zu\n", size());
    }

    void thread_init() {
        assert(!_slab);
        _slab = new slab<HashTableEntry<K,V>>(_allSlabs.load(std::memory_order_relaxed));
        while(!_allSlabs.compare_exchange_weak(_slab->next, _slab, std::memory_order_release, std::memory_order_relaxed)) {
        }
    }

    static HashTableEntry<K,V>* createHTE(size_t length, const char* keyData, V const& value, HashTableEntry<K,V>* next) {
        assert(_slab);
        assert(_slab->nextentry < _slab->end);
        return new(_slab->create(length)) HashTableEntry<K,V>(length, keyData, value, next);
    }

    ~HashTable() {
        auto current = _allSlabs.load(std::memory_order_relaxed);
        while(current) {
            auto next = current->next;
            delete current;
            current = next;
        }
    }

    struct stats {
        size_t size;
        size_t usedBuckets;
        size_t collisions;
        size_t longestChain;
        double avgChainLength;
    };

    void getStats(stats& s) {
        s.size = 0;
        s.usedBuckets = 0;
        s.collisions = 0;
        s.longestChain = 0;
        s.avgChainLength = 0.0;

        for(size_t idx = 0; idx < _buckets; ++idx) {
            HashTableEntry<K,V>* bucket = _map[idx].load(std::memory_order_relaxed);
            if(bucket) {
                HashTableEntry<K,V>* entry = bucket;
                size_t chainSize = 0;
                while(entry) {
                    chainSize++;
                    entry = (HashTableEntry<K,V>*)((intptr_t)entry & 0x0000FFFFFFFFFFFFULL);
                    entry = entry->_next.load(std::memory_order_relaxed);
                }
                s.usedBuckets++;
                s.size += chainSize;
                s.collisions += chainSize - 1;
                if(chainSize > s.longestChain) s.longestChain = chainSize;
            }
        }
        if(_buckets > 0) {
            s.avgChainLength = (double)s.size / (double)_buckets;
        }
    }

    template<typename CONTAINER>
    void getDensityStats(size_t bars, CONTAINER& elements) {

        size_t bucketPerBar = _buckets / bars;
        bucketPerBar += bucketPerBar == 0;

        for(size_t idx = 0; idx < _buckets;) {
            size_t elementsInThisBar = 0;
            size_t max = std::min(_buckets, idx + bucketPerBar);
            for(; idx < max; ++idx) {

                HashTableEntry<K,V>* bucket = _map[idx].load(std::memory_order_relaxed);
                if(bucket) {
                    HashTableEntry<K,V>* entry = bucket;
                    size_t chainSize = 0;
                    while(entry) {
                        chainSize++;
                        entry = (HashTableEntry<K,V>*)((intptr_t)entry & 0x0000FFFFFFFFFFFFULL);
                        entry = entry->_next.load(std::memory_order_relaxed);
                    }
                    elementsInThisBar += chainSize;
                }
            }
            elements.push_back(elementsInThisBar);
        }

    }

private:
    size_t _buckets;
    std::atomic<HashTableEntry<K,V>*>* _map;
    std::atomic<slab<HashTableEntry<K,V>>*> _allSlabs;

    static __thread slab<HashTableEntry<K,V>>* _slab;
};

template<typename K, typename V>
__thread slab<HashTableEntry<K,V>>* HashTable<K,V>::_slab;

}
