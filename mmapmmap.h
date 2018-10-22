#pragma once

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include <atomic>
#include <new>

#include "mmapper.h"

namespace mmapmmap {

template<typename HTE>
struct slab {
    HTE* entries;
    HTE* nextentry;
    HTE* end;
    slab* next;

    slab(slab* next): next(next) {
        size_t bytesNeeded = sizeof(HTE) << 26;
        size_t map_page_size = 18 << MAP_HUGE_SHIFT;
        entries = nextentry = (HTE*)mmap(nullptr, bytesNeeded, PROT_READ|PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE | map_page_size, -1, 0);
        end = entries + bytesNeeded/sizeof(HTE);
        //printf("entries @ %p: %zu bytes, %zu entries\n", (void*)entries, bytesNeeded, bytesNeeded/sizeof(HTE));
        assert(entries);
    }

    ~slab() {
        //printf("unmapped entries @ %p\n", (void*)entries);
        munmap(entries, (end-entries)*sizeof(HTE));
    }

    HTE* create() {
        return nextentry++;
    }
};

template<typename K, typename V>
class HashTableEntry {
public:

    HashTableEntry(K const& key, V const& value): _key(key), _value(value) {
    }

public:
    K _key;
    V _value;
};

template<typename K, typename V>
class HashTable {
public:
    HashTable(size_t bucketsScale)
        : _buckets(1ULL << bucketsScale)
        , _map(nullptr)
        , _allSlabs(nullptr)
        {
        _map = (decltype(_map))MMapper::mmapForMap(_buckets * sizeof(std::atomic<HashTableEntry<K,V>**>));
        std::atomic<HTBucketEntry*>* bucket = _map;
        std::atomic<HTBucketEntry*>* end = _map + _buckets;
        while(bucket < end) {
            auto newEntries = (HTBucketEntry*)MMapper::mmapForBucket();
            bucket->store(newEntries, std::memory_order_relaxed);
            bucket++;
        }
    }
public:

    using HTBucketEntry = std::atomic<HashTableEntry<K,V>*>;

    size_t insert(K const& key, V const& value) {
//        printf("key:   %zx\n", key);
        size_t h = hash(key);
        size_t h16l = hash16LeftFromHash(h);
        size_t e = entryFromhash(h);
//        printf("entry: %zx\n", e);

        HTBucketEntry* entries = _map[e].load(std::memory_order_relaxed);
//        if(__builtin_expect(!entries, 0)) {
//            auto newEntries = (decltype(entries))MMapper::mmapForBucket();
//            if(!_map[e].compare_exchange_strong(entries, newEntries, std::memory_order_release, std::memory_order_relaxed)) {
//                MMapper::munmap(newEntries, 1 << 12);
//            } else {
//                entries = newEntries;
//            }
//        }

        HashTableEntry<K,V>* current = entries->load(std::memory_order_relaxed);
        size_t currentIdx = 0;
//        printf("cur:   %p\n", current);

        while(current) {
//            printf("checking existing entry: %zx -> %zx\n", current->_key, current->_value);
            size_t currentHash = ((intptr_t)current & 0xFFFF000000000000ULL);
            current = (HashTableEntry<K,V>*)((intptr_t)current & 0x0000FFFFFFFFFFFFULL);
            if(currentHash == h16l) {
                if(current->_key == key) return current->_value;
            }
            current = entries[++currentIdx].load(std::memory_order_relaxed);
        }

        HashTableEntry<K,V>* hte = createHTE(key, value);
        HashTableEntry<K,V>* hteWithHash = (HashTableEntry<K,V>*)(((intptr_t)hte)|h16l);
        while(!entries[currentIdx].compare_exchange_weak(current, hteWithHash, std::memory_order_release, std::memory_order_relaxed)) {
            while(current) {
                size_t currentHash = ((intptr_t)current & 0xFFFF000000000000ULL);
                current = (HashTableEntry<K,V>*)((intptr_t)current & 0x0000FFFFFFFFFFFFULL);
                if(currentHash == h16l) {
                    if(current->_key == key) {
                        //delete hte;
                        return current->_value;
                    }
                }
                current = entries[++currentIdx].load(std::memory_order_relaxed);
            }
        }
        return value;
    }

    bool get(K const& key, V& value) {
        size_t h = hash(key);
        size_t e = entryFromhash(h);
//        printf("entry: %zx\n", e);

        HTBucketEntry* entries = _map[e].load(std::memory_order_relaxed);
        if(__builtin_expect(!entries, 0)) {
            return false;
        }

        size_t h16l = hash16LeftFromHash(h);

        HashTableEntry<K,V>* current = entries->load(std::memory_order_relaxed);
        size_t currentIdx = 0;
//        printf("cur:   %p\n", current);

        while(current) {
//            printf("checking existing entry: %zx -> %zx\n", current->_key, current->_value);
            size_t currentHash = ((intptr_t)current & 0xFFFF000000000000ULL);
            current = (HashTableEntry<K,V>*)((intptr_t)current & 0x0000FFFFFFFFFFFFULL);
            if(currentHash == h16l) {
                if(current->_key == key) {
                    value = current->_value;
                    return true;
                }
            }
            current = entries[++currentIdx].load(std::memory_order_relaxed);
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

    size_t bucketSize(HTBucketEntry* bucket) {
        HTBucketEntry* current = bucket;
        while(current->load(std::memory_order_relaxed)) {
            current++;
        }
        return current - bucket;
    }

    size_t size() {
        size_t s = 0;
        std::atomic<HTBucketEntry*>* bucket = _map;
        std::atomic<HTBucketEntry*>* end = _map + _buckets;
        while(bucket < end) {
            s += bucketSize(bucket->load(std::memory_order_relaxed));
            bucket++;
        }
        return s;
    }

    void thread_init() {
        assert(!_slab);
        _slab = new slab<HashTableEntry<K,V>>(_allSlabs.load(std::memory_order_relaxed));
        while(!_allSlabs.compare_exchange_weak(_slab->next, _slab, std::memory_order_release, std::memory_order_relaxed)) {
        }
    }

    static HashTableEntry<K,V>* createHTE(K const& key, V const& value) {
        assert(_slab);
        assert(_slab->nextentry < _slab->end);
        return new(_slab->create()) HashTableEntry<K,V>(key, value);
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
            HTBucketEntry* entries = _map[idx].load(std::memory_order_relaxed);
            if(entries) {
                HTBucketEntry* entry = entries;
                size_t chainSize = 0;
                while(*entry) {
                    chainSize++;
                    entry++;
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

                HTBucketEntry* bucket = _map[idx].load(std::memory_order_relaxed);
                if(bucket) {
                    HTBucketEntry* entry = bucket;
                    size_t chainSize = 0;
                    while(*entry) {
                        chainSize++;
                        entry++;
                    }
                    elementsInThisBar += chainSize;
                }
            }
            elements.push_back(elementsInThisBar);
        }

    }

private:
    size_t _buckets;
    std::atomic<HTBucketEntry*>* _map;
    std::atomic<slab<HashTableEntry<K,V>>*> _allSlabs;

    static __thread slab<HashTableEntry<K,V>>* _slab;
};

template<typename K, typename V>
__thread slab<HashTableEntry<K,V>>* HashTable<K,V>::_slab;

}
