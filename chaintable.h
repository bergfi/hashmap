#pragma once

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <sys/mman.h>

#include <atomic>
#include <new>

#include "mmapper.h"

namespace chaintable {

template<typename K, typename V>
class HashTableEntry {
public:

    HashTableEntry(K const& key, V const& value, HashTableEntry* next): _key(key), _value(value), _next(next) {
    }

    HashTableEntry* getNext() {
        return _next.load(std::memory_order_relaxed);
    }
    void setNext(HashTableEntry* next) {
        _next.store(next, std::memory_order_relaxed);
    }

public:
    K _key;
    V _value;
    std::atomic<HashTableEntry*> _next;
};

template<typename K, typename V>
class HashTable {
public:
    static constexpr size_t PAGE_SIZE_P2 = 20;
public:
    HashTable(size_t bucketsScale)
        : _buckets(1ULL << bucketsScale)
        , _map(nullptr)
        {
        _map = (decltype(_map))MMapper::mmapForMap(_buckets * sizeof(std::atomic<HashTableEntry<K,V>*>));
    }
public:

    using HTE = HashTableEntry<K,V>;

    size_t insert(K const& key, V const& value) {
//        printf("key:   %zx\n", key);
        size_t e = entry(key);
//        printf("entry: %zx\n", e);
        HashTableEntry<K,V>* current = _map[e].load(std::memory_order_relaxed);
//        printf("cur:   %p\n", current);

        std::atomic<HashTableEntry<K,V>*>* parentLink = &_map[e];
        while(current) {
//            printf("checking existing entry: %zx -> %zx\n", current->_key, current->_value);
            if(current->_key == key) return current->_value;
            parentLink = &(current->_next);
            current = current->getNext();
        }

        HashTableEntry<K,V>* hte = createHTE(key, value, nullptr);
        while(!parentLink->compare_exchange_weak(current, hte, std::memory_order_release, std::memory_order_relaxed)) {
            while(current) {
                if(current->_key == key) {
                    giveMemoryBack(hte);
                    return current->_value;
                }
                parentLink = &current->_next;
                current = current->getNext();
            }
        }
        return value;
    }

    bool get(K const& key, V& value) {
        size_t e = entry(key);
//        printf("entry: %zx\n", e);
        HashTableEntry<K,V>* current = _map[e].load(std::memory_order_relaxed);
//        printf("cur:   %p\n", current);

        while(current) {
//            printf("checking existing entry: %zx -> %zx\n", current->_key, current->_value);
            if(current->_key == key) {
                value = current->_value;
                return true;
            }
            current = current->getNext();
        }

        return false;
    }

    size_t hash(K const& key) {
        return MurmurHash64(key);
    }

    size_t entry(K const& key) {
        return hash(key) & (_buckets-1);
    }

    size_t bucketSize(std::atomic<HashTableEntry<K,V>*>* bucket) {
        size_t s = 0;
        HashTableEntry<K,V>* current = bucket->load(std::memory_order_relaxed);
        while(current) {
            s++;
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
        _slabManager.thread_init();
    }

    HashTableEntry<K,V>* createHTE(K const& key, V const& value, HashTableEntry<K,V>* next) {
        HTE* hte = new(_slabManager.alloc<HTE>()) HTE(key, value, next);
        assert( (((intptr_t)hte)&0x3) == 0);
        return hte;
    }

    template<typename T>
    void giveMemoryBack(T* const& hte) {
        return _slabManager.free(hte);
    }

    ~HashTable() {
        munmap(_map, _buckets * sizeof(std::atomic<HashTableEntry<K,V>*>));
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
    SlabManager _slabManager;
};

}
