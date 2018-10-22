#pragma once

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <sys/mman.h>

#include <atomic>
#include <new>
#include <vector>

#include "allocator.h"
#include "mmapper.h"
#include "murmurhash.h"

namespace chaintableints {

class HashTableEntry {
public:

    HashTableEntry(size_t key, size_t value, HashTableEntry* next): _key(key), _value(value), _next(next) {
    }

    HashTableEntry* getNext() {
        return _next.load(std::memory_order_relaxed);
    }
    void setNext(HashTableEntry* next) {
        _next.store(next, std::memory_order_relaxed);
    }

public:
    size_t _key;
    size_t _value;
    std::atomic<HashTableEntry*> _next;
};

class HashTable {
public:
    static constexpr size_t PAGE_SIZE_P2 = 20;
public:
    HashTable(size_t bucketsScale): _buckets(1ULL << bucketsScale) {
        _map = (decltype(_map))MMapper::mmapForMap(_buckets * sizeof(std::atomic<HashTableEntry*>));
    }
public:
    size_t insert(size_t key, size_t value) {
//        printf("key:   %zx\n", key);
        size_t e = entry(key);
//        printf("entry: %zx\n", e);
        HashTableEntry* current = _map[e].load(std::memory_order_relaxed);
//        printf("cur:   %p\n", current);

        std::atomic<HashTableEntry*>* parentLink = &_map[e];
        while(current) {
//            printf("checking existing entry: %zx -> %zx\n", current->_key, current->_value);
            if(current->_key == key) return current->_value;
            parentLink = &(current->_next);
            current = current->getNext();
        }

        HashTableEntry* hte = createHTE(key, value, nullptr);
        while(!parentLink->compare_exchange_weak(current, hte, std::memory_order_release, std::memory_order_relaxed)) {
            while(current) {
                if(current->_key == key) {
                    //delete hte;
                    return current->_value;
                }
                parentLink = &current->_next;
                current = current->getNext();
            }
        }
        return value;
    }

    HashTableEntry* createHTE(size_t key, size_t value, HashTableEntry* next) {
        return new(_slabManager.alloc<HashTableEntry>()) HashTableEntry(key, value, next);
    }

    bool get(size_t key, size_t& value) {
        size_t e = entry(key);
//        printf("entry: %zx\n", e);
        HashTableEntry* current = _map[e].load(std::memory_order_relaxed);
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

    size_t entry(size_t key) {
        size_t hash = MurmurHash64(key);
        return hash & (_buckets-1);
    }

    size_t bucketSize(std::atomic<HashTableEntry*>* bucket) {
        size_t s = 0;
        HashTableEntry* current = bucket->load(std::memory_order_relaxed);
        while(current) {
            s++;
            current = current->getNext();
        }
        return s;
    }

    size_t size() {
        size_t s = 0;
        std::atomic<HashTableEntry*>* bucket = _map;
        std::atomic<HashTableEntry*>* end = _map + _buckets;
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

//    ~HashTable() {
//        auto current = _allSlabs.load(std::memory_order_relaxed);
//        while(current) {
//            auto next = current->next;
//            delete current;
//            current = next;
//        }
//        munmap(_map, _buckets*sizeof(std::atomic<HashTableEntry*>));
//    }

    template<typename CONTAINER>
    void getDensityStats(size_t bars, CONTAINER& elements) {

        size_t bucketPerBar = _buckets / bars;
        bucketPerBar += bucketPerBar == 0;

        for(size_t idx = 0; idx < _buckets;) {
            size_t elementsInThisBar = 0;
            size_t max = std::min(_buckets, idx + bucketPerBar);
            for(; idx < max; ++idx) {

                HashTableEntry* bucket = _map[idx].load(std::memory_order_relaxed);
                if(bucket) {
                    HashTableEntry* entry = bucket;
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
            HashTableEntry* bucket = _map[idx].load(std::memory_order_relaxed);
            if(bucket) {
                HashTableEntry* entry = bucket;
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

    void thread_init() {
        _slabManager.thread_init();
    }

private:
    SlabManager _slabManager;
    size_t const _buckets;
    std::atomic<HashTableEntry*>* _map;
};

}
