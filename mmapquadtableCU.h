#pragma once

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <sys/mman.h>

#include <atomic>
#include <new>

#include "allocator.h"
#include "mmapper.h"
#include "murmurhash.h"

#define CACHE_LINE_SIZE_BP2 6
#define CACHE_LINE_SIZE_IN_BYTES (1<<CACHE_LINE_SIZE_BP2)

namespace mmapquadtableCU {

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
    static constexpr size_t PAGE_SIZE_P2 = 20;
public:
    HashTable(size_t bucketsScale)
    : _bucketsScale(bucketsScale)
    , _buckets((1ULL << _bucketsScale)/_entriesPerBucket)
    , _bucketsMask((_buckets-1ULL))
    , _entries(_buckets*_entriesPerBucket)
    , _entriesMask( (_entries-1ULL))
    {
        _map = (decltype(_map))MMapper::mmapForMap(_buckets * _bucketSize);
    }
public:

    template<typename T>
    static size_t getHash(T* ptr) {
        return ((intptr_t)ptr & 0xFFFF000000000000ULL);
    }

    template<typename T>
    static T* getPtr(T* ptr) {
        return (T*)((intptr_t)ptr & 0x0000FFFFFFFFFFFFULL);
    }

    template<typename T>
    static T* makePtrWithHash(T* ptr, size_t h) {
        return (T*)(((intptr_t)ptr)|h);
    }

    size_t insert(K const& key, V const& value) {

        size_t h = hash(key);
        size_t h16l = hash16LeftFromHash(h);
        size_t e = entryFromhash(h);
        HashTableEntry<K,V>* current = _map[e].load(std::memory_order_relaxed);

        size_t base = e & (~(_entriesPerBucket-1));
        e -= base;

        size_t end = e;
        size_t increment = 0;

        while(current) {
            size_t currentHash = getHash(current);
            current = getPtr(current);
            if(currentHash == h16l) {
                if(current->_key == key) return current->_value;
            }
            e = (e+1) & (_entriesPerBucket-1);
            if(e==end) {
                base += _entriesPerBucket * (1 + increment * 2);
                base &= _entriesMask;
                increment++;
            }
            current = _map[base+e].load(std::memory_order_relaxed);
        }

        HashTableEntry<K,V>* hte = createHTE(key, value);
        HashTableEntry<K,V>* hteWithHash = makePtrWithHash(hte, h16l);
        while(!_map[base+e].compare_exchange_weak(current, hteWithHash, std::memory_order_release, std::memory_order_relaxed)) {
            while(current) {
                size_t currentHash = getHash(current);
                current = getPtr(current);
                if(currentHash == h16l) {
                    if(current->_key == key) return current->_value;
                }
                e = (e+1) & (_entriesPerBucket-1);
                if(e==end) {
                    base += _entriesPerBucket * (1 + increment * 2);
                    base &= _entriesMask;
                    increment++;
                }
                current = _map[base+e].load(std::memory_order_relaxed);
            }
        }
        return value;
    }

    bool get(K const& key, V& value) {
        size_t h = hash(key);
        size_t h16l = hash16LeftFromHash(h);
        size_t e = entryFromhash(h);
        HashTableEntry<K,V>* current = _map[e].load(std::memory_order_relaxed);

        size_t base = e & (~(_entriesPerBucket-1));
        e -= base;

        size_t end = e;
        size_t increment = 0;

        while(current) {
            size_t currentHash = getHash(current);
            current = getPtr(current);
            if(currentHash == h16l) {
                if(current->_key == key) {
                    value = current->_value;
                    return true;
                }
            }
            e = (e+1) & (_entriesPerBucket-1);
            if(e==end) {
                base += _entriesPerBucket * (1 + increment * 2);
                base &= _entriesMask;
                increment++;
            }
            current = _map[base+e].load(std::memory_order_relaxed);
        }
        return false;
    }

    size_t hash16LeftFromHash(size_t h) const {
//        h ^= h << 32ULL;
//        h ^= h << 16ULL;
        return h & 0xFFFF000000000000ULL;
    }

    size_t entryFromhash(size_t const& h) {
        return h & _entriesMask;
    }

    size_t hash(K const& key) {
        return MurmurHash64(key);
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

    HashTableEntry<K,V>* createHTE(K const& key, V const& value) {
        return new(_slabManager.alloc<HashTableEntry<K,V>>()) HashTableEntry<K,V>(key, value);
    }

    ~HashTable() {
        munmap(_map, _buckets * _bucketSize);
    }

    template<typename CONTAINER>
    void getDensityStats(size_t bars, CONTAINER& elements) {

        size_t entriesPerBar = _entries / bars;
        entriesPerBar += entriesPerBar == 0;

        for(size_t idx = 0; idx < _entries;) {
            size_t elementsInThisBar = 0;
            size_t max = std::min(_entries, idx + entriesPerBar);
            for(; idx < max; idx += _entriesPerBucket) {

                size_t bucketSize = 0;

                for(size_t b = 0; b < _entriesPerBucket; ++b) {
                    if(_map[idx+b].load(std::memory_order_relaxed)) {
                        bucketSize++;
                    }
                }

                if(bucketSize > 0) {
                    elementsInThisBar+= bucketSize;
                }
            }
            elements.push_back(elementsInThisBar);
        }

    }

    struct stats {
        size_t size;
        size_t usedBuckets;
        size_t collisions;
        size_t biggestBucket;
        double avgBucketSize;
    };

    void getStats(stats& s) {
        s.size = 0;
        s.usedBuckets = 0;
        s.collisions = 0;
        s.biggestBucket = 0;
        s.avgBucketSize = 0.0;

        for(size_t idx = 0; idx < _entries; idx += _entriesPerBucket) {
            size_t bucketSize = 0;

            for(size_t b = 0; b < _entriesPerBucket; ++b) {
                if(_map[idx+b].load(std::memory_order_relaxed)) {
                    bucketSize++;
                }
            }

            if(bucketSize > 0) {
                s.usedBuckets++;
                s.size += bucketSize;
                s.collisions += bucketSize - 1;
                if(bucketSize > s.biggestBucket) s.biggestBucket = bucketSize;
            }

        }

        if(_buckets > 0) {
            s.avgBucketSize = (double)s.size / (double)_buckets;
        }
    }

private:
    size_t const _bucketsScale;
    size_t const _buckets;
    size_t const _bucketsMask;
    size_t const _entries;
    size_t const _entriesMask;
    std::atomic<HashTableEntry<K,V>*>* _map;
    SlabManager _slabManager;

private:
    static size_t constexpr _bucketSize = CACHE_LINE_SIZE_IN_BYTES;
    static size_t constexpr _entriesPerBucket = _bucketSize/sizeof(void*);
};

}
