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

namespace insituRevCasUB {

template<typename K, typename V>
class HashTableEntry {
public:

    HashTableEntry(K const& key, V const& value): _key(key), _value(value) {
    }

public:
    std::atomic<size_t> _key;
    std::atomic<size_t> _value;
};

/*
 * 16 bits hash, 48 bits key
 * 16 bits ..., 48 bits value
 */

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
    , _entriesMask(_entries-1ULL)
    {
        _map = (decltype(_map))MMapper::mmapForMap(_buckets * _bucketSize);
    }
public:

    static size_t getHash(size_t ptr) {
        return ((intptr_t)ptr & 0xFFFF000000000000ULL);
    }

    static K getPtr(size_t ptr) {
        return (K)((intptr_t)ptr & 0x0000FFFFFFFFFFFFULL);
    }

    static size_t makePtrWithHash(K const& ptr, size_t h) {
        return (size_t)(((intptr_t)ptr)|h);
    }

    size_t insert(K const& key, V const& value) {
//        printf("key:   %zx\n", key);
        size_t h = hash(key);
        size_t h16l = hash16LeftFromHash(h);
        size_t e = entryFromhash(h);
//        printf("entry: %zx\n", e);
        HashTableEntry<K,V>* current = &_map[e];
//        printf("cur:   %p\n", current);

        size_t newValue = value;
        while(true) {
            while(true) {
                size_t kAndHash = current->_key.load(std::memory_order_relaxed);
                //printf("checking existing entry: %zx\n", kAndHash); fflush(stdout);
                if(kAndHash == 0ULL) break;
                size_t currentHash = getHash(kAndHash);
                K k = getPtr(kAndHash);
                if(currentHash == h16l) {
                    if(k == key) {
                        std::atomic_thread_fence(std::memory_order_acquire);
                        return current->_value.load(std::memory_order_relaxed);
                    }
                }
                e = (e+1) & _entriesMask;
                current = &_map[e];
            }
            size_t oldValue = 0ULL;
            if(current->_value.compare_exchange_strong(oldValue, newValue, std::memory_order_release, std::memory_order_relaxed)) {
                break;
            }
            while(current->_key.load(std::memory_order_relaxed) == 0) {
                _mm_pause();
            }
        }
        size_t oldKey = 0ULL;
        size_t newKey = key | h16l;
        current->_key.compare_exchange_strong(oldKey, newKey, std::memory_order_relaxed, std::memory_order_relaxed);
        return value;
    }

    bool get(K const& key, V& value) {
        size_t h = hash(key);
        size_t h16l = hash16LeftFromHash(h);
        size_t e = entryFromhash(h);
//        printf("entry: %zx\n", e);
        HashTableEntry<K,V>* current = &_map[e];
//        printf("cur:   %p\n", current);

        while(true) {
//            printf("checking existing entry: %zx -> %zx\n", current->_key, current->_value);
            size_t kAndHash = current->_key.load(std::memory_order_relaxed);
            if(kAndHash == 0ULL) break;
            size_t currentHash = getHash(kAndHash);
            K k = getPtr(kAndHash);
            if(currentHash == h16l) {
                if(k == key) {
                    std::atomic_thread_fence(std::memory_order_acquire);
                    while(true) {
                        value = current->_value.load(std::memory_order_relaxed);
                        return true;
                    }
                }
            }
            e = (e+1) & _entriesMask;
            current = &_map[e];
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
                    if(_map[idx+b]._key.load(std::memory_order_relaxed)) {
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
                if(_map[idx+b]._key.load(std::memory_order_relaxed)) {
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
    HashTableEntry<K,V>* _map;
    SlabManager _slabManager;

private:
    static size_t constexpr _bucketSize = CACHE_LINE_SIZE_IN_BYTES;
    static size_t constexpr _entriesPerBucket = _bucketSize/(sizeof(HashTableEntry<K, V>));
};

}
