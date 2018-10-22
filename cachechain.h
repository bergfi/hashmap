#pragma once

// Cache chain 1: chains of cachebuckets with 7 entries

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <sys/mman.h>

#include <atomic>
#include <new>

#define CACHE_LINE_SIZE_BP2 6
#define CACHE_LINE_SIZE_IN_BYTES (1<<CACHE_LINE_SIZE_BP2)

#include "allocator.h"
#include "mmapper.h"
#include "murmurhash.h"

namespace cachechain {

template<typename K, typename V>
class HashTableEntry {
public:

    HashTableEntry(K const& key, V const& value): _key(key), _value(value) {
    }

public:
    K _key;
    V _value;
};

template<typename HTE>
class Bucket {
public:
    Bucket() {}
    Bucket(HTE* hte, size_t pos) {
        _entries[pos].store(hte, std::memory_order_relaxed);
    }
    static constexpr size_t _entries_n = (CACHE_LINE_SIZE_IN_BYTES-sizeof(Bucket*))/sizeof(HTE*);
    std::atomic<HTE*> _entries[_entries_n];
    std::atomic<Bucket*> _next;

    Bucket* getNext() const {
        return _next.load(std::memory_order_relaxed);
    }
};

template<typename K, typename V>
class HashTable {
public:
    static constexpr size_t PAGE_SIZE_P2 = 20;
public:

    using HTE = HashTableEntry<K,V>;
    using BucketHTE = Bucket<HTE>;

    HashTable(size_t bucketsScale)
    : _bucketsScale(bucketsScale)
    , _buckets((1ULL << _bucketsScale)/_bucketStride)
    , _bucketsMask((_buckets-1ULL))
    , _entries(_buckets*_entriesPerBucket)
    {
        _map = (decltype(_map))MMapper::mmapForMap(_buckets * _bucketSize);
    }
public:

    V const& insertInBucket(BucketHTE* bucket, K const& key, V const& value, size_t e = 0) {

//        printf("insertInBucket %i\n", __LINE__);

        HTE* hte = createHTE(key, value);
        HTE* current = bucket->_entries[e].load(std::memory_order_relaxed);

        size_t end = e;

//        BucketHTE* expectedBucket = bucket->getNext();
//        if(expectedBucket) {
//            __builtin_prefetch(expectedBucket, 0, 3);
//            e += expectedBucket->getNext() == expectedBucket;
//        }

        do {
            while(current) {
                if(current->_key == key) {
                    giveMemoryBack(hte);
                    return current->_value;
                }
                e++;
                e *= (e<_entriesPerBucket);
                if(e == end) {
                    BucketHTE* expectedBucket = bucket->getNext();
                    if(expectedBucket) {
                        return insertInBucket(expectedBucket, key, value, e);
                    } else {
                        BucketHTE* newBucket = createBucket(hte, e);
                        if(bucket->_next.compare_exchange_strong(expectedBucket, newBucket, std::memory_order_release, std::memory_order_relaxed)) {
                            return value;
                        } else {
                            newBucket->_entries[e].store(nullptr, std::memory_order_relaxed);
                            giveMemoryBack(newBucket);
                            return insertInBucket(expectedBucket, key, value, e);
                        }
                    }
                }
                current = bucket->_entries[e].load(std::memory_order_relaxed);
            }
        } while(!bucket->_entries[e].compare_exchange_weak(current, hte, std::memory_order_release, std::memory_order_relaxed));
        return value;
    }

    size_t insert(K const& key, V const& value) {
        size_t e = entry(key);
        size_t bucket = e / _entriesPerBucket;
        return insertInBucket(&_map[bucket], key, value, e - bucket*_entriesPerBucket);
    }

//
//    size_t insert(K const& key, V const& value) {
//
//        size_t e = entry(key);
//
//        insertInBucket(_map[e].load(std::memory_order_relaxed));
//
//// ---------
//
//
////        printf("entry: %zx\n", e);
//        HashTableEntry<K,V>* current = _map[e].load(std::memory_order_relaxed);
////        printf("cur:   %p\n", current);
//
//        std::atomic<HashTableEntry<K,V>*>* parentLink = &_map[e];
//        while(current) {
////            printf("checking existing entry: %zx -> %zx\n", current->_key, current->_value);
//            if(current->_key == key) return current->_value;
//            parentLink = &(current->_next);
//            current = current->getNext();
//        }
//
//        HashTableEntry<K,V>* hte = createHTE(key, value, nullptr);
//        while(!parentLink->compare_exchange_weak(current, hte, std::memory_order_release, std::memory_order_relaxed)) {
//            while(current) {
//                if(current->_key == key) {
//                    //delete hte;
//                    return current->_value;
//                }
//                parentLink = &current->_next;
//                current = current->getNext();
//            }
//        }
//        return value;
//    }
//    size_t insert(K const& key, V const& value) {
////        printf("key:   %zx\n", key);
//        size_t e = entry(key);
//
//        _map[e].insert(key, value);
//
////        printf("entry: %zx\n", e);
//        HashTableEntry<K,V>* current = _map[e].load(std::memory_order_relaxed);
////        printf("cur:   %p\n", current);
//
//        std::atomic<HashTableEntry<K,V>*>* parentLink = &_map[e];
//        while(current) {
////            printf("checking existing entry: %zx -> %zx\n", current->_key, current->_value);
//            if(current->_key == key) return current->_value;
//            parentLink = &(current->_next);
//            current = current->getNext();
//        }
//
//        HashTableEntry<K,V>* hte = createHTE(key, value, nullptr);
//        while(!parentLink->compare_exchange_weak(current, hte, std::memory_order_release, std::memory_order_relaxed)) {
//            while(current) {
//                if(current->_key == key) {
//                    //delete hte;
//                    return current->_value;
//                }
//                parentLink = &current->_next;
//                current = current->getNext();
//            }
//        }
//        return value;
//    }

    bool get(K const& key, V& value) {

        size_t e = entry(key);
        size_t bucketIdx = e / _entriesPerBucket;
        auto bucket = &_map[bucketIdx];
        e -= bucketIdx * _entriesPerBucket;

        HTE* current = bucket->_entries[e].load(std::memory_order_relaxed);

        size_t end = e;

        while(current) {
            if(current->_key == key) {
                value = current->_value;
                return true;
            }
            e++;
            e *= (e<_entriesPerBucket);
            if(e == end) {
                bucket = bucket->getNext();
                if(!bucket) {
                    goto notfound;
                }
            }
            current = bucket->_entries[e].load(std::memory_order_relaxed);
        }
        notfound:
        return false;
    }

    size_t entry(K const& key) {
        //return (std::hash<K>{}(key) & _bucketsMask);
        size_t hash = MurmurHash64(key);
        return (hash % _entries);
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
        return new(_slabManager.alloc<HTE>()) HTE(key, value);
    }

    BucketHTE* createBucket(HashTableEntry<K,V>* const& hte, size_t e) {
        return new(_slabManager.alloc<BucketHTE>()) BucketHTE(hte, e);
    }

    template<typename T>
    void giveMemoryBack(T* const& hte) {
        return _slabManager.free(hte);
    }

    ~HashTable() {
        munmap(_map, _buckets * _bucketSize);
    }

    struct stats {
        size_t size;
        size_t usedBuckets;
        size_t collisions;
        size_t biggestBucket;
        double avgBucketSize;
        size_t totalChain;
        size_t longestChain;
        double avgChainLength;
    };

    template<typename CONTAINER>
    void getDensityStats(size_t bars, CONTAINER& elements) {

        size_t bucketPerBar = _buckets / bars;
        bucketPerBar += bucketPerBar == 0;

        for(size_t idx = 0; idx < _buckets;) {
            size_t elementsInThisBar = 0;
            size_t max = std::min(_buckets, idx + bucketPerBar);
            for(; idx < max; ++idx) {
                BucketHTE* bucket = &_map[idx];

                size_t bucketSize = 0;
                size_t chainSize = 0;

                while(bucket) {
                    size_t s = 0;
                    while(s < BucketHTE::_entries_n) {
                        if(bucket->_entries[s]) bucketSize++;
                        s++;
                    }
                    chainSize++;
                    bucket = bucket->_next.load(std::memory_order_relaxed);
                }

                if(bucketSize > 0) {
                    elementsInThisBar+= bucketSize;
                }
            }
            elements.push_back(elementsInThisBar);
        }
    }

    void getStats(stats& s) {
        s.size = 0;
        s.usedBuckets = 0;
        s.collisions = 0;
        s.biggestBucket = 0;
        s.avgBucketSize = 0.0;
        s.totalChain = 0;
        s.longestChain = 0;
        s.avgChainLength = 0.0;

        for(size_t idx = 0; idx < _buckets; ++idx) {
            BucketHTE* bucket = &_map[idx];

            size_t bucketSize = 0;
            size_t chainSize = 0;

            while(bucket) {
                size_t s = 0;
                while(s < BucketHTE::_entries_n) {
                    if(bucket->_entries[s]) bucketSize++;
                    s++;
                }
                chainSize++;
                bucket = bucket->_next.load(std::memory_order_relaxed);
            }

            if(bucketSize > 0) {
                s.totalChain += chainSize;
                s.usedBuckets++;
                s.size += bucketSize;
                s.collisions += bucketSize - 1;
                if(chainSize > s.longestChain) s.longestChain = chainSize;
                if(bucketSize > s.biggestBucket) s.biggestBucket = bucketSize;
            }

        }

        if(_buckets > 0) {
            s.avgChainLength = (double)s.totalChain / (double)_buckets;
            s.avgBucketSize = (double)s.size / (double)_buckets;
        }
    }


private:
    size_t const _bucketsScale;
    size_t const _buckets;
    size_t const _bucketsMask;
    size_t const _entries;
    BucketHTE* _map;
    SlabManager _slabManager;

private:
    static size_t constexpr _bucketSize = CACHE_LINE_SIZE_IN_BYTES;
    static size_t constexpr _bucketStride = _bucketSize/sizeof(void*);
    static size_t constexpr _entriesPerBucket = _bucketSize/sizeof(void*)-1;
};

}
