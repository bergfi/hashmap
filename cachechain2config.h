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

namespace cachechain2config {

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

    Bucket(HTE* hteFirst, HTE* hte) {
        _entries[0].store(hteFirst, std::memory_order_relaxed);
        _entries[1].store(hte, std::memory_order_relaxed);
    }

    static constexpr size_t _entries_n = (CACHE_LINE_SIZE_IN_BYTES)/sizeof(HTE*);
    std::atomic<HTE*> _entries[_entries_n];

    __attribute__((always_inline))
    static HTE* pointerWithTargetPos(HTE* hte, size_t targetPos) {
        return (HTE*)(((intptr_t)hte)|targetPos);
    }

    template<typename T>
    __attribute__((always_inline))
    static T* getRealPointer(T* hte) {
        return (T*)(((intptr_t)hte)&~0xFULL);
    }

    __attribute__((always_inline))
    static size_t getConfigBits(HTE* hte) {
        return ((intptr_t)hte)&0xFULL;
    }

    __attribute__((always_inline))
    static bool isNext(HTE* hte) {
        return ((intptr_t)hte)&0x8ULL;
    }

    __attribute__((always_inline))
    static bool isNext(Bucket* b) {
        return ((intptr_t)b)&0x8ULL;
    }

    template<typename T>
    __attribute__((always_inline))
    static Bucket* makeNext(T* hte) {
        return (Bucket*)(((intptr_t)hte)^0x8ULL);
    }

    __attribute__((always_inline))
    Bucket* getNext() const {
        auto ptr = (Bucket*)(_entries[_entries_n-1].load(std::memory_order_relaxed));
        if(isNext(ptr)) return getRealPointer(ptr);
        else return nullptr;
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
    , _entriesMask(_entries-1ULL)
    {
        _map = (decltype(_map))MMapper::mmapForMap(_buckets * _bucketSize);
    }
public:

    V const& insertInBucket(BucketHTE* bucket, K const& key, V const& value, size_t e) {

        bucket = BucketHTE::getRealPointer(bucket);

        HTE* hte = createHTE(key, value);
        HTE* hteWithConfigBits = BucketHTE::pointerWithTargetPos(hte, e);
        HTE* current = bucket->_entries[e].load(std::memory_order_relaxed);

        size_t end = e;

// Does NOT make it faster, seemed to make it slower. Prefetching at the page
// level does not yield a performance boost
//        if(BucketHTE::isNext(lastEntry)) {
//            posix_madvise(lastEntry, CACHE_LINE_SIZE_BP2, POSIX_MADV_WILLNEED);
//        }

        do {
            while(current) {
                if(BucketHTE::getConfigBits(current) == end) {
                    HTE* currentReal = BucketHTE::getRealPointer(current);
                    if(currentReal->_key == key) {
                        giveMemoryBack(hte);
                        return currentReal->_value;
                    }
                }
                e = (e+1) & (_entriesPerBucket-1);
                if(e == end) {
                    auto& targetEntry = bucket->_entries[BucketHTE::_entries_n-1];
                    auto lastEntry = targetEntry.load(std::memory_order_relaxed);;
                    if(BucketHTE::isNext(lastEntry)) {
                        return insertInBucketFromStart((BucketHTE*)lastEntry, key, value, end);
                    } else {
                        BucketHTE* newBucket = createBucket(lastEntry, hteWithConfigBits);
                        if(targetEntry.compare_exchange_strong(lastEntry, (HTE*)BucketHTE::makeNext(newBucket), std::memory_order_release, std::memory_order_relaxed)) {
                            return value;
                        } else {
                            newBucket->_entries[0].store(nullptr, std::memory_order_relaxed);
                            newBucket->_entries[1].store(nullptr, std::memory_order_relaxed);
                            giveMemoryBack(newBucket);
                            return insertInBucketFromStart((BucketHTE*)lastEntry, key, value, end);
                        }
                    }
                }
                current = bucket->_entries[e].load(std::memory_order_relaxed);
            }
        } while(!bucket->_entries[e].compare_exchange_weak(current, hteWithConfigBits, std::memory_order_release, std::memory_order_relaxed));
        return value;
    }

    V const& insertInBucketFromStart(BucketHTE* bucket, K const& key, V const& value, size_t eOrig) {

        bucket = BucketHTE::getRealPointer(bucket);

        HTE* hte = createHTE(key, value);
        HTE* hteWithConfigBits = BucketHTE::pointerWithTargetPos(hte, eOrig);

        size_t e = 0;
        HTE* current = bucket->_entries[e].load(std::memory_order_relaxed);

// Does NOT make it faster, seemed to make it slower. Prefetching at the page
// level does not yield a performance boost
//        if(BucketHTE::isNext(lastEntry)) {
//            posix_madvise(lastEntry, CACHE_LINE_SIZE_BP2, POSIX_MADV_WILLNEED);
//        }

        do {
            while(current) {
                if(BucketHTE::getConfigBits(current) == eOrig) {
                    HTE* currentReal = BucketHTE::getRealPointer(current);
                    if(currentReal->_key == key) {
                        giveMemoryBack(hte);
                        return currentReal->_value;
                    }
                }
                e++;
                if(e == _entriesPerBucket) {
                    auto& targetEntry = bucket->_entries[BucketHTE::_entries_n-1];
                    auto lastEntry = targetEntry.load(std::memory_order_relaxed);;
                    if(BucketHTE::isNext(lastEntry)) {
                        return insertInBucketFromStart((BucketHTE*)lastEntry, key, value, eOrig);
                    } else {
                        BucketHTE* newBucket = createBucket(lastEntry, hteWithConfigBits);
                        if(targetEntry.compare_exchange_strong(lastEntry, (HTE*)BucketHTE::makeNext(newBucket), std::memory_order_release, std::memory_order_relaxed)) {
                            return value;
                        } else {
                            newBucket->_entries[0].store(nullptr, std::memory_order_relaxed);
                            newBucket->_entries[1].store(nullptr, std::memory_order_relaxed);
                            giveMemoryBack(newBucket);
                            return insertInBucketFromStart((BucketHTE*)lastEntry, key, value, eOrig);
                        }
                    }
                }
                current = bucket->_entries[e].load(std::memory_order_relaxed);
            }
        } while(!bucket->_entries[e].compare_exchange_weak(current, hteWithConfigBits, std::memory_order_release, std::memory_order_relaxed));
        return value;
    }

    size_t insert(K const& key, V const& value) {
        size_t e = entry(key);
        size_t bucket = e >> _entriesPerBucketPower;
        return insertInBucket(&_map[bucket], key, value, e & (_entriesPerBucket-1));
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
        size_t bucketIdx = e >> _entriesPerBucketPower;
        auto bucket = &_map[bucketIdx];
        e &= _entriesPerBucket - 1ULL;

        HTE* current = bucket->_entries[e].load(std::memory_order_relaxed);

        size_t eOrig = e;
        size_t end = e;

        while(current) {
            if(BucketHTE::getConfigBits(current) == eOrig) {
                HTE* currentReal = BucketHTE::getRealPointer(current);
                if(currentReal->_key == key) {
                    value = currentReal->_value;
                    return true;
                }
            }
            e = (e+1) & (_entriesPerBucket-1);
            if(e == end) {
                bucket = bucket->getNext();
                if(!bucket) {
                    goto notfound;
                }
                e = end = 0;
            }
            current = bucket->_entries[e].load(std::memory_order_relaxed);
        }
        notfound:
        return false;
    }

    size_t entry(K const& key) const {
        //return (std::hash<K>{}(key) & _bucketsMask);
        size_t hash = MurmurHash64(key);
        return (hash & _entriesMask);
    }

    size_t bucketSize(std::atomic<HashTableEntry<K,V>*>* bucket) const {
        size_t s = 0;
        HashTableEntry<K,V>* current = bucket->load(std::memory_order_relaxed);
        while(current) {
            s++;
            current = current->getNext();
        }
        return s;
    }

    size_t size() const {
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

    BucketHTE* createBucket(HTE* const& hteFirst, HTE* const& hte) {
        return new(_slabManager.alloc<BucketHTE>()) BucketHTE(hteFirst, hte);
    }

    template<typename T>
    void giveMemoryBack(T* const& hte) {
        return _slabManager.free(hte);
    }

    ~HashTable() {
        munmap(_map, _buckets * _bucketSize);
    }

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
                    bucket = bucket->getNext();
                    if(bucket) bucketSize--;
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
        size_t totalChain;
        size_t longestChain;
        double avgChainLength;
    };

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
                bucket = bucket->getNext();
                if(bucket) bucketSize--;
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
    size_t const _entriesMask;
    BucketHTE* _map;
    SlabManager _slabManager;

private:
    static size_t constexpr _bucketSize = CACHE_LINE_SIZE_IN_BYTES;
    static size_t constexpr _bucketStride = _bucketSize/sizeof(void*);
    static size_t constexpr _entriesPerBucket = _bucketSize/sizeof(void*);
    static size_t constexpr _entriesPerBucketPower = __builtin_ctz(_entriesPerBucket);

};

}
