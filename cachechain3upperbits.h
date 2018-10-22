#pragma once

// Cache chain 3: chains of cachebuckets with 8 entries
// Last bit in last bucket is used to determine whether or not it is a next pointer

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

namespace cachechain3upperbits {

template<typename K, typename V>
class HashTableEntry {
public:

    HashTableEntry(K const& key, V const& value): _key(key), _value(value) {
    }

public:
    K _key;
    V _value;
    char rest[4 - ((sizeof(K)+sizeof(V))&0x3)];
};

template<typename HTE>
class Bucket {
public:

    static constexpr size_t ENTRY_BITS_PTR = 48;
    static constexpr size_t ENTRY_BITS_HASH = 16;
    static constexpr size_t ENTRY_MASK_PTR = 0x0000FFFFFFFFFFFCULL;
    static constexpr size_t ENTRY_MASK_CONFIG = 0xFFFF000000000003ULL;
    static constexpr size_t ENTRY_MASK_CONFIG_HASH = 0xFFFF000000000000ULL;
    static constexpr size_t ENTRY_MASK_CONFIG_LOCATION = 0x0007000000000000ULL;
    static constexpr size_t ENTRY_MASK_CONFIG_NEXT = 0x0000000000000001ULL;
    static constexpr size_t ENTRY_MASK_CONFIG_HASHANDNEXT = ENTRY_MASK_CONFIG_HASH|ENTRY_MASK_CONFIG_NEXT;

    Bucket() {}

    Bucket(HTE* hte1, HTE* hte2) {
        size_t bits1 = getLocation(hte1);
        size_t bits2 = getLocation(hte2);
        if(bits1 == bits2) {
            bits2 = (bits2+1) & (_entries_n-1);
        }
        _entries[bits1].store(hte1, std::memory_order_relaxed);
        _entries[bits2].store(hte2, std::memory_order_relaxed);
    }

    void clearFromNew(HTE* hte1, HTE* hte2) {
        size_t bits1 = getLocation(hte1);
        size_t bits2 = getLocation(hte2);
        if(bits1 == bits2) {
            bits2 = (bits2+1) & (_entries_n-1);
        }
        _entries[bits1].store(nullptr, std::memory_order_relaxed);
        _entries[bits2].store(nullptr, std::memory_order_relaxed);
    }

    static constexpr size_t _entries_n = (CACHE_LINE_SIZE_IN_BYTES)/sizeof(HTE*);
    std::atomic<HTE*> _entries[_entries_n];

    __attribute__((always_inline))
    static HTE* pointerWithHash(HTE* hte, size_t hash16l) {
        return (HTE*)(((intptr_t)hte)|hash16l);
    }

    template<typename T>
    __attribute__((always_inline))
    static T* getRealPointer(T* hte) {
        return (T*)(((intptr_t)hte)&ENTRY_MASK_PTR);
    }

    __attribute__((always_inline))
    static size_t getHashBits(HTE* hte) {
        return ((intptr_t)hte)&ENTRY_MASK_CONFIG_HASH;
    }

    __attribute__((always_inline))
    static size_t getHashAndNext(HTE* hte) {
        return ((intptr_t)hte)&ENTRY_MASK_CONFIG_HASHANDNEXT;
    }

    __attribute__((always_inline))
    static size_t getLocation(HTE* hte) {
        return (((intptr_t)hte)&ENTRY_MASK_CONFIG_LOCATION) >> ENTRY_BITS_PTR;
    }

    template<typename T>
    __attribute__((always_inline))
    static bool isNext(T* hte) {
        return ((intptr_t)hte)&ENTRY_MASK_CONFIG_NEXT;
    }

    template<typename T>
    __attribute__((always_inline))
    static Bucket* makeNext(T* hte) {
        return (Bucket*)(((intptr_t)hte)^ENTRY_MASK_CONFIG_NEXT);
    }

    __attribute__((always_inline))
    Bucket* getNext() const {
        auto ptr = (Bucket*)(_entries[_entries_n-1].load(std::memory_order_relaxed));
        if(isNext(ptr)) return getRealPointer(ptr);
        else return nullptr;
    }
};

#  define PREFETCHW(x)		     asm volatile("prefetchw %0" :: "m" (*(unsigned long *)x))
#  define PREFETCH(x)		     asm volatile("prefetch %0" :: "m" (*(unsigned long *)x))

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

    V const& insertInBucket(BucketHTE* bucket, K const& key, V const& value, size_t hash16) {

//        printf("insertInBucket %i\n", __LINE__);

        size_t hash16l = hash16 << 48ULL;
        size_t e = eFromHash16(hash16);
        size_t eOrig = e;

        HTE* hte = createHTE(key, value);
        HTE* hteWithHash = BucketHTE::pointerWithHash(hte, hash16l);
        HTE* current = bucket->_entries[e].load(std::memory_order_relaxed);

// Does NOT make it faster, seemed to make it slower. Prefetching at the page
// level does not yield a performance boost
//        if(BucketHTE::isNext(lastEntry)) {
//            //posix_madvise(lastEntry, CACHE_LINE_SIZE_BP2, POSIX_MADV_WILLNEED);
//            //__builtin_prefetch(lastEntry, 0, 0); // slower
//            //__builtin_prefetch(lastEntry, 1, 0); // slower
//            //__builtin_prefetch(lastEntry, 1, 1); // slower
//            //__builtin_prefetch(lastEntry, 0, 1); // slower
//        }

        do {

            // Go through all the buckets in the cachebucket
            while(current) {

                // Check the current bucket in the cachebucket.
                // If the config bits are the same, then the bucket
                if(BucketHTE::getHashAndNext(current) == hash16l) {
                    HTE* currentReal = BucketHTE::getRealPointer(current);
                    if(currentReal->_key == key) {
                        giveMemoryBack(hte);
                        return currentReal->_value;
                    }
                }
                e = (e+1) & (_entriesPerBucket-1);

                // If we checked all buckets in the cachebucket
                if(e == eOrig) {
                    auto& targetEntry = bucket->_entries[BucketHTE::_entries_n-1];
                    auto lastEntry = targetEntry.load(std::memory_order_relaxed);
                    if(BucketHTE::isNext(lastEntry)) {
                        return insertInBucket(BucketHTE::getRealPointer((BucketHTE*)lastEntry), key, value, hash16);
                    } else {

                        // Create a bucket containing the new entry and the
                        // entry we will overwrite with the cachebucket link
                        BucketHTE* newBucket = createBucket(hteWithHash, lastEntry);

                        // Attempt to link the new cachebucket
                        // If it succeeds, we are done: just return the value
                        // If it fails, another thread linked in a new
                        // cachebucket
                        if(targetEntry.compare_exchange_strong(lastEntry, (HTE*)BucketHTE::makeNext(newBucket), std::memory_order_release, std::memory_order_relaxed)) {
                            return value;
                        } else {
                            if(!BucketHTE::isNext(lastEntry)) {
                                std::cout << "ERROR: last entry is not a bucket!" << std::endl;
                            }
                            newBucket->clearFromNew(hteWithHash, lastEntry);
                            giveMemoryBack(newBucket);
                            return insertInBucket(BucketHTE::getRealPointer((BucketHTE*)lastEntry), key, value, hash16);
                        }
                    }
                }
                current = bucket->_entries[e].load(std::memory_order_relaxed);
            }
        } while(!bucket->_entries[e].compare_exchange_weak(current, hteWithHash, std::memory_order_release, std::memory_order_relaxed));
        return value;
    }

    size_t insert(K const& key, V const& value) {
        size_t h = hash(key);
        size_t e = entryFromHash(h);
        size_t bucket = e >> _entriesPerBucketPower;
        size_t hash16 = hash16FromHash(h);
        return insertInBucket(&_map[bucket], key, value, hash16);
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

        size_t h = hash(key);
        size_t hash16 = hash16FromHash(h);
        size_t hash16l = hash16 << 48ULL;
        size_t e = eFromHash16(hash16);
        size_t eOrig = e;

        size_t bucketIdx = entryFromHash(h) >> _entriesPerBucketPower;
        auto bucket = &_map[bucketIdx];

        HTE* current = bucket->_entries[e].load(std::memory_order_relaxed);

        while(current) {
            if(BucketHTE::getHashAndNext(current) == hash16l) {
                HTE* currentReal = BucketHTE::getRealPointer(current);
                if(currentReal->_key == key) {
                    value = currentReal->_value;
                    return true;
                }
            }
            e = (e+1) & (_entriesPerBucket-1);
            if(e == eOrig) {
                bucket = bucket->getNext();
                if(!bucket) {
                    goto notfound;
                }
            }
            current = bucket->_entries[e].load(std::memory_order_relaxed);
        }
        notfound:
        return get2(key,value);
    }

    bool get2(K const& key, V& value) {

        size_t h = hash(key);
        size_t hash16 = hash16FromHash(h);
        size_t hash16l = hash16 << 48ULL;
        size_t e = eFromHash16(hash16);
        size_t eOrig = e;

        size_t bucketIdx = entryFromHash(h) >> _entriesPerBucketPower;
        auto bucket = &_map[bucketIdx];

        HTE* current = bucket->_entries[e].load(std::memory_order_relaxed);

        std::cout << "==== Looking for entry: " << key << ", bits=" << eOrig << ", hash=" << std::hex << hash16l << std::endl;

        bool wouldNotHaveFound = false;

        while(true) {
            if(current) {
                std::cout << "Checking entry[" << e << "]: " << std::hex << BucketHTE::getHashAndNext(current) << std::endl;
                if(BucketHTE::getHashAndNext(current) == hash16l) {
                    HTE* currentReal = BucketHTE::getRealPointer(current);
                    std::cout << "  Key/Value: " << currentReal->_key << " -> " << currentReal->_value << std::endl;
                    if(currentReal->_key == key) {
                        std::cout << "  Match." << std::endl;
                        value = currentReal->_value;
                        return true;
                    }
                } else if(!BucketHTE::isNext(current)) {
                    HTE* currentReal = BucketHTE::getRealPointer(current);
                    std::cout << "  Key/Value: " << currentReal->_key << " -> " << currentReal->_value << std::endl;
                }
            } else {
                std::cout << "Skipping entry: " << BucketHTE::getHashAndNext(current) << std::endl;
            }
            e = (e+1) & (_entriesPerBucket-1);
            if(e == eOrig) {
                std::cout << "Went through all in this cachebucket" << std::endl;
                bucket = bucket->getNext();
                if(!bucket) {
                    std::cout << "  No next, entry not found!" << std::endl;
                    goto notfound;
                }
            }
            current = bucket->_entries[e].load(std::memory_order_relaxed);
            if(!current && !wouldNotHaveFound) {
                wouldNotHaveFound = true;
                std::cout << "  (WOULD NOT HAVE FOUND!)" << std::endl;
            }
        }
        notfound:
        return false;
    }

    size_t hash(K const& key) const {
        auto r = MurmurHash64(key);
        assert(r);
        return r;
    }

    size_t entryFromHash(size_t const& h) const {
        return h & _entriesMask;
    }

    size_t hash16FromHash(size_t h) const {

//        h ^= h >> 32ULL;
//        h ^= h >> 16ULL;
        return h & 0xFFFFULL;
    }

    size_t eFromHash16(size_t const& h) const {
        return h & (BucketHTE::_entries_n-1);
    }

    size_t entry(K const& key) const {
        //return (std::hash<K>{}(key) & _bucketsMask);
        size_t h = hash(key);
        return entryFromHash(h);
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
        HTE* hte = new(_slabManager.alloc<HTE>()) HTE(key, value);
        assert( (((intptr_t)hte)&0x3) == 0);
        return hte;
    }

    BucketHTE* createBucket(HTE* const& hteFirst, HTE* const& hte) {
        BucketHTE* bucket = new(_slabManager.alloc<BucketHTE>()) BucketHTE(hteFirst, hte);
        assert( (((intptr_t)bucket)&0x3) == 0);
        return bucket;
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
