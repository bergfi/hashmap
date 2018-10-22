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
#include "key_accessor.h"

#define CACHE_LINE_SIZE_BP2 6
#define CACHE_LINE_SIZE_IN_BYTES (1<<CACHE_LINE_SIZE_BP2)

namespace cachechain3vkeysize {

template<typename K, typename V>
class HashTableEntry {
public:

    HashTableEntry(size_t length, const char* keyData, V const& value): _value(value), _length(length) {
        memmove(_keyData, keyData, length);
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
    char _keyData[0];
};

template<typename HTE>
class Bucket {
public:

    static constexpr size_t CONFIG_BITS = __builtin_ctz(sizeof(decltype(HTE::_length))+sizeof(decltype(HTE::_value)));
    static constexpr size_t CONFIG_MASK = (1ULL << CONFIG_BITS) - 1ULL;
    static constexpr size_t CONFIG_MASK_ISNEXT = (1ULL << (CONFIG_BITS-1ULL));
    static constexpr size_t CONFIG_MASK_E = CONFIG_MASK_ISNEXT - 1ULL;

    Bucket() {}

    Bucket(HTE* hte1, HTE* hte2) {
        size_t bits1 = getConfigBits(hte1);
        size_t bits2 = getConfigBits(hte2);
        if(bits1 == bits2) {
            bits2 = (bits2+1) & (_entries_n-1);
        }
        _entries[bits1].store(hte1, std::memory_order_relaxed);
        _entries[bits2].store(hte2, std::memory_order_relaxed);
    }

    void clearFromNew(HTE* hte1, HTE* hte2) {
        size_t bits1 = getConfigBits(hte1);
        size_t bits2 = getConfigBits(hte2);
        if(bits1 == bits2) {
            bits2 = (bits2+1) & (_entries_n-1);
        }
        _entries[bits1].store(nullptr, std::memory_order_relaxed);
        _entries[bits2].store(nullptr, std::memory_order_relaxed);
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
        return (T*)(((intptr_t)hte)&~CONFIG_MASK);
    }

    __attribute__((always_inline))
    static size_t getConfigBits(HTE* hte) {
        return ((intptr_t)hte)&CONFIG_MASK;
    }

    template<typename T>
    __attribute__((always_inline))
    static bool isNext(T* hte) {
        return ((intptr_t)hte)&CONFIG_MASK_ISNEXT;
    }

    template<typename T>
    __attribute__((always_inline))
    static Bucket* makeNext(T* hte) {
        return (Bucket*)(((intptr_t)hte)^CONFIG_MASK_ISNEXT);
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

        static_assert(sizeof(HTE) >= 16);
        static_assert((sizeof(HTE)&0xF) == 0);

        _map = (decltype(_map))MMapper::mmapForMap(_buckets * _bucketSize);
    }
public:

    void printHex(const char* key, size_t length) {
        for(size_t i = 0; i < length; ++i) {
            printf(" %X", ((const unsigned char*)key)[i] & 0xFF);
        }
    }

    V const& insertInBucket(BucketHTE* bucket, K const& key, V const& value, size_t e, HTE* hteWithConfigBits) {

//        printf("insertInBucket %i\n", __LINE__);

        bucket = BucketHTE::getRealPointer(bucket);

        HTE* current = bucket->_entries[e].load(std::memory_order_relaxed);

        size_t eOrig = e;
        size_t length = hashtables::key_accessor<K>::size(key);

        do {

            // Go through all the buckets in the cachebucket
            while(current) {

                // Check the current bucket in the cachebucket.
                // If the config bits are the same, then the bucket
                if(BucketHTE::getConfigBits(current) == eOrig) {
                    HTE* currentReal = BucketHTE::getRealPointer(current);
                    if(currentReal->matches(length, hashtables::key_accessor<K>::data(key))) {
                        if(hteWithConfigBits) giveMemoryBack((char*)BucketHTE::getRealPointer(hteWithConfigBits), sizeof(HTE) + length);
                        return currentReal->_value;
                    }
                }
                e = (e+1) & (_entriesPerBucket-1);

                // If we checked all buckets in the cachebucket
                if(e == eOrig) {
                    auto& targetEntry = bucket->_entries[BucketHTE::_entries_n-1];
                    auto lastEntry = targetEntry.load(std::memory_order_relaxed);
                    if(BucketHTE::isNext(lastEntry)) {
                        return insertInBucket((BucketHTE*)lastEntry, key, value, eOrig, hteWithConfigBits);
                    } else {

                        if(!hteWithConfigBits) hteWithConfigBits = BucketHTE::pointerWithTargetPos(createHTE(length, hashtables::key_accessor<K>::data(key), value), eOrig);

                        // Create a bucket containing the new entry and the
                        // entry we will overwrite with the cachebucket link
                        BucketHTE* newBucket = createBucket(hteWithConfigBits, lastEntry);

                        // Attempt to link the new cachebucket
                        // If it succeeds, we are done: just return the value
                        // If it fails, another thread linked in a new
                        // cachebucket
                        if(targetEntry.compare_exchange_strong(lastEntry, (HTE*)BucketHTE::makeNext(newBucket), std::memory_order_release, std::memory_order_relaxed)) {
                            return value;
                        } else {
                            newBucket->clearFromNew(hteWithConfigBits, lastEntry);
                            giveMemoryBack(newBucket);
                            return insertInBucket((BucketHTE*)lastEntry, key, value, eOrig, hteWithConfigBits);
                        }
                    }
                }
                current = bucket->_entries[e].load(std::memory_order_relaxed);
            }
            if(!hteWithConfigBits) hteWithConfigBits = BucketHTE::pointerWithTargetPos(createHTE(length, hashtables::key_accessor<K>::data(key), value), eOrig);
        } while(!bucket->_entries[e].compare_exchange_weak(current, hteWithConfigBits, std::memory_order_release, std::memory_order_relaxed));
        //std::cout << "Inserted @ " << bucket << " [" << e << "] " << std::string(hashtables::key_accessor<K>::data(key), length) << ") -> " << value << std::endl;
        return value;
    }

    size_t insert(K const& key, V const& value) {
        size_t e = entry(key);
        size_t bucket = e >> _entriesPerBucketPower;
        return insertInBucket(&_map[bucket], key, value, e & (_entriesPerBucket-1), nullptr);
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
        e &= _entriesPerBucketMask;

        HTE* current = bucket->_entries[e].load(std::memory_order_relaxed);

        size_t eOrig = e;

//        if(e == 7) {
//            while(
//        }

        size_t length = hashtables::key_accessor<K>::size(key);

        while(current) {
            if(BucketHTE::getConfigBits(current) == eOrig) {
                HTE* currentReal = BucketHTE::getRealPointer(current);
                if(currentReal->matches(length, hashtables::key_accessor<K>::data(key))) {
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
        return get2(key, value);
    }

    bool get2(K const& key, V& value) {

        size_t e = entry(key);
        size_t bucketIdx = e >> _entriesPerBucketPower;
        auto bucket = &_map[bucketIdx];
        e &= _entriesPerBucketMask;

        HTE* current = bucket->_entries[e].load(std::memory_order_relaxed);

        size_t eOrig = e;

        const char* keyData = hashtables::key_accessor<K>::data(key);
        size_t length = hashtables::key_accessor<K>::size(key);

        std::cout << "==== Looking for entry: ";
        printHex(keyData, length);
        std::cout << ", bits=" << eOrig << std::endl;

        bool wouldNotHaveFound = false;

        while(true) {
            if(current) {
                std::cout << "Checking entry << " << bucket << " [" << e << "]: " << BucketHTE::getConfigBits(current) << std::endl;
                if(BucketHTE::getConfigBits(current) == eOrig) {
                    HTE* currentReal = BucketHTE::getRealPointer(current);
                    //std::cout << "Key/Value: (" << currentReal->_length << "," << std::string(currentReal->_keyData, currentReal->_length) << ") -> " << currentReal->_value << std::endl;
                    std::cout << "Key/Value: (";
                    printHex(currentReal->_keyData, currentReal->_length);
                    std::cout << ") -> " << currentReal->_value << std::endl;
                    if(currentReal->matches(length, hashtables::key_accessor<K>::data(key))) {
                        std::cout << "  Match." << std::endl;
                        value = currentReal->_value;
                        return true;
                    }
                }
            } else {
                std::cout << "Skipping entry[" << e << "]: " << BucketHTE::getConfigBits(current) << std::endl;
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

    size_t entry(K const& key) const {
        //return (std::hash<K>{}(key) & _bucketsMask);
        size_t hash = MurmurHash64(hashtables::key_accessor<K>::data(key), hashtables::key_accessor<K>::size(key), 0);
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

    HashTableEntry<K,V>* createHTE(size_t length, const char* keyData, V const& value) {
        HTE* hte = new(_slabManager.alloc<16>(sizeof(HTE)+length)) HTE(length, keyData, value);
        assert( (((intptr_t)hte)&0xF) == 0);
        return hte;
    }

    BucketHTE* createBucket(HTE* const& hteFirst, HTE* const& hte) {
        BucketHTE* bucket = new(_slabManager.alloc<BucketHTE>()) BucketHTE(hteFirst, hte);
        assert( (((intptr_t)bucket)&0xF) == 0);
        return bucket;
    }

    template<typename T>
    void giveMemoryBack(T* const& hte) {
        return _slabManager.free(hte);
    }

    void giveMemoryBack(char* mem, size_t size) {
        return _slabManager.free(mem, size);
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
    static size_t constexpr _entriesPerBucketMask = _entriesPerBucket-1;
    static size_t constexpr _entriesPerBucketPower = __builtin_ctz(_entriesPerBucket);

};

}
