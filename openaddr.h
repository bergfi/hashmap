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
#include "key_accessor.h"

#define CACHE_LINE_SIZE_BP2 6
#define CACHE_LINE_SIZE_IN_BYTES (1<<CACHE_LINE_SIZE_BP2)

namespace openaddr {

template<typename HT>
struct COMP_KEY {

    __attribute__((always_inline))
    static bool match(typename HT::HTE* key_in_table, typename HT::key_type const& key, size_t hash16l) {
        return key == *key_in_table;
    }

    __attribute__((always_inline))
    static size_t hash16LeftFromHash(size_t h) {
        return 0ULL;
    }
};

template<typename HT>
struct COMP_KEY_AND_HASH {

    __attribute__((always_inline))
    static bool match(typename HT::HTE* hte, typename HT::key_type const& key, size_t hash16l) {
        if( HT::getHash(hte) != hash16l ) {
            return false;
        }
        return key == *(typename HT::key_type*)HT::getPtr(hte)->_data;
    }

    __attribute__((always_inline))
    static size_t hash16LeftFromHash(size_t h) {
        return h & 0xFFFF000000000000ULL;
    }
};

template<typename K, typename V>
class HashTableEntry {
public:

    HashTableEntry(size_t lengthKey, size_t lengthValue, const char* keyData, const char* valueData): _lengthKey(lengthKey), _lengthValue(lengthValue) {
        memmove(_data, keyData, lengthKey);
        memmove(_data+lengthKey, valueData, lengthValue);
    }

    size_t size() const {
        return sizeof(HashTableEntry) + _lengthKey + _lengthValue;
    }

//    bool matches(size_t length, const char* keyData) const {
//        if(_lengthKey != length) return false;
//        return !memcmp(_data, keyData, length);
//    }

public:
    __int32_t _lengthKey;
    __int32_t _lengthValue;
    char _data[0];
};

template<typename HT>
struct QuadraticCacheLinearBucketSearchStart0 {

    QuadraticCacheLinearBucketSearchStart0(HT& ht, size_t& e)
        : _ht(ht)
        , _e(e)
        , _increment(0)
        {
        _e &= (HT::_entriesPerBucket-1);
    }

    bool next(typename HT::key_type const& key, typename HT::HTE*& current, size_t const& h16l) {
        current = _ht._map[_e].load(std::memory_order_relaxed);
        while(current) {
            if(HT::KeyComparator::match(current, key, h16l)) {
                current = _ht.getPtr(current);
                return true;
            }
            _e++;
            if((_e & (HT::_entriesPerBucket-1)) == 0) {
                _e += HT::_entriesPerBucket * (2 * _increment);
                _e &= _ht._entriesMask;
                _increment++;
            }
            current = _ht._map[_e].load(std::memory_order_relaxed);
        }
        return false;
    }

    HT& _ht;
    size_t& _e;
    size_t _increment;
};

template<typename HT>
struct LinearCacheLinearBucketSearchStart0 {

    LinearCacheLinearBucketSearchStart0(HT& ht, size_t& e)
        : _ht(ht)
        , _e(e)
        , _end(e)
        {
        _e &= (HT::_entriesPerBucket-1);
    }

    bool next(typename HT::key_type const& key, typename HT::HTE*& current, size_t const& h16l) {
        current = _ht._map[_e].load(std::memory_order_relaxed);
        while(current) {
            if(HT::KeyComparator::match(current, key, h16l)) {
                current = _ht.getPtr(current);
                return true;
            }
            _e++;
            _e &= _ht._entriesMask;
            current = _ht._map[_e].load(std::memory_order_relaxed);
        }
        return false;
    }

    HT& _ht;
    size_t& _e;
    size_t _end;
};

template<typename HT>
struct QuadraticCacheLinearBucketSearch {

    QuadraticCacheLinearBucketSearch(HT& ht, size_t& e)
        : _ht(ht)
        , _e(e)
        , _base(e & ~(HT::_entriesPerBucket-1))
        , _increment(0)
        , _end(e & (HT::_entriesPerBucket-1))
        {
//        _e -= _base;
    }

    bool next(typename HT::key_type const& key, typename HT::HTE*& current, size_t const& h16l) {
        current = _ht._map[_e].load(std::memory_order_relaxed);
        while(current) {
            if(HT::KeyComparator::match(current, key, h16l)) {
                current = _ht.getPtr(current);
                return true;
            }
            _e -= _base;
            _e = (_e+1) & (HT::_entriesPerBucket-1);
            if(_e==_end) {
                _base += HT::_entriesPerBucket * (2 * _increment + 1);
                _base &= _ht._entriesMask;
                _increment++;
            }
            _e += _base;
            current = _ht._map[_e].load(std::memory_order_relaxed);
        }
        return false;
    }

    HT& _ht;
    size_t& _e;
    size_t _base;
    size_t _increment;
    size_t _end;
};

template<typename HT>
struct LinearCacheLinearBucketSearch {

    LinearCacheLinearBucketSearch(HT& ht, size_t& e)
        : _ht(ht)
        , _e(e)
        , _base(e & ~(HT::_entriesPerBucket-1))
        , _increment(0)
        , _end(e)
        {
//        _e -= _base;
    }

    bool next(typename HT::key_type const& key, typename HT::HTE*& current, size_t const& h16l) {
        current = _ht._map[_e].load(std::memory_order_relaxed);
        while(current) {
            if(HT::KeyComparator::match(current, key, h16l)) {
                current = _ht.getPtr(current);
                return true;
            }
            _e -= _base;
            _e = (_e+1) & (HT::_entriesPerBucket-1);
            if(_e==_end) {
                _base += HT::_entriesPerBucket;
                _base &= _ht._entriesMask;
                _increment++;
            }
            _e += _base;
            current = _ht._map[_e].load(std::memory_order_relaxed);
        }
        return false;
    }

    HT& _ht;
    size_t& _e;
    size_t _base;
    size_t _increment;
    size_t _end;
};

template<typename HT>
struct QuadraticSearch {

    QuadraticSearch(HT& ht, size_t& e)
        : _ht(ht)
        , _e(e)
        , _increment(0)
        {
//        _e -= _base;
    }

    bool next(typename HT::key_type const& key, typename HT::HTE*& current, size_t const& h16l) {
        current = _ht._map[_e].load(std::memory_order_relaxed);
        while(current) {
            if(HT::KeyComparator::match(current, key, h16l)) {
                current = _ht.getPtr(current);
                return true;
            }
            _e += 2 * _increment + 1;
            _e &= _ht._entriesMask;
            _increment++;
            current = _ht._map[_e].load(std::memory_order_relaxed);
        }
        return false;
    }

    HT& _ht;
    size_t& _e;
    size_t _increment;
};

template<typename HT>
struct LinearSearch {

    LinearSearch(HT& ht, size_t& e)
        : _ht(ht)
        , _e(e)
        {
//        _e -= _base;
    }

    bool next(typename HT::key_type const& key, typename HT::HTE*& current, size_t const& h16l) {
        current = _ht._map[_e].load(std::memory_order_relaxed);
        while(current) {
            if(HT::KeyComparator::match(current, key, h16l)) {
                current = _ht.getPtr(current);
                return true;
            }
            _e = (_e+1) & _ht._entriesMask;
            current = _ht._map[_e].load(std::memory_order_relaxed);
        }
        return false;
    }

    HT& _ht;
    size_t& _e;
};

template< typename K
        , typename V
        , template<typename> typename HASHER
        , template<typename> typename KEY_COMP = COMP_KEY_AND_HASH
        , template<typename> typename DATA_ACCESSOR = hashtables::key_accessor
        , template<typename> typename BUCKET_SEARCH = QuadraticCacheLinearBucketSearch
        >
class HashTable {
public:

    using HTE = HashTableEntry<K, V>;
    using AccessorKey = DATA_ACCESSOR<K>;
    using AccessorValue = DATA_ACCESSOR<V>;
    using KeyComparator = KEY_COMP<HashTable>;
    using Hasher = HASHER<K>;
    using BucketSearcher = BUCKET_SEARCH<HashTable>;

    friend BucketSearcher;

    using key_type = K;
    using value_type = V;

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

        size_t h = Hasher{}(key);
        size_t h16l = KeyComparator::hash16LeftFromHash(h);
        size_t e = entryFromhash(h);

        BucketSearcher search(*this, e);

        HashTableEntry<K,V>* current = nullptr;

        if(search.next(key, current, h16l)) {
            return *(V*)(current->_data + current->_lengthKey);
        }

        size_t keyLength = AccessorKey::size(key);
        const char* keyData = AccessorKey::data(key);
        size_t valueLength = AccessorValue::size(value);
        const char* valueData = AccessorValue::data(value);
        HashTableEntry<K,V>* hte = createHTE(keyLength, valueLength, keyData, valueData);
        HashTableEntry<K,V>* hteWithHash = makePtrWithHash(hte, h16l);
        while(!_map[e].compare_exchange_weak(current, hteWithHash, std::memory_order_release, std::memory_order_relaxed)) {
            if(search.next(key, current, h16l)) {
                return *(V*)(current->_data + current->_lengthKey);
            }
        }
        return value;
    }

    bool get(K const& key, V& value) {
        size_t h = Hasher{}(key);
        size_t h16l = KeyComparator::hash16LeftFromHash(h);
        size_t e = entryFromhash(h);

        BucketSearcher search(*this, e);

        HashTableEntry<K,V>* current = nullptr;

        if(search.next(key, current, h16l)) {
            value = *(V*)(current->_data + current->_lengthKey);
            return true;
        } else {
            return false;
        }
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

    HashTableEntry<K,V>* createHTE(size_t keyLength, size_t valueLength, const char* keyData, const char* valueData) {
        HTE* hte = new(_slabManager.alloc<4>(sizeof(HTE) + keyLength + valueLength)) HTE(keyLength, valueLength, keyData, valueData);
        return hte;
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
