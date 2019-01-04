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

namespace genht {

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

template<typename K, typename V>
class HashTableEntryNext {
public:

    HashTableEntryNext(size_t lengthKey, size_t lengthValue, const char* keyData, const char* valueData): _next(nullptr), _lengthKey(lengthKey), _lengthValue(lengthValue) {
        memmove(_data, keyData, lengthKey);
        memmove(_data+lengthKey, valueData, lengthValue);
    }

    size_t size() const {
        return sizeof(HashTableEntryNext) + _lengthKey + _lengthValue;
    }

//    bool matches(size_t length, const char* keyData) const {
//        if(_lengthKey != length) return false;
//        return !memcmp(_data, keyData, length);
//    }

public:
    HashTableEntryNext* _next;
    __int32_t _lengthKey;
    __int32_t _lengthValue;
    char _data[0];
};

template<typename HT, typename HTE>
struct BucketSearcher {

    BucketSearcher(HT& ht)
        : _ht(ht)
        {
    }

    bool next() { abort(); return false; };
    std::atomic<HTE*>& link() { abort(); return *((std::atomic<HTE*>*)nullptr); };

    bool find(typename HT::key_type const& key, HTE*& current, size_t const& h16l) {
        current = link().load(std::memory_order_relaxed); // TODO: move these to constructor
        while(current) {
            if(HT::KeyComparator::match(current, key, h16l)) {
                current = _ht.getPtr(current);
                return true;
            }
            next();
        }
        return false;
    }
    HT& _ht;

    __attribute__((always_inline))
    bool insert(HTE* current, HTE* hteWithHash) {
        return link().compare_exchange_weak(current, hteWithHash, std::memory_order_release, std::memory_order_relaxed);
    }
};

template<typename HT>
struct OpenAddressing: public BucketSearcher<HT, HashTableEntry<typename HT::key_type, typename HT::value_type>> {
    using HTE = HashTableEntry<typename HT::key_type, typename HT::value_type>;

    OpenAddressing(HT& ht, size_t& e)
        : BucketSearcher<HT, HashTableEntry<typename HT::key_type, typename HT::value_type>>(ht)
        , _e(e)
        {
    }

    std::atomic<HTE*>& link() {
        return this->_ht._map[_e];
    }

    size_t& _e;
};

template<typename HT>
struct Chaining: public BucketSearcher<HT, HashTableEntry<typename HT::key_type, typename HT::value_type>> {
    using HTE = HashTableEntryNext<typename HT::key_type, typename HT::value_type>;

    Chaining(HT& ht, size_t& e)
        : BucketSearcher<HT, HashTableEntry<typename HT::key_type, typename HT::value_type>>(ht)
        {
    }

    std::atomic<HTE*>& link() {
        return *_link;
    }

    std::atomic<HTE*>* _link;
};

template<typename HT>
struct QuadraticCacheLinearBucketSearchStart0: public OpenAddressing<HT> {

    QuadraticCacheLinearBucketSearchStart0(HT& ht, size_t& e)
        : OpenAddressing<HT>(ht, e)
        , _increment(0)
        {
        this->_e &= (HT::_entriesPerBucket-1);
    }

    __attribute__((always_inline))
    void next() {
        this->_e++;
        if((this->_e & (HT::_entriesPerBucket-1)) == 0) {
            this->_e += HT::_entriesPerBucket * (2 * _increment);
            this->_e &= this->_ht._entriesMask;
            _increment++;
        }
    }

    size_t _increment;
};

template<typename HT>
struct LinearCacheLinearBucketSearchStart0: public OpenAddressing<HT> {

    LinearCacheLinearBucketSearchStart0(HT& ht, size_t& e)
        : OpenAddressing<HT>(ht, e)
        , _end(e)
        {
        this->_e &= (HT::_entriesPerBucket-1);
    }

    __attribute__((always_inline))
    void next() {
        this->_e++;
        this->_e &= this->_ht._entriesMask;
    }

    size_t _end;
};

template<typename HT>
struct QuadraticCacheLinearBucketSearch: public OpenAddressing<HT> {

    QuadraticCacheLinearBucketSearch(HT& ht, size_t& e)
        : OpenAddressing<HT>(ht, e)
        , _base(e & ~(HT::_entriesPerBucket-1))
        , _increment(0)
        , _end(e & (HT::_entriesPerBucket-1))
        {
    }

    __attribute__((always_inline))
    void next() {
        this->_e -= _base;
        this->_e = (this->_e+1) & (HT::_entriesPerBucket-1);
        if(this->_e==_end) {
            _base += HT::_entriesPerBucket * (2 * _increment + 1);
            _base &= this->_ht._entriesMask;
            _increment++;
        }
        this->_e += _base;
    }

    size_t _base;
    size_t _increment;
    size_t _end;
};

template<typename HT>
struct LinearCacheLinearBucketSearch: public OpenAddressing<HT> {

    LinearCacheLinearBucketSearch(HT& ht, size_t& e)
        : OpenAddressing<HT>(ht, e)
        , _base(e & ~(HT::_entriesPerBucket-1))
        , _increment(0)
        , _end(e)
        {
    }

    __attribute__((always_inline))
    void next() {
        this->_e -= _base;
        this->_e = (this->_e+1) & (HT::_entriesPerBucket-1);
        if(this->_e==_end) {
            _base += HT::_entriesPerBucket;
            _base &= this->_ht._entriesMask;
            _increment++;
        }
        this->_e += _base;
    }

    size_t _base;
    size_t _increment;
    size_t _end;
};

template<typename HT>
struct QuadraticSearch: public OpenAddressing<HT> {

    QuadraticSearch(HT& ht, size_t& e)
        : OpenAddressing<HT>(ht, e)
        , _increment(0)
        {
    }

    __attribute__((always_inline))
    void next() {
        this->_e += 2 * _increment + 1;
        this->_e &= this->_ht._entriesMask;
        _increment++;
    }

    size_t _increment;
};

template<typename HT>
struct LinearSearch: public OpenAddressing<HT> {

    LinearSearch(HT& ht, size_t& e)
        : OpenAddressing<HT>(ht, e)
        {
    }

    __attribute__((always_inline))
    void next() {
        this->_e = (this->_e+1) & this->_ht._entriesMask;
    }

};

template<typename HT>
struct ChainSearch: public Chaining<HT> {

    ChainSearch(HT& ht, size_t& e)
        : Chaining<HT>(ht, e)
        {
    }

    __attribute__((always_inline))
    void next() {
    }

};

template<typename HT>
struct SingleBucket {

    std::atomic<typename HT::HTE*> _bucket;
};

template< typename K
        , typename V
        , template<typename> typename HASHER
        , template<typename> typename KEY_COMP = COMP_KEY_AND_HASH
        , template<typename> typename DATA_ACCESSOR = hashtables::key_accessor
        , template<typename> typename BUCKET_SEARCH = QuadraticCacheLinearBucketSearch
        , template<typename> typename BUCKET = SingleBucket
        >
class HashTable {
public:

    using key_type = K;
    using value_type = V;

    using AccessorKey = DATA_ACCESSOR<K>;
    using AccessorValue = DATA_ACCESSOR<V>;
    using KeyComparator = KEY_COMP<HashTable>;
    using Hasher = HASHER<K>;
    using BucketSearcher = BUCKET_SEARCH<HashTable>;
    using HTE = typename BucketSearcher::HTE;
    using Bucket = BUCKET<HashTable>;

    friend BucketSearcher;
    friend Bucket;

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

        BucketSearcher searcher(*this, e);

        HTE* current = nullptr;

        if(searcher.find(key, current, h16l)) {
            return *(V*)(current->_data + current->_lengthKey);
        }

        size_t keyLength = AccessorKey::size(key);
        const char* keyData = AccessorKey::data(key);
        size_t valueLength = AccessorValue::size(value);
        const char* valueData = AccessorValue::data(value);
        HTE* hte = createHTE(keyLength, valueLength, keyData, valueData);
        HTE* hteWithHash = makePtrWithHash(hte, h16l);
        while(!searcher.insert(current, hteWithHash)) {
            if(searcher.find(key, current, h16l)) {
                return *(V*)(current->_data + current->_lengthKey);
            }
        }
        return value;
    }

    bool get(K const& key, V& value) {
        size_t h = Hasher{}(key);
        size_t h16l = KeyComparator::hash16LeftFromHash(h);
        size_t e = entryFromhash(h);

        BucketSearcher searcher(*this, e);

        HTE* current = nullptr;

        if(searcher.find(key, current, h16l)) {
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

    size_t bucketSize(Bucket* bucket) {
        size_t s = 0;
        HTE* current = bucket->_bucket.load(std::memory_order_relaxed);
        while(current) {
            s++;
            current = current->getNext();
        }
        return s;
    }

    size_t size() {
        size_t s = 0;
        Bucket* bucket = _map;
        Bucket* end = _map + _buckets;
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

    HTE* createHTE(size_t keyLength, size_t valueLength, const char* keyData, const char* valueData) {
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
                    if(_map[idx+b]._bucket.load(std::memory_order_relaxed)) {
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
                if(_map[idx+b]._bucket.load(std::memory_order_relaxed)) {
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
    Bucket* _map;
    SlabManager _slabManager;

private:
    static size_t constexpr _bucketSize = CACHE_LINE_SIZE_IN_BYTES;
    static size_t constexpr _entriesPerBucket = _bucketSize/sizeof(void*);
};

}
