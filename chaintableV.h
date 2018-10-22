#pragma once

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include <atomic>
#include <new>

#include "mmapper.h"
#include "key_accessor.h"

namespace chaintablegenericV {

template<typename K, typename V>
class HashTableEntry {
public:

    HashTableEntry(size_t length, const char* keyData, V const& value, HashTableEntry* next): _value(value), _length(length), _next(next) {
        memmove(_keyData, keyData, length);
    }

    HashTableEntry* getNext() {
        return _next.load(std::memory_order_relaxed);
    }
    void setNext(HashTableEntry* next) {
        _next.store(next, std::memory_order_relaxed);
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
    std::atomic<HashTableEntry*> _next;
    char _keyData[0];
};

template<typename K, typename V>
class HashTable {
public:
    static constexpr size_t PAGE_SIZE_P2 = 20;
public:

    using HTE = HashTableEntry<K,V>;

    HashTable(size_t bucketsScale)
        : _buckets(1ULL << bucketsScale)
        , _map(nullptr)
        {
        _map = (decltype(_map))MMapper::mmapForMap(_buckets * sizeof(std::atomic<HashTableEntry<K,V>*>));
    }
public:
    size_t insert(K const& key, V const& value) {
//        printf("key:   %zx\n", key);
        size_t h = hash(key);
        size_t e = entryFromhash(h);
//        printf("entry: %zx\n", e);
        HashTableEntry<K,V>* current = _map[e].load(std::memory_order_relaxed);
//        printf("cur:   %p\n", current);

        const char* keyData = hashtables::key_accessor<K>::data(key);
        size_t length = hashtables::key_accessor<K>::size(key);

        std::atomic<HashTableEntry<K,V>*>* parentLink = &_map[e];
        while(current) {
//            printf("checking existing entry: %zx -> %zx\n", current->_key, current->_value);
            current = (HashTableEntry<K,V>*)((intptr_t)current & 0x0000FFFFFFFFFFFFULL);
            if(current->matches(length, keyData)) return current->_value;
            parentLink = &(current->_next);
            current = current->getNext();
        }

        HashTableEntry<K,V>* hte = createHTE(length, keyData, value, nullptr);
        while(!parentLink->compare_exchange_weak(current, hte, std::memory_order_release, std::memory_order_relaxed)) {
            while(current) {
                current = (HashTableEntry<K,V>*)((intptr_t)current & 0x0000FFFFFFFFFFFFULL);
                if(current->matches(length, keyData)) {
                    //delete hte;
                    return current->_value;
                }
                parentLink = &current->_next;
                current = current->getNext();
            }
        }
//        std::cout << "Inserted " << hte << " @ [" << e << "] ";
//        printHex(keyData, length);
//        std::cout << ") -> " << value << ", h=" << h << ", hash16l=" << h16l << std::endl;

        return value;
    }

    bool get(K const& key, V& value) {
        size_t h = hash(key);
        size_t e = entryFromhash(h);
//        printf("entry: %zx\n", e);
        HashTableEntry<K,V>* current = _map[e].load(std::memory_order_relaxed);
//        printf("cur:   %p\n", current);
        size_t length = hashtables::key_accessor<K>::size(key);

        while(current) {
//            printf("checking existing entry: %zx -> %zx\n", current->_key, current->_value);
            current = (HashTableEntry<K,V>*)((intptr_t)current & 0x0000FFFFFFFFFFFFULL);
            if(current->matches(length, hashtables::key_accessor<K>::data(key))) {
                value = current->_value;
                return true;
            }
            current = current->getNext();
        }

        return get2(key, value);
    }

    void printHex(const char* key, size_t length) {
        for(size_t i = 0; i < length; ++i) {
            printf(" %X", ((const unsigned char*)key)[i] & 0xFF);
        }
    }

    bool get2(K const& key, V& value) {
        size_t h = hash(key);
        size_t e = entryFromhash(h);
        HashTableEntry<K,V>* current = _map[e].load(std::memory_order_relaxed);

        const char* keyData = hashtables::key_accessor<K>::data(key);
        size_t length = hashtables::key_accessor<K>::size(key);

        std::cout << "==== Looking for entry: ";
        printHex(keyData, length);
        std::cout << ", h=" << h
                  << ", e=" << e
                  << std::endl;

        while(current) {
            std::cout << "Checking entry " << current << std::endl;
            current = (HashTableEntry<K,V>*)((intptr_t)current & 0x0000FFFFFFFFFFFFULL);
            std::cout << "  Key/Value: (";
            printHex(current->_keyData, current->_length);
            std::cout << ") -> " << current->_value << std::endl;
            if(current->matches(length, keyData)) {
                value = current->_value;
                std::cout << "    Match." << std::endl;
                return true;
            }
            current = current->getNext();
        }

        return false;
    }

    size_t entryFromhash(size_t const& h) {
        return h & (_buckets-1);
    }

    size_t hash(K const& key) {
        return MurmurHash64(key);
    }

    size_t bucketSize(std::atomic<HashTableEntry<K,V>*>* bucket) {
        size_t s = 0;
        HashTableEntry<K,V>* current = bucket->load(std::memory_order_relaxed);
        while(current) {
            s++;
            current = (HashTableEntry<K,V>*)((intptr_t)current & 0x0000FFFFFFFFFFFFULL);
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

    HashTableEntry<K,V>* createHTE(size_t length, const char* keyData, V const& value, HashTableEntry<K,V>* next) {
        HTE* hte = new(_slabManager.alloc<4>(sizeof(HTE) + length)) HTE(length, keyData, value, next);
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
                    entry = (HashTableEntry<K,V>*)((intptr_t)entry & 0x0000FFFFFFFFFFFFULL);
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
                        entry = (HashTableEntry<K,V>*)((intptr_t)entry & 0x0000FFFFFFFFFFFFULL);
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
