#include <hre/config.h>

#include <atomic>
#include <thread>
#include <vector>
extern "C" {
#include <mc-lib/hashtable.h>
}

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/mman.h>


#include <atomic>
#include <boost/mpl/vector.hpp>
#include <boost/mpl/for_each.hpp>
#include <deque>
#include <dlfcn.h>
#include <getopt.h>
#include <string>
#include <iomanip>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>
#include <shared_mutex>

#include <numa.h>

#include "mmaptable.h"
#include "mmaptableUB.h"
#include "mmapcache.h"
#include "mmapquadtable.h"
#include "mmapquadtableC.h"
#include "mmapquadtableCU.h"
#include "mmapquadtableCUV.h"
#include "chaintable.h"
#include "chaintableUB.h"
#include "chaintableUBVK.h"
#include "chaintableV.h"
#include "chaintablegeneric.h"
#include "chaintableints.h"
#include "cachechain.h"
#include "cachechain2.h"
#include "cachechain2config.h"
#include "cachechain3.h"
#include "cachechain3adaptiveconfig.h"
#include "cachechain3upperbits.h"
#include "cachechain3vkeysize.h"
#include "cachechain3UBVK.h"
#include "mmapmmap.h"
#include "insituUB.h"
#include "insituUBquad.h"
#include "insituRevCasUB.h"
#include "insituRevCasUBquad.h"
#include "insituDCASUB.h"
#include "insituDCASUBquad.h"
#include "insitu32.h"
#include "insituQ32.h"
#include "openaddr.h"

#include "test_ints.h"
#include "test_ints2.h"
#include "test_strings.h"
#include "test_words.h"
#include "test_vectors.h"

#include "wrappers.h"
#include "mystring.h"
#include "myvector.h"

#include <libfrugi/Settings.h>

using namespace libfrugi;

#define __STRINGIFY(x) #x
#define STRINGIFY(x) __STRINGIFY(x)

#define HAVE_DIVINE

#ifdef HAVE_DIVINE
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wall -Wextra -Wpedantic"
#include <brick-hashset>
#pragma GCC diagnostic pop
#endif

// CLHT
#ifdef HAVE_CLHT
extern "C" {
#include "atomic_ops.h"

#if defined(USE_SSPFD)
#   include "sspfd.h"
#endif

#include "CLHT/include/clht.h"
}
#endif
// END CHLT

class Timer {
private:
#if WIN32
    LARGE_INTEGER freq;
    LARGE_INTEGER start;
#else
    timespec start;
#endif
public:
    Timer() {
        reset();
    }
    void reset() {
        clock_gettime(CLOCK_MONOTONIC_RAW, &start);
    }
    double getElapsedSeconds() {
        timespec now;
        clock_gettime(CLOCK_MONOTONIC_RAW, &now);
        return (double)(now.tv_sec -start.tv_sec )
             + (double)(now.tv_nsec-start.tv_nsec)*0.000000001;
    }
};


int      size_t_cmp   (void * a, void * b, void * ctx) {
    return (size_t)b-(size_t)a;
}
void *   size_t_clone (void * a, void * ctx) {
    return a;
}
uint32_t size_t_hash  (void * a, void * ctx) {
    return std::_Hash_impl::hash(&a, sizeof(size_t));
}
void     size_t_free  (void * a) {
}

// ----------

template<typename OUT, typename CONTAINER>
void printDensitygraph(OUT& out, CONTAINER& elements) {

    unsigned char level[] = {' ', '_', '-', '+', '=', '*', '#'};

    size_t maxElements = 0;
    for(size_t element: elements) {
        maxElements = std::max(maxElements, element);
    }

    out << "\033[1;37;44m";
    for(size_t i = 0; i < elements.size(); ++i) {
        if(elements[i]==0) {
            out << char(level[0]);
        } else if(elements[i]==maxElements) {
            out << char(level[6]);
        } else {
            size_t percent = 100*elements[i]/maxElements;
            if(95<percent) {
                out << char(level[5]);
            } else {
                out << char(level[1+3*elements[i]/maxElements]);
            }
        }
    }
    out << "\033[0m";
    out << " " << "\033[1;37;44m" "#"<< "\033[0m" << " = " << maxElements;
}

// ----------


class HTImpl {
};

template<template<typename,typename> typename HT, typename K, typename V>
class ImplMyAPI: public HTImpl {
public:

    using key_type = K;
    using value_type = V;

    ImplMyAPI(std::string const& name): _name(name) {}

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        ht = new HT<K,V>(bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        (void)tid;
        ht->thread_init();
    }

    __attribute__((always_inline))
    void insert(K const& k, V const& v) {
        ht->insert(k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, V& v) {
        return ht->get(k, v);
    }

    __attribute__((always_inline))
    void cleanup() {
        delete ht;
    }

    __attribute__((always_inline))
    std::string name() const {
        return _name + "<" + std::string(typeid(K).name()) + "," + std::string(typeid(V).name()) + ">";
    }

protected:
    std::string _name;
    HT<K,V>* ht;
};

template<typename HT>
class ImplMyAPI2: public HTImpl {
public:

    using key_type = typename HT::key_type;
    using value_type = typename HT::value_type;

    ImplMyAPI2(std::string const& name): _name(name) {}

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        ht = new HT(bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        (void)tid;
        ht->thread_init();
    }

    __attribute__((always_inline))
    void insert(key_type const& k, value_type const& v) {
        ht->insert(k, v);
    }

    __attribute__((always_inline))
    bool get(key_type const& k, value_type& v) {
        return ht->get(k, v);
    }

    __attribute__((always_inline))
    void cleanup() {
        delete ht;
    }

    __attribute__((always_inline))
    std::string name() const {
        return _name + "<" + std::string(typeid(key_type).name()) + "," + std::string(typeid(value_type).name()) + ">";
    }

protected:
    std::string _name;
    HT* ht;
};

class ImplChainInts: public HTImpl {
public:

    using key_type = size_t;
    using value_type = size_t;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        ht = new chaintableints::HashTable(bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        (void)tid;
        ht->thread_init();
    }

    __attribute__((always_inline))
    void insert(size_t k, size_t v) {
        ht->insert(k, v);
    }

    __attribute__((always_inline))
    bool get(size_t k, size_t& v) {
        return ht->get(k, v);
    }

    __attribute__((always_inline))
    void cleanup() {
        delete ht;
    }

    __attribute__((always_inline))
    std::string name() const {
        return "Chain";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        chaintableints::HashTable::stats stats;
        ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg chn: " << stats.avgChainLength
            << ", lngst chn: " << stats.longestChain
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }

private:
    chaintableints::HashTable* ht;
};

template<typename K, typename V>
class ImplChain: public ImplMyAPI<chaintable::HashTable, K, V> {
public:

    ImplChain(): ImplMyAPI<chaintable::HashTable, K, V>("Chain") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename chaintable::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg chn: " << stats.avgChainLength
            << ", lngst chn: " << stats.longestChain
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplChainSlab: public ImplMyAPI<chaintablegeneric::HashTable, K, V> {
public:

    ImplChainSlab(): ImplMyAPI<chaintablegeneric::HashTable, K, V>("ChainSlab") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename chaintablegeneric::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg chn: " << stats.avgChainLength
            << ", lngst chn: " << stats.longestChain
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplChainGenericUB: public ImplMyAPI<chaintablegenericUB::HashTable, K, V> {
public:

    ImplChainGenericUB(): ImplMyAPI<chaintablegenericUB::HashTable, K, V>("ChainU") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename chaintablegenericUB::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg chn: " << stats.avgChainLength
            << ", lngst chn: " << stats.longestChain
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplChainGenericUBVK: public ImplMyAPI<chaintablegenericUBVK::HashTable, K, V> {
public:

    ImplChainGenericUBVK(): ImplMyAPI<chaintablegenericUBVK::HashTable, K, V>("ChainUV") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename chaintablegenericUBVK::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg chn: " << stats.avgChainLength
            << ", lngst chn: " << stats.longestChain
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplChainGenericV: public ImplMyAPI<chaintablegenericV::HashTable, K, V> {
public:

    ImplChainGenericV(): ImplMyAPI<chaintablegenericV::HashTable, K, V>("ChainV") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename chaintablegenericV::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg chn: " << stats.avgChainLength
            << ", lngst chn: " << stats.longestChain
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplMmap: public ImplMyAPI<mmaptable::HashTable_mmap, K, V> {
public:

    ImplMmap(): ImplMyAPI<mmaptable::HashTable_mmap, K, V>("Mmap") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename mmaptable::HashTable_mmap<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplMmapU: public ImplMyAPI<mmaptableUB::HashTable_mmap, K, V> {
public:

    ImplMmapU(): ImplMyAPI<mmaptableUB::HashTable_mmap, K, V>("MmapU") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename mmaptableUB::HashTable_mmap<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplMmapCache: public HTImpl {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        ht = new mmapcachetable::HashTable<K,V>(bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        (void)tid;
        ht->thread_init();
    }

    __attribute__((always_inline))
    void insert(K const& k, V const& v) {
        ht->insert(k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, V& v) {
        return ht->get(k, v);
    }

    __attribute__((always_inline))
    void cleanup() {
        delete ht;
    }

    __attribute__((always_inline))
    std::string name() const {
        return "MmapC" + std::string(typeid(K).name());
    }

    __attribute__((always_inline))
    void printInfo(FILE* file) {
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename mmapcachetable::HashTable<K,V>::stats stats;
        ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }

private:
    mmapcachetable::HashTable<K,V>* ht;
};
//
//template<typename K, typename V>
//class ImplMmapQuadC: public HTImpl {
//public:
//
//    using key_type = K;
//    using value_type = V;
//
//    __attribute__((always_inline))
//    void init(size_t bucketScale) {
//        ht = new mmapquadtable::HashTable<K,V>(bucketScale);
//    }
//
//    __attribute__((always_inline))
//    void thread_init(int tid) {
//        (void)tid;
//        ht->thread_init();
//    }
//
//    __attribute__((always_inline))
//    void insert(K const& k, V const& v) {
//        ht->insert(k, v);
//    }
//
//    __attribute__((always_inline))
//    bool get(K const& k, V& v) {
//        return ht->get(k, v);
//    }
//
//    __attribute__((always_inline))
//    void cleanup() {
//        delete ht;
//    }
//
//    __attribute__((always_inline))
//    std::string name() const {
//        return "MmapQC" + std::string(typeid(K).name());
//    }
//
//    __attribute__((always_inline))
//    void printInfo(FILE* file) {
//    }
//
//    __attribute__((always_inline))
//    void statsString(std::ostream& out, size_t bars) {
//        typename mmapquadtable::HashTable<K,V>::stats stats;
//        ht->getStats(stats);
//        out << "size: " << stats.size
//            << ", buckets: " << stats.usedBuckets
//            << ", cols: " << stats.collisions
//            << ", avg b. size: " << stats.avgBucketSize
//            << ", bgst bucket: " << stats.biggestBucket
//            ;
//        out << std::endl;
//        std::vector<size_t> elements;
//        elements.reserve(bars);
//        ht->getDensityStats(bars, elements);
//        printDensitygraph(out, elements);
//    }
//
//private:
//    mmapquadtable::HashTable<K,V>* ht;
//};

template<typename K, typename V>
class ImplMmapQuad: public ImplMyAPI<mmapquadtable::HashTable, K, V> {
public:

    ImplMmapQuad(): ImplMyAPI<mmapquadtable::HashTable, K, V>("MmapQ") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename mmapquadtable::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplMmapQuadC: public ImplMyAPI<mmapquadtableC::HashTable, K, V> {
public:

    ImplMmapQuadC(): ImplMyAPI<mmapquadtableC::HashTable, K, V>("MmapQC") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename mmapquadtableC::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplMmapQuadCU: public ImplMyAPI<mmapquadtableCU::HashTable, K, V> {
public:

    ImplMmapQuadCU(): ImplMyAPI<mmapquadtableCU::HashTable, K, V>("MmapQCU") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename mmapquadtableCU::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplMmapQuadCUV: public ImplMyAPI<mmapquadtableCUV::HashTable, K, V> {
public:

    ImplMmapQuadCUV(): ImplMyAPI<mmapquadtableCUV::HashTable, K, V>("MmapQCUV") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename mmapquadtableCUV::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplOpenAddr: public ImplMyAPI2<openaddr::HashTable<K, V, MurmurHasher>> {
public:

    ImplOpenAddr(): ImplMyAPI2<openaddr::HashTable<K, V, MurmurHasher>>("OpenAddr") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename openaddr::HashTable<K,V, MurmurHasher>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplCacheChain: public HTImpl {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        ht = new cachechain::HashTable<K,V>(bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        (void)tid;
        ht->thread_init();
    }

    __attribute__((always_inline))
    void insert(K const& k, V const& v) {
        ht->insert(k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, V& v) {
        return ht->get(k, v);
    }

    __attribute__((always_inline))
    void cleanup() {
        delete ht;
    }

    __attribute__((always_inline))
    std::string name() const {
        return "ChainC7" + std::string(typeid(K).name());
    }

    __attribute__((always_inline))
    void printInfo(FILE* file) {
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename cachechain::HashTable<K,V>::stats stats;
        ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            << ", total chn: " << stats.totalChain
            << ", avg chn: " << stats.avgChainLength
            << ", lngst chn: " << stats.longestChain
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }

private:
    cachechain::HashTable<K,V>* ht;
};

template<typename K, typename V>
class ImplCacheChain2: public HTImpl {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        ht = new cachechain2::HashTable<K,V>(bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        (void)tid;
        ht->thread_init();
    }

    __attribute__((always_inline))
    void insert(K const& k, V const& v) {
        ht->insert(k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, V& v) {
        return ht->get(k, v);
    }

    __attribute__((always_inline))
    void cleanup() {
        delete ht;
    }

    __attribute__((always_inline))
    std::string name() const {
        return "ChainC0" + std::string(typeid(K).name());
    }

    __attribute__((always_inline))
    void printInfo(FILE* file) {
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename cachechain2::HashTable<K,V>::stats stats;
        ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            << ", total chn: " << stats.totalChain
            << ", avg chn: " << stats.avgChainLength
            << ", lngst chn: " << stats.longestChain
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }

private:
    cachechain2::HashTable<K,V>* ht;
};

template<typename K, typename V>
class ImplCacheChain2Config: public HTImpl {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        ht = new cachechain2config::HashTable<K,V>(bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        (void)tid;
        ht->thread_init();
    }

    __attribute__((always_inline))
    void insert(K const& k, V const& v) {
        ht->insert(k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, V& v) {
        return ht->get(k, v);
    }

    __attribute__((always_inline))
    void cleanup() {
        delete ht;
    }

    __attribute__((always_inline))
    std::string name() const {
        return "CChain2C" + std::string(typeid(K).name());
    }

    __attribute__((always_inline))
    void printInfo(FILE* file) {
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename cachechain2config::HashTable<K,V>::stats stats;
        ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            << ", total chn: " << stats.totalChain
            << ", avg chn: " << stats.avgChainLength
            << ", lngst chn: " << stats.longestChain
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }

private:
    cachechain2config::HashTable<K,V>* ht;
};

template<typename K, typename V>
class ImplCacheChain3: public ImplMyAPI<cachechain3::HashTable, K, V> {
public:

    ImplCacheChain3(): ImplMyAPI<cachechain3::HashTable, K, V>("ChainC") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename cachechain3::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            << ", total chn: " << stats.totalChain
            << ", avg chn: " << stats.avgChainLength
            << ", lngst chn: " << stats.longestChain
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplCacheChain3AC: public ImplMyAPI<cachechain3adaptiveconfig::HashTable, K, V> {
public:

    ImplCacheChain3AC(): ImplMyAPI<cachechain3adaptiveconfig::HashTable, K, V>("CChain3AC") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename cachechain3adaptiveconfig::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            << ", total chn: " << stats.totalChain
            << ", avg chn: " << stats.avgChainLength
            << ", lngst chn: " << stats.longestChain
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplCacheChain3VK: public ImplMyAPI<cachechain3vkeysize::HashTable, K, V> {
public:

    ImplCacheChain3VK(): ImplMyAPI<cachechain3vkeysize::HashTable, K, V>("ChainCV") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename cachechain3vkeysize::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            << ", total chn: " << stats.totalChain
            << ", avg chn: " << stats.avgChainLength
            << ", lngst chn: " << stats.longestChain
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplCacheChain3UB: public ImplMyAPI<cachechain3upperbits::HashTable, K, V> {
public:

    ImplCacheChain3UB(): ImplMyAPI<cachechain3upperbits::HashTable, K, V>("ChainCU") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename cachechain3upperbits::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            << ", total chn: " << stats.totalChain
            << ", avg chn: " << stats.avgChainLength
            << ", lngst chn: " << stats.longestChain
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplCacheChain3UBVK: public ImplMyAPI<cachechain3UBVK::HashTable, K, V> {
public:

    ImplCacheChain3UBVK(): ImplMyAPI<cachechain3UBVK::HashTable, K, V>("ChainCUV") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename cachechain3UBVK::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            << ", total chn: " << stats.totalChain
            << ", avg chn: " << stats.avgChainLength
            << ", lngst chn: " << stats.longestChain
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplMmapMmap: public ImplMyAPI<mmapmmap::HashTable, K, V> {
public:

    ImplMmapMmap(): ImplMyAPI<mmapmmap::HashTable, K, V>("MmapMmap") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename mmapmmap::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg chn: " << stats.avgChainLength
            << ", lngst chn: " << stats.longestChain
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

#ifdef HAVE_DIVINE
template<typename K>
class ImplDivineHT: public HTImpl {
public:

    using key_type = K;
    using value_type = size_t;
    using map_type = brick::hashset::FastConcurrent<K, MurmurHasher<key_type>>;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        ht = new map_type;
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        (void)tid;
    }

    __attribute__((always_inline))
    void insert(key_type k, value_type v) {
        ht->insert(k);
    }

    __attribute__((always_inline))
    bool get(key_type k, value_type& v) {
        auto it = ht->find(k);
        if(it.valid()) {
            v = 0;
            return true;
        } else {
            return false;
        }
    }

    __attribute__((always_inline))
    void cleanup() {
        delete ht;
    }

    __attribute__((always_inline))
    std::string name() const {
        return "DIVINEHT";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
    }

private:
    map_type* ht;
};
#endif

class ImplCpp: public HTImpl {
public:

    using mutex_type = std::shared_timed_mutex;
    using read_only_lock  = std::shared_lock<mutex_type>;
    using updatable_lock = std::unique_lock<mutex_type>;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        map.reserve(1 << bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
    }

    __attribute__((always_inline))
    void insert(size_t k, size_t v) {
        updatable_lock lock(mtx);
        map[k] = v;
//        printf("put %zu = %zu\n", k, v);
    }

    __attribute__((always_inline))
    bool get(size_t k, size_t& v) {
        read_only_lock lock(mtx);
        auto it = map.find(k);
        if(it != map.end()) {
            v = it->second;
//            printf("get %zu = %zu\n", k, v);
            return true;
        } else {
//            printf("get %zu = <no value>\n", k);
            return false;
        }
    }

    __attribute__((always_inline))
    void cleanup() {
    }

    __attribute__((always_inline))
    std::string name() const {
        return "C++";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        out << "size: " << map.size()
            ;
    }

private:
    std::unordered_map<size_t, size_t> map;
    mutex_type mtx;
};

class ImplChunkHT: public HTImpl {
public:

    using key_type = size_t;
    using value_type = size_t;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        ht_datatype.cmp = size_t_cmp;
        ht_datatype.clone = size_t_clone;
        ht_datatype.hash = size_t_hash;
        ht_datatype.free = size_t_free;
        ht_datatype.cmp = nullptr;//size_t_cmp;
        ht_datatype.clone = nullptr;//size_t_clone;
        ht_datatype.hash = nullptr;//size_t_hash;
        ht_datatype.free = nullptr;//size_t_free;

        ht = ht_alloc(&ht_datatype, bucketScale);

    }

    __attribute__((always_inline))
    void thread_init(int tid) {
    }

    __attribute__((always_inline))
    void insert(size_t k, size_t v) {
        assert(v);
        size_t clone;
        ht_cas_empty(ht, k, v, &clone, nullptr);
    }

    __attribute__((always_inline))
    bool get(size_t k, size_t& v) {
        v = ht_get(ht, k, nullptr);
        return v != 0;
    }

    __attribute__((always_inline))
    void cleanup() {
    }

    __attribute__((always_inline))
    std::string name() const {
        return "ChunkHT";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        (void)out;
    }

private:
    datatype_t ht_datatype;
    hashtable_t* ht;
};

template<typename K>
class ImplChunkHTGeneric: public HTImpl {
public:

    using key_type = K;
    using value_type = size_t;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        ht_datatype.cmp = K::cmp;
        ht_datatype.clone = K::clone;
        ht_datatype.hash = K::s_hash;
        ht_datatype.free = K::free;

        ht = ht_alloc(&ht_datatype, bucketScale);

    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        _slabManager.thread_init();
    }

    __attribute__((always_inline))
    void insert(K const& k, size_t v) {
        assert(v);
        //K clone;
        map_key_t clone = 0;
        //map_key_t key = (intptr_t)new my_string(k);
        ht_cas_empty(ht, (intptr_t)&k, v, (map_key_t*)&clone, &_slabManager);
    }

    __attribute__((always_inline))
    bool get(K const& k, size_t& v) {
        v = ht_get(ht, (map_key_t)&k, &_slabManager);
        return v != 0;
    }

    __attribute__((always_inline))
    void cleanup() {
    }

    __attribute__((always_inline))
    static std::string name() {
        return "ChunkHT";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        (void)out;
    }

private:
    datatype_t ht_datatype;
    hashtable_t* ht;
    SlabManager _slabManager;
};

#ifdef HAVE_CLHT
class ImplCLHT: public HTImpl {
public:

    using key_type = size_t;
    using value_type = size_t;

    ImplCLHT(): myCLHT(nullptr) {}

    ImplCLHT& operator=(ImplCLHT&&) = delete;
    ImplCLHT& operator=(ImplCLHT const&) = delete;
    ImplCLHT(ImplCLHT const&) = delete;
    ImplCLHT(ImplCLHT&&) = delete;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        myCLHT = clht_create(1 << bucketScale);
        assert(myCLHT);
        clht_gc_thread_init(myCLHT, 0);
        std::atomic_thread_fence(std::memory_order_release);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        assert(myCLHT);
        std::atomic_thread_fence(std::memory_order_acquire);
        clht_gc_thread_init(myCLHT, 1 + tid);
    }

    __attribute__((always_inline))
    void insert(size_t k, size_t v) {
        clht_put(myCLHT, k, v);
    }

    __attribute__((always_inline))
    bool get(size_t k, size_t& v) {
        v = clht_get(myCLHT->ht, k);
        return v != 0;
    }

    __attribute__((always_inline))
    void cleanup() {
        clht_gc_destroy(myCLHT);
    }

    __attribute__((always_inline))
    static std::string name() {
        return "CLHT";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        (void)out;
    }

private:
    clht_t* myCLHT;
};

template<typename K, typename V>
class ImplCLHTGeneric;

template<>
class ImplCLHTGeneric<size_t, size_t>: public ImplCLHT {};

size_t fromKeyToValue(size_t k) {
    return k;
}
#endif

template<typename K, typename V>
class ImplInsituU: public ImplMyAPI<insituUB::HashTable, K, V> {
public:

    ImplInsituU(): ImplMyAPI<insituUB::HashTable, K, V>("InsituU") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename insituUB::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplInsituUBquad: public ImplMyAPI<insituUBquad::HashTable, K, V> {
public:

    ImplInsituUBquad(): ImplMyAPI<insituUBquad::HashTable, K, V>("InsituUBquad") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename insituUBquad::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplInsituRevCasUB: public ImplMyAPI<insituRevCasUB::HashTable, K, V> {
public:

    ImplInsituRevCasUB(): ImplMyAPI<insituRevCasUB::HashTable, K, V>("InsituRevCasUB") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename insituRevCasUB::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplInsituRevCasUBquad: public ImplMyAPI<insituRevCasUBquad::HashTable, K, V> {
public:

    ImplInsituRevCasUBquad(): ImplMyAPI<insituRevCasUBquad::HashTable, K, V>("InsituRevCasUBquad") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename insituRevCasUBquad::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplInsituDCASUB: public ImplMyAPI<insituDCASUB::HashTable, K, V> {
public:

    ImplInsituDCASUB(): ImplMyAPI<insituDCASUB::HashTable, K, V>("InsituDCASUB") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename insituDCASUB::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplInsituDCASUBquad: public ImplMyAPI<insituDCASUBquad::HashTable, K, V> {
public:

    ImplInsituDCASUBquad(): ImplMyAPI<insituDCASUBquad::HashTable, K, V>("InsituDCASUBquad") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename insituDCASUBquad::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplInsitu32: public ImplMyAPI<insitu32::HashTable, K, V> {
public:

    ImplInsitu32(): ImplMyAPI<insitu32::HashTable, K, V>("Insitu32") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename insitu32::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename K, typename V>
class ImplInsituQ32: public ImplMyAPI<insituQ32::HashTable, K, V> {
public:

    ImplInsituQ32(): ImplMyAPI<insituQ32::HashTable, K, V>("InsituQ32") {}

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        typename insituQ32::HashTable<K,V>::stats stats;
        this->ht->getStats(stats);
        out << "size: " << stats.size
            << ", buckets: " << stats.usedBuckets
            << ", cols: " << stats.collisions
            << ", avg b. size: " << stats.avgBucketSize
            << ", bgst bucket: " << stats.biggestBucket
            ;
        out << std::endl;
        std::vector<size_t> elements;
        elements.reserve(bars);
        this->ht->getDensityStats(bars, elements);
        printDensitygraph(out, elements);
    }
};

template<typename TEST, typename IMPL>
struct thread_data {
    TEST* test;
    IMPL* impl;
    size_t tid;
    size_t inserts;
    double result_time;
    volatile int* phase;
    std::atomic<size_t>* tcount;
};

typedef void*(*pfunc)(void*);

struct cpuset {

    static cpuset createForNUMACPU(size_t cpu) {
        size_t numCPU = sysconf( _SC_NPROCESSORS_ONLN );
        cpu_set_t* _mask = CPU_ALLOC(numCPU);
        size_t _size = CPU_ALLOC_SIZE(numCPU);
        CPU_ZERO_S(_size, _mask);
        for(size_t t = cpu; t < numCPU; t+=4) {
            CPU_SET_S(t, _size, _mask);
        }
        return cpuset(_size, _mask);
    }

    static cpuset createForCPU(size_t cpu) {
        size_t numCPU = sysconf( _SC_NPROCESSORS_ONLN );
        cpu_set_t* _mask = CPU_ALLOC(numCPU);
        size_t _size = CPU_ALLOC_SIZE(numCPU);
        CPU_ZERO_S(_size, _mask);
        CPU_SET_S(cpu, _size, _mask);
        return cpuset(_size, _mask);
    }

    cpuset(): _size(0), _mask(nullptr) {}
    cpuset(size_t size, cpu_set_t* mask): _size(size), _mask(mask) {}

//    ~cpuset() {
//        if(_mask) {
//            CPU_FREE(_mask);
//        }
//    }

    size_t _size;
    cpu_set_t* _mask;
};


template<typename TEST, typename IMPL>
void* insertData(thread_data<TEST, IMPL>* data) {
    TEST& test = *data->test;
    IMPL& impl = *data->impl;
    size_t& tid = data->tid;

//    Timer t;
    for(size_t i=data->inserts; i--;) {
        auto&& k = test.key(tid, i);
        auto v = test.value(tid, i, k);
        impl.insert(k, v);
    }
//    data->result_time = t.getElapsedSeconds();
    return nullptr;
}

template<typename TEST, typename IMPL>
void* verifyData(thread_data<TEST, IMPL>* data) {
    TEST& test = *data->test;
    IMPL& impl = *data->impl;
    size_t& tid = data->tid;
    size_t wrong = data->inserts;

//    Timer t;
    for(size_t i=data->inserts; i--;) {
        auto&& k = test.key(tid, i);
        typename IMPL::value_type const&& v_correct = test.value(tid, i, k);
        typename IMPL::value_type v = {0};
        bool exists = impl.get(k, v);
        if(!exists || v != v_correct) {
            wrong = (wrong != data->inserts) ? wrong : i;
        }
    }
//    data->result_time = t.getElapsedSeconds();
    return (void*)wrong;
}

template<typename TEST, typename IMPL>
void* insertThread(thread_data<TEST, IMPL>* data) {
    IMPL& impl = *data->impl;
    size_t tid = data->tid;
    if constexpr(has_thread_init_method<IMPL>::value) {
        impl.thread_init(tid);
    }
    (*data->tcount)--;
    while(*data->phase < 1);
    return insertData(data);
}

template<typename TEST, typename IMPL>
void* verifyThread(thread_data<TEST, IMPL>* data) {
    IMPL& impl = *data->impl;
    size_t tid = data->tid;
    if constexpr(has_thread_init_method<IMPL>::value) {
        impl.thread_init(tid);
    }
    (*data->tcount)--;
    while(*data->phase < 1);
    return verifyData(data);
}

template<typename TEST, typename IMPL>
void setupThreadData(thread_data<TEST, IMPL>& td, TEST* test, IMPL* impl, size_t tid, size_t inserts, int* phase, std::atomic<size_t>* tcount) {
    td.test = test;
    td.impl = impl;
    td.tid = tid;
    td.inserts = inserts;
    td.result_time = 0.0;
    td.phase = phase;
    td.tcount = tcount;
}

//std::atomic<int> my_string::total_copies;
//std::atomic<int> my_string::total_moves;

template< template <typename> class TEST, typename IMPL>
int httestGen(TEST<IMPL>& test, IMPL& impl) {

//    my_string::total_copies = 0;
//    my_string::total_moves = 0;

    Settings& settings = Settings::global();

    size_t bucketScale = settings["buckets_scale"].asUnsignedValue();
    size_t threads = settings["threads"].asUnsignedValue();
    size_t inserts = settings["inserts"].asUnsignedValue();
    int phase = 0;
    std::atomic<size_t> tcount(threads);

    double dr = settings["duplicateratio"].as<double>();
    double cr = settings["collisionratio"].as<double>();

    if constexpr(!has_thread_init_method<IMPL>::value) {
        inserts *= threads;
        threads = 1;
    }

    Timer t;

    test.setup(bucketScale, threads, inserts, dr, cr);

    pthread_t workers[threads];
    pthread_attr_t attributes[threads];
    thread_data<TEST<IMPL>, IMPL> tdata[threads];

    cpuset set[192];
    for(int t = 0; t < 192; ++t) {
        set[t] = cpuset::createForCPU(t);
    }

    // Setup
    phase = 0;
    t.reset();
    impl.init(bucketScale);

    for(size_t tid=threads; tid--;) {
        setupThreadData(tdata[tid], &test, &impl, tid, inserts, &phase, &tcount);
        pthread_attr_init(&attributes[tid]);
        pthread_attr_setaffinity_np(&attributes[tid], set[tid]._size, set[tid]._mask);
        pthread_create(&workers[tid], &attributes[tid], (pfunc)insertThread<TEST<IMPL>, IMPL>, (void*)&tdata[tid]);
    }
    while(tcount > 0);
    auto elapsed_setup = t.getElapsedSeconds();

    // Insertion
    phase = 1;
    std::atomic_thread_fence(std::memory_order_seq_cst);
    t.reset();

    for(size_t tid=threads; tid--;) {
        size_t result = inserts;
        pthread_join(workers[tid], nullptr);
        pthread_attr_destroy(&attributes[tid]);
    }

    auto elapsed_insert = t.getElapsedSeconds();
    phase = 0;

    // Verify
    bool ok = true;
    typename TEST<IMPL>::key_type wrongKey;
    typename TEST<IMPL>::value_type expectedValue;
    typename IMPL::value_type wrongValue;
    bool didNotExist = false;

//    if(threads > 1) {
        for(size_t tid=threads; tid--;) {
            pthread_attr_init(&attributes[tid]);
            pthread_attr_setaffinity_np(&attributes[tid], set[tid]._size, set[tid]._mask);
            pthread_create(&workers[tid], &attributes[tid], (pfunc)verifyThread<TEST<IMPL>, IMPL>, (void*)&tdata[tid]);
        }

        phase = 1;
        std::atomic_thread_fence(std::memory_order_seq_cst);
        t.reset();

        for(size_t tid=threads; tid--;) {
            size_t result = inserts;
            pthread_join(workers[tid], (void**)&result);
            pthread_attr_destroy(&attributes[tid]);
            if(result != inserts && ok) {
                wrongKey = test.key(tid, (size_t)result);
                wrongValue = 0;
                expectedValue = test.value(tid, (size_t)result, wrongKey);
                didNotExist = !impl.get(wrongKey, wrongValue);
                ok = false;
            }
        }
//    } else {
//        size_t result = (intptr_t)checkThread<TEST<IMPL>, IMPL>(&tdata[0]);
//        if(result != inserts) {
//            wrongKey = test.key(0, (size_t)result);
//            expectedValue = test.value(0, (size_t)result, wrongKey);
//            didNotExist = !impl.get(wrongKey, wrongValue);
//            ok = false;
//        }
//    }
    auto elapsed_verify = t.getElapsedSeconds();
    phase = 0;

    // Stats
    std::stringstream ss;
    if(ok) {
        ss << " ok, ";
    } else {
        ss << " wrong(key" << wrongKey << "), value=";
        if(didNotExist) {
            ss << "<no value>";
        } else {
            ss << wrongValue;
        }
        ss << ", expected=" << expectedValue << ", hash=" << std::hash<decltype(wrongKey)>{}(wrongKey) << " ";
    }
    if(settings["stats"].isOn()) {
        impl.statsString(ss, settings["bars"].asUnsignedValue());
    }

    // Cleanup
    t.reset();
    impl.cleanup();
    auto elapsed_cleanup = t.getElapsedSeconds();

    // Print
    auto elapsed_total = elapsed_setup + elapsed_insert + elapsed_cleanup + elapsed_verify;
    fflush(stdout);
    fflush(stderr);
    std::cerr << std::fixed << std::setw( 25 ) << impl.name()
              << std::fixed << std::setw(  4 ) << settings["buckets_scale"].asString()
              << std::fixed << std::setw(  4 ) << settings["page_size_scale"].asString()
              << std::fixed << std::setw(  4 ) << threads
              << std::fixed << std::setw(  9 ) << inserts
              << std::fixed << std::setw(  7 ) << std::setprecision( 2 ) << elapsed_setup
              << std::fixed << std::setw(  7 ) << std::setprecision( 2 ) << elapsed_insert
              << std::fixed << std::setw(  8 ) << std::setprecision( 2 ) << "0.00"
              << std::fixed << std::setw(  7 ) << std::setprecision( 2 ) << elapsed_verify
              << std::fixed << std::setw(  8 ) << std::setprecision( 2 ) << "0.00"
              << std::fixed << std::setw(  7 ) << std::setprecision( 2 ) << elapsed_cleanup
              << std::fixed << std::setw(  7 ) << std::setprecision( 2 ) << elapsed_total
              << ss.str()
              << std::endl;
              ;

//    std::cout << "total copies: " << my_string::total_copies << ", total moves: " << my_string::total_moves << std::endl;
    fflush(stdout);
    fflush(stderr);

    return 0;

}

void runTest(std::string const& htName) {
//    typedef boost::mpl::vector<ImplChain, ImplCLHT, ImplCpp, ImplDivineHT, ImplMmap<size_t,size_t>, ImplCacheChain> vec;
//    boost::mpl::for_each<vec>(value_printer{htName});
    if(htName == "ChainInts:i") {
        ImplChainInts impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Chain:i") {
        ImplChain<size_t,size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainSlab:i") {
        ImplChainSlab<size_t,size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainU:i") {
        ImplChainGenericUB<size_t,size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainUV:i") {
        ImplChainGenericUBVK<size_t,size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainV:i") {
        ImplChainGenericV<size_t,size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
#ifdef HAVE_CLHT
    } else if(htName == "CLHT:i") {
        ImplCLHT impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
#endif
    } else if(htName == "ChainC7:i") {
        ImplCacheChain<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainC0:i") {
        ImplCacheChain2<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "CChain2C:i") {
        ImplCacheChain2Config<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainC:i") {
        ImplCacheChain3<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "CChain3AC:i") {
        ImplCacheChain3AC<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainCV:i") {
        ImplCacheChain3VK<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainCU:i") {
        ImplCacheChain3UB<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainCUV:i") {
        ImplCacheChain3UBVK<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChunkHT:i") {
        ImplChunkHT impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "dbsll:i") {
        ImplDBSLL<size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Mmap:i") {
        ImplMmap<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapU:i") {
        ImplMmapU<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapC:i") {
        ImplMmapCache<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQ:i") {
        ImplMmapQuad<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQC:i") {
        ImplMmapQuadC<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQCU:i") {
        ImplMmapQuadCU<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQCUV:i") {
        ImplMmapQuadCUV<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapMmap:i") {
        ImplMmapMmap<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "TBB:i") {
        ImplTBBHashMapDefaultAllocator<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
#ifdef HAVE_DIVINE
    } else if(htName == "DIVINE:i") {
        ImplDivineHT<size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
#endif
    } else if(htName == "Junction.Crude:i") {
        ImplJunctionCrude<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Junction.Linear:i") {
        ImplJunctionLinear<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Junction.LeapFrog:i") {
        ImplJunctionLeapFrog<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Junction.Grampa:i") {
        ImplJunctionGrampa<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Sylvan:i") {
        ImplSylvan<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "bytell:i") {
        ImplBytell<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "bytell_s:i") {
        ImplBytell_s<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "InsituU:i") {
        ImplInsituU<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "InsituQU:i") {
        ImplInsituUBquad<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "InsituRevCasU:i") {
        ImplInsituRevCasUB<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "InsituRevCasQU:i") {
        ImplInsituRevCasUBquad<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "InsituDU:i") {
        ImplInsituDCASUB<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "InsituQDU:i") {
        ImplInsituDCASUBquad<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Insitu32:i") {
        ImplInsitu32<__uint32_t, __uint32_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "InsituQ32:i") {
        ImplInsituQ32<__uint32_t, __uint32_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "libcuckoo.sa:i") {
        ImplLibCuckooSA<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "folklore:i") {
        ImplGrowtFolklore<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "uaGrow:i") {
        ImplGrowtUAGrow<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "usGrow:i") {
        ImplGrowtUSGrow<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "usnGrow:i") {
        ImplGrowtUSNGrow<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "paGrow:i") {
        ImplGrowtPAGrow<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "psGrow:i") {
        ImplGrowtPSGrow<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "psnGrow:i") {
        ImplGrowtPSNGrow<size_t, size_t> impl;
        TestInts::Test<decltype(impl)> test;
        httestGen(test, impl);

    } else if(htName == "ChainInts:j") {
        ImplChainInts impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Chain:j") {
        ImplChain<size_t,size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainSlab:j") {
        ImplChainSlab<size_t,size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainU:j") {
        ImplChainGenericUB<size_t,size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainUV:j") {
        ImplChainGenericUBVK<size_t,size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainV:j") {
        ImplChainGenericV<size_t,size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
#ifdef HAVE_CLHT
    } else if(htName == "CLHT:j") {
        ImplCLHT impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
#endif
    } else if(htName == "ChainC7:j") {
        ImplCacheChain<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainC0:j") {
        ImplCacheChain2<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "CChain2C:j") {
        ImplCacheChain2Config<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainC:j") {
        ImplCacheChain3<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "CChain3AC:j") {
        ImplCacheChain3AC<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainCV:j") {
        ImplCacheChain3VK<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainCU:j") {
        ImplCacheChain3UB<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainCUV:j") {
        ImplCacheChain3UBVK<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChunkHT:j") {
        ImplChunkHT impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "dbsll:j") {
        ImplDBSLL<size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Mmap:j") {
        ImplMmap<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapU:j") {
        ImplMmapU<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapC:j") {
        ImplMmapCache<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQ:j") {
        ImplMmapQuad<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQC:j") {
        ImplMmapQuadC<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQCU:j") {
        ImplMmapQuadCU<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQCUV:j") {
        ImplMmapQuadCUV<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapMmap:j") {
        ImplMmapMmap<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "TBB:j") {
        ImplTBBHashMapDefaultAllocator<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
#ifdef HAVE_DIVINE
    } else if(htName == "DIVINE:j") {
        ImplDivineHT<size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
#endif
    } else if(htName == "Junction.Crude:j") {
        ImplJunctionCrude<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Junction.Linear:j") {
        ImplJunctionLinear<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Junction.LeapFrog:j") {
        ImplJunctionLeapFrog<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Junction.Grampa:j") {
        ImplJunctionGrampa<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Sylvan:j") {
        ImplSylvan<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "bytell:j") {
        ImplBytell<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "bytell_s:j") {
        ImplBytell_s<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "InsituU:j") {
        ImplInsituU<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "InsituQU:j") {
        ImplInsituUBquad<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "InsituRevCasU:j") {
        ImplInsituRevCasUB<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "InsituRevCasQU:j") {
        ImplInsituRevCasUBquad<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "InsituDU:j") {
        ImplInsituDCASUB<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "InsituQDU:j") {
        ImplInsituDCASUBquad<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Insitu32:j") {
        ImplInsitu32<__uint32_t, __uint32_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "InsituQ32:j") {
        ImplInsituQ32<__uint32_t, __uint32_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "libcuckoo.sa:j") {
        ImplLibCuckooSA<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "folklore:j") {
        ImplGrowtFolklore<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "uaGrow:j") {
        ImplGrowtUAGrow<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "usGrow:j") {
        ImplGrowtUSGrow<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "usnGrow:j") {
        ImplGrowtUSNGrow<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "paGrow:j") {
        ImplGrowtPAGrow<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "psGrow:j") {
        ImplGrowtPSGrow<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "psnGrow:j") {
        ImplGrowtPSNGrow<size_t, size_t> impl;
        TestInts2::Test<decltype(impl)> test;
        httestGen(test, impl);

    } else if(htName == "Chain:s") {
        ImplChain<my_string,size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainSlab:s") {
        ImplChainSlab<my_string,size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainU:s") {
        ImplChainGenericUB<my_string,size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainUV:s") {
        ImplChainGenericUBVK<my_string,size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainV:s") {
        ImplChainGenericV<my_string,size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainC7:s") {
        ImplCacheChain<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainC0:s") {
        ImplCacheChain2<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "CChain2C:s") {
        ImplCacheChain2Config<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainC:s") {
        ImplCacheChain3<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "CChain3AC:s") {
        ImplCacheChain3AC<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainCV:s") {
        ImplCacheChain3VK<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainCU:s") {
        ImplCacheChain3UB<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainCUV:s") {
        ImplCacheChain3UBVK<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Mmap:s") {
        ImplMmap<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapU:s") {
        ImplMmapU<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapC:s") {
        ImplMmapCache<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQ:s") {
        ImplMmapQuad<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQC:s") {
        ImplMmapQuadC<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQCU:s") {
        ImplMmapQuadCU<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQCUV:s") {
        ImplMmapQuadCUV<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChunkHT:s") {
        ImplChunkHTGeneric<my_string> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "TBB:s") {
        ImplTBBHashMapDefaultAllocator<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "bytell:s") {
        ImplBytell<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "bytell_s:s") {
        ImplBytell_s<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "libcuckoo.sa:s") {
        ImplLibCuckooSA<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
//    } else if(htName == "Junction.s") {
//        ImplJunction<my_string, size_t, junction::ConcurrentMap_Leapfrog> impl;
//        TestStrings::Test<decltype(impl)> test;
//        httestGen(test, impl);

    } else if(htName == "Chain:w") {
        ImplChain<my_string,size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainSlab:w") {
        ImplChainSlab<my_string,size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
//        auto _words = WordData::getTestDataCache()[0]._words;
//        auto _wordsTotal = WordData::getTestDataCache()[0]._wordsTotal;
//        for(;_wordsTotal--;) {
//            size_t v;
//            if(!impl.get(_words[_wordsTotal], v)) {
//                std::cout << "does not have " << _words[_wordsTotal] << " -> " << v << std::endl;
//            }
//        }
    } else if(htName == "ChainU:w") {
        ImplChainGenericUB<my_string,size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainUV:w") {
        ImplChainGenericUBVK<my_string,size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainV:w") {
        ImplChainGenericV<my_string,size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainC7:w") {
        ImplCacheChain<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainC0:w") {
        ImplCacheChain2<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "CChain2C:w") {
        ImplCacheChain2Config<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainC:w") {
        ImplCacheChain3<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "CChain3AC:w") {
        ImplCacheChain3AC<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainCV:w") {
        ImplCacheChain3VK<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainCU:w") {
        ImplCacheChain3UB<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainCUV:w") {
        ImplCacheChain3UBVK<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Mmap:w") {
        ImplMmap<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapU:w") {
        ImplMmapU<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapC:w") {
        ImplMmapCache<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQ:w") {
        ImplMmapQuad<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQC:w") {
        ImplMmapQuadC<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQCU:w") {
        ImplMmapQuadCU<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQCUV:w") {
        ImplMmapQuadCUV<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "TBB:w") {
        ImplTBBHashMapDefaultAllocator<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "bytell:w") {
        ImplBytell<my_string, size_t> impl;
        TestWords1<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "bytell_s:w") {
        ImplBytell_s<my_string, size_t> impl;
        TestStrings::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "libcuckoo.sa:w") {
        ImplLibCuckooSA<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);

    } else if(htName == "Chain:v") {
        ImplChain<myvector,size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainSlab:v") {
        ImplChainSlab<myvector,size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainU:v") {
        ImplChainGenericUB<myvector,size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainUV:v") {
        ImplChainGenericUBVK<myvector,size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainV:v") {
        ImplChainGenericV<myvector,size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainC7:v") {
        ImplCacheChain<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainC0:v") {
        ImplCacheChain2<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "CChain2C:v") {
        ImplCacheChain2Config<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainC:v") {
        ImplCacheChain3<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "CChain3AC:v") {
        ImplCacheChain3AC<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainCV:v") {
        ImplCacheChain3VK<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainCU:v") {
        ImplCacheChain3UB<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "ChainCUV:v") {
        ImplCacheChain3UBVK<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
//    } else if(htName == "ChunkHT:v") {
//        ImplChunkHT impl;
//        TestVectors::Test<decltype(impl)> test;
//        httestGen(test, impl);
    } else if(htName == "dbsll:v") {
        ImplDBSLL<myvector> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "Mmap:v") {
        ImplMmap<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapU:v") {
        ImplMmapU<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapC:v") {
        ImplMmapCache<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQ:v") {
        ImplMmapQuad<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQC:v") {
        ImplMmapQuadC<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQCU:v") {
        ImplMmapQuadCU<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapQCUV:v") {
        ImplMmapQuadCUV<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "OpenAddr:v") {
        ImplOpenAddr<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "MmapMmap:v") {
        ImplMmapMmap<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "TBB:v") {
        ImplTBBHashMapDefaultAllocator<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "TBBu:v") {
        ImplTBBUnorderedMapDefaultAllocator<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "TBB:sa:v") {
        ImplTBBHashMapScalableAllocator<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "TBBu:sa:v") {
        ImplTBBUnorderedMapScalableAllocator<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
#ifdef HAVE_DIVINE
    } else if(htName == "DIVINE:v") {
        ImplDivineHT<myvector> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
#endif
    } else if(htName == "libcuckoo.sa:v") {
        ImplLibCuckooSA<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
//    } else if(htName == "Sylvan:v") {
//        ImplSylvan<myvector, size_t> impl;
//        TestVectors::Test<decltype(impl)> test;
//        httestGen(test, impl);
//    } else if(htName == "Junction.Crude:v") {
//        ImplJunctionCrude<myvector, size_t> impl;
//        TestVectors::Test<decltype(impl)> test;
//        httestGen(test, impl);
//    } else if(htName == "Junction.Linear:v") {
//        ImplJunctionLinear<myvector, size_t> impl;
//        TestVectors::Test<decltype(impl)> test;
//        httestGen(test, impl);
//    } else if(htName == "Junction.LeapFrog:v") {
//        ImplJunctionLeapFrog<myvector, size_t> impl;
//        TestVectors::Test<decltype(impl)> test;
//        httestGen(test, impl);
//    } else if(htName == "Junction.Grampa:v") {
//        ImplJunctionGrampa<myvector, size_t> impl;
//        TestVectors::Test<decltype(impl)> test;
//        httestGen(test, impl);
    } else if(htName == "bytell:v") {
        ImplBytell<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);
    } else if(htName == "bytell_s:v") {
        ImplBytell_s<myvector, size_t> impl;
        TestVectors::Test<decltype(impl)> test;
        httestGen(test, impl);

    } else if(!htName.empty()) {
        printf("Unknown hash map: %s\n", htName.c_str());
    } else {
        printf("No hash table selected\n");
    }
}

int main(int argc, char** argv) {

    Settings& settings = Settings::global();

    settings["threads"] = 32;
    settings["duplicateratio"] = 0.0;
    settings["collisionratio"] = 1.0;
    settings["inserts"] = 100000;
    settings["buckets_scale"] = 28;
    settings["page_size_scale"] = 28;
    settings["stats"] = 0;
    settings["bars"] = 128;

    std::cout << "\033[1m"
              << std::fixed << std::setw( 25 ) << "name"
              << std::fixed << std::setw(  4 ) << "scl"
              << std::fixed << std::setw(  4 ) << "pss"
              << std::fixed << std::setw(  4 ) << "ths"
              << std::fixed << std::setw(  9 ) << "inserts"
              << std::fixed << std::setw(  7 ) << "setup"
              << std::fixed << std::setw(  7 ) << "insert"
              << std::fixed << std::setw(  8 ) << "inssum"
              << std::fixed << std::setw(  7 ) << "verify"
              << std::fixed << std::setw(  8 ) << "vfysum"
              << std::fixed << std::setw(  7 ) << "clean"
              << std::fixed << std::setw(  7 ) << "total"
              << std::endl
              << "\033[0m"
              ;

    struct option long_options[] =
    {
//        {"stats",   no_argument, &STATS, 1},
        {0, 0, 0, 0}
    };
    int option_index = 0;

    int c = 0;
    while ((c = getopt(argc, argv, "i:c:d:s:t:T:p:-:")) != -1) {
//    while ((c = getopt_long(argc, argv, "i:d:s:t:T:p:-:", long_options, &option_index)) != -1) {
        switch(c) {
            case 't':
                if(optarg) {
                    settings["threads"] = std::stoi(optarg);
                }
                break;
            case 'd':
                if(optarg) {
                    settings["duplicateratio"] = std::string(optarg);
                }
                break;
            case 'c':
                if(optarg) {
                    settings["collisionratio"] = std::string(optarg);
                }
                break;
            case 'T':
                if(optarg) {
                    settings["test"] = std::string(optarg);
                }
                break;
            case 'i':
                settings["inserts"] = std::stoi(optarg);
                break;
            case 's':
                settings["buckets_scale"] = std::stoi(optarg);
                break;
            case 'p':
                settings["page_size_scale"] = std::stoi(optarg);
                break;
            case '-':
                settings.insertKeyValue(optarg);
        }
    }

    int htindex = optind;
    while(argv[htindex]) {
        runTest(std::string(argv[htindex]) + ":" + settings["test"].asString());
        htindex++;
    }

}

//TLS<slab> SlabManager::_slab;
__thread slab* SlabManager::_slab;
