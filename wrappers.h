#include <type_traits>

#include "junction/ConcurrentMap_Crude.h"
#include "junction/ConcurrentMap_Linear.h"
#include "junction/ConcurrentMap_Leapfrog.h"
#include "junction/ConcurrentMap_Grampa.h"

#if HM_USE_VENDOR_TBB
#include "tbb/include/tbb/concurrent_unordered_map.h"
#include "tbb/include/tbb/concurrent_hash_map.h"
#include "tbb/include/tbb/scalable_allocator.h"
#endif

#include "SYLVAN_TABLE.h"
#include "bytell_hash_map.hpp"

#include "cuckoohash_map.hh"

#include "hopscotch/fine_grained_improved/hopscotch.hpp"

#if HM_USE_VENDOR_LIBCDS
#include "cds/init.h"
#include <cds/gc/dhp.h>
#include "cds/details/make_const_type.h"
#include "cds/container/michael_map.h"
#include "cds/container/michael_map_nogc.h"
#include "cds/container/michael_list_nogc.h"
#include "cds/container/michael_kvlist_dhp.h"
#include "cds/container/michael_kvlist_nogc.h"
#include "cds/container/lazy_kvlist_nogc.h"
#include "cds/container/skip_list_map_nogc.h"
#include "cds/container/split_list_map_nogc.h"
#endif

#if HM_USE_VENDOR_FOLLY
#include "folly/AtomicHashMap.h"
#endif

#include "sparsehash/sparse_hash_map"
#include "sparsehash/dense_hash_map"

//#include "Honey/Thread/LockFree/UnorderedMap.h"

#include "mystring.h"

extern "C" {
#include "dbs-ll.h"
}

#include "data-structures/definitions.h"

//template<typename T>
//struct has_thread_init_method {
//private:
//    typedef std::true_type yes;
//    typedef std::false_type no;
//
//    template<typename U> static auto test(int) -> decltype(std::declval<U>().thread_init() == 1, yes());
//
//    template<typename> static no test(...);
//
//public:
//    static constexpr bool value = std::is_same<decltype(test<T>(0)),yes>::value;
//};

#if HM_USE_VENDOR_TBB
using namespace tbb;

namespace tbb {
template<>
struct tbb_hash_compare<my_string> {

    __attribute__((always_inline))
    bool equal( const my_string& j, const my_string& k ) const {
        return j == k;
    }

    __attribute__((always_inline))
    size_t hash( const my_string& k ) const {
        return k.hash();
    }
};

template<>
class tbb_hash<my_string> {
public:
    tbb_hash() {}

    size_t operator()(const my_string& k) const
    {
        return k.hash();
    }
};

template<>
struct tbb_hash_compare<myvector> {

    __attribute__((always_inline))
    bool equal( const myvector& j, const myvector& k ) const {
        return j == k;
    }

    __attribute__((always_inline))
    size_t hash( const myvector& k ) const {
        return k.hash();
    }
};

template<>
class tbb_hash<myvector> {
public:
    tbb_hash() {}

    size_t operator()(const myvector& k) const
    {
        return k.hash();
    }
};

template<>
struct tbb_hash_compare<size_t> {

    __attribute__((always_inline))
    bool equal( const size_t& j, const size_t& k ) const {
        return j == k;
    }

    __attribute__((always_inline))
    size_t hash( const size_t& k ) const {
        return k;
    }
};

template<>
class tbb_hash<size_t> {
public:
    tbb_hash() {}

    size_t operator()(const size_t& k) const
    {
        return k;
    }
};

}

template<typename K, typename V, typename TBBMAP>
class ImplTBBHash {
public:
    using key_type = K;
    using value_type = V;
    using map_type = TBBMAP;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        ht = new map_type(1 << bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        (void)tid;
        //ht->thread_init();
    }

    __attribute__((always_inline))
    void insert(K const& k, V const& v) {
        ht->insert(std::make_pair(k, v));
    }

    __attribute__((always_inline))
    bool get(K const& k, V& v) {
        typename map_type::const_accessor acc;
        bool b = ht->find(acc, k);
        if(b) {
            v = acc->second;
        }
        return b;
    }

    __attribute__((always_inline))
    void cleanup() {
        delete ht;
    }

    __attribute__((always_inline))
    std::string name() const {
        return "TBB" + std::string(typeid(K).name());
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        out << "size: " << ht->size()
            << ", buckets: " << ht->bucket_count();
            ;
    }
private:
    map_type* ht;
};

template<typename K, typename V, typename TBBMAP>
class ImplTBBUnordered {
public:
    using key_type = K;
    using value_type = V;
    using map_type = TBBMAP;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        ht = new map_type(1 << bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        (void)tid;
        //ht->thread_init();
    }

    __attribute__((always_inline))
    void insert(K const& k, V const& v) {
        ht->insert(std::make_pair(k, v));
    }

    __attribute__((always_inline))
    bool get(K const& k, V& v) {
        typename map_type::const_iterator acc = ht->find(k);
        if(acc != ht->end()) {
            v = acc->second;
            return true;
        }
        return false;
    }

    __attribute__((always_inline))
    void cleanup() {
        delete ht;
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        out << "size: " << ht->size()
            ;
    }
private:
    map_type* ht;
};

template<typename K, typename V>
class ImplTBBHashMapDefaultAllocator: public ImplTBBHash<K, V, tbb::concurrent_hash_map<K,V, tbb_hash_compare<K>, tbb::tbb_allocator<std::pair<K,V> > > > {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "TBB" + std::string(typeid(K).name());
    }
};

template<typename K, typename V>
class ImplTBBUnorderedMapDefaultAllocator: public ImplTBBUnordered<K, V, tbb::concurrent_unordered_map<K,V, tbb_hash<K>, std::equal_to<K>, tbb::tbb_allocator<std::pair<K,V> > > > {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "TBBu" + std::string(typeid(K).name());
    }
};

template<typename K, typename V>
class ImplTBBHashMapScalableAllocator: public ImplTBBHash<K, V, tbb::concurrent_hash_map<K,V, tbb_hash_compare<K>, tbb::scalable_allocator<std::pair<K,V> > > > {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "TBB:sa" + std::string(typeid(K).name());
    }
};

template<typename K, typename V>
class ImplTBBUnorderedMapScalableAllocator: public ImplTBBUnordered<K, V, tbb::concurrent_unordered_map<K,V, tbb_hash<K>, std::equal_to<K>, tbb::scalable_allocator<std::pair<K,V> > > > {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "TBBu:sa" + std::string(typeid(K).name());
    }
};

template<typename K, typename V>
class ImplTBBHashMapMyAllocator: public ImplTBBHash<K, V, tbb::concurrent_hash_map<K,V, tbb_hash_compare<K>, SlabPerThreadAllocator<std::pair<K,V> > > > {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "TBB:ma" + std::string(typeid(K).name());
    }
};

#endif

template<typename K, typename V, typename HT>
class ImplJunction {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        ht = new HT(1 << bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        (void)tid;
        //ht->thread_init();
    }

    __attribute__((always_inline))
    void insert(K const& k, V const& v) {
        ht->assign(k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, V& v) {
        v = ht->get(k);
        return v != typename HT::Value(HT::ValueTraits::NullValue);
    }

    __attribute__((always_inline))
    void cleanup() {
        delete ht;
    }

    __attribute__((always_inline))
    std::string name() const {
        return "Junction" + std::string(typeid(K).name());
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
    }

private:
    HT* ht;
};

template <class T>
struct JunctionKeyTraits {
    typedef T Key;
    typedef typename turf::util::BestFit<T>::Unsigned Hash;
    static const Key NullKey = Key(0);
    static const Hash NullHash = Hash(0);
    static Hash hash(T key) {
        return MurmurHash64(key);
    }
    static Key dehash(Hash hash) {
        return (T) turf::util::deavalanche(hash);
    }
};

template<typename K, typename V>
class ImplJunctionCrude: public ImplJunction<K, V, junction::ConcurrentMap_Crude<K,V>> {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "Junction.Crude<" + std::string(typeid(K).name()) + ">";
    }
};

template<typename K, typename V>
class ImplJunctionCrudeMurmur: public ImplJunction<K, V, junction::ConcurrentMap_Crude<K,V, JunctionKeyTraits<K>>> {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "Junction.Crude.Murmur<" + std::string(typeid(K).name()) + ">";
    }
};

template<typename K, typename V>
class ImplJunctionLinear: public ImplJunction<K, V, junction::ConcurrentMap_Linear<K,V>> {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "Junction.Linear<" + std::string(typeid(K).name()) + ">";
    }
};

template<typename K, typename V>
class ImplJunctionLeapFrog: public ImplJunction<K, V, junction::ConcurrentMap_Leapfrog<K,V>> {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "JunctionLeapFrog<" + std::string(typeid(K).name()) + ">";
    }
};

template<typename K, typename V>
class ImplJunctionGrampa: public ImplJunction<K, V, junction::ConcurrentMap_Grampa<K,V>> {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "Junction.Grampa<" + std::string(typeid(K).name()) + ">";
    }
};

template<typename K, typename V>
class ImplSylvan {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        auto size = 1ULL << bucketScale;
        ht = llmsset_create(size, size);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        llmsset_thread_init(ht, tid);
    }

    __attribute__((always_inline))
    void insert(K const& k, V const& v) {
        int created;
        llmsset_lookup(ht, k, 0, &created);
    }

    __attribute__((always_inline))
    bool get(K const& k, V& v) {
        int created;
        v = llmsset_lookup(ht, k, 0, &created);
        return created == 0;
    }

    __attribute__((always_inline))
    void cleanup() {
        llmsset_free(ht);
    }

    __attribute__((always_inline))
    std::string name() const {
        return "Sylvan" + std::string(typeid(K).name());
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        out << "size: " << llmsset_get_size(ht)
            ;
    }

private:
    llmsset_t ht;
};

template<typename K, typename V>
class ImplBytell {
public:

    using key_type = K;
    using value_type = V;
    using map_type = ska::bytell_hash_map<K, V, std::hash<K>, std::equal_to<K>, slaballocator< std::pair<K, V> > >;

    using mutex_type = std::shared_timed_mutex;
    using read_only_lock  = std::shared_lock<mutex_type>;
    using updatable_lock = std::unique_lock<mutex_type>;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        map = new map_type();
        map->reserve(1 << bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
    }

    __attribute__((always_inline))
    void insert(K const& k, V const& v) {
        updatable_lock lock(mtx);
        (*map)[k] = v;
//        printf("put %zu = %zu\n", k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, V& v) {
        read_only_lock lock(mtx);
        auto it = map->find(k);
        if(it != map->end()) {
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
        return "bytell";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        out << "size: " << map->size()
            ;
    }

private:
    map_type* map;
    mutex_type mtx;
};

template<typename K, typename V>
class ImplBytell_s {
public:

    using key_type = K;
    using value_type = V;
    using map_type = ska::bytell_hash_map<K, V, std::hash<K>, std::equal_to<K>, slaballocator< std::pair<K, V> > >;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        map = new map_type();
        map->reserve(1 << bucketScale);
    }

    __attribute__((always_inline))
    void insert(K const& k, V const& v) {
        (*map)[k] = v;
//        printf("put %zu = %zu\n", k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, V& v) {
        auto it = map->find(k);
        if(it != map->end()) {
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
        delete map;
    }

    __attribute__((always_inline))
    std::string name() const {
        return "bytell_s";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        out << "size: " << map->size()
            ;
    }

private:
    map_type* map;
};

template<typename T>
size_t DBSLL_hash_old(const char *key, int len, unsigned int seed) {
    return MurmurHash64(key, len, seed);
}

template<typename T>
size_t DBSLL_hash(const char *key, int len, unsigned int seed) {
    return MurmurHash64(key, len, seed);
}

template<>
size_t DBSLL_hash<size_t>(const char *key, int len, unsigned int seed) {
    return *(size_t*)key;
}

template<typename K>
class ImplDBSLL {
public:

    using key_type = K;
    using value_type = size_t;
    using map_type = dbs_ll_t;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        // Size of key is in number of ints
        map = DBSLLcreate_sized(sizeof(K)/sizeof(int), bucketScale, DBSLL_hash<K>, 0);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
    }

    __attribute__((always_inline))
    void insert(K const& k, value_type const& v) {
        size_t v_temp;
        DBSLLfop_hash(map, (const int*)&k, &v_temp, nullptr, 1);
    }

    __attribute__((always_inline))
    bool get(K const& k, value_type& v) {
        auto r = DBSLLfop_hash(map, (const int*)&k, &v, nullptr, 0);
        return r == 1;
    }

    __attribute__((always_inline))
    void cleanup() {
        DBSLLfree(map);
    }

    __attribute__((always_inline))
    std::string name() const {
        return "dbsll";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        stats_t* stats = DBSLLstats(map);
        out << "misses: " << stats->misses
            ;
    }

    bool isActuallySet() {
        return true;
    }

    bool supportsOvercommit() {
        return false;
    }

private:
    map_type map;
};

template<typename K>
class ImplDBSLLOld {
public:

    using key_type = K;
    using value_type = size_t;
    using map_type = dbs_ll_t;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        // Size of key is in number of ints
        map = DBSLLcreate_sized(sizeof(K)/sizeof(int), bucketScale, DBSLL_hash_old<K>, 0);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
    }

    __attribute__((always_inline))
    void insert(K const& k, value_type const& v) {
        size_t v_temp;
        DBSLLfop_hash(map, (const int*)&k, &v_temp, nullptr, 1);
    }

    __attribute__((always_inline))
    bool get(K const& k, value_type& v) {
        auto r = DBSLLfop_hash(map, (const int*)&k, &v, nullptr, 0);
        return r == 1;
    }

    __attribute__((always_inline))
    void cleanup() {
        DBSLLfree(map);
    }

    __attribute__((always_inline))
    std::string name() const {
        return "dbsll";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        stats_t* stats = DBSLLstats(map);
        out << "misses: " << stats->misses
            ;
    }

    bool isActuallySet() {
        return true;
    }

    bool supportsOvercommit() {
        return false;
    }

private:
    map_type map;
};

#if HM_USE_VENDOR_TBB
template<typename K, typename V>
class ImplLibCuckooSA {
public:

    using key_type = K;
    using value_type = size_t;
    using map_type = cuckoohash_map<K, V, std::hash<K>, std::equal_to<K>, tbb::scalable_allocator<std::pair<K, V>>>;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        // Size of key is in number of ints
        map = new map_type(1 << bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
    }

    __attribute__((always_inline))
    void insert(K const& k, value_type const& v) {
        map->insert_or_assign(k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, value_type& v) {
        return map->find(k, v);
    }

    __attribute__((always_inline))
    void cleanup() {
        delete map;
    }

    __attribute__((always_inline))
    std::string name() const {
        return "libcuckoo.sa";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        out << "size: " << map->size()
            ;
    }

private:
    map_type* map;
};
#endif

template<typename K, typename V>
class ImplLibCuckooMA {
public:

    using key_type = K;
    using value_type = size_t;
    using map_type = cuckoohash_map<K, V, std::hash<K>, std::equal_to<K>, SlabPerThreadAllocator<std::pair<K, V>>>;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        // Size of key is in number of ints
        map = new map_type(1 << bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
    }

    __attribute__((always_inline))
    void insert(K const& k, value_type const& v) {
        map->insert_or_assign(k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, value_type& v) {
        return map->find(k, v);
    }

    __attribute__((always_inline))
    void cleanup() {
        delete map;
    }

    __attribute__((always_inline))
    std::string name() const {
        return "libcuckoo.ma";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        out << "size: " << map->size()
            ;
    }

private:
    map_type* map;
};

template<typename K, typename V, typename map_type>
class ImplGrowt {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        // Size of key is in number of ints
        map = new map_type(1 << bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        handle = new typename map_type::Handle(std::move(map->getHandle()));
    }

    __attribute__((always_inline))
    void insert(K const& k, value_type const& v) {
        handle->insert_or_assign(k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, value_type& v) {
        auto it = handle->find(k);
        if(it != handle->end()) {
            v = (*it).second;
            return true;
        }
        return false;
    }

    __attribute__((always_inline))
    void cleanup() {
        delete map;
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
    }

private:
    map_type* map;
    TLS<typename map_type::Handle> handle;
};

template<typename K, typename V>
class ImplGrowtFolklore {
public:

    using key_type = K;
    using value_type = V;
    using map_type = growt::folklore<murmur_hasher>;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        // Size of key is in number of ints
        map = new map_type(1 << bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
    }

    __attribute__((always_inline))
    void insert(K const& k, value_type const& v) {
        map->insert_or_assign(k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, value_type& v) {
        auto it = map->find(k);
        if(it != map->end()) {
            v = (*it).second;
            return true;
        }
        return false;
    }

    __attribute__((always_inline))
    void cleanup() {
        delete map;
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
    }

    __attribute__((always_inline))
    std::string name() const {
        return "folklore";
    }
private:
    map_type* map;
};

//template<typename K, typename V>
//class ImplGrowtNoGrow: public ImplGrowt<K, V, growt::NoGrow<>> {
//public:
//    __attribute__((always_inline))
//    std::string name() const {
//        return "growt.nogrow";
//    }
//};

template<typename K, typename V>
class ImplGrowtUAGrow: public ImplGrowt<K, V, growt::uaGrow<murmur_hasher>> {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "uaGrow";
    }
};

template<typename K, typename V>
class ImplGrowtUSGrow: public ImplGrowt<K, V, growt::usGrow<murmur_hasher>> {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "usGrow";
    }
};

template<typename K, typename V>
class ImplGrowtUSNGrow: public ImplGrowt<K, V, growt::usnGrow<murmur_hasher>> {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "usnGrow";
    }
};

template<typename K, typename V>
class ImplGrowtPAGrow: public ImplGrowt<K, V, growt::paGrow<murmur_hasher>> {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "paGrow";
    }
};

template<typename K, typename V>
class ImplGrowtPSGrow: public ImplGrowt<K, V, growt::psGrow<murmur_hasher>> {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "psGrow";
    }
};

template<typename K, typename V>
class ImplGrowtPSNGrow: public ImplGrowt<K, V, growt::psnGrow<murmur_hasher>> {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "psnGrow";
    }
};

template<typename Key>
struct itemcmp;

template<>
struct itemcmp<size_t> {
    int operator ()(size_t const& k1, size_t const& k2) const
    {
        if ( k1 < k2)
            return -1;
        return k2 < k1 ? 1 : 0;
    }
};

namespace std {

template<>
struct less<std::pair<size_t, size_t>> {
    std::size_t operator()(std::pair<size_t, size_t> const& k1, std::pair<size_t, size_t> const& k2) const {
        return k1 < k2;
    }
};

}

#if HM_USE_VENDOR_LIBCDS
struct traits_MichaelList_cmp :
    public cds::container::michael_list::make_traits<
        cds::opt::compare< itemcmp<size_t> >
    >::type
{};

#if HM_USE_VENDOR_TBB
struct traits_MichaelList_cmp_ma :
    public cds::container::michael_list::make_traits
    < cds::opt::allocator< StatelessSlabPerThreadAllocator<std::pair<size_t,size_t>> >
    , cds::opt::compare< itemcmp<size_t> >
    >::type
{};
#endif

struct traits_MichaelList_cmp_sa :
    public cds::container::michael_list::make_traits
    < cds::opt::allocator< tbb::scalable_allocator<std::pair<size_t,size_t> > >
    , cds::opt::compare< itemcmp<size_t> >
    >::type
{};

struct traits_MichaelMap_cmp_ma :
    public cds::container::michael_map::make_traits
    < cds::opt::allocator< StatelessSlabPerThreadAllocator<std::pair<size_t,size_t>> >
    , cds::opt::compare< itemcmp<size_t> >
    >::type
{};

struct traits_MichaelMap_hash :
    public cds::container::michael_map::make_traits
    < cds::opt::hash< std::hash<size_t> >
    , cds::opt::item_counter< cds::atomicity::empty_item_counter >
    >::type
{};

class traits_SkipListMap_less_turbo32: public cds::container::skip_list::make_traits
    < cds::opt::less< typename std::less<size_t> >
    , cds::opt::hash< std::hash<size_t> >
    , cds::opt::item_counter< cds::atomicity::empty_item_counter >
    >::type
{};

struct traits_SplitList_Michael_st_cmp: public cds::container::split_list::make_traits
        < cds::container::split_list::ordered_list<cds::container::michael_list_tag>
        , cds::container::split_list::dynamic_bucket_table< false >
        , cds::opt::hash< hash<size_t> >
        , cds::container::split_list::ordered_list_traits
          < typename cds::container::michael_list::make_traits
            < cds::opt::compare< itemcmp<size_t> >
            >::type
          >
        >::type
{};

struct traits_SplitList_Michael_st_cmp_ma: public cds::container::split_list::make_traits
        < cds::container::split_list::ordered_list<cds::container::michael_list_tag>
        , cds::container::split_list::dynamic_bucket_table< false >
        , cds::opt::hash< hash<size_t> >
        , cds::container::split_list::ordered_list_traits
          < typename cds::container::michael_list::make_traits
            < cds::opt::compare< itemcmp<size_t> >
            , cds::opt::allocator< StatelessSlabPerThreadAllocator<size_t> >
            >::type
          >
        , cds::opt::allocator< StatelessSlabPerThreadAllocator<size_t> >
        >::type
{};

template<typename K, typename V, typename map_type>
class ImplCDS {
public:

    using key_type = K;
    using value_type = V;

    template<typename... Args>
    __attribute__((always_inline))
    void initPrivate(Args... args) {
        cds::Initialize();
        if(dhp == nullptr) {
            dhp = new cds::gc::DHP();
        }
        cds::threading::Manager::attachThread() ;
        // Size of key is in number of ints
        map = new map_type(args...);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
        cds::threading::Manager::attachThread();
    }

    __attribute__((always_inline))
    void insert(K const& k, value_type const& v) {
        map->insert(k, v);
    }

    __attribute__((always_inline))
    void cleanup() {
        delete map;
        cds::Terminate();
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
    }

protected:
    static cds::gc::DHP* dhp;
    map_type* map;
};
template<typename K, typename V, typename map_type>
cds::gc::DHP* ImplCDS<K, V, map_type>::dhp;

template<typename K, typename V>
class ImplCDSMichaelMLNOGC: public ImplCDS<K, V, cds::container::MichaelHashMap<cds::gc::nogc, cds::container::MichaelKVList< cds::gc::nogc, K, V, traits_MichaelList_cmp >, traits_MichaelMap_hash>> {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        this->initPrivate(1 << bucketScale, 1);
    }

    __attribute__((always_inline))
    std::string name() const {
        return "cdsmichaelML";
    }

    __attribute__((always_inline))
    bool get(K const& k, value_type& v) {
        auto it = this->map->contains(k);
        if(it != this->map->end()) {
            v = it->second;
            return true;
        }
        return false;
    }
};

template<typename K, typename V>
class ImplCDSMichaelMLNOGCMA: public ImplCDS<K, V, cds::container::MichaelHashMap<cds::gc::nogc, cds::container::MichaelKVList< cds::gc::nogc, K, V, traits_MichaelList_cmp_ma >, traits_MichaelMap_hash>> {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        this->initPrivate(1 << bucketScale, 1);
    }

    __attribute__((always_inline))
    std::string name() const {
        return "cdsmichaelMLMA";
    }

    __attribute__((always_inline))
    bool get(K const& k, value_type& v) {
        auto it = this->map->contains(k);
        if(it != this->map->end()) {
            v = it->second;
            return true;
        }
        return false;
    }
};

#if HM_USE_VENDOR_TBB
template<typename K, typename V>
class ImplCDSMichaelMLNOGCSA: public ImplCDS<K, V, cds::container::MichaelHashMap<cds::gc::nogc, cds::container::MichaelKVList< cds::gc::nogc, K, V, traits_MichaelList_cmp_sa >, traits_MichaelMap_hash>> {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        this->initPrivate(1 << bucketScale, 1);
    }

    __attribute__((always_inline))
    std::string name() const {
        return "cdsmichaelMLMA";
    }

    __attribute__((always_inline))
    bool get(K const& k, value_type& v) {
        auto it = this->map->contains(k);
        if(it != this->map->end()) {
            v = it->second;
            return true;
        }
        return false;
    }
};
#endif

template<typename K, typename V>
class ImplCDSMichaelMLDHP: public ImplCDS<K, V, cds::container::MichaelHashMap<cds::gc::DHP, cds::container::MichaelKVList< cds::gc::DHP, K, V, traits_MichaelList_cmp >, traits_MichaelMap_hash>> {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        this->initPrivate(1 << bucketScale, 1);
    }

    __attribute__((always_inline))
    std::string name() const {
        return "cdsmichaelDHP";
    }

    __attribute__((always_inline))
    bool get(K const& k, value_type& v) {
        bool b = false;
        this->map->find(k, [&v, &b](std::pair<key_type,value_type> const& item){
            v = item.second;
            b = true;
        });
        return b;
    }
};

template<typename K, typename V>//traits_MichaelList_cmp
class ImplCDSSkipList: public ImplCDS<K, V, cds::container::SkipListMap<cds::gc::nogc, K, V, traits_SkipListMap_less_turbo32>> {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        this->initPrivate();
    }

    __attribute__((always_inline))
    std::string name() const {
        return "cdsskiplist";
    }

    __attribute__((always_inline))
    bool get(K const& k, value_type& v) {
        auto it = this->map->contains(k);
        if(it != this->map->end()) {
            v = it->second;
            return true;
        }
        return false;
    }
};

template<typename K, typename V>//traits_MichaelList_cmp
class ImplCDSSplitList: public ImplCDS<K, V, cds::container::SplitListMap<cds::gc::nogc, K, V, traits_SplitList_Michael_st_cmp>> {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        this->initPrivate(1 << bucketScale, 1);
    }

    __attribute__((always_inline))
    std::string name() const {
        return "cdsskiplist";
    }

    __attribute__((always_inline))
    bool get(K const& k, value_type& v) {
        auto it = this->map->contains(k);
        if(it != this->map->end()) {
            v = it->second;
            return true;
        }
        return false;
    }
};

template<typename K, typename V>//traits_MichaelList_cmp
class ImplCDSSplitListMA: public ImplCDS<K, V, cds::container::SplitListMap<cds::gc::nogc, K, V, traits_SplitList_Michael_st_cmp_ma>> {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        this->initPrivate(1 << bucketScale, 1);
    }

    __attribute__((always_inline))
    std::string name() const {
        return "cdsskiplistMA";
    }

    __attribute__((always_inline))
    bool get(K const& k, value_type& v) {
        auto it = this->map->contains(k);
        if(it != this->map->end()) {
            v = it->second;
            return true;
        }
        return false;
    }
};

template<typename K, typename V>
class ImplCDSMichaelLLNOGC: public ImplCDS<K, V, cds::container::MichaelHashMap<cds::gc::nogc, cds::container::LazyKVList< cds::gc::nogc, K, V, traits_MichaelList_cmp >>> {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        this->initPrivate(1 << bucketScale, 1);
    }

    __attribute__((always_inline))
    std::string name() const {
        return "cdsmichaelLL";
    }

    __attribute__((always_inline))
    bool get(K const& k, value_type& v) {
        auto it = this->map->contains(k);
        if(it != this->map->end()) {
            v = it->second;
            return true;
        }
        return false;
    }
};
#endif

#if HM_USE_VENDOR_FOLLY
template<typename K, typename V, typename M>
class ImplFollyBase {
public:

    using key_type = K;
    using value_type = V;
    using map_type = M;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        // Size of key is in number of ints
        map = new map_type(1 << bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
    }

    __attribute__((always_inline))
    void insert(K const& k, value_type const& v) {
        map->emplace(k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, value_type& v) {
        auto it = map->find(k);
        if(it == map->end()) {
            return false;
        } else {
            v = it->second;
            return true;
        }
    }

    __attribute__((always_inline))
    void cleanup() {
        delete map;
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
    }

    __attribute__((always_inline))
    std::string name() const {
        return "folly";
    }
private:
    map_type* map;
};

template<typename K, typename V>
class ImplFolly: public ImplFollyBase<K, V, folly::AtomicHashMap<K, V>> {
public:
};

template<typename K, typename V>
class ImplFollyQ: public ImplFollyBase<K, V, folly::AtomicHashMap<K, V, typename std::hash<K>, typename std::equal_to<K>, std::allocator<char>, folly::AtomicHashArrayQuadraticProbeFcn>> {
public:
};

template<typename K, typename V>
class ImplFollyQMA: public ImplFollyBase<K, V, folly::AtomicHashMap<K, V, typename std::hash<K>, typename std::equal_to<K>, StatelessSlabPerThreadAllocator<char>, folly::AtomicHashArrayQuadraticProbeFcn>> {
public:
};
#endif

template<typename K, typename V>
class ImplGoogleSparseHash {
public:

    using key_type = K;
    using value_type = V;
    using map_type = google::sparse_hash_map<K, V, std::hash<K>, std::equal_to<K> >;

    using mutex_type = std::shared_timed_mutex;
    using read_only_lock  = std::shared_lock<mutex_type>;
    using updatable_lock = std::unique_lock<mutex_type>;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        map = new map_type(1 << bucketScale);
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
    }

    __attribute__((always_inline))
    void insert(K const& k, V const& v) {
        updatable_lock lock(mtx);
        (*map)[k] = v;
//        printf("put %zu = %zu\n", k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, V& v) {
        read_only_lock lock(mtx);
        auto it = map->find(k);
        if(it != map->end()) {
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
        return "gsparse";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        out << "size: " << map->size()
            ;
    }

private:
    map_type* map;
    mutex_type mtx;
};

template<typename K, typename V>
class ImplGoogleSparseHash_s {
public:

    using key_type = K;
    using value_type = V;
    using map_type = google::sparse_hash_map<K, V, std::hash<K>, std::equal_to<K> >;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        map = new map_type(1 << bucketScale);
    }

    __attribute__((always_inline))
    void insert(K const& k, V const& v) {
        (*map)[k] = v;
//        printf("put %zu = %zu\n", k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, V& v) {
        auto it = map->find(k);
        if(it != map->end()) {
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
        delete map;
    }

    __attribute__((always_inline))
    std::string name() const {
        return "gsparse_s";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        out << "size: " << map->size()
            ;
    }

private:
    map_type* map;
};

template<typename K, typename V>
class ImplGoogleDenseHash {
public:

    using key_type = K;
    using value_type = V;
    using map_type = google::dense_hash_map<K, V, std::hash<K>, std::equal_to<K> >;

    using mutex_type = std::shared_timed_mutex;
    using read_only_lock  = std::shared_lock<mutex_type>;
    using updatable_lock = std::unique_lock<mutex_type>;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        map = new map_type(1 << bucketScale);
        map->set_empty_key(K());
    }

    __attribute__((always_inline))
    void thread_init(int tid) {
    }

    __attribute__((always_inline))
    void insert(K const& k, V const& v) {
        updatable_lock lock(mtx);
        (*map)[k] = v;
//        printf("put %zu = %zu\n", k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, V& v) {
        read_only_lock lock(mtx);
        auto it = map->find(k);
        if(it != map->end()) {
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
        return "gdense";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        out << "size: " << map->size()
            ;
    }

private:
    map_type* map;
    mutex_type mtx;
};

template<typename K, typename V>
class ImplGoogleDenseHash_s {
public:

    using key_type = K;
    using value_type = V;
    using map_type = google::dense_hash_map<K, V, std::hash<K>, std::equal_to<K> >;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        map = new map_type(1 << bucketScale);
        map->set_empty_key(K());
    }

    __attribute__((always_inline))
    void insert(K const& k, V const& v) {
        (*map)[k] = v;
//        printf("put %zu = %zu\n", k, v);
    }

    __attribute__((always_inline))
    bool get(K const& k, V& v) {
        auto it = map->find(k);
        if(it != map->end()) {
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
        delete map;
    }

    __attribute__((always_inline))
    std::string name() const {
        return "gdense_s";
    }

    __attribute__((always_inline))
    void statsString(std::ostream& out, size_t bars) {
        out << "size: " << map->size()
            ;
    }

private:
    map_type* map;
};
