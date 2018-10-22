#include <type_traits>

#include "junction/ConcurrentMap_Crude.h"
#include "junction/ConcurrentMap_Linear.h"
#include "junction/ConcurrentMap_Leapfrog.h"
#include "junction/ConcurrentMap_Grampa.h"
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/scalable_allocator.h>

#include "SYLVAN_TABLE.h"
#include "bytell_hash_map.hpp"

#include "cuckoohash_map.hh"

//#include "Honey/Thread/LockFree/UnorderedMap.h"

#include "mystring.h"

extern "C" {
#include "dbs-ll.h"
}

#include "data-structures/definitions.h"

using namespace tbb;

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

template <typename T, typename = int>
struct has_thread_init_method : std::false_type { };

template <typename T>
struct has_thread_init_method <T, decltype((void) &T::thread_init, 0)> : std::true_type { };

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

template<typename K, typename V, template<typename...> typename HT>
class ImplJunction {
public:

    using key_type = K;
    using value_type = V;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        ht = new HT<K,V>(1 << bucketScale);
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
        return v != typename HT<K,V>::Value(HT<K,V>::ValueTraits::NullValue);
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
    HT<K,V>* ht;
};

template<typename K, typename V>
class ImplJunctionCrude: public ImplJunction<K, V, junction::ConcurrentMap_Crude> {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "JunctionCrude<" + std::string(typeid(K).name()) + ">";
    }
};

template<typename K, typename V>
class ImplJunctionLinear: public ImplJunction<K, V, junction::ConcurrentMap_Linear> {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "JunctionLinear<" + std::string(typeid(K).name()) + ">";
    }
};

template<typename K, typename V>
class ImplJunctionLeapFrog: public ImplJunction<K, V, junction::ConcurrentMap_Leapfrog> {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "JunctionLeapFrog<" + std::string(typeid(K).name()) + ">";
    }
};

template<typename K, typename V>
class ImplJunctionGrampa: public ImplJunction<K, V, junction::ConcurrentMap_Grampa> {
public:
    __attribute__((always_inline))
    std::string name() const {
        return "JunctionGrampa<" + std::string(typeid(K).name()) + ">";
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


template<typename K>
class ImplDBSLL {
public:

    using key_type = K;
    using value_type = size_t;
    using map_type = dbs_ll_t;

    __attribute__((always_inline))
    void init(size_t bucketScale) {
        // Size of key is in number of ints
        map = DBSLLcreate_sized(sizeof(K)/sizeof(int), bucketScale, MurmurHash64, 0);
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
        return r != DB_NOT_FOUND;
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

private:
    map_type map;
};

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
