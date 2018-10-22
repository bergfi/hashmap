#pragma once

#include <random>

#include "murmurhash.h"
#include "mystring.h"

namespace TestStrings {

struct TestData {
    size_t _inserts;
    size_t _threads;
    int _stringsTotal;
    my_string* _randomStrings;

    TestData(size_t inserts, size_t threads)
    : _inserts(inserts)
    , _threads(threads)
    , _stringsTotal(inserts*threads)
    , _randomStrings(nullptr)
    {

    }

    ~TestData() {
        if(_randomStrings) {
            for(my_string* s = _randomStrings, *e = _randomStrings + _stringsTotal; s < e; s++) {
                free(s->s);
                s->s = nullptr;
            }
            free(_randomStrings);
        }
    }

    static std::vector<TestData>& getTestDataCache() {
        static std::vector<TestData> testDataCache;
        return testDataCache;
    }
};

template<typename IMPL>
class Test {
public:

public:

    using key_type = my_string;
    using value_type = size_t;

    struct thread_data {
        Test* test;
        IMPL* impl;
        size_t tid;
    };

    Test()
    : testData(nullptr)
    {
    }

    template<typename DIST, typename RNG>
    void generateRandomString(my_string* to, DIST& dist, RNG& rng, const size_t& len) {

        char* s = (char*)malloc(sizeof(char) * (len+1));
        static char const alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

        for (size_t i = 0; i < len; ++i) {
            s[i] = alphanum[dist(rng)];
        }

        s[len] = 0;
        new(to) my_string(s,len);
    }

    void generateSomeStrings(TestData& td) {

        std::mt19937 rng;
        rng.seed(1234);
        std::uniform_int_distribution<std::mt19937::result_type> dist(0,10+26+26-1);

        td._stringsTotal = td._inserts * td._threads;
        td._randomStrings = (my_string*)realloc(td._randomStrings, sizeof(my_string) * td._stringsTotal);
        for (int i = td._stringsTotal; i--; ) {
            generateRandomString(&td._randomStrings[i], dist, rng, 32);
        }
    }

    my_string const& getSomeString(int tid, size_t i) {
        return testData->_randomStrings[(tid*testData->_inserts+i) % testData->_stringsTotal];
    }

    my_string const& key(size_t tid, size_t i) {
        return getSomeString(tid, i);
    }

    size_t value(size_t tid, size_t i, my_string const& key) {
        //return (key.len+3) * (tid+2) * (i+1);
        return (key.len);
    }

    bool setup(size_t bucketScale, size_t threads, size_t inserts, double duplicateRatio = 0.0, double collisionRatio = 1.0) {
        assert(duplicateRatio == 0.0 && "duplicate setting not supported");
        assert(collisionRatio == 1.0 && "collision setting not supported");
        for(TestData& td: TestData::getTestDataCache()) {
            if(td._threads == threads && td._inserts == inserts) {
                testData = &td;
                return true;
            }
        }
        TestData::getTestDataCache().emplace_back(inserts, threads);
        auto& td = TestData::getTestDataCache().back();
        generateSomeStrings(td);
        testData = &td;
        return true;
    }

    bool reset() {
        return true;
    }

private:
    TestData* testData;
};

}
