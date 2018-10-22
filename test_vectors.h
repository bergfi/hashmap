#pragma once

#include <random>
#include <thread>

#include "murmurhash.h"
#include "myvector.h"

namespace TestVectors {

struct TestData {
    size_t _inserts;
    size_t _threads;
    double _duplicateRatio;
    size_t _stringsTotal;
    std::vector<myvector> _randomStrings;

    TestData(size_t inserts, size_t threads, double duplicateRatio)
    : _inserts(inserts)
    , _threads(threads)
    , _duplicateRatio(duplicateRatio)
    , _stringsTotal(inserts * threads)
    , _randomStrings()
    {

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

    using key_type = myvector;
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

    void generateVector(myvector& to, size_t seed) {

        static char const alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

        size_t i = 0;

        for (; i < 7; ++i) {
            to.data[i] = alphanum[i%61];
        }
        to.data[i++] = '_';

        size_t r = seed;
        while(r > 0) {
            to.data[i] = alphanum[r%61];
            r /= 61;
            ++i;
        }
        to.data[i++] = '_';
        for (; i < to.size; ++i) {
            to.data[i] = alphanum[i%61];
        }

        to.data[to.size-1] = 0;

//        std::cout << "Generated vector " << std::string(to.data) << std::endl;

    }

    void generateSomeStrings(TestData& td) {

//        if(td._stringsTotal < 1024) {
//            std::cout << "Please use enough inserts, more than 1024" << std::endl;
//            abort();
//        }

        td._randomStrings.reserve(td._stringsTotal);

        size_t conc = std::thread::hardware_concurrency();

        for( size_t d = 1; conc != 0 && (td._stringsTotal % conc) != 0; ++d) {
            conc = std::thread::hardware_concurrency() / d;
        }

        if(conc == 0) {
            conc = 1 << (31 - __builtin_clz((int)std::thread::hardware_concurrency()));
            for( size_t d = 1; conc != 0 && (td._stringsTotal % conc) != 0; ++d) {
                conc = std::thread::hardware_concurrency() / d;
            }
        }

        if(conc == 0) {
            conc = 4;
        }

        {
//            std::cout << "Generating " << td._inserts << " vectors, of which " << td._duplicateRatio << " are duplicates" << std::endl;

            std::vector<std::thread> threads;
            for (size_t i1 = conc; i1--; ) {
                threads.push_back(std::thread([this,i1,&td, conc]() {
                    size_t insertsDone = 0;
                    size_t duplicatesDone = 0;
                    size_t total = td._stringsTotal / conc;
                    for (size_t i2 = 0; i2 < total; ++i2) {
                        size_t location = i1 * total + i2;
                        bool isDup = false;
                        if(insertsDone) {
                            double ratio = (double)duplicatesDone / insertsDone;
                            if(ratio < td._duplicateRatio) {
//                                std::cout << "Ratio " << ratio << " < " << td._duplicateRatio << ", so duplicate" << std::endl;
                                ++duplicatesDone;
                                isDup = true;
                            } else {
//                                std::cout << "Normal insert" << std::endl;
                            }
                        } else {
//                            std::cout << "First, normal insert" << std::endl;
                        }
                        ++insertsDone;
                        if(isDup) {
                            strcpy(td._randomStrings[location].data, "DUP");
                            td._randomStrings[location].data[td._randomStrings[location].size-1] = 'D';
                        } else {
                            generateVector(td._randomStrings[location], location);
                        }
                    }

//                    size_t i1_copy = i1;
//                    size_t inserts = td._inserts / conc;
//                    size_t total = td._stringsTotal / conc;
//                    for (size_t i2 = 0; i2 < inserts; ++i2) {
//                        size_t location = i1_copy * inserts + i2;
//                        generateVector(td._randomStrings, location);
//                    }
//                    i1_copy = (i1_copy + 1) % conc;
//                    for (size_t i2 = inserts; i2 < total; ++i2) {
//                        size_t location = i1_copy * inserts + i2;
//                        generateVector(td._randomStrings, location);
//                    }
                }));
            }
            for(auto& t: threads) t.join();
            threads.clear();
//            for (size_t location = 0; location < td._stringsTotal; ++location) {
//                std::cout << location << " -> " << td._randomStrings[location] << std::endl;
//            }
//            for (size_t i1 = conc; i1--; ) {
//                size_t insertsDone = 0;
//                size_t duplicatesDone = 0;
//                size_t total = td._stringsTotal / conc;
//                for (size_t i2 = 0; i2 < total; ++i2) {
//                    size_t location = i1 * total + i2;
//                    if(td._randomStrings[location].data[0] == 'D') {
//                        size_t location2 = ((i1 + 1) % conc) * total + i2;
//                        location2 = (location2 + 1) % td._stringsTotal;
//
//                        // This assumes x86
//                        while(td._randomStrings[location2].data[0] == 'D' && td._randomStrings[location2].data[td._randomStrings[location2].size-1] == 'D') {
//                            location2 = (location2 + 1) % td._stringsTotal;
//                        }
//                        std::cout << "FIXING " << location << std::endl;
//                        strncpy(td._randomStrings[location].data, td._randomStrings[location2].data, td._randomStrings[location2].size);
//                        //generateVector(td._randomStrings[location], location2);
//                    }
//                }
//            }
            size_t locationSource = td._stringsTotal;
            for (size_t location = 0; location < td._stringsTotal; ++location) {
                if(td._randomStrings[location].data[0] == 'D') {
                    do {
                        if(locationSource == 0) locationSource = td._stringsTotal;
                        locationSource--;
                    } while(td._randomStrings[locationSource].data[0] == 'D');
                    strncpy(td._randomStrings[location].data, td._randomStrings[locationSource].data, td._randomStrings[locationSource].size);
                }
            }
//            for(auto& t: threads) t.join();
//            for (size_t location = 0; location < td._stringsTotal; ++location) {
//                std::cout << location << " -> " << td._randomStrings[location] << std::endl;
//            }
        }

    }

    myvector const& getSomeString(int tid, size_t i) {
        return testData->_randomStrings[(tid*testData->_inserts+i) % testData->_stringsTotal];
    }

    myvector const& key(size_t tid, size_t i) {
        return getSomeString(tid, i);
    }

    size_t value(size_t tid, size_t i, myvector const& key) {
        //return (key.len+3) * (tid+2) * (i+1);
        return (key.size);
    }

    bool setup(size_t bucketScale, size_t threads, size_t inserts, double duplicateRatio = 0.0, double collisionRatio = 1.0) {
        assert(collisionRatio == 1.0 && "collision setting not supported");
        for(TestData& td: TestData::getTestDataCache()) {
            if(td._duplicateRatio == duplicateRatio && td._threads == threads && td._inserts == inserts) {
                testData = &td;
                return true;
            }
        }
        TestData::getTestDataCache().emplace_back(inserts, threads, duplicateRatio);
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
