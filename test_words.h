#pragma once

#include <cstring>
#include <iostream>
#include <fstream>
#include <random>

#include "murmurhash.h"
#include "mystring.h"

struct WordData {
    size_t _inserts;
    size_t _threads;
    int _wordsTotal;
    my_string* _words;

    WordData(size_t inserts, size_t threads)
    : _inserts(inserts)
    , _threads(threads)
    , _wordsTotal(0)
    , _words(nullptr)
    {

    }

    ~WordData() {
        if(_words) {
            for(my_string* s = _words, *e = _words + _wordsTotal; s < e; s++) {
                free(s->s);
                s->s = nullptr;
            }
            free(_words);
        }
    }

    static std::vector<WordData>& getTestDataCache() {
        static std::vector<WordData> testDataCache;
        return testDataCache;
    }
};

template<typename IMPL>
class TestWords1 {
public:

public:

    using key_type = my_string;
    using value_type = size_t;

    struct thread_data {
        TestWords1* test;
        IMPL* impl;
        size_t tid;
    };

    TestWords1()
    : testData(nullptr)
    {
    }

    template<typename DIST, typename RNG>
    my_string generateRandomString(DIST& dist, RNG& rng, const size_t& len) {

        char* s = (char*)malloc(sizeof(char) * (len+1));
        static char const alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

        for (size_t i = 0; i < len; ++i) {
            s[i] = alphanum[dist(rng)];
        }

        s[len] = 0;
        return my_string{s,len};
    }

    void readInWords(WordData& td) {
        std::ifstream file;
        file.open("../../words.txt");
        std::string word;

        size_t wordsRead = 0;
        size_t spaceFor = 0;

        td._words = nullptr;
        while(file >> word) {
            if(wordsRead == spaceFor) {
                if(spaceFor) {
                    spaceFor <<= 1;
                } else {
                    spaceFor = 1 << 16;
                }
                td._words = (my_string*)realloc(td._words, spaceFor * sizeof(my_string));
            }
            new(&td._words[wordsRead++]) my_string(strdup(word.c_str()), word.length());
        }
        td._wordsTotal = wordsRead;
        std::cout << "Read " << wordsRead << " words" << std::endl;
    }

    my_string const& getSomeString(int tid, size_t i) {
        return testData->_words[(tid*testData->_inserts+i) % testData->_wordsTotal];
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
        for(WordData& td: WordData::getTestDataCache()) {
            if(td._threads == threads && td._inserts == inserts) {
                testData = &td;
                return true;
            }
        }
        WordData::getTestDataCache().emplace_back(inserts, threads);
        auto& td = WordData::getTestDataCache().back();
        readInWords(td);
        testData = &td;
        return true;
    }

    bool reset() {
        return true;
    }

private:
    WordData* testData;
};
