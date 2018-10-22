#pragma once

namespace TestInts {

struct TestData {
    size_t _inserts;
    size_t _threads;
    std::vector<size_t> _randomInts1;

    TestData(size_t inserts, size_t threads)
    : _inserts(inserts)
    , _threads(threads)
    , _randomInts1()
    {
        _threads = threads;
        _inserts = inserts;
        _randomInts1.clear();
        _randomInts1.reserve(_threads*_inserts);
        srand(1234567);
        for(size_t i = _threads*_inserts; i--;) {
            _randomInts1.push_back((1 + rand()) & 0xFFFFFFFFFFFFULL);
        }
    }

    ~TestData() {
    }

    static std::vector<TestData>& getTestDataCache() {
        static std::vector<TestData> testDataCache;
        return testDataCache;
    }
};

template<typename IMPL>
class Test {
public:

    using key_type = size_t;
    using value_type = size_t;

    Test()
    {
    }

    size_t const& key(size_t tid, size_t i) {
        return testData->_randomInts1[tid*testData->_inserts+i];
    }

    size_t value(size_t tid, size_t i, size_t const& k) {
        return k;
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
