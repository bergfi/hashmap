#pragma once

namespace TestInts3 {

template<typename IMPL>
class Test {
public:

    using key_type = size_t;
    using value_type = size_t;

    size_t duplicateThreshold;
    size_t collisionThreshold;

    size_t _bucketScale;
    size_t _buckets;
    size_t _inserts;
    size_t _threads;
    //const size_t prime = 2147483647;
    const size_t prime = 131071; //(1 << 17) - 1;

    Test()
    {
    }

    size_t key(size_t tid, size_t i) const {
        if(i >= duplicateThreshold) {
            i %= duplicateThreshold;
        }
        if(i >= collisionThreshold) {
            size_t nr = i / collisionThreshold;
            i -= nr * collisionThreshold;
            size_t k = 1ULL + tid * _inserts + i;
            return (nr * _buckets + MurmurHash64(&k, sizeof(k), 0)) & 0xFFFFFFFFULL;
        } else {
            size_t k = 1ULL + tid * _inserts + i;
            return MurmurHash64(&k, sizeof(k), 0) & 0xFFFFFFFFULL;
        }
    }

    size_t value(size_t tid, size_t i, size_t k) {
        return k;
    }

    bool setup(size_t bucketScale, size_t threads, size_t inserts, double duplicateRatio = 0.0, double collisionRatio = 1.0) {
        _bucketScale = bucketScale;
        _buckets =  (1ULL << _bucketScale);
        _threads = threads;
        _inserts = inserts;

        if(collisionRatio < 1.0) collisionRatio = 1.0;
        if(duplicateRatio < 0.0) duplicateRatio = 0.0;
        if(duplicateRatio > 1.0) duplicateRatio = 1.0;

        duplicateThreshold = _inserts - _inserts * duplicateRatio;
        collisionThreshold = duplicateThreshold / collisionRatio;

        if(duplicateThreshold == 0) duplicateThreshold = 1;

        // collisionRatio = 1 -> normal
        // collisionRatio = 2 -> half is normal, other half causes collision

        return true;
    }

    bool reset() {
        return true;
    }

};

}
