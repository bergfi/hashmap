/*
 * Copyright 2011-2016 Formal Methods and Tools, University of Twente
 * Copyright 2016-2017 Tom van Dijk, Johannes Kepler University Linz
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "SYLVAN_TABLE.h"

#include <errno.h>  // for errno
#include <string.h> // memset
#include <sys/mman.h> // for mmap
#include <atomic>
#include <cstdlib>
#include <cstdio>

#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

#ifndef cas
#define cas(ptr, old, new) (__sync_bool_compare_and_swap((ptr),(old),(new)))
#endif

static std::atomic<size_t> totalThreads;
static __thread size_t myTID;

static __thread uint64_t my_region;

static uint64_t
claim_data_bucket(const llmsset_t dbs)
{
    auto myreg = my_region;

    for (;;) {
        if (myreg != (uint64_t)-1) {
            // find empty bucket in region <my_region>
            uint64_t *ptr = dbs->bitmap2 + (myreg*8);
            int i=0;
            for (;i<8;) {
                uint64_t v = *ptr;
                if (v != 0xffffffffffffffffLL) {
                    int j = __builtin_clzll(~v);
                    *ptr |= (0x8000000000000000LL>>j);
                    return (8 * myreg + i) * 64 + j;
                }
                i++;
                ptr++;
            }
        } else {
            // special case on startup or after garbage collection
            myreg += (myTID*(dbs->table_size/(64*8)))/totalThreads;
        }
        uint64_t count = dbs->table_size/(64*8);
        for (;;) {
            // check if table maybe full
            if (count-- == 0) return (uint64_t)-1;

            myreg += 1;
            if (myreg >= (dbs->table_size/(64*8))) myreg = 0;

            // try to claim it
            uint64_t *ptr = dbs->bitmap1 + (myreg/64);
            uint64_t mask = 0x8000000000000000LL >> (myreg&63);
            uint64_t v;
restart:
            v = *ptr;
            if (v & mask) continue; // taken
            if (cas(ptr, v, v|mask)) break;
            else goto restart;
        }
        my_region = myreg;
    }
}

static void
release_data_bucket(const llmsset_t dbs, uint64_t index)
{
    uint64_t *ptr = dbs->bitmap2 + (index/64);
    uint64_t mask = 0x8000000000000000LL >> (index&63);
    *ptr &= ~mask;
}

static void
set_custom_bucket(const llmsset_t dbs, uint64_t index, int on)
{
    uint64_t *ptr = dbs->bitmapc + (index/64);
    uint64_t mask = 0x8000000000000000LL >> (index&63);
    if (on) *ptr |= mask;
    else *ptr &= ~mask;
}

static int
is_custom_bucket(const llmsset_t dbs, uint64_t index)
{
    uint64_t *ptr = dbs->bitmapc + (index/64);
    uint64_t mask = 0x8000000000000000LL >> (index&63);
    return (*ptr & mask) ? 1 : 0;
}

uint64_t
llmsset_hash(const uint64_t a, const uint64_t b, const uint64_t seed)
{
    // The FNV-1a hash for 64 bits
    const uint64_t prime = 1099511628211;
    uint64_t hash = seed;
    hash = (hash ^ a) * prime;
    hash = (hash ^ b) * prime;
    return hash ^ (hash>>32);
}

/*
 * CL_MASK and CL_MASK_R are for the probe sequence calculation.
 * With 64 bytes per cacheline, there are 8 64-bit values per cacheline.
 */
// The LINE_SIZE is defined in lace.h
static const uint64_t LINE_SIZE = 64;
static const uint64_t CL_MASK     = ~(((LINE_SIZE) / 8) - 1);
static const uint64_t CL_MASK_R   = ((LINE_SIZE) / 8) - 1;

/* 40 bits for the index, 24 bits for the hash */
#define MASK_INDEX ((uint64_t)0x000000ffffffffff)
#define MASK_HASH  ((uint64_t)0xffffff0000000000)

static inline uint64_t
llmsset_lookup2(const llmsset_t dbs, uint64_t a, uint64_t b, int* created, const int custom)
{
    uint64_t hash_rehash = 14695981039346656037LLU;
    if (custom) hash_rehash = dbs->hash_cb(a, b, hash_rehash);
    else hash_rehash = llmsset_hash(a, b, hash_rehash);

    const uint64_t step = (((hash_rehash >> 20) | 1) << 3);
    const uint64_t hash = hash_rehash & MASK_HASH;
    uint64_t idx, last, cidx = 0;
    int i=0;

#if LLMSSET_MASK
    last = idx = hash_rehash & dbs->mask;
#else
    last = idx = hash_rehash % dbs->table_size;
#endif

    for (;;) {
        volatile uint64_t *bucket = dbs->table + idx;
        uint64_t v = *bucket;

        if (v == 0) {
            if (cidx == 0) {
                // Claim data bucket and write data
                cidx = claim_data_bucket(dbs);
                if (cidx == (uint64_t)-1) return 0; // failed to claim a data bucket
                if (custom) dbs->create_cb(&a, &b);
                uint64_t *d_ptr = ((uint64_t*)dbs->data) + 2*cidx;
                d_ptr[0] = a;
                d_ptr[1] = b;
            }
            if (cas(bucket, 0, hash | cidx)) {
                if (custom) set_custom_bucket(dbs, cidx, custom);
                *created = 1;
                return cidx;
            } else {
                v = *bucket;
            }
        }

        if (hash == (v & MASK_HASH)) {
            uint64_t d_idx = v & MASK_INDEX;
            uint64_t *d_ptr = ((uint64_t*)dbs->data) + 2*d_idx;
            if (custom) {
                if (dbs->equals_cb(a, b, d_ptr[0], d_ptr[1])) {
                    if (cidx != 0) {
                        dbs->destroy_cb(a, b);
                        release_data_bucket(dbs, cidx);
                    }
                    *created = 0;
                    return d_idx;
                }
            } else {
                if (d_ptr[0] == a && d_ptr[1] == b) {
                    if (cidx != 0) release_data_bucket(dbs, cidx);
                    *created = 0;
                    return d_idx;
                }
            }
        }

        // find next idx on probe sequence
        idx = (idx & CL_MASK) | ((idx+1) & CL_MASK_R);
        if (idx == last) {
            if (++i == dbs->threshold) return 0; // failed to find empty spot in probe sequence

            // go to next cache line in probe sequence
            hash_rehash += step;

#if LLMSSET_MASK
            last = idx = hash_rehash & dbs->mask;
#else
            last = idx = hash_rehash % dbs->table_size;
#endif
        }
    }
}

uint64_t
llmsset_lookup(const llmsset_t dbs, const uint64_t a, const uint64_t b, int* created)
{
    return llmsset_lookup2(dbs, a, b, created, 0);
}

uint64_t
llmsset_lookupc(const llmsset_t dbs, const uint64_t a, const uint64_t b, int* created)
{
    return llmsset_lookup2(dbs, a, b, created, 1);
}

int
llmsset_rehash_bucket(const llmsset_t dbs, uint64_t d_idx)
{
    const uint64_t * const d_ptr = ((uint64_t*)dbs->data) + 2*d_idx;
    const uint64_t a = d_ptr[0];
    const uint64_t b = d_ptr[1];

    uint64_t hash_rehash = 14695981039346656037LLU;
    const int custom = is_custom_bucket(dbs, d_idx) ? 1 : 0;
    if (custom) hash_rehash = dbs->hash_cb(a, b, hash_rehash);
    else hash_rehash = llmsset_hash(a, b, hash_rehash);
    const uint64_t step = (((hash_rehash >> 20) | 1) << 3);
    const uint64_t new_v = (hash_rehash & MASK_HASH) | d_idx;
    int i=0;

    uint64_t idx, last;
#if LLMSSET_MASK
    last = idx = hash_rehash & dbs->mask;
#else
    last = idx = hash_rehash % dbs->table_size;
#endif

    for (;;) {
        volatile uint64_t *bucket = &dbs->table[idx];
        if (*bucket == 0 && cas(bucket, 0, new_v)) return 1;

        // find next idx on probe sequence
        idx = (idx & CL_MASK) | ((idx+1) & CL_MASK_R);
        if (idx == last) {
            if (++i == *(volatile int16_t*)&dbs->threshold) {
                // failed to find empty spot in probe sequence
                // solution: increase probe sequence length...
                __sync_fetch_and_add(&dbs->threshold, 1);
            }

            // go to next cache line in probe sequence
            hash_rehash += step;

#if LLMSSET_MASK
            last = idx = hash_rehash & dbs->mask;
#else
            last = idx = hash_rehash % dbs->table_size;
#endif
        }
    }
}

llmsset_t
llmsset_create(size_t initial_size, size_t max_size)
{
    llmsset_t dbs = NULL;
    if (posix_memalign((void**)&dbs, LINE_SIZE, sizeof(struct llmsset)) != 0) {
        fprintf(stderr, "llmsset_create: Unable to allocate memory!\n");
        exit(1);
    }

#if LLMSSET_MASK
    /* Check if initial_size and max_size are powers of 2 */
    if (__builtin_popcountll(initial_size) != 1) {
        fprintf(stderr, "llmsset_create: initial_size is not a power of 2!\n");
        exit(1);
    }

    if (__builtin_popcountll(max_size) != 1) {
        fprintf(stderr, "llmsset_create: max_size is not a power of 2!\n");
        exit(1);
    }
#endif

    if (initial_size > max_size) {
        fprintf(stderr, "llmsset_create: initial_size > max_size!\n");
        exit(1);
    }

    // minimum size is now 512 buckets (region size, but of course, n_workers * 512 is suggested as minimum)

    if (initial_size < 512) {
        fprintf(stderr, "llmsset_create: initial_size too small!\n");
        exit(1);
    }

    dbs->max_size = max_size;
    llmsset_set_size(dbs, initial_size);

    /* This implementation of "resizable hash table" allocates the max_size table in virtual memory,
       but only uses the "actual size" part in real memory */

    dbs->table = (uint64_t*)mmap(0, dbs->max_size * 8, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    dbs->data = (uint8_t*)mmap(0, dbs->max_size * 16, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

    /* Also allocate bitmaps. Each region is 64*8 = 512 buckets.
       Overhead of bitmap1: 1 bit per 4096 bucket.
       Overhead of bitmap2: 1 bit per bucket.
       Overhead of bitmapc: 1 bit per bucket. */

    dbs->bitmap1 = (uint64_t*)mmap(0, dbs->max_size / (512*8), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    dbs->bitmap2 = (uint64_t*)mmap(0, dbs->max_size / 8, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    dbs->bitmapc = (uint64_t*)mmap(0, dbs->max_size / 8, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

    if (dbs->table == (uint64_t*)-1 || dbs->data == (uint8_t*)-1 || dbs->bitmap1 == (uint64_t*)-1 || dbs->bitmap2 == (uint64_t*)-1 || dbs->bitmapc == (uint64_t*)-1) {
        fprintf(stderr, "llmsset_create: Unable to allocate memory: %s!\n", strerror(errno));
        exit(1);
    }

#if defined(madvise) && defined(MADV_RANDOM)
    madvise(dbs->table, dbs->max_size * 8, MADV_RANDOM);
#endif

    // forbid first two positions (index 0 and 1)
    dbs->bitmap2[0] = 0xc000000000000000LL;

    dbs->hash_cb = NULL;
    dbs->equals_cb = NULL;
    dbs->create_cb = NULL;
    dbs->destroy_cb = NULL;

    // yes, ugly. for now, we use a global thread-local value.
    // that is a problem with multiple tables.
    // so, for now, do NOT use multiple tables!!

    return dbs;
}

void
llmsset_free(llmsset_t dbs)
{
    munmap(dbs->table, dbs->max_size * 8);
    munmap(dbs->data, dbs->max_size * 16);
    munmap(dbs->bitmap1, dbs->max_size / (512*8));
    munmap(dbs->bitmap2, dbs->max_size / 8);
    munmap(dbs->bitmapc, dbs->max_size / 8);
    free(dbs);
}

int
llmsset_is_marked(const llmsset_t dbs, uint64_t index)
{
    volatile uint64_t *ptr = dbs->bitmap2 + (index/64);
    uint64_t mask = 0x8000000000000000LL >> (index&63);
    return (*ptr & mask) ? 1 : 0;
}

int
llmsset_mark(const llmsset_t dbs, uint64_t index)
{
    volatile uint64_t *ptr = dbs->bitmap2 + (index/64);
    uint64_t mask = 0x8000000000000000LL >> (index&63);
    for (;;) {
        uint64_t v = *ptr;
        if (v & mask) return 0;
        if (cas(ptr, v, v|mask)) return 1;
    }
}

/**
 * Set custom functions
 */
void llmsset_set_custom(const llmsset_t dbs, llmsset_hash_cb hash_cb, llmsset_equals_cb equals_cb, llmsset_create_cb create_cb, llmsset_destroy_cb destroy_cb)
{
    dbs->hash_cb = hash_cb;
    dbs->equals_cb = equals_cb;
    dbs->create_cb = create_cb;
    dbs->destroy_cb = destroy_cb;
}

void llmsset_thread_init(const llmsset_t dbs, size_t tid) {
    myTID = tid;
    totalThreads++;
}
