#pragma once

#include <sys/mman.h>
#include <libfrugi/Settings.h>

using namespace libfrugi;

class MMapper {
public:
    static void* mmap(size_t bytesNeeded, size_t pageSizePower) {
        size_t page_size = 1ULL << pageSizePower;
        size_t numberOfPages = bytesNeeded / page_size;
        if(numberOfPages > 1ULL) {
        } else {
            numberOfPages = 1ULL;
        }
        size_t map_page_size = pageSizePower << MAP_HUGE_SHIFT;
        auto map = ::mmap(nullptr, bytesNeeded, PROT_READ|PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE | map_page_size, -1, 0);
        return map;
    }

    static void* mmapForMap(size_t bytesNeeded) {
        auto map = MMapper::mmap(bytesNeeded, Settings::global()["page_size_scale"].asUnsignedValue());
        posix_madvise(map, bytesNeeded, POSIX_MADV_RANDOM);
        return map;
    }

    static void* mmapForBucket() {
        size_t bytesNeeded = 1 << 2;
        auto map = ::mmap(nullptr, bytesNeeded, PROT_READ|PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        posix_madvise(map, bytesNeeded, POSIX_MADV_SEQUENTIAL);
        return map;
    }

    static void munmap(void* addr, size_t len) {
        ::munmap(addr, len);
    }
};
