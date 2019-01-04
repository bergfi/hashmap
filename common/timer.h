#pragma once

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
