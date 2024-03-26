#pragma once
#include <atomic>
#include <chrono>
#include "./lock-util.hpp"
#include <set>
#include <iterator>
#include <array>

namespace spectrum {

class Statistics {
    public:
    std::atomic<size_t> count_commit{0};
    std::atomic<size_t> count_execution{0};
    std::atomic<size_t> count_latency_25us{0};
    std::atomic<size_t> count_latency_50us{0};
    std::atomic<size_t> count_latency_100us{0};
    std::atomic<size_t> count_latency_100us_above{0};
    std::array<size_t, 100> percentile_latency{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99};
    SpinLock            percentile_latency_mu;
    std::atomic<size_t> percentile_latency_size{0};
    Statistics() = default;
    Statistics(const Statistics& statistics) = delete;
    void JournalCommit(size_t latency);
    void JournalExecute();
    std::string Print();
    std::string PrintWithDuration(std::chrono::milliseconds duration);
};

} // namespace spectrum
