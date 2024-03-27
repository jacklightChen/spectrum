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
    std::vector<size_t> count_commit{0};
    std::vector<size_t> count_execution{0};
    std::vector<size_t> count_latency_25us{0};
    std::vector<size_t> count_latency_50us{0};
    std::vector<size_t> count_latency_100us{0};
    std::vector<size_t> count_latency_100us_above{0};
    static const int sample_size = 10000;
    std::array<size_t, sample_size> latency_array;
    void SortLatency();
    size_t PercentileLatency(size_t p);
    SpinLock            percentile_latency_mu;
    Statistics() = default;
    Statistics(const Statistics& statistics) = delete;
    void JournalCommit(size_t latency);
    void JournalExecute();
    std::string Print();
    std::string PrintWithDuration(std::chrono::milliseconds duration);
};

} // namespace spectrum
