#pragma once
#include <atomic>
#include <chrono>
#include <mutex>
#include "./percentile.hpp"

namespace spectrum {

class Statistics {
    public:
    std::atomic<size_t> count_commit{0};
    std::atomic<size_t> count_execution{0};
    std::atomic<size_t> count_latency_25ms{0};
    std::atomic<size_t> count_latency_50ms{0};
    std::atomic<size_t> count_latency_100ms{0};
    std::atomic<size_t> count_latency_100ms_above{0};
    // Percentile<size_t> percentile;
    Statistics() = default;
    Statistics(const Statistics& statistics) = delete;
    void JournalCommit(size_t latency);
    void JournalExecute();
    std::string Print();
    std::string PrintWithDuration(std::chrono::milliseconds duration);
};

} // namespace spectrum
