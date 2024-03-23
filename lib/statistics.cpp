#include "./statistics.hpp"
#include <fmt/core.h>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/args.h>
#include <iostream>
#include <glog/logging.h>

namespace spectrum {

void Statistics::JournalCommit(size_t latency) {
    count_commit.fetch_add(1, std::memory_order_seq_cst);
    if (latency <= 25) {
        count_latency_25ms.fetch_add(1, std::memory_order_seq_cst);
    }
    else if (latency <= 50) {
        count_latency_50ms.fetch_add(1, std::memory_order_seq_cst);
    }
    else if (latency <= 100) {
        count_latency_100ms.fetch_add(1, std::memory_order_seq_cst);
    }
    else {
        count_latency_100ms_above.fetch_add(1, std::memory_order_seq_cst);
    }
}

void Statistics::JournalExecute() {
    count_execution.fetch_add(1, std::memory_order_seq_cst);
}

std::string Statistics::Print() {
    return std::string(fmt::format(
        "@{}\n"
        "commit        {}\n"
        "execution     {}\n"
        "25ms          {}\n"
        "50ms          {}\n"
        "100ms         {}\n"
        ">100ms        {}\n",
        std::chrono::system_clock::now(),
        count_commit.load(),
        count_execution.load(),
        count_latency_25ms.load(),
        count_latency_50ms.load(),
        count_latency_100ms.load(),
        count_latency_100ms_above.load()
    ));
}

std::string Statistics::PrintWithDuration(std::chrono::milliseconds duration) {
    #define AVG(X) ((double)(X.load()) / (double)(duration.count()) * (double)(1000))
    return std::string(fmt::format(
        "@{}\n"
        "duration      {}\n"
        "commit        {:.4f} tx/s\n"
        "execution     {:.4f} tx/s\n"
        "25ms          {:.4f} tx/s\n"
        "50ms          {:.4f} tx/s\n"
        "100ms         {:.4f} tx/s\n"
        ">100ms        {:.4f} tx/s\n",
        std::chrono::system_clock::now(),
        duration,
        AVG(count_commit),
        AVG(count_execution),
        AVG(count_latency_25ms),
        AVG(count_latency_50ms),
        AVG(count_latency_100ms),
        AVG(count_latency_100ms_above)
    ));
    #undef AVG
}

} // namespace spectrum