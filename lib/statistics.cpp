#include "./statistics.hpp"
#include <fmt/core.h>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/args.h>
#include <iostream>
#include <glog/logging.h>

namespace spectrum {

void Statistics::JournalCommit(size_t latency) {
    auto guard = std::lock_guard{mu};
    count_commit+= 1;
    if (latency <= 25) {
        count_latency_25ms+= 1;
    }
    else if (latency <= 50) {
        count_latency_50ms+= 1;
    }
    else if (latency <= 100) {
        count_latency_100ms+= 1;
    }
    else {
        count_latency_100ms_above+= 1;
    }
}

void Statistics::JournalExecute() {
    auto guard = std::lock_guard{mu};
    count_execution += 1;
}

void Statistics::Print() {
    auto guard = std::lock_guard{mu};
    fmt::print(
        "@{}\n"
        "commit        {}\n"
        "execution     {}\n"
        "25ms          {}\n"
        "50ms          {}\n"
        "100ms         {}\n"
        ">100ms        {}\n",
        std::chrono::system_clock::now(),
        count_commit,
        count_execution,
        count_latency_25ms,
        count_latency_50ms,
        count_latency_100ms,
        count_latency_100ms_above
    );
}

void Statistics::PrintWithDuration(std::chrono::milliseconds duration) {
    auto guard = std::lock_guard{mu};
    #define AVG(X) ((double)(X) / (double)(duration.count()) * (double)(1000))
    fmt::print(
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
    );
    #undef AVG
}

} // namespace spectrum