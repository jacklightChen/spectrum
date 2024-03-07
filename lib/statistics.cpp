#include "./statistics.hpp"
#include <fmt/core.h>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/args.h>

namespace spectrum {

Statistics::Statistics(const Statistics& _):
    count_commit{_.count_commit.load()},
    count_execution{_.count_execution.load()},
    count_latency_25ms{_.count_latency_25ms.load()},
    count_latency_50ms{_.count_latency_50ms.load()},
    count_latency_100ms{_.count_latency_100ms.load()},
    count_latency_100ms_above{_.count_latency_100ms_above.load()}
{}

void Statistics::JournalCommit(size_t latency) {
    this->count_commit.fetch_add(1);
    if (latency <= 25) {
        this->count_latency_25ms.fetch_add(1);
    }
    else if (latency <= 50) {
        this->count_latency_50ms.fetch_add(1);
    }
    else if (latency <= 100) {
        this->count_latency_100ms.fetch_add(1);
    }
    else {
        this->count_latency_100ms_above.fetch_add(1);
    }
}

void Statistics::JournalExecute() {
    this->count_execution.fetch_add(1);
}

void Statistics::Print() {
    fmt::print(
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
    );
}

void Statistics::PrintWithDuration(std::chrono::milliseconds duration) {
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
        AVG(count_commit.load()),
        AVG(count_execution.load()),
        AVG(count_latency_25ms.load()),
        AVG(count_latency_50ms.load()),
        AVG(count_latency_100ms.load()),
        AVG(count_latency_100ms_above.load())
    );
    #undef AVG
}

} // namespace spectrum