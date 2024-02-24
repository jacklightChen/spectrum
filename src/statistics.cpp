#include "./statistics.hpp"
#include <fmt/core.h>

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
        "#commit        {}\n"
        "#execution     {}\n"
        "#25ms          {}\n"
        "#50ms          {}\n"
        "#100ms         {}\n"
        "#>100ms        {}\n",
        count_commit.load(),
        count_execution.load(),
        count_latency_25ms.load(),
        count_latency_50ms.load(),
        count_latency_100ms.load(),
        count_latency_100ms_above.load()
    );
}