#include "./statistics.hpp"
#include <fmt/core.h>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/args.h>
#include <iostream>
#include <glog/logging.h>
#include <cstdlib>

namespace spectrum {

Statistics::Statistics() {
    for (size_t i = 0; i < percentile_latency.size(); ++i) {
        percentile_latency[i] = 0;
    }
}

void Statistics::JournalCommit(size_t latency) {
    count_commit.fetch_add(1, std::memory_order_seq_cst);
    if (latency <= 25) {
        count_latency_25us.fetch_add(1, std::memory_order_seq_cst);
    }
    else if (latency <= 50) {
        count_latency_50us.fetch_add(1, std::memory_order_seq_cst);
    }
    else if (latency <= 100) {
        count_latency_100us.fetch_add(1, std::memory_order_seq_cst);
    }
    else {
        count_latency_100us_above.fetch_add(1, std::memory_order_seq_cst);
    }
    // substitute the closest value in percentile
    if (percentile_populated.load() && rand() % 100 != 0) { return; }
    auto guard = Guard{percentile_latency_mu};
    if (latency < percentile_latency[0]) {
        percentile_latency[0] = latency;
        return;
    }
    for (size_t i = 0; i < percentile_latency.size() - 1; ++i) {
        if (latency >= percentile_latency[i+1]) { continue; }
        percentile_latency[i]   = latency;
        percentile_latency[i+1] = latency;
        if (i == 0) percentile_populated.store(true);
        return;
    }
    percentile_latency[percentile_latency.size()-1] = latency;
}

void Statistics::JournalExecute() {
    count_execution.fetch_add(1, std::memory_order_seq_cst);
}

std::string Statistics::Print() {
    return std::string(fmt::format(
        "@{}\n"
        "commit             {}\n"
        "execution          {}\n"
        "25us               {}\n"
        "50us               {}\n"
        "100us              {}\n"
        ">100us             {}\n"
        "latency(50%)       {}us\n"
        "latency(75%)       {}us\n"
        "latency(95%)       {}us\n"
        "latency(99%)       {}us\n",
        std::chrono::system_clock::now(),
        count_commit.load(),
        count_execution.load(),
        count_latency_25us.load(),
        count_latency_50us.load(),
        count_latency_100us.load(),
        count_latency_100us_above.load(),
        percentile_latency[49], 
        percentile_latency[74], 
        percentile_latency[94], 
        percentile_latency[98]
    ));
    #undef nth
}

std::string Statistics::PrintWithDuration(std::chrono::milliseconds duration) {
    #define AVG(X) ((double)(X.load()) / (double)(duration.count()) * (double)(1000))
    return std::string(fmt::format(
        "@{}\n"
        "duration      {}\n"
        "commit        {:.4f} tx/s\n"
        "execution     {:.4f} tx/s\n"
        "25us          {:.4f} tx/s\n"
        "50us          {:.4f} tx/s\n"
        "100us         {:.4f} tx/s\n"
        ">100us        {:.4f} tx/s\n"
        "latency(50%)  {}us\n"
        "latency(75%)  {}us\n"
        "latency(95%)  {}us\n"
        "latency(99%)  {}us\n",
        std::chrono::system_clock::now(),
        duration,
        AVG(count_commit),
        AVG(count_execution),
        AVG(count_latency_25us),
        AVG(count_latency_50us),
        AVG(count_latency_100us),
        AVG(count_latency_100us_above),
        percentile_latency[49], 
        percentile_latency[74], 
        percentile_latency[94], 
        percentile_latency[98]
    ));
    #undef AVG
    #undef nth
}

} // namespace spectrum
