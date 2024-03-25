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
    DLOG(INFO) << "latency: " << latency << std::endl;
    // substitute the closest value in percentile
    auto guard = Guard{percentile_latency_lock};
    if (percentile_latency.size() < 100) {
        percentile_latency.insert(latency);
        return;
    }
    auto p = percentile_latency.insert(latency).first;
    if (p == percentile_latency.end()) {
        percentile_latency.erase(--p);
        return;
    }
    else if (p == percentile_latency.begin()) {
        percentile_latency.erase(++p);
        return;
    }
    auto q = ++p;
    auto k = --(--p);
    if (latency - *k > *q - latency) {
        percentile_latency.erase(q); 
    }
    else {
        percentile_latency.erase(k);
    }
}

void Statistics::JournalExecute() {
    count_execution.fetch_add(1, std::memory_order_seq_cst);
}


std::string Statistics::Print() {
    #define nth(i) (*std::next(percentile_latency.begin(), i * (percentile_latency.size()-1) / 100))
    auto guard = Guard{percentile_latency_lock};
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
        nth(50), nth(75), nth(95), nth(99)
    ));
    #undef nth
}

std::string Statistics::PrintWithDuration(std::chrono::milliseconds duration) {
    #define AVG(X) ((double)(X.load()) / (double)(duration.count()) * (double)(1000))
    #define nth(i) (*std::next(percentile_latency.begin(), i * (percentile_latency.size()-1) / 100))
    auto guard = Guard{percentile_latency_lock};
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
        nth(50), nth(75), nth(95), nth(99)
    ));
    #undef AVG
    #undef nth
}

} // namespace spectrum
