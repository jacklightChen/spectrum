#include "./statistics.hpp"
#include <fmt/core.h>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/args.h>
#include <iostream>
#include <glog/logging.h>
#include <cstdlib>

namespace spectrum {

void Statistics::JournalCommit(size_t latency) {
    auto _count_commit = count_commit.fetch_add(1, std::memory_order_seq_cst);
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

    // Reservoir Sampling
    if (_count_commit < sample_size) {
        latency_array[_count_commit] = latency;
    } else {
        // rand() is not thread-safe, and it cost a lot of time
        auto index = rand() % _count_commit;
        if (index < sample_size) latency_array[index] = latency;
    }
}

void Statistics::JournalExecute() {
    count_execution.fetch_add(1, std::memory_order_seq_cst);
}

void Statistics::SortLatency() {
    if (count_commit.load() <= sample_size) {
        std::sort(latency_array.begin(), latency_array.begin() + count_commit.load());
    } else {
        std::sort(latency_array.begin(), latency_array.end());
    }
}

size_t Statistics::PercentileLatency(size_t p) {
    if (count_commit.load() <= sample_size) {
        return latency_array[p * count_commit.load() / 100];
    } else {
        return latency_array[p * sample_size / 100];
    }
}

std::string Statistics::Print() {
    SortLatency();
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
        PercentileLatency(50), 
        PercentileLatency(75),
        PercentileLatency(95),
        PercentileLatency(99)
    ));
    #undef nth
}

std::string Statistics::PrintWithDuration(std::chrono::milliseconds duration) {
    SortLatency();
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
        PercentileLatency(50), 
        PercentileLatency(75),
        PercentileLatency(95),
        PercentileLatency(99)
    ));
    #undef AVG
    #undef nth
}

} // namespace spectrum
