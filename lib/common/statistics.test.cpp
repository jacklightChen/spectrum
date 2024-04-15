#include <spectrum/common/statistics.hpp>
#include <thread>
#include <spectrum/common/glog-prefix.hpp>
#include <gtest/gtest.h>

namespace {

using namespace std::chrono_literals;
using namespace std::chrono;

TEST(Statistics, BenchJournal) {
    auto statistics  = spectrum::Statistics();
    auto stop_flag   = std::atomic<bool>{false};
    auto handle      = std::thread([&]() { while (!stop_flag.load()) {
        statistics.JournalExecute();
        statistics.JournalCommit(10);
    }});
    std::this_thread::sleep_for(200ms);
    stop_flag.store(true);
    handle.join();
    std::cerr << statistics.PrintWithDuration(200ms) << std::endl;
}

}