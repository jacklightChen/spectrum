#include <iostream>
#include <thread>
#include <ranges>
#include <string_view>
#include <fmt/core.h>
#include <iterator>
#include <stdexcept>
#include <chrono>
#include <glog/logging.h>
#include <string.h>
#include "argparse.hpp"
#include <spectrum/common/glog-prefix.hpp>

int main(int argc, char* argv[]) {
    // configure prefix formatting and eat google logging command line arguments
    FLAGS_stderrthreshold = 1;
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InstallPrefixFormatter(PrefixFormatter);
    google::InitGoogleLogging(argv[0]);
    // check if the rest arguments have the correct number
    CHECK(argc == 4) << "Except google logging flags, we expect 3 arguments. " << "But we got " << argc - 1 << " ." << std::endl;
    DLOG(WARNING) << "Debug Mode: don't expect good performance. " << std::endl;
    // parse args and allocate resources
    auto statistics = std::make_unique<Statistics>();
    auto workload = ParseWorkload(argv[2]);
    auto protocol = ParseProtocol(argv[1], *workload, *statistics);
    auto duration = to<milliseconds>(argv[3]);
    // start running
    auto start_time = steady_clock::now();
    protocol->Start();
    std::this_thread::sleep_for(duration);
    protocol->Stop();
    // stop running and print statistics
    DLOG(WARNING) << "Debug Mode: don't expect good performance. " << std::endl;
    std::cerr << statistics->PrintWithDuration(duration_cast<milliseconds>(steady_clock::now() - start_time));
}