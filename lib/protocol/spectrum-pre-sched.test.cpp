#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <spectrum/protocol/spectrum-pre-sched.hpp>
#include <spectrum/transaction/evm-transaction.hpp>
#include <spectrum/workload/ycsb.hpp>
#include <spectrum/common/glog-prefix.hpp>

namespace {

#define TX(CODE, INPUT) Transaction(EVMType::BASIC, evmc::address{0}, evmc::address{1}, std::span{(CODE)}, std::span<uint8_t>{(INPUT)})

using namespace spectrum;
using namespace std::chrono_literals;

TEST(SpectrumPreSched, JustRunYCSB) {
    google::InstallPrefixFormatter(PrefixFormatter);
    auto statistics = Statistics();
    auto workload = YCSB(11, 0.0);
    auto protocol = SpectrumPreSched(workload, statistics, 8, 32, EVMType::COPYONWRITE);
    protocol.Start();
    std::this_thread::sleep_for(100ms);
    protocol.Stop();
    statistics.Print();
}

}