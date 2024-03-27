#include <gtest/gtest.h>
#include <spectrum/protocol/spectrum.hpp>
#include <spectrum/common/evm_transaction.hpp>
#include <spectrum/workload/smallbank.hpp>
#include <span>
#include <spectrum/common/glog-prefix.hpp>

namespace {

#define TX(CODE, INPUT) Transaction(EVMType::BASIC, evmc::address{0}, evmc::address{1}, std::span{(CODE)}, std::span<uint8_t>{(INPUT)})

using namespace spectrum;
using namespace std::chrono_literals;

TEST(Spectrum, JustRunSmallbank) {
    google::InstallPrefixFormatter(PrefixFormatter);
    auto statistics = Statistics();
    auto workload = Smallbank(10000, 0.0);
    auto protocol = Spectrum(workload, statistics, 8, 32, 2, EVMType::COPYONWRITE);
    protocol.Start();
    std::this_thread::sleep_for(1000ms);
    protocol.Stop();
    statistics.Print();
}

}