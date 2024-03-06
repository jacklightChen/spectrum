#include <gtest/gtest.h>
#include <spectrum/protocol-spectrum.hpp>
#include <spectrum/evm_transaction.hpp>
#include <spectrum/workload-smallbank.hpp>
#include <span>

namespace {

#define TX(CODE, INPUT) Transaction(EVMType::BASIC, evmc::address{0}, evmc::address{1}, std::span{(CODE)}, std::span<uint8_t>{(INPUT)})

using namespace spectrum;
using namespace std::chrono_literals;

TEST(Spectrum, JustRunSmallbank) {
    auto workload = Smallbank();
    auto protocol = Spectrum(workload, 8, 32, 16);
    protocol.Start();
    std::this_thread::sleep_for(2000ms);
    auto statistics = protocol.Stop();
    statistics.Print();
}

}