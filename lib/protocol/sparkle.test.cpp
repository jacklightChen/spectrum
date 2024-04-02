#include <gtest/gtest.h>
#include <spectrum/protocol/sparkle.hpp>
#include <spectrum/common/evm_transaction.hpp>
#include <spectrum/workload/smallbank.hpp>
#include <span>
#include <spectrum/common/glog-prefix.hpp>

namespace {

#define TX(CODE, INPUT) Transaction(EVMType::BASIC, evmc::address{0}, evmc::address{1}, std::span{(CODE)}, std::span<uint8_t>{(INPUT)})

using namespace spectrum;
using namespace std::chrono_literals;

TEST(Sparkle, TableWriteAfterRead) {
    google::InstallPrefixFormatter(PrefixFormatter);
    auto code   = std::array<uint8_t, 2>();
    auto input  = std::array<uint8_t, 2>();
    auto table  = SparkleTable(8);
    auto t0 = SparkleTransaction(TX(code, input), 1);
    auto t1 = SparkleTransaction(TX(code, input), 2);
    auto t2 = SparkleTransaction(TX(code, input), 3);
    auto k0 = std::make_tuple(evmc::address{0}, evmc::bytes32{0});
    auto v0 = evmc::bytes32{0};
    auto v1 = evmc::bytes32{1};
    auto v2 = evmc::bytes32{2};
    auto version = size_t{0};
    // because t2 reads the default value, but later this is updated by both t0 then t1, so t2 is read a stalled value and it is invalid. 
    table.Get(&t2, k0, v1, version);
    table.Put(&t0, k0, v2);
    table.Put(&t1, k0, v0);
    ASSERT_TRUE (t2.HasRerunFlag()) << "t2 rerun_flag";
    ASSERT_FALSE(t0.HasRerunFlag()) << "t0 rerun_flag";
    ASSERT_FALSE(t1.HasRerunFlag()) << "t1 rerun_flag";
}

TEST(Sparkle, TableWriteAfterWrite) {
    google::InstallPrefixFormatter(PrefixFormatter);
    auto code   = std::array<uint8_t, 2>();
    auto input  = std::array<uint8_t, 2>();
    auto table  = SparkleTable(8);
    auto t0 = SparkleTransaction(TX(code, input), 1);
    auto t1 = SparkleTransaction(TX(code, input), 2);
    auto t2 = SparkleTransaction(TX(code, input), 3);
    auto k0 = std::make_tuple(evmc::address{0}, evmc::bytes32{0});
    auto v0 = evmc::bytes32{0};
    auto v1 = evmc::bytes32{1};
    auto v2 = evmc::bytes32{2};
    auto version = size_t{0};
    // because t2 reads the value from t1, as long as t1 keeps valid, t2 will not be evicted because of this read
    table.Put(&t1, k0, v0);
    table.Get(&t2, k0, v1, version);
    table.Put(&t0, k0, v2);
    ASSERT_FALSE(t2.HasRerunFlag()) << "t2 rerun_flag";
    ASSERT_FALSE(t0.HasRerunFlag()) << "t0 rerun_flag";
    ASSERT_FALSE(t1.HasRerunFlag()) << "t1 rerun_flag";
}

TEST(Sparkle, JustRunSmallbank) {
    google::InstallPrefixFormatter(PrefixFormatter);
    auto statistics = Statistics();
    auto workload = Smallbank(10000, 0.0);
    auto protocol = Sparkle(workload, statistics, 6, 32);
    protocol.Start();
    std::this_thread::sleep_for(100ms);
    protocol.Stop();
    statistics.Print();   
}

#undef TX

}
