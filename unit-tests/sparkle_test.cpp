#include<gtest/gtest.h>
#include<spectrum/protocol-sparkle.hpp>
#include<spectrum/evm_transaction.hpp>
#include<span>

namespace {

#define TX(CODE, INPUT) Transaction(EVMType::BASIC, evmc::address{0}, evmc::address{1}, std::span{(CODE)}, std::span<uint8_t>{(INPUT)})

using namespace spectrum;

TEST(Sparkle, TableOperations) {
    auto code   = std::array<uint8_t, 2>();
    auto input  = std::array<uint8_t, 2>();
    auto table  = SparkleTable(8);
    auto t0 = SparkleTransaction(std::move(TX(code, input)), 0);
    auto t1 = SparkleTransaction(std::move(TX(code, input)), 1);
    auto t2 = SparkleTransaction(std::move(TX(code, input)), 2);
    auto k0 = std::make_tuple(evmc::address{0}, evmc::bytes32{0});
    auto v0 = evmc::bytes32{0};
    auto v1 = evmc::bytes32{1};
    auto v2 = evmc::bytes32{2};
    table.Put(&t0, k0, v0);
    table.Put(&t0, k0, v1);
    table.Put(&t0, k0, v2);
}

#undef TX

}
