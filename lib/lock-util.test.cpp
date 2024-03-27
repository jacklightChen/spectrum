#include <gtest/gtest.h>
#include <spectrum/lock-util.hpp>
#include <spectrum/evm_hash.hpp>
#include <spectrum/hex.hpp>
#include <evmc/evmc.hpp>
#include <span>
#include "glog-prefix-install.test.hpp"

namespace {

using namespace spectrum;

TEST(Table, Operations) {
    GLOG_PREFIX;
    auto table = spectrum::Table<std::tuple<evmc::address, evmc::bytes32>, evmc::bytes32, KeyHasher>(20);
    auto k = std::make_tuple(evmc::address{0x1}, evmc::bytes32{0x2});
    table.Put(k, [](evmc::bytes32& v) { v = evmc::bytes32{0x100}; });
    auto v = evmc::bytes32{0};
    table.Get(k, [&](auto _v) { v = _v; });
    ASSERT_EQ(spectrum::to_hex(std::span{(uint8_t*)&v, 32}), "0000000000000000000000000000000000000000000000000000000000000100");
}

}
