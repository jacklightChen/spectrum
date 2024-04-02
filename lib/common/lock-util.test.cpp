#include <gtest/gtest.h>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/transaction/evm-hash.hpp>
#include <spectrum/common/hex.hpp>
#include <evmc/evmc.hpp>
#include <span>
#include <spectrum/common/glog-prefix.hpp>

namespace {

using namespace spectrum;

TEST(Table, Operations) {
    google::InstallPrefixFormatter(PrefixFormatter);
    auto table = spectrum::Table<std::tuple<evmc::address, evmc::bytes32>, evmc::bytes32, KeyHasher>(20);
    auto k = std::make_tuple(evmc::address{0x1}, evmc::bytes32{0x2});
    table.Put(k, [](evmc::bytes32& v) { v = evmc::bytes32{0x100}; });
    auto v = evmc::bytes32{0};
    table.Get(k, [&](auto _v) { v = _v; });
    ASSERT_EQ(spectrum::to_hex(std::span{(uint8_t*)&v, 32}), "0000000000000000000000000000000000000000000000000000000000000100");
}

}
