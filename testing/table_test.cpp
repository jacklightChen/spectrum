#include <gtest/gtest.h>
#include <table.hpp>
#include <evmc/evmc.hpp>
#include <hex.hpp>
#include <span>
#include <evm_hash.hpp>

namespace {

using namespace spectrum;

TEST(Table, Operations) {
    auto table = spectrum::Table<std::tuple<evmc::address, evmc::bytes32>, evmc::bytes32, KeyHasher>(20);
    auto k = std::make_tuple(evmc::address{0x1}, evmc::bytes32{0x2});
    table.Put(k, [](evmc::bytes32& v) { v = evmc::bytes32{0x100}; });
    auto v = evmc::bytes32{0};
    table.Get(k, v);
    ASSERT_EQ(spectrum::to_hex(std::span{(uint8_t*)&v, 32}), "0000000000000000000000000000000000000000000000000000000000000100");
}

}
