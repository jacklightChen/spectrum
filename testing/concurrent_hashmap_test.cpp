#include <gtest/gtest.h>
#include <concurrent_hashmap.hpp>
#include <evmc/evmc.hpp>
#include <hex.hpp>
#include <span>

namespace {

struct KeyHasher {
    size_t operator()(const std::tuple<evmc::address, evmc::bytes32> key) const {
        auto addr = std::get<0>(key);
        auto keyx = std::get<1>(key);
        size_t h = 0;
        for (auto x: addr.bytes) {
            h ^= std::hash<int>{}(x)  + 0x9e3779b9 + (h << 6) + (h >> 2);
        }
        for (auto x: keyx.bytes) {
            h ^= std::hash<int>{}(x)  + 0x9e3779b9 + (h << 6) + (h >> 2);
        }
        return h;
    } 
};

TEST(Table, Operations) {
    auto table = spectrum::Table<std::tuple<evmc::address, evmc::bytes32>, evmc::bytes32, KeyHasher>(20);
    auto k = std::make_tuple(evmc::address{0x1}, evmc::bytes32{0x2});
    table.Put(k, [](evmc::bytes32& v) { v = evmc::bytes32{0x100}; });
    auto v = evmc::bytes32{0};
    table.Get(k, v);
    ASSERT_EQ(spectrum::to_hex(std::span{(uint8_t*)&v, 32}), "0000000000000000000000000000000000000000000000000000000000000100");
}

}
