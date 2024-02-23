#include <gtest/gtest.h>
#include <unordered_map>
#include <evmc/evmc.hpp>
#include <tuple>
#include <evm_transaction.hpp>
#include <hex.hpp>
#include <iostream>
#include <sstream>
#include <span>

#define CODE \
    spectrum::from_hex(std::string{\
        "608060405234801561001057600080fd5b506004361061007d5760003560e01c806397"\
        "b632121161005b57806397b63212146100ea578063a5843f0814610106578063ad0f98"\
        "c014610122578063bb27eb2c1461013e5761007d565b80631e01043914610082578063"\
        "83406251146100b25780638ac10b9c146100ce575b600080fd5b61009c600480360381"\
        "01906100979190610404565b61015a565b6040516100a99190610440565b6040518091"\
        "0390f35b6100cc60048036038101906100c7919061045b565b61019f565b005b6100e8"\
        "60048036038101906100e3919061049b565b6101e3565b005b61010460048036038101"\
        "906100ff919061045b565b610274565b005b610120600480360381019061011b919061"\
        "045b565b6102e4565b005b61013c6004803603810190610137919061045b565b610317"\
        "565b005b6101586004803603810190610153919061045b565b610383565b005b600080"\
        "6000808481526020019081526020016000205490506000600160008581526020019081"\
        "526020016000205490508082610196919061051d565b92505050919050565b60008060"\
        "00848152602001908152602001600020549050600082905080826101c7919061051d56"\
        "5b6000808681526020019081526020016000208190555050505050565b600060016000"\
        "8581526020019081526020016000205490506000600160008581526020019081526020"\
        "01600020549050600083905080831061026c57808361022b9190610551565b92508082"\
        "610239919061051d565b91508260016000888152602001908152602001600020819055"\
        "508160016000878152602001908152602001600020819055505b505050505050565b60"\
        "0080600084815260200190815260200160002054905060006001600084815260200190"\
        "8152602001600020549050600060016000858152602001908152602001600020819055"\
        "5080826102c8919061051d565b60008086815260200190815260200160002081905550"\
        "50505050565b8060008084815260200190815260200160002081905550806001600084"\
        "8152602001908152602001600020819055505050565b60006001600084815260200190"\
        "815260200160002054905060008290508181116103635780826103479190610551565b"\
        "600160008681526020019081526020016000208190555061037d565b60006001600086"\
        "8152602001908152602001600020819055505b50505050565b60006001600084815260"\
        "2001908152602001600020549050600082905080826103ac919061051d565b60016000"\
        "8681526020019081526020016000208190555050505050565b600080fd5b6000819050"\
        "919050565b6103e1816103ce565b81146103ec57600080fd5b50565b60008135905061"\
        "03fe816103d8565b92915050565b60006020828403121561041a576104196103c9565b"\
        "5b6000610428848285016103ef565b91505092915050565b61043a816103ce565b8252"\
        "5050565b60006020820190506104556000830184610431565b92915050565b60008060"\
        "408385031215610472576104716103c9565b5b6000610480858286016103ef565b9250"\
        "506020610491858286016103ef565b9150509250929050565b60008060006060848603"\
        "12156104b4576104b36103c9565b5b60006104c2868287016103ef565b935050602061"\
        "04d3868287016103ef565b92505060406104e4868287016103ef565b91505092509250"\
        "92565b7f4e487b71000000000000000000000000000000000000000000000000000000"\
        "00600052601160045260246000fd5b6000610528826103ce565b9150610533836103ce"\
        "565b925082820190508082111561054b5761054a6104ee565b5b92915050565b600061"\
        "055c826103ce565b9150610567836103ce565b925082820390508181111561057f5761"\
        "057e6104ee565b5b9291505056fea26469706673582212204ecbd2eaff1feae44da4ae"\
        "ccb0e8e3055edb63a8145f1fea161d723fc8c5aa6f64736f6c63430008120033"\
    }).value()

namespace {

struct KeyHasher {
    std::size_t operator()(const std::tuple<evmc::address, evmc::bytes32> key) const {
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

class MockTable {

    private:
    std::unordered_map<std::tuple<evmc::address, evmc::bytes32>, evmc::bytes32, KeyHasher> inner;

    public:
    evmc::bytes32 GetStorage(
        const evmc::address& addr, 
        const evmc::bytes32& key
    ) {
        return inner[std::make_tuple(addr, key)];
    }
    void SetStorage(
        const evmc::address& addr, 
        const evmc::bytes32& key, 
        const evmc::bytes32& value
    ) {
        inner[std::make_tuple(addr, key)] = value;
    }

};

inline std::string to_string(int32_t key) {
    auto ss = std::ostringstream();
    ss << std::setw(64) << std::setfill('0') << key;
    return ss.str();
}

TEST(Transaction, RunBasic) {
    auto code = CODE;
    auto input = spectrum::from_hex(std::string{"1e010439"} + to_string(10)).value();
    auto table = MockTable();
    for (int i = 0; i < 200; ++i) {
        auto transaction = spectrum::Transaction(
            spectrum::EVMType::BASIC,
            evmc::address{0x1},
            evmc::address{0x2},
            std::span{code},
            std::span{input}
        );
        transaction.UpdateGetStorageHandler(
            [&](
                const evmc::address& addr, 
                const evmc::bytes32& key
            ){
                return table.GetStorage(addr, key);
            }
        );
        transaction.UpdateSetStorageHandler(
            [&](
                const evmc::address& addr, 
                const evmc::bytes32& key, 
                const evmc::bytes32& value
            ){
                table.SetStorage(addr, key, value);
                return evmc_storage_status::EVMC_STORAGE_ASSIGNED;
            }
        );
        auto result = transaction.Execute();
        ASSERT_EQ(result.status_code, 0) << result.status_code;
    }
}

TEST(Transaction, RunStrawman) {
    auto code = CODE;
    auto input = spectrum::from_hex(std::string{"1e010439"} + to_string(10)).value();
    auto table = MockTable();
    auto transaction = spectrum::Transaction(
        spectrum::EVMType::STRAWMAN,
        evmc::address{0x1},
        evmc::address{0x2},
        std::span{code},
        std::span{input}
    );
    for (int i = 0; i < 100; ++i) {
        transaction.UpdateGetStorageHandler(
            [&](
                const evmc::address& addr, 
                const evmc::bytes32& key
            ){
                transaction.MakeCheckpoint();
                return table.GetStorage(addr, key);
            }
        );
        transaction.UpdateSetStorageHandler(
            [&](
                const evmc::address& addr, 
                const evmc::bytes32& key, 
                const evmc::bytes32& value
            ){
                transaction.MakeCheckpoint();
                table.SetStorage(addr, key, value);
                return evmc_storage_status::EVMC_STORAGE_ASSIGNED;
            }
        );
        evmc_result result;
        // execute transaction for the first time, and expect it exits successfully
        result = transaction.Execute();
        ASSERT_EQ(result.status_code, 0) << result.status_code;
        // go to second checkpoint, and expect it to produce the same execution trace
        // therefore it should also exit successfully
        transaction.ApplyCheckpoint(1);
        result = transaction.Execute();
        ASSERT_EQ(result.status_code, 0) << result.status_code;
    }
}

TEST(Transaction, RunCopyOnWrite) {
    auto code = CODE;
    auto input = spectrum::from_hex(std::string{"1e010439"} + to_string(10)).value();
    auto table = MockTable();
    auto transaction = spectrum::Transaction(
        spectrum::EVMType::STRAWMAN,
        evmc::address{0x1},
        evmc::address{0x2},
        std::span{code},
        std::span{input}
    );
    for (int i = 0; i < 100; ++i) {
        transaction.UpdateGetStorageHandler(
            [&](
                const evmc::address& addr, 
                const evmc::bytes32& key
            ){
                transaction.MakeCheckpoint();
                return table.GetStorage(addr, key);
            }
        );
        transaction.UpdateSetStorageHandler(
            [&](
                const evmc::address& addr,
                const evmc::bytes32& key, 
                const evmc::bytes32& value
            ){
                transaction.MakeCheckpoint();
                table.SetStorage(addr, key, value);
                return evmc_storage_status::EVMC_STORAGE_ASSIGNED;
            }
        );
        evmc_result result;
        // execute transaction for the first time, and expect it exits successfully
        result = transaction.Execute();
        ASSERT_EQ(result.status_code, 0) << result.status_code;
        // go to second checkpoint, and expect it to produce the same execution trace
        // therefore it should also exit successfully
        transaction.ApplyCheckpoint(1);
        result = transaction.Execute();
        ASSERT_EQ(result.status_code, 0) << result.status_code;
    }
}

}

#undef CODE
