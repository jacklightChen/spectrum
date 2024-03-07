#include <spectrum/workload-smallbank.hpp>
#include <spectrum/evm_hash.hpp>
#include <spectrum/hex.hpp>
#include <gtest/gtest.h>
#include <evmc/evmc.hpp>
#include <glog/logging.h>
#include <chrono>

namespace {

using std::chrono_literals;

class MockTable {

    private:
    std::unordered_map<std::tuple<evmc::address, evmc::bytes32>, evmc::bytes32, spectrum::KeyHasher> inner;

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

TEST(Smallbank, JustRunWorkload) {
    auto workload    = spectrum::Smallbank();
    auto table       = MockTable();
    auto stop_flag   = std::atomic<bool>{false};
    auto handle      = std::thread([&]() { while (!stop_flag.load()) {
        auto transaction = workload.Next();
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
        transaction.Execute();
    }});
    std::thread::this_thread::sleep(2000ms);
    stop_flag.store(true);
    handle.join();
}

}
