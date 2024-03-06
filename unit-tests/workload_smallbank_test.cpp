#include <spectrum/workload-smallbank.hpp>
#include <gtest/gtest.h>

namespace {

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

TEST(Smallbank, JustRunWorkload) {
    auto workload    = Smallbank();
    auto table       = MockTable();
    for (size_t i = 0; i < 1000; ++i) {
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
    }
}

}
