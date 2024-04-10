#include <spectrum/workload/smallbank.hpp>
#include <spectrum/transaction/evm-hash.hpp>
#include <spectrum/common/hex.hpp>
#include <gtest/gtest.h>
#include <evmc/evmc.hpp>
#include <glog/logging.h>
#include <chrono>
#include <thread>
#include <spectrum/common/statistics.hpp>
#include <spectrum/common/glog-prefix.hpp>

namespace {

using namespace std::chrono_literals;
using namespace std::chrono;

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
    google::InstallPrefixFormatter(PrefixFormatter);
    auto workload    = spectrum::Smallbank(100000, 10.0);
    auto table       = MockTable();
    auto stop_flag   = std::atomic<bool>{false};
    auto statistics  = spectrum::Statistics();
    auto handle      = std::thread([&]() { while (!stop_flag.load()) {
        auto transaction = workload.Next();
        auto start_time  = steady_clock::now();
        transaction.InstallGetStorageHandler(
            [&](
                const evmc::address& addr, 
                const evmc::bytes32& key
            ){
                return table.GetStorage(addr, key);
            }
        );
        transaction.InstallSetStorageHandler(
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
        statistics.JournalExecute();
        statistics.JournalCommit(duration_cast<milliseconds>(steady_clock::now() - start_time).count());
        statistics.JournalMemory(transaction.mm_count);
    }});
    std::this_thread::sleep_for(100ms);
    stop_flag.store(true);
    handle.join();
    std::cerr << statistics.Print() << std::endl;
}

TEST(Smallbank, JustFetchWorkload) {
    auto workload    = spectrum::Smallbank(100000, 10.0);
    auto stop_flag   = std::atomic<bool>{false};
    auto statistics  = spectrum::Statistics();
    auto handle      = std::thread([&]() { while (!stop_flag.load()) {
        auto transaction = workload.Next();
        statistics.JournalExecute();
    }});
    std::this_thread::sleep_for(200ms);
    stop_flag.store(true);
    handle.join();
    std::cerr << statistics.PrintWithDuration(200ms) << std::endl;
}

// verify the transaction produce the same execution trace
TEST(Smallbank, RollbackReadSame) {
    auto workload = spectrum::Smallbank(1000, 1.0);
    for (size_t i = 0; i < 100; ++i) {
        auto transaction = workload.Next();
        auto checkpoints = std::vector<std::tuple<size_t, size_t>>();
        // set up get storage handler to track read keys
        transaction.InstallGetStorageHandler(
            [&](auto addr, auto key) {
                checkpoints.push_back(std::tuple{
                    transaction.MakeCheckpoint(), 
                    spectrum::KeyHasher()({addr, key})
                });
                return key;
            }
        );
        transaction.InstallSetStorageHandler(
            [&](auto addr, auto key, auto value) {
                return evmc_storage_status::EVMC_STORAGE_ASSIGNED;
            }
        );
        transaction.Execute();
        if (i >= checkpoints.size()) continue;
        // rollback and check if we read the same keys in the second execution
        auto checkpoint_id = std::get<0>(checkpoints[i]);
        auto second_record = std::vector<std::tuple<size_t, size_t>>();
        transaction.ApplyCheckpoint(checkpoint_id);
        transaction.InstallGetStorageHandler(
            [&](auto addr, auto key) {
                second_record.push_back(std::tuple{
                    transaction.MakeCheckpoint(), 
                    spectrum::KeyHasher()({addr, key})
                });
                return key;
            }
        );
        for (auto j = 0; j < second_record.size(); ++j) {
            ASSERT_EQ(second_record[j], checkpoints[j + i]);
        }
    }
}

}