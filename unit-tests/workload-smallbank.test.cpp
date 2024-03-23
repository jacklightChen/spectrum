#include <spectrum/workload-smallbank.hpp>
#include <spectrum/evm_hash.hpp>
#include <spectrum/hex.hpp>
#include <gtest/gtest.h>
#include <evmc/evmc.hpp>
#include <glog/logging.h>
#include <chrono>
#include <thread>
#include <spectrum/statistics.hpp>
#include "glog-prefix-install.hpp"

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
    GLOG_PREFIX;
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
    }});
    std::this_thread::sleep_for(1000ms);
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
    std::this_thread::sleep_for(2000ms);
    stop_flag.store(true);
    handle.join();
    std::cerr << statistics.PrintWithDuration(2000ms) << std::endl;
}

}
