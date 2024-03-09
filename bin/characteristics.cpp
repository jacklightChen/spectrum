#include <iostream>
#include <spectrum/hex.hpp>
#include <spectrum/workload.hpp>
#include <spectrum/workload-smallbank.hpp>
#include <ranges>
#include <string_view>
#include <fmt/core.h>
#include <iterator>
#include <stdexcept>
#include <chrono>
#include <glog/logging.h>
#include <glog/flags.h>
#include <string.h>
#include "argparse.hpp"
#include <evmc/evmc.h>
#include <evmc/evmc.hpp>

#define K std::tuple<evmc::address, evmc::bytes32>

class MockTable {

    private:
    std::unordered_map<K, evmc::bytes32, spectrum::KeyHasher> inner;
    std::unordered_map<K, std::vector<std::tuple<size_t, const char*>>, spectrum::KeyHasher> histogram;

    public:
    evmc::bytes32 GetStorage(
        size_t id,
        const evmc::address& addr, 
        const evmc::bytes32& key
    ) {
        histogram[std::make_tuple(addr, key)].push_back({id, "get"});
        return inner[std::make_tuple(addr, key)];
    }

    void SetStorage(
        size_t id,
        const evmc::address& addr, 
        const evmc::bytes32& key, 
        const evmc::bytes32& value
    ) {
        histogram[std::make_tuple(addr, key)].push_back({id, "put"});
        inner[std::make_tuple(addr, key)] = value;
    }

    std::unordered_map<K, std::string, spectrum::KeyHasher> Report(size_t tx_count) {
        auto collector_map = std::unordered_map<K, std::string, spectrum::KeyHasher>();
        for (const auto& entry: histogram) {
            auto j = size_t{0};
            auto k = std::get<0>(entry);
            auto v = std::get<1>(entry);
            auto collector = std::string();
            for (size_t i = 0; i != tx_count; i += 1) {
                if (j < v.size() && std::get<0>(v[j]) == i) {
                    collector.push_back(std::get<1>(v[j]) == std::string("get") ? '?' : '!');
                    j += 1;
                }
                else {
                    collector.push_back('-');
                }
            }
            collector_map[k] = collector;
        }
        return collector_map;
    }

};

using namespace spectrum;

int main(int argc, char* argv[]) {
    CHECK(argc == 3) << "We only expect 2 flags. ";
    auto mock_table = MockTable();
    auto statistics = std::make_unique<Statistics>();
    auto workload   = ParseWorkload(argv[1]);
    auto duration   = to<milliseconds>(argv[2]);
    auto stop_flag  = std::atomic<bool>();
    auto start_time = steady_clock::now();
    auto tx_count   = size_t{0};
    auto handle     = std::thread([&]() {
        while (!stop_flag.load()) {
            auto transaction = workload->Next();
            auto start_time  = steady_clock::now();
            transaction.UpdateGetStorageHandler([&](auto addr, auto key){
                return mock_table.GetStorage(tx_count, addr, key);
            });
            transaction.UpdateSetStorageHandler([&](auto addr, auto key, auto value){
                mock_table.SetStorage(tx_count, addr, key, value);
                return evmc_storage_status::EVMC_STORAGE_ASSIGNED;
            });
            transaction.Execute();
            statistics->JournalExecute();
            statistics->JournalCommit(duration_cast<milliseconds>(steady_clock::now() - start_time).count());
            ++tx_count;
        }
    });
    // wait for a given duration, stop running, and print statistics
    std::this_thread::sleep_for(duration);
    stop_flag.store(true); handle.join();
    std::cerr << "?:get !:put _:no-operation" << std::endl;
    std::cerr << statistics->PrintWithDuration(duration_cast<milliseconds>(steady_clock::now() - start_time));
    for (auto& entry: mock_table.Report(tx_count)) {
        auto& addr  = std::get<0>(std::get<0>(entry));
        auto& key   = std::get<1>(std::get<0>(entry));
        auto& hist  = std::get<1>(entry);
        std::cerr << to_hex(std::span{(uint8_t*)&addr, 20}) << ":" << to_hex(std::span{(uint8_t*)&key, 32}) << std::endl;
        std::cerr << hist << std::endl;
    }
}

#undef K