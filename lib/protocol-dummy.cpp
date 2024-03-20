#include "protocol-dummy.hpp"
#include <fmt/core.h>

namespace spectrum {

using namespace std::chrono;

Dummy::Dummy(Workload& workload, Statistics& statistics, size_t n_threads, size_t table_partitions, EVMType evm_type):
    workload{workload},
    statistics{statistics},
    n_threads{n_threads},
    table(table_partitions)
{
    LOG(INFO) << fmt::format("Dummy(n_threads={}, table_partitions={})", n_threads, table_partitions) << std::endl;
    workload.SetEVMType(evm_type);
}

void Dummy::Start() {
    for (size_t i = 0; i < n_threads; ++i) {executors.push_back(std::thread([this]() {while(!stop_flag.load()) {
        auto tx         = workload.Next();
        auto start_time = steady_clock::now();
        tx.UpdateGetStorageHandler([&](auto& address, auto& key) {
            evmc::bytes32 v; table.Get({address, key}, [&](auto& _v) { v = _v; });
            return v;
        });
        tx.UpdateSetStorageHandler([&](auto& address, auto& key, auto& v) {
            table.Put({address, key}, [&](auto& _v) { _v = v; });
            return evmc_storage_status::EVMC_STORAGE_MODIFIED;
        });
        tx.Execute();
        statistics.JournalExecute();
        statistics.JournalCommit(duration_cast<microseconds>(steady_clock::now() - start_time).count());
    }}));}
}

void Dummy::Stop() {
    stop_flag.store(true);
    for (auto& x: executors) { x.join(); }
}

} // namespace spectrum
