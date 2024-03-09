#include "protocol-serial.hpp"
#include <chrono>

namespace spectrum {

using namespace std::chrono;

Serial::Serial(Workload& workload, Statistics& statistics, EVMType evm_type, size_t repeat):
    workload{workload},
    statistics{statistics},
    // evm_type{evm_type},
    repeat{repeat}
{
    workload.SetEVMType(evm_type);
}

void Serial::Start() {
    thread = new std::thread([&]() { while (!stop_flag.load()) {
        auto transaction = workload.Next();
        auto start_time  = steady_clock::now();
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
        for (size_t i = 0; i < repeat; ++i) {
            transaction.Execute();
            statistics.JournalExecute();
            statistics.JournalCommit(duration_cast<milliseconds>(steady_clock::now() - start_time).count());
        }
    }});
}

void Serial::Stop() {
    stop_flag.store(true);
    thread->join();
    delete thread;
    thread = nullptr;
}

evmc::bytes32 SerialTable::GetStorage(const evmc::address& addr, const evmc::bytes32& key) {
    return inner[std::make_tuple(addr, key)];
}

void SerialTable::SetStorage(const evmc::address& addr, const evmc::bytes32& key, const evmc::bytes32& value) {
    inner[std::make_tuple(addr, key)] = value;
}

} // namespace spectrum
