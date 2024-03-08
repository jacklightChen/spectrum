#include "protocol.hpp"
#include "workload.hpp"
#include "statistics.hpp"
#include "evm_hash.hpp"
#include <thread>
#include <atomic>

namespace spectrum {

class SerialTable {

    private:
    std::unordered_map<std::tuple<evmc::address, evmc::bytes32>, evmc::bytes32, KeyHasher> inner;

    public:
    evmc::bytes32 GetStorage(const evmc::address& addr, const evmc::bytes32& key);
    void SetStorage(const evmc::address& addr, const evmc::bytes32& key, const evmc::bytes32& value);

};

class Serial: public Protocol {

    private:
    EVMType             evm_type;
    SerialTable         table;
    std::thread*        thread{nullptr};
    std::atomic<bool>   stop_flag{false};
    Workload&           workload;
    Statistics&         statistics;

    public:
    Serial(Workload& workload, Statistics& statistics, EVMType evm_type);
    void Start() override;
    void Stop()  override;

};

} // namespace spectrum