#include "lock-util.hpp"
#include "protocol.hpp"
#include "workload.hpp"
#include "statistics.hpp"
#include "evm_hash.hpp"
#include <thread>
#include <vector>
#include <atomic>

namespace spectrum {

#define K std::tuple<evmc::address, evmc::bytes32>

using DummyTable = Table<K, evmc::bytes32, KeyHasher>;

class Dummy: public Protocol {

    private:
    DummyTable                  table;
    std::atomic<bool>           stop_flag{false};
    Workload&                   workload;
    size_t                      n_threads;
    Statistics&                 statistics;
    std::vector<std::thread>    executors;

    public:
    Dummy(Workload& workload, Statistics& statistics, size_t n_threads, size_t table_partitions, EVMType evm_type);
    void Start() override;
    void Stop()  override;

};

} // namespace spectrum
