#include <functional>
#include <thread_pool/BS_thread_pool.hpp>
#include <thread_pool/BS_thread_pool_utils.hpp>
#include "table.hpp"
#include "evm_hash.hpp"
#include "evm_transaction.hpp"
#include "workload.hpp"
#include <tuple>
#include <list>
#include <unordered_map>

namespace spectrum
{

#define K std::tuple<evmc::address, evmc::bytes32>
#define T AriaTransaction

class AriaTransaction;

class Aria {

    private:
    Workload&       workload;
    BS::thread_pool pool;
    void ParallelEach(
        std::function<void(AriaTransaction&)> map, 
        std::vector<AriaTransaction>& batch
    );

    public:
    void Start(size_t n_threads);

};

struct AriaTransaction: public Transaction {

    std::unordered_map<K, evmc::bytes32, KeyHasher>  local_put;
    std::unordered_map<K, evmc::bytes32, KeyHasher>  local_get;
    AriaTransaction();

};

struct AriaEntry {
    // lock transactions sorted by id increasing
    std::list<T*>   lock_transactions   = std::list<T*>();
    // the value of this entry
    evmc::bytes32   value               = evmc::bytes32{0};
};

class AriaTable: public Table<K, AriaEntry, KeyHasher> {

};

class AriaExecutor {

    public:
    void Execute(AriaTransaction& tx, AriaTable& table);
    void Reserve(AriaTransaction& tx);
    void Commit(AriaTransaction& tx);
    void AcquireLock(AriaTransaction& tx);
    void ReleaseLock(AriaTransaction& tx);

};

#undef K
#undef T

} // namespace spectrum