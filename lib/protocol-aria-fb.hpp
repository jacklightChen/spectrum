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
        std::function<void(T&)> map, 
        std::vector<AriaTransaction>& batch
    );

    public:
    void Start(size_t n_threads);

};

struct AriaTransaction: public Transaction {

    size_t     id;
    size_t     batch_id;
    bool       flag_conflict;
    std::unordered_map<K, evmc::bytes32, KeyHasher>  local_put;
    std::unordered_map<K, evmc::bytes32, KeyHasher>  local_get;
    AriaTransaction();

};

struct AriaEntry {
    // lock transactions sorted by id increasing
    std::list<T*>   lock_transactions   = std::list<T*>();
    // the value of this entry
    evmc::bytes32   value               = evmc::bytes32{0};
    // read and write reservation
    size_t  batch_id_get;
    size_t  batch_id_put;
    T*      reserved_get_tx = nullptr;
    T*      reserved_put_tx = nullptr;
};

struct AriaTable: public Table<K, AriaEntry, KeyHasher> {
    void ReserveGet(T* tx, const K& k);
    void ReservePut(T* tx, const K& k);
    bool CompareReservedGet(T* tx, const K& k);
    bool CompareReservedPut(T* tx, const K& k);
};

struct AriaExecutor {

    void Execute(T& tx, AriaTable& table);
    void Reserve(T& tx, AriaTable& table);
    void Verify(T& tx, AriaTable& table);
    void Commit(T& tx, AriaTable& table);
    void AcquireLock(T& tx);
    void ReleaseLock(T& tx);

};

#undef K
#undef T

} // namespace spectrum