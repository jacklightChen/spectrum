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
#include <optional>

namespace spectrum
{

#define K std::tuple<evmc::address, evmc::bytes32>
#define T AriaTransaction


struct AriaTransaction: public Transaction {
    size_t     id;
    size_t     batch_id;
    bool       flag_conflict{false};
    std::unordered_map<K, evmc::bytes32, KeyHasher>  local_put;
    std::unordered_map<K, evmc::bytes32, KeyHasher>  local_get;
    AriaTransaction(Transaction&& inner, size_t id, size_t batch_id);
    void Reset();
};

struct AriaTable: public Table<K, AriaEntry, KeyHasher> {
    void ReserveGet(T* tx, const K& k);
    void ReservePut(T* tx, const K& k);
    bool CompareReservedGet(T* tx, const K& k);
    bool CompareReservedPut(T* tx, const K& k);
};

class Aria {

    private:
    Workload&           workload;
    size_t              batch_size;
    AriaTable           table;
    std::atomic<bool>   stop_flag;
    std::atomic<size_t> transaction_counter;
    std::unique_ptr<BS::thread_pool> pool;
    void ParallelEach(
        std::function<void(T&)>         map, 
        std::vector<std::optional<T>>&  batch
    );
    T NextTransaction();

    public:
    void Start();

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

struct AriaExecutor {
    void Execute(T& tx, AriaTable& table);
    void Reserve(T& tx, AriaTable& table);
    void Verify(T& tx, AriaTable& table);
    void Commit(T& tx, AriaTable& table);
    void AcquireLock(T& tx, AriaTable& table);
    void ReleaseLock(T& tx, AriaTable& table);
};

#undef K
#undef T

} // namespace spectrum