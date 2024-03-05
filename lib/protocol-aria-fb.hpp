#include <functional>
#include <thread_pool/BS_thread_pool.hpp>
#include <thread_pool/BS_thread_pool_utils.hpp>
#include "table.hpp"
#include "evm_hash.hpp"
#include "evm_transaction.hpp"
#include "workload.hpp"
#include "protocol.hpp"
#include "statistics.hpp"
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

struct AriaEntry {
    evmc::bytes32   value   = evmc::bytes32{0};
    size_t  batch_id_get    = 0;
    size_t  batch_id_put    = 0;
    T*      reserved_get_tx = nullptr;
    T*      reserved_put_tx = nullptr;
};

struct AriaTable: public Table<K, AriaEntry, KeyHasher> {
    void ReserveGet(T* tx, const K& k);
    void ReservePut(T* tx, const K& k);
    bool CompareReservedGet(T* tx, const K& k);
    bool CompareReservedPut(T* tx, const K& k);
};

struct AriaLockEntry {
    
};

struct AriaLockTable: public Table<K, AriaEntry, KeyHasher> {

};

class Aria: virtual public Protocol {

    private:
    Statistics          statistics;
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
    void Start() override;
    Statistics Stop() override;

};

struct AriaExecutor {
    static void Execute(T& tx, AriaTable& table);
    static void Reserve(T& tx, AriaTable& table);
    static void Verify(T& tx, AriaTable& table);
    static void Commit(T& tx, AriaTable& table);
    static void AcquireLock(T& tx, AriaTable& table);
    static void ReleaseLock(T& tx, AriaTable& table);
};

#undef K
#undef T

} // namespace spectrum