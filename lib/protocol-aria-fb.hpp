#include <functional>
#include <thread_pool/BS_thread_pool.hpp>
#include <thread_pool/BS_thread_pool_utils.hpp>
#include "lock-util.hpp"
#include "evm_hash.hpp"
#include "evm_transaction.hpp"
#include "workload.hpp"
#include "protocol.hpp"
#include "statistics.hpp"
#include <tuple>
#include <list>
#include <unordered_map>
#include <optional>
#include <atomic>
#include <memory>
#include <chrono>

namespace spectrum
{

#define K std::tuple<evmc::address, evmc::bytes32>
#define T AriaTransaction

/// @brief aria tranaction with local read and write set. 
struct AriaTransaction: public Transaction {
    size_t      id;
    size_t      batch_id;
    bool        flag_conflict{false};
    std::atomic<bool>   commited{false};
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    std::unordered_map<K, evmc::bytes32, KeyHasher>  local_put;
    std::unordered_map<K, evmc::bytes32, KeyHasher>  local_get;
    AriaTransaction(Transaction&& inner, size_t id, size_t batch_id);
};

/// @brief aria table entry for first round execution
struct AriaEntry {
    evmc::bytes32   value   = evmc::bytes32{0};
    size_t  batch_id_get    = 0;
    size_t  batch_id_put    = 0;
    T*      reserved_get_tx = nullptr;
    T*      reserved_put_tx = nullptr;
};

/// @brief aria table for first round execution
struct AriaTable: public Table<K, AriaEntry, KeyHasher> {
    void ReserveGet(T* tx, const K& k);
    void ReservePut(T* tx, const K& k);
    bool CompareReservedGet(T* tx, const K& k);
    bool CompareReservedPut(T* tx, const K& k);
};

/// @brief aria table entry for fallback pessimistic execution
struct AriaLockEntry {
    std::vector<T*>     deps_get;
    std::vector<T*>     deps_put;
};

/// @brief aria table for fallback pessimistic execution
struct AriaLockTable: public Table<K, AriaLockEntry, KeyHasher> {
    AriaLockTable(size_t partitions);
};

/// @brief aria protocol master class
class Aria: public Protocol {

    private:
    Statistics&         statistics;
    Workload&           workload;
    size_t              batch_size;
    // size_t              table_partitions;
    AriaTable           table;
    bool                enable_reordering;
    volatile std::atomic<bool>      stop_flag{false};
    volatile std::atomic<size_t>    tx_counter{0};
    BS::thread_pool     pool;
    void ParallelEach(
        std::function<void(T*)>             map, 
        std::vector<std::unique_ptr<T>>&    batch
    );
    std::unique_ptr<T> NextTransaction();

    public:
    Aria(Workload& workload, Statistics& statistics, size_t n_threads, size_t table_partitions, size_t batch_size, bool enable_reordering);
    void Start() override;
    void Stop() override;

};

/// @brief routines to be executed in various execution stages
struct AriaExecutor {
    static void Execute(T* tx, AriaTable& table);
    static void Reserve(T* tx, AriaTable& table);
    static void Verify(T* tx, AriaTable& table, bool enable_reordering);
    static void Commit(T* tx, AriaTable& table);
    static void PrepareLockTable(T* tx, AriaLockTable& lock_table);
    static void Fallback(T* tx, AriaTable& table, AriaLockTable& lock_table);
};

#undef K
#undef T

} // namespace spectrum
