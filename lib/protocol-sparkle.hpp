#include "./workload.hpp"
#include "lock-util.hpp"
#include "./protocol.hpp"
#include "./evm_hash.hpp"
#include <list>
#include <atomic>
#include <tuple>
#include <vector>
#include <unordered_set>
#include <chrono>
#include <thread>
#include <thread_pool/BS_thread_pool.hpp>
#include <thread_pool/BS_thread_pool_utils.hpp>

namespace spectrum {

using namespace std::chrono;

// some shorthands to prevent prohibitively long names
#define K std::tuple<evmc::address, evmc::bytes32>
#define V SparkleVersionList
#define T SparkleTransaction

struct SparkleTransaction: public Transaction {
    size_t      id;
    size_t      execution_count{0};
    std::vector<std::tuple<K, evmc::bytes32, size_t>>   tuples_get{};
    std::vector<std::tuple<K, evmc::bytes32>>           tuples_put{};
    std::atomic<bool>   rerun_flag{false};
    std::atomic<bool>   berun_flag{false};
    time_point<steady_clock>    start_time;
    SparkleTransaction(Transaction&& inner, size_t id);
};

struct SparkleEntry {
    evmc::bytes32   value;
    size_t          version;
    // we store raw pointers here because when a transaction is destructed, it always removes itself from table. 
    std::unordered_set<T*>  readers;
};

struct SparkleVersionList {
    T*          tx = nullptr;
    std::list<SparkleEntry> entries;
    // readers that read default value
    std::unordered_set<T*>  readers_default;
};

struct SparkleTable: private Table<K, V, KeyHasher> {

    SparkleTable(size_t partitions);
    void Get(T* tx, const K& k, evmc::bytes32& v, size_t& version);
    void Put(T* tx, const K& k, const evmc::bytes32& v);
    bool Lock(T* tx, const K& k);
    void RegretGet(T* tx, const K& k, size_t version);
    void RegretPut(T* tx, const K& k);
    void ClearGet(T* tx, const K& k, size_t version);
    void ClearPut(T* tx, const K& k);

};

using SparkleQueue = LockPriorityQueue<T>;
class SparkleDispatch;
class SparkleExecutor;

class Sparkle: public Protocol {

    private:
    size_t              n_executors;
    size_t              n_dispatchers;
    Workload&           workload;
    SparkleTable        table;
    Statistics&         statistics;
    std::atomic<size_t> last_execute{1};
    std::atomic<size_t> last_finalized{0};
    std::atomic<bool>   stop_flag{false};
    std::vector<SparkleQueue>   queue_bundle;
    std::vector<std::thread>    executors{};
    std::vector<std::thread>    dispatchers{};
    friend class SparkleDispatch;
    friend class SparkleExecutor;

    public:
    Sparkle(Workload& workload, Statistics& statistics, size_t n_executors, size_t n_dispatchers, size_t table_partitions);
    void Start() override;
    void Stop() override;

};

/// @brief a dispatcher that sends transactions to executors in a round-robin manner
class SparkleDispatch {

    private:
    Workload&                   workload;
    std::atomic<size_t>&        last_execute;
    std::vector<SparkleQueue>&  queue_bundle;
    std::atomic<bool>&          stop_flag;

    public:
    SparkleDispatch(Sparkle& sparkle);
    void Run();

};

/// @brief an executor that fetch transactions from queue and execute them
class SparkleExecutor {

    private:
    SparkleQueue&           queue;
    SparkleTable&           table;
    Statistics&             statistics;
    std::atomic<size_t>&    last_finalized;
    std::atomic<bool>&      stop_flag;

    public:
    SparkleExecutor(Sparkle& sparkle, SparkleQueue& queue);
    std::unique_ptr<T> Create();
    void ReExecute(SparkleTransaction* tx);
    void Run();

};

#undef T
#undef V
#undef K

} // namespace spectrum
