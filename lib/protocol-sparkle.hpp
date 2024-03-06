#include "./workload.hpp"
#include "./table.hpp"
#include "./protocol.hpp"
#include "./evm_hash.hpp"
#include <list>
#include <atomic>
#include <tuple>
#include <vector>
#include <unordered_set>
#include <thread>

namespace spectrum {

// some shorthands to prevent prohibitively long names
#define K std::tuple<evmc::address, evmc::bytes32>
#define V SparkleVersionList
#define T SparkleTransaction

struct SparkleTransaction: public Transaction {
    size_t      id;
    std::vector<std::tuple<K, evmc::bytes32, size_t>>   tuples_get{};
    std::vector<std::tuple<K, evmc::bytes32>>           tuples_put{};
    std::atomic<bool>   rerun_flag{false};
    SparkleTransaction(Transaction&& inner, size_t id);
    void Reset();
};

struct SparkleEntry {
    evmc::bytes32   value;
    size_t          version;
    // we store raw pointers here because when a transaction is destructed, it always removes itself from table. 
    std::unordered_set<T*>  readers;
};

struct SparkleVersionList {
    std::mutex  mu;
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

class SparkleExecutor;

class Sparkle: virtual public Protocol {

    private:
    size_t              n_threads;
    Workload&           workload;
    SparkleTable        table;
    Statistics          statistics;
    volatile std::atomic<size_t> last_execute{1};
    volatile std::atomic<size_t> last_finalized{0};
    volatile std::atomic<bool>   stop_flag{false};
    std::vector<SparkleExecutor>    executors{};
    std::vector<std::thread>        threads{};
    friend class SparkleExecutor;

    public:
    Sparkle(Workload& workload, size_t n_threads, size_t table_partitions);
    void Start() override;
    Statistics Stop() override;
    Statistics Report() override;

};

class SparkleExecutor {

    private:
    Workload&               workload;
    SparkleTable&           table;
    Statistics&             statistics;
    volatile std::atomic<size_t>&    last_execute;
    volatile std::atomic<size_t>&    last_finalized;
    volatile std::atomic<bool>&      stop_flag;

    public:
    SparkleExecutor(Sparkle& sparkle);
    void Run();

};

#undef T
#undef V
#undef K

} // namespace spectrum
