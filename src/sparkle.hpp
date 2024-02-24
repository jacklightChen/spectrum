#include"./workload.hpp"
#include"./table.hpp"
#include<list>
#include<atomic>
#include<tuple>
#include<vector>
#include<unordered_set>

namespace spectrum {

#define K std::tuple<evmc::address, evmc::bytes32>
#define V SparkleValue
#define T SparkleTransaction

struct KeyHasher {
    size_t operator()(const K& key) const;
};

struct SparkleTransaction: Transaction {
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
    std::unordered_set<T*>  readers;
};

struct SparkleValue {
    std::mutex  mu;
    T*          tx = nullptr;
    std::list<SparkleEntry> entries;
};

class SparkleTable: Table<K, V, KeyHasher> {
    public:
    SparkleTable(size_t partitions);
    void Get(T* tx, const K& k, evmc::bytes32& v, size_t& version);
    void Put(T* tx, const K& k, const evmc::bytes32& v);
    bool Lock(T* tx, const K& k);
    void RegretGet(T* tx, const K& k, size_t version);
    void RegretPut(T* tx, const K& k);
    void ClearGet(T* tx, const K& k, size_t version);
    void ClearPut(T* tx, const K& k);
};

class Sparkle {
    public:
    Workload&           workload;
    SparkleTable        table;
    std::atomic<size_t> last_execute{1};
    std::atomic<size_t> last_commit{1};
    std::atomic<bool>   stop_flag{false};
    Sparkle(Workload& workload, size_t table_partitions);
};

class SparkleExecutor {
    Workload&               workload;
    SparkleTable&           table;
    std::atomic<size_t>&    last_execute;
    std::atomic<size_t>&    last_commit;
    std::atomic<bool>&      stop_flag;
    SparkleExecutor(Sparkle& sparkle);
    void Run();
};

#undef T
#undef V
#undef K

}
