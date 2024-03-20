#include "./workload.hpp"
#include "./table.hpp"
#include "./protocol.hpp"
#include "./evm_hash.hpp"
#include "./lock_queue.hpp"
#include <list>
#include <atomic>
#include <tuple>
#include <vector>
#include <unordered_set>
#include <thread>
#include <queue>
#include <optional>

namespace spectrum {

// some shorthands to prevent prohibitively long names
#define K std::tuple<evmc::address, evmc::bytes32>
#define V SpectrumVersionList
#define T SpectrumTransaction

using namespace std::chrono;

struct SpectrumPutTuple {
    K               key;
    evmc::bytes32   value;
    bool            is_committed;
};

struct SpectrumGetTuple {
    K               key;
    evmc::bytes32   value;
    size_t          version;
    size_t          tuples_put_len;
    size_t          checkpoint_id;
};

struct SpectrumTransaction: public Transaction {
    size_t      id;
    size_t      should_wait{0};
    std::mutex          rerun_keys_mu;
    std::vector<K>      rerun_keys;
    std::atomic<bool>   berun_flag{false};
    time_point<steady_clock>            start_time;
    std::vector<SpectrumGetTuple>       tuples_get{};
    std::vector<SpectrumPutTuple>       tuples_put{};
    SpectrumTransaction(Transaction&& inner, size_t id);
    bool HasRerunKeys();
    void AddRerunKeys(const K& key, size_t cause_id);
};

struct SpectrumEntry {
    evmc::bytes32   value;
    size_t          version;
    // we store raw pointers here because when a transaction is destructed, it always removes itself from table. 
    std::unordered_set<T*>  readers;
};

struct SpectrumVersionList {
    T*          tx = nullptr;
    std::list<SpectrumEntry> entries;
    // readers that read default value
    std::unordered_set<T*>  readers_default;
};

struct SpectrumTable: private Table<K, V, KeyHasher> {

    SpectrumTable(size_t partitions);
    void Get(T* tx, const K& k, evmc::bytes32& v, size_t& version);
    void Put(T* tx, const K& k, const evmc::bytes32& v);
    void RegretGet(T* tx, const K& k, size_t version);
    void RegretPut(T* tx, const K& k);
    void ClearGet(T* tx, const K& k, size_t version);
    void ClearPut(T* tx, const K& k);

};

using SpectrumQueue = LockPriorityQueue<T>;
class SpectrumExecutor;
class SpectrumDispatch;

class Spectrum: public Protocol {

    private:
    size_t              n_executors;
    size_t              n_dispatchers;
    Workload&           workload;
    SpectrumTable       table;
    Statistics&         statistics;
    EVMType             evm_type;
    std::atomic<size_t> last_execute{1};
    std::atomic<size_t> last_finalized{0};
    std::atomic<bool>   stop_flag{false};
    std::vector<SpectrumQueue>  queue_bundle;
    std::vector<std::thread>    executors{};
    std::vector<std::thread>    dispatchers{};
    friend class SpectrumExecutor;
    friend class SpectrumDispatch;

    public:
    Spectrum(Workload& workload, Statistics& statistics, size_t n_executors, size_t n_dispatchers, size_t table_partitions, EVMType evm_type);
    void Start() override;
    void Stop() override;

};

class SpectrumExecutor {

    private:
    SpectrumQueue&          queue;
    SpectrumTable&          table;
    Statistics&             statistics;
    std::atomic<size_t>&    last_finalized;
    std::atomic<bool>&      stop_flag;

    public:
    SpectrumExecutor(Spectrum& spectrum, SpectrumQueue& queue);
    std::unique_ptr<T> Create();
    void ReExecute(SpectrumTransaction* tx);
    void Run();

};

class SpectrumDispatch {

    private:
    Workload&                   workload;
    std::atomic<size_t>&        last_execute;
    std::vector<SpectrumQueue>& queue_bundle;
    std::atomic<bool>&          stop_flag;

    public:
    SpectrumDispatch(Spectrum& spectrum);
    void Run();

};

#undef T
#undef V
#undef K

} // namespace spectrum
