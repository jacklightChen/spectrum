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
#include <conqueue/concurrentqueue.h>

namespace spectrum {

// some shorthands to prevent prohibitively long names
#define K std::tuple<evmc::address, evmc::bytes32>
#define V SpectrumVersionList
#define T SpectrumTransaction

struct SpectrumTransaction: public Transaction {
    size_t      id;
    size_t      should_wait;
    bool        first_run{false};
    std::chrono::time_point<std::chrono::steady_clock>  start_time;
    std::vector<std::tuple<K, evmc::bytes32, size_t>>   tuples_get{};
    std::vector<std::tuple<K, evmc::bytes32>>           tuples_put{};
    std::mutex       rerun_keys_mu;
    std::vector<K>   rerun_keys{false};
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
    std::mutex  mu;
    T*          tx = nullptr;
    std::list<SpectrumEntry> entries;
    // readers that read default value
    std::unordered_set<T*>  readers_default;
};

struct SpectrumTable: private Table<K, V, KeyHasher> {

    SpectrumTable(size_t partitions);
    void Get(T* tx, const K& k, evmc::bytes32& v, size_t& version);
    void Put(T* tx, const K& k, const evmc::bytes32& v);
    bool Lock(T* tx, const K& k);
    void RegretGet(T* tx, const K& k, size_t version);
    void RegretPut(T* tx, const K& k);
    void ClearGet(T* tx, const K& k, size_t version);
    void ClearPut(T* tx, const K& k);

};

class SpectrumExecutor;

using ConQueue = moodycamel::ConcurrentQueue<std::unique_ptr<T>>;

class Spectrum: virtual public Protocol {

    private:
    size_t              n_threads;
    Workload&           workload;
    ConQueue            queue;
    SpectrumTable       table;
    Statistics          statistics;
    std::atomic<size_t> last_execute{1};
    std::atomic<size_t> last_finalized{1};
    std::atomic<bool>   stop_flag{false};
    std::vector<SpectrumExecutor>       executors{};
    std::vector<std::thread>            threads{};
    friend class SpectrumExecutor;

    public:
    Spectrum(Workload& workload, size_t n_threads, size_t table_partitions);
    void Start() override;
    Statistics Stop() override;
    Statistics Report() override;

};

class SpectrumExecutor {


    private:
    Workload&               workload;
    ConQueue&               queue;
    SpectrumTable&          table;
    Statistics&             statistics;
    std::atomic<size_t>&    last_execute;
    std::atomic<size_t>&    last_finalized;
    std::atomic<bool>&      stop_flag;

    public:
    SpectrumExecutor(Spectrum& spectrum);
    void Run();

};

#undef T
#undef V
#undef K

} // namespace spectrum
