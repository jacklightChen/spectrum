#include <spectrum/common/evm_hash.hpp>
#include <spectrum/protocol/abstraction.hpp>
#include <spectrum/common/statistics.hpp>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/workload/abstraction.hpp>
#include <atomic>
#include <deque>
#include <chrono>
#include <functional>
#include <queue>
#include <thread>
#include <unordered_set>
#include <list>
#include <memory>

namespace spectrum {
#define K std::tuple<evmc::address, evmc::bytes32>
#define V evmc::bytes32
#define T CalvinTransaction

struct CalvinTransaction : public Transaction {

    SpinLock            mu;
    size_t              id;
    size_t              should_wait{0};
    Prediction          prediction{};
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    CalvinTransaction(Transaction &&inner, size_t id);
    void UpdateWait(size_t id);

};

struct CalvinLockEntry {
    T*                      tx;
    size_t                  version;
    std::unordered_set<T*>  readers;
};

struct CalvinLockQueue {

    std::list<CalvinLockEntry>  entries;
    std::unordered_set<T*>      readers_default;

};

struct CalvinLockTable: private Table<K, CalvinLockQueue, KeyHasher> {

    CalvinLockTable(size_t partitions);
    void Get(T* tx, const K& k);
    void Put(T* tx, const K& k);
    void Release(T* tx, const K& k);

};

using CalvinTable = Table<K, V, KeyHasher>;
using CalvinQueue = LockQueue<T>;
class CalvinExecutor;
class CalvinDispatch;

class Calvin : public Protocol {

    private:
    Workload&                   workload;
    Statistics&                 statistics;
    CalvinTable                 table;
    CalvinLockTable             lock_table;
    size_t                      num_dispatchers;
    size_t                      num_executors;
    std::atomic<bool>           stop_flag{false};
    std::atomic<size_t>         last_scheduled{1};
    std::atomic<size_t>         last_committed{0};
    std::atomic<size_t>         last_assigned{0};
    std::vector<CalvinQueue>    queue_bundle;
    std::vector<std::thread>    executors;
    std::vector<std::thread>    dispatchers;

    friend class CalvinExecutor;
    friend class CalvinDispatch;

    public:
    Calvin(Workload& workload, Statistics& statistics, size_t num_executors, size_t num_dispatchers, size_t table_partitions);
    void Start() override;
    void Stop() override;

};

class CalvinExecutor {

    private:
    CalvinTable&                table;
    CalvinQueue&                queue;
    Statistics&                 statistics;
    std::atomic<bool>&          stop_flag;
    std::atomic<size_t>&        last_committed;

    public:
    CalvinExecutor(Calvin& calvin, CalvinQueue& queue);
    void Run();

};

class CalvinDispatch {

    private:
    Workload&                   workload;
    std::atomic<bool>&          stop_flag;
    CalvinLockTable&            lock_table;
    std::atomic<size_t>&        last_scheduled;
    std::atomic<size_t>&        last_committed;
    std::atomic<size_t>&        last_assigned;
    std::vector<CalvinQueue>&   queue_bundle;

    public:
    CalvinDispatch(Calvin& calvin);
    void Run();

};

#undef K
#undef T

} // namespace spectrum
