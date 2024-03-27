#include "./protocol-calvin.hpp"
#include "./hex.hpp"
#include "./thread-util.hpp"
#include <algorithm>
#include <string>
#include <vector>

namespace spectrum {
#define K std::tuple<evmc::address, evmc::bytes32>
#define V evmc::bytes32
#define T CalvinTransaction

using namespace std::chrono;

/// @brief wrap a transaction into calvin transaction
/// @param inner inner transaction
/// @param id the schedule id of this transaction
CalvinTransaction::CalvinTransaction(Transaction&& inner, size_t id):
    Transaction{std::move(inner)},
    id{id},
    start_time{std::chrono::steady_clock::now()}
{
    this->Analyze(this->prediction);
}

void CalvinTransaction::UpdateWait(size_t _id) {
    auto guard = Guard{mu};
    if (_id < id) { 
        should_wait = std::max(_id, should_wait);
    }
}

/// @brief the multi-version lock table for calvin
/// @param partitions the number of partitions
CalvinLockTable::CalvinLockTable(size_t partitions):
    Table<K, CalvinLockQueue, KeyHasher>{partitions}
{}

/// @brief remove versions preceeding current transaction
/// @param tx the transaction the previously wrote this entry
/// @param k the key of written entry
void CalvinLockTable::Release(T* tx, const K& k) {
    DLOG(INFO) << "clear put " << tx->id << std::endl;
    Table::Put(k, [&](auto& _v) {
        for (auto& entry: _v.entries) {
            entry.readers.erase(tx);
        }
        _v.readers_default.erase(tx);
        while (_v.entries.size() && _v.entries.front().version <= tx->id) {
            _v.entries.pop_front();
        }
    });
}

/// @brief append a read request to lock table
/// @param tx the transaction that reads the value
/// @param k the key of the read entry
void CalvinLockTable::Get(T* tx, const K& k) {
    DLOG(INFO) << tx->id << " get";
    Table::Put(k, [&](auto& _v) {
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        while (rit != end) {
            if (rit->version > tx->id) {
                ++rit; continue;
            }
            rit->readers.insert(tx);
            rit->tx->UpdateWait(tx->id);
            if (rit != _v.entries.rbegin()) {
                (--rit)->tx->UpdateWait(tx->id);
            }
            return;
        }
        DLOG(INFO) << "default";
        _v.readers_default.insert(tx);
    });
}

/// @brief append a write request to lock table
/// @param tx the transaction the writes the value
/// @param k the key of write entry
void CalvinLockTable::Put(T* tx, const K& k) {
    DLOG(INFO) << tx->id << " put";
        Table::Put(k, [&](auto& _v) {
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        auto readers_ = std::unordered_set<T*>();
        // search from insertion position
        while (rit != end) {
            if (rit->version > tx->id) {
                ++rit; continue;
            }
            // reset transactions' should_wait that read outdated keys
            for (auto it = rit->readers.begin(); it != rit->readers.end();) {
                if ((*it)->id >= tx->id) {
                    readers_.insert(*it);
                    (*it)->UpdateWait(tx->id);
                    // this one is very tricky. we **must** use `it++` here.  
                    rit->readers.erase(it++);
                }
                else {
                    tx->UpdateWait((*it)->id);
                    ++it;
                }
            }
            break;
        }
        for (auto it = _v.readers_default.begin(); it != _v.readers_default.end();) {
            if ((*it)->id >= tx->id) {
                readers_.insert(*it);
                (*it)->UpdateWait(tx->id);
                _v.readers_default.erase(it++);
            }
            else {
                tx->UpdateWait((*it)->id);
                ++it;
            }
        }
        // insert an entry, with readers exempted from previous version
        _v.entries.insert(rit.base(), {
            .tx      = tx,
            .version = tx->id,
            .readers = readers_
        });
    });
}

Calvin::Calvin(Workload& workload, Statistics& statistics, size_t n_executors, size_t num_dispatchers, size_t table_partitions):
    workload{workload},
    statistics{statistics},
    n_executors{n_executors},
    num_dispatchers{num_dispatchers},
    queue_bundle(n_executors),
    table{table_partitions},
    lock_table{table_partitions}
{
    LOG(INFO) << fmt::format("Calvin(n_executors={}, num_dispatchers={}, n_table_partitions={})", n_executors, num_dispatchers, table_partitions);
    workload.SetEVMType(EVMType::BASIC);
}

/// @brief start calvin protocol
void Calvin::Start() {
    stop_flag.store(false);
    for (size_t i = 0; i != n_executors; ++i) {
        DLOG(INFO) << "start executor " << i << std::endl;
        auto queue = &queue_bundle[i];
        executors.push_back(std::thread([this, queue] {
            CalvinExecutor(*this, *queue).Run();
        }));
        PinRoundRobin(executors[i], i + num_dispatchers);
    }
    for (size_t i = 0; i != num_dispatchers; ++i) {
        DLOG(INFO) << "start dispatcher " << i << std::endl;
        dispatchers.push_back(std::thread([this] {
            CalvinDispatch(*this).Run();
        }));
        PinRoundRobin(dispatchers[i], i);
    }
}

/// @brief stop calvin protocol
void Calvin::Stop() {
    stop_flag.store(true);
    for (auto& x: dispatchers)  { x.join(); }
    for (auto& x: executors)    { x.join(); }
}

/// @brief initialize a calvin executor
/// @param calvin the calvin instance with configuration
/// @param queue the queue of transactions
CalvinExecutor::CalvinExecutor(Calvin& calvin, CalvinQueue& queue):
    queue{queue},
    stop_flag{calvin.stop_flag},
    last_committed{calvin.last_committed},
    table{calvin.table},
    statistics{calvin.statistics}
{}

/// @brief run calvin executor until stop_flag modified to true
void CalvinExecutor::Run() {while(!stop_flag.load()) {
    auto tx = queue.Pop();
    if (tx == nullptr) { continue; }
    DLOG(INFO) << "execute " << tx->id << std::endl;
    tx->InstallGetStorageHandler([&](auto& address, auto& key) {
        V v; table.Get({address, key}, [&](auto& _v) { v = _v; });
        return v;
    });
    tx->InstallSetStorageHandler([&](auto& address, auto& key, auto& v) {
        table.Put({address, key}, [&](auto& _v) { _v = v; });
        return evmc_storage_status::EVMC_STORAGE_MODIFIED;
    });
    tx->Execute();
    statistics.JournalExecute();
    statistics.JournalCommit(duration_cast<microseconds>(steady_clock::now() - tx->start_time).count());
    last_committed.fetch_add(1, std::memory_order_seq_cst);
}}

/// @brief dispatch a transaction
/// @param calvin the calvin instance with configuration
CalvinDispatch::CalvinDispatch(Calvin& calvin):
    workload{calvin.workload},
    stop_flag{calvin.stop_flag},
    queue_bundle{calvin.queue_bundle},
    lock_table{calvin.lock_table},
    last_scheduled{calvin.last_scheduled},
    last_assigned{calvin.last_assigned},
    last_committed{calvin.last_committed}
{}

/// @brief run a calvin dispatcher
void CalvinDispatch::Run() {while(!stop_flag.load()) {
    // generate and analyze the transaction
    auto tx = std::make_unique<T>(workload.Next(), last_scheduled.fetch_add(1, std::memory_order_seq_cst));
    // make get/put requests in lock table
    DLOG(INFO) << tx->id << " fetch locks" << std::endl;
    for (auto& k: tx->prediction.get) {
        lock_table.Get(tx.get(), k);
    }
    for (auto& k: tx->prediction.put) {
        lock_table.Put(tx.get(), k);
    }
    // wait until the should_wait to finalize
    while (tx->id != last_assigned.load() + 1) {
        DLOG(INFO) << tx->id << " wait last assigned" << std::endl;
    }
    last_assigned.fetch_add(1, std::memory_order_seq_cst);
    DLOG(INFO) << tx->id << " release locks" << std::endl;
    for (auto& k: tx->prediction.get) {
        lock_table.Release(tx.get(), k);
    }
    for (auto& k: tx->prediction.put) {
        lock_table.Release(tx.get(), k);
    }
    // now we have the real should_wait, so we wait until it can be executed
    while (true) {
        auto guard = Guard{tx->mu}; 
        DLOG(INFO) << tx->id << " wait " << tx->should_wait << std::endl;
        if (tx->should_wait <= last_committed.load()) break;
    }
    queue_bundle[tx->id % queue_bundle.size()].Push(std::move(tx));
}}

#undef K
#undef V
#undef T

} // namespace spectrum
