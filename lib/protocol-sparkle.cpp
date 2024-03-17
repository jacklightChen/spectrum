#include "./protocol-sparkle.hpp"
#include "./table.hpp"
#include "./hex.hpp"
#include <functional>
#include <thread>
#include <chrono>
#include <glog/logging.h>
#include <fmt/core.h>

/*
    This is a implementation of "Sparkle: Speculative Deterministic Concurrency Control for Partially Replicated Transactional Data Stores" (Zhongmiao Li, Peter Van Roy and Paolo Romano). 
 */

namespace spectrum {

using namespace std::chrono;

#define K std::tuple<evmc::address, evmc::bytes32>
#define V SparkleVersionList
#define T SparkleTransaction

/// @brief wrap a base transaction into a sparkle transaction
/// @param inner the base transaction
/// @param id transaction id
SparkleTransaction::SparkleTransaction(Transaction&& inner, size_t id):
    Transaction{std::move(inner)},
    id{id}
{}

/// @brief reset the transaction (local) information
void SparkleTransaction::Reset() {
    tuples_get.resize(0);
    tuples_put.resize(0);
    rerun_flag.store(false);
}

/// @brief the multi-version table for sparkle
/// @param partitions the number of partitions
SparkleTable::SparkleTable(size_t partitions):
    Table<K, V, KeyHasher>{partitions}
{}

/// @brief get a value
/// @param tx the transaction that reads the value
/// @param k the key of the read entry
/// @param v (mutated to be) the value of read entry
/// @param version (mutated to be) the version of read entry
void SparkleTable::Get(T* tx, const K k, evmc::bytes32& v, size_t& version) {
    DLOG(INFO) << tx->id << " get";
    Table::Put(k, [&](V& _v) {
        auto guard = std::lock_guard{_v.mu};
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        while (rit != end) {
            if (rit->version > tx->id) {
                ++rit; continue;
            }
            v = rit->value;
            version = rit->version;
            rit->readers.insert(tx);
            return;
        }
        DLOG(INFO) << "default";
        version = 0;
        _v.readers_default.insert(tx);
    });
}

/// @brief put a value
/// @param tx the transaction that writes the value
/// @param k the key of the written entry
/// @param v the value to write
void SparkleTable::Put(T* tx, const K k, const evmc::bytes32& v) {
    CHECK(tx->id > 0) << "we reserve version(0) for default value";
    DLOG(INFO) << "commit " << tx->id;
    Table::Put(k, [&](V& _v) {
        auto guard = std::lock_guard{_v.mu};
        _v.tx = nullptr;
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        // search from insertion position
        while (rit != end) {
            if (rit->version > tx->id) {
                ++rit; continue;
            }
            // abort transactions that read outdated keys
            for (auto _tx: rit->readers) {
                if (_tx->id > tx->id) {
                    _tx->rerun_flag.store(true);
                    DLOG(INFO) << tx->id << " abort " << _tx->id;
                }
            }
            break;
        }
        for (auto _tx: _v.readers_default) {
            if (_tx->id > tx->id) {
                _tx->rerun_flag.store(true);
                DLOG(INFO) << tx->id << " abort " << _tx->id;
            }
        }
        if (rit != end && rit->version == tx->id) {
            rit->value = v;
            return;
        }
        // insert an entry
        _v.entries.insert(rit.base(), SparkleEntry {
            .value   = v,
            .version = tx->id,
            .readers = std::unordered_set<T*>()
        });
    });
}

/// @brief lock an entry for later writting
/// @param tx the transaction containing write operation
/// @param k the key of written entry
/// @return true if lock succeeds
bool SparkleTable::Lock(T* tx, const K k) {
    bool succeed = false;
    Table::Put(k, [&](V& _v) {
        auto guard = std::lock_guard{_v.mu};
        succeed = _v.tx == nullptr || _v.tx->id >= tx->id;
        if (_v.tx && _v.tx->id < tx->id) {
            _v.tx->rerun_flag.store(true);
            DLOG(INFO) << tx->id << " abort " << _v.tx->id;
        }
        if (succeed) { _v.tx = tx; }
    });
    return succeed;
}

/// @brief remove a read dependency from this entry
/// @param tx the transaction that previously read this entry
/// @param k the key of read entry
void SparkleTable::RegretGet(T* tx, const K k, size_t version) {
    DLOG(INFO) << "regret get " << tx->id << std::endl;
    Table::Put(k, [&](V& _v) {
        auto guard = std::lock_guard{_v.mu};
        auto vit = _v.entries.begin();
        auto end = _v.entries.end();
        while (vit != end) {
            if (vit->version != version) {
                ++vit; continue;
            }
            vit->readers.erase(tx);
            break;
        }
        if (version == 0) {
            _v.readers_default.erase(tx);
        }
    });
}

/// @brief undo a put operation and abort all dependent transactions
/// @param tx the transaction that previously put into this entry
/// @param k the key of this put entry
void SparkleTable::RegretPut(T* tx, const K k) {
    DLOG(INFO) << "regret put" << tx->id << std::endl;
    Table::Put(k, [&](V& _v) {
        auto guard = std::lock_guard{_v.mu};
        auto vit = _v.entries.begin();
        auto end = _v.entries.end();
        while (vit != end) {
            if (vit->version != tx->id) {
                ++vit; continue;
            }
            // abort transactions that read from current transaction
            for (auto _tx: vit->readers) {
                _tx->rerun_flag.store(true);
                DLOG(INFO) << tx->id << " abort " << _tx->id;
            }
            break;
        }
        if (vit != end) {
            _v.entries.erase(vit);
        }
    });
}

/// @brief remove a read dependency from this entry
/// @param tx the transaction that previously read this entry
/// @param k the key of read entry
/// @param version the version of read entry, which indicates the transaction that writes this value
void SparkleTable::ClearGet(T* tx, const K k, size_t version) {
    DLOG(INFO) << "clear get " << tx->id << std::endl;
    Table::Put(k, [&](V& _v) {
        auto guard = std::lock_guard{_v.mu};
        auto vit = _v.entries.begin();
        auto end = _v.entries.end();
        while (vit != end) {
            if (vit->version == version) {
                vit->readers.erase(tx);
                break;
            }
            ++vit;
        }
        if (version == 0) {
            _v.readers_default.erase(tx);
        }
    });
}

/// @brief remove versions preceeding current transaction
/// @param tx the transaction the previously wrote this entry
/// @param k the key of written entry
void SparkleTable::ClearPut(T* tx, const K k) {
    DLOG(INFO) << "regret put" << tx->id << std::endl;
    Table::Put(k, [&](V& _v) {
        auto guard = std::lock_guard{_v.mu};
        while (_v.entries.size() && _v.entries.front().version < tx->id) {
            _v.entries.pop_front();
        }
    });
}

/// @brief push transaction into the queue
/// @param tx a unique pointer to transaction (boxed transaction)
void SparkleQueue::Push(std::unique_ptr<T>&& tx) {
    auto guard = std::lock_guard{mu};
    queue.push(std::move(tx));
}

/// @brief pop a transaction from the queue
/// @return a unique pointer to transaction (boxed transaction)
std::unique_ptr<T> SparkleQueue::Pop() {
    auto guard = std::lock_guard{mu};
    if (!queue.size()) return {nullptr};
    auto tx = std::move(queue.front());
    queue.pop();
    return tx;
}

/// @brief sparkle initialization parameters
/// @param workload the transaction generator
/// @param table_partitions the number of parallel partitions to use in the hash table
Sparkle::Sparkle(Workload& workload, Statistics& statistics, size_t n_executors, size_t n_dispatchers, size_t table_partitions):
    workload{workload},
    statistics{statistics},
    n_executors{n_executors},
    n_dispatchers{n_dispatchers},
    queue_bundle(n_executors),
    table{table_partitions}
{
    LOG(INFO) << fmt::format("Sparkle(n_executors={}, n_dispatchers={}, n_table_partitions={})", n_executors, n_dispatchers, table_partitions);
}

void pin_thread_to_core(std::thread &t) {
#ifndef __APPLE__
    static std::size_t core_id = 56;
    LOG(INFO) << "core_id: " << core_id;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    auto _core_id = core_id;
    ++core_id;
    CPU_SET(core_id, &cpuset);
    int rc =
        pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
    CHECK(rc == 0);
#endif
}

/// @brief start sparkle protocol
void Sparkle::Start() {
    stop_flag.store(false);
    for (size_t i = 0; i != n_dispatchers; ++i) {
        DLOG(INFO) << "start dispatcher " << i << std::endl;
        dispatchers.push_back(std::thread([this] {
            SparkleDispatch(*this).Run();
        }));
    }
    for (size_t i = 0; i != n_executors; ++i) {
        DLOG(INFO) << "start executor " << i << std::endl;
        auto queue = &queue_bundle[i];
        executors.push_back(std::thread([this, queue] {
            SparkleExecutor(*this, *queue).Run();
        }));
        pin_thread_to_core(executors[i]);
    }
}

/// @brief stop sparkle protocol
/// @return statistics of this execution
void Sparkle::Stop() {
    stop_flag.store(true);
    for (auto& x: dispatchers)  { x.join(); }
    for (auto& x: executors)    { x.join(); }
}

/// @brief initialize a dispatcher
/// @param sparkle the sparkle protocol configuration
SparkleDispatch::SparkleDispatch(Sparkle& sparkle):
    workload{sparkle.workload},
    last_execute{sparkle.last_execute},
    queue_bundle{sparkle.queue_bundle},
    stop_flag{sparkle.stop_flag}
{}

/// @brief run dispatcher
void SparkleDispatch::Run() {
    while(!stop_flag.load()) {for (auto& queue: queue_bundle) {
        // round-robin dispatch
        queue.Push(std::make_unique<T>(workload.Next(), last_execute.fetch_add(1)));
    }}
}

/// @brief sparkle executor
/// @param sparkle sparkle initialization paremeters
SparkleExecutor::SparkleExecutor(Sparkle& sparkle, SparkleQueue& queue):
    queue{queue},
    table{sparkle.table},
    last_finalized{sparkle.last_finalized},
    statistics{sparkle.statistics},
    stop_flag{sparkle.stop_flag}
{}

/// @brief start an executor
void SparkleExecutor::Run() { while (!stop_flag.load()) {
    auto tx = queue.Pop();
    if (tx == nullptr) { continue; }
    auto start = steady_clock::now();
    tx->UpdateSetStorageHandler([&](
        const evmc::address &addr, 
        const evmc::bytes32 &key, 
        const evmc::bytes32 &value
    ) {
        DLOG(INFO) << tx->id << " set";
        auto _key   = std::make_tuple(addr, key);
        // just push back
        tx->tuples_put.push_back(std::make_tuple(_key, value));
        return evmc_storage_status::EVMC_STORAGE_MODIFIED;
    });
    tx->UpdateGetStorageHandler([&](
        const evmc::address &addr, 
        const evmc::bytes32 &key
    ) {
        DLOG(INFO) << tx->id << " get";
        auto _key   = std::make_tuple(addr, key);
        auto value  = evmc::bytes32{0};
        auto version = size_t{0};
        // one key from one transaction will be commited once
        for (auto& tup: tx->tuples_put) {
            if (std::get<0>(tup) == _key) {
                return std::get<1>(tup);
            }
        }
        for (auto& tup: tx->tuples_get) {
            if (std::get<0>(tup) == _key) {
                return std::get<1>(tup);
            }
        }
        table.Get(tx.get(), _key, value, version);
        tx->tuples_get.push_back(std::make_tuple(_key, value, version));
        return value;
    });
    tx->rerun_flag.store(true);
    while (!stop_flag.load()) {
        DLOG(INFO) << "recycle " << tx->id << " finalized " << last_finalized.load();
        if (tx->rerun_flag.load()) {
            // sweep all operations from previous execution
            for (auto entry: tx->tuples_get) {
                table.RegretGet(tx.get(), std::get<0>(entry), std::get<2>(entry));
            }
            for (auto entry: tx->tuples_put) {
                table.RegretPut(tx.get(), std::get<0>(entry));
            }
            tx->Reset();
            // execute and try to commit
            DLOG(INFO) << "execute (in) " << tx->id;
            statistics.JournalExecute();
            tx->Execute();
            DLOG(INFO) << "execute (out) " << tx->id;
            if (tx->rerun_flag.load()) { continue; }
            for (auto entry: tx->tuples_put) {
                table.Put(tx.get(), std::get<0>(entry), std::get<1>(entry));
            }
        }
        else if (last_finalized.load() + 1 == tx->id) {
            // here no previous transaction will affect the result of this transaction. 
            // therefore, we can determine inaccessible values and remove them. 
            for (auto entry: tx->tuples_get) {
                table.ClearGet(tx.get(), std::get<0>(entry), std::get<2>(entry));
            }
            for (auto entry: tx->tuples_put) {
                table.ClearPut(tx.get(), std::get<0>(entry));
            }
            last_finalized.fetch_add(1);
            DLOG(INFO) << "commit " << tx->id;
            auto latency = duration_cast<microseconds>(steady_clock::now() - start).count();
            statistics.JournalCommit(latency);
            break;
        }
        std::this_thread::yield();
    }
}}

#undef T
#undef V
#undef K

} // namespace spectrum
