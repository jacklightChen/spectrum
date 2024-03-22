#include "./protocol-sparkle.hpp"
#include "lock-util.hpp"
#include "./hex.hpp"
#include "./thread-util.hpp"
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
    id{id},
    start_time{steady_clock::now()}
{}

/// @brief reset the transaction (local) information
void SparkleTransaction::Reset() {
    tuples_get.resize(0);
    tuples_put.resize(0);
    rerun_flag.store(false);
    Transaction::ApplyCheckpoint(0);
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
void SparkleTable::Get(T* tx, const K& k, evmc::bytes32& v, size_t& version) {
    DLOG(INFO) << tx->id << " get";
    Table::Put(k, [&](V& _v) {
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
void SparkleTable::Put(T* tx, const K& k, const evmc::bytes32& v) {
    CHECK(tx->id > 0) << "we reserve version(0) for default value";
    DLOG(INFO) << "commit " << tx->id;
    Table::Put(k, [&](V& _v) {
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
                DLOG(INFO) << KeyHasher()(k) << " has read dependency " << "(" << _tx << ")" << std::endl;
                if (_tx->id > tx->id) {
                    _tx->rerun_flag.store(true);
                    DLOG(INFO) << tx->id << " abort " << _tx->id;
                }
            }
            break;
        }
        for (auto _tx: _v.readers_default) {
            DLOG(INFO) << KeyHasher()(k) << " has read dependency " << "(" << _tx << ")" << std::endl;
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
bool SparkleTable::Lock(T* tx, const K& k) {
    bool succeed = false;
    Table::Put(k, [&](V& _v) {
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
void SparkleTable::RegretGet(T* tx, const K& k, size_t version) {
    DLOG(INFO) << "regret get " << tx->id << std::endl;
    Table::Put(k, [&](V& _v) {
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
        // run an extra check in debug mode
        #if !defined(NDEBUG)
        {
            auto end = _v.entries.end();
            for (auto vit = _v.entries.begin(); vit != end; ++vit) {
                DLOG(INFO) << "spot version " << vit->version << std::endl;
                if (vit->readers.contains(tx)) {
                    DLOG(FATAL) << "didn't remove " << tx->id << "(" << tx << ")" << " still on version " << vit->version  << std::endl;
                }
            }
        }
        #endif
    });
}

/// @brief undo a put operation and abort all dependent transactions
/// @param tx the transaction that previously put into this entry
/// @param k the key of this put entry
void SparkleTable::RegretPut(T* tx, const K& k) {
    DLOG(INFO) << "regret put " << tx->id << std::endl;
    Table::Put(k, [&](V& _v) {
        auto vit = _v.entries.begin();
        auto end = _v.entries.end();
        while (vit != end) {
            if (vit->version != tx->id) {
                ++vit; continue;
            }
            // abort transactions that read from current transaction
            for (auto _tx: vit->readers) {
                DLOG(INFO) << KeyHasher()(k) << " has read dependency " << "(" << _tx << ")" << std::endl;
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
void SparkleTable::ClearGet(T* tx, const K& k, size_t version) {
    DLOG(INFO) << "clear get " << tx->id << std::endl;
    Table::Put(k, [&](V& _v) {
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
        // run an extra check in debug mode
        #if !defined(NDEBUG)
        {
            auto end = _v.entries.end();
            for (auto vit = _v.entries.begin(); vit != end; ++vit) {
                DLOG(INFO) << "spot version " << vit->version << std::endl;
                if (vit->readers.contains(tx)) {
                    DLOG(FATAL) << "didn't remove " << tx->id << "(" << tx << ")" << " still on version " << vit->version  << std::endl;
                }
            }
        }
        #endif
    });
}

/// @brief remove versions preceeding current transaction
/// @param tx the transaction the previously wrote this entry
/// @param k the key of written entry
void SparkleTable::ClearPut(T* tx, const K& k) {
    DLOG(INFO) << "clear put " << tx->id << std::endl;
    Table::Put(k, [&](V& _v) {
        while (_v.entries.size() && _v.entries.front().version < tx->id) {
            _v.entries.pop_front();
        }
    });
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
    workload.SetEVMType(EVMType::BASIC);
}

/// @brief start sparkle protocol
void Sparkle::Start() {
    stop_flag.store(false);
    for (size_t i = 0; i != n_executors; ++i) {
        DLOG(INFO) << "start executor " << i << std::endl;
        auto queue = &queue_bundle[i];
        executors.push_back(std::thread([this, queue] {
            SparkleExecutor(*this, *queue).Run();
        }));
        PinRoundRobin(executors[i], i + n_dispatchers);
    }
    for (size_t i = 0; i != n_dispatchers; ++i) {
        DLOG(INFO) << "start dispatcher " << i << std::endl;
        dispatchers.push_back(std::thread([this] {
            SparkleDispatch(*this).Run();
        }));
        PinRoundRobin(dispatchers[i], i);
    }
}

/// @brief stop sparkle protocol
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
    while(!stop_flag.load()) {
        auto tx = std::make_unique<T>(workload.Next(), last_execute.fetch_add(1));
        queue_bundle[tx->id % queue_bundle.size()].Push(std::move(tx));
    }
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
    // when a transaction is not runned before, execute that transaction
    if (!tx->berun_flag.load()) {
        tx->berun_flag.store(true);
        auto tx_ref = tx.get();
        tx->UpdateSetStorageHandler([tx_ref](
            const evmc::address &addr, 
            const evmc::bytes32 &key, 
            const evmc::bytes32 &value
        ) {
            DLOG(INFO) << tx_ref->id << " set";
            auto _key   = std::make_tuple(addr, key);
            // just push back
            tx_ref->tuples_put.push_back(std::make_tuple(_key, value));
            return evmc_storage_status::EVMC_STORAGE_MODIFIED;
        });
        tx->UpdateGetStorageHandler([tx_ref, this](
            const evmc::address &addr, 
            const evmc::bytes32 &key
        ) {
            DLOG(INFO) << tx_ref->id << " get";
            auto _key   = std::make_tuple(addr, key);
            auto value  = evmc::bytes32{0};
            auto version = size_t{0};
            // one key from one transaction will be commited once
            for (auto& tup: tx_ref->tuples_put) {
                if (std::get<0>(tup) == _key) {
                    return std::get<1>(tup);
                }
            }
            for (auto& tup: tx_ref->tuples_get) {
                if (std::get<0>(tup) == _key) {
                    return std::get<1>(tup);
                }
            }
            table.Get(tx_ref, _key, value, version);
            tx_ref->tuples_get.push_back(std::make_tuple(_key, value, version));
            return value;
        });
        DLOG(INFO) << "execute (in) " << tx->id;
        statistics.JournalExecute();
        tx->execution_count += 1;
        if(tx->execution_count >= 10) DLOG(ERROR) << tx->id << " execution " << tx->execution_count << std::endl;
        tx->Execute();
        DLOG(INFO) << "execute (out) " << tx->id;
        if (tx->rerun_flag.load()) { queue.Push(std::move(tx)); continue; }
        for (auto entry: tx->tuples_put) {
            if (tx->rerun_flag.load()) { break; }
            table.Put(tx.get(), std::get<0>(entry), std::get<1>(entry));
        }
    }
    while (true) {
        DLOG(INFO) << "recycle " << tx->id << " finalized " << last_finalized.load();
        if (stop_flag.load()) {
            // we add this to prevent something bad like the following:
            // 1. some transaction ta is executing on thread A. 
            // 2. another transaction tb is executing on thread B. 
            // 3. on thread A, stop_flag.load() == true, we exit without reserving ta (therefore, ta is deleted). 
            // 4. tb access a key, previously ta registered itself as a read dependency on this key, reading ta->id cause read after release. 
            queue.Push(std::move(tx));
            break;
        } else if (tx->rerun_flag.load()) {
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
            tx->execution_count += 1;
            if(tx->execution_count >= 10) DLOG(ERROR) << tx->id << " execution " << tx->execution_count << std::endl;
            tx->Execute();
            DLOG(INFO) << "execute (out) " << tx->id;
            if (tx->rerun_flag.load()) { continue; }
            for (auto entry: tx->tuples_put) {
                if (tx->rerun_flag.load()) { break; }
                table.Put(tx.get(), std::get<0>(entry), std::get<1>(entry));
            }
        }
        else if (last_finalized.load() + 1 == tx->id) {
            // here no previous transaction will affect the result of this transaction. 
            // therefore, we can determine inaccessible values and remove them. 
            last_finalized.fetch_add(1);
            for (auto entry: tx->tuples_get) {
                table.ClearGet(tx.get(), std::get<0>(entry), std::get<2>(entry));
            }
            for (auto entry: tx->tuples_put) {
                table.ClearPut(tx.get(), std::get<0>(entry));
            }
            DLOG(INFO) << "final commit " << tx->id;
            auto latency = duration_cast<microseconds>(steady_clock::now() - tx->start_time).count();
            statistics.JournalCommit(latency);
            break;
        }
        else {
            queue.Push(std::move(tx));
            break;
        }
    }
}}

#undef T
#undef V
#undef K

} // namespace spectrum
