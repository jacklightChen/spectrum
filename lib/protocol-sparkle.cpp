#include "./protocol-sparkle.hpp"
#include "./table.hpp"
#include "./hex.hpp"
#include <functional>
#include <thread>
#include <chrono>

namespace spectrum {

using namespace std::chrono;

#define K std::tuple<evmc::address, evmc::bytes32>
#define V SparkleVersionList
#define T SparkleTransaction

/// @brief wrap a base transaction into a sparkle transaction
/// @param inner the base transaction
/// @param id transaction id
T::T(Transaction&& inner, size_t id):
    Transaction{std::move(inner)},
    id{id}
{}

/// @brief reset the transaction (local) information
void T::Reset() {
    tuples_get.resize(0);
    tuples_put.resize(0);
    rerun_flag.store(0);
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
    });
}

/// @brief put a value
/// @param tx the transaction that writes the value
/// @param k the key of the written entry
/// @param v the value to write
void SparkleTable::Put(T* tx, const K& k, const evmc::bytes32& v) {
    Table::Put(k, [&](V& _v) {
        _v.tx = nullptr;
        auto guard = std::lock_guard{_v.mu};
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        // search from insertion position
        while (rit != end && rit->version != tx->id) {
            if (rit->version > tx->id) {
                ++rit; continue;
            }
            // abort transactions that read outdated keys
            for (auto _tx: rit->readers) {
                if (_tx->id > tx->id) {
                    _tx->rerun_flag.store(true);
                }
            }
            break;
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
        auto guard = std::lock_guard{_v.mu};
        succeed = !_v.tx || _v.tx->id >= tx->id;
        if (_v.tx && _v.tx->id < tx->id) {
            _v.tx->rerun_flag.store(true);
        }
        if (succeed) { _v.tx = tx; }
    });
    return succeed;
}

/// @brief remove a read dependency from this entry
/// @param tx the transaction that previously read this entry
/// @param k the key of read entry
void SparkleTable::RegretGet(T* tx, const K& k, size_t version) {
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
    });
}

/// @brief undo a put operation and abort all dependent transactions
/// @param tx the transaction that previously put into this entry
/// @param k the key of this put entry
void SparkleTable::RegretPut(T* tx, const K& k) {
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
            }
            break;
        }
        _v.entries.erase(vit);
    });
}

/// @brief remove a read dependency from this entry
/// @param tx the transaction that previously read this entry
/// @param k the key of read entry
/// @param version the version of read entry, which indicates the transaction that writes this value
void SparkleTable::ClearGet(T* tx, const K& k, size_t version) {
    Table::Put(k, [&](V& _v) {
        auto guard = std::lock_guard{_v.mu};
        auto vit = _v.entries.begin();
        auto end = _v.entries.end();
        while (vit != end) {
            if (vit->version == version) {
                vit->readers.erase(tx);
                break;
            }
        }
    });
}

/// @brief remove versions preceeding current transaction
/// @param tx the transaction the previously wrote this entry
/// @param k the key of written entry
void SparkleTable::ClearPut(T* tx, const K& k) {
    Table::Put(k, [&](V& _v) {
        auto guard = std::lock_guard{_v.mu};
        while (_v.entries.size() && _v.entries.front().version < tx->id) {
            _v.entries.pop_front();
        }
    });
}

/// @brief sparkle initialization parameters
/// @param workload the transaction generator
/// @param table_partitions the number of parallel partitions to use in the hash table
Sparkle::Sparkle(Workload& workload, size_t table_partitions):
    workload{workload},
    table{table_partitions}
{}

/// @brief start sparkle protocol
/// @param n_threads the number of threads to start
void Sparkle::Start(size_t n_threads) {
    stop_flag.store(false);
    for (size_t i = 0; i != n_threads; ++i) {
        executors.push_back(SparkleExecutor(*this));
        threads.emplace_back(std::move(std::thread([&](){ executors.back().Run(); })));
    }
}

/// @brief stop sparkle protocol
/// @return statistics of this execution
Statistics Sparkle::Stop() {
    stop_flag.store(true);
    for (auto& worker: threads) {
        worker.join();
    }
    return this->statistics;
}

/// @brief sparkle executor
/// @param sparkle sparkle initialization paremeters
SparkleExecutor::SparkleExecutor(Sparkle& sparkle):
    workload{sparkle.workload},
    table{sparkle.table},
    last_commit{sparkle.last_commit},
    last_execute{sparkle.last_execute},
    stop_flag{sparkle.stop_flag},
    statistics{sparkle.statistics}
{}

/// @brief start an executor
void SparkleExecutor::Run() { while (!stop_flag.load()) {
    auto tx = T(std::move(workload.Next()), last_execute.fetch_add(1));
    auto start = steady_clock::now();
    tx.UpdateSetStorageHandler([&](
        const evmc::address &addr, 
        const evmc::bytes32 &key, 
        const evmc::bytes32 &value
    ) {
        auto _key   = std::make_tuple(addr, key);
        auto version = size_t{0};
        while (!table.Lock(&tx, _key)) {
            std::this_thread::yield(); 
        }
        tx.tuples_put.push_back(std::make_tuple(_key, value));
        return evmc_storage_status::EVMC_STORAGE_MODIFIED;
    });
    tx.UpdateGetStorageHandler([&](
        const evmc::address &addr, 
        const evmc::bytes32 &key
    ) {
        auto _key   = std::make_tuple(addr, key);
        auto value  = evmc::bytes32{0};
        auto version = size_t{0};
        table.Get(&tx, _key, value, version);
        tx.tuples_get.push_back(std::make_tuple(_key, value, version));
        return value;
    });
    tx.rerun_flag.store(true);
    while (true) {
        if (tx.rerun_flag.load()) {
            // sweep all operations from previous execution
            for (auto entry: tx.tuples_get) {
                table.RegretGet(&tx, std::get<0>(entry), std::get<2>(entry));
            }
            for (auto entry: tx.tuples_put) {
                table.RegretPut(&tx, std::get<0>(entry));
            }
            tx.Reset();
            // execute and try to commit
            statistics.JournalExecute();
            tx.Execute();
            if (tx.rerun_flag.load()) { continue; }
            for (auto entry: tx.tuples_put) {
                table.Put(&tx, std::get<0>(entry), std::get<1>(entry));
            }
        }
        else if (last_commit.load() + 1 == tx.id) {
            // here no previous transaction will affect the result of this transaction. 
            // therefore, we can determine inaccessible values and remove them. 
            for (auto entry: tx.tuples_get) {
                table.ClearGet(&tx, std::get<0>(entry), std::get<2>(entry));
            }
            for (auto entry: tx.tuples_put) {
                table.ClearPut(&tx, std::get<0>(entry));
            }
            last_commit.fetch_add(1);
            break;
        }
    }
    auto latency = duration_cast<microseconds>(steady_clock::now() - start).count();
    statistics.JournalCommit(latency);
}}

#undef T
#undef V
#undef K

} // namespace spectrum
