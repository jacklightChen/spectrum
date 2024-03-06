#include "./protocol-spectrum.hpp"
#include "./table.hpp"
#include "./hex.hpp"
#include <functional>
#include <thread>
#include <chrono>
#include <glog/logging.h>

/*
    This is a implementation of "Spectrum: Speculative Deterministic Concurrency Control for Partially Replicated Transactional Data Stores" (Zhongmiao Li, Peter Van Roy and Paolo Romano). 
 */

namespace spectrum {

using namespace std::chrono;

#define K std::tuple<evmc::address, evmc::bytes32>
#define V SpectrumVersionList
#define T SpectrumTransaction

/// @brief wrap a base transaction into a spectrum transaction
/// @param inner the base transaction
/// @param id transaction id
SpectrumTransaction::SpectrumTransaction(Transaction&& inner, size_t id):
    Transaction{std::move(inner)},
    id{id},
    start_time{std::chrono::steady_clock::now()}
{}

/// @brief determine transaction has to rerun
/// @return if transaction has to rerun
bool SpectrumTransaction::HasRerunKeys() {
    auto guard = std::lock_guard{rerun_keys_mu};
    return rerun_keys.size() != 0;
}

/// @brief call the transaction to rerun providing the key that caused it
/// @param key the key that caused rerun
void SpectrumTransaction::AddRerunKeys(const K& key, size_t cause_id) {
    auto guard = std::lock_guard{rerun_keys_mu};
    rerun_keys.push_back(key);
    should_wait = std::min(should_wait, cause_id);
}

/// @brief the multi-version table for spectrum
/// @param partitions the number of partitions
SpectrumTable::SpectrumTable(size_t partitions):
    Table<K, V, KeyHasher>{partitions}
{}

/// @brief get a value
/// @param tx the transaction that reads the value
/// @param k the key of the read entry
/// @param v (mutated to be) the value of read entry
/// @param version (mutated to be) the version of read entry
void SpectrumTable::Get(T* tx, const K& k, evmc::bytes32& v, size_t& version) {
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
        version = 0;
        _v.readers_default.insert(tx);
    });
}

/// @brief put a value
/// @param tx the transaction that writes the value
/// @param k the key of the written entry
/// @param v the value to write
void SpectrumTable::Put(T* tx, const K& k, const evmc::bytes32& v) {
    CHECK(tx->id > 0) << "we reserve version(0) for default value";
    Table::Put(k, [&](V& _v) {
        _v.tx = nullptr;
        auto guard = std::lock_guard{_v.mu};
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
                    _tx->AddRerunKeys(k, tx->id);
                }
            }
            break;
        }
        for (auto _tx: _v.readers_default) {
            if (_tx->id > tx->id) {
                _tx->AddRerunKeys(k, tx->id);
            }
        }
        // insert an entry
        _v.entries.insert(rit.base(), SpectrumEntry {
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
bool SpectrumTable::Lock(T* tx, const K& k) {
    bool succeed = false;
    Table::Put(k, [&](V& _v) {
        auto guard = std::lock_guard{_v.mu};
        succeed = !_v.tx || _v.tx->id >= tx->id;
        if (_v.tx && _v.tx->id < tx->id) {
            _v.tx->AddRerunKeys(k, tx->id);
        }
        if (succeed) { _v.tx = tx; }
    });
    return succeed;
}

/// @brief remove a read dependency from this entry
/// @param tx the transaction that previously read this entry
/// @param k the key of read entry
void SpectrumTable::RegretGet(T* tx, const K& k, size_t version) {
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
void SpectrumTable::RegretPut(T* tx, const K& k) {
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
                _tx->AddRerunKeys(k, tx->id);
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
void SpectrumTable::ClearGet(T* tx, const K& k, size_t version) {
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
        if (version == 0) {
            _v.readers_default.erase(tx);
        }
    });
}

/// @brief remove versions preceeding current transaction
/// @param tx the transaction the previously wrote this entry
/// @param k the key of written entry
void SpectrumTable::ClearPut(T* tx, const K& k) {
    Table::Put(k, [&](V& _v) {
        auto guard = std::lock_guard{_v.mu};
        while (_v.entries.size() && _v.entries.front().version < tx->id) {
            _v.entries.pop_front();
        }
    });
}

/// @brief spectrum initialization parameters
/// @param workload the transaction generator
/// @param table_partitions the number of parallel partitions to use in the hash table
Spectrum::Spectrum(Workload& workload, size_t n_threads, size_t table_partitions):
    workload{workload},
    n_threads{n_threads},
    table{table_partitions}
{}

/// @brief start spectrum protocol
/// @param n_threads the number of threads to start
void Spectrum::Start() {
    stop_flag.store(false);
    for (size_t i = 0; i != n_threads; ++i) {
        executors.push_back(SpectrumExecutor(*this));
        threads.emplace_back(std::move(std::thread([&](){ executors.back().Run(); })));
    }
}

/// @brief stop spectrum protocol
/// @return statistics of this execution
Statistics Spectrum::Stop() {
    stop_flag.store(true);
    for (auto& worker: threads) {
        worker.join();
    }
    return this->statistics;
}

/// @brief report spectrum statistics
/// @return current statistics of spectrum execution
Statistics Spectrum::Report() {
    return this->statistics;
}

/// @brief spectrum executor
/// @param spectrum spectrum initialization paremeters
SpectrumExecutor::SpectrumExecutor(Spectrum& spectrum):
    workload{spectrum.workload},
    table{spectrum.table},
    last_finalized{spectrum.last_finalized},
    last_execute{spectrum.last_execute},
    stop_flag{spectrum.stop_flag},
    statistics{spectrum.statistics},
    queue{spectrum.queue}
{}

/// @brief start an executor
void SpectrumExecutor::Run() {while (!stop_flag.load()) {
    auto tx = std::unique_ptr<T>(new T(std::move(workload.Next()), last_execute.fetch_add(1)));
    tx->UpdateSetStorageHandler([&](
        const evmc::address &addr, 
        const evmc::bytes32 &key, 
        const evmc::bytes32 &value
    ) {
        auto _key   = std::make_tuple(addr, key);
        auto version = size_t{0};
        while (!table.Lock(tx.get(), _key)) {
            std::this_thread::yield(); 
        }
        // when there exists some key, do this
        for (auto& tup: tx->tuples_put) {
            if (std::get<0>(tup) == _key) {
                std::get<1>(tup) = value;
                return evmc_storage_status::EVMC_STORAGE_MODIFIED;
            }
        }
        // else just push back
        tx->tuples_put.push_back(std::make_tuple(_key, value));
        return evmc_storage_status::EVMC_STORAGE_MODIFIED;
    });
    tx->UpdateGetStorageHandler([&](
        const evmc::address &addr, 
        const evmc::bytes32 &key
    ) {
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
    auto first_run = bool{true};
    while (true) {
        if (first_run || tx->HasRerunKeys()) {
            // sweep all operations from previous execution
            for (auto entry: tx->tuples_get) {
                table.RegretGet(tx.get(), std::get<0>(entry), std::get<2>(entry));
            }
            for (auto entry: tx->tuples_put) {
                table.RegretPut(tx.get(), std::get<0>(entry));
            }
            // execute and try to commit
            statistics.JournalExecute();
            tx->Execute();
            if (tx->HasRerunKeys()) { continue; }
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
            break;
        }
    }
    auto latency = duration_cast<microseconds>(steady_clock::now() - tx->start_time).count();
    statistics.JournalCommit(latency);
}}

#undef T
#undef V
#undef K

} // namespace spectrum
