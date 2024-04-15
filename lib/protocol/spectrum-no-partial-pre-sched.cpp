#include "evmc/evmc.hpp"
#include <spectrum/protocol/spectrum-no-partial-pre-sched.hpp>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/common/hex.hpp>
#include <spectrum/common/thread-util.hpp>
#include <functional>
#include <thread>
#include <chrono>
#include <glog/logging.h>
#include <ranges>
#include <fmt/core.h>
#include <iostream>

/*
    This is a implementation of "Spectrum: Speedy and Strictly-Deterministic Smart Contract Transactions for Blockchain Ledgers" (Zhihao Chen, Tianji Yang, Yixiao Zheng, Zhao Zhang, Cheqing Jin and Aoying Zhou). 
 */


namespace spectrum {

using namespace std::chrono;

#define K std::tuple<evmc::address, evmc::bytes32>
#define V SpectrumNoPartialPreSchedVersionList
#define T SpectrumNoPartialPreSchedTransaction

/// @brief wrap a base transaction into a spectrum transaction
/// @param inner the base transaction
/// @param id transaction id
SpectrumNoPartialPreSchedTransaction::SpectrumNoPartialPreSchedTransaction(Transaction&& inner, size_t id):
    Transaction{std::move(inner)},
    id{id},
    start_time{std::chrono::steady_clock::now()}
{}

/// @brief determine transaction has to rerun
/// @return if transaction has to rerun
bool SpectrumNoPartialPreSchedTransaction::HasWAR() {
    auto guard = Guard{rerun_keys_mu};
    return rerun_keys.size() != 0;
}

/// @brief call the transaction to rerun providing the key that caused it
/// @param key the key that caused rerun
void SpectrumNoPartialPreSchedTransaction::SetWAR(const K& key, size_t writer_id, bool pre_schedule) {
    if (!pre_schedule) {
        auto guard = Guard{rerun_keys_mu};
        rerun_keys.push_back(key);
        return;
    }
    if (should_wait.contains(key)) {
        should_wait[key] = std::max(should_wait[key], writer_id);
    }
    else {
        should_wait[key] = writer_id;
    }
}

size_t SpectrumNoPartialPreSchedTransaction::ShouldWait(const K& key) {
    // auto guard = Guard{rerun_keys_mu};
    return should_wait.contains(key) ? should_wait[key] : 0;
}

/// @brief the multi-version table for spectrum
/// @param partitions the number of partitions
SpectrumNoPartialPreSchedLockTable::SpectrumNoPartialPreSchedLockTable(size_t partitions):
    Table<K, V, KeyHasher>{partitions}
{}

/// @brief get a value
/// @param tx the transaction that reads the value
/// @param k the key of the read entry
/// @param version (mutated to be) the version of read entry
void SpectrumNoPartialPreSchedLockTable::Get(T* tx, const K& k) {
    Table::Put(k, [&](V& _v) {
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        while (rit != end) {
            if (rit->version > tx->id) {
                ++rit; continue;
            }
            rit->readers.insert(tx);
            tx->SetWAR(k, rit->version, true);
            DLOG(INFO) << tx->id << "(" << tx << ")" << " read " << KeyHasher()(k) % 1000 << " version " << rit->version << std::endl;
            return;
        }
        DLOG(INFO) << tx->id << "(" << tx << ")" << " read " << KeyHasher()(k) % 1000 << " version 0" << std::endl;
        _v.readers_default.insert(tx);
    });
}

/// @brief put a value
/// @param tx the transaction that writes the value
/// @param k the key of the written entry
void SpectrumNoPartialPreSchedLockTable::Put(T* tx, const K& k) {
    CHECK(tx->id > 0) << "we reserve version(0) for default value";
    DLOG(INFO) << tx->id << "(" << tx << ")" << " write " << KeyHasher()(k) % 1000 << std::endl;
    Table::Put(k, [&](V& _v) {
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        auto readers_ = std::unordered_set<T*>();
        // search from insertion position
        while (rit != end) {
            if (rit->version > tx->id) {
                ++rit; continue;
            }
            // abort transactions that read outdated keys
            for (auto it = rit->readers.begin(); it != rit->readers.end();) {
                DLOG(INFO) << KeyHasher()(k) % 1000 << " has read dependency " << "(" << (*it) << ")" << std::endl;
                if ((*it)->id <= tx->id) { ++it; continue; }
                DLOG(INFO) << tx->id << " abort " << (*it)->id << std::endl;
                (*it)->SetWAR(k, tx->id, true);
                readers_.insert(*it);
                it = rit->readers.erase(it);
            }
            break;
        }
        for (auto it = _v.readers_default.begin(); it != _v.readers_default.end();) {
            DLOG(INFO) << KeyHasher()(k) % 1000 << " has read dependency " << "(" << (*it) << ")" << std::endl;
            if ((*it)->id <= tx->id) { ++it; continue; }
            DLOG(INFO) << tx->id << " abort " << (*it)->id << std::endl;
            (*it)->SetWAR(k, tx->id, true);
            readers_.insert(*it);
            it = _v.readers_default.erase(it);
        }
        // handle duplicated write on the same key
        if (rit != end && rit->version == tx->id) {
            DCHECK(readers_.size() == 0);
            return;
        }
        // insert an entry
        _v.entries.insert(rit.base(), SpectrumNoPartialPreSchedEntry {
            .value   = evmc::bytes32{0},
            .version = tx->id,
            .readers = readers_
        });
    });
}

/// @brief remove versions preceeding current transaction
/// @param tx the transaction the previously wrote this entry
/// @param k the key of written entry
void SpectrumNoPartialPreSchedLockTable::ClearPut(T* tx, const K& k) {
    DLOG(INFO) << "remove write record before " << tx->id << "(" << tx << ")" << " from " << KeyHasher()(k) % 1000 << std::endl;
    Table::Put(k, [&](V& _v) {
        while (_v.entries.size() && _v.entries.front().version < tx->id) {
            _v.entries.pop_front();
        }
    });
}

/// @brief remove a read dependency from this entry
/// @param tx the transaction that previously read this entry
/// @param k the key of read entry
void SpectrumNoPartialPreSchedLockTable::ClearGet(T* tx, const K& k) {
    DLOG(INFO) << "remove read record " << tx->id << "(" << tx << ")" << " from " << KeyHasher()(k) % 1000 << std::endl;
    Table::Put(k, [&](V& _v) {
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        while (rit != end) {
            if (rit->version >= tx->id) {
                ++rit; continue;
            }
            DLOG(INFO) << "remove " << tx->id << "(" << tx << ")" << " from version " << rit->version << std::endl; 
            rit->readers.erase(tx);
            break;
        }
        _v.readers_default.erase(tx);
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
            if (_v.readers_default.contains(tx)) {
                DLOG(FATAL) << "didn't remove " << tx->id << "(" << tx << ")" << " still on version 0" << std::endl;
            }
        }
        #endif
    });
}

/// @brief the multi-version table for spectrum
/// @param partitions the number of partitions
SpectrumNoPartialPreSchedTable::SpectrumNoPartialPreSchedTable(size_t partitions):
    Table<K, V, KeyHasher>{partitions}
{}

/// @brief get a value
/// @param tx the transaction that reads the value
/// @param k the key of the read entry
/// @param v (mutated to be) the value of read entry
/// @param version (mutated to be) the version of read entry
void SpectrumNoPartialPreSchedTable::Get(T* tx, const K& k, evmc::bytes32& v, size_t& version) {
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
            DLOG(INFO) << tx->id << "(" << tx << ")" << " read " << KeyHasher()(k) % 1000 << " version " << rit->version << std::endl;
            return;
        }
        version = 0;
        DLOG(INFO) << tx->id << "(" << tx << ")" << " read " << KeyHasher()(k) % 1000 << " version 0" << std::endl;
        _v.readers_default.insert(tx);
        v = evmc::bytes32{0};
    });
}

/// @brief put a value
/// @param tx the transaction that writes the value
/// @param k the key of the written entry
/// @param v the value to write
void SpectrumNoPartialPreSchedTable::Put(T* tx, const K& k, const evmc::bytes32& v) {
    CHECK(tx->id > 0) << "we reserve version(0) for default value";
    DLOG(INFO) << tx->id << "(" << tx << ")" << " write " << KeyHasher()(k) % 1000 << std::endl;
    Table::Put(k, [&](V& _v) {
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        // search from insertion position
        while (rit != end) {
            if (rit->version > tx->id) {
                ++rit; continue;
            }
            // abort transactions that read outdated keys
            for (auto _tx: rit->readers) {
                DLOG(INFO) << KeyHasher()(k) % 1000 << " has read dependency " << "(" << _tx << ")" << std::endl;
                if (_tx->id > tx->id) {
                    DLOG(INFO) << tx->id << " abort " << _tx->id << std::endl;
                    _tx->SetWAR(k, tx->id, false);
                }
            }
            break;
        }
        for (auto _tx: _v.readers_default) {
            DLOG(INFO) << KeyHasher()(k) % 1000 << " has read dependency " << "(" << _tx << ")" << std::endl;
            if (_tx->id > tx->id) {
                DLOG(INFO) << tx->id << " abort " << _tx->id << std::endl;
                _tx->SetWAR(k, tx->id, false);
            }
        }
        // handle duplicated write on the same key
        if (rit != end && rit->version == tx->id) {
            rit->value = v;
            return;
        }
        // insert an entry
        _v.entries.insert(rit.base(), SpectrumNoPartialPreSchedEntry {
            .value   = v,
            .version = tx->id,
            .readers = std::unordered_set<T*>()
        });
    });
}

/// @brief remove a read dependency from this entry
/// @param tx the transaction that previously read this entry
/// @param k the key of read entry
void SpectrumNoPartialPreSchedTable::RegretGet(T* tx, const K& k, size_t version) {
    DLOG(INFO) << "remove read record " << tx->id << "(" << tx << ")" << " from " << KeyHasher()(k) % 1000 << std::endl;
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
            if (_v.readers_default.contains(tx)) {
                DLOG(FATAL) << "didn't remove " << tx->id << "(" << tx << ")" << " still on version " << vit->version  << std::endl;
            }
        }
        #endif
    });
}

/// @brief undo a put operation and abort all dependent transactions
/// @param tx the transaction that previously put into this entry
/// @param k the key of this put entry
void SpectrumNoPartialPreSchedTable::RegretPut(T* tx, const K& k) {
    DLOG(INFO) << "remove write record " << tx->id << "(" << tx << ")" << " from " << KeyHasher()(k) % 1000 << std::endl;
    Table::Put(k, [&](V& _v) {
        auto vit = _v.entries.begin();
        auto end = _v.entries.end();
        while (vit != end) {
            if (vit->version != tx->id) {
                ++vit; continue;
            }
            // abort transactions that read from current transaction
            for (auto _tx: vit->readers) {
                DLOG(INFO) << KeyHasher()(k) % 1000 << " has read dependency " << "(" << _tx << ")" << std::endl;
                DLOG(INFO) << tx->id << " abort " << _tx->id << std::endl;
                _tx->SetWAR(k, tx->id, false);
            }
            break;
        }
        if (vit != end) { _v.entries.erase(vit); }
    });
}

/// @brief remove a read dependency from this entry
/// @param tx the transaction that previously read this entry
/// @param k the key of read entry
/// @param version the version of read entry, which indicates the transaction that writes this value
void SpectrumNoPartialPreSchedTable::ClearGet(T* tx, const K& k, size_t version) {
    DLOG(INFO) << "remove read record " << tx->id << "(" << tx << ")" << " from " << KeyHasher()(k) % 1000 << std::endl;
    Table::Put(k, [&](V& _v) {
        auto vit = _v.entries.begin();
        auto end = _v.entries.end();
        while (vit != end) {
            if (vit->version != version) {
                ++vit; continue;
            }
            DLOG(INFO) << "remove " << tx->id << "(" << tx << ")" << " from version " << vit->version << std::endl; 
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
            if (_v.readers_default.contains(tx)) {
                DLOG(FATAL) << "didn't remove " << tx->id << "(" << tx << ")" << " still on version " << vit->version  << std::endl;
            }
        }
        #endif
    });
}

/// @brief remove versions preceeding current transaction
/// @param tx the transaction the previously wrote this entry
/// @param k the key of written entry
void SpectrumNoPartialPreSchedTable::ClearPut(T* tx, const K& k) {
    DLOG(INFO) << "remove write record before " << tx->id << "(" << tx << ")" << " from " << KeyHasher()(k) % 1000 << std::endl;
    Table::Put(k, [&](V& _v) {
        while (_v.entries.size() && _v.entries.front().version < tx->id) {
            _v.entries.pop_front();
        }
    });
}

/// @brief spectrum initialization parameters
/// @param workload the transaction generator
/// @param table_partitions the number of parallel partitions to use in the hash table
SpectrumNoPartialPreSched::SpectrumNoPartialPreSched(Workload& workload, Statistics& statistics, size_t num_executors, size_t table_partitions, EVMType evm_type):
    workload{workload},
    statistics{statistics},
    num_executors{num_executors},
    table{table_partitions},
    lock_table{table_partitions},
    stop_latch{static_cast<ptrdiff_t>(num_executors), []{}}
{
    LOG(INFO) << fmt::format("SpectrumNoPartialPreSched(num_executors={}, table_partitions={}, evm_type={})", num_executors, table_partitions, evm_type);
    workload.SetEVMType(evm_type);
}

/// @brief start spectrum protocol
/// @param num_executors the number of threads to start
void SpectrumNoPartialPreSched::Start() {
    stop_flag.store(false);
    for (size_t i = 0; i != num_executors; ++i) {
        executors.push_back(std::thread([this]{
            std::make_unique<SpectrumNoPartialPreSchedExecutor>(*this)->Run();
        }));
        PinRoundRobin(executors[i], i);
    }
}

/// @brief stop spectrum protocol
void SpectrumNoPartialPreSched::Stop() {
    stop_flag.store(true);
    for (auto& x: executors) 	{ x.join(); }
}

/// @brief spectrum executor
/// @param spectrum spectrum initialization paremeters
SpectrumNoPartialPreSchedExecutor::SpectrumNoPartialPreSchedExecutor(SpectrumNoPartialPreSched& spectrum):
    table{spectrum.table},
    lock_table{spectrum.lock_table},
    last_finalized{spectrum.last_finalized},
    last_scheduled{spectrum.last_scheduled},
    last_committed{spectrum.last_committed},
    stop_flag{spectrum.stop_flag},
    statistics{spectrum.statistics},
    workload{spectrum.workload},
    last_executed{spectrum.last_executed},
    stop_latch{spectrum.stop_latch}
{}

/// @brief generate a transaction and execute it
void SpectrumNoPartialPreSchedExecutor::Execute() {
    tx->berun_flag.store(true);
    auto tx_ref = tx.get();
    tx->InstallSetStorageHandler([tx_ref](
        const evmc::address &addr, 
        const evmc::bytes32 &key, 
        const evmc::bytes32 &value
    ) {
        auto tx = tx_ref;
        auto _key   = std::make_tuple(addr, key);
        tx->tuples_put.push_back({
            .key = _key, 
            .value = value, 
            .is_committed=false
        });
        if (tx->HasWAR()) { tx->Break(); }
        return evmc_storage_status::EVMC_STORAGE_MODIFIED;
    });
    tx->InstallGetStorageHandler([tx_ref, this](
        const evmc::address &addr, 
        const evmc::bytes32 &key
    ) {
        auto tx = tx_ref;
        auto _key   = std::make_tuple(addr, key);
        auto value  = evmc::bytes32{0};
        auto version = size_t{0};
        for (auto& tup: tx->tuples_put | std::views::reverse) {
            if (tup.key == _key) { return tup.value; }
        }
        for (auto& tup: tx->tuples_get) {
            if (tup.key == _key) { return tup.value; }
        }
        if (tx->HasWAR()) { tx->Break(); }
        // wait until the writer transcation to finalize
        while (!stop_flag.load() && tx->ShouldWait(_key) > last_finalized.load()) {
            continue;
        }
        table.Get(tx, _key, value, version);
        size_t checkpoint_id = tx->MakeCheckpoint();
        tx->tuples_get.push_back({
            .key            = _key, 
            .value          = value, 
            .version        = version,
            .tuples_put_len = tx->tuples_put.size(),
            .checkpoint_id  = checkpoint_id
        });
        return value;
    });
    DLOG(INFO) << "spectrum execute " << tx->id;
    tx->Execute();
    statistics.JournalExecute();
    statistics.JournalOperations(tx->CountOperations());
    // commit all results if possible & necessary
    for (auto entry: tx->tuples_put) {
        if (tx->HasWAR()) { break; }
        if (entry.is_committed) { continue; }
        table.Put(tx.get(), entry.key, entry.value);
        entry.is_committed = true;
    }
    // if (last_committed.load() < tx->id) {
    //     last_committed.store(tx->id + 1);
    // }
}

void SpectrumNoPartialPreSchedExecutor::ReExecute() {
    DLOG(INFO) << "spectrum-no-partial re-execute " << tx->id;
    tx->rerun_keys.clear();
    tx->ApplyCheckpoint(0);
    for (auto entry: tx->tuples_get) {
        table.RegretGet(tx.get(), entry.key, entry.version);
    }
    for (auto entry: tx->tuples_put) {
        table.RegretPut(tx.get(), entry.key);
    }
    tx->tuples_put.resize(0);
    tx->tuples_get.resize(0);
    tx->Execute();
    statistics.JournalExecute();
    statistics.JournalOperations(tx->CountOperations());
    for (auto i = size_t{0}; i < tx->tuples_put.size(); ++i) {
        auto& entry = tx->tuples_put[i];
        table.Put(tx.get(), entry.key, entry.value);
        if (tx->HasWAR()) break;
    }
}

/// @brief finalize a spectrum transaction
void SpectrumNoPartialPreSchedExecutor::Finalize() {
    DLOG(INFO) << "spectrum finalize " << tx->id;
    last_finalized.fetch_add(1, std::memory_order_seq_cst);
    for (auto entry: tx->tuples_get) {
        table.ClearGet(tx.get(), entry.key, entry.version);
    }
    for (auto entry: tx->tuples_put) {
        table.ClearPut(tx.get(), entry.key);
    }
    for (auto k: tx->predicted_get_storage) {
        lock_table.ClearGet(tx.get(), k);
    }
    for (auto k: tx->predicted_set_storage) {
        lock_table.ClearPut(tx.get(), k);
    }
    auto latency = duration_cast<microseconds>(steady_clock::now() - tx->start_time).count();
    statistics.JournalCommit(latency);
    statistics.JournalMemory(tx->mm_count);
    // tx = nullptr;
}

/// @brief schedule a transaction (put back to queue, swap a nullptr into it)
void SpectrumNoPartialPreSchedExecutor::Schedule() {
    // no available transaction, so we have to generate one!
    if(queue.Size() != 0) { tx = queue.Pop(); return; }
    tx = std::make_unique<T>(workload.Next(), last_executed.fetch_add(1));
    tx->start_time = steady_clock::now();
    // prepare lock table, and gather lock information
    for (auto k: tx->predicted_get_storage) {
        lock_table.Get(tx.get(), k);
    }
    for (auto k: tx->predicted_set_storage) {
        lock_table.Put(tx.get(), k);
    }
    // wait lock table to stablize
    while (!stop_flag.load() && last_scheduled.load() + 1 != tx->id) continue;
    last_scheduled.fetch_add(1);
}


/// @brief start an executor
void SpectrumNoPartialPreSchedExecutor::Run() {
    while (!stop_flag.load()) {
        // find smallest workable transaction
        Schedule();
        if (!tx->berun_flag.load()) {
            Execute();
        }
        if (tx->HasWAR()) {
            ReExecute();
            queue.Push(std::move(tx));
        }
        else if (last_finalized.load() + 1 == tx->id && !tx->HasWAR()) {
            Finalize();
        }else {
            queue.Push(std::move(tx));
        }
    }
    stop_latch.arrive_and_wait();
}

#undef T
#undef V
#undef K

} // namespace spectrum
