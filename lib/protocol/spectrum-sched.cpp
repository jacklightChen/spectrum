#include <spectrum/protocol/spectrum-sched.hpp>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/common/hex.hpp>
#include <spectrum/common/thread-util.hpp>
#include <functional>
#include <thread>
#include <chrono>
#include <glog/logging.h>
#include <ranges>
#include <fmt/core.h>

/*
    DISCARDED.
 */

namespace spectrum {

using namespace std::chrono;

#define K std::tuple<evmc::address, evmc::bytes32>
#define V SpectrumSchedVersionList
#define T SpectrumSchedTransaction

/// @brief wrap a base transaction into a spectrum transaction
/// @param inner the base transaction
/// @param id transaction id
SpectrumSchedTransaction::SpectrumSchedTransaction(Transaction&& inner, size_t id):
    Transaction{std::move(inner)},
    id{id},
    start_time{std::chrono::steady_clock::now()}
{}

/// @brief determine transaction has to rerun
/// @return if transaction has to rerun
bool SpectrumSchedTransaction::HasWAR() {
    auto guard = Guard{rerun_keys_mu};
    return rerun_keys.size() != 0;
}

/// @brief call the transaction to rerun providing the key that caused it
/// @param key the key that caused rerun
void SpectrumSchedTransaction::SetWAR(const K& key, size_t cause_id) {
    auto guard = Guard{rerun_keys_mu};
    rerun_keys.push_back(key);
    should_wait = std::max(should_wait, cause_id);
}

/// @brief the multi-version table for spectrum
/// @param partitions the number of partitions
SpectrumSchedTable::SpectrumSchedTable(size_t partitions):
    Table<K, V, KeyHasher>{partitions}
{}

/// @brief get a value
/// @param tx the transaction that reads the value
/// @param k the key of the read entry
/// @param v (mutated to be) the value of read entry
/// @param version (mutated to be) the version of read entry
void SpectrumSchedTable::Get(T* tx, const K& k, evmc::bytes32& v, size_t& version) {
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
    });
}

/// @brief put a value
/// @param tx the transaction that writes the value
/// @param k the key of the written entry
/// @param v the value to write
void SpectrumSchedTable::Put(T* tx, const K& k, const evmc::bytes32& v) {
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
                    _tx->SetWAR(k, tx->id);
                }
            }
            break;
        }
        for (auto _tx: _v.readers_default) {
            DLOG(INFO) << KeyHasher()(k) % 1000 << " has read dependency " << "(" << _tx << ")" << std::endl;
            if (_tx->id > tx->id) {
                DLOG(INFO) << tx->id << " abort " << _tx->id << std::endl;
                _tx->SetWAR(k, tx->id);
            }
        }
        // handle duplicated write on the same key
        if (rit != end && rit->version == tx->id) {
            rit->value = v;
            return;
        }
        // insert an entry
        _v.entries.insert(rit.base(), SpectrumSchedEntry {
            .value   = v,
            .version = tx->id,
            .readers = std::unordered_set<T*>()
        });
    });
}

/// @brief remove a read dependency from this entry
/// @param tx the transaction that previously read this entry
/// @param k the key of read entry
void SpectrumSchedTable::RegretGet(T* tx, const K& k, size_t version) {
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
void SpectrumSchedTable::RegretPut(T* tx, const K& k) {
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
                _tx->SetWAR(k, tx->id);
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
void SpectrumSchedTable::ClearGet(T* tx, const K& k, size_t version) {
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
void SpectrumSchedTable::ClearPut(T* tx, const K& k) {
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
SpectrumSched::SpectrumSched(Workload& workload, Statistics& statistics, size_t num_executors, size_t table_partitions, EVMType evm_type):
    workload{workload},
    statistics{statistics},
    num_executors{num_executors},
    table{table_partitions},
    stop_latch{static_cast<ptrdiff_t>(num_executors), []{}}
{
    LOG(INFO) << fmt::format("SpectrumSched(num_executors={}, table_partitions={}, evm_type={})", num_executors, table_partitions, evm_type);
    workload.SetEVMType(evm_type);
}

/// @brief start spectrum protocol
/// @param num_executors the number of threads to start
void SpectrumSched::Start() {
    stop_flag.store(false);
    for (size_t i = 0; i != num_executors; ++i) {
        executors.push_back(std::thread([this]{
            std::make_unique<SpectrumSchedExecutor>(*this)->Run();
        }));
        PinRoundRobin(executors[i], i);
    }
}

/// @brief stop spectrum protocol
void SpectrumSched::Stop() {
    stop_flag.store(true);
    for (auto& x: executors) 	{ x.join(); }
}

/// @brief spectrum executor
/// @param spectrum spectrum initialization paremeters
SpectrumSchedExecutor::SpectrumSchedExecutor(SpectrumSched& spectrum):
    table{spectrum.table},
    last_finalized{spectrum.last_finalized},
    stop_flag{spectrum.stop_flag},
    statistics{spectrum.statistics},
    workload{spectrum.workload},
    last_executed{spectrum.last_executed},
    stop_latch{spectrum.stop_latch}
{}

/// @brief generate a transaction and execute it
void SpectrumSchedExecutor::Generate() {
    tx = std::make_unique<T>(workload.Next(), last_executed.fetch_add(1));
    tx->start_time = steady_clock::now();
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
}

/// @brief rollback transaction with given rollback signal
/// @param tx the transaction to rollback
void SpectrumSchedExecutor::ReExecute() {
    DLOG(INFO) << "spectrum re-execute " << tx->id;
    // get current rerun keys
    std::vector<K> rerun_keys{};
    {
        auto guard = Guard{tx->rerun_keys_mu}; 
        std::swap(tx->rerun_keys, rerun_keys);
    }
    auto back_to = ~size_t{0};
    // find checkpoint
    for (auto& key: rerun_keys) {
        for (size_t i = 0; i < tx->tuples_get.size(); ++i) {
            if (tx->tuples_get[i].key != key) { continue; }
            back_to = std::min(i, back_to); break;
        }
    }
    // good news: we don't have to rollback
    if (back_to == ~size_t{0}) { return; }
    // bad news: we have to rollback
    auto& tup = tx->tuples_get[back_to];
    tx->ApplyCheckpoint(tup.checkpoint_id);
    for (size_t i = tup.tuples_put_len; i < tx->tuples_put.size(); ++i) {
        if (tx->tuples_put[i].is_committed) {
            table.RegretPut(tx.get(), tx->tuples_put[i].key);
        }
    }
    for (size_t i = back_to; i < tx->tuples_get.size(); ++i) {
        table.RegretGet(tx.get(), tx->tuples_get[i].key, tx->tuples_get[i].version);
    }
    tx->tuples_put.resize(tup.tuples_put_len);
    tx->tuples_get.resize(back_to);
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
}

/// @brief finalize a spectrum transaction
void SpectrumSchedExecutor::Finalize() {
    DLOG(INFO) << "spectrum finalize " << tx->id;
    last_finalized.fetch_add(1, std::memory_order_seq_cst);
    for (auto entry: tx->tuples_get) {
        table.ClearGet(tx.get(), entry.key, entry.version);
    }
    for (auto entry: tx->tuples_put) {
        table.ClearPut(tx.get(), entry.key);
    }
    auto latency = duration_cast<microseconds>(steady_clock::now() - tx->start_time).count();
    statistics.JournalCommit(latency);
    statistics.JournalMemory(tx->mm_count);
    tx = nullptr;
}

/// @brief schedule a transaction (put back to queue, swap a nullptr into it)
void SpectrumSchedExecutor::Schedule() {
    // find the earliest transaction that is waited
    if (tx != nullptr) {
        for (auto it = idle_queue.begin(); it != idle_queue.end(); ++it) {
            if ((*it)->id < tx->id) continue;
            idle_queue.insert(it, std::move(tx)); break; 
        }
    }
    if (tx != nullptr) {
        idle_queue.insert(idle_queue.end(), std::move(tx));
    }
    auto mark = last_finalized.load();
    for (auto it = idle_queue.begin(); it != idle_queue.end(); ++it) {
        if ((*it)->should_wait > mark) { continue; }
        tx = std::move(*it); idle_queue.erase(it);
        DLOG(INFO) << "spectrum schedule " << tx->id << std::endl;
        return;
    }
    // if we cannot find one, we just keep the incoming one in idle_queue and generate another. 
    Generate();
}

/// @brief start an executor
void SpectrumSchedExecutor::Run() {
    // find smallest workable transaction
    Generate();
    while (!stop_flag.load()) {
        Schedule();
        if (tx->HasWAR()) {
            ReExecute();
        }
        else if (last_finalized.load() + 1 == tx->id && !tx->HasWAR()) {
            Finalize();
        }
    }
    stop_latch.arrive_and_wait();
}

#undef T
#undef V
#undef K

} // namespace spectrum
