#include <spectrum/protocol/sparkle.hpp>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/common/hex.hpp>
#include <spectrum/common/thread-util.hpp>
#include <functional>
#include <thread>
#include <chrono>
#include <glog/logging.h>
#include <fmt/core.h>
#include <ranges>

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

/// @brief the multi-version table for sparkle
/// @param partitions the number of partitions
SparkleTable::SparkleTable(size_t partitions):
    Table<K, V, KeyHasher>{partitions}
{}

/// @brief determine transaction has to rerun
/// @return if transaction has to rerun
bool SparkleTransaction::HasRerunFlag() {
    auto guard = Guard{rerun_flag_mu};
    return rerun_flag;
}

/// @brief set rerun flag to true or false
/// @param flag the value to assign to rerun_flag
void SparkleTransaction::SetRerunFlag(bool flag) {
    auto guard = Guard{rerun_flag_mu};
    rerun_flag = flag;
}

/// @brief get a value
/// @param tx the transaction that reads the value
/// @param k the key of the read entry
/// @param v (mutated to be) the value of read entry
/// @param version (mutated to be) the version of read entry
void SparkleTable::Get(T* tx, const K& k, evmc::bytes32& v, size_t& version) {
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
            DLOG(INFO) << tx->id << "(" << tx << ")" << " read " << KeyHasher()(k) << " version " << rit->version << std::endl;
            return;
        }
        version = 0;
        DLOG(INFO) << tx->id << "(" << tx << ")" << " read " << KeyHasher()(k) << " version 0" << std::endl;
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
                    DLOG(INFO) << tx->id << " abort " << _tx->id;
                    _tx->SetRerunFlag(true);
                }
            }
            break;
        }
        for (auto _tx: _v.readers_default) {
            DLOG(INFO) << KeyHasher()(k) << " has read dependency " << "(" << _tx << ")" << std::endl;
            if (_tx->id > tx->id) {
                DLOG(INFO) << tx->id << " abort " << _tx->id;
                _tx->SetRerunFlag(true);
            }
        }
        // handle duplicated write on the same key
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
            _v.tx->SetRerunFlag(true);
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
    DLOG(INFO) << "remove read record " << tx->id << "(" << tx << ")" << " from " << KeyHasher()(k) << std::endl;
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
                DLOG(INFO) << tx->id << " abort " << _tx->id << std::endl;
                _tx->SetRerunFlag(true);
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
void SparkleTable::ClearGet(T* tx, const K& k, size_t version) {
    DLOG(INFO) << "clear get " << tx->id << std::endl;
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
void SparkleTable::ClearPut(T* tx, const K& k) {
    DLOG(INFO) << "remove write record before " << tx->id << "(" << tx << ")" << " from " << KeyHasher()(k) << std::endl;
    Table::Put(k, [&](V& _v) {
        while (_v.entries.size() && _v.entries.front().version < tx->id) {
            _v.entries.pop_front();
        }
    });
}

/// @brief sparkle initialization parameters
/// @param workload the transaction generator
/// @param table_partitions the number of parallel partitions to use in the hash table
Sparkle::Sparkle(Workload& workload, Statistics& statistics, size_t num_executors, size_t table_partitions):
    workload{workload},
    statistics{statistics},
    num_executors{num_executors},
    table{table_partitions}
{
    LOG(INFO) << fmt::format("Sparkle(num_executors={}, n_table_partitions={})", num_executors, table_partitions);
    workload.SetEVMType(EVMType::BASIC);
}

/// @brief start sparkle protocol
void Sparkle::Start() {
    stop_flag.store(false);
    for (size_t i = 0; i != num_executors; ++i) {
        DLOG(INFO) << "start executor " << i << std::endl;
        executors.push_back(std::thread([this] {
            std::make_unique<SparkleExecutor>(*this)->Run();
        }));
        PinRoundRobin(executors[i], i);
    }
}

/// @brief stop sparkle protocol
void Sparkle::Stop() {
    for (size_t i = 0; i != num_executors; ++i) {
        executors[i].join();
    }
}

/// @brief sparkle executor
/// @param sparkle sparkle initialization paremeters
SparkleExecutor::SparkleExecutor(Sparkle& sparkle):
    table{sparkle.table},
    last_finalized{sparkle.last_finalized},
    statistics{sparkle.statistics},
    stop_flag{sparkle.stop_flag},
    workload{sparkle.workload},
    last_execute{sparkle.last_execute}
{}

/// @brief generate a transaction and execute it
std::unique_ptr<T> SparkleExecutor::Create() {
    auto tx = std::make_unique<T>(workload.Next(), last_execute.fetch_add(1));
    if (tx == nullptr || tx->berun_flag) return tx;
    tx->start_time = steady_clock::now();
    tx->berun_flag = true;
    auto tx_ref = tx.get();
    tx->InstallSetStorageHandler([tx_ref](
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
    tx->InstallGetStorageHandler([tx_ref, this](
        const evmc::address &addr, 
        const evmc::bytes32 &key
    ) {
        DLOG(INFO) << tx_ref->id << " get";
        auto _key   = std::make_tuple(addr, key);
        auto value  = evmc::bytes32{0};
        auto version = size_t{0};
        // one key from one transaction will be commited once
        for (auto& tup: tx_ref->tuples_put | std::views::reverse) {
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
    DLOG(INFO) << "sparkle execute " << tx->id;
    tx->Execute();
    statistics.JournalExecute();
    // tx->execution_count += 1;
    // if(tx->execution_count >= 10) LOG(ERROR) << tx->id << " execution " << tx->execution_count << std::endl;
    // commit all results if possible & necessary
    for (auto i = size_t{0}; i < tx->tuples_put.size(); ++i) {
        auto& entry = tx->tuples_put[i];
        if (tx->HasRerunFlag()) { tx->tuples_put.resize(i); break; }
        table.Put(tx.get(), std::get<0>(entry), std::get<1>(entry));
    }
    return tx;
}

/// @brief rollback transaction with given rollback signal
/// @param tx the transaction to rollback
void SparkleExecutor::ReExecute(SparkleTransaction* tx) {
    DLOG(INFO) << "sparkle re-execute " << tx->id;
    tx->SetRerunFlag(false);
    tx->ApplyCheckpoint(0);
    for (auto entry: tx->tuples_get) {
        table.RegretGet(tx, std::get<0>(entry), std::get<2>(entry));
    }
    for (auto entry: tx->tuples_put) {
        table.RegretPut(tx, std::get<0>(entry));
    }
    tx->tuples_put.resize(0);
    tx->tuples_get.resize(0);
    tx->Execute();
    statistics.JournalExecute();
    // tx->execution_count += 1;
    // if(tx->execution_count >= 10) LOG(ERROR) << tx->id << " execution " << tx->execution_count << std::endl;
    // commit all results if possible & necessary
    for (auto i = size_t{0}; i < tx->tuples_put.size(); ++i) {
        auto& entry = tx->tuples_put[i];
        if (tx->HasRerunFlag()) { tx->tuples_put.resize(i); break; }
        table.Put(tx, std::get<0>(entry), std::get<1>(entry));
    }
}

/// @brief start an executor
void SparkleExecutor::Run() { while (!stop_flag.load()) {
    auto tx = Create();
    if (tx == nullptr) continue;
    while (stop_flag.load()) {
        if (tx->HasRerunFlag()) {
            // sweep all operations from previous execution
            DLOG(INFO) << "re-execute " << tx->id;
            ReExecute(tx.get());
        }
        else if (last_finalized.load() + 1 == tx->id && !tx->HasRerunFlag()) {
            DLOG(INFO) << "sparkle finalize " << tx->id;
            last_finalized.fetch_add(1, std::memory_order_seq_cst);
            for (auto entry: tx->tuples_get) {
                table.ClearGet(tx.get(), std::get<0>(entry), std::get<2>(entry));
            }
            for (auto entry: tx->tuples_put) {
                table.ClearPut(tx.get(), std::get<0>(entry));
            }
            auto latency = duration_cast<microseconds>(steady_clock::now() - tx->start_time).count();
            statistics.JournalCommit(latency);
            break;
        }
    }
}}

#undef T
#undef V
#undef K

} // namespace sparkle
