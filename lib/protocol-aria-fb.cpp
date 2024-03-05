#include "./protocol-aria-fb.hpp"

/*
    This is a implementation of "Aria: A Fast and Practical Deterministic OLTP Database" (Yi Lu, Xiangyao Yu, Lei Cao, Samuel Madden). 
    In this implementation we adopt the fallback strategy and reordering strategy discussed in this paper. 
 */

namespace spectrum 
{

#define K std::tuple<evmc::address, evmc::bytes32>
#define T AriaTransaction

/// @brief execute multiple transactions in parallel
/// @param map the function to execute
/// @param batch the current aria batch to work on
void Aria::ParallelEach(
    std::function<void(T&)>         map, 
    std::vector<std::optional<T>>&  batch
) {
    pool->submit_loop(
        size_t{0}, batch.size(), 
        [&](size_t i) {
            if (batch[i] == std::nullopt) {
                batch[i].emplace(this->NextTransaction());
            }
            map(batch[i].value());
        }
    ).wait();
}

/// @brief generate a wrapped transaction with atomic id
/// @return the wrapped transactoin
T Aria::NextTransaction() {
    throw "todo";
}

/// @brief start aria protocol
void Aria::Start() {
    while(!stop_flag.load()) {
        // -- construct an empty batch
        auto batch = std::vector<std::optional<T>>();
        for (size_t i = 0; i < batch_size; ++i) {
            batch.emplace_back(std::move(std::nullopt));
        }
        // -- execution stage
        ParallelEach([&](auto& tx) {
            AriaExecutor::Execute(tx, table);
            AriaExecutor::Reserve(tx, table);
        }, batch);
        // -- first commit stage
        ParallelEach([&](auto& tx) {
            AriaExecutor::Verify(tx, table);
            if (tx.flag_conflict) { return; }
            AriaExecutor::Commit(tx, table);
        }, batch);
        // -- fallback stage
        ParallelEach([&](auto& tx) {
            if (!tx.flag_conflict) { return; }
            AriaExecutor::AcquireLock(tx, table);
            AriaExecutor::Fallback(tx, table);
            AriaExecutor::ReleaseLock(tx, table);
        }, batch);
    }
}

/// @brief stop aria protocol and return statistics
/// @return statistics of current execution
Statistics Aria::Stop() {
    this->stop_flag.store(true);
    return this->statistics;    
}

/// @brief construct an empty aria transaction
AriaTransaction::AriaTransaction(
    Transaction&& inner, 
    size_t id, size_t batch_id
):
    Transaction{std::move(inner)},
    id{id},
    batch_id{batch_id}
{}

/// @brief 
/// @param tx 
/// @param k 
void AriaTable::ReserveGet(T* tx, const K& k) {
    Table::Put(k, [&](AriaEntry& entry) {
        if (entry.batch_id_get != tx->batch_id) {
            entry.reserved_get_tx = nullptr;
            entry.batch_id_get = tx->batch_id;
        }
        if (entry.reserved_get_tx == nullptr || entry.reserved_get_tx->id < tx->id) {
            entry.reserved_get_tx = tx;
        }
    });
}

void AriaTable::ReservePut(T* tx, const K& k) {
    Table::Put(k, [&](AriaEntry& entry) {
        if (entry.batch_id_get != tx->batch_id) {
            entry.reserved_put_tx = nullptr;
            entry.batch_id_put = tx->batch_id;
        }
        if (entry.reserved_put_tx == nullptr || entry.reserved_put_tx->id < tx->id) {
            entry.reserved_put_tx = tx;
        }
    });
}

bool AriaTable::CompareReservedGet(T* tx, const K& k) {
    bool eq = true;
    Table::Put(k, [&](AriaEntry& entry) {
        eq = entry.reserved_get_tx == nullptr || 
             entry.reserved_get_tx->id >= tx->id;
    });
    return eq;
}

bool AriaTable::CompareReservedPut(T* tx, const K& k) {
    bool eq = true;
    Table::Put(k, [&](AriaEntry& entry) {
        eq = entry.reserved_put_tx == nullptr || 
             entry.reserved_put_tx->id >= tx->id;
    });
    return eq;
}

/// @brief execute a transaction and journal write operations locally
/// @param tx the transaction
/// @param table the table
void AriaExecutor::Execute(T& tx, AriaTable& table) {
    // read from the public table
    tx.UpdateGetStorageHandler([&](
        const evmc::address &addr,
        const evmc::bytes32 &key
    ) {
        // if some write on this entry is issued previously, 
        //  the read dependency will be barricated from journal. 
        auto tup = std::make_tuple(addr, key);
        if (tx.local_put.contains(tup)) {
            return tx.local_put[tup];
        }
        if (tx.local_get.contains(tup)) {
            return tx.local_get[tup];
        }
        auto value = evmc::bytes32{0};
        table.Get(tup, [&](auto& entry){
            value = entry.value;
        });
        tx.local_get[tup] = value;
        return value;
    });
    // write locally to local storage
    tx.UpdateSetStorageHandler([&](
        const evmc::address &addr, 
        const evmc::bytes32 &key,
        const evmc::bytes32 &value
    ) {
        auto tup = std::make_tuple(addr, key);
        tx.local_put[tup] = value;
        return evmc_storage_status::EVMC_STORAGE_MODIFIED;
    });
    // execute the transaction
    tx.Execute();
}

/// @brief journal the smallest reader/writer transaction to table
/// @param tx the transaction
/// @param table the aria shared table
void AriaExecutor::Reserve(T& tx, AriaTable& table) {
    // journal all entries to the reservation table
    for (auto& tup: tx.local_get) {
        table.ReserveGet(&tx, std::get<0>(tup));
    }
    for (auto& tup: tx.local_put) {
        table.ReservePut(&tx, std::get<0>(tup));
    }
}

/// @brief verify transaction by checking dependencies
/// @param tx the transaction
/// @param table the aria shared table
void AriaExecutor::Verify(T& tx, AriaTable& table) {
    // conceptually, we take a snapshot on the database before we execute a batch
    //  , and all transactions are executed viewing the snapshot. 
    // however, we want the global state transitioned 
    //  as if we executed some of these transactions sequentially. 
    // therefore, we have to pick some transactions and arange them into a sequence. 
    // this algorithm implicitly does it for us. 
    bool war = false, raw = false, waw = false;
    for (auto& tup: tx.local_get) {
        // the value is updated, snapshot contains out-dated value
        raw |= !table.CompareReservedPut(&tx, std::get<0>(tup));
    }
    for (auto& tup: tx.local_put) {
        // the value is read before, therefore we should not update it
        war |= !table.CompareReservedGet(&tx, std::get<0>(tup));
    }
    for (auto& tup: tx.local_put) {
        // if some write happened after write
        waw |= !table.CompareReservedPut(&tx, std::get<0>(tup));
    }
    tx.flag_conflict = waw || (raw && war);
}

/// @brief commit written values into table
/// @param tx the transaction
/// @param table the aria shared table
void AriaExecutor::Commit(T& tx, AriaTable& table) {
    for (auto& tup: tx.local_put) {
        table.Put(std::get<0>(tup), [&](auto& entry) {
            entry.value = std::get<1>(tup);
        });
    }
}

/// @brief release lock from table
/// @param tx 
/// @param table 
void AriaExecutor::ReleaseLock(T& tx, AriaTable& table) {
}

/// @brief acquire lock from table, only returns when last transaction has all locks or finished. 
/// @param tx the transaction
/// @param lock_table lock manager table of aria transaction
void AriaExecutor::AcquireLock(T& tx, AriaTable& lock_table) {
}

/// @brief fallback execution without constant
/// @param tx 
/// @param table 
void AriaExecutor::Fallback(T& tx, AriaTable& table) {
    // read from the public table
    tx.UpdateGetStorageHandler([&](
        const evmc::address &addr,
        const evmc::bytes32 &key
    ) {
        auto tup = std::make_tuple(addr, key);
        auto value = evmc::bytes32{0};
        table.Get(tup, [&](auto& entry){
            value = entry.value;
        });
        return value;
    });
    // write directly into the public table
    tx.UpdateSetStorageHandler([&](
        const evmc::address &addr, 
        const evmc::bytes32 &key,
        const evmc::bytes32 &value
    ) {
        auto tup = std::make_tuple(addr, key);
        table.Put(tup, [&](auto& entry){
            entry.value = value;
        });
        return evmc_storage_status::EVMC_STORAGE_MODIFIED;
    });
    tx.Execute();
}

#undef K
#undef T

} // namespace spectrum