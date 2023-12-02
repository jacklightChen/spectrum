#pragma once
#include <dcc/core/Partitioner.h>
#include <dcc/protocol/AriaFB/AriaFBHelper.h>
#include <dcc/protocol/AriaFB/AriaFBTransaction.h>

namespace dcc {

template <class Database>
class AriaFB {
 public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using TransactionType = AriaFBTransaction;

  AriaFB(DatabaseType &db, ContextType &context, Partitioner &partitioner)
      : db(db), context(context), partitioner(partitioner) {}

  void abort(TransactionType &txn) { txn.abort_lock = true; }

  bool commit(TransactionType &txn) {
    auto &writeSet = txn.writeSet;
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (partitioner.has_master_partition(partitionId)) {
        auto key = writeKey.get_key();
        auto value = writeKey.get_value();

        // test intergrity
        // if (std::chrono::steady_clock::now().time_since_epoch().count() % 7
        // !=
        //     0) {
        //   //   table->update(key, value);
        // } else {
        //   table->update(key, value);
        // }

        // print key & value
        // auto &tmp_key = *static_cast<const evmc::bytes32*>(key);
        // auto &tmp_val = *static_cast<const evmc::bytes32*>(value);
        // LOG(INFO)<<"commit key: "<<silkworm::to_hex(tmp_key);
        // LOG(INFO)<<"commit value: "<<silkworm::to_hex(tmp_val);
        // table->update(key, value);
        table->update(key, value);
      }
    }

    return true;
  }

  /* the following functions are for Calvin */

  void calvin_abort(TransactionType &txn, std::size_t lock_manager_id,
                    std::size_t n_lock_manager,
                    std::size_t replica_group_size) {
    // release read locks
    calvin_release_read_locks(txn, lock_manager_id, n_lock_manager,
                              replica_group_size);
  }

  bool calvin_commit(TransactionType &txn, std::size_t lock_manager_id,
                     std::size_t n_lock_manager,
                     std::size_t replica_group_size) {
    // write to db
    calvin_write(txn, lock_manager_id, n_lock_manager, replica_group_size);

    // release read/write locks
    calvin_release_read_locks(txn, lock_manager_id, n_lock_manager,
                              replica_group_size);
    calvin_release_write_locks(txn, lock_manager_id, n_lock_manager,
                               replica_group_size);

    return true;
  }

  void calvin_write(TransactionType &txn, std::size_t lock_manager_id,
                    std::size_t n_lock_manager,
                    std::size_t replica_group_size) {
    auto &writeSet = txn.writeSet;
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (!partitioner.has_master_partition(partitionId)) {
        continue;
      }

      if (AriaFBHelper::partition_id_to_lock_manager_id(
              writeKey.get_partition_id(), n_lock_manager,
              replica_group_size) != lock_manager_id) {
        continue;
      }

      auto key = writeKey.get_key();
      auto value = writeKey.get_value();
      // auto &tmp_key = *static_cast<const evmc::bytes32*>(key);
      // auto &tmp_val = *static_cast<const evmc::bytes32*>(value);
      // LOG(INFO)<<"calvin commit key: "<<silkworm::to_hex(tmp_key);
      // LOG(INFO)<<"calvin commit value: "<<silkworm::to_hex(tmp_val);
      table->update(key, value);
    }
  }

  void calvin_release_read_locks(TransactionType &txn,
                                 std::size_t lock_manager_id,
                                 std::size_t n_lock_manager,
                                 std::size_t replica_group_size) {
    // release read locks
    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey = readSet[i];
      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (!partitioner.has_master_partition(partitionId)) {
        continue;
      }

      bool is_read_only = AriaFBHelper::isReadOnly(readKey, writeSet);

      if (!is_read_only) {
        continue;
      }

      if (AriaFBHelper::partition_id_to_lock_manager_id(
              readKey.get_partition_id(), n_lock_manager, replica_group_size) !=
          lock_manager_id) {
        continue;
      }

      auto key = readKey.get_key();
      auto value = readKey.get_value();
      std::atomic<uint64_t> &tid = table->search_metadata(key);
      AriaFBHelper::read_lock_release(tid);
    }
  }

  void calvin_release_write_locks(TransactionType &txn,
                                  std::size_t lock_manager_id,
                                  std::size_t n_lock_manager,
                                  std::size_t replica_group_size) {
    // release write lock
    auto &writeSet = txn.writeSet;

    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (!partitioner.has_master_partition(partitionId)) {
        continue;
      }

      if (AriaFBHelper::partition_id_to_lock_manager_id(
              writeKey.get_partition_id(), n_lock_manager,
              replica_group_size) != lock_manager_id) {
        continue;
      }

      auto key = writeKey.get_key();
      auto value = writeKey.get_value();

      // auto &tmp_key = *static_cast<const evmc::bytes32*>(key);
      // auto &tmp_val = *static_cast<const evmc::bytes32*>(value);
      // LOG(INFO)<<"calvin release key: "<<silkworm::to_hex(tmp_key);
      // LOG(INFO)<<"calvin release value: "<<silkworm::to_hex(tmp_val);

      std::atomic<uint64_t> &tid = table->search_metadata(key);
      AriaFBHelper::write_lock_release(tid);
    }
  }

 private:
  DatabaseType &db;
  ContextType &context;
  Partitioner &partitioner;
};
}  // namespace dcc