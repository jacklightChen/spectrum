#pragma once
#include <dcc/core/Partitioner.h>
#include <dcc/protocol/Serial/SerialTransaction.h>

namespace dcc {

template <class Database>
class Serial {
 public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using TransactionType = SerialTransaction;

  Serial(DatabaseType &db, ContextType &context, Partitioner &partitioner)
      : db(db), context(context), partitioner(partitioner) {}

  void abort(TransactionType &txn) {
    // nothing needs to be done
  }

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
        // print key & value
        // auto &tmp_key = *static_cast<const evmc::bytes32 *>(key);
        // auto &tmp_val = *static_cast<const evmc::bytes32 *>(value);
        // LOG(INFO) << "commit key: " << silkworm::to_hex(tmp_key);
        // LOG(INFO) << "commit value: " << silkworm::to_hex(tmp_val);
        table->update(key, value);
      }
    }

    return true;
  }

 private:
  DatabaseType &db;
  ContextType &context;
  Partitioner &partitioner;
};
}  // namespace dcc