
#pragma once

#include <dcc/benchmark/ycsb/Context.h>
#include <dcc/benchmark/ycsb/Database.h>
#include <dcc/benchmark/ycsb/Random.h>
#include <dcc/benchmark/ycsb/Storage.h>
#include <dcc/benchmark/ycsb/Transaction.h>
#include <dcc/core/Partitioner.h>

namespace dcc {

namespace ycsb {

template <class T>
class Workload {
 public:
  using TransactionType = T;
  using DatabaseType = Database;
  using ContextType = Context;
  using RandomType = Random;
  using StorageType = Storage;

  Workload(std::size_t coordinator_id, DatabaseType &db, RandomType &random,
           Partitioner &partitioner)
      : coordinator_id(coordinator_id),
        db(db),
        random(random),
        partitioner(partitioner) {}

  std::unique_ptr<TransactionType> next_transaction(ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage) {
    // storage -> temporal tx storage
    std::unique_ptr<TransactionType> p = std::make_unique<ReadModifyWrite<T>>(
        coordinator_id, partition_id, db, context, random, partitioner,
        storage);

    return p;
  }

  std::unique_ptr<TransactionType> next_transaction_kvstore(
      ContextType &context, std::size_t partition_id, StorageType &storage) {
    // storage -> temporal tx storage
    // std::unique_ptr<TransactionType> p =
    //     std::make_unique<ReadModifyWrite<Transaction>>(
    //         coordinator_id, partition_id, db, context, random, partitioner,
    //         storage);
    std::unique_ptr<TransactionType> p =
        std::make_unique<KVStore<TransactionType>>(coordinator_id, partition_id,
                                                   db, context, random,
                                                   partitioner, storage);
    return p;
  }

 private:
  std::size_t coordinator_id;
  DatabaseType &db;
  RandomType &random;
  Partitioner &partitioner;
};

}  // namespace ycsb
}  // namespace dcc
