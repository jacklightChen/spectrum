
#pragma once

#include <dcc/benchmark/tpcc/Context.h>
#include <dcc/benchmark/tpcc/Database.h>
#include <dcc/benchmark/tpcc/Random.h>
#include <dcc/benchmark/tpcc/Storage.h>
#include <dcc/benchmark/tpcc/Transaction.h>
#include <dcc/core/Partitioner.h>

namespace dcc {

namespace tpcc {

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
    int x = random.uniform_dist(1, 100);
    std::unique_ptr<TransactionType> p;

    if (context.workloadType == TPCCWorkloadType::MIXED) {
      if (x <= 50) {
        p = std::make_unique<NewOrder<T>>(coordinator_id, partition_id, db,
                                          context, random, partitioner,
                                          storage);
      } else {
        p = std::make_unique<Payment<T>>(coordinator_id, partition_id, db,
                                         context, random, partitioner, storage);
      }
    } else if (context.workloadType == TPCCWorkloadType::NEW_ORDER_ONLY) {
      p = std::make_unique<NewOrder<T>>(coordinator_id, partition_id, db,
                                        context, random, partitioner, storage);
    } else {
      p = std::make_unique<Payment<T>>(coordinator_id, partition_id, db,
                                       context, random, partitioner, storage);
    }

    return p;
  }

 private:
  std::size_t coordinator_id;
  DatabaseType &db;
  RandomType &random;
  Partitioner &partitioner;
};

}  // namespace tpcc
}  // namespace dcc
