#pragma once

#include <dcc/benchmark/evm/Context.h>
#include <dcc/benchmark/evm/Database.h>
#include <dcc/benchmark/evm/Random.h>
#include <dcc/benchmark/evm/Storage.h>
#include <dcc/benchmark/evm/Transaction.h>
#include <dcc/core/Partitioner.h>

#include <mutex>
#include <queue>
#include <string>
#include <vector>

namespace dcc {

namespace evm {

template <class T>
class Workload {
 public:
  using TransactionType = T;
  using BenchTxn = Invoke<T>;
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
                                                    StorageType &storage,
                                                    size_t id = 0) {
    if (context.record_transaction) {
      std::lock_guard<std::mutex> lock(context.mu);
      uint64_t seed = random.get_seed();
      context.record.push_back(std::make_tuple(id, seed));
    }
    if (context.replay_transaction) {
      if (context.record.empty()) {
        std::unique_ptr<TransactionType> p =
            std::make_unique<BenchTxn>(
                coordinator_id, partition_id, db, context, random, partitioner,
                storage);
        std::string null_input_code{""};
        p->set_input_param_replay(null_input_code);
        return p;
      } else {
        size_t id = std::get<0>(context.record.back());
        random.set_seed(std::get<1>(context.record.back()));
        context.record.pop_back();
        std::unique_ptr<TransactionType> p =
            std::make_unique<BenchTxn>(
                coordinator_id, partition_id, db, context, random, partitioner,
                storage);
        return p;
      }
    }
    std::unique_ptr<TransactionType> p =
        std::make_unique<BenchTxn>(coordinator_id, partition_id,
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

}  // namespace evm
}  // namespace dcc
