#pragma once

#include <dcc/core/Manager.h>
#include <dcc/core/Partitioner.h>
#include <dcc/protocol/Serial/Serial.h>
#include <dcc/protocol/Serial/SerialExecutor.h>
#include <dcc/protocol/Serial/SerialTransaction.h>
#include <glog/logging.h>

#include <atomic>
#include <thread>
#include <vector>

namespace dcc {

template <class Workload>
class SerialManager : public dcc::Manager {
 public:
  using base_type = dcc::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = SerialTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  SerialManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db), epoch(0) {
    storages.resize(context.batch_size);
    transactions.resize(context.batch_size);
    LOG(INFO) << "batch size: " << context.batch_size;
  }

 public:
  RandomType random;
  DatabaseType &db;
  std::atomic<uint32_t> epoch;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
  std::atomic<uint32_t> total_abort;
};
}  // namespace dcc