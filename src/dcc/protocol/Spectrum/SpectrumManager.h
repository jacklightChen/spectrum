#pragma once

#include <dcc/core/Manager.h>
#include <dcc/core/Partitioner.h>
#include <dcc/protocol/Spectrum/Spectrum.h>
#include <dcc/protocol/Spectrum/SpectrumExecutor.h>
#include <dcc/protocol/Spectrum/SpectrumTransaction.h>
#include <glog/logging.h>

#include <atomic>
#include <thread>
#include <vector>

namespace dcc {

template <class Workload>
class SpectrumManager : public dcc::Manager {
 public:
  using base_type = dcc::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = SpectrumTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  SpectrumManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                  ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db), epoch(0) {
    storages.resize(context.batch_size);
    transactions.resize(context.batch_size);

    // incorrect version stored in map (FIXED)
    // storages.resize(100001);
    // transactions.resize(100001);
    NEXT_TX.store(1);
    LOG(INFO) << "batch size: " << context.batch_size;
  }

 public:
  RandomType random;
  DatabaseType &db;
  std::atomic<uint32_t> epoch;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
  std::atomic<uint32_t> total_abort;
  std::atomic<uint32_t> NEXT_TX;
};
}  // namespace dcc