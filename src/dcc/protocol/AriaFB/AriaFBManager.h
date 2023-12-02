#pragma once

#include <dcc/core/Manager.h>
#include <dcc/core/Partitioner.h>
#include <dcc/protocol/AriaFB/AriaFB.h>
#include <dcc/protocol/AriaFB/AriaFBExecutor.h>
#include <dcc/protocol/AriaFB/AriaFBTransaction.h>
#include <glog/logging.h>

#include <atomic>
#include <thread>
#include <vector>

namespace dcc {

template <class Workload>
class AriaFBManager : public dcc::Manager {
 public:
  using base_type = dcc::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = AriaFBTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  AriaFBManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db), epoch(0) {
    storages.resize(context.batch_size);
    transactions.resize(context.batch_size);
    LOG(INFO) << "batch size: " << context.batch_size;
  }

  void start() override {
    LOG(INFO) << "AriaFB Manager started. ";
    coordinator_start();
  }

  void coordinator_start() override {
    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;
    while (!stopFlag.load()) {
      // the coordinator on each machine first moves the aborted
      // transactions from the last batch earlier to the next batch and
      // set remaining transaction slots to null. then, each worker
      // threads generates a transaction using the same seed.
      epoch.fetch_add(1);

      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::AriaFB_READ);
      wait_all_workers_start();
      wait_all_workers_finish();

      // Allow each worker to commit transactions
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::AriaFB_COMMIT);
      wait_all_workers_start();
      wait_all_workers_finish();

      // clean batch now
      cleanup_batch();

      // prepare transactions for calvin and clear the metadata
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::AriaFB_Fallback_Prepare);
      wait_all_workers_start();
      wait_all_workers_finish();

      // // calvin execution
      n_started_workers.store(0);
      n_completed_workers.store(0);
      clear_lock_manager_status();
      signal_worker(ExecutorStatus::AriaFB_Fallback);
      wait_all_workers_start();
      wait_all_workers_finish();
    }
    signal_worker(ExecutorStatus::EXIT);
  }

  void cleanup_batch() { abort_tids.clear(); }

  void add_worker(const std::shared_ptr<AriaFBExecutor<WorkloadType>> &w) {
    workers.push_back(w);
  }

  void clear_lock_manager_status() { lock_manager_status.store(0); }

 public:
  RandomType random;
  DatabaseType &db;
  std::atomic<uint32_t> epoch;
  std::atomic<uint32_t> lock_manager_status;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
  std::atomic<uint32_t> total_abort;
  std::vector<int> abort_tids;
  std::vector<std::shared_ptr<AriaFBExecutor<WorkloadType>>> workers;
};
}  // namespace dcc