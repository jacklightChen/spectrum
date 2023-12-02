#pragma once

#include <dcc/benchmark/evm/Workload.h>
#include <dcc/benchmark/tpcc/Workload.h>
#include <dcc/benchmark/ycsb/Workload.h>
#include <dcc/core/Defs.h>
#include <dcc/core/Executor.h>
#include <dcc/core/Manager.h>
#include <dcc/protocol/AriaFB/AriaFB.h>
#include <dcc/protocol/AriaFB/AriaFBExecutor.h>
#include <dcc/protocol/AriaFB/AriaFBManager.h>
#include <dcc/protocol/AriaFB/AriaFBTransaction.h>
#include <dcc/protocol/Serial/Serial.h>
#include <dcc/protocol/Serial/SerialExecutor.h>
#include <dcc/protocol/Serial/SerialManager.h>
#include <dcc/protocol/Serial/SerialTransaction.h>
#include <dcc/protocol/Sparkle/Sparkle.h>
#include <dcc/protocol/Sparkle/SparkleExecutor.h>
#include <dcc/protocol/Sparkle/SparkleManager.h>
#include <dcc/protocol/Sparkle/SparkleTransaction.h>
#include <dcc/protocol/Spectrum/Spectrum.h>
#include <dcc/protocol/Spectrum/SpectrumExecutor.h>
#include <dcc/protocol/Spectrum/SpectrumManager.h>
#include <dcc/protocol/Spectrum/SpectrumTransaction.h>
#include <glog/logging.h>

#include <unordered_set>

namespace dcc {

template <class Context>
class InferType {};

template <>
class InferType<dcc::tpcc::Context> {
 public:
  template <class T>
  using WorkloadType = dcc::tpcc::Workload<T>;
};

template <>
class InferType<dcc::ycsb::Context> {
 public:
  template <class T>
  using WorkloadType = dcc::ycsb::Workload<T>;
};

template <>
class InferType<dcc::evm::Context> {
 public:
  template <class T>
  using WorkloadType = dcc::evm::Workload<T>;
};

class WorkerFactory {
 public:
  template <class Database, class Context>
  static std::vector<std::shared_ptr<Worker>> create_workers(
      std::size_t coordinator_id, Database &db, Context &context,
      std::atomic<bool> &stop_flag) {
    // choose protocols to run
    // generate workers
    // std::unordered_set<std::string> protocols = {
    //     "TwoPL",       "Calvin",      "Bohm",         "Aria",
    //     "AriaFB",      "Pwv",         "Serial",       "Sparkle",
    //     "Spectrum" };
    std::unordered_set<std::string> protocols = {"Serial", "AriaFB", "Sparkle",
                                                 "Spectrum"};
    CHECK(protocols.count(context.protocol) == 1) << "Unsupported protocols";

    std::vector<std::shared_ptr<Worker>> workers;

    if (context.protocol == "Serial") {
      using TransactionType = dcc::SerialTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      // create manager
      // std::atomic<bool> &stop_flag
      auto manager = std::make_shared<SerialManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker
      workers.push_back(std::make_shared<SerialExecutor<WorkloadType>>(
          coordinator_id, 0, db, context, manager->transactions,
          manager->storages, manager->epoch, manager->worker_status,
          manager->total_abort, manager->n_completed_workers,
          manager->n_started_workers, stop_flag));

      workers.push_back(manager);
    } else if (context.protocol == "AriaFB") {
      CHECK(context.worker_num >= 2)
          << "AriaFB needs at least 2 workers for fallback";
      using TransactionType = dcc::AriaFBTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      // create manager

      auto manager = std::make_shared<AriaFBManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker

      std::vector<AriaFBExecutor<WorkloadType> *> all_executors;

      for (auto i = 0u; i < context.worker_num; i++) {
        auto w = std::make_shared<AriaFBExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->transactions,
            manager->storages, manager->epoch, manager->lock_manager_status,
            manager->worker_status, manager->total_abort,
            manager->n_completed_workers, manager->n_started_workers);
        workers.push_back(w);
        manager->add_worker(w);
        all_executors.push_back(w.get());
      }

      // push manager to workers
      workers.push_back(manager);

      for (auto i = 0u; i < context.worker_num; i++) {
        static_cast<AriaFBExecutor<WorkloadType> *>(workers[i].get())
            ->set_all_executors(all_executors);
      }
    } else if (context.protocol == "Sparkle" ||
               context.protocol == "Spectrum") {
      // We implement Spectrum named as Sparkle, original Sparkle is in the
      // no-partial branch, we may rename and reimplementation in the future
      using TransactionType = dcc::SparkleTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      // create manager
      auto manager = std::make_shared<SparkleManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker
      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<SparkleExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->transactions,
            manager->storages, manager->epoch, manager->worker_status,
            manager->total_abort, manager->n_completed_workers,
            manager->n_started_workers, stop_flag, manager->NEXT_TX));
      }

      workers.push_back(manager);

    } else if (context.protocol == "Spectrum") {
      using TransactionType = dcc::SpectrumTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      // create manager
      auto manager = std::make_shared<SpectrumManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker
      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<SpectrumExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->transactions,
            manager->storages, manager->epoch, manager->worker_status,
            manager->total_abort, manager->n_completed_workers,
            manager->n_started_workers, stop_flag, manager->NEXT_TX));
      }

      workers.push_back(manager);
    } else {
      CHECK(false) << "protocol: " << context.protocol << " is not supported.";
    }
    LOG(INFO) << "workers size (including manager): " << workers.size();
    return workers;
  }

  template <class Database, class Context>
  static void init_tables(std::size_t coordinator_id, Database &db,
                          Context &context) {
    using TransactionType = dcc::SerialTransaction;
    using WorkloadType =
        typename InferType<Context>::template WorkloadType<TransactionType>;

    // create manager
    std::atomic<bool> stop_flag;
    auto manager = std::make_unique<SerialManager<WorkloadType>>(
        coordinator_id, context.worker_num, db, context, stop_flag);

    // create worker
    auto exec = std::make_unique<SerialExecutor<WorkloadType>>(
        coordinator_id, 0, db, context, manager->transactions,
        manager->storages, manager->epoch, manager->worker_status,
        manager->total_abort, manager->n_completed_workers,
        manager->n_started_workers, stop_flag);

    exec->init_tables();

    // after init set benchmark contract (forbidden)
    // db.deploy_benchmark_contract(context);
  }
};
}  // namespace dcc