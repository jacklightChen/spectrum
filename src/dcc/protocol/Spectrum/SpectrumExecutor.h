#pragma once

#include <dcc/common/Percentile.h>
#include <dcc/core/Defs.h>
#include <dcc/core/Delay.h>
#include <dcc/core/Partitioner.h>
#include <dcc/core/Worker.h>
#include <dcc/protocol/Spectrum/Spectrum.h>
#include <dcc/protocol/Spectrum/SpectrumHelper.h>
#include <unistd.h>

#include <chrono>
#include <cstddef>
#include <intx/intx.hpp>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "dcc/benchmark/evm/Transaction.h"
#include "glog/logging.h"

namespace dcc {

template <class Workload>
class SpectrumExecutor : public Worker {
 public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = SpectrumTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");

  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = Spectrum<DatabaseType>;

  SpectrumExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                  ContextType &context,
                  std::vector<std::unique_ptr<TransactionType>> &transactions,
                  std::vector<StorageType> &storages,
                  std::atomic<uint32_t> &epoch,
                  std::atomic<uint32_t> &worker_status,
                  std::atomic<uint32_t> &total_abort,
                  std::atomic<uint32_t> &n_complete_workers,
                  std::atomic<uint32_t> &n_started_workers,
                  std::atomic<bool> &stopFlag, std::atomic<uint32_t> &NEXT_TX)
      : Worker(coordinator_id, id),
        db(db),
        context(context),
        transactions(transactions),
        storages(storages),
        epoch(epoch),
        worker_status(worker_status),
        total_abort(total_abort),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(PartitionerFactory::create_partitioner(
            context.partitioner, coordinator_id, context.coordinator_num)),
        workload(coordinator_id, db, random, *partitioner),
        random(reinterpret_cast<uint64_t>(this)),
        protocol(db, context, *partitioner),
        delay(std::make_unique<SameDelay>(
            coordinator_id, context.coordinator_num, context.delay_time)),
        stopFlag(stopFlag),
        initialWindowSize(context.initialWindowSize),
        shrinkWindowSize(context.shrinkWindowSize),
        NEXT_TX(NEXT_TX) {}

  ~SpectrumExecutor() = default;

  void push_message(Message *message) override {}

  Message *pop_message() override { return nullptr; }

  void start() override {
    LOG(INFO) << "SpectrumExecutor " << id << " started. ";

    auto i = id;
    int size = transactions.size();
    std::queue<std::unique_ptr<TransactionType>> pool;
    // for (int j = 0; j < context.look_ahead; ++j) {
    //   auto partition_id = get_partition_id();
    //   int vec_size = transactions.size();
    //   int tx_idx = i % vec_size;
    //   auto _transaction = workload.next_transaction(context, partition_id,
    //                                                 storages[tx_idx], i + 1);
    //   _transaction->health_check("boom");
    //   _transaction->set_id(i + 1);
    //   _transaction->stage = 0;
    //   // LOG(INFO) << "<- " << _transaction->id << _transaction->stage <<
    //   // std::endl;
    //   auto &_evm_transaction =
    //       *static_cast<dcc::evm::Invoke<TransactionType>
    //       *>(_transaction.get());
    //   _evm_transaction.evm.id = i + 1;
    //   pool.push(std::move(_transaction));
    //   i += context.worker_num;
    // }

    while (!stopFlag.load()) {
      auto _partition_id = get_partition_id();
      int _vec_size = transactions.size();
      int _tx_idx = i % _vec_size;
      auto _transaction = workload.next_transaction(context, _partition_id,
                                                    storages[_tx_idx], i + 1);
      auto &_evm_transaction =
          *static_cast<dcc::evm::Invoke<TransactionType> *>(_transaction.get());
      _evm_transaction.evm.id = i + 1;
      _transaction->health_check("boom");
      _transaction->set_id(i + 1);
      _transaction->stage = 0;
      // LOG(INFO) << "<- " << _transaction->id << ":"
      //           << (_transaction->id == NEXT_TX.load()) <<
      //           _transaction->stage
      //           << std::endl;
      pool.push(std::move(_transaction));
      while (true) {
        // -------------------------------------------------------------------------------------
        auto ntx = NEXT_TX.load();
        auto transaction = std::move(pool.front());
        pool.pop();
        setupHandlers(*transaction);
        transaction->set_exec_type(ExecType::Exec_Spectrum);
        // LOG(INFO) << "-> " << transaction->id << ":"
        //           << transaction->localCheckpoint.size() << ":"
        //           << (ntx == transaction->id) << transaction->stage
        //           << std::endl;
        // -------------------------------------------------------------------------------------
        switch (transaction->stage) {
          case 0:
            goto EXECUTE;
          case 1:
            goto SPECULATIVE_COMMIT;
          case 2:
            goto FINAL_COMMIT;
          default:
            CHECK(false) << "unreachable";
        }
      // -------------------------------------------------------------------------------------
      EXECUTE:
        while (NEXT_TX.load() + initialWindowSize <= transaction->id ||
               (NEXT_TX.load() != transaction->id &&
                transaction->has_recorded_abort &&
                !transaction->will_local_abort() &&
                (int)NEXT_TX.load() + shrinkWindowSize <=
                    (int)transaction->abort_by)) {
          std::this_thread::yield();
        }
        if (transaction->will_local_abort()) {
          bool isRealAbort = localAbort(*transaction);
          if (isRealAbort) {
            transaction->stage = 0;
            pool.push(std::move(transaction));
            continue;
          }
        }
        transaction->stage = 0;
        transaction->execution_phase = true;
        transaction->health_check("before execution");
        // LOG(INFO) << "ex " << transaction->id << ":" << (ntx ==
        // transaction->id)
        //           << transaction->stage << std::endl;
        transaction->execute(id);
        transaction->health_check("after execution");
      // -------------------------------------------------------------------------------------
      SPECULATIVE_COMMIT:
        if (transaction->will_local_abort()) {
          bool isRealAbort = localAbort(*transaction);
          if (isRealAbort) {
            transaction->stage = 0;
            pool.push(std::move(transaction));
            continue;
          }
        }
        transaction->stage = 1;
        speculative_commit(*transaction);
        // LOG(INFO) << "sc " << transaction->id << ":" << (ntx ==
        // transaction->id)
        //          << transaction->stage << std::endl;
        // -------------------------------------------------------------------------------------
      FINAL_COMMIT:
        if (transaction->will_local_abort()) {
          bool isRealAbort = localAbort(*transaction);
          if (isRealAbort) {
            transaction->stage = 0;
            pool.push(std::move(transaction));
            continue;
          }
        }
        transaction->stage = 2;
        bool fc_success = finalCommit(*transaction);
        // LOG(INFO) << "fc " << transaction->id << ":" << (ntx ==
        // transaction->id)
        //          << transaction->stage << std::endl;
        if (fc_success) {
          // LOG(INFO) << "ok " << transaction->id << ":"
          //           << (ntx == transaction->id) << transaction->stage
          //           << std::endl;
          n_commit.fetch_add(1);
          auto now = std::chrono::steady_clock::now();
          auto _latency = now - transaction->startTime;
          auto latency =
              std::chrono::duration_cast<std::chrono::microseconds>(_latency);
          percentile.add(latency.count());
          break;
        } else {
          pool.push(std::move(transaction));
          continue;
        }
      }
      i += context.worker_num;
    }

    LOG(INFO) << "SpectrumExecutor " << id << " exits. ";
  }

  void speculative_commit(TransactionType &txn) {
    for (auto i = txn.writePub + 1; i < txn.writeSet.size(); i++) {
      auto &writeKey = txn.writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();

      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();
      auto value = writeKey.get_value();
      CHECK(key && value);

      auto &tmp_key = *static_cast<const evmc::bytes32 *>(key);
      auto &tmp_val = *static_cast<const evmc::bytes32 *>(value);
      if (table->addVersion(key, value, &txn, NEXT_TX) == 0) {
        CHECK(txn.will_local_abort())
            << txn.id << "\n"
            << "\t" << silkworm::to_hex(tmp_key) << std::endl;
        return;
      };
      txn.writePub = i;
    }
    txn.write_public = true;
  }

  bool finalCommit(TransactionType &txn) {
    CHECK(txn.write_public) << txn.stage;
    if (txn.id != NEXT_TX.load() || txn.will_local_abort()) {
      return false;
    }
    if (txn.id == NEXT_TX.load() && !txn.will_local_abort()) {
      NEXT_TX.fetch_add(1);
      for (auto i = 0u; i < txn.writeSet.size(); i++) {
        auto &writeKey = txn.writeSet[i];
        auto tableId = writeKey.get_table_id();
        auto partitionId = writeKey.get_partition_id();

        auto table = db.find_table(tableId, partitionId);
        auto key = writeKey.get_key();
        auto value = writeKey.get_value();
        CHECK(key && value);

        // print key & value
        auto &tmp_key = *static_cast<const evmc::bytes32 *>(key);
        auto &tmp_val = *static_cast<const evmc::bytes32 *>(value);
        table->UnlockAndVaccum(key, &txn);
      }

      for (auto i = 0u; i < txn.readSet.size(); i++) {
        auto &readKey = txn.readSet[i];
        auto tableId = readKey.get_table_id();
        auto partitionId = readKey.get_partition_id();
        auto table = db.find_table(tableId, partitionId);
        auto key = readKey.get_key();
        table->RemoveFromDeps(key, &txn);
      }
      auto &evmTxn = *static_cast<dcc::evm::Invoke<TransactionType> *>(&txn);
      n_operations.fetch_add(evmTxn.evm.execution_state->count);
      return true;
    } else {
      return false;
    }
  }

  bool localAbort(TransactionType &txn) {
    // auto rollback_key_guard = std::lock_guard{txn.rollback_key_mu};
    n_abort_lock.fetch_add(1);
    if (txn.is_cascade_abort.load()) {
      n_abort_cascade_lock.fetch_add(1);
    }
    auto rollback_key_set = std::unordered_set<evmc::bytes32>{};
    {
      std::lock_guard<std::mutex> mu_lock(txn.rollback_key_mu);
      std::swap(txn.rollback_key, rollback_key_set);
    }
    auto writeSetLength = 0u;
    auto readSetLength = 0u;
    evmc::bytes32 rollback_key{0};
    txn.health_check("before rollback");
    // {
    //   auto ss = std::stringstream{""};
    //   ss << txn.id << " abort key set: \n";
    //   for (auto& _k: rollback_key_set) {
    //     ss << "\t" << silkworm::to_hex(_k) << "\n";
    //   }
    //   ss << "local checkpoints: \n";
    //   for (auto& _k: txn.localCheckpoint) {
    //     ss << "\t" << silkworm::to_hex(std::get<0>(_k)) << "\n";
    //   }
    //   LOG(INFO) << ss.str() << std::endl;
    // }
    for (auto i = 0; i <= txn.localCheckpoint.size(); ++i) {
      if (i == txn.localCheckpoint.size()) {
        return false;
      }
      if (!rollback_key_set.contains(std::get<0>(txn.localCheckpoint[i]))) {
        continue;
      } else {
        std::tie(readSetLength, writeSetLength) =
            std::get<1>(txn.localCheckpoint[i]);
        rollback_key = std::get<0>(txn.localCheckpoint[i]);
        partial_revert[txn.localCheckpoint.size() - i].fetch_add(1);
        txn.localCheckpoint.resize(i);
        break;
      }
    }
    // LOG(INFO) << "ab " << txn.id << ":" << txn.localCheckpoint.size()
    //           << std::endl;
    // ---------------------------------------------------------------
    txn.health_check("before reset write keys");
    for (auto i = writeSetLength; i < txn.writeSet.size(); i++) {
      auto &writeKey = txn.writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();

      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();
      auto value = writeKey.get_value();

      table->UnlockAndRemove(key, &txn);
    }
    // ---------------------------------------------------------------
    txn.writePub = std::min(txn.writePub, int{writeSetLength} - 1);
    auto &evmTxn = *static_cast<dcc::evm::Invoke<TransactionType> *>(&txn);
    evmTxn.evm.execution_state->partial_revert_key =
        intx::be::load<intx::uint256>(rollback_key);
    evmTxn.evm.execution_state->will_partial_revert = true;
    txn.health_check("after set rollback key");
    // ---------------------------------------------------------------
    txn.reset();
    txn.health_check("before resize");
    // ---------------------------------------------------------------
    txn.writeSet.resize(writeSetLength);
    txn.readSet.resize(readSetLength);
    txn.tuple_num = readSetLength + writeSetLength;
    // ---------------------------------------------------------------
    txn.health_check("after resize");
    txn.write_public = false;
    txn.stage = 0;
    txn.has_recorded_abort = true;
    // ---------------------------------------------------------------
    return true;
  }

  std::size_t get_partition_id() {
    std::size_t partition_id;

    CHECK(context.partition_num % context.coordinator_num == 0);

    auto partition_num_per_node = context.partition_num;
    partition_id = random.uniform_dist(0, partition_num_per_node - 1);
    partition_id *= context.coordinator_num;
    partition_id += coordinator_id;
    CHECK(partitioner->has_master_partition(partition_id));
    return partition_id;
  }

  void setupHandlers(TransactionType &txn) {
    txn.readRequestHandler = [this, &txn](SpectrumRWKey &readKey,
                                          std::size_t tid,
                                          uint32_t key_offset) {
      auto table_id = readKey.get_table_id();
      auto partition_id = readKey.get_partition_id();
      const void *key = readKey.get_key();
      void *value = readKey.get_value();

      ITable *table = db.find_table(table_id, partition_id);
      if (context.cold_record_ratio > 0) {
        // simulate disk read
        auto rand = txn.id % 100 + 1;
        if (rand <= context.cold_record_ratio) {
          std::this_thread::sleep_for(
              std::chrono::nanoseconds(context.cold_record_time));
        }
      }
      auto row = table->read(key, &txn);
      SpectrumHelper::read(row, value, table->value_size());
      if (txn.will_local_abort()) {
        auto &evmTxn =
            *static_cast<dcc::evm::Invoke<SpectrumTransaction> *>(&txn);
        evmTxn.evm.execution_state->signal_early_interrupt = true;
      }
    };

    txn.writeRequestHandler = [this, &txn](SpectrumRWKey &writeKey,
                                           std::size_t tid,
                                           uint32_t key_offset) {
      auto table_id = writeKey.get_table_id();
      auto partition_id = writeKey.get_partition_id();
      const void *key = writeKey.get_key();
      void *value = writeKey.get_value();

      ITable *table = db.find_table(table_id, partition_id);
      while (table->lock(key, &txn) == 0) {
        std::this_thread::yield();
      }
      if (txn.will_local_abort()) {
        auto &evmTxn =
            *static_cast<dcc::evm::Invoke<SpectrumTransaction> *>(&txn);
        evmTxn.evm.execution_state->signal_early_interrupt = true;
      }
    };
  }

  void onExit() override {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << " us (50%) " << percentile.nth(75) << " us (75%) "
              << percentile.nth(95) << " us (95%) " << percentile.nth(99)
              << " us (99%).";
  }

 private:
  DatabaseType &db;
  ContextType &context;
  std::vector<std::unique_ptr<TransactionType>> &transactions;
  std::vector<StorageType> &storages;
  std::atomic<uint32_t> &epoch, &worker_status, &total_abort;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  std::unique_ptr<Partitioner> partitioner;
  WorkloadType workload;
  RandomType random;
  ProtocolType protocol;
  std::unique_ptr<Delay> delay;
  Percentile<int64_t> percentile;
  std::atomic<bool> &stopFlag;
  std::atomic<uint32_t> &NEXT_TX;
  int64_t initialWindowSize = 40, shrinkWindowSize = 8;
};

}  // namespace dcc
