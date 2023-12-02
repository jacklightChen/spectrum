#pragma once

#include <dcc/common/LockfreeQueue.h>
#include <dcc/common/Message.h>
#include <dcc/common/Socket.h>
#include <dcc/core/ControlMessage.h>
#include <dcc/core/Dispatcher.h>
#include <dcc/core/Executor.h>
#include <dcc/core/Worker.h>
#include <dcc/core/factory/WorkerFactory.h>
#include <glog/logging.h>

#include <thread>
#include <vector>

namespace dcc {

class Coordinator {
 public:
  template <class Database, class Context>
  Coordinator(std::size_t id, Database &db, Context &context)
      : id(id),
        coordinator_num(context.peers.size()),
        peers(context.peers),
        context(context) {
    workerStopFlag.store(false);
    ioStopFlag.store(false);
    workers = WorkerFactory::create_workers(id, db, context, workerStopFlag);
  }

  ~Coordinator() = default;

  template <class Database, class Context>
  void prepare_tables(std::size_t id, Database &db, Context &context) {
    WorkerFactory::init_tables(id, db, context);
  }

  void start() {
    std::vector<std::thread> threads;

    LOG(INFO) << "Coordinator starts to run " << workers.size() << " workers.";

    for (auto i = 0u; i < workers.size(); i++) {
      if (i == workers.size() - 1 &&
          (context.protocol == "Serial" || context.protocol == "Sparkle")) {
        LOG(INFO) << "Protocol Serial/Sparkle detected! Ignore manager "
                     "initialization.";
        continue;
      }

      threads.emplace_back(&Worker::start, workers[i].get());
      if (context.cpu_affinity) {
        pin_thread_to_core(threads[i]);
      }
    }

    // run timeToRun seconds
    int timeToRun = context.time_to_run;
    auto warmup = 0, cooldown = 0;
    if (context.replay_transaction) {
      timeToRun *= 2;
    }
    auto startTime = std::chrono::steady_clock::now();

    uint64_t total_commit = 0, total_abort_no_retry = 0, total_abort_lock = 0,
             total_abort_cascade_lock = 0, total_abort_read_validation = 0,
             total_local = 0, total_si_in_serializable = 0, total_operations = 0,
             total_network_size = 0;
    uint64_t total_partial_revert[20] = {0};
    int count = 0;

    do {
      std::this_thread::sleep_for(std::chrono::seconds(1));

      uint64_t n_commit = 0, n_abort_no_retry = 0, n_abort_lock = 0, n_operations = 0,
               n_abort_cascade_lock = 0, n_abort_read_validation = 0,
               n_local = 0, n_si_in_serializable = 0, n_network_size = 0;
      uint64_t partial_revert[20] = {0};

      for (auto i = 0u; i < workers.size(); ++i) {
        n_commit += workers[i]->n_commit.exchange(0);
        n_operations += workers[i]->n_operations.exchange(0);
        n_abort_no_retry += workers[i]->n_abort_no_retry.exchange(0);
        n_abort_lock += workers[i]->n_abort_lock.exchange(0);
        n_abort_cascade_lock += workers[i]->n_abort_cascade_lock.exchange(0);
        n_abort_read_validation +=
            workers[i]->n_abort_read_validation.exchange(0);
        n_local += workers[i]->n_local.exchange(0);
        n_si_in_serializable += workers[i]->n_si_in_serializable.exchange(0);
        n_network_size += workers[i]->n_network_size.exchange(0);
        if (context.protocol == "Sparkle") {
          for (auto j = 0u; j < 20; ++j) {
            partial_revert[j] += workers[i]->partial_revert[j].exchange(0);
          }
        }
      }

      LOG(INFO) << "commit: " << n_commit << " abort: "
                << n_abort_no_retry + n_abort_lock + n_abort_read_validation
                << " (" << n_abort_no_retry << "/" << n_abort_lock << "/"
                << n_abort_read_validation << "), "
                << "cascade abort: " << n_abort_cascade_lock << ", "
                << "network size: " << n_network_size
                << ", avg network size: " << 1.0 * n_network_size / n_commit
                << ", si_in_serializable: " << n_si_in_serializable << " "
                << 100.0 * n_si_in_serializable / n_commit << " %"
                << ", local: " << 100.0 * n_local / n_commit << " %"
                << ", operations: " << n_operations
                << ", partial revert 0: " << partial_revert[0]
                << ", partial revert 1: " << partial_revert[1]
                << ", partial revert 2:  " << partial_revert[2]
                << ", partial revert 3:  " << partial_revert[3]
                << ", partial revert 4:  " << partial_revert[4]
                << ", partial revert 5:  " << partial_revert[5]
                << ", partial revert 6:  " << partial_revert[6]
                << ", partial revert 7:  " << partial_revert[7]
                << ", partial revert 8:  " << partial_revert[8]
                << ", partial revert 9:  " << partial_revert[9]
                << ", partial revert 10: " << partial_revert[10]
                << ", partial revert 11: " << partial_revert[11]
                << ", partial revert 12: " << partial_revert[12]
                << ", partial revert 13: " << partial_revert[13]
                << ", partial revert 14: " << partial_revert[14]
                << ", partial revert 15: " << partial_revert[15]
                << ", partial revert 16: " << partial_revert[16]
                << ", partial revert 17: " << partial_revert[17]
                << ", partial revert 18: " << partial_revert[18]
                << ", partial revert 19: " << partial_revert[19] << std::endl;
      count++;
      if (count > warmup && count <= timeToRun - cooldown) {
        total_commit += n_commit;
        total_abort_no_retry += n_abort_no_retry;
        total_abort_lock += n_abort_lock;
        total_abort_cascade_lock += n_abort_cascade_lock;
        total_abort_read_validation += n_abort_read_validation;
        total_local += n_local;
        total_si_in_serializable += n_si_in_serializable;
        total_network_size += n_network_size;
        for (int i = 0; i < 20; ++i) {
          total_partial_revert[i] += partial_revert[i];
        }
      }

    } while (std::chrono::duration_cast<std::chrono::seconds>(
                 std::chrono::steady_clock::now() - startTime)
                 .count() < timeToRun);

    count = timeToRun - warmup - cooldown;

    LOG(INFO)
        << "average commit: " << 1.0 * total_commit / count << " abort: "
        << 1.0 *
               (total_abort_no_retry + total_abort_lock +
                total_abort_read_validation) /
               count
        << " (" << 1.0 * total_abort_no_retry / count << "/"
        << 1.0 * total_abort_lock / count << "/"
        << 1.0 * total_abort_read_validation / count << "), "
        << "abort cascade: " << 1.0 * total_abort_cascade_lock / count << ", "
        << "network size: " << total_network_size
        << ", avg network size: " << 1.0 * total_network_size / total_commit
        << ", si_in_serializable: " << total_si_in_serializable << " "
        << 100.0 * total_si_in_serializable / total_commit << " %"
        << " , local: " << 100.0 * total_local / total_commit << " %"
        << ", operations: " << total_operations
        << " , partial revert 0: " << 1.0 * total_partial_revert[0] / count
        << " , partial revert 1: " << 1.0 * total_partial_revert[1] / count
        << " , partial revert 2:  " << 1.0 * total_partial_revert[2] / count
        << " , partial revert 3:  " << 1.0 * total_partial_revert[3] / count
        << " , partial revert 4:  " << 1.0 * total_partial_revert[4] / count
        << " , partial revert 5:  " << 1.0 * total_partial_revert[5] / count
        << " , partial revert 6:  " << 1.0 * total_partial_revert[6] / count
        << " , partial revert 7:  " << 1.0 * total_partial_revert[7] / count
        << " , partial revert 8:  " << 1.0 * total_partial_revert[8] / count
        << " , partial revert 9:  " << 1.0 * total_partial_revert[9] / count
        << " , partial revert 10: " << 1.0 * total_partial_revert[10] / count
        << " , partial revert 11: " << 1.0 * total_partial_revert[11] / count
        << " , partial revert 12: " << 1.0 * total_partial_revert[12] / count
        << " , partial revert 13: " << 1.0 * total_partial_revert[13] / count
        << " , partial revert 14: " << 1.0 * total_partial_revert[14] / count
        << " , partial revert 15: " << 1.0 * total_partial_revert[15] / count
        << " , partial revert 16: " << 1.0 * total_partial_revert[16] / count
        << " , partial revert 17: " << 1.0 * total_partial_revert[17] / count
        << " , partial revert 18: " << 1.0 * total_partial_revert[18] / count
        << " , partial revert 19: " << 1.0 * total_partial_revert[19] / count
        << std::endl;

    workerStopFlag.store(true);

    for (auto i = 0u; i < threads.size(); i++) {
      workers[i]->onExit();
      threads[i].join();
    }

    // gather throughput
    // double sum_commit = gather(1.0 * total_commit / count);
    // if (id == 0) {
    //   LOG(INFO) << "total commit: " << sum_commit;
    // }

    // make sure all messages are sent
    // std::this_thread::sleep_for(std::chrono::seconds(1));

    ioStopFlag.store(true);

    LOG(INFO) << "Coordinator exits.";
  }

 private:
  void pin_thread_to_core(std::thread &t) {
#ifndef __APPLE__
    static std::size_t core_id = context.cpu_core_id;
    LOG(INFO) << "core_id: " << core_id;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    auto _core_id = core_id;
    ++core_id;
    CPU_SET(core_id, &cpuset);
    int rc =
        pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
    CHECK(rc == 0);
#endif
  }

 private:
  std::size_t id, coordinator_num;
  const std::vector<std::string> &peers;
  Context &context;
  std::vector<std::vector<Socket>> inSockets, outSockets;
  std::atomic<bool> workerStopFlag, ioStopFlag;
  std::vector<std::shared_ptr<Worker>> workers;
};
}  // namespace dcc
