#pragma once

#include <dcc/common/LockfreeQueue.h>
#include <dcc/common/Message.h>
#include <atomic>
#include <glog/logging.h>
#include <queue>

namespace dcc {

class Worker {
public:
  Worker(std::size_t coordinator_id, std::size_t id)
      : coordinator_id(coordinator_id), id(id) {
    n_commit.store(0);
    n_abort_no_retry.store(0);
    n_abort_lock.store(0);
    n_abort_cascade_lock.store(0);
    n_abort_read_validation.store(0);
    n_local.store(0);
    n_si_in_serializable.store(0);
    n_network_size.store(0);
    n_operations.store(0);
    for (int i = 0; i < 20; ++i) { partial_revert[i].store(0); }
  }

  virtual ~Worker() = default;

  virtual void start() = 0;

  virtual void onExit() {}

  virtual void push_message(Message *message) = 0;

  virtual Message *pop_message() = 0;

public:
  std::size_t coordinator_id;
  std::size_t id;
  std::atomic<uint64_t> n_commit, n_abort_no_retry, n_abort_lock, n_abort_cascade_lock, 
      n_abort_read_validation, n_local, n_si_in_serializable, n_network_size, n_operations;
  std::atomic<uint64_t> partial_revert[20] = {0};
};

} // namespace dcc  
