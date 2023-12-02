#pragma once

#include <dcc/core/Defs.h>
#include <dcc/core/Partitioner.h>
#include <dcc/protocol/Spectrum/SpectrumRWKey.h>
#include <glog/logging.h>

#include <chrono>
#include <mutex>
#include <thread>
#include <tuple>
#include <unordered_map>

#include "evmc/evmc.hpp"

namespace dcc {

class SpectrumTransaction {
 public:
  SpectrumTransaction(std::size_t coordinator_id, std::size_t partition_id,
                      Partitioner &partitioner)
      : coordinator_id(coordinator_id),
        partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()),
        partitioner(partitioner) {
    reset();
  }

  virtual ~SpectrumTransaction() = default;

  void health_check(std::string msg) {
    for (auto k : readSet) {
      CHECK(k.get_key() && k.get_value()) << msg;
    }
    for (auto k : writeSet) {
      CHECK(k.get_key() && k.get_value()) << msg;
    }
  }

  void reset() {
    abort_lock = false;
    abort_no_retry = false;
    abort_read_validation = false;
    execution_phase = false;
    has_recorded_abort = false;
    write_public = false;
  }

  virtual TransactionResult execute(std::size_t worker_id) = 0;

  virtual void reset_query() = 0;

  template <class KeyType, class ValueType>
  void search_local_index(std::size_t table_id, std::size_t partition_id,
                          const KeyType &key, ValueType &value) {
    CHECK(false);
  }

  template <class KeyType, class ValueType>
  void search_for_read(std::size_t table_id, std::size_t partition_id,
                       const KeyType &key, ValueType &value) {
    CHECK(tuple_num == writeSet.size() + readSet.size());
    // LOG(INFO) << id << " rd\n"
    //   "\t" << "key: " << silkworm::to_hex(key)  << "\n"
    //   "\t" << "rdset len: " << this->readSet.size()  << "\n"
    //   "\t" << "wrset len: " << this->writeSet.size() << std::endl;
    localCheckpoint.push_back(std::make_tuple(
        key, std::make_tuple(this->readSet.size(), this->writeSet.size())));
    SpectrumRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    auto &tuple = tupleList[tuple_num];
    tuple = std::make_tuple(key, value);
    auto &update_key = std::get<0>(tuple);
    auto &update_val = std::get<1>(tuple);
    tuple_num++;

    readKey.set_key(&update_key);
    readKey.set_value(const_cast<ValueType *>(&update_val));
    readKey.set_read_request_bit();
    readRequestHandler(readKey, 0, 0);
    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value) {
    // LOG(INFO) << id << " rd\n"
    //   "\t" << "key: " << silkworm::to_hex(key)  << "\n"
    //   "\t" << "rdset len: " << this->readSet.size()  << "\n"
    //   "\t" << "wrset len: " << this->writeSet.size() << std::endl;
    localCheckpoint.push_back(std::make_tuple(
        key, std::make_tuple(this->readSet.size(), this->writeSet.size())));
    CHECK(tuple_num == writeSet.size() + readSet.size());
    SpectrumRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    auto &tuple = tupleList[tuple_num];
    tuple = std::make_tuple(key, value);
    auto &update_key = std::get<0>(tuple);
    auto &update_val = std::get<1>(tuple);
    tuple_num++;

    readKey.set_key(&update_key);
    readKey.set_value(const_cast<ValueType *>(&update_val));
    readKey.set_read_request_bit();

    readRequestHandler(readKey, 0, 0);
    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {
    CHECK(tuple_num == writeSet.size() + readSet.size());
    // LOG(INFO) << id << " wr\n"
    //   "\t" << "key: " << silkworm::to_hex(key)  << "\n"
    //   "\t" << "rdset len: " << this->readSet.size()  << "\n"
    //   "\t" << "wrset len: " << this->writeSet.size() << std::endl;
    SpectrumRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    auto &tuple = tupleList[tuple_num];
    tuple = std::make_tuple(key, value);
    auto &update_key = std::get<0>(tuple);
    auto &update_val = std::get<1>(tuple);
    tuple_num++;

    writeKey.set_key(&update_key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&update_val));
    writeRequestHandler(writeKey, 0, 0);

    add_to_write_set(writeKey);
  }

  std::size_t add_to_read_set(const SpectrumRWKey &key) {
    CHECK(key.get_key());
    is_cascade_abort.store(false);
    // LOG(INFO) << id << " add read" << "\n"
    //   "\trdset len:" << readSet.size() << std::endl;
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const SpectrumRWKey &key) {
    CHECK(key.get_key());
    is_cascade_abort.store(false);
    for (int i = 0; i < writeSet.size(); ++i) {
      CHECK(key.get_key() != writeSet[i].get_key());
    }
    // LOG(INFO) << id << " add read" << "\n"
    //   "\twrset len:" << writeSet.size() << std::endl;
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

  bool will_local_abort() {
    std::lock_guard<std::mutex> mu_lock(rollback_key_mu);
    bool x = !rollback_key.empty();
    return x;
  }

  void add_rollback_key(evmc::bytes32 key, int who, bool is_cascade) {
    std::lock_guard<std::mutex> mu_lock(rollback_key_mu);

    rollback_key.insert(key);
    is_cascade_abort.store(is_cascade);
    if (who > this->abort_by) {
      this->abort_by = who;
    }
  }

  void set_id(std::size_t id) { this->id = id; }

  void set_tid_offset(std::size_t offset) { this->tid_offset = offset; }

  bool process_requests(std::size_t worker_id) { return false; }

  bool is_read_only() { return writeSet.size() == 0; }

  virtual void set_exec_type(dcc::ExecType exec_type) = 0;

  virtual void set_input_param(std::string &str) = 0;

  virtual void set_input_param_replay(std::string &str) = 0;

 public:
  std::size_t coordinator_id, partition_id, id, tid_offset;
  std::chrono::steady_clock::time_point startTime;
  std::size_t pendingResponses;
  std::size_t network_size;

  bool abort_lock, abort_no_retry, abort_read_validation;
  bool distributed_transaction;
  bool execution_phase;

  // read_key, id, key_offset
  std::function<void(SpectrumRWKey &, std::size_t, std::size_t)>
      readRequestHandler;
  std::function<void(SpectrumRWKey &, std::size_t, std::size_t)>
      writeRequestHandler;

  Partitioner &partitioner;
  std::vector<SpectrumRWKey> readSet, writeSet;
  int writePub{-1};
  std::vector<std::tuple<evmc::bytes32, std::tuple<std::size_t, std::size_t>>>
      localCheckpoint;
  std::mutex rollback_key_mu;
  std::unordered_set<evmc::bytes32> rollback_key{};
  bool has_recorded_abort{false};
  int abort_by{-1};
  bool write_public{false};
  int stage{0};
  int windowSize{32};
  std::atomic<bool> is_cascade_abort{false};

  pthread_mutex_t waiting_lock = PTHREAD_MUTEX_INITIALIZER;
  pthread_cond_t waiting_cond = PTHREAD_COND_INITIALIZER;
  volatile int waiting = 0;

  std::tuple<evmc::bytes32, evmc::bytes32> tupleList[TUPLE_LIST_SIZE];
  std::size_t tuple_num{0};
  bool stopFlag{false};
};
}  // namespace dcc
