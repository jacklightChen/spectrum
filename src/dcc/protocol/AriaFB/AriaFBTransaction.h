#pragma once

#include <dcc/core/Defs.h>
#include <dcc/core/Partitioner.h>
#include <dcc/protocol/AriaFB/AriaFBRWKey.h>
#include <glog/logging.h>

#include <chrono>
#include <silkworm/common/util.hpp>
#include <thread>

namespace dcc {

class AriaFBTransaction {
 public:
  AriaFBTransaction(std::size_t coordinator_id, std::size_t partition_id,
                    Partitioner &partitioner)
      : coordinator_id(coordinator_id),
        partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()),
        partitioner(partitioner) {
    reset();
  }

  virtual ~AriaFBTransaction() = default;

  void reset() {
    run_in_aria = false;

    abort_lock = false;
    abort_no_retry = false;
    abort_read_validation = false;
    execution_phase = false;
    readSet.clear();
    writeSet.clear();

    relevant = false;
    waw = false;
    war = false;
    raw = false;
    tuple_num = 0;
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
    AriaFBRWKey readKey;

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

    readRequestHandler(readKey, id, 0);
    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value) {
    CHECK(false);
    AriaFBRWKey readKey;

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
    readKey.set_write_lock_bit();
    readRequestHandler(readKey, id, 0);
    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {
    AriaFBRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    auto &tuple = tupleList[tuple_num];
    tuple = std::make_tuple(key, value);
    auto &update_key = std::get<0>(tuple);
    auto &update_val = std::get<1>(tuple);
    tuple_num++;

    writeKey.set_key(&update_key);
    writeKey.set_value(const_cast<ValueType *>(&update_val));

    writeKey.set_write_lock_bit();
    add_to_write_set(writeKey);
  }

  std::size_t add_to_read_set(const AriaFBRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const AriaFBRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

  void set_id(std::size_t id) { this->id = id; }

  void set_tid_offset(std::size_t offset) { this->tid_offset = offset; }

  bool process_requests(std::size_t worker_id) { return false; }

  bool is_read_only() { return writeSet.size() == 0; }

  void set_epoch(uint32_t epoch) { this->epoch = epoch; }

  virtual void set_exec_type(dcc::ExecType exec_type) = 0;

  virtual void set_input_param(std::string &str) = 0;

  virtual void set_input_param_replay(std::string &str) = 0;

 public:
  std::size_t coordinator_id, partition_id, id, tid_offset;
  uint32_t epoch;
  std::chrono::steady_clock::time_point startTime;
  std::size_t pendingResponses;
  std::size_t network_size;

  bool abort_lock, abort_no_retry, abort_read_validation;
  bool distributed_transaction;
  bool execution_phase;
  bool relevant, run_in_aria;
  bool waw, war, raw;

  // read_key, id, key_offset
  std::function<void(AriaFBRWKey &, std::size_t, std::size_t)>
      readRequestHandler;

  Partitioner &partitioner;
  std::vector<AriaFBRWKey> readSet, writeSet;

  std::tuple<evmc::bytes32, evmc::bytes32> tupleList[TUPLE_LIST_SIZE];
  std::size_t tuple_num;
};
}  // namespace dcc