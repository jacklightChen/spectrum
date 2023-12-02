#pragma once

#include <dcc/core/Defs.h>
#include <dcc/core/Partitioner.h>
#include <dcc/protocol/Serial/SerialRWKey.h>
#include <glog/logging.h>

#include <chrono>
#include <cstddef>
#include <silkworm/common/util.hpp>
#include <thread>
namespace dcc {

class SerialTransaction {
 public:
  SerialTransaction(std::size_t coordinator_id, std::size_t partition_id,
                    Partitioner &partitioner)
      : coordinator_id(coordinator_id),
        partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()),
        partitioner(partitioner) {
    reset();
  }

  virtual ~SerialTransaction() = default;

  void reset() {
    abort_lock = false;
    abort_no_retry = false;
    abort_read_validation = false;
    execution_phase = false;
    readSet.clear();
    writeSet.clear();
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
    SerialRWKey readKey;

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
    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_read_init(std::size_t table_id, std::size_t partition_id,
                            const KeyType &key, ValueType &value) {
    SerialRWKey readKey;

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
    // readRequestHandler(readKey, id, 0);
    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value) {
    SerialRWKey readKey;

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
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {
    SerialRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    auto &tuple = tupleList[tuple_num];
    tuple = std::make_tuple(key, value);
    auto &update_key = std::get<0>(tuple);
    auto &update_val = std::get<1>(tuple);
    tuple_num++;

    writeKey.set_key(&update_key);
    writeKey.set_value(&update_val);

    add_to_write_set(writeKey);

    // KeyType * newkey = new KeyType(key);
    // const ValueType newvalue = value;
    // void *k1 = (void *)&key;
    // auto &tmp1 = *static_cast<evmc::bytes32*>(k1);

    // void *k2 = (void *)&value;
    // auto &tmp2 = *static_cast<evmc::bytes32*>(k2);

    // LOG(INFO)<<"reproduce k: "<<silkworm::to_hex(tmp1);
    // LOG(INFO)<<"reproduce v: "<<silkworm::to_hex(tmp2);
  }

  std::size_t add_to_read_set(const SerialRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const SerialRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

  void set_id(std::size_t id) { this->id = id; }

  void set_tid_offset(std::size_t offset) { this->tid_offset = offset; }

  bool process_requests(std::size_t worker_id) {
    // cannot use unsigned type in reverse iteration
    for (int i = int(readSet.size()) - 1; i >= 0; i--) {
      // early return
      if (!readSet[i].get_read_request_bit()) {
        break;
      }

      SerialRWKey &readKey = readSet[i];
      readRequestHandler(readKey, id, i);
      readSet[i].clear_read_request_bit();
    }

    return false;
  }

  bool is_read_only() { return writeSet.size() == 0; }

  virtual void set_exec_type(dcc::ExecType exec_type) = 0;

  virtual void set_input_param(std::string &str) = 0;

  virtual void set_input_param_replay(std::string &str) = 0;

 public:
  std::size_t coordinator_id, partition_id, id, tid_offset;
  std::chrono::steady_clock::time_point startTime;

  bool abort_lock, abort_no_retry, abort_read_validation;
  bool execution_phase;

  // read_key, id, key_offset
  std::function<void(SerialRWKey &, std::size_t, std::size_t)>
      readRequestHandler;

  Partitioner &partitioner;
  std::vector<SerialRWKey> readSet, writeSet;
  std::tuple<evmc::bytes32, evmc::bytes32> tupleList[TUPLE_LIST_SIZE];
  std::size_t tuple_num;
};
}  // namespace dcc