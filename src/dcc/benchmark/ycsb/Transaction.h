#pragma once

#include <dcc/benchmark/ycsb/Database.h>
#include <dcc/benchmark/ycsb/Query.h>
#include <dcc/benchmark/ycsb/Schema.h>
#include <dcc/benchmark/ycsb/Storage.h>
#include <dcc/common/Operation.h>
#include <dcc/core/Defs.h>
#include <dcc/core/Partitioner.h>
#include <dcc/core/Table.h>

#include <silkworm/common/hash_maps.hpp>
#include <silkworm/common/test_util.hpp>
#include <silkworm/execution/evm.hpp>

#include "glog/logging.h"

namespace dcc {
namespace ycsb {

template <class T>
class ReadModifyWrite : public T {
 public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using StorageType = Storage;

  static constexpr std::size_t keys_num = 10;

  ReadModifyWrite(std::size_t coordinator_id, std::size_t partition_id,
                  DatabaseType &db, ContextType &context, RandomType &random,
                  Partitioner &partitioner, Storage &storage)
      : T(coordinator_id, partition_id, partitioner),
        db(db),
        context(context),
        random(random),
        storage(storage),
        partition_id(partition_id),
        query(makeYCSBQuery<keys_num>()(context, partition_id, random)) {}

  virtual ~ReadModifyWrite() override = default;

  TransactionResult execute(std::size_t worker_id) override {
    // LOG(INFO)<< context.keysPerTransaction;

    DCHECK(context.keysPerTransaction == keys_num);

    int ycsbTableID = ycsb::tableID;

    for (auto i = 0u; i < keys_num; i++) {
      auto key = query.Y_KEY[i];
      storage.ycsb_keys[i].Y_KEY = key;
      if (query.UPDATE[i]) {
        this->search_for_update(ycsbTableID, context.getPartitionID(key),
                                storage.ycsb_keys[i], storage.ycsb_values[i]);
      } else {
        this->search_for_read(ycsbTableID, context.getPartitionID(key),
                              storage.ycsb_keys[i], storage.ycsb_values[i]);
      }
    }

    if (this->process_requests(worker_id)) {
      return TransactionResult::ABORT;
    }

    for (auto i = 0u; i < keys_num; i++) {
      auto key = query.Y_KEY[i];
      if (query.UPDATE[i]) {
        if (this->execution_phase) {
          RandomType local_random;
          storage.ycsb_values[i].Y_F01.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F02.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F03.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F04.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F05.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F06.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F07.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F08.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F09.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
          storage.ycsb_values[i].Y_F10.assign(
              local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        }

        this->update(ycsbTableID, context.getPartitionID(key),
                     storage.ycsb_keys[i], storage.ycsb_values[i]);
      }
    }
    return TransactionResult::READY_TO_COMMIT;
  }

  void reset_query() override {
    query = makeYCSBQuery<keys_num>()(context, partition_id, random);
  }

 private:
  DatabaseType &db;
  ContextType &context;
  RandomType &random;
  Storage &storage;
  std::size_t partition_id;
  YCSBQuery<keys_num> query;
};

template <class T>
class KVStore : public T {
 public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using StorageType = Storage;

  static constexpr std::size_t keys_num = 10;

  KVStore(std::size_t coordinator_id, std::size_t partition_id,
          DatabaseType &db, ContextType &context, RandomType &random,
          Partitioner &partitioner, Storage &storage)
      : T(coordinator_id, partition_id, partitioner),
        db(db),
        context(context),
        random(random),
        storage(storage),
        partition_id(partition_id),
        query(makeYCSBQuery<keys_num>()(context, partition_id, random)) {}

  virtual ~KVStore() override = default;

  TransactionResult execute(std::size_t worker_id) override {
    silkworm::Block block{};
    silkworm::EVM evm{block, db.state_db_wrapper(),
                      silkworm::test::kShanghaiConfig};
    // evm_{silkworm::EVM(block, db.state_db_wrapper(),
    // silkworm::test::kShanghaiConfig)}; std::string input_str{};
    // std::string input_str {"
    // 04c402f4000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000000000000000000000000000000000000000014"};

    std::string input_str_prefix{"1ab06ee5"};
    std::string input_str_key{
        "000000000000000000000000000000000000000000000000000000000000"};
    std::string input_str_val{
        "00000000000000000000000000000000000000000000000000000000000"};
    RandomType local_random;
    local_random.init_seed(
        std::chrono::steady_clock::now().time_since_epoch().count());
    input_str_key += local_random.rand_str(4);
    // avoid pure zero
    // input_str_val += local_random.rand_str(4);
    input_str_val += local_random.rand_str(4);
    input_str_val += "1";

    // std::string s = std::to_string(rand()%100000000);
    // while(s.size()<8){
    //   s="0"+s;
    // }
    // input_str_key += s;

    // s = std::to_string(rand()%100000000);
    // while(s.size()<8){
    //   s="0"+s;
    // }
    // input_str_val += s;
    std::string input_str;
    if (input.size() > 0) {
      input_str = input;
    } else {
      input_str = input_str_prefix + input_str_key + input_str_val;
      input = input_str;
    }
    // auto input_str = input_str_prefix + input_str_key + input_str_val;

    silkworm::Bytes input_code{*silkworm::from_hex(input_str)};
    evmc::address caller{0x8e4d1ea201b908ab5e1f5a1c3f9f1b4f6c1e9cf1_address};
    evmc::address contract{0x3589d05a1ec4af9f65b0e5554e645707775ee43c_address};

    silkworm::Transaction txn{};
    txn.from = caller;
    txn.to = contract;
    txn.data = input_code;

    uint64_t gas{1'000'000};

    // link wrapped txn
    // txn.wrapped_txn = dynamic_cast<SerialTransaction*>(this);
    txn.wrapped_txn = this;
    txn.worker_id = worker_id;
    txn.wrapped_exec_type = exec_type;
    if (is_init) {
      txn.is_init = true;
    }

    // if(dynamic_cast<SerialTransaction*>(this)){
    //   txn.wraped_txn_type = 1;
    // }else if(dynamic_cast<AriaFBTransaction*>(this)){
    //   txn.wraped_txn_type = 2;
    // }else if(dynamic_cast<SparkleTransaction*>(this)){
    //   txn.wraped_txn_type = 3;
    // }

    // before execute clear
    // db.state_db_wrapper().clear_journal_and_substate();
    silkworm::CallResult res{evm.execute(txn, gas)};
    if (res.status != EVMC_SUCCESS) {
      std::cout << "res.status:" << res.status << std::endl;
      return TransactionResult::ABORT;
    }

    return TransactionResult::READY_TO_COMMIT;
  }

  void reset_query() override {
    query = makeYCSBQuery<keys_num>()(context, partition_id, random);
  }

  void set_exec_type(dcc::ExecType exec_type) { this->exec_type = exec_type; }

  void set_input_param(std::string &str) {
    // TBD
    CHECK(false);
  };

  void set_input_param_replay(std::string &str) { CHECK(false); };

 private:
  DatabaseType &db;
  ContextType &context;
  RandomType &random;
  Storage &storage;
  std::size_t partition_id;
  YCSBQuery<keys_num> query;
  // std::random_device rd;

  // HZC add
  // silkworm::FlatHashMap<evmc::bytes32, evmc::bytes32> current;
  // silkworm::EVM evm_;
  std::size_t id;
  dcc::ExecType exec_type;
  std::string input{""};
  bool is_init{false};
};

}  // namespace ycsb

}  // namespace dcc
