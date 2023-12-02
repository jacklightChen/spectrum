#pragma once

#include <dcc/benchmark/ycsb/Context.h>
#include <dcc/benchmark/ycsb/Random.h>
#include <dcc/benchmark/ycsb/Schema.h>
#include <dcc/benchmark/ycsb/Storage.h>
#include <dcc/common/Operation.h>
#include <dcc/core/Partitioner.h>
#include <dcc/core/Table.h>
#include <dcc/core/factory/TableFactory.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>
#include <unordered_map>
#include <vector>

#include "silkworm/common/base.hpp"
// silkworm
#include <silkworm/common/util.hpp>
#include <silkworm/execution/address.hpp>
#include <silkworm/state/in_memory_state.hpp>
#include <silkworm/state/intra_block_state.hpp>

using namespace evmc::literals;

namespace dcc {
namespace ycsb {

class Database {
 public:
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = Context;
  using RandomType = Random;
  using StorageType = Storage;

  ITable *find_table(std::size_t table_id, std::size_t partition_id) {
    DCHECK(table_id < tbl_vecs.size());
    DCHECK(partition_id < tbl_vecs[table_id].size());
    return tbl_vecs[table_id][partition_id];
  }

  template <class InitFunc>
  void initTables(const std::string &name, InitFunc initFunc,
                  std::size_t partitionNum, std::size_t threadsNum,
                  Partitioner *partitioner) {
    std::vector<int> all_parts;
    // do not effect single node
    for (auto i = 0u; i < partitionNum; i++) {
      if (partitioner == nullptr ||
          partitioner->is_partition_replicated_on_me(i)) {
        all_parts.push_back(i);
      }
    }

    std::vector<std::thread> v;
    auto now = std::chrono::steady_clock::now();

    for (auto threadID = 0u; threadID < threadsNum; threadID++) {
      v.emplace_back([=]() {
        for (auto i = threadID; i < all_parts.size(); i += threadsNum) {
          auto partitionID = all_parts[i];
          initFunc(partitionID);
        }
      });
    }
    for (auto &t : v) {
      t.join();
    }
    LOG(INFO) << name << " initialization finished in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
  }

  void initialize(Context &context) {
    // HZC add
    // deploy contract
    evmc::address caller{0x8e4d1ea201b908ab5e1f5a1c3f9f1b4f6c1e9cf1_address};
    evmc::address contract{0x3589d05a1ec4af9f65b0e5554e645707775ee43c_address};
    // silkworm::Bytes
    // code{*silkworm::from_hex("608060405234801561001057600080fd5b50600436106100365760003560e01c80631ab06ee51461003b5780639507d39a14610057575b600080fd5b6100556004803603810190610050919061023b565b610087565b005b610071600480360381019061006c919061020e565b6101dd565b60405161007e919061028a565b60405180910390f35b8060008084815260200190815260200160002081905550806000806001856100af91906102a5565b815260200190815260200160002081905550806000806002856100d291906102a5565b815260200190815260200160002081905550806000806003856100f591906102a5565b8152602001908152602001600020819055508060008060048561011891906102a5565b8152602001908152602001600020819055508060008060058561013b91906102a5565b8152602001908152602001600020819055508060008060068561015e91906102a5565b8152602001908152602001600020819055508060008060078561018191906102a5565b815260200190815260200160002081905550806000806008856101a491906102a5565b815260200190815260200160002081905550806000806009856101c791906102a5565b8152602001908152602001600020819055505050565b6000806000838152602001908152602001600020549050919050565b60008135905061020881610339565b92915050565b60006020828403121561022457610223610334565b5b6000610232848285016101f9565b91505092915050565b6000806040838503121561025257610251610334565b5b6000610260858286016101f9565b9250506020610271858286016101f9565b9150509250929050565b610284816102fb565b82525050565b600060208201905061029f600083018461027b565b92915050565b60006102b0826102fb565b91506102bb836102fb565b9250827fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff038211156102f0576102ef610305565b5b828201905092915050565b6000819050919050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052601160045260246000fd5b600080fd5b610342816102fb565b811461034d57600080fd5b5056fea2646970667358221220d24ef49785b4641127e344dafc9f8590ce12c29572b5c4cd5192e77973f5c63e64736f6c63430008070033")};

    silkworm::Bytes code{*silkworm::from_hex(
        "608060405234801561001057600080fd5b50600436106100365760003560e01c80631a"
        "b06ee51461003b5780639507d39a14610057575b600080fd5b61005560048036038101"
        "9061005091906100f9565b610087565b005b610071600480360381019061006c919061"
        "0139565b6100a2565b60405161007e9190610175565b60405180910390f35b80600080"
        "848152602001908152602001600020819055505050565b600080600083815260200190"
        "8152602001600020549050919050565b600080fd5b6000819050919050565b6100d681"
        "6100c3565b81146100e157600080fd5b50565b6000813590506100f3816100cd565b92"
        "915050565b600080604083850312156101105761010f6100be565b5b600061011e8582"
        "86016100e4565b925050602061012f858286016100e4565b9150509250929050565b60"
        "006020828403121561014f5761014e6100be565b5b600061015d848285016100e4565b"
        "91505092915050565b61016f816100c3565b82525050565b600060208201905061018a"
        "6000830184610166565b9291505056fea26469706673582212205d39501fc71fb820c0"
        "10df48b89bc2c9d45b75fc82c7ed62b64d370e2cff372e64736f6c6343000812003"
        "3")};

    state_db.set_code(contract, code);

    std::size_t coordinator_id = context.coordinator_id;
    std::size_t partitionNum = context.partition_num;
    std::size_t threadsNum = context.worker_num;

    auto partitioner = PartitionerFactory::create_partitioner(
        context.partitioner, coordinator_id, context.coordinator_num);

    // partitionNum = tbl_ycsb_vec size
    for (auto partitionID = 0u; partitionID < partitionNum; partitionID++) {
      auto ycsbTableID = ycsb::tableID;
      // tbl_ycsb_vec.push_back(
      //     TableFactory::create_table<9973, ycsb::key, ycsb::value>(
      //         context, ycsbTableID, partitionID));
      tbl_ycsb_vec.push_back(
          TableFactory::create_table<9973, evmc::bytes32, evmc::bytes32>(
              context, ycsbTableID, partitionID));
    }

    // there is 1 table in ycsb
    tbl_vecs.resize(1);

    auto tFunc = [](std::unique_ptr<ITable> &table) { return table.get(); };

    std::transform(tbl_ycsb_vec.begin(), tbl_ycsb_vec.end(),
                   std::back_inserter(tbl_vecs[0]), tFunc);

    using std::placeholders::_1;
    initTables(
        "ycsb",
        [&context, this](std::size_t partitionID) {
          // LOG(INFO) << "enter lambda";
          ycsbInit(context, partitionID);
        },
        partitionNum, threadsNum, partitioner.get());
  }

  void apply_operation(const Operation &operation) {
    CHECK(false);  // not supported
  }

  silkworm::IntraBlockState &state_db_wrapper() { return state_db; }

 private:
  void ycsbInit(Context &context, std::size_t partitionID) {
    Random random;
    ITable *table = tbl_ycsb_vec[partitionID].get();

    std::size_t keysPerPartition =
        context.keysPerPartition;  // 5M keys per partition
    // LOG(INFO) << "keysPerPartition: "<<keysPerPartition;
    std::size_t partitionNum = context.partition_num;
    std::size_t totalKeys = keysPerPartition * partitionNum;

    // if (context.strategy == PartitionStrategy::RANGE) {
    //   LOG(INFO) << "enter range: ";
    //   // use range partitioning

    //   for (auto i = partitionID * keysPerPartition;
    //        i < (partitionID + 1) * keysPerPartition; i++) {

    //     DCHECK(context.getPartitionID(i) == partitionID);

    //     ycsb::key key(i);
    //     ycsb::value value;
    //     value.Y_F01.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F02.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F03.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F04.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F05.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F06.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F07.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F08.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F09.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F10.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));

    //     table->insert(&key, &value);
    //   }
    // } else {
    //   LOG(INFO) << "enter round-robin: ";
    //   // use round-robin hash partitioning

    //   for (auto i = partitionID; i < totalKeys; i += partitionNum) {
    //     DCHECK(context.getPartitionID(i) == partitionID);

    //     ycsb::key key(i);
    //     ycsb::value value;
    //     value.Y_F01.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F02.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F03.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F04.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F05.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F06.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F07.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F08.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F09.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));
    //     value.Y_F10.assign(random.a_string(YCSB_FIELD_SIZE,
    //     YCSB_FIELD_SIZE));

    //     table->insert(&key, &value);
    //     // LOG(INFO) << "insert succ"<<static_cast<int32_t>(key.Y_KEY);
    //   }
    // }
  }

 private:
  std::vector<std::vector<ITable *>> tbl_vecs;
  std::vector<std::unique_ptr<ITable>> tbl_ycsb_vec;
  // for vm
  silkworm::InMemoryState db;
  silkworm::IntraBlockState state_db{db};
};
}  // namespace ycsb
}  // namespace dcc
