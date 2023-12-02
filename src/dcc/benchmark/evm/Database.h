#pragma once

#include <dcc/benchmark/evm/Context.h>
#include <dcc/benchmark/evm/Contract.h>
#include <dcc/benchmark/evm/Random.h>
#include <dcc/benchmark/evm/Schema.h>
#include <dcc/benchmark/evm/Storage.h>
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

// silkworm
#include <silkworm/common/util.hpp>
#include <silkworm/execution/address.hpp>
#include <silkworm/state/in_memory_state.hpp>
#include <silkworm/state/intra_block_state.hpp>

using namespace evmc::literals;

namespace dcc {
namespace evm {

class Database {
 public:
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = Context;
  using RandomType = Random;
  using StorageType = Storage;

  ITable *find_table(std::size_t table_id, std::size_t partition_id) {
    CHECK(table_id < tbl_vecs.size());
    CHECK(partition_id < tbl_vecs[table_id].size());
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
    // deploy contract
    evmc::address caller{0x8e4d1ea201b908ab5e1f5a1c3f9f1b4f6c1e9cf1_address};
    evmc::address contract{0x3589d05a1ec4af9f65b0e5554e645707775ee43c_address};

    ContractGen gen;
    gen.init();
    // init SINGLE_KV_STORE for insert init records (forbidden)
    state_db.set_code(contract, gen.get_contract_code(context.contract_type));
    // state_db.set_code(contract,
    //                   gen.get_contract_code(CONTRACT_TYPE::SINGLE_KV_STORE));

    std::size_t coordinator_id = context.coordinator_id;
    std::size_t partitionNum = context.partition_num;
    std::size_t threadsNum = context.worker_num;

    LOG(INFO) << "partition_num: " << partitionNum;

    auto partitioner = PartitionerFactory::create_partitioner(
        context.partitioner, coordinator_id, context.coordinator_num);

    for (auto partitionID = 0u; partitionID < partitionNum; partitionID++) {
      auto evmTableID = 0;
      tbl_evm_vec.push_back(
          TableFactory::create_table<9973, evmc::bytes32, evmc::bytes32>(
              context, evmTableID, partitionID));
    }

    tbl_vecs.resize(1);

    auto tFunc = [](std::unique_ptr<ITable> &table) { return table.get(); };

    std::transform(tbl_evm_vec.begin(), tbl_evm_vec.end(),
                   std::back_inserter(tbl_vecs[0]), tFunc);

    using std::placeholders::_1;
    initTables(
        "evm_bench",
        [&context, this](std::size_t partitionID) {
          evmInit(context, partitionID);
        },
        partitionNum, threadsNum, partitioner.get());
  }

  void apply_operation(const Operation &operation) {
    CHECK(false);  // not supported
  }

  silkworm::IntraBlockState &state_db_wrapper() { return state_db; }

  void deploy_benchmark_contract(Context &context) {
    evmc::address contract{0x3589d05a1ec4af9f65b0e5554e645707775ee43c_address};
    ContractGen gen;
    gen.init();

    CHECK(context.get_contract_type() <= gen.contract_type_num());
    
    state_db.set_code(contract, gen.get_contract_code(context.get_contract_type()));
  }

  size_t tableNum() { return tbl_vecs.size(); }

 private:
  void evmInit(Context &context, std::size_t partitionID) {
    // Random random;
    // ITable *table = tbl_evm_vec[partitionID].get();

    // std::size_t keysPerPartition =
    //     context.keysPerPartition;  // 5M keys per partition
    // // LOG(INFO) << "keysPerPartition: "<<keysPerPartition;
    // std::size_t partitionNum = context.partition_num;
    // std::size_t totalKeys = keysPerPartition * partitionNum;
  }

 private:
  std::vector<std::vector<ITable *>> tbl_vecs;
  std::vector<std::unique_ptr<ITable>> tbl_evm_vec;
  // for vm
  silkworm::InMemoryState db;
  silkworm::IntraBlockState state_db{db};
};
}  // namespace evm
}  // namespace dcc
