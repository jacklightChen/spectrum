#include <gflags/gflags.h>

#include <cstdint>
#include <cstdio>
#include <evmone/execution_state.hpp>
#include <iostream>
#include <map>
#include <unordered_map>
#include <vector>

#include "evmc/evmc.h"
#include "evmc/evmc.hpp"
// #include <silkpre/precompile.h>

#include <dcc/benchmark/evm/Database.h>
#include <dcc/benchmark/ycsb/Database.h>
#include <dcc/common/Zipf.h>
#include <dcc/core/Coordinator.h>
#include <dcc/core/Macros.h>

#include <random>
#include <silkworm/chain/protocol_param.hpp>
#include <silkworm/common/test_util.hpp>
#include <silkworm/common/util.hpp>
#include <silkworm/execution/address.hpp>
#include <silkworm/execution/evm.hpp>
#include <silkworm/state/intra_block_state.hpp>

#include "third_party/evmone/lib/evmone/baseline.cpp"

// spectrum
DEFINE_int32(read_write_ratio, 80, "read write ratio");
DEFINE_int32(read_only_ratio, 0, "read only transaction ratio");
DEFINE_int32(cross_ratio, 0, "cross partition transaction ratio");
DEFINE_int32(keys, 100000, "keys in a partition.");
DEFINE_double(zipf, 0, "skew factor");
DEFINE_string(skew_pattern, "both", "skew pattern: both, read, write");
DEFINE_bool(two_partitions, false, "dist transactions access two partitions.");
DEFINE_bool(pwv_ycsb_star, false, "ycsb keys dependency.");
DEFINE_bool(global_key_space, true, "ycsb global key space.");
DEFINE_int32(contract_type, 0, "smart contract benchmark type");
DEFINE_bool(needs_check, false, "intergrity check");
DEFINE_int32(time_to_run, 20, "time to run");
DEFINE_int32(look_ahead, 0, "look ahead threads");
DEFINE_int32(initialWindowSize, 40, "initial window size");
DEFINE_int32(shrinkWindowSize, 8, "shrink window size");
DEFINE_int32(cold_record_ratio, 0, "cold_record_ratio");
DEFINE_int32(cold_record_time, 8000, "cold_record_time");

const size_t N = 9973;
using KeyType = evmc::bytes32;
using ValueType = evmc::bytes32;

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  google::ParseCommandLineFlags(&argc, &argv, true);

  dcc::evm::Context context;
  SETUP_CONTEXT(context);

  // if (FLAGS_skew_pattern == "both") {
  //     context.skewPattern = dcc::ycsb::YCSBSkewPattern::BOTH;
  // } else if (FLAGS_skew_pattern == "read") {
  //     context.skewPattern = dcc::ycsb::YCSBSkewPattern::READ;
  // } else if (FLAGS_skew_pattern == "write") {
  //     context.skewPattern = dcc::ycsb::YCSBSkewPattern::WRITE;
  // } else {
  //     CHECK(false);
  // }

  context.readWriteRatio = FLAGS_read_write_ratio;
  context.readOnlyTransaction = FLAGS_read_only_ratio;
  context.crossPartitionProbability = FLAGS_cross_ratio;
  context.keysPerPartition = FLAGS_keys;
  context.two_partitions = FLAGS_two_partitions;
  context.pwv_ycsb_star = FLAGS_pwv_ycsb_star;
  context.global_key_space = FLAGS_global_key_space;
  context.intergrity_check = FLAGS_needs_check;
  context.record_transaction = FLAGS_needs_check;
  context.contract_type = FLAGS_contract_type;
  context.time_to_run = FLAGS_time_to_run;
  context.look_ahead = FLAGS_look_ahead;
  context.initialWindowSize = FLAGS_initialWindowSize;
  context.shrinkWindowSize = FLAGS_shrinkWindowSize;
  context.cold_record_ratio = FLAGS_cold_record_ratio;
  context.cold_record_time = FLAGS_cold_record_time;

  if (FLAGS_zipf > 0) {
    context.isUniform = false;
    if (context.global_key_space) {
      dcc::Zipf::globalZipf().init(
          context.keysPerPartition * context.partition_num, FLAGS_zipf);
    } else {
      dcc::Zipf::globalZipf().init(context.keysPerPartition, FLAGS_zipf);
    }
  }

  dcc::evm::Database db;
  db.initialize(context);

  dcc::Coordinator c(FLAGS_id, db, context);

  // use serial executor to fill records
  WORKING = false;
  c.prepare_tables(FLAGS_id, db, context);

  // sleep(20);
  WORKING = true;
  c.start();

  WORKING = false;
  if (context.intergrity_check) {
    // sort transactions by id
    std::sort(context.record.begin(), context.record.end(), std::greater<>());
    // for (auto i = 0; i < 200; i += 1) {
    //     LOG(INFO) << "record row: (" << std::get<0>(context.record[i]) << ",
    //     "
    //                         << std::get<1>(context.record[i]) << ")";
    // }
    context.record_transaction = false;
    context.replay_transaction = true;
    context.protocol = "Serial";
    dcc::evm::Database another_db;
    another_db.initialize(context);
    dcc::Coordinator another_c(FLAGS_id, another_db, context);
    another_c.prepare_tables(FLAGS_id, another_db, context);
    another_c.start();
    CHECK(context.record.empty());
    assert(another_db.tableNum() == db.tableNum());
    auto tableNum = db.tableNum();
    for (size_t i = 0; i < tableNum; ++i) {
      auto partitionNum = context.partition_num;
      for (size_t j = 0; j < partitionNum; ++j) {
        auto keys = static_cast<dcc::Table<N, KeyType, ValueType> *>(
                        another_db.find_table(i, j))
                        ->getKeys();
        auto tbl = db.find_table(i, j);
        auto another_tbl = another_db.find_table(i, j);
        for (auto k : keys) {
          auto value = tbl->search_value_prev(&k, ~(uint64_t)0);
          auto another_value = another_tbl->search_value_prev(&k, ~(uint64_t)0);
          auto pre_val = *static_cast<const evmc::bytes32 *>(value);
          auto aft_val = *static_cast<const evmc::bytes32 *>(another_value);
          CHECK(pre_val == aft_val) << "compare failed";
        }
      }
    }
  }

  // test_vm();
  return 0;
}