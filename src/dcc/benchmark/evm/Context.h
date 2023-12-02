#pragma once

#include <dcc/benchmark/evm/Random.h>
#include <dcc/core/Context.h>
#include <glog/logging.h>

#include <cstddef>
#include <mutex>
#include <queue>
#include <random>
#include <tuple>

namespace dcc {
namespace evm {
enum class PartitionStrategy { RANGE, ROUND_ROBIN, RANDOM, FIXED };

class Context : public dcc::Context {
 public:
  // std::size_t getPartitionID(std::size_t key) const {
  //   DCHECK(key >= 0 && key < partition_num * keysPerPartition);

  //   if (strategy == PartitionStrategy::FIXED) {
  //     return 0;
  //   }

  //   // if (strategy == PartitionStrategy::RANDOM) {
  //   //   // return random.uniform_dist(0, partition_num);
  //   // }

  //   if (strategy == PartitionStrategy::ROUND_ROBIN) {
  //     return key % partition_num;
  //   } else {
  //     return key / keysPerPartition;
  //   }
  // }

  // std::size_t get_partition_id(const evmc::bytes32 &key,
  //                             std::size_t partition_num) {
  //   // test key are in int64
  //   auto key_sets = key.bytes;
  //   uint64_t v = 0;

  //   v += uint64_t(key_sets[24]) << 56;
  //   v += uint64_t(key_sets[26]) << 48;
  //   v += uint64_t(key_sets[27]) << 40;
  //   v += uint64_t(key_sets[28]) << 32;
  //   v += uint64_t(key_sets[29]) << 24;
  //   v += uint64_t(key_sets[30]) << 8;
  //   v += uint64_t(key_sets[31]) << 0;

  //   // PartitionStrategy::ROUND_ROBIN
  //   return v % partition_num;
  // }

 public:
  int readWriteRatio = 0;             // out of 100
  int readOnlyTransaction = 0;        //  out of 100
  int crossPartitionProbability = 0;  // out of 100

  std::size_t keysPerTransaction = 10;
  std::size_t keysPerPartition = 200000;

  bool isUniform = true;
  bool two_partitions = false;
  bool global_key_space = false;

  PartitionStrategy strategy = PartitionStrategy::ROUND_ROBIN;
  std::vector<std::tuple<size_t, uint64_t>> record;
  std::mutex mu;
};
}  // namespace evm
}  // namespace dcc
