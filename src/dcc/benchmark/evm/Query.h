#pragma once

#include <dcc/benchmark/evm/Context.h>
#include <dcc/benchmark/evm/Contract.h>
#include <dcc/benchmark/evm/Random.h>
#include <dcc/common/Zipf.h>

#include <iomanip>
#include <sstream>
#define KEY_LENGTH 64

namespace dcc {
namespace evm {

template <std::size_t N>
struct EVMQuery {
  std::string INPUT_CODE{""};
  std::vector<int32_t> RKEY{};
  std::vector<int32_t> WKEY{};
  // std::string Y_KEY[N];
  // bool UPDATE[N];
};

inline std::string input_key_to_string(int32_t key) {
  std::ostringstream ss;
  ss << std::setw(64) << std::setfill('0') << key;
  return ss.str();
  // std::string key_str = std::to_string(key);
  // auto zero_prefix_length = KEY_LENGTH - key_str.length();
  // std::string zero_prefix = "";
  // for (auto i = 0u; i < zero_prefix_length; i++) {
  //   zero_prefix += "0";
  // }
  // return zero_prefix + key_str;
}

inline std::vector<int32_t> get_unique_keys(int _n, Context &context,
                                            uint32_t partitionID,
                                            Random &random) {
  std::vector<int32_t> vec;

  if (context.synthetic) {
    if (_n == 1) {
      while (true) {
        int32_t key = Zipf::globalZipf().value(random.next_double());
        bool has_same = false;
        for (auto it = vec.begin(); it != vec.end(); ++it) {
          has_same |= (*it == key);
        }
        if (!has_same) {
          vec.push_back(key);
          break;
        }
      }
    } else {
      int mid = _n / 2;

      for (auto i = 0; i < _n; i++) {
        if (i < mid) {
          while (true) {
            int32_t key = random.uniform_dist(
                0, static_cast<int>(context.keysPerPartition *
                                    context.partition_num) -
                       1);
            bool has_same = false;
            for (auto it = vec.begin(); it != vec.end(); ++it) {
              has_same |= (*it == key);
            }
            if (!has_same) {
              vec.push_back(key);
              break;
            }
          }
        } else {
          while (true) {
            int32_t key = Zipf::globalZipf().value(random.next_double());
            bool has_same = false;
            for (auto it = vec.begin(); it != vec.end(); ++it) {
              has_same |= (*it == key);
            }
            if (!has_same) {
              vec.push_back(key);
              break;
            }
          }
        }
      }
    }
    return vec;
  }

  for (auto i = _n - 1; i >= 0; --i) {
    if (context.isUniform) {
      while (true) {
        int32_t key = random.uniform_dist(
            0,
            static_cast<int>(context.keysPerPartition * context.partition_num) -
                1);
        bool has_same = false;
        for (auto it = vec.begin(); it != vec.end(); ++it) {
          has_same |= (*it == key);
        }
        if (!has_same) {
          vec.push_back(key);
          break;
        }
      }
    } else {
      while (true) {
        int32_t key = Zipf::globalZipf().value(random.next_double());
        bool has_same = false;
        for (auto it = vec.begin(); it != vec.end(); ++it) {
          has_same |= (*it == key);
        }
        if (!has_same) {
          vec.push_back(key);
          break;
        }
      }
    }
  }
  for (int i = 0; i < _n; ++i) {
    int j = random.uniform_dist(i, _n - 1);
    int x = vec[i];
    vec[i] = vec[j];
    vec[j] = x;
  }
  // check if unique
  // std::set<int32_t> set;
  // for (int i = 0; i < _n; i++) {
  //   set.insert(vec[i]);
  // }
  // if (set.size() != 10) CHECK(false) << "input wrong";
  return vec;
}

template <std::size_t N>
class makeEVMQuery {
 private:
  void make_global_key_space_query(EVMQuery<N> &query, Context &context,
                                   uint32_t partitionID, Random &random) const {
    //  invoke_map[SINGLE_KV_STORE] = "1ab06ee5";
    //  invoke_map[SMALL_BANK_TRANSFER] = "43b0e8df";

    switch (context.get_contract_type()) {
      case CONTRACT_TYPE::SINGLE_KV_STORE: {
        // LOG(INFO)<<"CONTRACT_TYPE::SINGLE_KV_STORE";
        int32_t key;
        int32_t value;

        auto vec = get_unique_keys(1, context, partitionID, random);
        key = vec[0];
        value = random.uniform_dist(
            0,
            static_cast<int>(context.keysPerPartition * context.partition_num) -
                1);
        // prefix
        query.INPUT_CODE += "1ab06ee5";
        // key
        query.INPUT_CODE += input_key_to_string(key);
        // value
        query.INPUT_CODE += input_key_to_string(value);
        break;
      }

      case CONTRACT_TYPE::SMALL_BANK_TRANSFER: {
        // LOG(INFO)<<"CONTRACT_TYPE::SMALL_BANK_TRANSFER";
        int32_t value;

        auto vec = get_unique_keys(2, context, partitionID, random);
        value = random.uniform_dist(
            0,
            static_cast<int>(context.keysPerPartition * context.partition_num) -
                1);

        // prefix
        query.INPUT_CODE += "43b0e8df";
        // key
        for (auto i = 0; i < vec.size(); i++) {
          query.INPUT_CODE += input_key_to_string(vec[i]);
        }
        // value
        query.INPUT_CODE += input_key_to_string(value);
        break;
      }

      case CONTRACT_TYPE::SLOAD: {
        // LOG(INFO)<<"CONTRACT_TYPE::SLOAD";
        int32_t key;
        int32_t value;

        auto vec = get_unique_keys(1, context, partitionID, random);
        value = random.uniform_dist(
            0,
            static_cast<int>(context.keysPerPartition * context.partition_num) -
                1);

        // prefix
        query.INPUT_CODE += "1ab06ee5";
        // key
        for (auto i = 0; i < vec.size(); i++) {
          query.INPUT_CODE += input_key_to_string(vec[i]);
          query.RKEY.push_back(vec[i]);
        }
        // value
        query.INPUT_CODE += input_key_to_string(value);
        break;
      }

      case CONTRACT_TYPE::MULTIREAD: {
        // LOG(INFO)<<"CONTRACT_TYPE::MULTIREAD";
        int32_t value;

        auto vec = get_unique_keys(3, context, partitionID, random);
        value = random.uniform_dist(
            0,
            static_cast<int>(context.keysPerPartition * context.partition_num) -
                1);

        // LOG(INFO)<<"keys: "<<key1<<" "<<key2<<" "<<key3<<" "<<value;
        // prefix
        query.INPUT_CODE += "606ce3bf";
        // key
        for (auto i = 0; i < vec.size(); i++) {
          query.INPUT_CODE += input_key_to_string(vec[i]);
          query.RKEY.push_back(vec[i]);
          query.WKEY.push_back(vec[i]);
        }
        // value
        query.INPUT_CODE += input_key_to_string(value);
        break;
      }

      case CONTRACT_TYPE::COMPLEX_MULTI_READ: {
        int32_t value;

        auto vec = get_unique_keys(3, context, partitionID, random);
        value = random.uniform_dist(
            0,
            static_cast<int>(context.keysPerPartition * context.partition_num) -
                1);
        // prefix
        query.INPUT_CODE += "606ce3bf";
        // key
        for (auto i = 0; i < vec.size(); i++) {
          query.INPUT_CODE += input_key_to_string(vec[i]);
          query.RKEY.push_back(vec[i]);
          query.WKEY.push_back(vec[i]);
        }
        // value
        query.INPUT_CODE += input_key_to_string(value);
        break;
      }

      case CONTRACT_TYPE::TEN_KV_STORE: {
        int32_t value;

        auto vec = get_unique_keys(10, context, partitionID, random);
        value = random.uniform_dist(
            0,
            static_cast<int>(context.keysPerPartition * context.partition_num) -
                1);
        // prefix
        query.INPUT_CODE += "f3d7af72";
        // key
        // for(auto i=0; i < vec.size(); i++){
        //   query.INPUT_CODE += input_key_to_string(vec[i]);
        // }
        // modify
        for (auto i = 0; i < vec.size(); i++) {
          query.INPUT_CODE += input_key_to_string(vec[i]);
          query.RKEY.push_back(vec[i]);
          query.WKEY.push_back(vec[i]);
        }
        // value
        query.INPUT_CODE += input_key_to_string(value);
        break;
      }

      case CONTRACT_TYPE::FIVE_KV_STORE: {
        int32_t value;

        auto vec = get_unique_keys(5, context, partitionID, random);
        value = random.uniform_dist(
            0,
            static_cast<int>(context.keysPerPartition * context.partition_num) -
                1);
        // prefix
        query.INPUT_CODE += "40cb7660";
        // key
        for (auto i = 0; i < vec.size(); i++) {
          query.INPUT_CODE += input_key_to_string(vec[i]);
          query.RKEY.push_back(vec[i]);
          query.WKEY.push_back(vec[i]);
        }
        // value
        query.INPUT_CODE += input_key_to_string(value);
        break;
      }

      case CONTRACT_TYPE::COMPLEX_TEN_KV_STORE: {
        int32_t value;
        int _n = 10;
        std::vector<int32_t> vec;
        for (auto i = 0; i < _n; i++) {
          if (i < 5) {
            while (true) {
              int32_t key = random.uniform_dist(
                  0, static_cast<int>(context.keysPerPartition *
                                      context.partition_num) -
                         1);
              bool has_same = false;
              for (auto it = vec.begin(); it != vec.end(); ++it) {
                has_same |= (*it == key);
              }
              if (!has_same) {
                vec.push_back(key);
                break;
              }
            }
          } else {
            while (true) {
              int32_t key = Zipf::globalZipf().value(random.next_double());
              bool has_same = false;
              for (auto it = vec.begin(); it != vec.end(); ++it) {
                has_same |= (*it == key);
              }
              if (!has_same) {
                vec.push_back(key);
                break;
              }
            }
          }
        }
        value = random.uniform_dist(
            0,
            static_cast<int>(context.keysPerPartition * context.partition_num) -
                1);
        // prefix
        query.INPUT_CODE += "f3d7af72";
        // key
        for (auto i = 0; i < vec.size(); i++) {
          query.INPUT_CODE += input_key_to_string(vec[i]);
          query.RKEY.push_back(vec[i]);
          query.WKEY.push_back(vec[i]);
        }
        // value
        query.INPUT_CODE += input_key_to_string(value);
        break;
      }

      case CONTRACT_TYPE::MUTABLE_RW: {
        int32_t value;

        auto vec = get_unique_keys(3, context, partitionID, random);
        value = random.uniform_dist(
            0,
            static_cast<int>(context.keysPerPartition * context.partition_num) -
                1);
        // prefix
        query.INPUT_CODE += "606ce3bf";
        // key
        for (auto i = 0; i < vec.size(); i++) {
          query.INPUT_CODE += input_key_to_string(vec[i]);
          query.RKEY.push_back(vec[i]);
          query.WKEY.push_back(vec[i]);
        }
        // value
        query.INPUT_CODE += input_key_to_string(value);
        break;
      }

      case CONTRACT_TYPE::TEN_KV_STORE_NOLOOP: {
        int32_t value;

        auto vec = get_unique_keys(10, context, partitionID, random);
        value = random.uniform_dist(
            0,
            static_cast<int>(context.keysPerPartition * context.partition_num) -
                1);
        // prefix
        query.INPUT_CODE += "f3d7af72";
        // key
        // for(auto i=0; i < vec.size(); i++){
        //   query.INPUT_CODE += input_key_to_string(vec[i]);
        // }
        // modify
        for (auto i = 0; i < vec.size(); i++) {
          query.INPUT_CODE += input_key_to_string(vec[i]);
          query.RKEY.push_back(vec[i]);
          query.WKEY.push_back(vec[i]);
        }
        // value
        query.INPUT_CODE += input_key_to_string(value);
        break;
      }

      case CONTRACT_TYPE::YCSB_10key_10r5w: {
        int32_t value;

        auto vec = get_unique_keys(10, context, partitionID, random);
        value = random.uniform_dist(
            0,
            static_cast<int>(context.keysPerPartition * context.partition_num) -
                1);
        // prefix
        query.INPUT_CODE += "f3d7af72";
        // key
        // for(auto i=0; i < vec.size(); i++){
        //   query.INPUT_CODE += input_key_to_string(vec[i]);
        // }
        // modify
        for (auto i = 0; i < vec.size(); i++) {
          query.INPUT_CODE += input_key_to_string(vec[i]);
          if (i % 2 == 0) {
            query.WKEY.push_back(vec[i]);
          } else {
            query.RKEY.push_back(vec[i]);
          }
        }
        // value
        query.INPUT_CODE += input_key_to_string(value);
        break;
      }

      case CONTRACT_TYPE::SMALL_BANK: {
        // 6 functions uniformly pick
        // "1e010439" getBalance(uint256 addr)
        // "bb27eb2c" deposit-Checking(uint256 addr, uint256 bal)
        // "ad0f98c0" write-Check(uint256 addr, uint256 bal)
        // "83406251" transact-Saving(uint256 addr, uint256 bal)
        // "8ac10b9c" sendPayment(uint256 addr0, uint256 addr1, uint256 bal)
        // "97b63212" amalgamate(uint256 addr0, uint256 addr1)
        std::vector param_vec{"1e010439", "bb27eb2c", "ad0f98c0",
                              "83406251", "8ac10b9c", "97b63212"};
        int32_t opt;
        opt = random.uniform_dist(0, param_vec.size() - 1);
        switch (opt) {
          case 0: {
            query.INPUT_CODE += param_vec[opt];

            auto vec = get_unique_keys(1, context, partitionID, random);
            query.INPUT_CODE += input_key_to_string(vec[0]);

            query.WKEY.push_back(vec[0]);
            break;
          }
          case 1: {
            query.INPUT_CODE += param_vec[opt];

            int32_t value;
            value = random.uniform_dist(
                0, static_cast<int>(context.keysPerPartition *
                                    context.partition_num) -
                       1);
            auto vec = get_unique_keys(1, context, partitionID, random);

            query.INPUT_CODE += input_key_to_string(vec[0]);
            query.INPUT_CODE += input_key_to_string(value);

            query.WKEY.push_back(vec[0]);
            break;
          }
          case 2: {
            query.INPUT_CODE += param_vec[opt];

            int32_t value;
            value = random.uniform_dist(
                0, static_cast<int>(context.keysPerPartition *
                                    context.partition_num) -
                       1);
            auto vec = get_unique_keys(1, context, partitionID, random);

            query.INPUT_CODE += input_key_to_string(vec[0]);
            query.INPUT_CODE += input_key_to_string(value);

            query.WKEY.push_back(vec[0]);
            break;
          }
          case 3: {
            query.INPUT_CODE += param_vec[opt];

            int32_t value;
            value = random.uniform_dist(
                0, static_cast<int>(context.keysPerPartition *
                                    context.partition_num) -
                       1);
            auto vec = get_unique_keys(1, context, partitionID, random);

            query.INPUT_CODE += input_key_to_string(vec[0]);
            query.INPUT_CODE += input_key_to_string(value);

            query.WKEY.push_back(vec[0]);
            break;
          }
          case 4: {
            query.INPUT_CODE += param_vec[opt];

            int32_t value;
            value = random.uniform_dist(
                0, static_cast<int>(context.keysPerPartition *
                                    context.partition_num) -
                       1);
            auto vec = get_unique_keys(2, context, partitionID, random);

            query.INPUT_CODE += input_key_to_string(vec[0]);
            query.INPUT_CODE += input_key_to_string(vec[1]);
            query.INPUT_CODE += input_key_to_string(value);

            query.WKEY.push_back(vec[0]);
            query.WKEY.push_back(vec[1]);
            break;
          }
          case 5: {
            query.INPUT_CODE += param_vec[opt];

            auto vec = get_unique_keys(2, context, partitionID, random);

            query.INPUT_CODE += input_key_to_string(vec[0]);
            query.INPUT_CODE += input_key_to_string(vec[1]);

            query.WKEY.push_back(vec[0]);
            query.WKEY.push_back(vec[1]);
            break;
          }
          default:
            CHECK(false) << "SMALLBANK contract type missmatch: ";
            break;
        }
        break;
      }
      case CONTRACT_TYPE::YCSB_10key_10r10w: {
        int32_t value;

        auto vec = get_unique_keys(10, context, partitionID, random);
        value = random.uniform_dist(
            0,
            static_cast<int>(context.keysPerPartition * context.partition_num) -
                1);
        // prefix
        query.INPUT_CODE += "f3d7af72";
        // key
        // for(auto i=0; i < vec.size(); i++){
        //   query.INPUT_CODE += input_key_to_string(vec[i]);
        // }
        // modify
        for (auto i = 0; i < vec.size(); i++) {
          query.INPUT_CODE += input_key_to_string(vec[i]);
          // if (i % 2 == 0) {
          query.WKEY.push_back(vec[i]);
          // } else {
          //   query.RKEY.push_back(vec[i]);
          // }
        }
        // value
        query.INPUT_CODE += input_key_to_string(value);
        break;
      }

      case CONTRACT_TYPE::YCSB_2key_2r1w: {
        int32_t value;

        auto vec = get_unique_keys(10, context, partitionID, random);
        value = random.uniform_dist(
            0,
            static_cast<int>(context.keysPerPartition * context.partition_num) -
                1);
        // prefix
        query.INPUT_CODE += "f3d7af72";
        // key
        // for(auto i=0; i < vec.size(); i++){
        //   query.INPUT_CODE += input_key_to_string(vec[i]);
        // }
        // modify
        for (auto i = 0; i < vec.size(); i++) {
          query.INPUT_CODE += input_key_to_string(vec[i]);
          if (i % 2 == 0) {
            query.WKEY.push_back(vec[i]);
          } else {
            query.RKEY.push_back(vec[i]);
          }
        }
        // value
        query.INPUT_CODE += input_key_to_string(value);
        break;
      }

      case CONTRACT_TYPE::YCSB_2key_2r2w: {
        int32_t value;

        auto vec = get_unique_keys(10, context, partitionID, random);
        value = random.uniform_dist(
            0,
            static_cast<int>(context.keysPerPartition * context.partition_num) -
                1);
        // prefix
        query.INPUT_CODE += "f3d7af72";
        // key
        // for(auto i=0; i < vec.size(); i++){
        //   query.INPUT_CODE += input_key_to_string(vec[i]);
        // }
        // modify
        for (auto i = 0; i < vec.size(); i++) {
          query.INPUT_CODE += input_key_to_string(vec[i]);
          // if (i % 2 == 0) {
          query.WKEY.push_back(vec[i]);
          // } else {
          //   query.RKEY.push_back(vec[i]);
          // }
        }

        // value
        query.INPUT_CODE += input_key_to_string(value);
        break;
      }

      case CONTRACT_TYPE::SMALL_BANK_v2: {
        // 6 functions
        // "1e010439" getBalance(uint256 addr)
        // "bb27eb2c" deposit-Checking(uint256 addr, uint256 bal)
        // "ad0f98c0" write-Check(uint256 addr, uint256 bal)
        // "83406251" transact-Saving(uint256 addr, uint256 bal)
        // "8ac10b9c" sendPayment(uint256 addr0, uint256 addr1, uint256 bal)
        // "97b63212" amalgamate(uint256 addr0, uint256 addr1)
        std::vector param_vec{"1e010439", "bb27eb2c", "ad0f98c0",
                              "83406251", "8ac10b9c", "97b63212"};
        int32_t opt;
        // opt = random.uniform_dist(0, param_vec.size() - 1);

        opt = random.uniform_dist(0, param_vec.size() * 5 - 1);
        opt = opt / 3;

        switch (opt) {
          case 0: {
            query.INPUT_CODE += param_vec[opt];

            auto vec = get_unique_keys(1, context, partitionID, random);
            query.INPUT_CODE += input_key_to_string(vec[0]);

            query.WKEY.push_back(vec[0]);
            break;
          }
          case 1: {
            query.INPUT_CODE += param_vec[opt];

            int32_t value;
            value = random.uniform_dist(
                0, static_cast<int>(context.keysPerPartition *
                                    context.partition_num) -
                       1);
            auto vec = get_unique_keys(1, context, partitionID, random);

            query.INPUT_CODE += input_key_to_string(vec[0]);
            query.INPUT_CODE += input_key_to_string(value);

            query.WKEY.push_back(vec[0]);
            break;
          }
          case 2: {
            query.INPUT_CODE += param_vec[opt];

            int32_t value;
            value = random.uniform_dist(
                0, static_cast<int>(context.keysPerPartition *
                                    context.partition_num) -
                       1);
            auto vec = get_unique_keys(1, context, partitionID, random);

            query.INPUT_CODE += input_key_to_string(vec[0]);
            query.INPUT_CODE += input_key_to_string(value);

            query.WKEY.push_back(vec[0]);
            break;
          }
          case 3: {
            query.INPUT_CODE += param_vec[opt];

            int32_t value;
            value = random.uniform_dist(
                0, static_cast<int>(context.keysPerPartition *
                                    context.partition_num) -
                       1);
            auto vec = get_unique_keys(1, context, partitionID, random);

            query.INPUT_CODE += input_key_to_string(vec[0]);
            query.INPUT_CODE += input_key_to_string(value);

            query.WKEY.push_back(vec[0]);
            break;
          }
          case 4:
          case 5:
          case 6: {
            query.INPUT_CODE += param_vec[4];

            int32_t value;
            // value = random.uniform_dist(
            //     0, static_cast<int>(context.keysPerPartition *
            //                         context.partition_num) -
            //            1);
            value = 0;
            auto vec = get_unique_keys(2, context, partitionID, random);

            query.INPUT_CODE += input_key_to_string(vec[0]);
            query.INPUT_CODE += input_key_to_string(vec[1]);
            query.INPUT_CODE += input_key_to_string(value);

            query.WKEY.push_back(vec[0]);
            query.WKEY.push_back(vec[1]);
            break;
          }
          case 7:
          case 8:
          case 9: {
            query.INPUT_CODE += param_vec[5];

            auto vec = get_unique_keys(2, context, partitionID, random);

            query.INPUT_CODE += input_key_to_string(vec[0]);
            query.INPUT_CODE += input_key_to_string(vec[1]);

            query.WKEY.push_back(vec[0]);
            query.WKEY.push_back(vec[1]);
            break;
          }
          default:
            CHECK(false) << "SMALLBANK contract type missmatch: ";
            break;
        }
        break;
      }

      default:
        CHECK(false) << "contract type missmatch: "
                     << context.get_contract_type();
        break;
    }
  }

 public:
  EVMQuery<N> operator()(Context &context, uint32_t partitionID,
                         Random &random) const {
    EVMQuery<N> query;

    // if (context.global_key_space) {
    make_global_key_space_query(query, context, partitionID, random);
    // } else {
    // CHECK(false);
    // }
    return query;
  }
};
}  // namespace evm
}  // namespace dcc