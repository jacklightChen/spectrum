#pragma once

#include <dcc/common/Random.h>

#include <cstddef>
#include <string>
#include <vector>

namespace dcc {
namespace evm {
class Random : public dcc::Random {
 public:
  using dcc::Random::Random;

  std::string rand_str(std::size_t length) {
    auto &characters_ = characters();
    auto characters_len = characters_.length();
    std::string result;
    for (auto i = 0u; i < length; i++) {
      int k = uniform_dist(0, characters_len - 1);
      result += characters_[k];
    }
    return result;
  }

  std::size_t rand_int(std::size_t limit) { return uniform_dist(0, limit); }

 private:
  static const std::string &characters() {
    // static std::string characters_ =
    //     "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static std::string characters_ = "0123456789abcdef";
    return characters_;
  };
};
}  // namespace evm
}  // namespace dcc
