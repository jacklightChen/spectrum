
#pragma once

#include <dcc/common/Encoder.h>
#include <string>

namespace dcc {

class Operation {

public:
  Operation() : tid(0), partition_id(0) {}

  void clear() {
    tid = 0;
    partition_id = 0;
    data.clear();
  }

  void set_tid(uint64_t id) { tid = id; }

  uint64_t get_tid() const { return tid; }

public:
  uint64_t tid;
  std::size_t partition_id;
  std::string data;
};
} // namespace dcc  