#pragma once

#include <dcc/core/Table.h>
#include <dcc/protocol/Spectrum/SpectrumRWKey.h>

#include <atomic>
#include <cstring>
#include <tuple>

#include "glog/logging.h"

namespace dcc {

class SpectrumHelper {
 public:
  using MetaDataType = std::atomic<uint64_t>;

  static void read(const std::tuple<MetaDataType *, void *> &row, void *dest,
                   std::size_t size) {  //

    // MetaDataType &tid = *std::get<0>(row);
    // LOG(INFO)<<row;
    void *src = std::get<1>(row);
    // LOG(INFO)<<src;
    std::memcpy(dest, src, size);
    return;
  }
};

}  // namespace dcc