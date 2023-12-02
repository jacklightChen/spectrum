#pragma once
#include <dcc/core/MVCCTable.h>
#include <dcc/core/Table.h>

namespace dcc {

class TableFactory {
 public:
  template <std::size_t N, class KeyType, class ValueType>
  static std::unique_ptr<ITable> create_table(Context &context,
                                              std::size_t tableID,
                                              std::size_t partitionID) {
    if (context.protocol == "Sparkle") {
      return std::make_unique<MVCCTable<N, KeyType, ValueType>>(tableID,
                                                                partitionID);
    } else if (context.protocol == "Spectrum") {
      return std::make_unique<SpectrumMVCCTable<N, KeyType, ValueType>>(
          tableID, partitionID);
    } else {
      return std::make_unique<Table<N, KeyType, ValueType>>(tableID,
                                                            partitionID);
    }
  }
};

}  // namespace dcc