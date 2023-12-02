#pragma once

#include <dcc/common/ClassOf.h>
#include <dcc/common/Encoder.h>
#include <dcc/common/HashMap.h>
#include <dcc/common/MVCCHashMap.h>
#include <dcc/core/Context.h>

#include <memory>

#include "glog/logging.h"

namespace dcc {

class ITable {
 public:
  using MetaDataType = std::atomic<uint64_t>;

  virtual ~ITable() = default;

  virtual std::tuple<MetaDataType *, void *> search(const void *key,
                                                    uint64_t version = 0) = 0;

  virtual void *search_value(const void *key, uint64_t version = 0) = 0;

  virtual MetaDataType &search_metadata(const void *key,
                                        uint64_t version = 0) = 0;

  virtual std::tuple<MetaDataType *, void *> search_prev(const void *key,
                                                         uint64_t version) = 0;

  virtual void *search_value_prev(const void *key, uint64_t version) = 0;

  virtual MetaDataType &search_metadata_prev(const void *key,
                                             uint64_t version) = 0;

  virtual void insert(const void *key, const void *value,
                      uint64_t version = 0) = 0;

  virtual void update(const void *key, const void *value,
                      uint64_t version = 0) = 0;

  virtual void garbage_collect(const void *key) = 0;

  virtual void deserialize_value(const void *key, StringPiece stringPiece,
                                 uint64_t version = 0) = 0;

  virtual void serialize_value(Encoder &enc, const void *value) = 0;

  virtual std::size_t key_size() = 0;

  virtual std::size_t value_size() = 0;

  virtual std::size_t field_size() = 0;

  virtual std::size_t tableID() = 0;

  virtual std::size_t partitionID() = 0;

  virtual int lock(const void *key, SparkleTransaction *T) = 0;

  virtual std::tuple<MetaDataType *, void *> read(const void *key,
                                                  SparkleTransaction *T) = 0;

  virtual void UnlockAndRemove(const void *key, SparkleTransaction *T) = 0;

  virtual void UnlockAndVaccum(const void *key, SparkleTransaction *T) = 0;

  virtual int addVersion(const void *key, const void *value,
                         SparkleTransaction *T,
                         std::atomic<uint32_t> &NEXT_TX) = 0;

  virtual void RemoveFromDeps(const void *key, SparkleTransaction *T) = 0;

  // HZC add 0926
  virtual int lock(const void *key, SpectrumTransaction *T) = 0;

  virtual std::tuple<MetaDataType *, void *> read(const void *key,
                                                  SpectrumTransaction *T) = 0;

  virtual void UnlockAndRemove(const void *key, SpectrumTransaction *T) = 0;

  virtual void UnlockAndVaccum(const void *key, SpectrumTransaction *T) = 0;

  virtual int addVersion(const void *key, const void *value,
                         SpectrumTransaction *T,
                         std::atomic<uint32_t> &NEXT_TX) = 0;
  virtual void RemoveFromDeps(const void *key, SpectrumTransaction *T) = 0;
};

}  // namespace dcc