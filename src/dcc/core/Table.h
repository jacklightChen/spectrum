#pragma once

#include <dcc/core/ITable.h>

#include <silkworm/common/util.hpp>

#include "glog/logging.h"

namespace dcc {

/* parameter version is not used in Table. */
template <std::size_t N, class KeyType, class ValueType>
class Table : public ITable {
 public:
  using MetaDataType = std::atomic<uint64_t>;

  virtual ~Table() override = default;

  Table(std::size_t tableID, std::size_t partitionID)
      : tableID_(tableID), partitionID_(partitionID) {}

  std::tuple<MetaDataType *, void *> search(const void *key,
                                            uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto &v = map_[k];
    return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
  }

  void *search_value(const void *key, uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    return &std::get<1>(map_[k]);
  }

  MetaDataType &search_metadata(const void *key,
                                uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    return std::get<0>(map_[k]);
  }

  std::tuple<MetaDataType *, void *> search_prev(const void *key,
                                                 uint64_t version) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto &v = map_[k];
    return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
  }

  void *search_value_prev(const void *key, uint64_t version) override {
    const auto &k = *static_cast<const KeyType *>(key);
    return &std::get<1>(map_[k]);
  }

  MetaDataType &search_metadata_prev(const void *key,
                                     uint64_t version) override {
    const auto &k = *static_cast<const KeyType *>(key);
    return std::get<0>(map_[k]);
  }

  void insert(const void *key, const void *value,
              uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    DCHECK(map_.contains(k) == false);
    // LOG(INFO) << silkworm::to_hex(k);
    // LOG(INFO) << silkworm::to_hex(v);
    auto &row = map_[k];
    std::get<0>(row).store(0);
    std::get<1>(row) = v;
  }

  void RemoveFromDeps(const void *key, SparkleTransaction *T) override {
    CHECK(false) << "no implementation";
  }

  void update(const void *key, const void *value,
              uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    auto &row = map_[k];
    std::get<1>(row) = v;
  }

  void garbage_collect(const void *key) override {}

  void deserialize_value(const void *key, StringPiece stringPiece,
                         uint64_t version = 0) override {
    std::size_t size = stringPiece.size();
    const auto &k = *static_cast<const KeyType *>(key);
    auto &row = map_[k];
    auto &v = std::get<1>(row);

    Decoder dec(stringPiece);
    dec >> v;

    DCHECK(size - dec.size() == ClassOf<ValueType>::size());
  }

  void serialize_value(Encoder &enc, const void *value) override {
    std::size_t size = enc.size();
    const auto &v = *static_cast<const ValueType *>(value);
    enc << v;

    DCHECK(enc.size() - size == ClassOf<ValueType>::size());
  }

  std::size_t key_size() override { return sizeof(KeyType); }

  std::size_t value_size() override { return sizeof(ValueType); }

  std::size_t field_size() override { return ClassOf<ValueType>::size(); }

  std::size_t tableID() override { return tableID_; }

  std::size_t partitionID() override { return partitionID_; }

  int lock(const void *key, SparkleTransaction *T) override { return 1; }

  std::tuple<MetaDataType *, void *> read(const void *key,
                                          SparkleTransaction *T) override {
    return std::make_tuple(nullptr, nullptr);
  }

  void UnlockAndRemove(const void *key, SparkleTransaction *T) override {
    return;
  }

  void UnlockAndVaccum(const void *key, SparkleTransaction *T) override {
    return;
  }

  int addVersion(const void *key, const void *value, SparkleTransaction *T,
                 std::atomic<uint32_t> &NEXT_TX) override {
    return 1;
  }

  int lock(const void *key, SpectrumTransaction *T) override { return 1; }

  std::tuple<MetaDataType *, void *> read(const void *key,
                                          SpectrumTransaction *T) override {
    return std::make_tuple(nullptr, nullptr);
  }

  void UnlockAndRemove(const void *key, SpectrumTransaction *T) override {
    return;
  }

  void UnlockAndVaccum(const void *key, SpectrumTransaction *T) override {
    return;
  }

  int addVersion(const void *key, const void *value, SpectrumTransaction *T,
                 std::atomic<uint32_t> &NEXT_TX) override {
    return 1;
  }

  void RemoveFromDeps(const void *key, SpectrumTransaction *T) override {
    CHECK(false) << "no implementation";
  }

  std::vector<KeyType> getKeys() { return map_.keys(); }

 private:
  HashMap<N, KeyType, std::tuple<MetaDataType, ValueType>> map_;
  std::size_t tableID_;
  std::size_t partitionID_;
};

}  // namespace dcc
