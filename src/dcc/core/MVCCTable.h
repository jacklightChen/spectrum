#pragma once

#include <dcc/core/ITable.h>

#include <cstdint>
#include <intx/intx.hpp>
#include <silkworm/common/util.hpp>

#include "dcc/protocol/Sparkle/SparkleTransaction.h"
#include "dcc/protocol/Spectrum/SpectrumTransaction.h"
#include "evmc/evmc.hpp"
#include "glog/logging.h"

namespace dcc {
template <std::size_t N, class KeyType, class ValueType>
class MVCCTable : public ITable {
 public:
  using MetaDataType = std::atomic<uint64_t>;

  virtual ~MVCCTable() override = default;

  MVCCTable(std::size_t tableID, std::size_t partitionID)
      : tableID_(tableID), partitionID_(partitionID) {}

  std::tuple<MetaDataType *, void *> search(const void *key,
                                            uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = *v_ptr;
    return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
  }

  void *search_value(const void *key, uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = *v_ptr;
    return &std::get<1>(v);
  }

  MetaDataType &search_metadata(const void *key,
                                uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = *v_ptr;
    return std::get<0>(v);
  }

  std::tuple<MetaDataType *, void *> search_prev(const void *key,
                                                 uint64_t version) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version_prev(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = std::get<1>(*v_ptr);
    return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
  }

  void *search_value_prev(const void *key, uint64_t version) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version_prev(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = *v_ptr;
    return &std::get<1>(v);
  }

  MetaDataType &search_metadata_prev(const void *key,
                                     uint64_t version) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version_prev(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = std::get<1>(*v_ptr);
    return std::get<0>(v);
  }

  void insert(const void *key, const void *value,
              uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    bool ok = map_.contains_key_version(k, version);
    DCHECK(ok == false) << "version: " << version << " already exists.";
    auto &row = map_.insert_key_version_holder(k, version);
    std::get<0>(row).store(version);
    std::get<1>(row) = v;
  }

  void update(const void *key, const void *value,
              uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    auto *row_ptr = map_.get_key_version(k, version);
    CHECK(row_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &row = *row_ptr;
    std::get<0>(row).store(0);
    std::get<1>(row) = v;
  }

  void garbage_collect(const void *key) override {
    const auto &k = *static_cast<const KeyType *>(key);
    DCHECK(map_.contains_key(k) == true) << "key to update does not exist.";
    map_.vacuum_key_keep_latest(k);
  }

  void deserialize_value(const void *key, StringPiece stringPiece,
                         uint64_t version = 0) override {
    std::size_t size = stringPiece.size();
    const auto &k = *static_cast<const KeyType *>(key);
    auto *row_ptr = map_.get_key_version(k, version);
    CHECK(row_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &row = *row_ptr;
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

  int lock(const void *key, SparkleTransaction *T) override {
    const auto &k = *static_cast<const KeyType *>(key);
    // {
    //   const auto &k =
    //       intx::be::load<intx::uint256>(*static_cast<const KeyType *>(key));
    //   LOG(INFO) << T->id <<" lock(" << T->writeSet.size() << ")\n"
    //             << "\tk:" << std::hex << k[0] << ":" << k[1] << ":" << k[2]
    //             << ":" << k[3] << std::endl;
    // }

    // try lock
    // auto &tmp_key = *static_cast<const evmc::bytes32*>(key);
    // LOG(INFO)<<"try lock "<<silkworm::to_hex(tmp_key) << ", lock tx: "<<
    // T->id;
    auto *metadata = map_.get_metadata_basic_sparkle(k);

    auto &lock = metadata->lock;
    pthread_mutex_lock(&lock);
    if (metadata->LOCK_TX == nullptr || metadata->LOCK_TX == T) {
      metadata->LOCK_TX = T;
      pthread_mutex_unlock(&lock);
      return 1;
    } else if (metadata->LOCK_TX->id < T->id) {
      metadata->WAIT_TXS.push_back(T);
      T->waiting = 1;
      pthread_mutex_unlock(&lock);
      return 0;
    } else {
      bool is_cascade = false;
      // const auto &tmp_k =
      //     intx::be::load<intx::uint256>(*static_cast<const KeyType *>(key));
      // LOG(INFO) << "replace: "
      //           << metadata->LOCK_TX->id << " with " << T->id  << "\n\t"
      //           << "on key: " << std::hex
      //           << tmp_k[0] << ":" << tmp_k[1] << ":"
      //           << tmp_k[2] << ":" << tmp_k[3] << std::endl;
      metadata->LOCK_TX->add_rollback_key(
          *static_cast<const evmc::bytes32 *>(key), T->id, is_cascade);
      metadata->LOCK_TX = T;
      pthread_mutex_unlock(&lock);
      return 1;
    }
  }

  std::tuple<MetaDataType *, void *> read(const void *key,
                                          SparkleTransaction *T) override {
    CHECK(key) << "nullptr";
    const auto &k = *static_cast<const KeyType *>(key);
    auto *metadata = map_.get_metadata_basic_sparkle(k);
    auto &lock = metadata->lock;
    // -------------------- // critical region start
    pthread_mutex_lock(&lock);
    auto *v_ptr = map_.get_key_version_prev(k, T->id);
    auto &v = *v_ptr;
    metadata->READ_DEPS.push_back(std::make_tuple(T, std::get<0>(v)));
    pthread_mutex_unlock(&lock);
    // -------------------- // critical region end
    auto &v1 = std::get<1>(v);
    return std::make_tuple(&std::get<0>(v1), &std::get<1>(v1));
  }

  void UnlockAndVaccum(const void *key, SparkleTransaction *T) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *metadata = map_.get_metadata_basic_sparkle(k);
    if (metadata == nullptr) {
      LOG(ERROR) << "HZC nullptr";
    }
    auto &lock = metadata->lock;
    pthread_mutex_lock(&lock);
    for (auto it = metadata->READ_DEPS.begin();
         it != metadata->READ_DEPS.end();) {
      auto *txn = std::get<0>(*it);
      if (T->id >= txn->id) {
        metadata->READ_DEPS.erase(it);
      } else {
        it++;
      }
    }
    pthread_mutex_unlock(&lock);
    map_.vacuum_key_versions(k, T->id);
  }

  void RemoveFromDeps(const void *key, SparkleTransaction *T) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *metadata = map_.get_metadata_basic_sparkle(k);
    if (metadata == nullptr) {
      LOG(ERROR) << "HZC nullptr";
    }
    auto &lock = metadata->lock;
    CHECK(metadata->LOCK_TX != T);
    pthread_mutex_lock(&lock);
    for (auto it = metadata->READ_DEPS.begin();
         it != metadata->READ_DEPS.end();) {
      auto *txn = std::get<0>(*it);
      if (T->id == txn->id) {
        metadata->READ_DEPS.erase(it);
      } else {
        it++;
      }
    }
    pthread_mutex_unlock(&lock);
  }

  void UnlockAndRemove(const void *key, SparkleTransaction *T) override {
    const auto &k = *static_cast<const KeyType *>(key);

    auto *metadata = map_.get_metadata_basic_sparkle(k);

    if (metadata == nullptr) {
      LOG(ERROR) << "HZC nullptr";
    }

    auto &lock = metadata->lock;
    pthread_mutex_lock(&lock);
    if (metadata->LOCK_TX == T) {
      // 当前的交易持锁, 说明还没写, 则将锁放掉
      // {
      //   const auto &tmp_k =
      //       intx::be::load<intx::uint256>(*static_cast<const KeyType
      //       *>(key));
      //   LOG(INFO) << "unlock: " << metadata->LOCK_TX->id << "\n\t"
      //             << "key: " << std::hex << tmp_k[0] << ":" << tmp_k[1] <<
      //             ":"
      //             << tmp_k[2] << ":" << tmp_k[3] << std::endl;
      // }
      metadata->LOCK_TX = nullptr;
      for (auto it = metadata->WAIT_TXS.begin(); it != metadata->WAIT_TXS.end();
           it++) {
        auto *txn = *it;
        pthread_mutex_lock(&txn->waiting_lock);
        txn->waiting = 0;
        pthread_cond_signal(&txn->waiting_cond);
        pthread_mutex_unlock(&txn->waiting_lock);
      }
      metadata->WAIT_TXS.clear();
    } else {
      // 当前交易已经写了
      map_.remove_key_version(k, T->id);
      for (auto it = metadata->READ_DEPS.begin();
           it != metadata->READ_DEPS.end();) {
        auto *txn = std::get<0>(*it);
        auto version = std::get<1>(*it);
        if (T->id == version) {
          // cascading aborts
          CHECK(T->id != txn->id);
          bool is_cascade = true;
          // LOG(INFO) << "cascade: " << txn->id << ":" << T->id << std::endl;
          txn->add_rollback_key(*static_cast<const evmc::bytes32 *>(key), T->id,
                                is_cascade);
          metadata->READ_DEPS.erase(it);
        } else if (T->id == txn->id) {
          metadata->READ_DEPS.erase(it);
        } else {
          it++;
        }
      }
    }
    pthread_mutex_unlock(&lock);
  }

  int addVersion(const void *key, const void *value, SparkleTransaction *T,
                 std::atomic<uint32_t> &NEXT_TX) override {
    // {
    //   const auto &k =
    //       intx::be::load<intx::uint256>(*static_cast<const KeyType *>(key));
    //   const auto &v =
    //       intx::be::load<intx::uint256>(*static_cast<const ValueType
    //       *>(value));
    //   LOG(INFO) << T->id <<" write(" << T->writeSet.size() << ")\n"
    //             << "\tk:" << std::hex << k[0] << ":" << k[1] << ":" << k[2]
    //             << ":" << k[3] << "\n"
    //             << "\tv:" << std::hex << v[0] << ":" << v[1] << ":" << v[2]
    //             << ":" << v[3] << std::endl;
    // }

    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    auto *metadata = map_.get_metadata_basic_sparkle(k);

    auto &lock = metadata->lock;

    pthread_mutex_lock(&lock);

    if (metadata->LOCK_TX != T) {
      // auto rollback_key_guard = std::lock_guard{T->rollback_key_mu};
      std::lock_guard<std::mutex> mu_lock(T->rollback_key_mu);
      auto lock_id = -1;
      if (metadata->LOCK_TX) {
        lock_id = metadata->LOCK_TX->id;
      }
      // for (auto key: T->rollback_key) {
      //  LOG(INFO) << T->id << ": " << silkworm::to_hex(key);
      // }
      // LOG(INFO)<<"------------------------------------------------------------";
      pthread_mutex_unlock(&lock);
      return 0;  // abort
    }

    auto &row = map_.insert_key_version_holder(k, T->id);
    std::get<0>(row).store(T->id);
    std::get<1>(row) = v;

    // LOG(INFO)<<metadata->READ_DEPS.size();
    for (auto it = metadata->READ_DEPS.begin();
         it != metadata->READ_DEPS.end();) {
      auto *txn = std::get<0>(*it);
      if (T->id < txn->id) {
        // LOG(INFO)<<T->id<<" SET ABORT FLAG OF "<<txn->id;
        bool is_cascade = false;
        txn->add_rollback_key(*static_cast<const evmc::bytes32 *>(key), T->id,
                              is_cascade);
        metadata->READ_DEPS.erase(it);
      } else if (txn->id < NEXT_TX.load()) {
        metadata->READ_DEPS.erase(it);
      } else {
        it++;
      }
    }

    metadata->LOCK_TX = nullptr;
    // std::stringstream ss;
    // ss << "wr " << T->id;
    // ss << "\n\tkey:" << silkworm::to_hex(k);
    // ss << "\n\tval:" << silkworm::to_hex(v);
    for (auto it = metadata->WAIT_TXS.begin(); it != metadata->WAIT_TXS.end();
         it++) {
      auto *txn = *it;
      // ss << "\n\trelease:" << txn->id;
      pthread_mutex_lock(&txn->waiting_lock);
      txn->waiting = 0;
      pthread_cond_signal(&txn->waiting_cond);
      pthread_mutex_unlock(&txn->waiting_lock);
    }
    metadata->WAIT_TXS.clear();
    // LOG(INFO) << ss.str() << std::endl;

    pthread_mutex_unlock(&lock);

    return 1;
  }

  int lock(const void *key, SpectrumTransaction *T) override {
    CHECK(false) << "no implementation";
    return 1;
  }

  std::tuple<MetaDataType *, void *> read(const void *key,
                                          SpectrumTransaction *T) override {
    CHECK(false) << "no implementation";
    return std::make_tuple(nullptr, nullptr);
  }

  void UnlockAndRemove(const void *key, SpectrumTransaction *T) override {
    CHECK(false) << "no implementation";
    return;
  }

  void UnlockAndVaccum(const void *key, SpectrumTransaction *T) override {
    CHECK(false) << "no implementation";
    return;
  }

  int addVersion(const void *key, const void *value, SpectrumTransaction *T,
                 std::atomic<uint32_t> &NEXT_TX) override {
    CHECK(false) << "no implementation";
    return 1;
  }

  void RemoveFromDeps(const void *key, SpectrumTransaction *T) override {
    CHECK(false) << "no implementation";
    return;
  }

 private:
  MVCCHashMap<N, KeyType, std::tuple<MetaDataType, ValueType>> map_;
  std::size_t tableID_;
  std::size_t partitionID_;
};

// Garbage implementation, just for fast verification
template <std::size_t N, class KeyType, class ValueType>
class SpectrumMVCCTable : public ITable {
 public:
  using MetaDataType = std::atomic<uint64_t>;

  virtual ~SpectrumMVCCTable() override = default;

  SpectrumMVCCTable(std::size_t tableID, std::size_t partitionID)
      : tableID_(tableID), partitionID_(partitionID) {}

  std::tuple<MetaDataType *, void *> search(const void *key,
                                            uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = *v_ptr;
    return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
  }

  void *search_value(const void *key, uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = *v_ptr;
    return &std::get<1>(v);
  }

  MetaDataType &search_metadata(const void *key,
                                uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = *v_ptr;
    return std::get<0>(v);
  }

  std::tuple<MetaDataType *, void *> search_prev(const void *key,
                                                 uint64_t version) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version_prev(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = std::get<1>(*v_ptr);
    return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
  }

  void *search_value_prev(const void *key, uint64_t version) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version_prev(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = *v_ptr;
    return &std::get<1>(v);
  }

  MetaDataType &search_metadata_prev(const void *key,
                                     uint64_t version) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *v_ptr = map_.get_key_version_prev(k, version);
    CHECK(v_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &v = std::get<1>(*v_ptr);
    return std::get<0>(v);
  }

  void insert(const void *key, const void *value,
              uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    bool ok = map_.contains_key_version(k, version);
    DCHECK(ok == false) << "version: " << version << " already exists.";
    auto &row = map_.insert_key_version_holder(k, version);
    std::get<0>(row).store(version);
    std::get<1>(row) = v;
  }

  void update(const void *key, const void *value,
              uint64_t version = 0) override {
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    auto *row_ptr = map_.get_key_version(k, version);
    CHECK(row_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &row = *row_ptr;
    std::get<0>(row).store(0);
    std::get<1>(row) = v;
  }

  void garbage_collect(const void *key) override {
    const auto &k = *static_cast<const KeyType *>(key);
    DCHECK(map_.contains_key(k) == true) << "key to update does not exist.";
    map_.vacuum_key_keep_latest(k);
  }

  void deserialize_value(const void *key, StringPiece stringPiece,
                         uint64_t version = 0) override {
    std::size_t size = stringPiece.size();
    const auto &k = *static_cast<const KeyType *>(key);
    auto *row_ptr = map_.get_key_version(k, version);
    CHECK(row_ptr != nullptr)
        << "key with version: " << version << " does not exist.";
    auto &row = *row_ptr;
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

  int lock(const void *key, SparkleTransaction *T) override {
    CHECK(false) << "no implementation";
    return 1;
  }

  std::tuple<MetaDataType *, void *> read(const void *key,
                                          SparkleTransaction *T) override {
    CHECK(false) << "no implementation";
    return std::make_tuple(nullptr, nullptr);
  }

  void UnlockAndRemove(const void *key, SparkleTransaction *T) override {
    CHECK(false) << "no implementation";
    return;
  }

  void UnlockAndVaccum(const void *key, SparkleTransaction *T) override {
    CHECK(false) << "no implementation";
    return;
  }

  int addVersion(const void *key, const void *value, SparkleTransaction *T,
                 std::atomic<uint32_t> &NEXT_TX) override {
    CHECK(false) << "no implementation";
    return 1;
  }

  void RemoveFromDeps(const void *key, SparkleTransaction *T) override {
    CHECK(false) << "no implementation";
    return;
  }

  int lock(const void *key, SpectrumTransaction *T) override {
    const auto &k = *static_cast<const KeyType *>(key);
    // {
    //   const auto &k =
    //       intx::be::load<intx::uint256>(*static_cast<const KeyType *>(key));
    //   LOG(INFO) << T->id <<" lock(" << T->writeSet.size() << ")\n"
    //             << "\tk:" << std::hex << k[0] << ":" << k[1] << ":" << k[2]
    //             << ":" << k[3] << std::endl;
    // }

    // try lock
    // auto &tmp_key = *static_cast<const evmc::bytes32*>(key);
    // LOG(INFO)<<"try lock "<<silkworm::to_hex(tmp_key) << ", lock tx: "<<
    // T->id;
    auto *metadata = map_.get_metadata_basic_spectrum(k);

    auto &lock = metadata->lock;
    pthread_mutex_lock(&lock);
    if (metadata->LOCK_TX == nullptr || metadata->LOCK_TX == T) {
      metadata->LOCK_TX = T;
      pthread_mutex_unlock(&lock);
      return 1;
    } else if (metadata->LOCK_TX->id < T->id) {
      metadata->WAIT_TXS.push_back(T);
      T->waiting = 1;
      pthread_mutex_unlock(&lock);
      return 0;
    } else {
      bool is_cascade = false;
      // const auto &tmp_k =
      //     intx::be::load<intx::uint256>(*static_cast<const KeyType *>(key));
      // LOG(INFO) << "replace: "
      //           << metadata->LOCK_TX->id << " with " << T->id  << "\n\t"
      //           << "on key: " << std::hex
      //           << tmp_k[0] << ":" << tmp_k[1] << ":"
      //           << tmp_k[2] << ":" << tmp_k[3] << std::endl;
      metadata->LOCK_TX->add_rollback_key(
          *static_cast<const evmc::bytes32 *>(key), T->id, is_cascade);
      metadata->LOCK_TX = T;
      pthread_mutex_unlock(&lock);
      return 1;
    }
  }

  std::tuple<MetaDataType *, void *> read(const void *key,
                                          SpectrumTransaction *T) override {
    CHECK(key) << "nullptr";
    const auto &k = *static_cast<const KeyType *>(key);
    auto *metadata = map_.get_metadata_basic_spectrum(k);
    auto &lock = metadata->lock;
    // -------------------- // critical region start
    pthread_mutex_lock(&lock);
    auto *v_ptr = map_.get_key_version_prev(k, T->id);
    auto &v = *v_ptr;
    metadata->READ_DEPS.push_back(std::make_tuple(T, std::get<0>(v)));
    pthread_mutex_unlock(&lock);
    // -------------------- // critical region end
    auto &v1 = std::get<1>(v);
    return std::make_tuple(&std::get<0>(v1), &std::get<1>(v1));
  }

  void UnlockAndVaccum(const void *key, SpectrumTransaction *T) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *metadata = map_.get_metadata_basic_spectrum(k);
    if (metadata == nullptr) {
      LOG(ERROR) << "HZC nullptr";
    }
    auto &lock = metadata->lock;
    pthread_mutex_lock(&lock);
    for (auto it = metadata->READ_DEPS.begin();
         it != metadata->READ_DEPS.end();) {
      auto *txn = std::get<0>(*it);
      if (T->id >= txn->id) {
        metadata->READ_DEPS.erase(it);
      } else {
        it++;
      }
    }
    pthread_mutex_unlock(&lock);
    map_.vacuum_key_versions(k, T->id);
  }

  void RemoveFromDeps(const void *key, SpectrumTransaction *T) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto *metadata = map_.get_metadata_basic_spectrum(k);
    if (metadata == nullptr) {
      LOG(ERROR) << "HZC nullptr";
    }
    auto &lock = metadata->lock;
    CHECK(metadata->LOCK_TX != T);
    pthread_mutex_lock(&lock);
    for (auto it = metadata->READ_DEPS.begin();
         it != metadata->READ_DEPS.end();) {
      auto *txn = std::get<0>(*it);
      if (T->id == txn->id) {
        metadata->READ_DEPS.erase(it);
      } else {
        it++;
      }
    }
    pthread_mutex_unlock(&lock);
  }

  void UnlockAndRemove(const void *key, SpectrumTransaction *T) override {
    const auto &k = *static_cast<const KeyType *>(key);

    auto *metadata = map_.get_metadata_basic_spectrum(k);

    if (metadata == nullptr) {
      LOG(ERROR) << "HZC nullptr";
    }

    auto &lock = metadata->lock;
    pthread_mutex_lock(&lock);
    if (metadata->LOCK_TX == T) {
      // 当前的交易持锁, 说明还没写, 则将锁放掉
      // {
      //   const auto &tmp_k =
      //       intx::be::load<intx::uint256>(*static_cast<const KeyType
      //       *>(key));
      //   LOG(INFO) << "unlock: " << metadata->LOCK_TX->id << "\n\t"
      //             << "key: " << std::hex << tmp_k[0] << ":" << tmp_k[1] <<
      //             ":"
      //             << tmp_k[2] << ":" << tmp_k[3] << std::endl;
      // }
      metadata->LOCK_TX = nullptr;
      for (auto it = metadata->WAIT_TXS.begin(); it != metadata->WAIT_TXS.end();
           it++) {
        auto *txn = *it;
        pthread_mutex_lock(&txn->waiting_lock);
        txn->waiting = 0;
        pthread_cond_signal(&txn->waiting_cond);
        pthread_mutex_unlock(&txn->waiting_lock);
      }
      metadata->WAIT_TXS.clear();
    } else {
      // 当前交易已经写了
      map_.remove_key_version(k, T->id);
      for (auto it = metadata->READ_DEPS.begin();
           it != metadata->READ_DEPS.end();) {
        auto *txn = std::get<0>(*it);
        auto version = std::get<1>(*it);
        if (T->id == version) {
          // cascading aborts
          CHECK(T->id != txn->id);
          bool is_cascade = true;
          // LOG(INFO) << "cascade: " << txn->id << ":" << T->id << std::endl;
          txn->add_rollback_key(*static_cast<const evmc::bytes32 *>(key), T->id,
                                is_cascade);
          metadata->READ_DEPS.erase(it);
        } else if (T->id == txn->id) {
          metadata->READ_DEPS.erase(it);
        } else {
          it++;
        }
      }
    }
    pthread_mutex_unlock(&lock);
  }

  int addVersion(const void *key, const void *value, SpectrumTransaction *T,
                 std::atomic<uint32_t> &NEXT_TX) override {
    // {
    //   const auto &k =
    //       intx::be::load<intx::uint256>(*static_cast<const KeyType *>(key));
    //   const auto &v =
    //       intx::be::load<intx::uint256>(*static_cast<const ValueType
    //       *>(value));
    //   LOG(INFO) << T->id <<" write(" << T->writeSet.size() << ")\n"
    //             << "\tk:" << std::hex << k[0] << ":" << k[1] << ":" << k[2]
    //             << ":" << k[3] << "\n"
    //             << "\tv:" << std::hex << v[0] << ":" << v[1] << ":" << v[2]
    //             << ":" << v[3] << std::endl;
    // }

    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    auto *metadata = map_.get_metadata_basic_spectrum(k);

    auto &lock = metadata->lock;

    pthread_mutex_lock(&lock);

    if (metadata->LOCK_TX != T) {
      // auto rollback_key_guard = std::lock_guard{T->rollback_key_mu};
      std::lock_guard<std::mutex> mu_lock(T->rollback_key_mu);
      auto lock_id = -1;
      if (metadata->LOCK_TX) {
        lock_id = metadata->LOCK_TX->id;
      }
      // for (auto key: T->rollback_key) {
      //  LOG(INFO) << T->id << ": " << silkworm::to_hex(key);
      // }
      // LOG(INFO)<<"------------------------------------------------------------";
      pthread_mutex_unlock(&lock);
      return 0;  // abort
    }

    auto &row = map_.insert_key_version_holder(k, T->id);
    std::get<0>(row).store(T->id);
    std::get<1>(row) = v;

    // LOG(INFO)<<metadata->READ_DEPS.size();
    for (auto it = metadata->READ_DEPS.begin();
         it != metadata->READ_DEPS.end();) {
      auto *txn = std::get<0>(*it);
      if (T->id < txn->id) {
        // LOG(INFO)<<T->id<<" SET ABORT FLAG OF "<<txn->id;
        bool is_cascade = false;
        txn->add_rollback_key(*static_cast<const evmc::bytes32 *>(key), T->id,
                              is_cascade);
        metadata->READ_DEPS.erase(it);
      } else if (txn->id < NEXT_TX.load()) {
        metadata->READ_DEPS.erase(it);
      } else {
        it++;
      }
    }

    metadata->LOCK_TX = nullptr;
    // std::stringstream ss;
    // ss << "wr " << T->id;
    // ss << "\n\tkey:" << silkworm::to_hex(k);
    // ss << "\n\tval:" << silkworm::to_hex(v);
    for (auto it = metadata->WAIT_TXS.begin(); it != metadata->WAIT_TXS.end();
         it++) {
      auto *txn = *it;
      // ss << "\n\trelease:" << txn->id;
      pthread_mutex_lock(&txn->waiting_lock);
      txn->waiting = 0;
      pthread_cond_signal(&txn->waiting_cond);
      pthread_mutex_unlock(&txn->waiting_lock);
    }
    metadata->WAIT_TXS.clear();
    // LOG(INFO) << ss.str() << std::endl;

    pthread_mutex_unlock(&lock);

    return 1;
  }

 private:
  SpectrumMVCCHashMap<N, KeyType, std::tuple<MetaDataType, ValueType>> map_;
  std::size_t tableID_;
  std::size_t partitionID_;
};
}  // namespace dcc
