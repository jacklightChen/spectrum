
#pragma once

#include <glog/logging.h>
#include "intx/intx.hpp"

#include <atomic>
#include <unordered_map>

#include "SpinLock.h"

namespace dcc {

template <std::size_t N, class KeyType, class ValueType>
class HashMap {
 public:
  using HashMapType = std::unordered_map<KeyType, ValueType>;
  using HasherType = typename HashMapType::hasher;

 public:
  bool remove(const KeyType &key) {
    return apply(
        [&key](HashMapType &map) {
          auto it = map.find(key);
          if (it == map.end()) {
            return false;
          } else {
            map.erase(it);
            return true;
          }
        },
        bucket_number(key));
  }

  bool contains(const KeyType &key) {
    return apply(
        [&key](const HashMapType &map) { return map.find(key) != map.end(); },
        bucket_number(key));
  }

  bool insert(const KeyType &key, const ValueType &value) {
    return apply(
        [&key, &value](HashMapType &map) {
          if (map.find(key) != map.end()) {
            return false;
          }
          map[key] = value;
          return true;
        },
        bucket_number(key));
  }

  ValueType &operator[](const KeyType &key) {
    return apply_ref(
        [&key](HashMapType &map) -> ValueType & { return map[key]; },
        bucket_number(key));
  }

  std::size_t size() {
    return fold(0, [](std::size_t totalSize, const HashMapType &map) {
      return totalSize + map.size();
    });
  }

  void clear() {
    map([](HashMapType &map) { map.clear(); });
  }

  std::vector<KeyType> keys() {
    std::vector<KeyType> key_vec;
    for (size_t i = 0; i < N; ++i) {
      for (auto &kv : maps[i]) {
        key_vec.push_back(kv.first);
      }
    }
    return key_vec;
  }

 private:
  template <class ApplyFunc>
  auto &apply_ref(ApplyFunc applyFunc, std::size_t i) {
    DCHECK(i < N) << "index " << i << " is greater than " << N;
    locks[i].lock();
    auto &result = applyFunc(maps[i]);
    locks[i].unlock();
    return result;
  }

  template <class ApplyFunc>
  auto apply(ApplyFunc applyFunc, std::size_t i) {
    DCHECK(i < N) << "index " << i << " is greater than " << N;
    locks[i].lock();
    auto result = applyFunc(maps[i]);
    locks[i].unlock();
    return result;
  }

  template <class MapFunc>
  void map(MapFunc mapFunc) {
    for (auto i = 0u; i < N; i++) {
      locks[i].lock();
      mapFunc(maps[i]);
      locks[i].unlock();
    }
  }

  template <class T, class FoldFunc>
  auto fold(const T &firstValue, FoldFunc foldFunc) {
    T finalValue = firstValue;
    for (auto i = 0u; i < N; i++) {
      locks[i].lock();
      finalValue = foldFunc(finalValue, maps[i]);
      locks[i].unlock();
    }
    return finalValue;
  }

  auto bucket_number(const KeyType &key) {
    auto _key = intx::be::load<intx::uint256>(key);
    // LOG(INFO) << "hash: \n" << 
    //   "\tkey: " << _key[0] << ":" << _key[1] << ":" << _key[2] << ":" << _key[3] << std::endl;
    return hasher(key) % N;
  }

 private:
  HasherType hasher;
  HashMapType maps[N];
  SpinLock locks[N];
};

}  // namespace dcc
