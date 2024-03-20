#pragma once
#include <unordered_map>
#include <evmc/evmc.hpp>
#include <glog/logging.h>
#include <tuple>
#include <mutex>
#include <vector>

namespace spectrum {

template<typename K, typename V, typename Hasher>
class Table {

    private:
    std::vector<std::mutex> locks;
    std::vector<std::unordered_map<K, V, Hasher>> partitions;
    size_t                  n_partitions;

    public:
    Table(size_t partitions);
    void Get(const K& k, std::function<void(const V& v)>&& vmap);
    void Put(const K& k, std::function<void(V& v)>&& vmap);

};

template<typename K, typename V, typename Hasher>
Table<K, V, Hasher>::Table(size_t partitions)
    : locks(partitions),
      partitions(partitions),
      n_partitions{partitions}
{}

template<typename K, typename V, typename Hasher>
void Table<K, V, Hasher>::Get(const K& k, std::function<void(const V& v)>&& vmap) {
    auto partition_id = ((size_t)Hasher()(k)) % n_partitions;
    DLOG(INFO) << "at partition " << partition_id;
    auto guard = std::lock_guard{locks[partition_id]};
    auto& partition = this->partitions[partition_id];
    if (partition.contains(k)) vmap(partition[k]);
}

template<typename K, typename V, typename Hasher>
void Table<K, V, Hasher>::Put(const K& k, std::function<void(V& v)>&& vmap) {
    auto partition_id = ((size_t)Hasher()(k)) % n_partitions;
    DLOG(INFO) << "at partition " << partition_id;
    auto guard = std::lock_guard{locks[partition_id]};
    auto& partition = this->partitions[partition_id];
    vmap(partition[k]);
}

} // namespace spectrum
