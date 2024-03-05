#pragma once

#include <unordered_map>
#include <evmc/evmc.hpp>
#include <tuple>
#include <shared_mutex>
#include <vector>

namespace spectrum {

template<typename K, typename V, typename Hasher>
class Table {

    private:
    std::vector<std::shared_mutex> locks;
    std::vector<std::unordered_map<K, V, Hasher>> partitions;
    Hasher hasher;

    public:
    Table(size_t partitions);
    void Get(const K& k, std::function<void(const V& v)> vmap);
    void Put(const K& k, std::function<void(V& v)> vmap);

};

template<typename K, typename V, typename Hasher>
Table<K, V, Hasher>::Table(size_t partitions)
    : hasher{Hasher{}},
      locks(partitions),
      partitions(partitions)
{}

template<typename K, typename V, typename Hasher>
void Table<K, V, Hasher>::Get(const K& k, std::function<void(const V& v)> vmap) {
    auto partition_id = (size_t)this->hasher(k) % this->partitions.size();
    auto& partition = this->partitions[partition_id];
    auto guard = std::shared_lock<std::shared_mutex>(locks[partition_id]);
    vmap(partition[k]);
}

template<typename K, typename V, typename Hasher>
void Table<K, V, Hasher>::Put(const K& k, std::function<void(V& v)> vmap) {
    auto partition_id = (size_t)this->hasher(k) % this->partitions.size();
    auto& partition = this->partitions[partition_id];
    auto guard = std::unique_lock<std::shared_mutex>(locks[partition_id]);
    vmap(partition[k]);
}

} // namespace spectrum
