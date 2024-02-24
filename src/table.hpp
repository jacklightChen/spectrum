#pragma once

#include <unordered_map>
#include <evmc/evmc.hpp>
#include <tuple>
#include <mutex>
#include <vector>

namespace spectrum {

template<typename K, typename V, typename Hasher>
class Table {

    private:
    std::vector<std::mutex> locks;
    std::vector<std::unordered_map<K, V, Hasher>> partitions;
    Hasher hasher;

    public:
    Table(size_t partitions);
    void Get(const K& k, V& v);
    void Put(const K& k, std::function<void(V& v)> vmap);

};

template<typename K, typename V, typename Hasher>
Table<K, V, Hasher>::Table(size_t partitions)
    : hasher{Hasher{}},
      locks(partitions),
      partitions(partitions)
{}

template<typename K, typename V, typename Hasher>
void Table<K, V, Hasher>::Get(const K& k, V& v) {
    auto partition_id = (size_t)this->hasher(k) % this->partitions.size();
    auto& partition = this->partitions[partition_id];
    auto guard = std::lock_guard<std::mutex>(locks[partition_id]);
    v = partition[k];
}

template<typename K, typename V, typename Hasher>
void Table<K, V, Hasher>::Put(const K& k, std::function<void(V& v)> vmap) {
    auto partition_id = (size_t)this->hasher(k) % this->partitions.size();
    auto& partition = this->partitions[partition_id];
    auto guard = std::lock_guard<std::mutex>(locks[partition_id]);
    vmap(partition[k]);
}

}
