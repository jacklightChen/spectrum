#pragma once
#include <mutex>
#include <queue>
#include <vector>

#define TP std::unique_ptr<T>

template<typename T>
class LockQueue {

    private:
    std::mutex mu;
    std::queue<TP>  queue;

    public:
    TP Pop() {
        auto guard = std::lock_guard{mu};
        if (!queue.size()) return {nullptr};
        auto tx = std::move(queue.front());
        queue.pop();
        return tx;
    }
    void Push(TP&& tx) {
        auto guard = std::lock_guard{mu};
        queue.push(std::move(tx));
    }
    size_t Size() {
        auto guard = std::lock_guard{mu};
        return queue.size();
    }

};

template<typename T>
class LockPriorityQueue {

    struct CMP {
        bool operator()(const TP& a, const TP& b) { return a->id > b->id; }
    };

    private:
    std::mutex  mu;
    std::priority_queue<TP, std::vector<TP>, CMP> queue;

    public:
    TP Pop() {
        auto guard = std::lock_guard{mu};
        if (!queue.size()) return {nullptr};
        auto tx = std::move(const_cast<TP&>(queue.top()));
        queue.pop();
        return tx;
    }
    void Push(TP&& tx) {
        auto guard = std::lock_guard{mu};
        queue.push(std::move(tx));
    }
    size_t Size() {
        auto guard = std::lock_guard{mu};
        return queue.size();
    }

};

#undef TP