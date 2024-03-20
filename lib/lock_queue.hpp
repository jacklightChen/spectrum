#pragma once
#include <mutex>
#include <queue>
#include <vector>

#define TP std::unique_ptr<T>

class SpinLock {
    
    private:
    std::atomic_flag flag = ATOMIC_FLAG_INIT;

    public:
    void Lock() {
        while(flag.test_and_set(std::memory_order_acquire)) {}
    }
    void Unlock() {
        flag.clear(std::memory_order_release);
    }

};

template<typename Lock>
class Guard {

    private:
    Lock& lock;

    public:
    Guard(Lock& lock): lock{lock} { lock.Lock(); }
    ~Guard() { lock.Unlock(); }

};

template<typename T>
class LockQueue {

    private:
    SpinLock mu;
    std::queue<TP>  queue;

    public:
    TP Pop() {
        auto guard = Guard{mu};
        if (!queue.size()) return {nullptr};
        auto tx = std::move(queue.front());
        queue.pop();
        return tx;
    }
    void Push(TP&& tx) {
        auto guard = Guard{mu};
        queue.push(std::move(tx));
    }
    size_t Size() {
        auto guard = Guard{mu};
        return queue.size();
    }

};

template<typename T>
class LockPriorityQueue {

    struct CMP {
        bool operator()(const TP& a, const TP& b) { return a->id > b->id; }
    };

    private:
    SpinLock  mu;
    std::priority_queue<TP, std::vector<TP>, CMP> queue;

    public:
    TP Pop() {
        auto guard = Guard{mu};
        if (!queue.size()) return {nullptr};
        auto tx = std::move(const_cast<TP&>(queue.top()));
        queue.pop();
        return tx;
    }
    void Push(TP&& tx) {
        auto guard = Guard{mu};
        queue.push(std::move(tx));
    }
    size_t Size() {
        auto guard = Guard{mu};
        return queue.size();
    }

};

#undef TP