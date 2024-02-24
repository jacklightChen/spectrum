#include<atomic>

class Statistics {
    public:
    std::atomic<size_t> count_commit{0};
    std::atomic<float>  count_execution{0.0};
    std::atomic<size_t> count_latency_25ms{0};
    std::atomic<size_t> count_latency_50ms{0};
    std::atomic<size_t> count_latency_100ms{0};
    std::atomic<size_t> count_latency_100ms_above{0};
    void Commit(size_t latency);
    void Execute();
    void Print();
};
