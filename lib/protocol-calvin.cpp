#include "protocol-calvin.hpp"

#include <chrono>
#include <deque>
#include <string>
#include <vector>

namespace spectrum {
#define K std::tuple<evmc::address, evmc::bytes32>
#define V evmc::bytes32
#define T CalvinTransaction
using namespace std::chrono;

Calvin::Calvin(Workload &workload, Statistics &statistics, size_t n_threads,
               size_t table_partitions, size_t batch_size)
    : workload{workload}, statistics{statistics}, batch_size{batch_size},
      pool{(unsigned int)n_threads}, table{table_partitions} {
    LOG(INFO) << fmt::format("Calvin({}, {}, {})", n_threads, table_partitions,
                             batch_size)
              << std::endl;
}

std::size_t get_available_worker(std::size_t n_lock_manager,
                                 std::size_t n_workers, std::size_t request_id,
                                 std::size_t scheduler_id) {
    // assume there are n lock managers and m workers
    // 0, 1, .. n-1 are lock managers
    // n, n + 1, .., n + m - 1 are workers

    // the first lock managers assign transactions to n, .. , n + m/n - 1

    auto start_worker_id =
        n_lock_manager + n_workers / n_lock_manager * scheduler_id;
    auto len = n_workers / n_lock_manager;
    return request_id % len + start_worker_id;
}

void CalvinScheduler::ScheduleTransactions() {
    auto i = 0;
    auto request_id = 0;
    std::queue<std::unique_ptr<T>> pool;

    while (!stop_flag.load()) {
        T *tmp;

        while (done_queue.try_dequeue(tmp) != false) {
            lock_manager->Release(tmp);
            delete tmp;
        }

        if (i - commit_num > batch_size * 2) {
            __asm volatile("pause" : :);
            continue;
        }

        for (int j = 0; j < batch_size; ++j) {
            // getNextTx()
            T *nxt = nullptr;
            lock_manager->Lock(nxt);
            i += n_lock_manager;
        }

        while (!lock_manager->ready_txns_.empty()) {
            auto txn = lock_manager->ready_txns_.front();
            lock_manager->ready_txns_.pop_front();
            
            auto worker = get_available_worker(n_lock_manager, n_workers,
                                               request_id++, scheduler_id);
            txn->scheduler_id = scheduler_id;
            txn->executor_id = worker;
            all_executors[worker]->transaction_queue.enqueue(txn);
        }
    }
}
void CalvinExecutor::RunTransactions() {
    while (!stop_flag.load()) {
        T *nxt = nullptr;
        while(transaction_queue.try_dequeue(nxt) != false){
            // do exec

            done_queue.enqueue(nxt);
        }
    }
}

void Calvin::Start() {}

void Calvin::Stop() {
    stop_flag.store(true);
    pool.wait();
    DLOG(INFO) << "calvin stop";
}

// evmc::bytes32 CalvinTable::GetStorage(const evmc::address& addr,
//                                       const evmc::bytes32& key) {
//   return inner[std::make_tuple(addr, key)];
// }

// void CalvinTable::SetStorage(const evmc::address& addr,
//                              const evmc::bytes32& key,
//                              const evmc::bytes32& value) {
//   inner[std::make_tuple(addr, key)] = value;
// }

#undef K
#undef V
#undef T
// class CalvinScheduler {
//   void run(){

//   };
// };

// class CalvinExecutor {
//   void run(){

//   };
// };

} // namespace spectrum