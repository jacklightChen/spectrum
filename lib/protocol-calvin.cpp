#include "protocol-calvin.hpp"

#include "./hex.hpp"
#include "./thread-util.hpp"
#include <chrono>
#include <string>
#include <vector>

namespace spectrum {
#define K std::tuple<evmc::address, evmc::bytes32>
#define V evmc::bytes32
#define T CalvinTransaction
using namespace std::chrono;

/// @brief wrap a base transaction into a calvin transaction
/// @param inner the base transaction
/// @param id transaction id
CalvinTransaction::CalvinTransaction(Transaction &&inner, size_t id)
    : Transaction{std::move(inner)}, id{id} {}

Calvin::Calvin(Workload &workload, Statistics &statistics, size_t n_threads,
               size_t n_dispatchers, size_t table_partitions)
    : workload{workload}, statistics{statistics}, n_workers{n_threads},
      table{table_partitions} {
    LOG(INFO) << fmt::format("Calvin({}, {}, {})", n_threads, table_partitions,
                             batch_size)
              << std::endl;
}

std::size_t get_available_worker(std::size_t n_lock_manager,
                                 std::size_t n_workers, std::size_t request_id,
                                 std::size_t scheduler_id) {
    auto start_worker_id = n_workers / n_lock_manager * scheduler_id;
    auto len = n_workers / n_lock_manager;
    return request_id % len + start_worker_id;
}

CalvinTable::CalvinTable(size_t partitions)
    : Table<K, V, KeyHasher>{partitions} {}

evmc::bytes32 CalvinTable::GetStorage(const evmc::address &addr,
                                      const evmc::bytes32 &key) {
    auto v = evmc::bytes32{0};
    Table::Get(std::make_tuple(addr, key), [&](auto _v) { v = _v; });
    return v;
}

void CalvinTable::SetStorage(const evmc::address &addr,
                             const evmc::bytes32 &key,
                             const evmc::bytes32 &value) {
    Table::Put(std::make_tuple(addr, key), [&](evmc::bytes32& v){ v = value; });
}

CalvinExecutor::CalvinExecutor(Calvin &calvin)
    : workload{calvin.workload}, table{calvin.table},
      stop_flag{calvin.stop_flag}, statistics{calvin.statistics},
      commit_num{calvin.commit_num}, n_lock_manager{calvin.n_lock_manager},
      n_workers{calvin.n_workers}, batch_size{calvin.batch_size},
      done_queue{calvin.done_queue} {}

CalvinScheduler::CalvinScheduler(Calvin &calvin)
    : workload{calvin.workload}, table{calvin.table},
      stop_flag{calvin.stop_flag}, statistics{calvin.statistics},
      commit_num{calvin.commit_num}, n_lock_manager{calvin.n_lock_manager},
      n_workers{calvin.n_workers}, batch_size{calvin.batch_size},
      done_queue{calvin.done_queue}, lock_manager{},
      all_executors{calvin.executors} {}

void CalvinScheduler::ScheduleTransactions() {
    auto i = 0;
    auto request_id = 0;

    while (!stop_flag.load()) {
        T *tmp;

        while (done_queue.try_dequeue(tmp) != false) {
            lock_manager.Release(tmp);
            delete tmp;
            commit_num++;
        }

        if (i - commit_num > batch_size * 2) {
            __asm volatile("pause" : :);
            continue;
        }

        for (int j = 0; j < batch_size; ++j) {
            auto tx = std::make_unique<T>(workload.Next(), i);
            Prediction p;
            tx->Analyze(p);
            std::set<std::string> wr_set;
            std::set<std::string> rd_set;

            for (auto &k : p.put) {
                auto &key = std::get<1>(k);
                wr_set.insert(to_hex(std::span{(uint8_t *)&key, 32}));
            }

            for (auto &k : p.get) {
                auto &key = std::get<1>(k);
                auto keystr = to_hex(std::span{(uint8_t *)&key, 32});
                if (!wr_set.contains(keystr)) {
                    rd_set.insert(keystr);
                }
            }

            for (auto &k : wr_set) {
                tx->wr_vec.push_back(k);
            }

            for (auto &k : rd_set) {
                tx->rd_vec.push_back(k);
            }

            lock_manager.Lock(tx.release());
            i += n_lock_manager;
        }

        while (!lock_manager.ready_txns_.empty()) {
            auto txn = lock_manager.ready_txns_.front();
            lock_manager.ready_txns_.pop_front();

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
        T *tx = nullptr;
        while (transaction_queue.try_dequeue(tx) != false) {
            if (tx == nullptr)
                continue;
            auto start = steady_clock::now();
            tx->UpdateGetStorageHandler(
                [&](const evmc::address &addr, const evmc::bytes32 &key) {
                    // transaction.MakeCheckpoint();
                    return table.GetStorage(addr, key);
                });
            tx->UpdateSetStorageHandler([&](const evmc::address &addr,
                                            const evmc::bytes32 &key,
                                            const evmc::bytes32 &value) {
                table.SetStorage(addr, key, value);
                return evmc_storage_status::EVMC_STORAGE_ASSIGNED;
            });
            statistics.JournalExecute();
            tx->Execute();
            done_queue.enqueue(tx);
            auto latency =
                duration_cast<microseconds>(steady_clock::now() - start)
                    .count();
            statistics.JournalCommit(latency);
        }
    }
}

void Calvin::Start() {
    stop_flag.store(false);

    // start lock manger and workers
    for (size_t i = 0; i != n_workers; ++i) {
        executors.push_back(std::make_unique<CalvinExecutor>(*this));
    }
    for (size_t i = 0; i != n_workers; ++i) {
        this->workers.push_back(
            std::thread([this, i] { executors[i]->RunTransactions(); }));
        PinRoundRobin(this->workers[i], i);
    }

    scheduler = std::make_unique<CalvinScheduler>(*this);

    sche_worker = std::thread([this] { scheduler->ScheduleTransactions(); });
    PinRoundRobin(sche_worker, n_workers);
}

void Calvin::Stop() {
    stop_flag.store(true);
    DLOG(INFO) << "calvin stop";
    sche_worker.join();
    for (auto &worker : workers) {
        worker.join();
    }
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

} // namespace spectrum
