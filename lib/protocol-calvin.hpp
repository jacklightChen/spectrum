#include "./evm_hash.hpp"
#include "protocol.hpp"
#include "statistics.hpp"
#include "table.hpp"
#include "workload.hpp"
#include <atomic>
#include <conqueue/concurrentqueue.h>
#include <deque>
#include <functional>
#include <queue>
#include <set>
#include <thread>
#include <chrono>

namespace spectrum {
#define K std::tuple<evmc::address, evmc::bytes32>
#define V evmc::bytes32
#define T CalvinTransaction

struct CalvinTransaction : public Transaction {
    size_t id;
    // auto cmp = [](K a, K b) { return keyHasher(a) };
    std::vector<std::string> get_rdset() { return rd_vec; };
    std::vector<std::string> get_wrset() { return wr_vec; };

    std::vector<std::string> rd_vec;
    std::vector<std::string> wr_vec;

    size_t scheduler_id;
    size_t executor_id;
    std::chrono::time_point<std::chrono::steady_clock> start_time;

    CalvinTransaction(Transaction &&inner, size_t id);
    void analysis(){};
};

class CalvinTable : public Table<K, V, KeyHasher> {
  public:
    CalvinTable(size_t partitions);
    evmc::bytes32 GetStorage(const evmc::address &addr,
                             const evmc::bytes32 &key);
    void SetStorage(const evmc::address &addr, const evmc::bytes32 &key,
                    const evmc::bytes32 &value);
};

#define TABLE_SIZE 10000

class LockManager {
    using TransactionType = CalvinTransaction;
    enum LockMode {
        UNLOCKED = 0,
        READ = 1,
        WRITE = 2,
    };

  public:
    LockManager() {
        for (int i = 0; i < TABLE_SIZE; i++) {
            lock_table_[i] = new std::deque<KeysList>();
            lock_table_[i]->clear();
        }
        ready_txns_.clear();
        txn_waits_.clear();
    }

    ~LockManager() {
        for (int i = 0; i < TABLE_SIZE; i++) {
            delete lock_table_[i];
        }
    }

    int Lock(TransactionType *txn) {
        int not_acquired = 0;
        // Handle read/write lock requests.

        auto wrvec = txn->get_wrset();
        int wr_size = wrvec.size();

        for (int i = 0; i < wr_size; i++) {
            auto& wr_key = wrvec[i];

            std::deque<KeysList> *key_requests = lock_table_[Hash(wr_key)];
            std::deque<KeysList>::iterator it;
            for (it = key_requests->begin();
                 (it != key_requests->end()) && (it->key != wr_key); ++it) {
            }
            std::deque<LockRequest> *requests;
            if (it == key_requests->end()) {
                requests = new std::deque<LockRequest>();
                requests->clear();
                key_requests->push_back(KeysList(wr_key, requests));
            } else {
                requests = it->locksrequest;
            }

            // Only need to request this if lock txn hasn't already
            // requested it.
            if (requests->empty() || txn != requests->back().txn) {
                requests->push_back(LockRequest(WRITE, txn));
                // Write lock request fails if there is any previous request
                // at all.
                if (requests->size() > 1)
                    not_acquired++;
            }
        }

        auto rdvec = txn->get_rdset();
        int rd_size = rdvec.size();

        for (int i = 0; i < rd_size; i++) {
            auto& rd_key = rdvec[i];
            std::deque<KeysList> *key_requests = lock_table_[Hash(rd_key)];

            std::deque<KeysList>::iterator it;
            for (it = key_requests->begin();
                 it != key_requests->end() && it->key != rd_key; ++it) {
            }
            std::deque<LockRequest> *requests;
            if (it == key_requests->end()) {
                requests = new std::deque<LockRequest>();
                key_requests->push_back(KeysList(rd_key, requests));
            } else {
                requests = it->locksrequest;
            }

            // Only need to request this if lock txn hasn't already
            // requested it.
            if (requests->empty() || txn != requests->back().txn) {
                requests->push_back(LockRequest(READ, txn));
                // Read lock request fails if there is any previous write
                // request.
                for (std::deque<LockRequest>::iterator it = requests->begin();
                     it != requests->end(); ++it) {
                    if (it->mode == WRITE) {
                        not_acquired++;
                        break;
                    }
                }
            }
        }

        // Record and return the number of locks that the txn is blocked on.
        if (not_acquired > 0) {
            txn_waits_[txn] = not_acquired;
        } else {
            ready_txns_.push_back(txn);
        }
        return not_acquired;
    }

    void Release(const std::string &key, TransactionType *txn) {
        // std::cout << "enter release" << txn->txid() << std::endl;
        // Avoid repeatedly looking up key in the unordered_map.
        std::deque<KeysList> *key_requests = lock_table_[Hash(key)];

        std::deque<KeysList>::iterator it1;
        for (it1 = key_requests->begin();
             it1 != key_requests->end() && it1->key != key; ++it1) {
        }
        std::deque<LockRequest> *requests = it1->locksrequest;

        // Seek to the target request. Note whether any write lock requests
        // precede the target.
        bool write_requests_precede_target = false;
        std::deque<LockRequest>::iterator it;
        for (it = requests->begin(); it != requests->end() && it->txn != txn;
             ++it) {
            if (it->mode == WRITE)
                write_requests_precede_target = true;
        }

        // If we found the request, erase it. No need to do anything
        // otherwise.
        if (it != requests->end()) {
            std::deque<LockRequest>::iterator target = it;
            ++it;
            // std::cout<<"iterator:"<<it->txn->txid()<<std::endl;
            if (it != requests->end()) {
                std::vector<TransactionType *> new_owners;
                if (target == requests->begin() &&
                    (target->mode == WRITE ||
                     (target->mode == READ &&
                      it->mode == WRITE))) { // (a) or (b)
                    // If a write lock request follows, grant it.
                    if (it->mode == WRITE)
                        new_owners.push_back(it->txn);
                    // If a sequence of read lock requests follows, grant
                    // all of them.
                    for (; it != requests->end() && it->mode == READ; ++it)
                        new_owners.push_back(it->txn);
                } else if (!write_requests_precede_target &&
                           target->mode == WRITE && it->mode == READ) { // (c)
                    // If a sequence of read lock requests follows, grant
                    // all of them.
                    for (; it != requests->end() && it->mode == READ; ++it)
                        new_owners.push_back(it->txn);
                }

                // Handle txns with newly granted requests that may now be
                // ready to run.
                for (uint64_t j = 0; j < new_owners.size(); j++) {
                    txn_waits_[new_owners[j]]--;
                    if (txn_waits_[new_owners[j]] == 0) {
                        ready_txns_.push_back(new_owners[j]);
                        txn_waits_.erase(new_owners[j]);
                    }
                }
            }

            // Now it is safe to actually erase the target request.
            requests->erase(target);
            if (requests->size() == 0) {
                delete requests;
                key_requests->erase(it1);
            }
        }
    }

    void Release(TransactionType *txn) {
        CHECK(txn != nullptr) << "txn should not be nullptr";
        auto rdvec = txn->get_rdset();

        int rd_size = rdvec.size();
        for (int i = 0; i < rd_size; i++) {
            Release(rdvec[i], txn);
        }

        auto wrvec = txn->get_wrset();

        int wr_size = wrvec.size();
        for (int i = 0; i < wr_size; i++) {
            Release(wrvec[i], txn);
        }
    }

  public:
    int Hash(const std::string &key) {
        uint64_t hash = 2166136261;
        for (size_t i = 0; i < key.size(); i++) {
            hash = hash ^ (key[i]);
            hash = hash * 16777619;
        }
        return hash % TABLE_SIZE;
        // return KeyHasher()(key) % TABLE_SIZE;
    }

    struct LockRequest {
        LockRequest(LockMode m, TransactionType *t) : txn(t), mode(m) {}
        TransactionType *txn; // Pointer to txn requesting the lock.
        LockMode
            mode; // Specifies whether this is a read or write lock request.
    };

    struct KeysList {
        KeysList(std::string m, std::deque<LockRequest> *t)
            : key(m), locksrequest(t) {}

        std::string key;
        std::deque<LockRequest> *locksrequest;
    };

  public:
    std::deque<KeysList> *lock_table_[TABLE_SIZE];
    std::deque<TransactionType *> ready_txns_;
    std::unordered_map<TransactionType *, int> txn_waits_;
};
#undef TABLE_SIZE

// /// @brief calvin queue
// class CalvinQueue {

//     private:
//     std::mutex                      mu;
//     std::queue<std::unique_ptr<T>>  queue;

//     public:
//     CalvinQueue() = default;
//     void Push(std::unique_ptr<T>&& tx);
//     std::unique_ptr<T> Pop();

// };

class CalvinExecutor;
class CalvinScheduler;

class Calvin : public Protocol {
  private:
    // EVMType             evm_type;
    CalvinTable table;
    Workload &workload;
    size_t batch_size{50};
    Statistics &statistics;
    std::atomic<bool> stop_flag{false};
    std::atomic<size_t> tx_counter{0};
    std::atomic<size_t> commit_num{0};

    std::unique_ptr<CalvinScheduler> scheduler;
    std::thread sche_worker;

    std::vector<std::unique_ptr<CalvinExecutor>> executors{};
    std::vector<std::thread> workers{};

    moodycamel::ConcurrentQueue<T *> done_queue;

    size_t n_lock_manager{1};
    size_t n_workers{1};
    friend class CalvinExecutor;
    friend class CalvinScheduler;

  public:
    Calvin(Workload &workload, Statistics &statistics, size_t n_threads,
           size_t n_dispatchers, size_t table_partitions);
    void Start() override;
    void Stop() override;
};

class CalvinExecutor {
  public:
    CalvinExecutor(Calvin &calvin);
    void RunTransactions();

  public:
    moodycamel::ConcurrentQueue<T *> transaction_queue;
    std::atomic<bool> &stop_flag;
    std::atomic<size_t> &commit_num;
    size_t &batch_size;
    size_t &n_lock_manager;
    size_t &n_workers;
    Workload &workload;
    Statistics &statistics;
    CalvinTable &table;
    std::size_t executor_id;
    moodycamel::ConcurrentQueue<T *> &done_queue;
};

class CalvinScheduler {
  public:
    CalvinScheduler(Calvin &calvin);
    std::unique_ptr<T> Create();
    void ScheduleTransactions();

  private:
    std::atomic<bool> &stop_flag;
    std::atomic<size_t> &commit_num;
    size_t &batch_size;
    size_t &n_lock_manager;
    size_t &n_workers;
    Workload &workload;
    Statistics &statistics;
    CalvinTable &table;
    size_t scheduler_id;
    LockManager lock_manager;
    moodycamel::ConcurrentQueue<T *> &done_queue;
    // std::vector<CalvinExecutor *> all_executors;
    std::vector<std::unique_ptr<CalvinExecutor>> &all_executors;
};

#undef K
#undef V
#undef T

} // namespace spectrum
