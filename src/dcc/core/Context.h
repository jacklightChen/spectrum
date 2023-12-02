
#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace dcc {
class Context {
 public:
  void set_star_partitioner() {
    if (protocol != "Star") {
      return;
    }
    if (coordinator_id == 0) {
      partitioner = "StarS";
    } else {
      partitioner = "StarC";
    }
  }

 public:
  std::size_t time_to_run = 20;
  std::size_t coordinator_id = 0;
  std::size_t partition_num = 0;
  std::size_t worker_num = 0;
  std::size_t coordinator_num = 0;
  std::size_t io_thread_num = 1;
  std::string protocol;
  std::string replica_group;
  std::string lock_manager;
  std::size_t batch_size = 240;  // star, calvin, dbx batch size
  std::size_t batch_flush = 10;
  std::size_t group_time = 40;  // ms
  std::size_t sleep_time = 50;  // us
  std::string partitioner;
  std::size_t delay_time = 0;
  std::string log_path;
  std::string cdf_path;
  std::size_t cpu_core_id = 0;
  std::size_t look_ahead = 0;
  int64_t initialWindowSize = 0;
  int64_t shrinkWindowSize = 0;
  std::size_t cold_record_ratio = 0;
  std::size_t cold_record_time = 8000;

  std::size_t durable_write_cost = 0;

  std::size_t contract_type = 0;
  std::size_t get_contract_type() { return contract_type; }

  bool tcp_no_delay = true;
  bool tcp_quick_ack = false;

  bool cpu_affinity = true;

  bool record_transaction = false;
  bool replay_transaction = false;

  bool sleep_on_retry = true;

  bool exact_group_commit = false;

  bool mvcc = false;
  bool bohm_local = false;
  bool bohm_single_spin = false;

  bool read_on_replica = false;
  bool local_validation = false;
  bool rts_sync = false;
  bool star_sync_in_single_master_phase = false;
  bool star_dynamic_batch_size = true;
  bool parallel_locking_and_validation = true;

  bool same_batch = false;  // calvin and bohm

  // spec modify
  bool aria_read_only_optmization = false;
  bool aria_reordering_optmization = false;
  bool aria_snapshot_isolation = false;

  std::size_t ariaFB_lock_manager;

  bool pwv_ycsb_star = false;

  bool operation_replication = false;

  bool intergrity_check = false;

  bool synthetic = false;

  std::vector<std::string> peers;
};
}  // namespace dcc
