#include "evmc/evmc.h"
#include "evmc/evmc.hpp"
#include <spectrum/transaction/evm-transaction.hpp>
#include <spectrum/transaction/evm-hash.hpp>
#include <spectrum/workload/ycsb.hpp>
#include <gtest/gtest.h>
#include <tuple>
#include <vector>

namespace {

// verify ycsb transaction always produce 5 reads and 5 writes
TEST(YCSB, Rollback5R5W) {
    auto workload = spectrum::YCSB(1000, 1.0);
    auto testing  = [&]{for (size_t i = 0; i < 5; ++i) {for (size_t j = 0; j < 5; ++j) {
        auto transaction = workload.Next();
        auto checkpoints = std::vector<size_t>();
        auto count_get = 0;
        auto count_put = 0;
        transaction.InstallGetStorageHandler(
            [&](auto addr, auto key) {
                count_get += 1;
                checkpoints.push_back(transaction.MakeCheckpoint());
                return key;
            }
        );
        transaction.InstallSetStorageHandler(
            [&](auto addr, auto key, auto value) {
                count_put += 1;
                return evmc_storage_status::EVMC_STORAGE_ASSIGNED;
            }
        );
        // execute for the first time
        transaction.Execute();
        ASSERT_EQ(count_get, 5);
        ASSERT_EQ(count_put, 5);
        // rollback and execute for the second time
        transaction.ApplyCheckpoint(checkpoints[i]);
        checkpoints.resize(i);
        transaction.Execute();
        ASSERT_EQ(count_get, 10 - i);
        ASSERT_EQ(count_put, 10 - i);
        // we also want to test repeated rollback, 
        //   so let's rollback and execute again!
        transaction.ApplyCheckpoint(checkpoints[j]);
        checkpoints.resize(j);
        transaction.Execute();
        ASSERT_EQ(count_get, 15 - i - j); // when i see you again
        ASSERT_EQ(count_put, 15 - i - j); // when i see you again ~~~
    }}};
    workload.SetEVMType(spectrum::COPYONWRITE);
    testing();
    workload.SetEVMType(spectrum::STRAWMAN);
    testing();
}

// verify the transaction produce the same execution trace
TEST(YCSB, RollbackReadSame) {
    auto workload = spectrum::YCSB(1000, 1.0);
    auto testing  = [&]{for (size_t i = 0; i < 100; ++i) {
        auto transaction = workload.Next();
        auto checkpoints = std::vector<std::tuple<size_t, size_t>>();
        // set up get storage handler to track read keys
        transaction.InstallGetStorageHandler(
            [&](auto addr, auto key) {
                checkpoints.push_back(std::tuple{
                    transaction.MakeCheckpoint(), 
                    spectrum::KeyHasher()(std::tuple{addr, key})
                });
                return key;
            }
        );
        transaction.InstallSetStorageHandler(
            [&](auto addr, auto key, auto value) {
                return evmc_storage_status::EVMC_STORAGE_ASSIGNED;
            }
        );
        transaction.Execute();
        if (i >= checkpoints.size()) continue;
        // rollback and check if we read the same keys in the second execution
        auto checkpoint_id = std::get<0>(checkpoints[i]);
        auto second_record = std::vector<std::tuple<size_t, size_t>>();
        transaction.ApplyCheckpoint(checkpoint_id);
        transaction.InstallGetStorageHandler(
            [&](auto addr, auto key) {
                second_record.push_back(std::tuple{
                    transaction.MakeCheckpoint(), 
                    spectrum::KeyHasher()(std::tuple{addr, key})
                });
                return key;
            }
        );
        for (auto j = 0; j < second_record.size(); ++j) {
            ASSERT_EQ(second_record[j], checkpoints[j + i]);
        }
    }};
    workload.SetEVMType(spectrum::COPYONWRITE);
    testing();
    workload.SetEVMType(spectrum::STRAWMAN);
    testing();
}

// see if the transaction continues correctly
TEST(YCSB, Continue) {
    auto workload = spectrum::YCSB(1000, 1.0);
    auto testing  = [&]{for (size_t i = 0; i < 100; ++i) {
        auto transaction = workload.Next();
        auto checkpoints = std::vector<std::tuple<size_t, size_t>>();
        // set up get storage handler to track read keys
        transaction.InstallGetStorageHandler(
            [&](auto addr, auto key) {
                checkpoints.push_back(std::tuple{
                    transaction.MakeCheckpoint(), 
                    spectrum::KeyHasher()(std::tuple{addr, key})
                });
                return key;
            }
        );
        transaction.InstallSetStorageHandler(
            [&](auto addr, auto key, auto value) {
                checkpoints.push_back(std::tuple{
                    transaction.MakeCheckpoint(), 
                    spectrum::KeyHasher()(std::tuple{addr, key})
                });
                return evmc_storage_status::EVMC_STORAGE_ASSIGNED;
            }
        );
        transaction.Execute();
        transaction.ApplyCheckpoint(0);
        // now let's how break works
        auto second_record = std::vector<std::tuple<size_t, size_t>>();
        transaction.InstallGetStorageHandler(
            [&](auto addr, auto key) {
                second_record.push_back(std::tuple{
                    transaction.MakeCheckpoint(),
                    spectrum::KeyHasher()(std::tuple{addr, key})
                });
                if (second_record.size() == i / 2  && i % 2 == 0) {
                    transaction.Break();
                }
                return key;
            }
        );
        transaction.InstallSetStorageHandler(
            [&](auto addr, auto key, auto value) {
                second_record.push_back(std::tuple{
                    transaction.MakeCheckpoint(),
                    spectrum::KeyHasher()(std::tuple{addr, key})
                });
                if (second_record.size() == i / 2  && i % 2 == 1) {
                    transaction.Break();
                }
                return evmc_storage_status::EVMC_STORAGE_ASSIGNED;
            }
        );
        transaction.Execute();
        transaction.Execute();
        ASSERT_EQ(second_record, checkpoints);
    }};
    workload.SetEVMType(spectrum::COPYONWRITE);
    testing();
    workload.SetEVMType(spectrum::STRAWMAN);
    testing();
}

} // namespace