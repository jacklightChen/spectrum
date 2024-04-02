#include <spectrum/evmtxn/evm_transaction.hpp>
#include <spectrum/workload/ycsb.hpp>
#include <gtest/gtest.h>
#include <vector>

namespace {

TEST(YCSB, Rollback5R5W) {
    auto workload = spectrum::YCSB(1000, 1.0);
    auto testing = [&]{for (size_t i = 0; i < 5; ++i) {for (size_t j = 0; j < 5; ++j) {
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

} // namespace