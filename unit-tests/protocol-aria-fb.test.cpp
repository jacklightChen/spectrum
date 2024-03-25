#include <spectrum/protocol-aria-fb.hpp>
#include <spectrum/workload-smallbank.hpp>
#include <gtest/gtest.h>
#include "glog-prefix-install.hpp"

namespace
{

using namespace std::chrono_literals;
using namespace spectrum;

TEST(Aria, JustRunSmallbank) {
    GLOG_PREFIX;
    auto statistics = Statistics();
    auto workload = Smallbank(10000, 0.0);
    auto protocol = Aria(
        workload, statistics, 8 /* threads */, 32 /* table partitions */, 2 /* repeat */,
        true /* enable reordering */
    );
    protocol.Start();
    std::this_thread::sleep_for(1000ms);
    protocol.Stop();
    statistics.Print();   
}

} // namespace
