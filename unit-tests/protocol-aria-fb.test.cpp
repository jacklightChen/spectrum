#include <spectrum/protocol-aria-fb.hpp>
#include <spectrum/workload-smallbank.hpp>
#include <gtest/gtest.h>

namespace
{

using namespace std::chrono_literals;

TEST(Aria, JustRunSmallbank) {
    auto workload = spectrum::Smallbank();
    auto protocol = spectrum::Aria(
        workload, 128 /* batch */, 
        8 /* threads */, 32 /* table partitions */,
        true /* enable reordering */
    );
    protocol.Start();
    std::this_thread::sleep_for(1000ms);
    auto statistics = protocol.Stop();
    statistics.Print();
}

} // namespace