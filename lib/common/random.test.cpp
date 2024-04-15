#include <spectrum/common/random.hpp>
#include <gtest/gtest.h>
#include <set>

TEST(Random, UniqueN) {
    for (auto i = 0; i < 100; ++i) {
        auto random = spectrum::Zipf(1000, 1.0);
        auto set = std::set<size_t>();
        auto vec = std::vector<size_t>(i + 1, 0);
        spectrum::SampleUniqueN(random, vec);
        for (auto x: vec) { set.insert(x); }
        ASSERT_EQ(set.size(), i + 1);
    }
}