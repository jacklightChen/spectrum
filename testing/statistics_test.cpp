#include<statistics.hpp>
#include<gtest/gtest.h>

TEST(Statistics, Print) {
    auto statistics = Statistics();
    statistics.Commit(10);
    statistics.Commit(20);
    statistics.Commit(30);
    statistics.Print();
}