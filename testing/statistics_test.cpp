#include<statistics.hpp>
#include<gtest/gtest.h>

TEST(Statistics, Print) {
    auto statistics = spectrum::Statistics();
    statistics.JournalCommit(10);
    statistics.JournalCommit(20);
    statistics.JournalCommit(30);
    statistics.Print();
}