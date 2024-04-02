#include <cstdlib>
#include <spectrum/evmcow/execution_state.hpp>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <vector>
#include <algorithm>
#include <fmt/core.h>

namespace {

TEST(EVMCOWState, StackTop) {
    for (size_t j = 0; j < 100; ++j) {
        auto stack_space  = evmcow::StackSpace();
        auto stack_of_top = std::vector{evmcow::StackTop(&stack_space.m_stack_space[0], &stack_space.m_stack_space[0] + stack_space.limit)};
        auto stack_of_vec = std::vector<std::vector<evmcow::uint256>>(1);
        for (size_t i = 0; i < 1024; ++i) {
            auto a = std::rand() % std::max(stack_of_vec.back().size(), size_t{1});
            auto b = std::rand() % 1000;
            switch (std::rand() % 6) {
                case 0:
                    stack_of_vec.back().push_back({b});
                    stack_of_top.back().push({b});
                    ASSERT_EQ(stack_of_top.back().get(0), stack_of_vec.back().back());
                    DLOG(INFO) << fmt::format("push {}", b) << std::endl;
                    break;
                case 1:
                case 2:
                    if (stack_of_vec.back().size() == 0) break;
                    stack_of_vec.back()[stack_of_vec.back().size() - a - 1] = b;
                    stack_of_top.back().get_mut(a) = b;
                    ASSERT_EQ(stack_of_top.back().get_mut(a), b);
                    DLOG(INFO) << fmt::format("[{}] = {}", a, b) << std::endl;
                    break;
                case 3:
                case 4:
                    if (stack_of_vec.back().size() == 0) break;
                    stack_of_vec.back().pop_back();
                    stack_of_top.back().pop();
                    DLOG(INFO) << fmt::format("pop") << std::endl;
                    break;
                case 5:
                    stack_of_vec.push_back(stack_of_vec.back());
                    stack_of_top.push_back(stack_of_top.back());
                    stack_of_top.back().ownership = {false};
                    DLOG(INFO) << fmt::format("check") << std::endl;
                    break;
            }
            for (size_t k = 0; k < stack_of_vec.size(); ++k) {
                for (size_t j = 0; j < stack_of_vec[k].size(); ++j) {
                    ASSERT_EQ(stack_of_vec[k][stack_of_vec[k].size() - j - 1], stack_of_top[k][j]);
                }
            }
        }
    }
}

} // namespace