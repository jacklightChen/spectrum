#include <iostream>
#include <spectrum/protocol.hpp>
#include <spectrum/protocol-spectrum.hpp>
#include <spectrum/protocol-sparkle.hpp>
#include <spectrum/protocol-aria-fb.hpp>
#include <spectrum/workload-smallbank.hpp>
#include <spectrum/workload.hpp>
#include <glog/logging.h>
#include <ranges>
#include <string_view>
#include <fmt/core.h>
#include <iterator>

using namespace spectrum;

auto split(std::string s) {
    return s | std::ranges::views::split(':') | 
    std::ranges::views::transform([](auto&& str) {
        return std::string_view(&*str.begin(), std::ranges::distance(str));
    });
}

int main(int argc, char* argv[]) {
    CHECK(argc == 3);
    auto workload = [&](){
        auto args = split(std::string{argv[1]});
        auto name = *args.begin();
        auto iter = args.begin();
        return Smallbank();
    };
    auto protocol = [&](){
        auto args = split(std::string{argv[0]});
        auto name = *args.begin();
        auto iter = args.begin();
        #define OPT(X, N, Y) if (name == (X)) { \
            auto dist = std::distance(args.begin(), args.end()); \
            if (dist != N) throw fmt::format("protocol ({}) has {} args, but we found {}", name, N, dist); \
            return (Y); \
        };
        OPT("aria",     3, Aria    (workload, size_t(++iter), size_t(++iter), size_t(++iter)))
        OPT("sparkle",  2, Sparkle (workload, size_t(++iter), size_t(++iter)))
        OPT("spectrum", 4, Spectrum(workload, size_t(++iter), size_t(++iter), size_t(++iter), ++iter))
        #undef OPT
        throw fmt::format("unknown protocol option ({})", name);
    }();
    std::cerr << protocol << std::endl;
}