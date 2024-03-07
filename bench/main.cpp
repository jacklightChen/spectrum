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
#include <stdexcept>
#include <chrono>
#include <glog/logging.h>

using namespace spectrum;
using namespace std::chrono_literals;
using namespace std::chrono;

auto split(std::basic_string_view<char> s) {
    return s | std::ranges::views::split(':')
    | std::ranges::views::transform([](auto&& str) { return std::string_view(&*str.begin(), std::ranges::distance(str)); });
}

auto to_size_t(std::basic_string_view<char> s) {
    std::stringstream sstream(std::string{s});
    size_t result; sstream >> result;
    return result;
}

auto to_bool(std::basic_string_view<char> s) {
    if (s == "TRUE")    { return true; }
    if (s == "FALSE")   { return false; }
    throw std::runtime_error(std::string{fmt::format("cannot recognize ({}) as boolean should be either TRUE or FALSE", s)});
}

auto to_duration(std::basic_string_view<char> s) {
    std::stringstream is(std::string{s});
	static const std::unordered_map<std::string, milliseconds> suffix {
        {"ms", 1ms}, {"s", 1s}, {"m", 1min}, {"h", 1h}};
    unsigned n {};
    std::string unit;
    if (is >> n >> unit) {
        try {
            return duration_cast<milliseconds>(n * suffix.at(unit));
        } catch (const std::out_of_range&) {
            std::cerr << "ERROR: Invalid unit specified\n";
        }
    } else {
        std::cerr << "ERROR: Could not convert to numeric value\n";
    }
	return milliseconds{};
}

int main(int argc, char* argv[]) {
    CHECK(argc == 4);
    google::InitGoogleLogging(argv[0]);
    auto workload = [&](){
        auto args = split(argv[2]);
        auto name = *args.begin();
        auto iter = args.begin();
        return std::unique_ptr<Workload>(new Smallbank());
    }();
    auto protocol = [&](){
        auto args = split(argv[1]);
        auto iter = args.begin();
        auto name = *iter++;
        #define INT     to_size_t(*iter++)
        #define BOOL    to_bool(*iter++)
        #define EVMTYPE ParseEVMType(*iter++)
        #define OPT(X, N, Y) if (name == (X)) { \
            auto dist = std::distance(args.begin(), args.end()); \
            if (dist != N + 1) throw std::runtime_error(std::string{fmt::format("protocol ({}) has {} args, but we found {}", X, N, dist - 1)}); \
            return std::unique_ptr<Protocol>(new Y); \
        };
        OPT("Aria",     4, Aria    (*workload, INT, INT, INT, BOOL))
        OPT("Sparkle",  2, Sparkle (*workload, INT, INT))
        OPT("Spectrum", 4, Spectrum(*workload, INT, INT, INT, EVMTYPE))
        #undef INT
        #undef BOOL
        #undef EVMTYPE
        #undef OPT
        throw std::runtime_error(std::string{fmt::format("unknown protocol option ({})", std::string{name})});
    }();
    auto start_time = steady_clock::now();
    protocol->Start();
    std::this_thread::sleep_for(to_duration(argv[3]));
    auto statistics = protocol->Stop();
    statistics.PrintWithDuration(duration_cast<milliseconds>(steady_clock::now() - start_time));
}