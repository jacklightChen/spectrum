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

#define NUMARGS_HELPER(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, N, ...)    N
#define NUMARGS(X...)  NUMARGS_HELPER(X, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)

static auto split(std::basic_string_view<char> s) {
    return s | std::ranges::views::split(':')
    | std::ranges::views::transform([](auto&& str) { return std::string_view(&*str.begin(), std::ranges::distance(str)); });
}

template<typename T>
static T to(std::basic_string_view<char> s) {
    std::stringstream sstream(std::string{s});
    T result; sstream >> result;
    return result;
}

template<>
bool to<bool>(std::basic_string_view<char> s) {
    if (s == "TRUE")    { return true; }
    if (s == "FALSE")   { return false; }
    throw std::runtime_error(std::string{fmt::format("cannot recognize ({}) as boolean should be either TRUE or FALSE", s)});
}

static auto to_duration(std::basic_string_view<char> s) {
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
    #define INT     to<size_t>(*iter++)
    #define DOUBLE  to<double>(*iter++)
    #define BOOL    to<bool>(*iter++)
    #define EVMTYPE ParseEVMType(*iter++)
    auto workload = [&](){
        #define OPT(X, Y...) if (name == #X) { \
            auto dist = (size_t) (std::distance(args.begin(), args.end()) - 1); \
            auto n = (size_t) NUMARGS(Y); \
            if (dist != n) throw std::runtime_error(std::string{fmt::format("protocol {} has {} args -- ({}), but we found only {} args", #X, n, #Y, dist)}); \
            return std::unique_ptr<Workload>(new X (Y)); \
        };
        auto args = split(argv[2]);
        auto iter = args.begin();
        auto name = *iter++;
        OPT(Smallbank, INT, DOUBLE);
        throw std::runtime_error(std::string{fmt::format("unknown w option ({})", std::string{name})});
        #undef OPT
    }();
    auto protocol = [&](){
        #define OPT(X, Y...) if (name == #X) { \
            auto dist = (size_t) (std::distance(args.begin(), args.end()) - 1); \
            auto n = (size_t) NUMARGS(Y); \
            if (dist != n) throw std::runtime_error(std::string{fmt::format("protocol {} has {} args -- ({}), but we found only {} args", #X, n, #Y, dist)}); \
            return std::unique_ptr<Protocol>(new X (*workload, Y)); \
        };
        auto args = split(argv[1]);
        auto iter = args.begin();
        auto name = *iter++;
        OPT(Aria,     INT, INT, INT, BOOL)
        OPT(Sparkle,  INT, INT)
        OPT(Spectrum, INT, INT, INT, EVMTYPE)
        throw std::runtime_error(std::string{fmt::format("unknown protocol option ({})", std::string{name})});
        #undef OPT
    }();
    #undef INT
    #undef BOOL
    #undef DOUBLE
    #undef EVMTYPE
    auto start_time = steady_clock::now();
    protocol->Start();
    std::this_thread::sleep_for(to_duration(argv[3]));
    auto statistics = protocol->Stop();
    statistics.PrintWithDuration(duration_cast<milliseconds>(steady_clock::now() - start_time));
}
