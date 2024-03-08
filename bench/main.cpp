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
#define THROW(X...)   throw std::runtime_error(std::string{fmt::format(X)})

static auto split(std::basic_string_view<char> s) {
    auto iter = s | std::ranges::views::split(':')
    | std::ranges::views::transform([](auto&& str) { return std::string_view(&*str.begin(), std::ranges::distance(str)); });
    auto toks = std::vector<std::string>();
    for (auto x: iter) {
        toks.push_back(std::string{x});
    }
    return toks;
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
    THROW("cannot recognize ({}) as boolean should be either TRUE or FALSE", s);
}

template<>
milliseconds to<milliseconds>(std::basic_string_view<char> s) {
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

void PrefixFormatter(std::ostream& s, const google::LogMessage& m, void* data) {
    auto color = [&m](){
        switch (m.severity()) {
            case 0: return "\e[1;36m";
            case 1: return "\e[1;33m";
            case 2: return "\e[1;31m";
            default: return "\e[0;30m";
        }
    }();
    s << color << std::setfill(' ') << std::setw(8)  << std::setiosflags(std::ios::left)
            << google::GetLogSeverityName(m.severity()) 
      << "\e[0;30m" << std::setfill(' ') << std::setw(30) 
            << fmt::format("{}:{}", m.basename(), m.line());
}

int main(int argc, char* argv[]) {
    // configure prefix formatting and eat google logging command line arguments
    google::InstallPrefixFormatter(PrefixFormatter);
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    // check if the rest arguments have the correct number
    CHECK(argc == 4) << "Except google logging flags, we only expect 3 flags. ";
    DLOG(WARNING) << "Debug Mode: don't expect good performance. " << std::endl;
    // declare a statistics collector
    auto statistics = std::make_unique<Statistics>();
    // declare some helper macros for argument parser (by default, we name the token iterator by 'iter')
    #define INT     to<size_t>  (*(iter++))
    #define DOUBLE  to<double>  (*(iter++))
    #define BOOL    to<bool>    (*(iter++))
    #define EVMTYPE ParseEVMType(*(iter++))
    // parse workload parameters
    auto workload = [argv](){
        auto args = split(argv[2]);
        auto name = *args.begin();
        auto dist = (size_t) (std::distance(args.begin(), args.end()) - 1);
        auto iter = args.rbegin();
        // map each option to an argparser
        #define OPT(X, Y...) if (name == #X) { \
            auto n = (size_t) NUMARGS(Y);      \
            DLOG(INFO) << #Y << std::endl;     \
            if (dist != n) THROW("workload {} has {} args -- ({}), but we found {} args", #X, n, #Y, dist); \
            return static_cast<std::unique_ptr<Workload>>(std::make_unique<X>(Y)); \
        };
        OPT(Smallbank, INT, DOUBLE)
        #undef OPT
        // fallback to an error
        THROW("unknown workload option ({})", std::string{name});
    }();
    // parse protocol parameters
    auto protocol = [&](){
        auto args = split(argv[1]);
        auto name = *args.begin();
        auto dist = (size_t) (std::distance(args.begin(), args.end()) - 1);
        auto iter = args.rbegin();
        // map each option to an argparser
        #define OPT(X, Y...) if (name == #X) { \
            auto n = (size_t) NUMARGS(Y);      \
            if (dist != n) THROW("protocol {} has {} args -- ({}), but we found {} args", #X, n, #Y, dist); \
            return static_cast<std::unique_ptr<Protocol>>(std::make_unique<X>(*workload, *statistics, Y));  \
        };
        OPT(Aria,     INT, INT, INT, BOOL)
        OPT(Sparkle,  INT, INT)
        OPT(Spectrum, INT, INT, INT, EVMTYPE)
        #undef OPT
        // fallback to an error
        THROW("unknown protocol option ({})", std::string{name});
    }();
    // parse test duration parameter
    auto duration = to<milliseconds>(argv[3]);
    // remove helper macros
    #undef INT
    #undef BOOL
    #undef DOUBLE
    #undef EVMTYPE
    // start running !!
    auto start_time = steady_clock::now();
    protocol->Start();
    std::this_thread::sleep_for(duration);
    protocol->Stop();
    // stop running and print statistics
    DLOG(WARNING) << "Debug Mode: don't expect good performance. " << std::endl;
    std::cerr << statistics->PrintWithDuration(duration_cast<milliseconds>(steady_clock::now() - start_time));
}

#undef THROW