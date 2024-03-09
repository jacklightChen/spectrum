#include <spectrum/protocol.hpp>
#include <spectrum/protocol-spectrum.hpp>
#include <spectrum/protocol-sparkle.hpp>
#include <spectrum/protocol-aria-fb.hpp>
#include <spectrum/protocol-serial.hpp>
#include <spectrum/workload.hpp>
#include <spectrum/workload-smallbank.hpp>

// count args and throw error
#define NUMARGS_HELPER(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, N, ...)    N
#define NUMARGS(X...)  NUMARGS_HELPER(X, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
#define THROW(X...)   throw std::runtime_error(std::string{fmt::format(X)})
// declare some helper macros for argument parser (by default, we name the token iterator by 'iter')
#define INT     to<size_t>  (*(iter++))
#define DOUBLE  to<double>  (*(iter++))
#define BOOL    to<bool>    (*(iter++))
#define EVMTYPE ParseEVMType(*(iter++))

using namespace spectrum;
using namespace std::chrono_literals;
using namespace std::chrono;

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

std::unique_ptr<Workload> ParseWorkload(const char* arg) {
    auto args = split(arg);
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
}

std::unique_ptr<Protocol> ParseProtocol(const char* arg, Workload& workload, Statistics& statistics) {
    auto args = split(arg);
    auto name = *args.begin();
    auto dist = (size_t) (std::distance(args.begin(), args.end()) - 1);
    auto iter = args.rbegin();
    // map each option to an argparser
    #define OPT(X, Y...) if (name == #X) { \
        auto n = (size_t) NUMARGS(Y);      \
        if (dist != n) THROW("protocol {} has {} args -- ({}), but we found {} args", #X, n, #Y, dist); \
        return static_cast<std::unique_ptr<Protocol>>(std::make_unique<X>(workload, statistics, Y));  \
    };
    OPT(Aria,     INT, INT, INT, BOOL)
    OPT(Sparkle,  INT, INT)
    OPT(Spectrum, INT, INT, INT, EVMTYPE)
    OPT(Serial,   EVMTYPE, INT)
    #undef OPT
    // fallback to an error
    THROW("unknown protocol option ({})", std::string{name});
}

// remove helper macros
#undef INT
#undef BOOL
#undef DOUBLE
#undef EVMTYPE
#undef NUMARGS_HELPER
#undef NUMARGS
#undef THROW