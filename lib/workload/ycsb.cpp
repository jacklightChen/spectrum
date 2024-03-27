#include "ycsb.hpp"
#include <spectrum/common/hex.hpp>
#include <fmt/core.h>
#include <glog/logging.h>
#include <optional>

namespace spectrum {

const static char* CODE = 
    #include "../../contracts/ycsb.bin"
;

YCSB::YCSB(size_t num_elements, double zipf_exponent): 
    evm_type{EVMType::STRAWMAN},
    rng{std::unique_ptr<Random>(new ThreadLocalRandom([&](){return (zipf_exponent > 0.0 ? 
        std::unique_ptr<Random>(new Zipf(num_elements, zipf_exponent)) : 
        std::unique_ptr<Random>(new Unif(num_elements))
    );}, std::thread::hardware_concurrency()))}
{
    LOG(INFO) << fmt::format("YCSB({}, {})", num_elements, zipf_exponent);
    this->code = spectrum::from_hex(std::string{CODE}).value();
}

void YCSB::SetEVMType(EVMType ty) { this->evm_type = ty; }

inline std::string to_string(uint32_t key) {
    auto ss = std::ostringstream();
    ss << std::setw(64) << std::setfill('0') << key;
    return ss.str();
}

Transaction YCSB::Next() {
    DLOG(INFO) << "ycsb next" << std::endl;
    #define X to_string(rng->Next())
    auto input = spectrum::from_hex([&]() {
        //  10 key 5 read 5 write(may be blind)
        auto s = std::string{"f3d7af72"};
        for (int i = 0; i <= 10; i++) { s += X; }
        return s;
    }()).value();
    #undef X
    return Transaction(this->evm_type, evmc::address{0x1}, evmc::address{0x1}, std::span{code}, std::span{input});
}

} // namespace spectrum
