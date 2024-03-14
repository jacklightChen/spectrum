#include "./workload-ycsb.hpp"
#include "./hex.hpp"
#include <fmt/core.h>
#include <glog/logging.h>
#include <optional>

namespace spectrum {

YCSB::YCSB(size_t num_elements, double zipf_exponent)
    : evm_type{EVMType::STRAWMAN},
      rng{(zipf_exponent > 0.0
               ? std::unique_ptr<Random>(new Zipf(num_elements, zipf_exponent))
               : std::unique_ptr<Random>(new Unif(num_elements)))} {
    LOG(INFO) << fmt::format("YCSB({}, {})", num_elements, zipf_exponent);
    this->code =
        spectrum::from_hex(
            std::string{
                "608060405234801561000f575f80fd5b506004361061003f575f3560e01c80"
                "639507d39a14610043578063a5843f0814610073578063f3d7af721461008f"
                "575b5f80fd5b61005d6004803603810190610058919061022a565b6100ab56"
                "5b60405161006a9190610264565b60405180910390f35b61008d6004803603"
                "810190610088919061027d565b6100c4565b005b6100a96004803603810190"
                "6100a491906102bb565b6100dd565b005b5f805f8381526020019081526020"
                "015f20549050919050565b805f808481526020019081526020015f20819055"
                "505050565b5f805f8d81526020019081526020015f2054905081816100fd91"
                "906103d5565b5f808d81526020019081526020015f20819055505f805f8c81"
                "526020019081526020015f20549050828161013191906103d5565b5f808c81"
                "526020019081526020015f20819055505f805f8b8152602001908152602001"
                "5f20549050838161016591906103d5565b5f808b8152602001908152602001"
                "5f20819055505f805f8a81526020019081526020015f205490508481610199"
                "91906103d5565b5f808a81526020019081526020015f20819055505f805f89"
                "81526020019081526020015f2054905085816101cd91906103d5565b5f8089"
                "81526020019081526020015f20819055505050505050505050505050505050"
                "5050565b5f80fd5b5f819050919050565b610209816101f7565b8114610213"
                "575f80fd5b50565b5f8135905061022481610200565b92915050565b5f6020"
                "828403121561023f5761023e6101f3565b5b5f61024c84828501610216565b"
                "91505092915050565b61025e816101f7565b82525050565b5f602082019050"
                "6102775f830184610255565b92915050565b5f806040838503121561029357"
                "6102926101f3565b5b5f6102a085828601610216565b92505060206102b185"
                "828601610216565b9150509250929050565b5f805f805f805f805f805f6101"
                "608c8e0312156102db576102da6101f3565b5b5f6102e88e828f0161021656"
                "5b9b505060206102f98e828f01610216565b9a5050604061030a8e828f0161"
                "0216565b995050606061031b8e828f01610216565b985050608061032c8e82"
                "8f01610216565b97505060a061033d8e828f01610216565b96505060c06103"
                "4e8e828f01610216565b95505060e061035f8e828f01610216565b94505061"
                "01006103718e828f01610216565b9350506101206103838e828f0161021656"
                "5b9250506101406103958e828f01610216565b9150509295989b509295989b"
                "9093969950565b7f4e487b7100000000000000000000000000000000000000"
                "0000000000000000005f52601160045260245ffd5b5f6103df826101f7565b"
                "91506103ea836101f7565b9250828201905080821115610402576104016103"
                "a8565b5b9291505056fea2646970667358221220d4c4a0620c69278e046854"
                "1b78c69477e2702a38b30675fd0262509505a30d2464736f6c634300081800"
                "33"})
            .value();
}

void YCSB::SetEVMType(EVMType ty) { this->evm_type = ty; }

inline std::string to_string(uint32_t key) {
    auto ss = std::ostringstream();
    ss << std::setw(64) << std::setfill('0') << key;
    return ss.str();
}

Transaction YCSB::Next() {
    DLOG(INFO) << "ycsb next" << std::endl;
    auto guard = std::lock_guard{mu};
#define X to_string(rng->Next())
    auto input = spectrum::from_hex([&]() {
                     std::stringstream ss;
                     ss << std::string{"f3d7af72"};
                     //  10 key 5 read 5 write(may be blind)
                     for (int i = 0; i < 10; i++) {
                         ss << X;
                     }
                     // val
                     ss << X;

                     return ss.str();
                 }())
                     .value();
#undef X
    return Transaction(this->evm_type, evmc::address{0x1}, evmc::address{0x1},
                       std::span{code}, std::span{input});
}

} // namespace spectrum
