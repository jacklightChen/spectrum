#pragma once
#include "evm_host_impl.hpp"
#include "evm_transaction.hpp"
#include "evmcow/baseline.hpp"
#include "evmcow/vm.hpp"
#include "evmone/baseline.hpp"
#include "evmone/vm.hpp"
#include <evmc/evmc.h>
#include <evmc/evmc.hpp>
#include <fmt/core.h>
#include <span>
#include <stdexcept>
#include <variant>
#include <vector>

namespace spectrum {

enum EVMType { BASIC = 0, STRAWMAN = 1, COPYONWRITE = 2 };

EVMType ParseEVMType(std::basic_string_view<char> s);

struct Result : public evmc_result {

    Result(evmc_result result);
    ~Result();
};

class Transaction {

    private:
    std::variant<evmone::VM, evmcow::VM> vm;
    spectrum::Host host;
    spectrum::EVMType evm_type;
    evmc_tx_context tx_context;
    bool partial;
    std::span<uint8_t> code;
    std::vector<uint8_t> input;
    evmc_message message;

    public:
    Transaction(EVMType evm_type, evmc::address from, evmc::address to,
                std::span<uint8_t> code, std::span<uint8_t> input);
    void UpdateSetStorageHandler(spectrum::SetStorage &&handler);
    void UpdateGetStorageHandler(spectrum::GetStorage &&handler);
    void Execute();
    void Break();
    void ApplyCheckpoint(size_t checkpoint_id);
    size_t MakeCheckpoint();

};

} // namespace spectrum

template <> struct fmt::formatter<spectrum::EVMType> {
    template <typename ParseContext> constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(spectrum::EVMType const &value, FormatContext &ctx) const {
        #define OPT(X) case spectrum::EVMType::X: return fmt::format_to(ctx.out(), "{}", #X);
        switch (value) { OPT(BASIC) OPT(STRAWMAN) OPT(COPYONWRITE) }
        #undef OPT
        throw std::runtime_error("unreachable");
    }
};
