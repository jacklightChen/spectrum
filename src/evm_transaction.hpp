#pragma once
#include "evm_host_impl.hpp"
#include "evm_transaction.hpp"
#include "evmcow/baseline.hpp"
#include "evmcow/vm.hpp"
#include "evmone/baseline.hpp"
#include "evmone/vm.hpp"
#include <evmc/evmc.h>
#include <evmc/evmc.hpp>
#include <variant>
#include <vector>
#include <span>

namespace spectrum {

enum EVMType {
    BASIC,
    STRAWMAN,
    COPYONWRITE
};

class Transaction {
  private:
    std::variant<evmone::VM, evmcow::VM> vm;
    spectrum::Host host;
    spectrum::EVMType evm_type;
    evmc_tx_context tx_context;
    bool partial;
    std::span<uint8_t> code;
    evmc_message message;

  public:
    Transaction(
        EVMType evm_type, 
        evmc::address from, 
        evmc::address to,
        std::span<uint8_t> code,
        std::span<uint8_t> input
    );
    void UpdateSetStorageHandler(spectrum::SetStorage handler);
    void UpdateGetStorageHandler(spectrum::GetStorage handler);
    evmc_result Execute();
    void Break();
    void ApplyCheckpoint(size_t checkpoint_id);
    size_t MakeCheckpoint();
};

} // namespace spectrum
