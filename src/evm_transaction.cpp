#include "evm_host_impl.hpp"
#include "evm_transaction.hpp"
#include "evmcow/baseline.hpp"
#include "evmone/baseline.hpp"
#include "evmcow/vm.hpp"
#include "evmone/vm.hpp"
#include <evmc/evmc.h>
#include <evmc/evmc.hpp>
#include <variant>
#include <iostream>
#include <span>

namespace spectrum {

// constructor for transaction object
Transaction::Transaction(
    EVMType evm_type, 
    evmc::address from, 
    evmc::address to, 
    std::span<uint8_t> code,
    std::span<uint8_t> input
):
    input(input.begin(), input.end()),
    evm_type{evm_type},
    vm{(evm_type == EVMType::BASIC || evm_type == EVMType::STRAWMAN) ? 
        std::variant<evmone::VM, evmcow::VM>(std::move(evmone::VM())) : 
        std::variant<evmone::VM, evmcow::VM>(evmcow::VM())
    },
    tx_context{evmc_tx_context{
        .block_number = 42,
        .block_timestamp = 66,
        .block_gas_limit = 10000000000,
    }},
    host{Host(this->tx_context)},
    code{code}
{
    this->message = evmc_message{
        .kind = EVMC_CALL,
        .depth = 0,
        .gas = 999999999,
        .recipient = to,
        .sender = from,
        .input_data = &input[0],
        .input_size = input.size(),
        .value{0},
    };
}

// update set_storage handler
void Transaction::UpdateSetStorageHandler(spectrum::SetStorage handler) {
    host.set_storage_inner = handler;
}

// update get_storage handler
void Transaction::UpdateGetStorageHandler(spectrum::GetStorage handler) {
    host.get_storage_inner = handler;
}

size_t Transaction::MakeCheckpoint() {
    if (evm_type == EVMType::BASIC) {
        return 0;
    }
    if (evm_type == EVMType::STRAWMAN) {
        auto& _vm = std::get<evmone::VM>(vm);
        _vm.checkpoints.push_back(std::make_unique<evmone::ExecutionState>(*_vm.state.value()));
        return _vm.checkpoints.size() - 1;
    }
    if (evm_type == EVMType::COPYONWRITE) {
        auto& _vm = std::get<evmcow::VM>(vm);
        _vm.checkpoints.push_back(_vm.state.value()->save_checkpoint());
        return _vm.checkpoints.size() - 1;
    }
    return 0;
}

void Transaction::ApplyCheckpoint(size_t checkpoint_id) {
    if (evm_type == EVMType::BASIC) {
        return;
    }
    if (evm_type == EVMType::STRAWMAN) {
        auto& _vm = std::get<evmone::VM>(vm);
        _vm.checkpoints.resize(checkpoint_id + 1);
        _vm.state = std::make_unique<evmone::ExecutionState>(*_vm.checkpoints.back());
        return;
    }
    if (evm_type == EVMType::COPYONWRITE) {
        auto& _vm = std::get<evmcow::VM>(vm);
        _vm.checkpoints.resize(checkpoint_id + 1);
        _vm.state.value()->load_checkpoint(_vm.checkpoints.back());
        return;
    }
}

void Transaction::Break() {
    if (evm_type == EVMType::BASIC || evm_type == EVMType::STRAWMAN) {
        auto& _vm = std::get<evmone::VM>(vm);
        _vm.state->get()->will_break = true;
    }
    if (evm_type == EVMType::COPYONWRITE) {
        auto& _vm = std::get<evmcow::VM>(vm);
        _vm.state->get()->will_break = true;
    }
}

evmc_result Transaction::Execute() {
    if (evm_type == EVMType::BASIC || evm_type == EVMType::STRAWMAN) {
        auto& _vm = std::get<evmone::VM>(vm);
        auto host_interface = &host.get_interface();
        auto host_context   = host.to_context();
        auto result = evmone::baseline::execute(
            _vm, host_interface, host_context, 
            EVMC_SHANGHAI, &message,
            &code[0], code.size() - 1
        );
        return result;
    }
    if (evm_type == EVMType::COPYONWRITE) {
        auto& _vm = std::get<evmcow::VM>(vm);
        auto host_interface = &host.get_interface();
        auto host_context   = host.to_context();
        auto result = evmcow::baseline::execute(
            _vm, host_interface, host_context,
            EVMC_SHANGHAI, &message,
            &code[0], code.size() - 1
        );
        return result;
    }
    std::cerr << "not possible" << std::endl;
    std::terminate();
}

} // namespace spectrum
