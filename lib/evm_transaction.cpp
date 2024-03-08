#include "evm_host_impl.hpp"
#include "evm_transaction.hpp"
#include "evmcow/baseline.hpp"
#include "evmone/baseline.hpp"
#include "evmcow/vm.hpp"
#include "evmone/vm.hpp"
#include "hex.hpp"
#include <evmc/evmc.h>
#include <evmc/evmc.hpp>
#include <variant>
#include <iostream>
#include <span>
#include <glog/logging.h>
#include <fmt/core.h>
#include <stdexcept>

namespace spectrum {

EVMType ParseEVMType(std::basic_string_view<char> s) {
    if (s == "BASIC")       { return EVMType::BASIC; }
    if (s == "STRAWMAN")    { return EVMType::STRAWMAN; }
    if (s == "COPYONWRITE") { return EVMType::COPYONWRITE; }
    throw std::runtime_error(std::string{fmt::format("unknown evmtype {}", s)});
}

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
        .input_data = &this->input[0],
        .input_size = this->input.size(),
        .value{0},
    };
}

// update set_storage handler
void Transaction::UpdateSetStorageHandler(spectrum::SetStorage&& handler) {
    host.set_storage_inner = handler;
}

// update get_storage handler
void Transaction::UpdateGetStorageHandler(spectrum::GetStorage&& handler) {
    host.get_storage_inner = handler;
}

size_t Transaction::MakeCheckpoint() {
    // can only be called inside execution
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
    // can only be called inside execution
    if (evm_type == EVMType::BASIC || evm_type == EVMType::STRAWMAN) {
        auto& _vm = std::get<evmone::VM>(vm);
        _vm.state->get()->will_break = true;
    }
    if (evm_type == EVMType::COPYONWRITE) {
        auto& _vm = std::get<evmcow::VM>(vm);
        _vm.state->get()->will_break = true;
    }
}

__attribute__((always_inline))
void Transaction::Execute() {
    if (evm_type == EVMType::BASIC || evm_type == EVMType::STRAWMAN) {
        auto& _vm = std::get<evmone::VM>(vm);
        auto host_interface = &host.get_interface();
        auto host_context   = host.to_context();
        const auto result = evmone::baseline::execute(
            _vm, host_interface, host_context, 
            EVMC_SHANGHAI, &message,
            &code[0], code.size() - 1
        );
        if (result.status_code != evmc_status_code::EVMC_SUCCESS) {
            DLOG(ERROR) << "function hash: " << to_hex(std::span{&input[0], 4}) <<  " transaction status: " << result.status_code << std::endl;
        }
        if (result.output_data) {
            result.release(&result);
        }
        return;
    }
    if (evm_type == EVMType::COPYONWRITE) {
        auto& _vm = std::get<evmcow::VM>(vm);
        auto host_interface = &host.get_interface();
        auto host_context   = host.to_context();
        const auto result = evmcow::baseline::execute(
            _vm, host_interface, host_context,
            EVMC_SHANGHAI, &message,
            &code[0], code.size() - 1
        );
        if (result.status_code != evmc_status_code::EVMC_SUCCESS) {
            DLOG(ERROR) << "transaction status: " << result.status_code << std::endl;
        }
        if (result.output_data) {
            result.release(&result);
        }
        return;
    }
    LOG(FATAL) << "not possible";
}

Result::Result(evmc_result result):
    evmc_result{result}
{}

Result::~Result() {
    if (this->output_data) {
        this->release(this);
    }
}

} // namespace spectrum
