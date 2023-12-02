#include "reuseTracer.hpp"
#include "evmone/advanced_analysis.hpp"
namespace silkworm {
void ReuseTracer::on_execution_start(evmc_revision rev, const evmc_message &msg,
                                     evmone::bytes_view bytecode) noexcept {
    execution_start_called_ = true;
    rev_ = rev;
    msg_ = msg;
    bytecode_ = Bytes{bytecode};
}
void ReuseTracer::on_instruction_start(
    uint32_t pc, const evmone::Stack& /*stack_top*/, int /*stack_height*/,
    const evmone::ExecutionState &state,
    const IntraBlockState &intra_block_state) noexcept {
    pc_stack_.push_back(pc);
    memory_size_stack_[pc] = state.memory.size();
    if (contract_address_) {
        storage_stack_[pc] = intra_block_state.get_current_storage(
            contract_address_.value(), key_.value_or(evmc::bytes32{}));
    }
}
void ReuseTracer::on_execution_end(
    const evmc_result &res, const IntraBlockState &intra_block_state) noexcept {
    execution_end_called_ = true;
    result_ = {res.status_code,
               static_cast<uint64_t>(res.gas_left),
               {res.output_data, res.output_size}};
    if (contract_address_ && pc_stack_.size() > 0) {
        const auto pc = pc_stack_.back();
        storage_stack_[pc] = intra_block_state.get_current_storage(
            contract_address_.value(), key_.value_or(evmc::bytes32{}));
    }
}
void ReuseTracer::on_precompiled_run(
    const evmc_result & /*result*/, int64_t /*gas*/,
    const IntraBlockState & /*intra_block_state*/) noexcept {}
void ReuseTracer::on_reward_granted(
    const CallResult & /*result*/,
    const IntraBlockState & /*intra_block_state*/) noexcept {}

bool ReuseTracer::execution_start_called() const {
    return execution_start_called_;
}
bool ReuseTracer::execution_end_called() const { return execution_end_called_; }
const Bytes &ReuseTracer::bytecode() const { return bytecode_; }
const evmc_revision &ReuseTracer::rev() const { return rev_; }
const evmc_message &ReuseTracer::msg() const { return msg_; }
const std::vector<uint32_t> &ReuseTracer::pc_stack() const { return pc_stack_; }
const std::map<uint32_t, std::size_t> &ReuseTracer::memory_size_stack() const {
    return memory_size_stack_;
}
const std::map<uint32_t, evmc::bytes32> &ReuseTracer::storage_stack() const {
    return storage_stack_;
}
const CallResult &ReuseTracer::result() const { return result_; }

} // namespace silkworm