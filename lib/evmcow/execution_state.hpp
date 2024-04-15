// evmcow: Fast Ethereum Virtual Machine implementation
// Copyright 2019 The evmcow Authors.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <evmc/evmc.hpp>
#include <intx/intx.hpp>
#include <string>
#include <vector>
#include <optional>
#include <glog/logging.h>

namespace evmcow
{
namespace baseline
{
class CodeAnalysis;
}

using code_iterator = const uint8_t*;
using uint256 = intx::uint256;
using bytes = std::basic_string<uint8_t>;
using bytes_view = std::basic_string_view<uint8_t>;

/// Provides memory for EVM stack.
class StackSpace
{
public:
    /// The maximum number of EVM stack items.
    static constexpr auto limit = 4096;

    /// Returns the pointer to the "bottom", i.e. below the stack space.
    [[nodiscard, clang::no_sanitize("bounds")]] uint256* bottom() const noexcept
    {
        return const_cast<uint256*>(m_stack_space - 1);
    }

    /// The storage allocated for maximum possible number of items.
    /// Items are aligned to 256 bits for better packing in cache lines.
    alignas(sizeof(uint256)) uint256 m_stack_space[limit];
};

const static size_t SLICE = size_t(2);

/// COW Stack implementation
class StackTop
{
public:
    /// stack height
    size_t height;
    /// space bottom
    uint256* base;
    /// space limit
    uint256* limit;
    /// pointers to slices
    std::array<uint256*, 1024/SLICE> slices;
    /// ownership
    std::array<bool, 1024/SLICE> ownership;
    /// initialize a stack top for nothing
    StackTop():
        height{0},
        base{nullptr},
        limit{nullptr},
        slices{{nullptr}},
        ownership{{false}}
    {}
    /// initialize a stack top with given space
    StackTop(uint256* base, uint256* limit): 
        height{0},
        base{base},
        limit{limit},
        slices{{nullptr}},
        ownership{{false}}
    {}
    /// copy a stack top
    StackTop(const StackTop& stack_top):
        height{stack_top.height},
        base{stack_top.base},
        limit{stack_top.limit},
        slices{stack_top.slices},
        ownership{stack_top.ownership}
    {}
    /// declare ownership of a slice
    inline void ensure(size_t slice_index) {
        if (ownership[slice_index] && slices[slice_index]) {
            return;
        }
        uint256* old_slice  = slices[slice_index];
        slices[slice_index] = base;
        uint256* new_slice  = base;
        ownership[slice_index] = true;
        base += SLICE;
        if (base > limit) {
            LOG(FATAL) << "exceed stack height limit";
        }
        if (old_slice != nullptr) {
            memcpy(new_slice, old_slice, SLICE * sizeof(uint256));
        }
    }
    /// push an item onto current stack top
    inline void push(const uint256& item) {
        ensure(height / SLICE);
        slices[height / SLICE][height % SLICE] = item;
        height += 1;
    }
    /// pop an item and return it
    inline const uint256 &pop() {
        height -= 1;
        return slices[height / SLICE][height % SLICE];
    }
    /// equivalent to get(0)
    inline const uint256& top() {
        return get(0);
    }
    /// equivalent to get(index)
    inline const uint256& operator[](size_t index) const {
        return get(index);
    }
    /// read item on stack, counting from stack top to stack bottom
    inline const uint256& get(size_t index) const {
        index = height - 1 - index;
        return slices[index / SLICE][index % SLICE];
    }
    /// write item to stack
    inline uint256& get_mut(size_t index) {
        index = height - 1 - index;
        ensure(index / SLICE);
        return slices[index / SLICE][index % SLICE];
    }
};

/// The EVM memory.
///
/// The implementations uses initial allocation of 4k and then grows capacity with 2x factor.
/// Some benchmarks has been done to confirm 4k is ok-ish value.
class Memory
{
    /// The size of allocation "page".
    static constexpr size_t page_size = 4 * 1024;

    /// The "virtual" size of the memory.
    size_t m_size = 0;

    /// Actual allocated memory.
    std::vector<uint8_t> m_data;

public:
    /// Creates Memory object with initial capacity allocation.
    Memory() noexcept: m_data{std::vector<uint8_t>(page_size)} { }

    /// Frees all allocated memory.
    ~Memory() noexcept {  }

    Memory(const Memory&) = default;

    uint8_t& operator[](size_t index) noexcept { return m_data[index]; }

    [[nodiscard]] const uint8_t* data() const noexcept {
        if (m_data.size() == 0) { return nullptr; }
        return &m_data.front();
    }
    [[nodiscard]] size_t size() const noexcept { return m_size; }

    /// Grows the memory to the given size. The extend is filled with zeros.
    ///
    /// @param new_size  New memory size. Must be larger than the current size and multiple of 32.
    void grow(size_t new_size) noexcept
    {
        // Restriction for future changes. EVM always has memory size as multiple of 32 bytes.
        INTX_REQUIRE(new_size % 32 == 0);

        // Allow only growing memory. Include hint for optimizing compiler.
        INTX_REQUIRE(new_size > m_size);

        m_data.resize(new_size, 0);
        m_size = new_size;
    }

    /// Virtually clears the memory by setting its size to 0. The capacity stays unchanged.
    void clear() noexcept { m_size = 0; }
};

/// Checkpoint of an execution state
struct Checkpoint {
    code_iterator code_it;
    StackTop    stack;
};

/// The execution position.
struct Position {
    code_iterator code_it;      ///< The position in the code.
};

/// Generic execution state for generic instructions implementations.
// NOLINTNEXTLINE(clang-analyzer-optin.performance.Padding)
class ExecutionState
{
public:
    std::optional<Position> position{std::nullopt};
    StackTop stack_top;
    int64_t gas_refund = 0;
    Memory memory;
    const evmc_message* msg = nullptr;
    evmc::HostContext host;
    evmc_revision rev = {};
    bytes return_data;
    bool will_break = false;

    /// Reference to original EVM code container.
    /// For legacy code this is a reference to entire original code.
    /// For EOF-formatted code this is a reference to entire container.
    bytes_view original_code;

    /// Reference to the EOF data section. May be empty.
    bytes_view data;

    evmc_status_code status = EVMC_SUCCESS;
    size_t output_offset = 0;
    size_t output_size = 0;

private:
    evmc_tx_context m_tx = {};

public:
    /// Pointer to code analysis.
    /// This should be set and used internally by execute() function of a particular interpreter.
    union
    {
        const baseline::CodeAnalysis* baseline = nullptr;
    } analysis{};

    std::vector<const uint8_t*> call_stack;

    /// Stack space allocation.
    ///
    /// This is the last field to make other fields' offsets of reasonable values.
    StackSpace stack_space;

    ExecutionState() = default;
    ~ExecutionState() = default;
    ExecutionState(const ExecutionState& state_ref):
        gas_refund{state_ref.gas_refund},
        memory{state_ref.memory},
        msg{state_ref.msg},
        host{state_ref.host},
        rev{state_ref.rev},
        return_data{state_ref.return_data},
        will_break{state_ref.will_break},
        original_code{state_ref.original_code},
        data{state_ref.data},
        status{state_ref.status},
        output_offset{state_ref.output_offset},
        output_size{state_ref.output_size},
        m_tx{state_ref.m_tx},
        analysis{state_ref.analysis},
        call_stack{state_ref.call_stack},
        stack_space{state_ref.stack_space},
        position{state_ref.position},
        stack_top{state_ref.stack_top}
    {}

    ExecutionState(const evmc_message& message, evmc_revision revision,
        const evmc_host_interface& host_interface, evmc_host_context* host_ctx, bytes_view _code,
        bytes_view _data) noexcept
      : msg{&message},
        host{host_interface, host_ctx},
        rev{revision},
        original_code{_code},
        data{_data}
    {
        this->stack_top = StackTop(
            &stack_space.m_stack_space[0], 
            &stack_space.m_stack_space[stack_space.limit]
        );
    }

    /// Make a checkpoint
    inline Checkpoint save_checkpoint() {
        auto checkpoint = Checkpoint{
            .code_it = this->position.value().code_it,
            .stack   = this->stack_top,
        };
        this->stack_top.ownership = std::array<bool, 1024/SLICE>{{false}};
        return checkpoint;
    }

    /// Load a checkpoint
    inline void load_checkpoint(const Checkpoint& checkpoint) {
        this->stack_top = checkpoint.stack;
        this->position  = std::optional{Position{checkpoint.code_it}};
    }

    /// Resets the contents of the ExecutionState so that it could be reused.
    void reset(const evmc_message& message, evmc_revision revision,
        const evmc_host_interface& host_interface, evmc_host_context* host_ctx, bytes_view _code,
        bytes_view _data) noexcept
    {
        position = std::nullopt;
        gas_refund = 0;
        memory.clear();
        msg = &message;
        host = {host_interface, host_ctx};
        rev = revision;
        return_data.clear();
        original_code = _code;
        data = _data;
        status = EVMC_SUCCESS;
        output_offset = 0;
        output_size = 0;
        m_tx = {};
    }

    [[nodiscard]] bool in_static_mode() const { return (msg->flags & EVMC_STATIC) != 0; }

    const evmc_tx_context& get_tx_context() noexcept
    {
        if (INTX_UNLIKELY(m_tx.block_timestamp == 0))
            m_tx = host.get_tx_context();
        return m_tx;
    }
};
}  // namespace evmcow
