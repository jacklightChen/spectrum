// evmone: Fast Ethereum Virtual Machine implementation
// Copyright 2019 The evmone Authors.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <evmc/evmc.hpp>
#include <intx/intx.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "glog/logging.h"

namespace evmone
{
namespace advanced
{
struct AdvancedCodeAnalysis;
}
namespace baseline
{
struct CodeAnalysis;
}

using uint256 = intx::uint256;
using bytes = std::basic_string<uint8_t>;
using bytes_view = std::basic_string_view<uint8_t>;

/// The view/controller for EVM stack.
class Stack {
 private:
  /// random it
  int id{0};
  /// the memory space bottom pointer
  uint256* m_lower = nullptr;
  /// the memory space upper bound pointer
  uint256* m_upper = nullptr;
  /// current version tracker
  uint32_t version{1};
  /// pointers to memory slices
  std::vector<std::array<uint256*, 16>> m_sli{std::array<uint256*, 16>{nullptr}};
  /// ownership flag
  std::vector<std::array<bool, 16>> m_own{std::array<bool, 16>{false}};
  /// ensure the slice indicated by i_sli is owned by current version
  void ensure(uint32_t i_sli) {
    // LOG(INFO) << "ensure " << i_sli << std::endl;
    if (m_own.back()[i_sli] && m_sli.back()[i_sli] != nullptr) return;
    auto old_sli = (m_sli.back()[i_sli]);
    auto new_sli = (m_sli.back()[i_sli] = m_lower);
    m_own.back()[i_sli] = true;
    m_lower += 64;
    if (old_sli != nullptr) memcpy(new_sli, old_sli, 64 * sizeof(uint256));
    CHECK(m_lower < m_upper) << "[" << m_lower << ":" << m_upper << ":" << m_sli.size() << "]" << std::endl;
  }

 public:
  /// current stack height, initially zero
  uint32_t height{0};

  /// Init with the provided stack space.
  Stack(uint256* space_bottom) noexcept {
    id = static_cast<int>(std::time(nullptr)) * 1000 + std::rand() % 1000; 
    reset(space_bottom);
  }

  /// The current number of items on the stack.

  [[nodiscard]] int size() const noexcept { return (int)height; }

  /// Returns the reference to the top item.
  // NOLINTNEXTLINE(readability-make-member-function-const)
  [[nodiscard]] uint256& top() {
    auto last = height - 1;
    ensure(last >> 6);
    auto& item = m_sli.back()[last >> 6][last & 63];
    // LOG(INFO) << "top value " << item[0] << ":" << item[1] << 
    // ":" << item[2] << ":" << item[3] << std::endl;
    return item;
  }

  /// Returns the reference to the stack item on given position from the stack
  /// top.
  // NOLINTNEXTLINE(readability-make-member-function-const)
  [[nodiscard]] uint256& get_mut(int index) {
    assert(index >= 0);
    auto rvs_index = height - 1 - (uint32_t)index;
    ensure(rvs_index >> 6);
    auto& item = m_sli.back()[rvs_index >> 6][rvs_index & 63];
    // LOG(INFO) << "stack " << id << " indexing " << index << " value " << item[0] << ":" << item[1] << 
    // ":" << item[2] << ":" << item[3] << std::endl;
    return item;
  }

  /// Returns the const reference to the stack item on given position from the
  /// stack top.
  [[nodiscard]] const uint256& get(int index) const {
    assert(index >= 0);
    auto rvs_index = height - 1 - (uint32_t)index;
    auto& item = m_sli.back()[rvs_index >> 6][rvs_index & 63];
    // LOG(INFO) << "stack " << id << " const indexing " << index << " value " << item[0] << ":" << item[1] << 
    // ":" << item[2] << ":" << item[3] << std::endl;
    return item;
  }

  /// Pushes an item on the stack. The stack limit is not checked.
  void push(const uint256& item) {
    ensure(height >> 6);
    m_sli.back()[height >> 6][height & 63] = item;
    // LOG(INFO) << "stack " << id << " push item " << item[0] << ":" << item[1] << ":" << item[2] << ":" << item[3] << " when height is " << height << std::endl;
    height += 1;
  }

  /// Returns an item popped from the top of the stack.
  uint256 pop() {
    height -= 1;
    auto& item = m_sli.back()[height >> 6][height & 63];
    // LOG(INFO) << "stack " << id << " pop item " << item[0] << ":" << item[1] << ":" << item[2] << ":" << item[3] << " when height is " << height << std::endl;
    return m_sli.back()[height >> 6][height & 63];
  }

  /// Empties the stack by resetting the top item pointer to the new provided
  /// stack space.
  void reset(uint256* space_bottom) {
    // LOG(INFO) << "stack " << id << " reset (" << space_bottom << ")" << std::endl;
    m_lower = space_bottom + 1;
    m_upper = space_bottom + 10241;
    m_sli.resize(0);
    m_sli.push_back(std::array<uint256*, 16>{nullptr});
    m_own.resize(0);
    m_own.push_back(std::array<bool, 16>{false});
    height = 0;
    version = 1;
  }

  /// Switch to a new version by adding a new slice array
  std::tuple<uint32_t, uint32_t, uint256*> make_checkpoint() {
    // LOG(INFO) << "stack " << id << " make checkpoint (" << version << ":" << height << ":" << m_lower << ")" << std::endl;
    CHECK(m_lower < m_upper);
    CHECK(version == m_sli.size());
    CHECK(version == m_own.size());
    m_sli.push_back(m_sli.back());
    m_own.push_back(std::array<bool, 16>{false});
    version += 1;
    return {version - 1, height, m_lower};
  }

  /// Roll back to a given checkpoint
  void goto_checkpoint(std::tuple<uint32_t, uint32_t, uint256*> ckpt) {
    // LOG(INFO) << "stack " << id << " goto checkpoint (" << version << ":" << height << ":" << m_lower << ")" << std::endl;
    CHECK(m_lower < m_upper);
    version = std::get<0>(ckpt);
    height = std::get<1>(ckpt);
    m_lower = std::get<2>(ckpt);
    m_sli.resize(version);
    m_own.resize(version); 
  }

  std::int64_t rest_size() {
    return m_upper - m_lower;
  }
};

/// Provides memory for EVM stack.
class StackSpace
{
public:
    /// The maximum number of EVM stack items.
    static constexpr auto limit_virt = 1024;
    static constexpr auto limit_phys = 8192;

    /// Returns the pointer to the "bottom", i.e. below the stack space.
    [[nodiscard, clang::no_sanitize("bounds")]] uint256* bottom() noexcept
    {
        return m_stack_space - 1;
    }

private:
    /// The storage allocated for maximum possible number of items.
    /// Items are aligned to 256 bits for better packing in cache lines.
    alignas(sizeof(uint256)) uint256 m_stack_space[limit_phys];
};


/// The EVM memory.
///
/// The implementations uses initial allocation of 4k and then grows capacity with 2x factor.
/// Some benchmarks has been done to confirm 4k is ok-ish value.
class Memory
{
    /// The size of allocation "page".
    static constexpr size_t page_size = 4 * 1024;

    /// Pointer to allocated memory.
    uint8_t* m_data = nullptr;

    /// The "virtual" size of the memory.
    size_t m_size = 0;

    /// The size of allocated memory. The initialization value is the initial capacity.
    size_t m_capacity = page_size;

    [[noreturn, gnu::cold]] static void handle_out_of_memory() noexcept { std::terminate(); }

    void allocate_capacity() noexcept
    {
        m_data = static_cast<uint8_t*>(std::realloc(m_data, m_capacity));
        if (m_data == nullptr)
            handle_out_of_memory();
    }

public:
    /// Creates Memory object with initial capacity allocation.
    Memory() noexcept { allocate_capacity(); }

    /// Frees all allocated memory.
    ~Memory() noexcept { std::free(m_data); }

    Memory(const Memory&) = delete;
    Memory& operator=(const Memory&) = delete;

    uint8_t& operator[](size_t index) noexcept { return m_data[index]; }

    [[nodiscard]] const uint8_t* data() const noexcept { return m_data; }
    [[nodiscard]] size_t size() const noexcept { return m_size; }

    /// Grows the memory to the given size. The extend is filled with zeros.
    ///
    /// @param new_size  New memory size. Must be larger than the current size and multiple of 32.
    void grow(size_t new_size) noexcept
    {
        // Restriction for future changes. EVM always has memory size as multiple of 32 bytes.
        assert(new_size % 32 == 0);

        // Allow only growing memory. Include hint for optimizing compiler.
        assert(new_size > m_size);
        if (new_size <= m_size)
            INTX_UNREACHABLE();  // TODO: NOLINT(misc-static-assert)

        if (new_size > m_capacity)
        {
            m_capacity *= 2;  // Double the capacity.

            if (m_capacity < new_size)  // If not enough.
            {
                // Set capacity to required size rounded to multiple of page_size.
                m_capacity = ((new_size + (page_size - 1)) / page_size) * page_size;
            }

            allocate_capacity();
        }
        std::memset(m_data + m_size, 0, new_size - m_size);
        m_size = new_size;
    }

    /// Virtually clears the memory by setting its size to 0. The capacity stays unchanged.
    void clear() noexcept { m_size = 0; }
};

struct ExStateCheckpoint {
    // execution key
    uint256 key;
    // stack checkpoint
    std::tuple<uint32_t, uint32_t, uint256*> stk_checkpoint;
    // program counter
    size_t pc;
    // return data
    bytes return_data;
    // output_offset
    size_t output_offset;
    size_t output_size;
    std::int64_t gas_left;
};

/// Generic execution state for generic instructions implementations.
class ExecutionState
{
public:
    size_t id = 0;
    int64_t gas_left = 0;
    Memory memory{};
    const evmc_message* msg = nullptr;
    evmc::HostContext host;
    evmc_revision rev = {};
    bytes return_data;
    std::vector<ExStateCheckpoint> ckpt_list{};
    Stack stack;
    size_t pc{0};
    size_t count{0};

    /// Reference to original EVM code.
    /// TODO: Code should be accessed via code analysis only and this should be removed.
    bytes_view code;

    evmc_status_code status = EVMC_SUCCESS;
    size_t output_offset = 0;
    size_t output_size = 0;
    uint256 partial_revert_key = 0;
    bool will_partial_revert = false;
    bool signal_early_interrupt = false;

private:
    evmc_tx_context m_tx = {};

public:
    /// Pointer to code analysis.
    /// This should be set and used internally by execute() function of a particular interpreter.
    union
    {
        const baseline::CodeAnalysis* baseline = nullptr;
        const advanced::AdvancedCodeAnalysis* advanced;
    } analysis{};

    /// Stack space allocation.
    ///
    /// This is the last field to make other fields' offsets of reasonable values.
    StackSpace stack_space;

    ExecutionState(): stack{stack_space.bottom()} {}

    ExecutionState(const evmc_message& message, evmc_revision revision,
        const evmc_host_interface& host_interface, evmc_host_context* host_ctx,
        bytes_view _code) noexcept
      : gas_left{message.gas},
        msg{&message},
        host{host_interface, host_ctx},
        rev{revision},
        code{_code},
        stack{stack_space.bottom()}
    {
	    this->make_checkpoint(uint256{0});
    }

    /// Resets the contents of the ExecutionState so that it could be reused.
    void reset(const evmc_message& message, evmc_revision revision,
        const evmc_host_interface& host_interface, evmc_host_context* host_ctx,
        bytes_view _code) noexcept
    {
        gas_left = message.gas;
        memory.clear();
        msg = &message;
        host = {host_interface, host_ctx};
        rev = revision;
        return_data.clear();
        code = _code;
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

    void make_checkpoint(uint256 key) {
        auto stack_checkpoint = stack.make_checkpoint();
        ckpt_list.push_back(ExStateCheckpoint{key, stack_checkpoint, pc, return_data, output_offset, output_size, gas_left});
        CHECK(!ckpt_list.empty());
    }

    void goto_checkpoint(uint256 key) {
        // LOG(INFO) << id << " goto checkpoint: \n\tkey:" << 
          // std::hex << key[0] << ":" << key[1] << ":" << key[2] << ":" << key[3] << std::endl;
    //  while (!ckpt_list.empty() && ckpt_list.back().key != key) { ckpt_list.pop_back(); }
    //  return_data = ckpt_list.back().return_data;
    //    output_offset = ckpt_list.back().output_offset;
    //    output_size = ckpt_list.back().output_size;
    //    gas_left = ckpt_list.back().gas_left;
    //    stack.goto_checkpoint(ckpt_list.back().stk_checkpoint);
    //    pc = ckpt_list.back().pc;
	      this->goto_initial_checkpoint();
    }

    void goto_initial_checkpoint() {
      // LOG(INFO) << std::hex << "goto initial checkpoint" << std::endl;
      CHECK(!ckpt_list.empty());
      while (ckpt_list.size() != 1) { ckpt_list.pop_back(); }
      return_data = ckpt_list.back().return_data;
      output_offset = ckpt_list.back().output_offset;
      output_size = ckpt_list.back().output_size;
      gas_left = ckpt_list.back().gas_left;
      stack.goto_checkpoint(ckpt_list.back().stk_checkpoint);
      pc = ckpt_list.back().pc;
    }
};
}  // namespace evmone
