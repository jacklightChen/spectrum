# evmone: Ethereum Virtual Machine
# Copyright 2019 The evmone Authors.
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.16...3.27)

project(spectrum LANGUAGES CXX C)
set(CMAKE_EXPORT_COMPILE_COMMANDS true)
add_compile_options(-Wno-attributes)

# -- Manage External Dependencies
include(FetchContent)
FetchContent_Declare(
    gtest
    GIT_REPOSITORY      "https://github.com/google/googletest"
    GIT_TAG             "v1.14.0"
)
FetchContent_Declare(
    gflags
    GIT_REPOSITORY      "https://github.com/gflags/gflags"
    GIT_TAG             "v2.2.2"
)
FetchContent_Declare(
    glog
    GIT_REPOSITORY      "https://github.com/google/glog"
    GIT_TAG             "v0.7.0-rc1"
)
FetchContent_Declare(
    ethash
    GIT_REPOSITORY      "https://github.com/chfast/ethash"
    GIT_TAG             "v1.0.1"
)
FetchContent_Declare(
    intx
    GIT_REPOSITORY      "https://github.com/chfast/intx"
    GIT_TAG             "v0.10.1"
)
FetchContent_MakeAvailable(glog gtest gflags ethash intx)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# -- Manage EVMC
if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/evmc/.git)
    message(FATAL_ERROR "Git submodules not initialized, execute:\n  git submodule update --init")
else()
    option(EVMC_TOOLS "Build EVMC test tools" ${EVMONE_TESTING})
    option(EVMC_INSTALL "Install EVMC" OFF)
    add_subdirectory(evmc)
endif()

# -- Determine Compile Definitions
if(CMAKE_SYSTEM_PROCESSOR STREQUAL x86_64)
    # Setup options for x86_64 micro-architecture levels.
    # https://clang.llvm.org/docs/UsersManual.html#x86

    set(EVMONE_X86_64_ARCH_LEVEL_INIT 2)
    if(APPLE)
        # On macos with Apple Silicon CPU (arm64) the x86 is emulated and SSE4.2 is not available.
        set(EVMONE_X86_64_ARCH_LEVEL_INIT 1)
    endif()

    set(EVMONE_X86_64_ARCH_LEVEL ${EVMONE_X86_64_ARCH_LEVEL_INIT} CACHE STRING "The x86_64 micro-architecture level")
    if(EVMONE_X86_64_ARCH_LEVEL GREATER_EQUAL 1 AND EVMONE_X86_64_ARCH_LEVEL LESS_EQUAL 4)
        message(STATUS "x86_64 micro-architecture level: ${EVMONE_X86_64_ARCH_LEVEL}")
        if(EVMONE_X86_64_ARCH_LEVEL GREATER_EQUAL 2)
            add_compile_options(-march=x86-64-v${EVMONE_X86_64_ARCH_LEVEL})
        endif()
    else()
        message(FATAL_ERROR "Invalid EVMONE_X86_64_ARCH_LEVEL: ${EVMONE_X86_64_ARCH_LEVEL}")
    endif()
endif()

add_compile_options(
    -O3
    -DPROJECT_VERSION="0.12.0-dev"
    -DEVMONE_X86_64_ARCH_LEVEL=${EVMONE_X86_64_ARCH_LEVEL}
    -DEVM_PRINT_INSTRUCTIONS=0
)

# -- add the main library
add_library(
    evmtwo
    src/evmcow/baseline.cpp
    src/evmcow/baseline_instruction_table.cpp
    src/evmcow/eof.cpp
    src/evmcow/instructions_calls.cpp
    src/evmcow/instructions_storage.cpp
    src/evmcow/tracing.cpp
    src/evmcow/vm.cpp
    src/evmone/baseline.cpp
    src/evmone/baseline_instruction_table.cpp
    src/evmone/eof.cpp
    src/evmone/instructions_calls.cpp
    src/evmone/instructions_storage.cpp
    src/evmone/tracing.cpp
    src/evmone/vm.cpp
    src/evm_host_impl.cpp
    src/evm_transaction.cpp
    src/workload.cpp
    src/hex.cpp
)

target_compile_features(
    evmtwo
    PUBLIC cxx_std_20
)
target_link_libraries(
    evmtwo
    PUBLIC evmc intx::intx
    PRIVATE ethash::keccak
)
target_include_directories(
    evmtwo
    PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
    $<INSTALL_INTERFACE:src>
)

# -- add a bench driver
add_executable(
    bench
    src/main.cpp
)
target_link_libraries(
    bench
    PRIVATE evmtwo
)

# -- some unit tests
add_executable(
    test
    testing/evm_transaction_test.cpp
    testing/table_test.cpp
)
target_link_libraries(
    test
    PRIVATE gtest_main evmtwo
)
