# evmone: Ethereum Virtual Machine
# Copyright 2019 The evmone Authors.
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.16...3.27)

# -- Manage EVMC
if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/evmc/.git)
    message(FATAL_ERROR "Git submodules not initialized, execute:\n  git submodule update --init")
endif()

option(BUILD_SHARED_LIBS "Build evmone as a shared library" ON)

include(cmake/cable/bootstrap.cmake)
include(CableBuildType)
include(CableCompilerSettings)
include(CablePackage)
include(CableToolchains)
include(CMakePackageConfigHelpers)

cable_configure_toolchain(DEFAULT cxx17-pic)
cable_set_build_type(DEFAULT Release CONFIGURATION_TYPES Release Debug)

include(Hunter/init)

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
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(glog gtest gflags)

project(spectrum LANGUAGES CXX C)
set(PROJECT_VERSION 0.12.0-dev)

hunter_add_package(ethash)
find_package(ethash CONFIG REQUIRED)

option(EVMC_TOOLS "Build EVMC test tools" ${EVMONE_TESTING})
option(EVMC_INSTALL "Install EVMC" OFF)

add_subdirectory(evmc)

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

include(GNUInstallDirs)

add_compile_definitions(PROJECT_VERSION="${PROJECT_VERSION}" EVMONE_X86_64_ARCH_LEVEL=${EVMONE_X86_64_ARCH_LEVEL})
add_compile_options(-Wno-attributes)

set(include_dir ${CMAKE_CURRENT_SOURCE_DIR}/include)

hunter_add_package(intx)
find_package(intx CONFIG REQUIRED)

add_library(evmone
    src/evmpersist/advanced_analysis.cpp
    src/evmpersist/advanced_analysis.hpp
    src/evmpersist/advanced_execution.cpp
    src/evmpersist/advanced_execution.hpp
    src/evmpersist/advanced_instructions.cpp
    src/evmpersist/baseline.cpp
    src/evmpersist/baseline.hpp
    src/evmpersist/baseline_instruction_table.cpp
    src/evmpersist/baseline_instruction_table.hpp
    src/evmpersist/eof.cpp
    src/evmpersist/eof.hpp
    src/evmpersist/instructions.hpp
    src/evmpersist/instructions_calls.cpp
    src/evmpersist/instructions_opcodes.hpp
    src/evmpersist/instructions_storage.cpp
    src/evmpersist/instructions_traits.hpp
    src/evmpersist/instructions_xmacro.hpp
    src/evmpersist/opcodes_helpers.h
    src/evmpersist/tracing.cpp
    src/evmpersist/tracing.hpp
    src/evmpersist/vm.cpp
    src/evmpersist/vm.hpp
    src/evmone/advanced_analysis.cpp
    src/evmone/advanced_analysis.hpp
    src/evmone/advanced_execution.cpp
    src/evmone/advanced_execution.hpp
    src/evmone/advanced_instructions.cpp
    src/evmone/baseline.cpp
    src/evmone/baseline.hpp
    src/evmone/baseline_instruction_table.cpp
    src/evmone/baseline_instruction_table.hpp
    src/evmone/eof.cpp
    src/evmone/eof.hpp
    src/evmone/instructions.hpp
    src/evmone/instructions_calls.cpp
    src/evmone/instructions_opcodes.hpp
    src/evmone/instructions_storage.cpp
    src/evmone/instructions_traits.hpp
    src/evmone/instructions_xmacro.hpp
    src/evmone/opcodes_helpers.h
    src/evmone/tracing.cpp
    src/evmone/tracing.hpp
    src/evmone/vm.cpp
    src/evmone/vm.hpp
    src/checkpoint.hpp
)
target_compile_features(evmone PUBLIC cxx_std_20)
target_link_libraries(evmone PUBLIC evmc::evmc intx::intx PRIVATE ethash::keccak)
target_include_directories(evmone PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOUCE_DIR}/src/evmpersist>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOUCE_DIR}/src/evmone>
)
