# evmone: Ethereum Virtual Machine
# Copyright 2019 The evmone Authors.
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.16...3.27)

project(spectrum LANGUAGES CXX C)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS true)
add_compile_options(-Wno-attributes)

# -- Manage External Dependencies
include(FetchContent)
FetchContent_Declare(
    evmc
    GIT_REPOSITORY      "https://github.com/ethereum/evmc"
    GIT_TAG             "v11.0.1"
)
FetchContent_Declare(
    thread_pool
    GIT_REPOSITORY      "https://github.com/bshoshany/thread-pool"
    GIT_TAG             "v4.0.1"
)
FetchContent_Declare(
    fmt
    GIT_REPOSITORY      "https://github.com/fmtlib/fmt"
    GIT_TAG             "10.2.1"
)
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
FetchContent_MakeAvailable(glog gtest gflags ethash intx fmt evmc)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

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

file(GLOB_RECURSE SRC lib/*.cpp)
file(GLOB_RECURSE INC lib/*.hpp lib/*.h)

foreach(F ${INC})
    file(RELATIVE_PATH REL ${CMAKE_CURRENT_SOURCE_DIR}/lib ${F})
    configure_file(lib/${REL} include/spectrum/${REL} COPYONLY)
    unset(REL)
endforeach()

add_library(spectrum ${SRC})

target_include_directories(
    spectrum
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/include
)
target_compile_features(
    spectrum
    PUBLIC cxx_std_20
)
target_link_libraries(
    spectrum
    PUBLIC evmc intx::intx
    PRIVATE ethash::keccak fmt
)

unset(SRC)
unset(INC)

# -- add some unit-tests

file(GLOB_RECURSE SRC unit-tests/*.cpp)

add_executable(unit-tests ${SRC})
target_link_libraries(unit-tests PRIVATE gtest_main spectrum)

unset(SRC)

# -- add the bench

file(GLOB_RECURSE SRC bench/*.cpp)

add_executable(bench ${SRC})
target_link_libraries(bench PRIVATE spectrum)

unset(SRC)