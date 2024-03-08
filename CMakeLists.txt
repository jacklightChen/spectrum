cmake_minimum_required(VERSION 3.16...3.27)

# -- Configure project compiler options
project(spectrum LANGUAGES CXX C)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS true)

option(DEBUG OFF)
option(EVM_PRINT_INSTRUCTIONS "print each evm instruction on instruction start" 0)

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
    glog
    GIT_REPOSITORY      "https://github.com/google/glog"
    GIT_TAG             "v0.7.0"
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
FetchContent_Declare(
    conqueue
    GIT_REPOSITORY      "https://github.com/cameron314/concurrentqueue"
    GIT_TAG             "v1.0.4"
)
FetchContent_MakeAvailable(glog gtest ethash intx fmt evmc thread_pool conqueue)
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

if(DEBUG)
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
    add_compile_options(
        -O0 -ggdb -Wall -Wno-reorder -Wno-attributes
        -DPROJECT_VERSION="0.12.0-dev"
        -DEVMONE_X86_64_ARCH_LEVEL=${EVMONE_X86_64_ARCH_LEVEL}
        -DEVM_PRINT_INSTRUCTIONS=${EVM_PRINT_INSTRUCTIONS}
    )
else()
    add_compile_options(
        -O3 -flto -Wall -Wno-reorder -Wno-attributes
        -DNDEBUG -ggdb
        -DPROJECT_VERSION="0.12.0-dev"
        -DEVMONE_X86_64_ARCH_LEVEL=${EVMONE_X86_64_ARCH_LEVEL}
    )
endif()

# -- handle header-only dependencies
file(GLOB INC ${CMAKE_BINARY_DIR}/_deps/thread_pool-src/include/*.hpp ${CMAKE_BINARY_DIR}/_deps/thread_pool-src/include/*.h)

foreach(F ${INC})
    file(RELATIVE_PATH REL ${CMAKE_BINARY_DIR}/_deps/thread_pool-src/include ${F})
    configure_file(${F} include/thread_pool/${REL} COPYONLY)
    unset(REL)
endforeach()

file(GLOB INC ${CMAKE_BINARY_DIR}/_deps/conqueue-src/*.hpp ${CMAKE_BINARY_DIR}/_deps/conqueue-src/*.h)

foreach(F ${INC})
    file(RELATIVE_PATH REL ${CMAKE_BINARY_DIR}/_deps/conqueue-src ${F})
    configure_file(${F} include/conqueue/${REL} COPYONLY)
    unset(REL)
endforeach()

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
    PUBLIC evmc intx::intx fmt
    PRIVATE ethash::keccak glog::glog
)

unset(SRC)
unset(INC)

# -- add some unit-tests

file(GLOB_RECURSE SRC unit-tests/*.cpp)

foreach(F ${SRC})
    file(RELATIVE_PATH REL ${CMAKE_CURRENT_SOURCE_DIR}/unit-tests ${F})
    string(REPLACE ".test.cpp" "" REL ${REL})
    add_executable(unit-test-${REL} ${F})
    target_link_libraries(unit-test-${REL} PRIVATE gtest_main spectrum glog::glog)
endforeach()

unset(SRC)

# -- add the bench

file(GLOB_RECURSE SRC bench/*.cpp)

add_executable(bench ${SRC})
target_link_libraries(bench PRIVATE spectrum glog::glog fmt)

unset(SRC)
