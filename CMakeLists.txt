cmake_minimum_required(VERSION 3.16...3.27)

# -- Configure project compiler options
project(spectrum LANGUAGES CXX C)
cmake_policy(SET CMP0077 NEW)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS true)
set(WITH_GFLAGS OFF)

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
FetchContent_MakeAvailable(gtest ethash intx fmt evmc thread_pool conqueue)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
find_package(Threads REQUIRED)

# -- download and build gflags
execute_process(
    COMMAND             git clone --branch "v2.2.2" "https://github.com/gflags/gflags" "_deps/gflags"
    WORKING_DIRECTORY   ${CMAKE_BINARY_DIR}
)
execute_process(
    COMMAND
        cmake -S . -B build
        -DBUILD_SHARED_LIBS=ON
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/_deps/gflags/build
    WORKING_DIRECTORY       ${CMAKE_BINARY_DIR}/_deps/gflags
    COMMAND_ERROR_IS_FATAL  ANY
)
execute_process(
    COMMAND                 cmake --build build --target install
    WORKING_DIRECTORY       ${CMAKE_BINARY_DIR}/_deps/gflags
    COMMAND_ERROR_IS_FATAL  ANY
)
set(gflags_DIR ${CMAKE_BINARY_DIR}/_deps/gflags/build)
find_package(gflags)
unset(gflags_DIR)

# -- download and build glog
execute_process(
    COMMAND                 git clone --branch "v0.7.0" "https://github.com/google/glog" "_deps/glog"
    WORKING_DIRECTORY       ${CMAKE_BINARY_DIR}
)
execute_process(
    COMMAND
        cmake -S . -B build
        -Dgflags_DIR=${CMAKE_BINARY_DIR}/_deps/gflags/build
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/_deps/glog/build
    WORKING_DIRECTORY       ${CMAKE_BINARY_DIR}/_deps/glog
    COMMAND_ERROR_IS_FATAL  ANY
)
execute_process(
    COMMAND                cmake --build build --target install
    WORKING_DIRECTORY       ${CMAKE_BINARY_DIR}/_deps/glog
    COMMAND_ERROR_IS_FATAL  ANY
)

set(glog_DIR ${CMAKE_BINARY_DIR}/_deps/glog/build)
find_package(glog)
unset(glog_DIR)

if(DEBUG)
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
    add_compile_options(
        -O0 -ggdb -Wall -Wno-reorder -Wno-attributes
        -DPROJECT_VERSION="0.12.0-dev"
        -DEVMONE_X86_64_ARCH_LEVEL=2
        -DEVM_PRINT_INSTRUCTIONS=${EVM_PRINT_INSTRUCTIONS}
    )
else()
    add_compile_options(
        -O3 -Wall -Wno-reorder -Wno-attributes
        -DNDEBUG -ggdb
        -DPROJECT_VERSION="0.12.0-dev"
        -DEVMONE_X86_64_ARCH_LEVEL=2
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
    PRIVATE gflags ethash::keccak glog::glog Threads::Threads
)

unset(SRC)
unset(INC)

# -- add some unit-tests

file(GLOB_RECURSE SRC unit-tests/*.test.cpp)

foreach(F ${SRC})
    file(RELATIVE_PATH REL ${CMAKE_CURRENT_SOURCE_DIR}/unit-tests ${F})
    string(REPLACE ".test.cpp" "" REL ${REL})
    add_executable(unit-test-${REL} ${F})
    target_link_libraries(
        unit-test-${REL}
        PRIVATE gflags gtest_main spectrum glog::glog Threads::Threads
    )
endforeach()

unset(SRC)

# -- add the bench

file(GLOB_RECURSE SRC bin/*.cpp)

foreach(F ${SRC})
    message(STATUS ${F})
    file(RELATIVE_PATH REL ${CMAKE_CURRENT_SOURCE_DIR}/bin ${F})
    string(REPLACE ".cpp" "" REL ${REL})
    add_executable(${REL} ${F})
    target_link_libraries(
        ${REL}
        PRIVATE gflags spectrum glog::glog fmt Threads::Threads
    )
endforeach()

unset(SRC)
