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
find_package(Threads REQUIRED)
function(GitHub)
    cmake_parse_arguments(GH "SQUASH" "USER;REPO;BRANCH;WITH;PACK;CONFIG;DIR" "" "${ARGV}")
    message(STATUS ${GH_USER}/${GH_REPO}/${GH_BRANCH})
    if (EXISTS "${CMAKE_BINARY_DIR}/_deps/${GH_USER}/${GH_REPO}/.git")
        message("        + download skipped")
    else()
        message("        + download finish")
        execute_process(
            COMMAND                 git clone --quiet --branch "${GH_BRANCH}" "https://github.com/${GH_USER}/${GH_REPO}" "${CMAKE_BINARY_DIR}/_deps/${GH_USER}/${GH_REPO}"
            WORKING_DIRECTORY       ${CMAKE_BINARY_DIR}
            COMMAND_ERROR_IS_FATAL  ANY
            OUTPUT_QUIET ERROR_QUIET
        )
    endif()
    if(${GH_WITH} STREQUAL "CMAKE SUBDIR")
        set(CMAKE_MESSAGE_LOG_LEVEL__ ${CMAKE_MESSAGE_LOG_LEVEL})
        set(CMAKE_MESSAGE_LOG_LEVEL NOTICE)
        add_subdirectory(${CMAKE_BINARY_DIR}/_deps/${GH_USER}/${GH_REPO})
        set(CMAKE_MESSAGE_LOG_LEVEL ${CMAKE_MESSAGE_LOG_LEVEL__})
        message("        + configuration finish (SUBDIR)")
    else()
        execute_process(
            COMMAND                 cmake -S . -B build ${GH_CONFIG} -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/_deps/${GH_USER}/${GH_REPO}/install
            WORKING_DIRECTORY       ${CMAKE_BINARY_DIR}/_deps/${GH_USER}/${GH_REPO}
            COMMAND_ERROR_IS_FATAL  ANY
            OUTPUT_QUIET ERROR_QUIET
        )
        message("        + configuration finish")
        execute_process(
            COMMAND                 cmake --build build -j
            WORKING_DIRECTORY       ${CMAKE_BINARY_DIR}/_deps/${GH_USER}/${GH_REPO}
            COMMAND_ERROR_IS_FATAL  ANY
            OUTPUT_QUIET ERROR_QUIET
        )
        message("        + build finish")
        execute_process(
            COMMAND                 cmake --build build --target install
            WORKING_DIRECTORY       ${CMAKE_BINARY_DIR}/_deps/${GH_USER}/${GH_REPO}
            COMMAND_ERROR_IS_FATAL  ANY
            OUTPUT_QUIET ERROR_QUIET
        )
        message("        + install finish")
        set(CMAKE_PREFIX_PATH "${CMAKE_BINARY_DIR}/_deps/${GH_USER}/${GH_REPO}/install")
        if("${GH_PACK}" STREQUAL "")
            set(GH_PACK ${GH_REPO})
        endif()
        if(${GH_WITH} STREQUAL "CMAKE INSTALL")
            find_package(${GH_PACK} REQUIRED QUIET)
            message("        + find package finish")
        elseif(${GH_WITH} STREQUAL "CMAKE SCANLIB")
            include_directories(${CMAKE_BINARY_DIR}/_deps/${GH_USER}/${GH_REPO}/install/include)
            link_directories(${CMAKE_BINARY_DIR}/_deps/${GH_USER}/${GH_REPO}/install/lib)
        endif()
    endif()
endfunction()

GitHub(
    USER    "ethereum"
    REPO    "evmc"
    BRANCH  "v11.0.1"
    WITH    "CMAKE SUBDIR"
)
GitHub(
    USER    "fmtlib"
    REPO    "fmt"
    BRANCH  "10.2.1"
    WITH    "CMAKE SUBDIR"
)
GitHub(
    USER    "chfast"
    REPO    "ethash"
    BRANCH  "v1.0.1"
    WITH    "CMAKE INSTALL"
)
GitHub(
    USER    "chfast"
    REPO    "intx"
    BRANCH  "v0.10.1"
    WITH    "CMAKE INSTALL"
)
GitHub(
    USER    "gflags"
    REPO    "gflags"
    BRANCH  "v2.2.2"
    WITH    "CMAKE INSTALL"
    CONFIG  "-DBUILD_SHARED_LIBS=ON"
)
GitHub(
    USER    "google"
    REPO    "glog"
    BRANCH  "v0.7.0"
    WITH    "CMAKE INSTALL"
    CONFIG  "-Dgflags_DIR=${gflags_DIR}"
)
GitHub(
    USER    "google"
    REPO    "googletest"
    BRANCH  "v1.14.0"
    PACK    "GTest"
    WITH    "CMAKE SUBDIR"
)
GitHub(
    USER    "gperftools"
    REPO    "gperftools"
    BRANCH  "gperftools-2.15"
    WITH    "CMAKE SCANLIB"
)

# -- compile options

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

# -- add executables

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
