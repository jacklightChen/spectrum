cmake_minimum_required(VERSION 3.16.2)

# if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/third_party/evmone/evmc/.git)
#   message(FATAL_ERROR "Git submodules not initialized, execute:\n  git submodule update --init --recursive")
# endif()

get_directory_property(SILKWORM_HAS_PARENT PARENT_DIRECTORY)
if(NOT SILKWORM_HAS_PARENT)
  include(third_party/evmone/cmake/cable/bootstrap.cmake)
  include(CableBuildType)
  include(CableBuildInfo)
  cable_set_build_type(DEFAULT Release CONFIGURATION_TYPES Release Debug)

  if(NOT CMAKE_TOOLCHAIN_FILE)
    set(CMAKE_TOOLCHAIN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/cmake/toolchain/cxx20.cmake CACHE FILEPATH "" FORCE)
  endif()

  include(third_party/evmone/cmake/cable/HunterGate.cmake)
  HunterGate(
    URL "https://github.com/cpp-pm/hunter/archive/v0.24.3.tar.gz"
    SHA1 "10738b59e539818a01090e64c2d09896247530c7"
    LOCAL
    )
endif()

project(specturm)
set(PROJECT_VERSION 0.0.1)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) 

cable_add_buildinfo_library(PROJECT_NAME ${PROJECT_NAME})

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/compiler_settings.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Hunter/core_packages.cmake)

add_compile_definitions(EVMC_LOADER_MOCK)

find_package(intx CONFIG REQUIRED)   # Required from here below
find_package(ethash CONFIG REQUIRED) # Required from here below
find_package(Microsoft.GSL CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(Catch2 CONFIG REQUIRED)

add_subdirectory(third_party/evmone/evmc)

add_library(evmone third_party/evmone/lib/evmone/advanced_analysis.cpp
                   third_party/evmone/lib/evmone/advanced_analysis.hpp
                   third_party/evmone/lib/evmone/advanced_execution.cpp
                   third_party/evmone/lib/evmone/advanced_execution.hpp
                   third_party/evmone/lib/evmone/advanced_instructions.cpp
                   third_party/evmone/lib/evmone/baseline_instruction_table.cpp
                   third_party/evmone/lib/evmone/baseline_instruction_table.hpp
                   third_party/evmone/lib/evmone/baseline.cpp
                   third_party/evmone/lib/evmone/baseline.hpp
                   third_party/evmone/lib/evmone/execution_state.hpp
                   third_party/evmone/lib/evmone/instructions_calls.cpp
                   third_party/evmone/lib/evmone/instructions_traits.hpp
                   third_party/evmone/lib/evmone/instructions_xmacro.hpp
                   third_party/evmone/lib/evmone/instructions.hpp
                   third_party/evmone/lib/evmone/opcodes_helpers.h
                   third_party/evmone/lib/evmone/tracing.cpp
                   third_party/evmone/lib/evmone/tracing.hpp
                   third_party/evmone/lib/evmone/vm.cpp
                   third_party/evmone/lib/evmone/vm.hpp)
set_source_files_properties(third_party/evmone/lib/evmone/vm.cpp PROPERTIES COMPILE_DEFINITIONS PROJECT_VERSION="0.9.0-dev")
target_include_directories(evmone PUBLIC third_party/evmone/include third_party/evmone/lib)
target_link_libraries(evmone PUBLIC evmc intx::intx PRIVATE evmc::instructions evmc::hex)

if(MSVC)
  target_compile_options(evmone PRIVATE /EHsc /w)
else()
  target_compile_options(evmone PRIVATE -fno-exceptions)
endif()
# add_subdirectory(third_party/silkpre)

if(NOT MSVC)
  # add_compile_options(-Werror -Wall -Wextra -pedantic)
  # add_compile_options(-Wshadow -Wimplicit-fallthrough -Wsign-conversion)
  # add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-Wold-style-cast>)
  # add_compile_options(-Wno-missing-field-initializers -Wnon-virtual-dtor)
  # HZC
  add_compile_options(-pthread -Wall -Wextra -Wno-unused-variable -Wno-unused-function -Wno-unused-label -Wno-variadic-macros -Wno-unused-parameter -Wno-attributes -Wno-missing-field-initializers -Wno-type-limits -Wno-unused-but-set-variable -Wno-deprecated-copy -pedantic -O2)

  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_compile_options(-Wno-attributes)

    # coroutines support
    if((CMAKE_CXX_STANDARD GREATER_EQUAL 20) AND (NOT SILKWORM_WASM_API))
      add_compile_options(-fcoroutines)
    endif()
  endif()

  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" AND CMAKE_SYSTEM_NAME MATCHES "Darwin")
    add_compile_definitions(_LIBCPP_ENABLE_THREAD_SAFETY_ANNOTATIONS)
    add_compile_options(-Wthread-safety)
  endif()
endif()

find_package(Jemalloc REQUIRED)

include_directories(${CMAKE_SOURCE_DIR}/src)
add_subdirectory(src)

add_executable(dcc_bench main.cpp)
target_link_libraries(dcc_bench silkworm_core glog gflags ${JEMALLOC_LIBRARIES})
# target_link_libraries(dcc_bench silkworm_core glog gflags)
# target_link_libraries(dcc_bench silkworm_core glog gflags)
# target_link_libraries(dcc_bench silkworm_core glog gflags tcmalloc)