#[[
   Copyright 2022 The Silkpre Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
]]

find_package(ethash CONFIG REQUIRED)
find_package(intx CONFIG REQUIRED)

add_library(silkpre
    silkpre/blake2b.c
    silkpre/blake2b.h
    silkpre/ecdsa.c
    silkpre/ecdsa.h
    silkpre/precompile.cpp
    silkpre/precompile.h
    silkpre/rmd160.c
    silkpre/rmd160.h
    silkpre/secp256k1n.cpp
    silkpre/secp256k1n.hpp
    silkpre/sha256.c
    silkpre/sha256.h
)
target_include_directories(silkpre PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(silkpre PUBLIC intx::intx secp256k1 PRIVATE ethash::keccak ff gmp)
