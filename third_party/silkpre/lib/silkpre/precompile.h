/*
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
*/

#ifndef SILKPRE_PRECOMPILE_H_
#define SILKPRE_PRECOMPILE_H_

#include <stddef.h>
#include <stdint.h>

// See Yellow Paper, Appendix E "Precompiled Contracts"

#if defined(__cplusplus)
extern "C" {
#endif

enum {
    SILKPRE_NUMBER_OF_FRONTIER_CONTRACTS = 4,
    SILKPRE_NUMBER_OF_BYZANTIUM_CONTRACTS = 8,
    SILKPRE_NUMBER_OF_ISTANBUL_CONTRACTS = 9,
};

typedef struct SilkpreOutput {
    uint8_t* data;  // Has to be freed if not NULL!!!
    size_t size;
} SilkpreOutput;

typedef uint64_t (*SilkpreGasFunction)(const uint8_t* input, size_t len, int evmc_revision);
typedef SilkpreOutput (*SilkpreRunFunction)(const uint8_t* input, size_t len);

typedef struct SilkpreContract {
    SilkpreGasFunction gas;
    SilkpreRunFunction run;
} SilkpreContract;

uint64_t silkpre_ecrec_gas(const uint8_t* input, size_t len, int evmc_revision);
SilkpreOutput silkpre_ecrec_run(const uint8_t* input, size_t len);

uint64_t silkpre_sha256_gas(const uint8_t* input, size_t len, int evmc_revision);
SilkpreOutput silkpre_sha256_run(const uint8_t* input, size_t len);

uint64_t silkpre_rip160_gas(const uint8_t* input, size_t len, int evmc_revision);
SilkpreOutput silkpre_rip160_run(const uint8_t* input, size_t len);

uint64_t silkpre_id_gas(const uint8_t* input, size_t len, int evmc_revision);
SilkpreOutput silkpre_id_run(const uint8_t* input, size_t len);

// EIP-2565: ModExp Gas Cost
uint64_t silkpre_expmod_gas(const uint8_t* input, size_t len, int evmc_revision);
// EIP-198: Big integer modular exponentiation
SilkpreOutput silkpre_expmod_run(const uint8_t* input, size_t len);

// EIP-196: Precompiled contracts for addition and scalar multiplication on the elliptic curve alt_bn128
uint64_t silkpre_bn_add_gas(const uint8_t* input, size_t len, int evmc_revision);
SilkpreOutput silkpre_bn_add_run(const uint8_t* input, size_t len);

// EIP-196: Precompiled contracts for addition and scalar multiplication on the elliptic curve alt_bn128
uint64_t silkpre_bn_mul_gas(const uint8_t* input, size_t len, int evmc_revision);
SilkpreOutput silkpre_bn_mul_run(const uint8_t* input, size_t len);

// EIP-197: Precompiled contracts for optimal ate pairing check on the elliptic curve alt_bn128
uint64_t silkpre_snarkv_gas(const uint8_t* input, size_t len, int evmc_revision);
SilkpreOutput silkpre_snarkv_run(const uint8_t* input, size_t len);

// EIP-152: Add BLAKE2 compression function `F` precompile
uint64_t silkpre_blake2_f_gas(const uint8_t* input, size_t len, int evmc_revision);
SilkpreOutput silkpre_blake2_f_run(const uint8_t* input, size_t len);

extern const SilkpreContract kSilkpreContracts[SILKPRE_NUMBER_OF_ISTANBUL_CONTRACTS];

#if defined(__cplusplus)
}
#endif

#endif  // SILKPRE_PRECOMPILE_H_
