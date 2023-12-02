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

#include "precompile.h"

#include <gmp.h>

#include <algorithm>
#include <cstring>
#include <limits>

#include <intx/intx.hpp>
#include <libff/algebra/curves/alt_bn128/alt_bn128_pairing.hpp>
#include <libff/algebra/curves/alt_bn128/alt_bn128_pp.hpp>
#include <libff/common/profiling.hpp>

#include <silkpre/blake2b.h>
#include <silkpre/ecdsa.h>
#include <silkpre/rmd160.h>
#include <silkpre/secp256k1n.hpp>
#include <silkpre/sha256.h>

enum {
    EVMC_ISTANBUL = 7,
    EVMC_BERLIN = 8,
};

static void right_pad(std::basic_string<uint8_t>& str, const size_t min_size) noexcept {
    if (str.length() < min_size) {
        str.resize(min_size, '\0');
    }
}

uint64_t silkpre_ecrec_gas(const uint8_t*, size_t, int) { return 3'000; }

SilkpreOutput silkpre_ecrec_run(const uint8_t* input, size_t len) {
    uint8_t* out{static_cast<uint8_t*>(std::malloc(32))};

    std::basic_string<uint8_t> d(input, len);
    right_pad(d, 128);

    const auto v{intx::be::unsafe::load<intx::uint256>(&d[32])};
    const auto r{intx::be::unsafe::load<intx::uint256>(&d[64])};
    const auto s{intx::be::unsafe::load<intx::uint256>(&d[96])};

    const bool homestead{false};  // See EIP-2
    if (!silkpre::is_valid_signature(r, s, homestead)) {
        return {out, 0};
    }

    if (v != 27 && v != 28) {
        return {out, 0};
    }

    std::memset(out, 0, 12);
    static secp256k1_context* context{secp256k1_context_create(SILKPRE_SECP256K1_CONTEXT_FLAGS)};
    if (!silkpre_recover_address(out + 12, &d[0], &d[64], v != 27, context)) {
        return {out, 0};
    }
    return {out, 32};
}

uint64_t silkpre_sha256_gas(const uint8_t*, size_t len, int) { return 60 + 12 * ((len + 31) / 32); }

SilkpreOutput silkpre_sha256_run(const uint8_t* input, size_t len) {
    uint8_t* out{static_cast<uint8_t*>(std::malloc(32))};
    silkpre_sha256(out, input, len, /*use_cpu_extensions=*/true);
    return {out, 32};
}

uint64_t silkpre_rip160_gas(const uint8_t*, size_t len, int) { return 600 + 120 * ((len + 31) / 32); }

SilkpreOutput silkpre_rip160_run(const uint8_t* input, size_t len) {
    uint8_t* out{static_cast<uint8_t*>(std::malloc(32))};
    std::memset(out, 0, 12);
    silkpre_rmd160(&out[12], input, len);
    return {out, 32};
}

uint64_t silkpre_id_gas(const uint8_t*, size_t len, int) { return 15 + 3 * ((len + 31) / 32); }

SilkpreOutput silkpre_id_run(const uint8_t* input, size_t len) {
    uint8_t* out{static_cast<uint8_t*>(std::malloc(len))};
    std::memcpy(out, input, len);
    return {out, len};
}

static intx::uint256 mult_complexity_eip198(const intx::uint256& x) noexcept {
    const intx::uint256 x_squared{x * x};
    if (x <= 64) {
        return x_squared;
    } else if (x <= 1024) {
        return (x_squared >> 2) + 96 * x - 3072;
    } else {
        return (x_squared >> 4) + 480 * x - 199680;
    }
}

static intx::uint256 mult_complexity_eip2565(const intx::uint256& max_length) noexcept {
    const intx::uint256 words{(max_length + 7) >> 3};  // ⌈max_length/8⌉
    return words * words;
}

uint64_t silkpre_expmod_gas(const uint8_t* ptr, size_t len, int rev) {
    const uint64_t min_gas{rev < EVMC_BERLIN ? 0 : 200u};

    std::basic_string<uint8_t> input(ptr, len);
    right_pad(input, 3 * 32);

    intx::uint256 base_len256{intx::be::unsafe::load<intx::uint256>(&input[0])};
    intx::uint256 exp_len256{intx::be::unsafe::load<intx::uint256>(&input[32])};
    intx::uint256 mod_len256{intx::be::unsafe::load<intx::uint256>(&input[64])};

    if (base_len256 == 0 && mod_len256 == 0) {
        return min_gas;
    }

    if (intx::count_significant_words(base_len256) > 1 || intx::count_significant_words(exp_len256) > 1 ||
        intx::count_significant_words(mod_len256) > 1) {
        return UINT64_MAX;
    }

    uint64_t base_len64{static_cast<uint64_t>(base_len256)};
    uint64_t exp_len64{static_cast<uint64_t>(exp_len256)};

    input.erase(0, 3 * 32);

    intx::uint256 exp_head{0};  // first 32 bytes of the exponent
    if (input.length() > base_len64) {
        input.erase(0, base_len64);
        right_pad(input, 3 * 32);
        if (exp_len64 < 32) {
            input.erase(exp_len64);
            input.insert(0, 32 - exp_len64, '\0');
        }
        exp_head = intx::be::unsafe::load<intx::uint256>(input.data());
    }
    unsigned bit_len{256 - clz(exp_head)};

    intx::uint256 adjusted_exponent_len{0};
    if (exp_len256 > 32) {
        adjusted_exponent_len = 8 * (exp_len256 - 32);
    }
    if (bit_len > 1) {
        adjusted_exponent_len += bit_len - 1;
    }

    if (adjusted_exponent_len < 1) {
        adjusted_exponent_len = 1;
    }

    const intx::uint256 max_length{std::max(mod_len256, base_len256)};

    intx::uint256 gas;
    if (rev < EVMC_BERLIN) {
        gas = mult_complexity_eip198(max_length) * adjusted_exponent_len / 20;
    } else {
        gas = mult_complexity_eip2565(max_length) * adjusted_exponent_len / 3;
    }

    if (intx::count_significant_words(gas) > 1) {
        return UINT64_MAX;
    } else {
        return std::max(min_gas, static_cast<uint64_t>(gas));
    }
}

SilkpreOutput silkpre_expmod_run(const uint8_t* ptr, size_t len) {
    std::basic_string<uint8_t> input(ptr, len);
    right_pad(input, 3 * 32);

    const uint64_t base_len{intx::be::unsafe::load<uint64_t>(&input[24])};
    input.erase(0, 32);

    const uint64_t exponent_len{intx::be::unsafe::load<uint64_t>(&input[24])};
    input.erase(0, 32);

    const uint64_t modulus_len{intx::be::unsafe::load<uint64_t>(&input[24])};
    input.erase(0, 32);

    if (modulus_len == 0) {
        uint8_t* out{static_cast<uint8_t*>(std::malloc(1))};
        return {out, 0};
    }

    right_pad(input, base_len + exponent_len + modulus_len);

    mpz_t base;
    mpz_init(base);
    if (base_len) {
        mpz_import(base, base_len, 1, 1, 0, 0, input.data());
        input.erase(0, base_len);
    }

    mpz_t exponent;
    mpz_init(exponent);
    if (exponent_len) {
        mpz_import(exponent, exponent_len, 1, 1, 0, 0, input.data());
        input.erase(0, exponent_len);
    }

    mpz_t modulus;
    mpz_init(modulus);
    mpz_import(modulus, modulus_len, 1, 1, 0, 0, input.data());

    uint8_t* out{static_cast<uint8_t*>(std::malloc(modulus_len))};
    std::memset(out, 0, modulus_len);

    if (mpz_sgn(modulus) == 0) {
        mpz_clear(modulus);
        mpz_clear(exponent);
        mpz_clear(base);

        return {out, static_cast<size_t>(modulus_len)};
    }

    mpz_t result;
    mpz_init(result);

    mpz_powm(result, base, exponent, modulus);

    // export as little-endian
    mpz_export(out, nullptr, -1, 1, 0, 0, result);
    // and convert to big-endian
    std::reverse(out, out + modulus_len);

    mpz_clear(result);
    mpz_clear(modulus);
    mpz_clear(exponent);
    mpz_clear(base);

    return {out, static_cast<size_t>(modulus_len)};
}

// Utility functions for zkSNARK related precompiled contracts.
// See Yellow Paper, Appendix E "Precompiled Contracts", as well as
// https://eips.ethereum.org/EIPS/eip-196
// https://eips.ethereum.org/EIPS/eip-197
using Scalar = libff::bigint<libff::alt_bn128_q_limbs>;

// Must be called prior to invoking any other method.
// May be called many times from multiple threads.
static void init_libff() noexcept {
    // magic static
    [[maybe_unused]] static bool initialized = []() noexcept {
        libff::inhibit_profiling_info = true;
        libff::inhibit_profiling_counters = true;
        libff::alt_bn128_pp::init_public_params();
        return true;
    }();
}

static Scalar to_scalar(const uint8_t bytes_be[32]) noexcept {
    mpz_t m;
    mpz_init(m);
    mpz_import(m, 32, /*order=*/1, /*size=*/1, /*endian=*/0, /*nails=*/0, bytes_be);
    Scalar out{m};
    mpz_clear(m);
    return out;
}

// Notation warning: Yellow Paper's p is the same libff's q.
// Returns x < p (YP notation).
static bool valid_element_of_fp(const Scalar& x) noexcept {
    return mpn_cmp(x.data, libff::alt_bn128_modulus_q.data, libff::alt_bn128_q_limbs) < 0;
}

static std::optional<libff::alt_bn128_G1> decode_g1_element(const uint8_t bytes_be[64]) noexcept {
    Scalar x{to_scalar(bytes_be)};
    if (!valid_element_of_fp(x)) {
        return {};
    }

    Scalar y{to_scalar(bytes_be + 32)};
    if (!valid_element_of_fp(y)) {
        return {};
    }

    if (x.is_zero() && y.is_zero()) {
        return libff::alt_bn128_G1::zero();
    }

    libff::alt_bn128_G1 point{x, y, libff::alt_bn128_Fq::one()};
    if (!point.is_well_formed()) {
        return {};
    }
    return point;
}

static std::optional<libff::alt_bn128_Fq2> decode_fp2_element(const uint8_t bytes_be[64]) noexcept {
    // big-endian encoding
    Scalar c0{to_scalar(bytes_be + 32)};
    Scalar c1{to_scalar(bytes_be)};

    if (!valid_element_of_fp(c0) || !valid_element_of_fp(c1)) {
        return {};
    }

    return libff::alt_bn128_Fq2{c0, c1};
}

static std::optional<libff::alt_bn128_G2> decode_g2_element(const uint8_t bytes_be[128]) noexcept {
    std::optional<libff::alt_bn128_Fq2> x{decode_fp2_element(bytes_be)};
    if (!x) {
        return {};
    }

    std::optional<libff::alt_bn128_Fq2> y{decode_fp2_element(bytes_be + 64)};
    if (!y) {
        return {};
    }

    if (x->is_zero() && y->is_zero()) {
        return libff::alt_bn128_G2::zero();
    }

    libff::alt_bn128_G2 point{*x, *y, libff::alt_bn128_Fq2::one()};
    if (!point.is_well_formed()) {
        return {};
    }

    if (!(libff::alt_bn128_G2::order() * point).is_zero()) {
        // wrong order, doesn't belong to the subgroup G2
        return {};
    }

    return point;
}

static std::basic_string<uint8_t> encode_g1_element(libff::alt_bn128_G1 p) noexcept {
    std::basic_string<uint8_t> out(64, '\0');
    if (p.is_zero()) {
        return out;
    }

    p.to_affine_coordinates();

    auto x{p.X.as_bigint()};
    auto y{p.Y.as_bigint()};

    // Here we convert little-endian data to big-endian output
    static_assert(sizeof(x.data) == 32);

    std::memcpy(&out[0], y.data, 32);
    std::memcpy(&out[32], x.data, 32);

    std::reverse(out.begin(), out.end());
    return out;
}

uint64_t silkpre_bn_add_gas(const uint8_t*, size_t, int rev) { return rev >= EVMC_ISTANBUL ? 150 : 500; }

SilkpreOutput silkpre_bn_add_run(const uint8_t* ptr, size_t len) {
    std::basic_string<uint8_t> input(ptr, len);
    right_pad(input, 128);

    init_libff();

    std::optional<libff::alt_bn128_G1> x{decode_g1_element(input.data())};
    if (!x) {
        return {nullptr, 0};
    }

    std::optional<libff::alt_bn128_G1> y{decode_g1_element(&input[64])};
    if (!y) {
        return {nullptr, 0};
    }

    libff::alt_bn128_G1 sum{*x + *y};
    const std::basic_string<uint8_t> res{encode_g1_element(sum)};

    uint8_t* out{static_cast<uint8_t*>(std::malloc(res.length()))};
    std::memcpy(out, res.data(), res.length());
    return {out, res.length()};
}

uint64_t silkpre_bn_mul_gas(const uint8_t*, size_t, int rev) { return rev >= EVMC_ISTANBUL ? 6'000 : 40'000; }

SilkpreOutput silkpre_bn_mul_run(const uint8_t* ptr, size_t len) {
    std::basic_string<uint8_t> input(ptr, len);
    right_pad(input, 96);

    init_libff();

    std::optional<libff::alt_bn128_G1> x{decode_g1_element(input.data())};
    if (!x) {
        return {nullptr, 0};
    }

    Scalar n{to_scalar(&input[64])};

    libff::alt_bn128_G1 product{n * *x};
    const std::basic_string<uint8_t> res{encode_g1_element(product)};

    uint8_t* out{static_cast<uint8_t*>(std::malloc(res.length()))};
    std::memcpy(out, res.data(), res.length());
    return {out, res.length()};
}

static constexpr size_t kSnarkvStride{192};

uint64_t silkpre_snarkv_gas(const uint8_t*, size_t len, int rev) {
    uint64_t k{len / kSnarkvStride};
    return rev >= EVMC_ISTANBUL ? 34'000 * k + 45'000 : 80'000 * k + 100'000;
}

SilkpreOutput silkpre_snarkv_run(const uint8_t* input, size_t len) {
    if (len % kSnarkvStride != 0) {
        return {nullptr, 0};
    }
    size_t k{len / kSnarkvStride};

    init_libff();
    using namespace libff;

    static const auto one{alt_bn128_Fq12::one()};
    auto accumulator{one};

    for (size_t i{0}; i < k; ++i) {
        std::optional<alt_bn128_G1> a{decode_g1_element(&input[i * kSnarkvStride])};
        if (!a) {
            return {nullptr, 0};
        }
        std::optional<alt_bn128_G2> b{decode_g2_element(&input[i * kSnarkvStride + 64])};
        if (!b) {
            return {nullptr, 0};
        }

        if (a->is_zero() || b->is_zero()) {
            continue;
        }

        accumulator = accumulator * alt_bn128_miller_loop(alt_bn128_precompute_G1(*a), alt_bn128_precompute_G2(*b));
    }

    uint8_t* out{static_cast<uint8_t*>(std::malloc(32))};
    std::memset(out, 0, 32);
    if (alt_bn128_final_exponentiation(accumulator) == one) {
        out[31] = 1;
    }
    return {out, 32};
}

uint64_t silkpre_blake2_f_gas(const uint8_t* input, size_t len, int) {
    if (len < 4) {
        // blake2_f_run will fail anyway
        return 0;
    }
    return intx::be::unsafe::load<uint32_t>(input);
}

SilkpreOutput silkpre_blake2_f_run(const uint8_t* input, size_t len) {
    if (len != 213) {
        return {nullptr, 0};
    }
    uint8_t f{input[212]};
    if (f != 0 && f != 1) {
        return {nullptr, 0};
    }

    SilkpreBlake2bState state{};
    if (f) {
        state.f[0] = std::numeric_limits<uint64_t>::max();
    }

    static_assert(intx::byte_order_is_little_endian);
    static_assert(sizeof(state.h) == 8 * 8);
    std::memcpy(&state.h, input + 4, 8 * 8);

    uint8_t block[SILKPRE_BLAKE2B_BLOCKBYTES];
    std::memcpy(block, input + 68, SILKPRE_BLAKE2B_BLOCKBYTES);

    std::memcpy(&state.t, input + 196, 8 * 2);

    uint32_t r{intx::be::unsafe::load<uint32_t>(input)};
    silkpre_blake2b_compress(&state, block, r);

    uint8_t* out{static_cast<uint8_t*>(std::malloc(64))};
    std::memcpy(&out[0], &state.h[0], 8 * 8);
    return {out, 64};
}

const SilkpreContract kSilkpreContracts[SILKPRE_NUMBER_OF_ISTANBUL_CONTRACTS] = {
    {silkpre_ecrec_gas, silkpre_ecrec_run},       {silkpre_sha256_gas, silkpre_sha256_run},
    {silkpre_rip160_gas, silkpre_rip160_run},     {silkpre_id_gas, silkpre_id_run},
    {silkpre_expmod_gas, silkpre_expmod_run},     {silkpre_bn_add_gas, silkpre_bn_add_run},
    {silkpre_bn_mul_gas, silkpre_bn_mul_run},     {silkpre_snarkv_gas, silkpre_snarkv_run},
    {silkpre_blake2_f_gas, silkpre_blake2_f_run},
};
