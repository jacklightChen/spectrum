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

#include "hex.hpp"

#include <cassert>

std::string to_hex(const uint8_t* bytes, size_t len) {
    static const char* kHexDigits{"0123456789abcdef"};

    std::string out{};
    out.reserve(2 * len);

    for (size_t i{0}; i < len; ++i) {
        uint8_t x{bytes[i]};
        char lo{kHexDigits[x & 0x0f]};
        char hi{kHexDigits[x >> 4]};
        out.push_back(hi);
        out.push_back(lo);
    }

    return out;
}

static unsigned decode_hex_digit(char ch) {
    if (ch >= '0' && ch <= '9') {
        return ch - '0';
    } else if (ch >= 'a' && ch <= 'f') {
        return ch - 'a' + 10;
    } else if (ch >= 'A' && ch <= 'F') {
        return ch - 'A' + 10;
    }
    throw std::out_of_range{"not a hex digit"};
}

std::basic_string<uint8_t> from_hex(std::string_view hex) {
    if (hex.length() >= 2 && hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X')) {
        hex.remove_prefix(2);
    }

    assert(hex.length() % 2 == 0);

    std::basic_string<uint8_t> out;
    out.reserve(hex.length() / 2);

    unsigned carry{0};
    for (size_t i{0}; i < hex.size(); ++i) {
        unsigned v{decode_hex_digit(hex[i])};
        if (i % 2 == 0) {
            carry = v << 4;
        } else {
            out.push_back(static_cast<uint8_t>(carry | v));
        }
    }

    return out;
}
