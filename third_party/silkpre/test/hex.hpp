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

#ifndef SILKPRE_HEX_HPP_
#define SILKPRE_HEX_HPP_

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <string_view>

std::string to_hex(const uint8_t* bytes, size_t len);

std::basic_string<uint8_t> from_hex(std::string_view hex);

#endif  // SILKPRE_HEX_HPP_
