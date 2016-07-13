// -------------------------------------------------------------------
//
// stringUtils.h: Declaration of string helper functions for the Basho C/C++ utility library
//
// Copyright (c) 2016 Basho Technologies, Inc. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------

#ifndef LIBBASHO_STRINGUTILS_H
#define LIBBASHO_STRINGUTILS_H

#include <stdint.h>
#include <string>

namespace basho {
namespace utils {

// removes any instances of characters from the ends of a string
std::string TrimCharacters( const std::string& StrIn, const std::string& CharsToTrim );

// removes any whitespace (space, tab, CR, LF chars) from the ends of a string
std::string TrimWhitespace( const std::string& StrIn );

// formats an integer as a human-readable string with thousands-separators (NOTE: this is not locale aware)
std::string FormatIntAsString( uint64_t Value );

// formats a size in bytes as a human-readable string (e.g., formats 987 as
// "987 bytes" and formats 303841 as "296.72 KB"); if the IncludeExactSize
// parameter is true, appends the exact byte count if the value of SizeInBytes
// is greater than 1 KB (e.g., formats formats 987 as "987 bytes" and formats
// 303841 as "296.72 KB (303,841 bytes)")
std::string FormatSizeAsString( uint64_t SizeInBytes, bool IncludeExactSize = false );

} // namespace utils
} // namespace basho

#endif // LIBBASHO_STRINGUTILS_H
