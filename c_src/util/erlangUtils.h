// -------------------------------------------------------------------
//
// erlangUtils.h: Declaration of Erlang helper functions for the Basho C/C++ utility library
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

#ifndef LIBBASHO_ERLANGUTILS_H
#define LIBBASHO_ERLANGUTILS_H

#include "buffer.h"

namespace basho {
namespace utils {

typedef Buffer<1024> ErlBuffer; // good general-purpose sized buffer for dealing with erlang term/binary stuff

// parses the string representation of an erlang binary term (e.g., "<<24,1,240>>"),
// putting the binary data in a Buffer (e.g., \x18\x01\F0)
int // number of bytes put in BinaryBuff; -1 on error
ParseErlangBinaryString(
    const std::string& BinaryStr,    // IN:  erlang binary term in string form
    ErlBuffer&         BinaryBuff ); // OUT: receives the binary bytes represented by BinaryStr; the BytesUsed property is set

// writes a 32-bit integer value to a buffer in big-endian format
bool
FormatBigEndianUint32(
    uint32_t Value,             // IN: value to write to the buffer
    char*    pBuff,             // IN: buffer where Value is written in big-endian format; must have at least 4 bytes available
    size_t   BuffSizeInBytes ); // IN: size of the buffer in bytes (ensures Value will fit)

// writes the contents of a buffer to an Erlang binary ETF record type
bool
WriteErlangBinary(
    const void* pBinary,          // IN: binary data to write as an Erlang binary ETF record
    size_t      BinarySize,       // IN: number of bytes in the binary data
    char*       OutputBuffer,     // IN: buffer where the record is written
    size_t      OutputBuffSize ); // IN: size of the buffer (ensures the record will fit)

} // namespace utils
} // namespace basho

#endif // LIBBASHO_ERLANGUTILS_H
