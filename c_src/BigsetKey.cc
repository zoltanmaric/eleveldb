// -------------------------------------------------------------------
//
// BigsetKey.cc: Implementation of the BigsetKey class, which assists with
// parsing a bigset key stored in leveldb. See the comment block below for
// information about the serialized format of a bigset key.
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

#include "BigsetKey.h"

// TODO: remove all this endian stuff and reuse the DecodeFixed32() and DecodeFixed64() functions from leveldb
//
// platform stuff taken from leveldb/port/port_posix.h
//
// NOTE: we need more build infrastructure for this to work; e.g., we need
// a build step that detects the platform type and sets things like the
// OS_XXX macro accordingly; for now, we assume we're running on a little-
// endian system
#if 0
#undef PLATFORM_IS_LITTLE_ENDIAN
#if defined(OS_MACOSX)

#include <machine/endian.h>

#if defined(__DARWIN_LITTLE_ENDIAN) && defined(__DARWIN_BYTE_ORDER)
#define PLATFORM_IS_LITTLE_ENDIAN \
        (__DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN)
#endif
#elif defined(OS_SOLARIS)
#include <sys/isa_defs.h>
#ifdef _LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN true
#else
#define PLATFORM_IS_LITTLE_ENDIAN false
#endif
#elif defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_NETBSD) ||\
      defined(OS_DRAGONFLYBSD) || defined(OS_ANDROID)
#include <sys/types.h>
#include <sys/endian.h>

#if !defined(PLATFORM_IS_LITTLE_ENDIAN) && defined(_BYTE_ORDER)
#define PLATFORM_IS_LITTLE_ENDIAN (_BYTE_ORDER == _LITTLE_ENDIAN)
#endif
#else
#include <endian.h>
#endif
#endif // 0
namespace port {
static const bool kLittleEndian = true; //PLATFORM_IS_LITTLE_ENDIAN;
}

// DecodeFixedXx() methods taken from leveldb/util/coding.h
//
// TODO: reuse the DecodeFixed32() function from leveldb
static uint32_t DecodeFixed32( const char* ptr )
{
    if ( port::kLittleEndian )
    {
        // Load the raw bytes
        uint32_t result;
        memcpy( &result,
                ptr,
                sizeof( result ) );  // gcc optimizes this to a plain load
        return result;
    }
    else
    {
        return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
                |
                (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) <<
                 8)
                |
                (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) <<
                 16)
                |
                (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) <<
                 24));
    }
}

// TODO: reuse the DecodeFixed64() function from leveldb
static uint64_t DecodeFixed64( const char* ptr )
{
    if ( port::kLittleEndian )
    {
        // Load the raw bytes
        uint64_t result;
        memcpy( &result,
                ptr,
                sizeof( result ) );  // gcc optimizes this to a plain load
        return result;
    }
    else
    {
        uint64_t lo = DecodeFixed32( ptr );
        uint64_t hi = DecodeFixed32( ptr + 4 );
        return (hi << 32) | lo;
    }
}

namespace basho {
namespace bigset {

namespace internal {

// key decoding helpers taken from leveldb/util/comparator.cc
Slice Get32PrefData( Slice& s )
{
    uint32_t actual = DecodeFixed32( s.data() );
    Slice    res    = Slice( s.data() + 4, actual );
    s.remove_prefix( 4 + actual );
    return res;
}

uint64_t GetCounter( Slice& s )
{
    uint64_t actual = DecodeFixed64( s.data() );
    s.remove_prefix( 8 );
    return actual;
}

Slice GetKeyType( Slice& s )
{
    Slice res = Slice( s.data(), 1 ); // one byte c, d, e, or z
    s.remove_prefix( 1 );
    return res;
}

} // namespace internal

// Bigset keys come in the following flavors:
//
// Clock keys are:
//  <<
//    SetNameLen:32/little-unsigned-integer, %% the length of the set name
//    SetName:SetNameLen/binary, %% Set name for byte-wise comparison
//    $c:1/binary, %% means clock
//    Actor/binary, %% The actual actor
//    >>
//
// Set Tombstone keys are:
//  <<
//    SetNameLen:32/little-unsigned-integer, %% the length of the set name
//    SetName:SetNameLen/binary, %% Set name for byte-wise comparison
//    $d:1/binary, %% means set tombstone
//    Actor/binary, %% The actual actor
//    >>
//
// Element keys are:
//  <<
//    SetNameLen:32/little-unsigned-integer, %% the length of the set name
//    SetName:SetNameLen/binary, %% Set name for byte-wise comparison
//    $e:1/binary, %% indicates an element
//    ElementLen:32/little-unsigned-integer, %% Length of the element
//    Element:ElementLen/binary, %% The actual element
//    ActorLen:32/little-unsigned-integer, %% Length of the actor ID
//    Actor:ActorLen/binary, %% The actual actor
//    Counter:64/little-unsigned-integer
//    >>
//
//  End Key is:
//  <<
//    SetNameLen:32/little-unsigned-integer, %% the length of the set name
//    SetName:SetNameLen/binary, %% Set name for byte-wise comparison
//    $z:1/binary, %% means end key, used for limiting streaming fold
//    >>
BigsetKey::BigsetKey( Slice key )
{
    m_SetName     = internal::Get32PrefData( key );
    Slice keyType = internal::GetKeyType( key );
    switch ( keyType[0] )
    {
        case 'c':
            m_KeyType = KeyTypeClock;
            break;

        case 'd':
            m_KeyType = KeyTypeSetTombstone;
            break;

        case 'e':
            m_KeyType = KeyTypeElement;
            break;

        case 'z':
            m_KeyType = KeyTypeEnd;
            break;

        default:
            m_KeyType = KeyTypeInvalid;
            break;
    }

    if ( IsClock() | IsSetTombstone() )
    {
        // actor is the remaining portion of the key (not length-prefixed)
        m_Actor = key;
    }
    else if ( IsElement() )
    {
        m_Element     = internal::Get32PrefData( key );
        m_Actor       = internal::Get32PrefData( key );
        m_Counter     = internal::GetCounter( key );
    }
}
} // namespace bigset
} // namespace basho
