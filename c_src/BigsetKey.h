// -------------------------------------------------------------------
//
// BigsetKey.h: Declaration of the BigsetKey class, which assists with parsing
// a bigset key stored in leveldb. See the comment block before the BigsetKey
// constructor in BigsetKey.cc for information about the serialized format of
// a bigset key.
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

#ifndef BASHO_BIGSET_KEY_H
#define BASHO_BIGSET_KEY_H

#include "BigsetClock.h"

namespace basho {
namespace bigset {

class BigsetKey
{
    enum KeyType
    {
        KeyTypeInvalid      = 0,
        KeyTypeClock        = 'c',
        KeyTypeSetTombstone = 'd',
        KeyTypeElement      = 'e',
        KeyTypeEnd          = 'z'
    };

    KeyType  m_KeyType;
    Slice    m_SetName;
    Slice    m_Element;
    Slice    m_Actor;
    uint64_t m_Counter;

public:
    BigsetKey( Slice key );

    KeyType GetKeyType() const { return m_KeyType; }

    bool IsClock() const { return KeyTypeClock == m_KeyType; }

    bool IsSetTombstone() const { return KeyTypeSetTombstone == m_KeyType; }

    bool IsElement() const { return KeyTypeElement == m_KeyType; }

    bool IsEnd() const { return KeyTypeEnd == m_KeyType; }

    bool IsValid() const { return KeyTypeInvalid != m_KeyType; }

    const Slice& GetSetName() const { return m_SetName; }

    const Slice& GetActor() const { return m_Actor; }

    const Slice& GetElement() const { return m_Element; }

    uint64_t GetCounter() const { return m_Counter; }
};

// key decoding helper routines (taken from leveldb/util/comparator.cc)
// used in BigsetKey and elsewhere (e.g., BigsetComparator)
namespace internal {

Slice Get32PrefData( Slice& s );
uint64_t GetCounter( Slice& s );
Slice GetKeyType( Slice& s );

} // namespace internal

} // namespace bigset
} // namespace basho

#endif // BASHO_BIGSET_KEY_H
