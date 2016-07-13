// -------------------------------------------------------------------
//
// BigsetAccumulator.h: Declaration of the BigsetAccumulator class, which
// assists with processing bigset records stored in leveldb
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

#ifndef BASHO_BIGSET_ACC_H
#define BASHO_BIGSET_ACC_H

#include <list>

#include <leveldb/env.h>
#include "BigsetClock.h"

namespace basho {
namespace bigset {

class BigsetAccumulator
{
    leveldb::Logger* m_pLogger;
    Actor            m_ThisActor;
    Buffer           m_CurrentSetName;
    Buffer           m_CurrentElement;
    BigsetClock      m_SetTombstone;
    DotList          m_CurrentDots;

    Buffer           m_ReadyKey;
    Buffer           m_ReadyValue;
    bool             m_ElementReady;
    bool             m_ActorClockReady;
    bool             m_ActorClockSeen;
    bool             m_ActorSetTombstoneSeen;
    bool             m_FinishedReadingMetadata; // true => the bigset clock and set tombstone (if present) have been read
    bool             m_ErlangBinaryFormat; // true => the entire blob we return is in erlang's external term format; else it's in the traditional list of length-prefixed KV pairs

    // determines if the current element is in the bigset by checking against the
    // set tombstone; if the element is in, sets m_ElementReady to true and
    // prepares the K/V pair we send to the caller
    void FinalizeElement();

public:
    BigsetAccumulator( const Actor& ThisActor, leveldb::Logger* pLogger )
        : m_pLogger( pLogger ),
          m_ThisActor( ThisActor ), m_ElementReady( false ),
          m_ActorClockReady( false ), m_ActorClockSeen( false ),
          m_ActorSetTombstoneSeen( false ), m_FinishedReadingMetadata( false ),
          m_ErlangBinaryFormat( false ) // TODO: make this a parameter?
    { }

    ~BigsetAccumulator() { }

    // adds the current record to the accumulator
    //
    // NOTE: throws a std::runtime_error if an error occurs
    bool // true => record processed; false => encountered an element record but have not seen a clock for the specified actor
    AddRecord( Slice Key, Slice Value );

    // clears any data accumulated for the current record
    void
    Clear();

    // returns whether or not the last record added is ready to be sent to the caller
    bool
    RecordReady() const
    {
        return m_ElementReady || m_ActorClockReady;
    }

    bool
    UseErlangBinaryFormat() const
    {
        return m_ErlangBinaryFormat;
    }

    // sets the K/V pair we send back to the caller
    void
    GetCurrentElement( Slice& Key, Slice& Value );

    // returns whether or not we're currently processing an element
    bool
    ProcessingElement() const { return !m_CurrentElement.IsEmpty(); }

    // returns the final element currently being processed
    //
    // NOTE: throws a std::runtime_error if an error occurs
    bool // true => final element returned, else no element to return
    GetFinalElement( Slice& Key, Slice& Value );

    // returns whether or not we've read all available metadata for this bigset;
    // this means we've either read the bigset clock and set tombstone for the
    // desired actor, or if there is no set tombstone, then we've started reading elements
    bool
    FinishedReadingMetadata() const { return m_FinishedReadingMetadata; }
};

} // namespace bigset
} // namespace basho

#endif // BASHO_BIGSET_ACC_H
