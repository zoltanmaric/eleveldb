// -------------------------------------------------------------------
//
// BigsetComparator.h: Declaration of the BigsetComparator class, which
// implements a custom leveldb comparator for bigsets.
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

#ifndef BASHO_BIGSET_COMPARATOR_H
#define BASHO_BIGSET_COMPARATOR_H

#include <leveldb/comparator.h>
#include <leveldb/slice.h>

namespace basho {
namespace bigset {

// Bigsets key comparator (copied and pasted from Engel's
// https://github.com/basho/leveldb/blob/prototype/timeseries/util/comparator.cc#L69
//
// @TODO add delegation to default comparator. Add a first byte(s?) that identifies bigsets
// Clock keys are
//  <<
//    SetNameLen:32/little-unsigned-integer, %% the lengthof the set name
//    SetName:SetNameLen/binary, %% Set name for bytewise comparison
//    $c, %% means clock
//    Actor/binary, %% The actual actor
//    >>
// Handoff Filter keys are
//  <<
//    SetNameLen:32/little-unsigned-integer, %% the lengthof the set name
//    SetName:SetNameLen/binary, %% Set name for bytewise comparison
//    $d, %% means handoff filter
//    Actor/binary, %% The actual actor
//    >>
// Element keys are
//  <<
//    SetNameLen:32/little-unsigned-integer, %% the length of the set name
//    SetName:SetNameLen/binary, %% Set name for bytewise comparison
//    $e:1/binary, %% indicates an element
//    ElementLen:32/little-unsigned-integer, %% Length of the element
//    Element:ElementLen/binary, %% The actual element
//    ActorLen:32/little-unsigned-integer, %% Length of the actor ID
//    Actor:ActorLen/binary, %% The actual actor
//    Counter:64/little-unsigned-integer
//    >>
//  End Key is:
//  <<
//    SetNameLen:32/little-unsigned-integer, %% the lengthof the set name
//    SetName:SetNameLen/binary, %% Set name for bytewise comparison
//    $z, %% means end key, used for limiting streaming fold
//    >>

class BigsetComparator : public leveldb::Comparator
{
    enum eSigValue
    {
        SigValInvalid = 0x49f32bea,
        SigValValid   = 0x943fb2ae
    };
    eSigValue m_Signature;

protected:
    // helper methods
    static bool IsClock( const leveldb::Slice& s );
    static bool IsHoff( const leveldb::Slice& s );
    static bool IsElement( const leveldb::Slice& s );
    static bool IsEnd( const leveldb::Slice& s );

  public:
    BigsetComparator() : m_Signature( SigValValid ) {}
    virtual ~BigsetComparator() { m_Signature = SigValInvalid; }

    bool IsValid() const { return SigValValid == m_Signature; }

    // overload of the Compare() method that optionally compares only up to the Element name
    int Compare( const leveldb::Slice& a, const leveldb::Slice& b, bool IgnoreActorCountForElement ) const;

    // returns the singleton BigsetComparator object
    static const BigsetComparator* GetComparator();

  /////////////////////////////////////
  // Public interface inherited from leveldb::Comparator
  /////////////////////////////////////
  public:
    // Three-way comparison.  Returns value:
    //   < 0 iff "a" < "b",
    //   == 0 iff "a" == "b",
    //   > 0 iff "a" > "b"
    virtual int Compare( const leveldb::Slice& a, const leveldb::Slice& b ) const
    {
        return Compare( a, b, false ); // false => compare Actor and Count if elements match up to that point
    }

    // The name of the comparator.  Used to check for comparator
    // mismatches (i.e., a DB created with one comparator is
    // accessed using a different comparator.
    //
    // The client of this package should switch to a new name whenever
    // the comparator implementation changes in a way that will cause
    // the relative ordering of any two keys to change.
    //
    // Names starting with "leveldb." are reserved and should not be used
    // by any clients of this package.
    virtual const char* Name() const;

    // Advanced functions: these are used to reduce the space requirements
    // for internal data structures like index blocks.

    // If *start < limit, changes *start to a short string in [start,limit).
    // Simple comparator implementations may return with *start unchanged,
    // i.e., an implementation of this method that does nothing is correct.
    virtual void FindShortestSeparator(
        std::string* /*start*/,
        const leveldb::Slice& /*limit*/ ) const
    {
        // no need to shorten a key since it is fixed size
    }

    // Changes *key to a short string >= *key.
    // Simple comparator implementations may return with *key unchanged,
    // i.e., an implementation of this method that does nothing is correct.
    virtual void FindShortSuccessor( std::string* /*key*/ ) const
    {
        // no need to shorten a key since it is fixed size
    }
};

} // namespace bigset
} // namespace basho

#endif //BASHO_BIGSET_COMPARATOR_H
