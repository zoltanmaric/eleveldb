//
// Created by Paul A. Place on 12/16/15.
//

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
