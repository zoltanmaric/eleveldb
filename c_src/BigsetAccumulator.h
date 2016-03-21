//
// Created by Paul A. Place on 12/16/15.
//

#ifndef BASHO_BIGSET_ACC_H
#define BASHO_BIGSET_ACC_H

#include <list>

#include "BigsetClock.h"

namespace basho {
namespace bigset {

class BigsetAccumulator
{
private:
    Actor       m_ThisActor;
    Buffer      m_CurrentSetName;
    Buffer      m_CurrentElement;
    BigsetClock m_CurrentContext;
    DotList     m_CurrentDots;

    Buffer      m_ReadyKey;
    Buffer      m_ReadyValue;
    bool        m_ElementReady;
    bool        m_ActorClockReady;
    bool        m_ActorClockSeen;
    bool        m_ErlangBinaryFormat; // true => the entire blob we return is in erlang's external term format; else it's in the traditional list of length-prefixed KV pairs

    void FinalizeElement();

public:
    BigsetAccumulator( const Actor& ThisActor ) : m_ThisActor( ThisActor ),
                                                  m_ElementReady( false ),
                                                  m_ActorClockReady( false ),
                                                  m_ActorClockSeen( false ),
                                                  m_ErlangBinaryFormat( true ) // TODO: make this a parameter?
    { }

    ~BigsetAccumulator() { }

    // adds the current record to the accumulator
    //
    // NOTE: throws a std::runtime_error if an error occurs
    bool // true => record processed; false => encountered an element record but have not seen a clock for the specified actor
    AddRecord( Slice key, Slice value );

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

    void
    GetCurrentElement( Slice& key, Slice& value )
    {
        if ( m_ElementReady )
        {
            Slice readyKey( m_ReadyKey.GetCharBuffer(), m_ReadyKey.GetBytesUsed() );
            key = readyKey;

            Slice readyValue( m_ReadyValue.GetCharBuffer(), m_ReadyValue.GetBytesUsed() );
            value = readyValue;

            m_ElementReady = false; // prepare for the next element
        }
        else if ( m_ActorClockReady )
        {
            // we send the key/value back to the caller as-is
            m_ActorClockReady = false;
        }
    }
};

} // namespace bigset
} // namespace basho

#endif // BASHO_BIGSET_ACC_H
