//
// Created by Paul A. Place on 12/16/15.
//

#ifndef BASHO_BIGSET_ACC_H
#define BASHO_BIGSET_ACC_H

#include <list>

#include "BigsetClock.h"
#include "util/buffer.h"

namespace basho {
namespace bigset {

class BigsetAccumulator
{
private:
    typedef utils::Buffer<32> Buffer;

    Actor         m_ThisActor;
    Buffer        m_CurrentSetName;
    Buffer        m_CurrentElement;
    BigsetClock   m_CurrentContext;
    VersionVector m_CurrentDots;

    Buffer        m_ReadyKey;
    Buffer        m_ReadyValue;
    bool          m_ElementReady;
    bool          m_ActorClockReady;
    bool          m_ActorClockSeen;

    void FinalizeElement();

public:
    BigsetAccumulator( const Actor& ThisActor ) : m_ThisActor( ThisActor ),
                                                  m_ElementReady( false ),
                                                  m_ActorClockReady( false ),
                                                  m_ActorClockSeen( false )
    { }

    ~BigsetAccumulator() { }

    // adds the current record to the accumulator
    //
    // NOTE: throws a std::runtime_error if an error occurs
    bool // true => record processed; false => encountered an element record but have not seen a clock for the specified actor
    AddRecord( Slice key, Slice value );

    bool
    RecordReady()
    {
        return m_ElementReady || m_ActorClockReady;
    }

    void
    GetCurrentElement( Slice& key, Slice& value )
    {
        if ( m_ElementReady )
        {
            Slice readyKey( m_ReadyKey.GetCharBuffer(), m_ReadyKey.GetBuffSize() );
            key = readyKey;

            Slice readyValue( m_ReadyValue.GetCharBuffer(), m_ReadyValue.GetBuffSize() );
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
