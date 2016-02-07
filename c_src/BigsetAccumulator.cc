#include "BigsetAccumulator.h"
#include "BigsetKey.h"

namespace basho {
namespace bigset {

void
BigsetAccumulator::FinalizeElement()
{
    VersionVector remainingDots = m_CurrentContext.SubtractSeen( m_CurrentDots );
    if ( !remainingDots.IsEmpty() )
    {
        // this element is "in" the set locally
        m_ReadyKey    = m_CurrentElement; // TODO: transfer the buffer if possible
        //m_ReadyValue  = remainingDots.ToValue(); // TODO: format is term_to_binary() formatted list of 2-tuples {actor,integer}
        m_ElementReady = true;

        m_CurrentContext.Clear();
        m_CurrentDots.Clear();
    }
}

// adds the current record to the accumulator
//
// NOTE: throws a std::runtime_error if an error occurs
bool // true => record processed; false => encountered an element record but have not seen a clock for the specified actor
BigsetAccumulator::AddRecord( Slice key, Slice value )
{
    BigsetKey keyToAdd( key );
    if ( keyToAdd.IsValid() )
    {
        const Slice& setName( keyToAdd.GetSetName() );
        if ( m_CurrentSetName.IsEmpty() )
        {
            // this is the first record for this bigset, so save the set's name
            m_CurrentSetName.Assign( setName );
        }
        else if ( m_CurrentSetName != setName )
        {
            // this is unexpected; we didn't hit an "end" key for the bigset

            // TODO: log an error and handle unexpected set name change
            throw std::runtime_error( "Unexpected set name change" );
        }

        if ( keyToAdd.IsClock() )
        {
            // we have a clock key; see if it's for the actor we're tracking; if not, we ignore this clock
            if ( m_ThisActor == keyToAdd.GetActor() )
            {
                if ( m_ActorClockSeen )
                {
                    // we should only see one clock for a given actor

                    // TODO: log an error about the unexpected second instance of a clock for this actor
                    throw std::runtime_error( "Unexpected second clock found for actor" );
                }
                m_ActorClockReady = true;
                m_ActorClockSeen = true;
            }
        }
        else if ( keyToAdd.IsElement() )
        {
            // ensure we've seen the clock for the desired actor
            if ( !m_ActorClockSeen )
            {
                // TODO: log a message that we did not see a clock for the specified actor; the erlang code treats this condition as "not found"
                return false;
            }

            // we have an element key; see if it's for the current element we're processing
            const Slice& element( keyToAdd.GetElement() );
            if ( !m_CurrentElement.IsEmpty() && m_CurrentElement != element )
            {
                // we are starting a new element, so finish processing of the previous element
                FinalizeElement();
            }

            // accumulate values
            if ( m_CurrentElement.IsEmpty() )
            {
                m_CurrentElement.Assign( element );
            }

            BigsetClock currentClock;
            std::string error;
            if ( !BigsetClock::ValueToBigsetClock( value, currentClock, error ) )
            {
                // TODO: log an error about converting value to a bigset clock
                throw std::runtime_error( "Unable to convert binary to bigset clock" );
            }
            m_CurrentContext.Merge( currentClock );

            Actor actor;
            if ( !actor.SetId( keyToAdd.GetActor() ) )
            {
                // TODO: log an error about creating an Actor object
                throw std::runtime_error( "Unable to set actor ID" );
            }
            m_CurrentDots.AddPair( actor,
                                   keyToAdd.GetCounter(),
                                   keyToAdd.GetTombstone() );
        }
        else if ( keyToAdd.IsEnd() )
        {
            // this is an end key, so we're done enumerating the elements in this
            // bigset, and we need to finish processing the previous element
            FinalizeElement();
        }
        else
        {
            // oops, we weren't expecting this
            // TODO: log an error and handle unexpected key type
            throw std::runtime_error( "Unexpected key type" );
        }
    }
    else
    {
        // TODO: log an error and handle invalid key
        throw std::runtime_error( "Unable to parse key" );
    }
    return true;
}

} // namespace bigset
} // namespace basho
