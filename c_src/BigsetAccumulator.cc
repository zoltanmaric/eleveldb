#include "BigsetAccumulator.h"
#include "BigsetKey.h"

namespace basho {
namespace bigset {

void BigsetAccumulator::FinalizeElement()
{
    Dots remainingDots = m_CurrentContext.SubtractSeen( m_CurrentDots );
    if ( !remainingDots.IsEmpty() )
    {
        // this element is "in" the set locally
        m_ReadyKey    = m_CurrentElement; // TODO: transfer the buffer if possible
        //m_ReadyValue  = remainingDots.ToValue(); // TODO: format is term_to_binary() formatted list of 2-tuples {actor,integer}
        m_RecordReady = true;

        m_CurrentContext.Clear();
        m_CurrentDots.Clear();
    }
}

void basho::bigset::BigsetAccumulator::AddRecord( Slice key, Slice value )
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
            // TODO: handle unexpected set name change
        }

        if ( keyToAdd.IsClock() )
        {
            // we have a clock key; see if it's for the actor we're tracking; if not, we ignore this clock
            if ( m_ThisActor == keyToAdd.GetActor() )
            {
                // get the clock value for this actor

                // TODO: return the key/value for this actor as-is as a record in the fold
            }
        }
        else if ( keyToAdd.IsElement() )
        {
            // we have an element key; see if it's for the current element we're processing
            if ( !m_CurrentElement.IsEmpty() && m_CurrentElement != keyToAdd.GetElement() )
            {
                // we are starting a new element, so finish processing of the previous element
                FinalizeElement();
            }

            // accumulate values
            m_CurrentElement.Assign( keyToAdd.GetElement() );

            BigsetClock currentClock;
            std::string error;
            if ( !BigsetClock::ValueToBigsetClock( value, currentClock, error ) )
            {
                // TODO: handle error converting value to a bigset clock
            }
            m_CurrentContext.Merge( currentClock );

            Actor actor;
            if ( !actor.SetId( keyToAdd.GetActor() ) )
            {
                // TODO: handle error creating an Actor object
            }
            m_CurrentDots.AddDot( actor,
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
            // TODO: handle unexpected element type
        }
    }
    else
    {
        // TODO: handle invalid element
    }
}

} // namespace bigset
} // namespace basho
