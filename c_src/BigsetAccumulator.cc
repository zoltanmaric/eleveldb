#include "BigsetAccumulator.h"

namespace basho {
namespace bigset {

void BigsetAccumulator::FinalizeElement()
{
    Dots remainingDots = m_CurrentContext.SubtractSeen( m_CurrentDots );
    if ( !remainingDots.IsEmpty() )
    {
        // this element is "in" the set locally
        m_ReadyKey    = m_CurrentElement;
        //m_ReadyValue  = remainingDots.ToValue();
        m_RecordReady = true;

        m_CurrentContext.Clear();
        m_CurrentDots.Clear();
    }
}

void basho::bigset::BigsetAccumulator::AddRecord( ErlTerm key, ErlTerm value )
{
    BigsetKey keyToAdd( key );
    if ( keyToAdd.IsValid() )
    {
        if ( m_CurrentSetName.empty() )
        {
            // this is the first record for this bigset, so save the set's name
            m_CurrentSetName = keyToAdd.GetSetName();
        }
        else if ( m_CurrentSetName != keyToAdd.GetSetName() )
        {
            // this is unexpected; we didn't hit an "end" key for the bigset
            // TODO: handle unexpected set name change
        }

        if ( keyToAdd.IsClock() )
        {
            // we have a clock key; see if it's for the actor we're tracking; if not, we ignore this clock
            if ( 0 == m_ThisActor.size() )
            {
                // this is the first clock key we've seen for this bigset, so save its associated actor
                m_ThisActor = keyToAdd.GetActor();
            }
            else if ( keyToAdd.GetActor() == m_ThisActor )
            {
                // get the clock value for this actor
            }
            else
            {
                // ignore this actor; not the one we care about
            }
        }
        else if ( keyToAdd.IsElement() )
        {
            // we have an element key; see if it's for the current element we're processing
            if ( keyToAdd.GetElement() != m_CurrentElement )
            {
                // we are starting a new element, so finish processing of the previous element
                FinalizeElement();
            }

            // accumulate values
            m_CurrentElement = keyToAdd.GetElement();

            BigsetClock currentClock;
            std::string error;
            if ( !BigsetClock::ValueToBigsetClock( value, currentClock, error ) )
            {
                // TODO: handle error converting value to a bigset clock
            }
            m_CurrentContext.Merge( currentClock );
            //m_CurrentDots.AddDot( keyToAdd.GetActor(),
            //                      keyToAdd.GetCounter(),
            //                      keyToAdd.GetTombstone() );
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
