#include "BigsetAccumulator.h"
#include "BigsetKey.h"

namespace basho {
namespace bigset {

void
BigsetAccumulator::FinalizeElement()
{
    m_SetTombstone.SubtractSeen( m_CurrentDots );
    if ( !m_CurrentDots.IsEmpty() )
    {
        // this element is "in" the set locally
        if ( !m_ReadyKey.Assign( m_CurrentElement, true ) ) // true => transfer ownership of the buffer from m_CurrentElement to m_ReadyKey
        {
            // TODO: log an error that buffer assignment failed
            throw std::runtime_error( "Failed to assign current element to ready key" );
        }
        if ( !m_CurrentDots.ToBinaryValue( m_ReadyValue, m_ErlangBinaryFormat ) )
        {
            // TODO: log an error that outputting the dot list failed
            throw std::runtime_error( "Failed to assign current dots to ready value" );
        }
        m_ElementReady = true;
        leveldb::Log( m_pLogger, "BigsetAccumulator::FinalizeElement: processed element key '%s' with %zu dot", m_ReadyKey.ToString().c_str(), m_CurrentDots.Size() );
    }
    m_CurrentElement.ResetBuffer();
    m_CurrentDots.Clear();
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
            if ( !m_CurrentSetName.Assign( setName ) )
            {
                // TODO: log an error that buffer assignment failed
                throw std::runtime_error( "Failed to assign current set name" );
            }
        }
        else if ( m_CurrentSetName != setName )
        {
            // this is unexpected; we didn't hit an "end" key for the bigset

            // TODO: log an error and handle unexpected set name change
            throw std::runtime_error( "Unexpected set name change" );
        }

        //leveldb::Log( m_pLogger, "BigsetAccumulator::AddRecord: processing key type '%c' for set '%s' with value size %zu", (char)keyToAdd.GetKeyType(), m_CurrentSetName.ToString().c_str(), value.size() );

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

                //utils::Buffer<100> valueBuff;
                //if ( valueBuff.Assign( value ) )
                //{
                //    leveldb::Log( m_pLogger, "BigsetAccumulator::AddRecord: clock key value: '%s'", valueBuff.ToString( utils::Buffer<100>::FormatAsBinaryDecimal ).c_str() );
                //}
                leveldb::Log( m_pLogger, "BigsetAccumulator::AddRecord: processed clock key for set '%s' with value size %zu", m_CurrentSetName.ToString().c_str(), value.size() );
            }
        }
        else if ( keyToAdd.IsSetTombstone() )
        {
            // we have a set tombstone key; see if it's for the actor we're tracking; if not, we ignore this set tombstone
            if ( m_ThisActor == keyToAdd.GetActor() )
            {
                if ( m_ActorSetTombstoneSeen )
                {
                    // we should only see one set tombstone for a given actor

                    // TODO: log an error about the unexpected second instance of a set tombstone for this actor
                    throw std::runtime_error( "Unexpected second set tombstone found for actor" );
                }

                std::string error;
                if ( !BigsetClock::ValueToBigsetClock( value, m_SetTombstone, error ) )
                {
                    std::string msg( "Error parsing set tombstone value: " );
                    msg += error;
                    throw std::runtime_error( msg );
                }

                m_ActorSetTombstoneSeen = true;
                leveldb::Log( m_pLogger,
                              "BigsetAccumulator::AddRecord: processed set tombstone key; version vector count=%zu, dot cloud count=%zu ",
                              m_SetTombstone.GetVersionVectorCount(), m_SetTombstone.GetDotCloudCount() );
            }
        }
        else if ( keyToAdd.IsElement() )
        {
            // ensure we've seen the clock for the desired actor
            if ( !m_ActorClockSeen )
            {
                // log a message that we did not see a clock for the specified actor;
                // the erlang code treats this condition as "not found"
                leveldb::Log( m_pLogger, "BigsetAccumulator::AddRecord(WARN): actor clock not seen before element record" );
                return false;
            }

            // we have an element key; see if it's for the current element we're processing
            const Slice& element( keyToAdd.GetElement() );
            if ( !m_CurrentElement.IsEmpty() && m_CurrentElement != element )
            {
                // we are starting a new element, so finish processing the previous element
                FinalizeElement();
            }

            // save the value of this element and accumulate its dots
            if ( m_CurrentElement.IsEmpty() )
            {
                if ( !m_CurrentElement.Assign( element ) )
                {
                    // TODO: log an error that buffer assignment failed
                    throw std::runtime_error( "Failed to assign current element" );
                }
            }

            Actor actor;
            if ( !actor.SetId( keyToAdd.GetActor() ) )
            {
                // TODO: log an error about creating an Actor object
                throw std::runtime_error( "Unable to set actor ID" );
            }
            m_CurrentDots.AddPair( actor,
                                   keyToAdd.GetCounter() );
        }
        else if ( keyToAdd.IsEnd() )
        {
            // this is an end key, so we're done enumerating the elements in this
            // bigset, and we need to finish processing the previous element
            FinalizeElement();
            leveldb::Log( m_pLogger, "BigsetAccumulator::AddRecord: processed end key" );
        }
        else
        {
            // oops, we weren't expecting this
            leveldb::Log( m_pLogger, "BigsetAccumulator::AddRecord(ERR): unexpected key type" );
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

void
BigsetAccumulator::GetCurrentElement( Slice& key, Slice& value )
{
    if ( m_ElementReady )
    {
        Slice readyKey( m_ReadyKey.GetCharBuffer(), m_ReadyKey.GetBytesUsed() );
        key = readyKey;

        Slice readyValue( m_ReadyValue.GetCharBuffer(), m_ReadyValue.GetBytesUsed() );
        value = readyValue;

        m_ElementReady = false; // prepare for the next element

        //leveldb::Log( m_pLogger, "BigsetAccumulator::GetCurrentElement: returning element; key size=%zu, value size=%zu", key.size(), value.size() );
    }
    else if ( m_ActorClockReady )
    {
        // we send the key/value back to the caller as-is
        m_ActorClockReady = false;

        //leveldb::Log( m_pLogger, "BigsetAccumulator::GetCurrentElement: returning clock; key size=%zu, value size=%zu", key.size(), value.size() );
    }
}

} // namespace bigset
} // namespace basho
