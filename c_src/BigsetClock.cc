//
// Created by Paul A. Place on 1/14/16.
//

#include "BigsetClock.h"
#include <sstream>

namespace basho {
namespace bigset {

std::string Actor::ToString() const
{
    std::stringstream actorStr;
    actorStr << "<<";
    for ( int j = 0; j < sizeof m_ID; ++j )
    {
        if ( j > 0 )
        {
            actorStr << ',';
        }
        actorStr << (uint32_t)((uint8_t)m_ID[j]);
    }
    actorStr << ">>";
    return actorStr.str();
}

std::string
BigsetClock::ToString() const
{
    std::stringstream bigsetClockStr;
    bigsetClockStr << "{[";

    int j = 0;
    const std::list< basho::crdtUtils::Dot<Actor, uint32_t> >& vv( m_VersionVector.GetDots() );
    for ( auto vvIt = vv.begin(); vvIt != vv.end(); ++vvIt )
    {
        if ( j++ > 0 )
        {
            bigsetClockStr << ',';
        }
        bigsetClockStr << '{';
        bigsetClockStr << (*vvIt).first.ToString();
        bigsetClockStr << ',';
        bigsetClockStr << (*vvIt).second;
        bigsetClockStr << '}';
    }

    bigsetClockStr << "],[";

    j = 0;
    for ( auto dcIt = m_DotCloud.begin(); dcIt != m_DotCloud.end(); ++dcIt )
    {
        if ( j++ > 0 )
        {
            bigsetClockStr << ',';
        }
        bigsetClockStr << '{';
        bigsetClockStr << (*dcIt).GetActor().ToString();
        bigsetClockStr << ",[";

        int k = 0;
        const std::list<uint32_t>& events( (*dcIt).GetEvents() );
        for ( auto evIt = events.begin(); evIt != events.end(); ++evIt )
        {
            if ( k++ > 0 )
            {
                bigsetClockStr << ',';
            }
            bigsetClockStr << (*evIt);
        }

        bigsetClockStr << "]}";
    }
    bigsetClockStr << "]}";
    return bigsetClockStr.str();
}

// Bigset clocks are serialized in Erlang using term_to_binary().
//
// A bigset clock is a 2-tuple { version_vector, dot_cloud }, where
// version_vector is a list of 2-tuples of the form
//
//      [{Actor :: binary(8), Count :: pos_integer()}]
//
// and dot_cloud is a list of 2-tuples of the form
//
//      [{Actor :: binary(8), [pos_integer()]}]
//
// The list of positive integers for an actor in the dot_cloud represents the
// non-contiguous events observed for the actor.
//
bool // returns true if the binary value was successfully converted to a bigset clock, else returns false
BigsetClock::ValueToBigsetClock(
    const leveldb::Slice& Value,
    BigsetClock& Clock )
{
    // initialize the caller's output parameter
    Clock.Clear();

    // TODO: all the hard-coded stuff below needs to be replaced with calls into the Erlang ei library

    // the following hacked code is based on the Erlang External Term Format
    // specified at http://erlang.org/doc/apps/erts/erl_ext_dist.html

    // first check for the version number (131) at the beginning of the byte stream
    const char* pData = Value.data();
    size_t bytesLeft = Value.size();
    if ( bytesLeft < 1 || pData[0] != (char)131 )
    {
        // External Term Format magic number not found at beginning
        return false;
    }
    pData += 1;
    bytesLeft -= 1;

    // now verify that we have a 2-tuple
    if ( !IsTwoTuple( pData, bytesLeft ) )
    {
        return false;
    }

    // get the first element of the 2-tuple, a version vector
    if ( !ProcessListOfTwoTuples( pData, bytesLeft, Clock, true ) )
    {
        return false;
    }

    // get the second element of the 2-tuple, a dot cloud
    if ( !ProcessListOfTwoTuples( pData, bytesLeft, Clock, false ) )
    {
        return false;
    }
    return true;
}

bool
BigsetClock::ProcessListOfTwoTuples(
    const char*& pData,
    size_t& BytesLeft,
    BigsetClock& Clock,
    bool IsVersionVector ) // true => each 2-tuple in the list is a version vector, else it's a dot cloud
{
    // a version vector is a list of 2-tuples of the form
    //
    //      [{Actor :: binary(8), Count :: pos_integer()}]
    //
    // and a dot cloud is a list of 2-tuples of the form
    //
    //      [{Actor :: binary(8), [pos_integer()]}]
    //
    // so this method can handle both, since the only variation is in the final
    // element of each 2-tuple

    // first verify that we have a list (and get the count of elements)
    uint32_t elementCount;
    if ( !IsList( pData, BytesLeft, elementCount ) )
    {
        return false;
    }

    // if we have an empty list, there's nothing to do
    if ( 0 == elementCount )
    {
        return true;
    }

    // each element is expected to be a 2-tuple of the form:
    // {Actor :: binary(8), Element }, where Element is either a positive
    // integer (if IsVersionVector is true) or a list of positive integers
    // if (IsVersionVector is false)
    while ( elementCount-- > 0 )
    {
        // ensure we have a 2-tuple
        if ( !IsTwoTuple( pData, BytesLeft ) )
        {
            return false;
        }

        // the first element in the 2-tuple is the actor ID
        Actor actor;
        if ( !GetActor( pData, BytesLeft, actor ) )
        {
            return false;
        }

        // the second element in the 2-tuple is either a positive integer or
        // a list of positive integers
        if ( IsVersionVector )
        {
            uint32_t event;
            if ( !GetInteger( pData, BytesLeft, event ) )
            {
                return false;
            }

            // add this actor/event to the version vector
            Clock.AddToVersionVector( actor, event );
        }
        else
        {
            std::list<uint32_t> events;
            if ( !GetIntegerList( pData, BytesLeft, events ) )
            {
                return false;
            }

            // add this actor/event list to the dot cloud
            Clock.AddToDotCloud( actor, events );
        }
    }

    // the list contains a tail element, which should be an empty list
    if ( !IsList( pData, BytesLeft, elementCount ) || 0 != elementCount )
    {
        return false;
    }

    // if we get here, all went well
    return true;
}

bool
BigsetClock::IsList(
    const char*& pData,
    size_t& BytesLeft,
    uint32_t& ElementCount )
{
    // initialize the caller's output parameter
    ElementCount = 0;

    // first verify that we have a list; valid list types we expect
    // are 106 (empty list) and 108 (list of elements)
    if ( BytesLeft < 1 )
    {
        return false;
    }
    BytesLeft -= 1;

    char idByte = pData[0];
    pData += 1;

    if ( idByte == (char)106 )
    {
        // we have an empty list
        return true;
    }
    if ( idByte != (char)108 )
    {
        // we have something besides a list of elements
        return false;
    }

    // the next 4 bytes is the length of the list (i.e., the number of elements)
    uint32_t elementCount;
    if ( !GetBigEndianUint32( pData, BytesLeft, elementCount ) )
    {
        return false;
    }

    // ensure the count of elements looks reasonable; each element will be
    // at least one byte
    if ( 0 == elementCount || BytesLeft < elementCount )
    {
        return false;
    }

    // set the caller's output parameter to the number of elements in the list
    ElementCount = elementCount;
    return true;
}

bool
BigsetClock::IsTwoTuple(
    const char*& pData,
    size_t& BytesLeft )
{
    // a 2-tuple is the identifier byte 104 followed by a byte indicating
    // the arity of the tuple
    if ( BytesLeft < 2 || pData[0] != (char)104 || pData[1] != (char)2 )
    {
        return false;
    }
    pData += 2;
    BytesLeft -= 2;
    return true;
}

bool
BigsetClock::GetBigEndianUint16(
    const char*& pData,
    size_t& BytesLeft,
    uint16_t& Value )
{
    if ( BytesLeft < 2 )
    {
        return false;
    }
    BytesLeft -= 2;

    // TODO: deal with running on a big-endian system
    union
    {
        uint16_t m_UInt16;
        char m_Bytes[2];
    } value;

    value.m_Bytes[0] = pData[1];
    value.m_Bytes[1] = pData[0];
    pData += 2;

    Value = value.m_UInt16;
    return true;
}


bool
BigsetClock::GetBigEndianUint32(
    const char*& pData,
    size_t& BytesLeft,
    uint32_t& Value )
{
    if ( BytesLeft < 4 )
    {
        return false;
    }
    BytesLeft -= 4;

    // TODO: deal with running on a big-endian system
    union
    {
        uint32_t m_Uint32;
        char m_Bytes[4];
    } value;

    value.m_Bytes[0] = pData[3];
    value.m_Bytes[1] = pData[2];
    value.m_Bytes[2] = pData[1];
    value.m_Bytes[3] = pData[0];
    pData += 4;

    Value = value.m_Uint32;
    return true;
}

bool
BigsetClock::GetActor(
    const char*& pData,
    size_t& BytesLeft,
    Actor& Act )
{
    // an actor is an 8-byte binary value, which is the identifier byte 109
    // followed by the 4-byte length (big-endian), which should be the value 8,
    // followed by the 8 bytes of the actor ID

    // first check that we have enough bytes in the buffer and check the id byte
    if ( BytesLeft < 13 || pData[0] != (char)109 ) // 13 = 1 (ID) + 4 (len) + 8 (bytes)
    {
        return false;
    }
    pData += 1;
    BytesLeft -= 1;

    // get the length of the binary byte stream and ensure it's 8
    uint32_t length;
    if ( !GetBigEndianUint32( pData, BytesLeft, length ) || 8 != length )
    {
        return false;
    }

    // copy the 8 bytes of binary data to the Actor object
    if ( !Act.SetId( pData, 8 ) )
    {
        return false;
    }
    pData += 8;
    BytesLeft -= 8;
    return true;
}

bool
BigsetClock::GetInteger(
    const char*& pData,
    size_t& BytesLeft,
    uint32_t& Value )
{
    // an integer can be 1 byte (identifier byte 97) or 4 (identifier byte 98)
    if ( BytesLeft < 2 ) // 2 = 1 (ID) + 1 (byte)
    {
        return false;
    }

    char idByte = pData[0];
    pData += 1;
    BytesLeft -= 1;

    bool retVal = true;
    if ( 97 == idByte )
    {
        // the next byte in the buffer is an 8-bit unsigned integer
        Value = static_cast<uint32_t>( (uint8_t)pData[0] );
        pData += 1;
        BytesLeft -= 1;
    }
    else if ( 98 == idByte )
    {
        // the next 4-bytes in the buffer are a 32-bit (big-endian) integer
        retVal = GetBigEndianUint32( pData, BytesLeft, Value );
    }
    else
    {
        retVal = false;
    }
    return retVal;
}

bool
BigsetClock::GetIntegerList(
    const char*& pData,
    size_t& BytesLeft,
    std::list<uint32_t>& Values )
{
    // initialize the caller's output value
    Values.clear();

    // if all the integers in the list are less than 255, erlang's
    // term_to_binary() serializes the list as a string, which is the
    // identifier byte 107, followed by the 2-byte (big-endian) length,
    // followed by the specified number of bytes
    //
    // if any of the integers is larger than 255, I assume term_to_binary()
    // produces a real list (need an example to be sure)
    if ( BytesLeft < 3 ) // 3 = 1 (ID) + 2 (len)
    {
        return false;
    }

    char idByte = pData[0];
    pData += 1;
    BytesLeft -= 1;

    bool retVal = true;
    if ( 107 == idByte )
    {
        // the next 2-bytes in the buffer are a 16-bit (big-endian) integer,
        // which is the count of integers (bytes) in the list
        uint16_t len;
        retVal = GetBigEndianUint16( pData, BytesLeft, len );
        if ( retVal )
        {
            if ( BytesLeft < static_cast<size_t>( len ) )
            {
                retVal = false;
            }
            else
            {
                while ( len-- > 0 )
                {
                    Values.push_back( static_cast<uint32_t>( (uint8_t)pData[0] ) );
                    pData += 1;
                }
                BytesLeft -= static_cast<size_t>( len );
            }
        }
    }
    else
    {
        retVal = false;
    }
    return retVal;
}

} // namespace bigset
} // namespace basho
