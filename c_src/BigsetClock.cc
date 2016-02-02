//
// Created by Paul A. Place on 1/14/16.
//

#include "BigsetClock.h"
#include <sstream>

namespace basho {
namespace bigset {

/////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////
//
// Actor class
//
/////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////

int Actor::Compare( const char* pData, size_t SizeInBytes ) const
{
    if ( SizeInBytes < sizeof m_ID )
    {
        throw std::invalid_argument( "SizeInBytes value is too small to be an Actor ID" );
    }

    int retVal = ::memcmp( m_ID, pData, sizeof m_ID );
    if ( retVal < 0 )
    {
        // memcmp() only promises < 0, but we want -1
        retVal = -1;
    }
    else if ( retVal > 0 )
    {
        retVal = 1;
    }
    return retVal;
}

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
        actorStr << static_cast<uint32_t>( static_cast<uint8_t>( m_ID[j] ) );
    }
    actorStr << ">>";
    return actorStr.str();
}

/////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////
//
// BigsetClock class
//
/////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////

std::string
BigsetClock::ToString() const
{
    std::stringstream bigsetClockStr;
    bigsetClockStr << "{[";

    int j = 0;
    auto vv( m_VersionVector.GetDots() );
    for ( auto vvIt = vv.begin(); vvIt != vv.end(); ++vvIt )
    {
        if ( j++ > 0 )
        {
            bigsetClockStr << ',';
        }
        bigsetClockStr << '{';
        bigsetClockStr << (*vvIt).GetActor().ToString();
        bigsetClockStr << ',';
        bigsetClockStr << (*vvIt).GetCounter();
        bigsetClockStr << '}';
    }

    bigsetClockStr << "],[";

    j = 0;
    auto dotCloud( m_DotCloud.GetDots() );
    for ( auto dcIt = dotCloud.begin(); dcIt != dotCloud.end(); ++dcIt )
    {
        if ( j++ > 0 )
        {
            bigsetClockStr << ',';
        }
        bigsetClockStr << '{';
        bigsetClockStr << (*dcIt).GetActor().ToString();
        bigsetClockStr << ",[";

        int k = 0;
        const CounterList& events( (*dcIt).GetCounter() );
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

// Parses a serialized bigset clock (serialization format is erlang's
// external term format, http://erlang.org/doc/apps/erts/erl_ext_dist.html),
// creating a BigsetClock object.
//
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
bool // returns true if the binary value was successfully converted to a BigsetClock, else returns false
BigsetClock::ValueToBigsetClock(
    const Slice&  Value,   // IN:  serialized bigset clock
    BigsetClock&  Clock,   // OUT: receives the parsed bigset clock
    std::string&  Error )  // OUT: if false returned, contains a description of the error
{
    // initialize the caller's output parameters
    Clock.Clear();
    Error.clear();

    // TODO: all the hard-coded stuff below (probably) needs to be replaced with calls into the Erlang ei library

    // the following hacked code is based on the Erlang External Term Format
    // specified at http://erlang.org/doc/apps/erts/erl_ext_dist.html

    // first check for the version number (131) at the beginning of the byte stream
    Slice data( Value );
    if ( data.size() < 1 || data[0] != (char)131 )
    {
        // External Term Format magic number not found at beginning
        Error = "magic number not found";
        return false;
    }
    data.remove_prefix( 1 );

    // now verify that we have a 2-tuple
    if ( !IsTwoTuple( data ) )
    {
        Error = "value is not a 2-tuple";
        return false;
    }

    // get the first element of the 2-tuple, a version vector
    if ( !ProcessListOfTwoTuples( data, Clock, true, Error ) )
    {
        if ( Error.empty() ) Error = "unable to parse version vector";
        return false;
    }

    // get the second element of the 2-tuple, a dot cloud
    if ( !ProcessListOfTwoTuples( data, Clock, false, Error ) )
    {
        if ( Error.empty() ) Error = "unable to parse dot cloud";
        return false;
    }
    return true;
}

bool
BigsetClock::ProcessListOfTwoTuples(
    Slice& Data,
    BigsetClock& Clock,
    bool IsVersionVector, // true => each 2-tuple in the list is a version vector, else it's a dot cloud
    std::string& Error )
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
    if ( !IsList( Data, elementCount ) )
    {
        ProcessListOfTwoTuplesErrorMessage( Error, IsVersionVector, "is not a list" );
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
    for ( uint32_t j = 1; j <= elementCount; ++j )
    {
        // ensure we have a 2-tuple
        if ( !IsTwoTuple( Data ) )
        {
            ProcessListOfTwoTuplesErrorMessage( Error, IsVersionVector, "is not a 2-tuple", j );
            return false;
        }

        // the first element in the 2-tuple is the actor ID
        Actor actor;
        if ( !GetActor( Data, actor ) )
        {
            ProcessListOfTwoTuplesErrorMessage( Error, IsVersionVector, "error getting actor", j );
            return false;
        }

        // the second element in the 2-tuple is either a positive integer or
        // a list of positive integers
        std::string getEventError;
        if ( IsVersionVector )
        {
            Counter event;
            if ( !GetInteger( Data, event, getEventError ) )
            {
                std::string specificError( "error getting event" );
                if ( !getEventError.empty() )
                {
                    specificError.append( " (" );
                    specificError.append( getEventError );
                    specificError.append( ")" );
                }
                ProcessListOfTwoTuplesErrorMessage( Error, IsVersionVector, specificError.c_str(), j );
                return false;
            }

            // add this actor/event to the version vector
            if ( !Clock.AddToVersionVector( actor, event ) )
            {
                ProcessListOfTwoTuplesErrorMessage( Error, IsVersionVector, "error adding actor/event", j );
                return false;
            }
        }
        else
        {
            CounterList events;
            if ( !GetIntegerList( Data, events, getEventError ) )
            {
                std::string specificError( "error getting event list" );
                if ( !getEventError.empty() )
                {
                    specificError.append( " (" );
                    specificError.append( getEventError );
                    specificError.append( ")" );
                }
                ProcessListOfTwoTuplesErrorMessage( Error, IsVersionVector, specificError.c_str(), j );
                return false;
            }

            // add this actor/event list to the dot cloud
            if ( !Clock.AddToDotCloud( actor, events ) )
            {
                ProcessListOfTwoTuplesErrorMessage( Error, IsVersionVector, "error adding actor/events", j );
                return false;
            }
        }
    }

    // the list contains a tail element, which should be an empty list
    if ( !IsList( Data, elementCount ) || 0 != elementCount )
    {
        ProcessListOfTwoTuplesErrorMessage( Error, IsVersionVector, "tail element is not an empty list" );
        return false;
    }

    // if we get here, all went well
    return true;
}

void
BigsetClock::ProcessListOfTwoTuplesErrorMessage(
    std::string& Error,
    bool         IsVersionVector,
    const char*  Message,
    uint32_t     ItemNumber )
{
    static const std::string c_VVStr( "version vector " );
    static const std::string c_DCStr( "dot cloud " );

    Error.reserve( 100 );
    Error = (IsVersionVector ? c_VVStr : c_DCStr);
    if ( ItemNumber != (uint32_t)-1 )
    {
        char buff[ 100 ];
        int charsWritten = ::snprintf( buff, sizeof buff, "(item %u) ", ItemNumber );
        if ( charsWritten > 0 && charsWritten < sizeof buff )
        {
            Error.append( buff );
        }
    }
    if ( NULL != Message )
    {
        Error.append( Message );
    }
}

bool
BigsetClock::IsList( Slice& Data, uint32_t& ElementCount )
{
    // initialize the caller's output parameter
    ElementCount = 0;

    // first verify that we have a list; valid list types we expect
    // are 106 (empty list) and 108 (list of elements)
    if ( Data.size() < 1 )
    {
        return false;
    }

    char idByte = Data[0];
    Data.remove_prefix( 1 );

    if ( (char)106 == idByte )
    {
        // we have an empty list
        return true;
    }
    if ( (char)108 != idByte )
    {
        // we have something besides a list of elements
        return false;
    }

    // the next 4 bytes is the length of the list (i.e., the number of elements)
    uint32_t elementCount;
    if ( !GetBigEndianUint32( Data, elementCount ) )
    {
        return false;
    }

    // ensure the count of elements looks reasonable; each element will be
    // at least one byte
    if ( 0 == elementCount || Data.size() < elementCount )
    {
        return false;
    }

    // set the caller's output parameter to the number of elements in the list
    ElementCount = elementCount;
    return true;
}

bool
BigsetClock::IsTwoTuple( Slice& Data )
{
    // a 2-tuple is the identifier byte 104 followed by a byte indicating
    // the arity of the tuple
    if ( Data.size() < 2 || Data[0] != (char)104 || Data[1] != (char)2 )
    {
        return false;
    }
    Data.remove_prefix( 2 );
    return true;
}

bool
BigsetClock::GetBigEndianUint16( Slice& Data, uint16_t& Value )
{
    if ( Data.size() < 2 )
    {
        return false;
    }

    // TODO: deal with running on a big-endian system
    union
    {
        uint16_t m_UInt16;
        char     m_Bytes[2];
    } value;

    value.m_Bytes[0] = Data[1];
    value.m_Bytes[1] = Data[0];
    Data.remove_prefix( 2 );

    Value = value.m_UInt16;
    return true;
}

bool
BigsetClock::GetBigEndianUint32( Slice& Data, uint32_t& Value )
{
    if ( Data.size() < 4 )
    {
        return false;
    }

    // TODO: deal with running on a big-endian system
    union
    {
        uint32_t m_Uint32;
        char     m_Bytes[4];
    } value;

    value.m_Bytes[0] = Data[3];
    value.m_Bytes[1] = Data[2];
    value.m_Bytes[2] = Data[1];
    value.m_Bytes[3] = Data[0];
    Data.remove_prefix( 4 );

    Value = value.m_Uint32;
    return true;
}

bool
BigsetClock::GetBignum( Slice& Data, Counter& Value )
{
    // a "small" bignum is formatted as a record ID byte (110) followed by a
    // byte containing the number of digits, followed by a sign byte, followed
    // by the digits with the LSB byte stored first
    //
    // this method is called with Data pointing at the "number of bytes" byte
    if ( Data.size() < 2 ) // 2 = 1 (digit count) + 1 (sign)
    {
        return false;
    }

    // get the "number of bytes" byte and the sign byte
    uint8_t numDigits = static_cast<uint8_t>( Data[0] );
    char sign = Data[1];
    Data.remove_prefix( 2 );

    // if numDigits is greater than sizeof Counter, or if the sign is non-0
    // (indicating a negative number), then we cannot handle this number
    const int c_CounterSize = sizeof( Counter );
    if ( numDigits > c_CounterSize || sign != 0 )
    {
        return false;
    }

    union
    {
        Counter m_Counter;
        char    m_Bytes[ c_CounterSize ];
    } value;

    // TODO: deal with running on a big-endian system
    value.m_Counter = 0;
    for ( uint8_t j = 0; j < numDigits; ++j )
    {
        value.m_Bytes[j] = Data[j];
    }
    Data.remove_prefix( numDigits );

    Value = value.m_Counter;
    return true;
}

bool
BigsetClock::GetActor( Slice& Data, Actor& Act )
{
    // an actor is an 8-byte binary value, which is the identifier byte 109
    // followed by the 4-byte length (big-endian), which should be the value 8,
    // followed by the 8 bytes of the actor ID

    // first check that we have enough bytes in the buffer and check the id byte
    if ( Data.size() < 13 || Data[0] != (char)109 ) // 13 = 1 (ID) + 4 (len) + 8 (bytes)
    {
        return false;
    }
    Data.remove_prefix( 1 );

    // get the length of the binary byte stream and ensure it's 8
    uint32_t length;
    if ( !GetBigEndianUint32( Data, length ) || 8 != length )
    {
        return false;
    }

    // copy the 8 bytes of binary data to the Actor object
    return Act.SetId( Data );
}

bool
BigsetClock::GetInteger(
    Slice&       Data,
    Counter&     Value,
    std::string& Error )
{
    // an integer can be 1 byte (identifier byte 97), 4 bytes (identifier byte 98),
    // or a variable number of bytes, AKA an erlang bignum (identifier byte 110)
    if ( Data.size() < 2 ) // 2 = 1 (ID) + 1 (byte) minimum
    {
        Error = "not enough bytes";
        return false;
    }

    char idByte = Data[0];
    Data.remove_prefix( 1 );

    bool retVal = true;
    if ( (char)97 == idByte )
    {
        // the next byte in the buffer is an 8-bit unsigned integer
        Value = static_cast<Counter>( static_cast<uint8_t>( Data[0] ) );
        Data.remove_prefix( 1 );
    }
    else if ( (char)98 == idByte )
    {
        // the next 4-bytes in the buffer are a 32-bit (big-endian) integer
        uint32_t value32;
        retVal = GetBigEndianUint32( Data, value32 );
        if ( retVal )
        {
            Value = value32;
        }
        else
        {
            Error = "error getting big-endian 32-bit integer";
        }
    }
    else if ( (char)110 == idByte )
    {
        // we have a "small" bignum
        retVal = GetBignum( Data, Value );
        if ( !retVal ) Error = "error getting bignum as 32-bit integer";
    }
    else
    {
        FormatUnrecognizedRecordTypeError( Error, idByte );
        retVal = false;
    }
    return retVal;
}

bool
BigsetClock::GetIntegerList(
    Slice&       Data,
    CounterList& Values,
    std::string& Error )
{
    // initialize the caller's output value
    Values.clear();

    // if all the integers in the list are less than 255, erlang's
    // term_to_binary() serializes the list as a string, which is the
    // identifier byte 107, followed by the 2-byte (big-endian) length,
    // followed by the specified number of bytes
    //
    // if any of the integers is larger than 255, then term_to_binary()
    // produces a real list
    //
    // note that we might also have an empty list
    if ( Data.size() < 1 ) // 1 (ID)
    {
        Error = "not enough bytes";
        return false;
    }

    // save a copy of the Data Slice in case we need to restore it to process a list
    Slice dataSave( Data );

    char idByte = Data[0];
    Data.remove_prefix( 1 );

    bool retVal;
    if ( (char)106 == idByte )
    {
        // we have an empty list
        retVal = true;
    }
    else if ( (char)107 == idByte )
    {
        // the next 2-bytes in the buffer are a 16-bit (big-endian) integer,
        // which is the count of integers (bytes) in the list
        uint16_t len;
        retVal = GetBigEndianUint16( Data, len );
        if ( retVal )
        {
            if ( Data.size() < static_cast<size_t>( len ) )
            {
                Error = "not enough bytes for complete list";
                retVal = false;
            }
            else
            {
                while ( len-- > 0 )
                {
                    Values.push_back( static_cast<uint8_t>( Data[0] ) );
                    Data.remove_prefix( 1 );
                }
            }
        }
        else
        {
            Error = "error getting list length as big-endian 16-bit integer";
        }
    }
    else if ( (char)108 == idByte)
    {
        // restore the state of the Data Slice prior to calling IsList()
        Data = dataSave;

        uint32_t elementCount;
        retVal = IsList( Data, elementCount );
        if ( retVal )
        {
            while ( retVal && elementCount-- > 0 )
            {
                if ( Data.size() < 2 ) // 2 = 1 (ID) + 1 (byte) minimum
                {
                    Error = "not enough bytes for next integer in the list";
                    retVal = false;
                }
                else
                {
                    Counter value;
                    retVal = GetInteger( Data, value, Error );
                    if ( retVal )
                    {
                        Values.push_back( value );
                    }
                }
            }

            // the list contains a tail element, which should be an empty list
            if ( retVal && (!IsList( Data, elementCount ) || 0 != elementCount) )
            {
                Error = "tail element is not an empty list";
                retVal = false;
            }
        }
        else
        {
            Error = "error getting list length";
        }
    }
    else
    {
        FormatUnrecognizedRecordTypeError( Error, idByte );
        retVal = false;
    }
    return retVal;
}

void
BigsetClock::FormatUnrecognizedRecordTypeError(
    std::string& Error,
    char RecordId )
{
    Error = "unrecognized record type";

    char buff[ 32 ];
    int charsWritten = ::snprintf( buff, sizeof buff, " %d", static_cast<int>( (uint8_t)RecordId ) );
    if ( charsWritten > 0 && charsWritten < sizeof buff )
    {
        Error.append( buff );
    }
}

bool // true => <Act,Event> added to the version vector, else not (likely because Act is already present)
BigsetClock::AddToVersionVector( const Actor& Act, Counter Event )
{
    // check if the caller's Actor is already in m_VersionVector; if so, do not re-add
    if ( !m_AllowDuplicateActors && m_VersionVector.ContainsActor( Act ) )
    {
        return false;
    }
    m_VersionVector.AddDot( Act, Event );
    return true;
}

bool // true => <Act, Evetns> added tot he dot cloud, else not (likely because Act is already present)
BigsetClock::AddToDotCloud( const Actor& Act, const CounterList& Events )
{
    // check if the caller's Actor is already in m_VersionVector; if so, do not re-add
    if ( !m_AllowDuplicateActors && m_DotCloud.ContainsActor( Act ) )
    {
        return false;
    }
    m_DotCloud.AddDot( Act, Events );
    return true;
}
} // namespace bigset
} // namespace basho
