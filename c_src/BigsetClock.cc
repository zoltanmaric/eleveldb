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
// VersionVector class
//
/////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////

bool VersionVector::AddPair( const Actor& Act, Counter Event, bool IsTombstone )
{
    bool retVal = false;
    if ( !IsTombstone )
    {
        auto result = m_Pairs.insert( std::pair<Actor, Counter>( Act, Event ) );
        retVal = result.second;
        if ( !retVal )
        {
            // the Actor is already in the map, so see if the incoming Event
            // value is larger than the one in the map
            if ( (*result.first).second < Event )
            {
                (*result.first).second = Event;
                retVal = true;
            }
        }
    }
    return retVal;
}

bool VersionVector::Merge( const VersionVector& That )
{
    if ( this != &That )
    {
        for ( auto vvIt = That.m_Pairs.begin(); vvIt != That.m_Pairs.end(); ++vvIt )
        {
            AddPair( (*vvIt).first, (*vvIt).second );
        }
    }
    return true;
}

bool VersionVector::ContainsActor( const Actor& Act, Counter* pEvent ) const
{
    auto pairIt = m_Pairs.find( Act );
    bool actorPresent = (pairIt != m_Pairs.end());
    if ( actorPresent && pEvent != NULL )
    {
        *pEvent = (*pairIt).second;
    }
    return actorPresent;
}

int VersionVector::Compare( const VersionVector& That ) const
{
    // if comparing to self, then definitely equal
    if ( this == &That )
    {
        return 0;
    }

    // see if That has a different number of entries than this object
    const size_t thisSize = Size();
    const size_t thatSize = That.Size();
    if ( thisSize < thatSize )
    {
        return -1;
    }
    if ( thisSize > thatSize )
    {
        return 1;
    }

    // this object has the same number of entries as That, so we need to compare entry-by-entry
    int retVal = 0;
    auto thisIt = m_Pairs.begin();
    auto thatIt = That.m_Pairs.begin();
    for ( size_t j = 0; 0 == retVal && j < thisSize; ++j, ++thisIt, ++thatIt )
    {
        // compare the Actor objects
        retVal = (*thisIt).first.Compare( (*thatIt).first );
        if ( 0 == retVal )
        {
            // the Actor objects are the same, so compare the event Counter values
            Counter thisCounter = (*thisIt).second;
            Counter thatCounter = (*thatIt).second;
            if ( thisCounter < thatCounter )
            {
                retVal = -1;
            }
            else if ( thisCounter > thatCounter )
            {
                retVal = 1;
            }
        }
    }
    return retVal;
}

std::string VersionVector::ToString() const
{
    std::stringstream versionVectorStr;
    int j = 0;
    for ( auto pairIt = m_Pairs.begin(); pairIt != m_Pairs.end(); ++pairIt )
    {
        if ( j++ > 0 )
        {
            versionVectorStr << ',';
        }
        versionVectorStr << '{';
        versionVectorStr << (*pairIt).first.ToString();
        versionVectorStr << ',';
        versionVectorStr << (*pairIt).second;
        versionVectorStr << '}';
    }
    return versionVectorStr.str();
}

/////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////
//
// DotCloud class
//
/////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////

bool DotCloud::AddDot( const Actor& Act, Counter Event )
{
    CounterSet eventsIn;
    eventsIn.insert( Event );

    auto result = m_Dots.insert( std::pair<Actor, CounterSet>( Act, eventsIn ) );
    bool retVal = result.second;
    if ( !retVal )
    {
        // the Actor is already in the map, so see if the incoming Event
        // value already in the Actor's CounterSet
        CounterSet& events( (*result.first).second );
        auto result2 = events.insert( Event );
        retVal = result2.second;
    }
    return retVal;
}

bool DotCloud::AddDots( const Actor& Act, const CounterSet& Events )
{
    auto result = m_Dots.insert( std::pair<Actor, CounterSet>( Act, Events ) );
    bool retVal = result.second;
    if ( !retVal )
    {
        // the Actor is already in the map, so see if we can add any of the
        // events from the caller's CounterSet
        for ( auto eventIt = Events.begin(); eventIt != Events.end(); ++eventIt )
        {
            bool eventRetVal = AddDot( Act, *eventIt );
            if ( eventRetVal )
            {
                retVal = true;
            }
        }
    }
    return retVal;
}

bool DotCloud::Merge( const DotCloud& That )
{
    if ( this != &That )
    {
        for ( auto dcIt = That.m_Dots.begin(); dcIt != That.m_Dots.end(); ++dcIt )
        {
            AddDots( (*dcIt).first, (*dcIt).second );
        }
    }
    return true;
}

int DotCloud::Compare( const DotCloud& That ) const
{
    // if comparing to self, then definitely equal
    if ( this == &That )
    {
        return 0;
    }

    // see if That has a different number of entries than this object
    const size_t thisSize = Size();
    const size_t thatSize = That.Size();
    if ( thisSize < thatSize )
    {
        return -1;
    }
    if ( thisSize > thatSize )
    {
        return 1;
    }

    // this object has the same number of entries as That, so we need to compare entry-by-entry
    int retVal = 0;
    auto thisIt = m_Dots.begin();
    auto thatIt = That.m_Dots.begin();
    for ( size_t j = 0; 0 == retVal && j < thisSize; ++j, ++thisIt, ++thatIt )
    {
        // compare the Actor objects
        retVal = (*thisIt).first.Compare( (*thatIt).first );
        if ( 0 == retVal )
        {
            // the Actor objects are the same, so compare the CounterSet objects
            size_t thisDotCount = (*thisIt).second.size();
            size_t thatDotCount = (*thatIt).second.size();
            if ( thisDotCount < thatDotCount )
            {
                retVal = -1;
            }
            else if ( thisDotCount > thatDotCount )
            {
                retVal = 1;
            }
            else
            {
                // the CounterSet objects contain the same number of events, so
                // compare the individual Counter values
                const CounterSet& thisCounterSet( (*thisIt).second );
                const CounterSet& thatCounterSet( (*thatIt).second );
                auto thisCounterIt = thisCounterSet.begin();
                auto thatCounterIt = thatCounterSet.begin();
                for ( size_t k = 0; 0 == retVal && k < thisDotCount; ++k, ++thisCounterIt, ++thatCounterIt )
                {
                    Counter thisCounter = *thisCounterIt;
                    Counter thatCounter = *thatCounterIt;
                    if ( thisCounter < thatCounter )
                    {
                        retVal = -1;
                    }
                    else if ( thisCounter > thatCounter )
                    {
                        retVal = 1;
                    }
                }
            }
        }
    }
    return retVal;
}

bool DotCloud::ContainsActor( const Actor& Act, CounterSet* pEvents ) const
{
    auto dotsIt = m_Dots.find( Act );
    bool actorPresent = (dotsIt != m_Dots.end());
    if ( actorPresent && pEvents != NULL )
    {
        pEvents->clear();
        *pEvents = (*dotsIt).second;
    }
    return actorPresent;
}

std::string DotCloud::ToString() const
{
    std::stringstream dotCloudStr;
    int j = 0;
    for ( auto dotsIt = m_Dots.begin(); dotsIt != m_Dots.end(); ++dotsIt )
    {
        if ( j++ > 0 )
        {
            dotCloudStr << ',';
        }
        dotCloudStr << '{';
        dotCloudStr << (*dotsIt).first.ToString();
        dotCloudStr << ",[";

        int k = 0;
        const CounterSet& events( (*dotsIt).second );
        for ( auto evIt = events.begin(); evIt != events.end(); ++evIt )
        {
            if ( k++ > 0 )
            {
                dotCloudStr << ',';
            }
            dotCloudStr << (*evIt);
        }

        dotCloudStr << "]}";
    }
    return dotCloudStr.str();
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
    bigsetClockStr << m_VersionVector.ToString();
    bigsetClockStr << "],[";
    bigsetClockStr << m_DotCloud.ToString();
    bigsetClockStr << "]}";

    return bigsetClockStr.str();
}

// Parses a serialized bigset clock (serialization format is erlang's
// external term format, http://erlang.org/doc/apps/erts/erl_ext_dist.html),
// creating a BigsetClock object.
//
// Bigset clocks are serialized in Erlang using term_to_binary().
//
// A bigset clock is a 2-tuple {version_vector, dot_cloud}, where
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
            CounterSet events;
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

    // get the length of the binary byte stream and ensure it's the right size
    uint32_t length;
    if ( !GetBigEndianUint32( Data, length ) || Actor::GetActorIdSizeInBytes() != length )
    {
        return false;
    }

    // copy the binary data to the Actor object
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
    CounterSet&  Values,
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
                    Values.insert( static_cast<uint8_t>( Data[0] ) );
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
                        Values.insert( value );
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
    if ( m_VersionVector.ContainsActor( Act ) )
    {
        return false;
    }
    m_VersionVector.AddPair( Act, Event );
    return true;
}

bool // true => <Act, Events> added to the dot cloud, else not (likely because Act is already present)
BigsetClock::AddToDotCloud( const Actor& Act, const CounterSet& Events )
{
    // check if the caller's Actor is already in m_VersionVector; if so, do not re-add
    if ( m_DotCloud.ContainsActor( Act ) )
    {
        return false;
    }
    m_DotCloud.AddDots( Act, Events );
    return true;
}

int BigsetClock::Compare( const BigsetClock& That ) const
{
    // if comparing to self, then definitely equal
    if ( this == &That )
    {
        return 0;
    }

    int retVal = m_VersionVector.Compare( That.m_VersionVector );
    if ( 0 == retVal )
    {
        retVal = m_DotCloud.Compare( That.m_DotCloud );
    }
    return retVal;
}

bool BigsetClock::Merge( const BigsetClock& That )
{
    // merging is an idempotent operation, so merging with self does nothing
    if ( this == &That )
    {
        return true;
    }

    // first merge the two VersionVector objects, then merge the DotCloud objects
    bool retVal = m_VersionVector.Merge( That.m_VersionVector );
    if ( retVal )
    {
        retVal = m_DotCloud.Merge( That.m_DotCloud );
        if ( retVal )
        {
            // all is good so far, so compress as much as possible
            CompressSeen();
        }
    }
    return retVal;
}

bool BigsetClock::IsSeen( const Actor& Act, Counter Event ) const
{
    bool isSeen = false;

    // check if the caller's dot (Actor/Event) is descended by this clock's version vector
    Counter event;
    bool containsActor = m_VersionVector.ContainsActor( Act, &event );
    if ( containsActor )
    {
        // is the caller's Event value less than or equal to the value in the VV?
        if ( Event <= event )
        {
            isSeen = true;
        }
    }

    // if the caller's dot was not seen by the VV, check for an exact match in the dot cloud
    if ( !isSeen )
    {
        CounterSet events;
        containsActor = m_DotCloud.ContainsActor( Act, &events );
        isSeen = containsActor && (events.count( Event ) > 0);
    }
    return isSeen;
}

void BigsetClock::CompressSeen()
{
    // walk through the entries in the dot cloud; for each, if the actor is in
    // the version vector, then see if we have contiguous events that can be
    // compacted

    // TODO: clean this up to not directly use the data members of m_DotCount and m_VersionVector
    std::map<Actor, Counter>& versionVector( m_VersionVector.m_Pairs );
    std::map<Actor, CounterSet>& dotCloud( m_DotCloud.m_Dots );
    for ( auto dcIt = dotCloud.begin(); dcIt != dotCloud.end(); /* dcIt advanced below */ )
    {
        // get the actor/events for this dot cloud entry
        const Actor& actor( (*dcIt).first );
        CounterSet& events( (*dcIt).second );

        // does the actor from this dot cloud entry appear in the version vector?
        auto vvIt = versionVector.find( actor );
        if ( vvIt != versionVector.end() )
        {
            // yes, the actor from the dot cloud entry is in the version vector;
            // we walk the list of events in this dot cloud entry, checking if
            // an event is contiguous with the VV (can then compress) or dominated
            // by the VV (can then remove)
            Counter vvEvent = (*vvIt).second;
            for ( auto eventIt = events.begin(); eventIt != events.end(); /* eventIt advanced below */ )
            {
                Counter dcEvent = *eventIt;
                if ( dcEvent == vvEvent + 1 )
                {
                    // the events are contiguous, so we can compress
                    (*vvIt).second = ++vvEvent;
                    events.erase( eventIt++ );
                }
                else if ( dcEvent <= vvEvent )
                {
                    // DC event dominated by VV event, so erase
                    events.erase( eventIt++ );
                }
                else
                {
                    ++eventIt;
                }
            }

            // advance our dot cloud iterator; if we removed all the events from
            // this dot cloud entry, then remove the entry
            if ( events.empty() )
            {
                dotCloud.erase( dcIt++ );
            }
            else
            {
                ++dcIt;
            }
        }
        else
        {
            // no, the actor from the dot cloud entry is not in the version vector;
            // if the event value is 1, then we can move this entry to the version vector
            if ( events.size() == 1 && events.count( 1 ) == 1 )
            {
                m_VersionVector.AddPair( actor, 1 );
                dotCloud.erase( dcIt++ );
            }
            else
            {
                ++dcIt;
            }
        }
    }
}

void BigsetClock::SubtractSeen( DotList& Dots ) const
{
    // walk the list of dots in the caller's DotList, removing any seen by this BigsetClock
    std::map<Actor, Counter>& dots( Dots.m_Pairs );
    for ( auto dotIt = dots.begin(); dotIt != dots.end(); /* dotIt advanced below */ )
    {
        if ( IsSeen( (*dotIt).first, (*dotIt).second ) )
        {
            dots.erase( dotIt++ );
        }
        else
        {
            ++dotIt;
        }
    }
}

} // namespace bigset
} // namespace basho
