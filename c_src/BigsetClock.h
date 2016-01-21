//
// Created by Paul A. Place on 1/12/16.
//

#ifndef BASHO_BIGSET_CLOCK_H
#define BASHO_BIGSET_CLOCK_H

#include "leveldb/slice.h"
#include "CrdtUtils.h"

namespace basho {
namespace bigset {

typedef leveldb::Slice ErlTerm;
typedef uint32_t       ErlCounter;

typedef leveldb::Slice     Slice;
typedef uint64_t           Counter;
typedef std::list<Counter> CounterList;

// Actor class: represents an actor who performs an action on a bigset
class Actor
{
    char m_ID[8];

public:
    Actor() { Clear(); }
    Actor( const Actor& That ) { ::memcpy( m_ID, That.m_ID, sizeof m_ID ); }
    Actor& operator=( const Actor& That ) { ::memcpy( m_ID, That.m_ID, sizeof m_ID ); return *this; }

    void Clear() { ::memset( m_ID, 0, sizeof m_ID ); }

    bool SetId( Slice& Data )
    {
        if ( Data.size() < sizeof m_ID )
        {
            return false;
        }
        ::memcpy( m_ID, Data.data(), sizeof m_ID );
        Data.remove_prefix( sizeof m_ID );
        return true;
    }

    // return the Actor as a string, in Erlang term format
    std::string
    ToString() const;
};

// DotCloudEntry class: is an actor together with a list of non-contiguous events observed for the actor
class DotCloudEntry
{
    Actor       m_Actor;
    CounterList m_Events;

public:
    DotCloudEntry( const Actor& Actor, const CounterList& Events ) : m_Actor( Actor ), m_Events( Events ) {}

    // accessors
    const Actor& GetActor() const { return m_Actor; }
    const CounterList& GetEvents() const { return m_Events; }
};

typedef basho::crdtUtils::Dot<Actor, Counter>  Dot;
typedef basho::crdtUtils::Dots<Actor, Counter> Dots;

class BigsetClock //: public basho::crdtUtils::DotContext<Actor, Event>
{
    Dots                     m_VersionVector;
    std::list<DotCloudEntry> m_DotCloud;

public:
    BigsetClock() { }
    virtual ~BigsetClock() { }

    void AddToVersionVector( const Actor& Act, Counter Event )
    {
        m_VersionVector.AddDot( Act, Event );
    }

    void AddToDotCloud( const Actor& Act, const CounterList& Events )
    {
        m_DotCloud.push_back( DotCloudEntry( Act, Events ) );
    }

    void Clear() { m_VersionVector.Clear(); m_DotCloud.clear(); }

    void
    Merge( const BigsetClock& /*clock*/ )
    {
    }

    Dots
    SubtractSeen( const Dots& /*dots*/ )
    {
        return Dots();
    }

    // return the bigset clock object as a string, in Erlang term format
    std::string
    ToString() const;

    // parses a serialized bigset clock (serialization format is erlang's
    // external term format, http://erlang.org/doc/apps/erts/erl_ext_dist.html),
    // creating a BigsetClock object
    static bool // returns true if the binary value was successfully converted to a BigsetClock, else returns false
    ValueToBigsetClock(
        const leveldb::Slice& Value,   // IN:  serialized bigset clock
        BigsetClock&          Clock,   // OUT: receives the parsed bigset clock
        std::string&          Error ); // OUT: if false returned, contains a description of the error

private:
    // helper methods used by ValueToBigsetClock()
    //
    // NOTE: all these methods advance pData and decrement BytesLeft appropriately

    static bool
    ProcessListOfTwoTuples(
        Slice&       Data,
        BigsetClock& Clock,
        bool         IsVersionVector, // true => each 2-tuple in the list is a version vector, else it's a dot cloud
        std::string& Error );

    // helper method called by ProcessListOfTwoTuples() to construct error messages
    static void
    ProcessListOfTwoTuplesErrorMessage(
        std::string& Error,
        bool         IsVersionVector,
        const char*  Message,
        uint32_t     ItemNumber = (uint32_t)-1 ); // if not -1, then an "item # " string is prepended to Message

    static bool
    IsList(
        Slice&    Data,
        uint32_t& ElementCount ); // OUT: receives count of elements in the list

    static bool
    IsTwoTuple( Slice& Data );

    static bool
    GetBigEndianUint16( Slice& Data, uint16_t& Value );

    static bool
    GetBigEndianUint32( Slice& Data, uint32_t& Value );

    static bool
    GetBignum( Slice& Data, Counter& Value );

    static bool
    GetActor( Slice& Data, Actor& Act );

    static bool
    GetInteger( Slice& Data, Counter& Value, std::string& Error );

    static bool
    GetIntegerList( Slice& Data, CounterList& Values, std::string& Error );

    static void
    FormatUnrecognizedRecordTypeError( std::string& Error, char RecordId );
};

} // namespace bigset
} // namespace basho

#endif //BASHO_BIGSET_CLOCK_H
