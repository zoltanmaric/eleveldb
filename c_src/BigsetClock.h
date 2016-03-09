//
// Created by Paul A. Place on 1/12/16.
//

#ifndef BASHO_BIGSET_CLOCK_H
#define BASHO_BIGSET_CLOCK_H

#include <map>
#include <set>
#include <string>
#include <stdexcept>
#include <leveldb/slice.h>
#include "util/buffer.h"

namespace basho {
namespace bigset {

typedef leveldb::Slice    Slice;
typedef uint64_t          Counter;
typedef std::set<Counter> CounterSet;
typedef utils::Buffer<32> Buffer;

class BigsetClock; // forward decl for friend decls; TODO: remove this if possible

///////////////////////////////////////////////////////////////////////////////
// Actor class: represents an actor who performs an action on a bigset
class Actor
{
    enum _Constants
    {
        ActorIdSizeInBytes = 8
    };
    char m_ID[ ActorIdSizeInBytes ];

public:
    ///////////////////////////////////
    // Ctors and Assignment Operator
    Actor()
    {
        Clear();
    }

    Actor( const Actor& That )
    {
        ::memcpy( m_ID, That.m_ID, sizeof m_ID );
    }

    // use this ctor with care; if the Slice does not contain enough data, it throws an exception
    Actor( const Slice& Data )
    {
        size_t bytesAvailable = Data.size();
        if ( bytesAvailable < sizeof m_ID )
        {
            throw std::invalid_argument( "Not enough bytes in Slice to construct an Actor" );
        }
        ::memcpy( m_ID, Data.data(), sizeof m_ID );
    }

    Actor& operator=( const Actor& That )
    {
        if ( this != &That )
        {
            ::memcpy( m_ID, That.m_ID, sizeof m_ID );
        }
        return *this;
    }

    ///////////////////////////////////
    // Assignment Methods
    bool SetId( const char* pData, size_t SizeInBytes )
    {
        if ( SizeInBytes < sizeof m_ID )
        {
            return false;
        }
        ::memcpy( m_ID, pData, sizeof m_ID );
        return true;
    }

    // sets this Actor object's ID from a Slice, leaving the Slice untouched
    bool SetId( const Slice& Data )
    {
        return SetId( Data.data(), Data.size() );
    }

    // sets this Actor object's ID from a Slice, and advances the pointer in the Slice
    bool SetId( Slice& Data )
    {
        bool retVal = SetId( Data.data(), Data.size() );
        if ( retVal )
        {
            Data.remove_prefix( sizeof m_ID );
        }
        return retVal;
    }

    // clears the ID of this actor
    void Clear() { ::memset( m_ID, 0, sizeof m_ID ); }

    ///////////////////////////////////
    // Comparison Methods
    //
    // NOTE: if the caller passes a buffer that's not at least sizeof(m_ID)
    // bytes, throws a std::invalid_argument exception
    int Compare( const char* pData, size_t SizeInBytes ) const;

    int Compare( const Actor& That ) const
    {
        return Compare( That.m_ID, sizeof That.m_ID );
    }

    int Compare( const Slice& Data ) const
    {
        return Compare( Data.data(), Data.size() );
    }

    bool operator==( const Actor& That ) const { return 0 == Compare( That ); }
    bool operator!=( const Actor& That ) const { return 0 != Compare( That ); }

    bool operator==( const Slice& Data ) const { return 0 == Compare( Data ); }
    bool operator!=( const Slice& Data ) const { return 0 != Compare( Data ); }

    bool operator<( const Actor& That ) const { return 0 > Compare( That ); }

    ///////////////////////////////////
    // Miscellaneous Methods

    // returns the Actor as a string, in Erlang term format
    std::string ToString() const;

    // appends the bytes of this Actor ID to the caller's Buffer
    bool AppendActorIdToBuffer( Buffer& Value ) const
    {
        return Value.Append( m_ID, ActorIdSizeInBytes );
    }

    // returns the size of an Actor ID in bytes
    static size_t
    GetActorIdSizeInBytes() { return ActorIdSizeInBytes; }
};

///////////////////////////////////////////////////////////////////////////////
// Some helpful typedefs
typedef std::map<Actor, Counter>          ActorToCounterMap;
typedef ActorToCounterMap::iterator       ActorToCounterMapIterator;
typedef ActorToCounterMap::const_iterator ActorToCounterMapConstIterator;

typedef std::map<Actor, CounterSet>          ActorToCounterSetMap;
typedef ActorToCounterSetMap::iterator       ActorToCounterSetMapIterator;
typedef ActorToCounterSetMap::const_iterator ActorToCounterSetMapConstIterator;

///////////////////////////////////////////////////////////////////////////////
// VersionVector class: contains a collection of Actor/Counter pairs, ordered by Actor
class VersionVector
{
    ActorToCounterMap m_Pairs;

    friend class BigsetClock; // TODO: remove this if possible

public:
    ///////////////////////////////////
    // Ctors and Assignment Operator
    VersionVector()
    {}

    VersionVector( const VersionVector& That ) : m_Pairs( That.m_Pairs )
    {}

    VersionVector& operator=( const VersionVector& That )
    {
        if ( this != &That )
        {
            Clear();
            m_Pairs = That.m_Pairs;
        }
        return *this;
    }

    ///////////////////////////////////
    // Assignment Methods

    // adds an Actor/Counter pair to this version vector; if the Actor is
    // already present in the version vector, updates the stored event count
    // iff the caller's Event value is larger than the stored value; returns
    // true if the pair is added/updated, else false
    bool AddPair( const Actor& Act, Counter Event, bool IsTombstone );
    bool AddPair( const Actor& Act, Counter Event )
    {
        return AddPair( Act, Event, false );
    }

    void Clear() { m_Pairs.clear(); }

    // merges the contents of a VersionVector with this object, calling AddPair() for each entry in That
    bool Merge( const VersionVector& That );

    ///////////////////////////////////
    // Comparison Methods
    int Compare( const VersionVector& That ) const;

    bool operator==( const VersionVector& That ) const { return 0 == Compare( That ); }
    bool operator!=( const VersionVector& That ) const { return 0 != Compare( That ); }

    bool operator<( const VersionVector& That ) const { return 0 > Compare( That ); }

    ///////////////////////////////////
    // Miscellaneous Methods

    // returns the number of Actor/Counter pairs in this version vector
    size_t Size() const { return m_Pairs.size(); }

    // returns whether or not this version vector is empty (i.e., does not
    // contain any Actor/Counter pairs)
    bool IsEmpty() const { return m_Pairs.empty(); }

    // returns whether or not a specific Actor is present in this version
    // vector; if the Actor is present, optionally returns the Counter for
    // the Actor
    bool ContainsActor( const Actor& Act, Counter* pEvent = NULL ) const;

    // formats the VersionVector in Erlang term_to_binary() format, placing
    // the bytes in the caller's Buffer
    bool ToBinaryValue( Buffer& Value ) const;

    // returns the VersionVector as a string, in Erlang term format
    std::string ToString() const;
};

// a DotList is conceptually the same as a version vector: a list of 2-tuples,
// where each 2-tuple is an actor and an event value
typedef VersionVector DotList;

///////////////////////////////////////////////////////////////////////////////
// DotCloud class: contains a collection of Actor/CounterSet pairs, ordered by Actor
class DotCloud
{
    ActorToCounterSetMap m_Dots;

    friend class BigsetClock; // TODO: remove this if possible

public:
    ///////////////////////////////////
    // Ctors and Assignment Operator
    DotCloud()
    {}

    DotCloud( const DotCloud& That ) : m_Dots( That.m_Dots )
    {}

    DotCloud& operator=( const DotCloud& That )
    {
        if ( this != &That )
        {
            Clear();
            m_Dots = That.m_Dots;
        }
        return *this;
    }

    ///////////////////////////////////
    // Assignment Methods

    // adds an Actor/Counter pair to this dot cloud; if the Actor is
    // already present in the dot cloud, updates the stored event set
    // iff the caller's Event value is not already in the set; returns
    // true if the pair is added/updated, else false
    bool AddDot( const Actor& Act, Counter Event );

    // adds an Actor/CounterSet pair to this dot cloud; if the Actor is
    // already present in the dot cloud, calls AddDot() for each value in
    // Events; returns true if any updates are made to the dot cloud
    bool AddDots( const Actor& Act, const CounterSet& Events );

    void Clear() { m_Dots.clear(); }

    // merges the contents of a DotCloud with this object, calling AddDots() for each entry in That
    bool Merge( const DotCloud& That );

    ///////////////////////////////////
    // Comparison Methods
    int Compare( const DotCloud& That ) const;

    bool operator==( const DotCloud& That ) const { return 0 == Compare( That ); }
    bool operator!=( const DotCloud& That ) const { return 0 != Compare( That ); }

    bool operator<( const DotCloud& That ) const { return 0 > Compare( That ); }

    ///////////////////////////////////
    // Miscellaneous Methods

    // returns the number of Actor/CounterSet pairs in this dot cloud
    size_t Size() const { return m_Dots.size(); }

    // returns whether or not this dot cloud is empty (i.e., does not
    // contain any Actor/CounterSet pairs)
    bool IsEmpty() const { return m_Dots.empty(); }

    // returns whether or not a specific Actor is present in this dot cloud;
    // if the Actor is present, optionally returns the CounterSet for
    // the Actor
    bool ContainsActor( const Actor& Act, CounterSet* pEvents = NULL ) const;

    // returns the VersionVector as a string, in Erlang term format
    std::string ToString() const;
};

///////////////////////////////////////////////////////////////////////////////
// BigsetClock class
class BigsetClock
{
    VersionVector m_VersionVector;
    DotCloud      m_DotCloud;

public:
    ///////////////////////////////////
    // Ctors and Assignment Operator
    BigsetClock() {}
    BigsetClock( const BigsetClock& That ) : m_VersionVector( That.m_VersionVector ), m_DotCloud( That.m_DotCloud )
    {}

    BigsetClock& operator=( const BigsetClock& That )
    {
        if ( this != &That )
        {
            m_VersionVector = That.m_VersionVector;
            m_DotCloud = That.m_DotCloud;
        }
        return *this;
    }

    ///////////////////////////////////
    // Assignment Methods
    bool // true => <Act,Event> added to the version vector, else not (likely because Act is already present)
    AddToVersionVector( const Actor& Act, Counter Event );

    bool // true => <Act, Events> added to the dot cloud, else not (likely because Act is already present)
    AddToDotCloud( const Actor& Act, const CounterSet& Events );

    void Clear()
    {
        m_VersionVector.Clear();
        m_DotCloud.Clear();
    }

    // parses a serialized bigset clock (serialization format is erlang's
    // external term format, http://erlang.org/doc/apps/erts/erl_ext_dist.html),
    // creating a BigsetClock object
    static bool // returns true if the binary value was successfully converted to a BigsetClock, else returns false
    ValueToBigsetClock(
        const Slice&  Value,   // IN:  serialized bigset clock
        BigsetClock&  Clock,   // OUT: receives the parsed bigset clock
        std::string&  Error ); // OUT: if false returned, contains a description of the error

    ///////////////////////////////////
    // Comparison Methods
    int Compare( const BigsetClock& That ) const;

    bool operator==( const BigsetClock& That ) const { return 0 == Compare( That ); }
    bool operator!=( const BigsetClock& That ) const { return 0 != Compare( That ); }

    ///////////////////////////////////
    // Basic Operations on a BigsetClock

    // merges the caller's BigsetClock with this one, updating the version
    // vector and dot cloud accordingly, and then calls CompressSeen()
    bool // true => merge succeeded, else false
    Merge( const BigsetClock& That );

    // returns whether or not the specified dot (actor/event) were seen by this clock
    bool IsSeen( const Actor& Act, Counter Event ) const;

    // walks through the entries in the dot cloud; for each, if the actor is in
    // the version vector, see if we have contiguous events that can be compressed
    void CompressSeen();

    // removes any dots seen by this BigsetClock from the caller's DotList
    void SubtractSeen( DotList& Dots ) const;

    ///////////////////////////////////
    // Miscellaneous Methods

    // returns whether or not this bigset clock is empty (i.e., does not
    // contain any entries in its version vector or dot cloud)
    bool IsEmpty() const { return m_VersionVector.IsEmpty() && m_DotCloud.IsEmpty(); }

    // return the bigset clock object as a string, in Erlang term format
    std::string
    ToString() const;

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
    GetIntegerList( Slice& Data, CounterSet& Values, std::string& Error );

    static void
    FormatUnrecognizedRecordTypeError( std::string& Error, char RecordId );
};

} // namespace bigset
} // namespace basho

#endif //BASHO_BIGSET_CLOCK_H
