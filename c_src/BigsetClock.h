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

// Actor class: represents an actor who performs an action on a bigset
class Actor
{
    char m_ID[8];

public:
    Actor() { Clear(); }
    Actor( const char* pID ) { ::memcpy( m_ID, pID, sizeof m_ID ); }
    Actor( const Actor& That ) { ::memcpy( m_ID, That.m_ID, sizeof m_ID ); }
    Actor& operator=( const Actor& That ) { ::memcpy( m_ID, That.m_ID, sizeof m_ID ); return *this; }

    void Clear() { ::memset( m_ID, 0, sizeof m_ID ); }

    bool SetId( const char* pID, size_t BytesAvailable )
    {
        if ( BytesAvailable < sizeof m_ID )
        {
            return false;
        }
        ::memcpy( m_ID, pID, sizeof m_ID );
        return true;
    }

    // return the Actor as a string, in Erlang term format
    std::string
    ToString() const;
};

// DotCloudEntry class: is an actor together with a list of non-contiguous events observed for the actor
class DotCloudEntry
{
    Actor               m_Actor;
    std::list<uint32_t> m_Events;

public:
    DotCloudEntry( const Actor& Actor, const std::list<uint32_t>& Events ) : m_Actor( Actor ), m_Events( Events ) {}

    // accessors
    const Actor& GetActor() const { return m_Actor; }
    const std::list<uint32_t>& GetEvents() const { return m_Events; }
};

typedef basho::crdtUtils::Dot<Actor, uint32_t>  Dot;
typedef basho::crdtUtils::Dots<Actor, uint32_t> Dots;

class BigsetClock //: public basho::crdtUtils::DotContext<Actor, uint32_t>
{
    Dots                     m_VersionVector;
    std::list<DotCloudEntry> m_DotCloud;

public:
    BigsetClock() { }
    virtual ~BigsetClock() { }

    void AddToVersionVector( const Actor& Act, uint32_t Event )
    {
        m_VersionVector.AddDot( Act, Event );
    }

    void AddToDotCloud( const Actor& Act, const std::list<uint32_t>& Events )
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

    static bool // returns true if the binary value was successfully converted to a bigset clock, else returns false
    ValueToBigsetClock( const leveldb::Slice& Value, BigsetClock& Clock );

private:
    // helper methods used by ValueToBigsetClock()
    //
    // NOTE: all these methods advance pData and decrement BytesLeft appropriately

    static bool
    ProcessListOfTwoTuples(
        const char*& pData,
        size_t& BytesLeft,
        BigsetClock& Clock,
        bool IsVersionVector ); // true => each 2-tuple in the list is a version vector, else it's a dot cloud

    static bool
    IsList(
        const char*& pData,
        size_t& BytesLeft,
        uint32_t& ElementCount ); // OUT: receives count of elements in the list

    static bool
    IsTwoTuple( const char*& pData, size_t& BytesLeft );

    static bool
    GetBigEndianUint16( const char*& pData, size_t& BytesLeft, uint16_t& Value );

    static bool
    GetBigEndianUint32( const char*& pData, size_t& BytesLeft, uint32_t& Value );

    static bool
    GetActor( const char*& pData, size_t& BytesLeft, Actor& Act );

    static bool
    GetInteger( const char*& pData, size_t& BytesLeft, uint32_t& Value );

    static bool
    GetIntegerList( const char*& pData, size_t& BytesLeft, std::list<uint32_t>& Values );
};

} // namespace bigset
} // namespace basho

#endif //BASHO_BIGSET_CLOCK_H
