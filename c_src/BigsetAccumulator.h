//
// Created by Paul A. Place on 12/16/15.
//

#ifndef BIGSETS_BIGSET_ACC_H
#define BIGSETS_BIGSET_ACC_H

#include <list>

#include "CrdtUtils.h"
#include "leveldb/slice.h"

// platform stuff taken from leveldb/port/port_posix.h
//
// NOTE: we need more build infrastructure for this to work; e.g., we need
// a build step that detects the platform type and sets things like the
// OS_XXX macro accordingly; for now, we assume we're running on a little-
// endian system
#if 0
#undef PLATFORM_IS_LITTLE_ENDIAN
#if defined(OS_MACOSX)

#include <machine/endian.h>

#if defined(__DARWIN_LITTLE_ENDIAN) && defined(__DARWIN_BYTE_ORDER)
#define PLATFORM_IS_LITTLE_ENDIAN \
        (__DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN)
#endif
#elif defined(OS_SOLARIS)
#include <sys/isa_defs.h>
#ifdef _LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN true
#else
#define PLATFORM_IS_LITTLE_ENDIAN false
#endif
#elif defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_NETBSD) ||\
      defined(OS_DRAGONFLYBSD) || defined(OS_ANDROID)
#include <sys/types.h>
#include <sys/endian.h>

#if !defined(PLATFORM_IS_LITTLE_ENDIAN) && defined(_BYTE_ORDER)
#define PLATFORM_IS_LITTLE_ENDIAN (_BYTE_ORDER == _LITTLE_ENDIAN)
#endif
#else
#include <endian.h>
#endif
#endif // 0
namespace port {
static const bool kLittleEndian = true; //PLATFORM_IS_LITTLE_ENDIAN;
}

namespace Bigset {

typedef leveldb::Slice ErlTerm;
typedef uint32_t       ErlCounter;

class Dot
{
private:
    ErlTerm    m_actor;
    ErlCounter m_counter;

public:
    Dot( ErlTerm actor, ErlCounter counter )
            : m_actor( actor ), m_counter( counter ) { }
};

class Dots
{
private:
    std::list<Dot> m_Dots;

public:
    Dots() { }

    ~Dots() { }

    void
    add( ErlTerm actor, ErlCounter counter, bool isTombstone )
    {
        if ( !isTombstone )
        {
            m_Dots.push_back( Dot( actor, counter ) );
        }
    }

    void clear() { m_Dots.clear(); }

    bool empty() const { return m_Dots.empty(); }

    ErlTerm toValue() const { return ErlTerm(); }
};

class BigsetClock : public CrdtUtils::DotContext<ErlTerm>
{
public:
    BigsetClock() { }

    virtual ~BigsetClock() { }

    void
    merge( const BigsetClock& /*clock*/ )
    {
    }

    Dots
    subtractSeen( const Dots& /*dots*/ )
    {
        return Dots();
    }

    static BigsetClock valueToClock( const ErlTerm& value )
    {
        return BigsetClock();
    }
};

// Clock keys are
//  <<
//    SetNameLen:32/little-unsigned-integer, %% the lengthof the set name
//    SetName:SetNameLen/binary, %% Set name for bytewise comparison
//    $c, %% means clock
//    Actor/binary, %% The actual actor
//    >>
// Element keys are
//  <<
//    SetNameLen:32/little-unsigned-integer, %% the length of the set name
//    SetName:SetNameLen/binary, %% Set name for bytewise comparison
//    $e, % indicates an element
//    ElementLen:32/little-unsigned-integer, %% Length of the element
//    Element:ElementLen/binary, %% The actual element
//    ActorLen:32/little-unsigned-integer, %% Length of the actor ID
//    Actor:ActorLen/binary, %% The actual actor
//    Counter:64/little-unsigned-integer,
//    $a | $r:8/binary, %% a|r single byte char to determine if the key is an add or a tombstone
//    >>
//  End Key is:
//  <<
//    SetNameLen:32/little-unsigned-integer, %% the lengthof the set name
//    SetName:SetNameLen/binary, %% Set name for bytewise comparison
//    $z, %% means end key, used for limiting streaming fold
//    >>
class BigsetKey
{
    enum KeyType
    {
        KeyTypeInvalid = 0,
        KeyTypeClock   = 'c',
        KeyTypeElement = 'a',
        KeyTypeEnd     = 'z'
    };

    KeyType  m_KeyType;
    ErlTerm  m_SetName;
    ErlTerm  m_Element;
    ErlTerm  m_Actor;
    uint64_t m_Counter;
    bool     m_IsTombstone;

    // DecodeFixedXx() methods taken from leveldb/util/coding.h
    static uint32_t DecodeFixed32( const char* ptr )
    {
        if ( port::kLittleEndian )
        {
            // Load the raw bytes
            uint32_t result;
            memcpy( &result,
                    ptr,
                    sizeof( result ) );  // gcc optimizes this to a plain load
            return result;
        }
        else
        {
            return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
                    |
                    (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) <<
                     8)
                    |
                    (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) <<
                     16)
                    |
                    (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) <<
                     24));
        }
    }

    static uint64_t DecodeFixed64( const char* ptr )
    {
        if ( port::kLittleEndian )
        {
            // Load the raw bytes
            uint64_t result;
            memcpy( &result,
                    ptr,
                    sizeof( result ) );  // gcc optimizes this to a plain load
            return result;
        }
        else
        {
            uint64_t lo = DecodeFixed32( ptr );
            uint64_t hi = DecodeFixed32( ptr + 4 );
            return (hi << 32) | lo;
        }
    }

    // key decoding helpers taken from leveldb/util/comparator.cc
    static leveldb::Slice Get32PrefData( leveldb::Slice& s )
    {
        uint32_t       actual = DecodeFixed32( s.data() );
        leveldb::Slice res    = leveldb::Slice( s.data() + 4, actual );
        s.remove_prefix( 4 + actual );
        return res;
    }

    static uint64_t GetCounter( leveldb::Slice& s )
    {
        uint64_t actual = DecodeFixed64( s.data() );
        s.remove_prefix( 8 );
        return actual;
    }

    static leveldb::Slice GetTsb( leveldb::Slice& s )
    {
        leveldb::Slice res = leveldb::Slice( s.data(), 1 ); // one byte a or r
        s.remove_prefix( 1 );
        return res;
    }

    static leveldb::Slice GetKeyType( leveldb::Slice& s )
    {
        leveldb::Slice res = leveldb::Slice( s.data(), 1 ); // one byte c, e, or z
        s.remove_prefix( 1 );
        return res;
    }

public:
    BigsetKey( ErlTerm key )
    {
        m_SetName = Get32PrefData( key );
        leveldb::Slice keyType = GetKeyType( key );
        switch ( keyType[0] )
        {
            case 'c':
                m_KeyType = KeyTypeClock;
                break;

            case 'e':
                m_KeyType = KeyTypeElement;
                break;

            case 'z':
                m_KeyType = KeyTypeEnd;
                break;

            default:
                m_KeyType = KeyTypeInvalid;
                break;
        }

        if ( isClock() )
        {
            // actor is the remaining portion of the key (not length-prefixed)
            m_Actor = key;
        }
        else if ( isElement() )
        {
            m_Element = Get32PrefData( key );
            m_Actor   = Get32PrefData( key );
            m_Counter = GetCounter( key );
            m_IsTombstone = ('r' == GetTsb( key )[0]);
        }
    }

    bool isClock()   const { return KeyTypeClock   == m_KeyType; }
    bool isElement() const { return KeyTypeElement == m_KeyType; }
    bool isEnd()     const { return KeyTypeEnd     == m_KeyType; }
    bool isValid()   const { return KeyTypeInvalid != m_KeyType; }

    const ErlTerm& getActor()     const { return m_Actor; }
    const ErlTerm& getElement()   const { return m_Element; }
    ErlCounter     getCounter()   const { return m_Counter; }
    bool           getTombstone() const { return m_IsTombstone; }
};

class BigsetAccumulator
{
private:
    bool        m_RecordReady;
    ErlTerm     m_ThisActor;
    ErlTerm     m_CurrentElement;
    BigsetClock m_CurrentContext;
    Dots        m_CurrentDots;

    ErlTerm     m_ReadyKey;
    ErlTerm     m_ReadyValue;

public:
    BigsetAccumulator() : m_RecordReady( false ) { }

    ~BigsetAccumulator() { }

    void
    add( ErlTerm key, ErlTerm value )
    {
        BigsetKey keyToAdd( key );
        if ( keyToAdd.isValid() )
        {
            if ( keyToAdd.isClock() )
            {
                // we have a clock key; see if it's for the actor we're tracking; if not, we ignore this clock
                if ( 0 == m_ThisActor.size() )
                {
                    // this is the first clock key we've seen for this bigset, so save its associated actor
                    m_ThisActor = keyToAdd.getActor();
                }
                else if ( keyToAdd.getActor() == m_ThisActor )
                {}
            }
            else if ( keyToAdd.isElement() )
            {
                // we have an element key; see if it's for the current element we're processing
                if ( keyToAdd.getElement() != m_CurrentElement )
                {
                    // we are starting a new element, so finish processing of the previous element
                    Dots remainingDots = m_CurrentContext.subtractSeen( m_CurrentDots );
                    if ( !remainingDots.empty() )
                    {
                        // this element is "in" the set locally
                        m_ReadyKey = m_CurrentElement;
                        m_ReadyValue = remainingDots.toValue();
                        m_RecordReady = true;
                        m_CurrentContext.clear();
                        m_CurrentDots.clear();
                    }
                }

                // accumulate values
                m_CurrentElement = keyToAdd.getElement();
                m_CurrentContext.merge( BigsetClock::valueToClock( value ) );
                m_CurrentDots.add( keyToAdd.getActor(), keyToAdd.getCounter(), keyToAdd.getTombstone() );
            }
            else if ( keyToAdd.isEnd() )
            {
                // this is an end key, so we're done enumerating the elements in this bigset
            }
            else
            {
                // oops, we weren't expecting this
            }
        }
        else
        {
            // throw exception? if so, caller better catch it
        }
    }

    bool
    recordReady()
    {
        return m_RecordReady;
    }

    void
    getCurrentElement( leveldb::Slice& key, leveldb::Slice& value )
    {
        if ( m_RecordReady )
        {
            key = m_ReadyKey;
            value = m_ReadyValue;
            m_RecordReady = false; // prepare for the next record
        }
    }
};

} // namespace Bigset

#endif //BIGSETS_BIGSET_ACC_H
