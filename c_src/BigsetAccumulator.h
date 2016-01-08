//
// Created by Paul A. Place on 12/16/15.
//

#ifndef BIGSETS_BIGSET_ACC_H
#define BIGSETS_BIGSET_ACC_H

#include <list>

#include "CrdtUtils.h"
#include "leveldb/slice.h"

// TODO: remove all this endian stuff and reuse the DecodeFixed32() and DecodeFixed64() functions from leveldb
//
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

namespace basho {
namespace Bigset {

typedef leveldb::Slice ErlTerm;
typedef uint32_t       ErlCounter;

typedef basho::CrdtUtils::Dot<ErlTerm, ErlCounter>  Dot;
typedef basho::CrdtUtils::Dots<ErlTerm, ErlCounter> Dots;

class BigsetClock : public basho::CrdtUtils::DotContext<ErlTerm, ErlCounter>
{
public:
    BigsetClock() { }

    virtual ~BigsetClock() { }


    void
    Merge( const BigsetClock& /*clock*/ )
    {
    }

    Dots
    SubtractSeen( const Dots& /*dots*/ )
    {
        return Dots();
    }

    static BigsetClock ValueToBigsetClock( const ErlTerm& value )
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
    //
    // TODO: reuse the DecodeFixed32() function from leveldb
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

    // TODO: reuse the DecodeFixed64() function from leveldb
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
        leveldb::Slice
                res = leveldb::Slice( s.data(), 1 ); // one byte c, e, or z
        s.remove_prefix( 1 );
        return res;
    }

public:
    BigsetKey( ErlTerm key )
    {
        m_SetName              = Get32PrefData( key );
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

        if ( IsClock() )
        {
            // actor is the remaining portion of the key (not length-prefixed)
            m_Actor = key;
        }
        else if ( IsElement() )
        {
            m_Element     = Get32PrefData( key );
            m_Actor       = Get32PrefData( key );
            m_Counter     = GetCounter( key );
            m_IsTombstone = ('r' == GetTsb( key )[0]);
        }
    }

    bool IsClock() const { return KeyTypeClock == m_KeyType; }

    bool IsElement() const { return KeyTypeElement == m_KeyType; }

    bool IsEnd() const { return KeyTypeEnd == m_KeyType; }

    bool IsValid() const { return KeyTypeInvalid != m_KeyType; }

    const ErlTerm& GetSetName() const { return m_SetName; }

    const ErlTerm& GetActor() const { return m_Actor; }

    const ErlTerm& GetElement() const { return m_Element; }

    ErlCounter GetCounter() const { return m_Counter; }

    bool GetTombstone() const { return m_IsTombstone; }
};

class BigsetAccumulator
{
private:
    ErlTerm     m_ThisActor;
    ErlTerm     m_CurrentSetName;
    ErlTerm     m_CurrentElement;
    BigsetClock m_CurrentContext;
    Dots        m_CurrentDots;

    ErlTerm m_ReadyKey;
    ErlTerm m_ReadyValue;
    bool    m_RecordReady;

    // sets the "ready" values after we have finished processing the last record for an element
    void
    FinalizeElement()
    {
        Dots remainingDots = m_CurrentContext.SubtractSeen( m_CurrentDots );
        if ( !remainingDots.IsEmpty() )
        {
            // this element is "in" the set locally
            m_ReadyKey    = m_CurrentElement;
            m_ReadyValue  = remainingDots.ToValue();
            m_RecordReady = true;

            m_CurrentContext.Clear();
            m_CurrentDots.Clear();
        }
    }

public:
    BigsetAccumulator() : m_RecordReady( false ) { }

    ~BigsetAccumulator() { }

    void
    AddRecord( ErlTerm key, ErlTerm value )
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
                m_CurrentContext.Merge( BigsetClock::ValueToBigsetClock( value ) );
                m_CurrentDots.AddDot( keyToAdd.GetActor(),
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
            key           = m_ReadyKey;
            value         = m_ReadyValue;
            m_RecordReady = false; // prepare for the next record
        }
    }
};

} // namespace Bigset
} // namespace basho

#endif //BIGSETS_BIGSET_ACC_H
