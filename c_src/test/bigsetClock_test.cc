//
// Created by Paul A. Place on 1/22/16.
//

#include "utils.h"
#include <BigsetClock.h>
#include <gtest/gtest.h>

using namespace basho::utils;
using namespace basho::bigset;

///////////////////////////////////////////////////////////////////////////////
// Actor class unit tests

// helper functions that construct Actor objects
static void SetActorId( Actor& Act, const char* pActorId, size_t ActorIdSize )
{
    // set the ID of the caller's Actor object from the specified ActorId bytes
    Slice actorIdSlice( pActorId, ActorIdSize );
    Act.SetId( actorIdSlice );
}

static void SetActorId( Actor& Act, const char ActorIdChar )
{
    // create an Actor object from the specified ActorId byte
    char actorId[8];
    ::memset( actorId, ActorIdChar, sizeof actorId );
    SetActorId( Act, actorId, sizeof actorId );
}

TEST(Actor, Ctor)
{
    // first test the default Actor ctor
    Actor actor;
    std::string actorStr = actor.ToString();
    std::string emptyActorStr( "<<0,0,0,0,0,0,0,0>>" );
    ASSERT_EQ( 0, ::strcmp( actorStr.c_str(), emptyActorStr.c_str() ) );

    // now construct an actor with a known ID and test the copy ctor
    char buff[8];
    buff[0] = 'A';
    buff[1] = 'b';
    buff[2] = (char)255;
    buff[3] = (char)0;
    buff[4] = 'C';
    buff[5] = (char)1;
    buff[6] = 'd';
    buff[7] = (char)129;

    std::string expectedActorStr( "<<65,98,255,0,67,1,100,129>>" );

    const Slice buffSlice( buff, sizeof buff );
    actor.SetId( buffSlice );
    actorStr = actor.ToString();
    ASSERT_EQ( 0, ::strcmp( actorStr.c_str(), expectedActorStr.c_str() ) );

    // now try the copy ctors
    Actor actorCopy( actor );
    actorStr = actorCopy.ToString();
    ASSERT_EQ( 0, ::strcmp( actorStr.c_str(), expectedActorStr.c_str() ) );

    Actor actorCopyFromSlice( buffSlice );
    actorStr = actorCopyFromSlice.ToString();
    ASSERT_EQ( 0, ::strcmp( actorStr.c_str(), expectedActorStr.c_str() ) );
}

TEST(Actor, Compare)
{
    // test the Compare() method and == operator, comparing to another Actor object

    // construct an actor with a known ID and test the copy ctor
    char buff[8];
    buff[0] = 'A';
    buff[1] = 'b';
    buff[2] = (char)255;
    buff[3] = (char)0;
    buff[4] = 'C';
    buff[5] = (char)1;
    buff[6] = 'd';
    buff[7] = (char)129;
    const Slice buffSlice( buff, sizeof buff );

    Actor actor1( buffSlice );
    Actor actor2( buffSlice );

    ASSERT_EQ( 0, actor1.Compare( buffSlice.data(), buffSlice.size() ) );
    ASSERT_EQ( 0, actor1.Compare( buffSlice ) );
    ASSERT_EQ( 0, actor1.Compare( actor2 ) );
    ASSERT_EQ( 0, actor2.Compare( actor1 ) );
    ASSERT_TRUE( actor1 == buffSlice );
    ASSERT_TRUE( actor1 == actor2 );

    // alter the buff slightly, construct another Actor, and ensure it compares as different
    buff[4]++;
    Actor actor3( buffSlice );

    ASSERT_LT( 0, actor3.Compare( actor1 ) ); // expect actor3 > actor1
    ASSERT_TRUE( actor3 != actor1 );
}

TEST(Actor, LessThan)
{
    Actor actor1, actor2;
    SetActorId( actor1, '1' );
    SetActorId( actor2, '2' );
    ASSERT_TRUE( actor1 < actor2 );
    ASSERT_FALSE( actor2 < actor1 );
    ASSERT_FALSE( actor1 < actor1 );
}

///////////////////////////////////////////////////////////////////////////////
// VersionVector class unit tests

TEST(VersionVector, AddPair)
{
    Actor actor1, actor2;
    SetActorId( actor1, '1' );
    SetActorId( actor2, '2' );

    // add a couple entries to the version vector
    VersionVector vv;
    ASSERT_TRUE( vv.IsEmpty() );
    ASSERT_TRUE( vv.AddPair( actor1, 101 ) );
    ASSERT_EQ( 1, vv.Size() );
    ASSERT_FALSE( vv.IsEmpty() );
    ASSERT_TRUE( vv.AddPair( actor2, 202 ) );
    ASSERT_EQ( 2, vv.Size() );

    // ensure we can query the event values for the actors
    Counter event;
    ASSERT_TRUE( vv.ContainsActor( actor1, &event ) );
    ASSERT_EQ( 101, event );
    ASSERT_TRUE( vv.ContainsActor( actor2, &event ) );
    ASSERT_EQ( 202, event );

    // now re-add the same actor with a larger event value (expect the new (larger) value to be stored)
    ASSERT_TRUE( vv.AddPair( actor1, 102 ) );
    ASSERT_EQ( 2, vv.Size() ); // we didn't actually add another entry, but simply updated the count for actor1
    ASSERT_TRUE( vv.ContainsActor( actor1, &event ) );
    ASSERT_EQ( 102, event );

    // if we try to re-add the same actor with a smaller event value, the value should not be updated
    ASSERT_FALSE( vv.AddPair( actor1, 101 ) );
    ASSERT_EQ( 2, vv.Size() ); // we didn't actually add another entry, but simply updated the count for actor1
    event = 0;
    ASSERT_TRUE( vv.ContainsActor( actor1, &event ) );
    ASSERT_EQ( 102, event );

    // if we add a pair with the tombstone flag set, then the pair should not be added
    ASSERT_FALSE( vv.AddPair( actor1, 1001, true ) );
}

TEST(VersionVector, Merge_Idempotent)
{
    Actor actor1, actor2;
    SetActorId( actor1, '1' );
    SetActorId( actor2, '2' );

    VersionVector vv;
    vv.AddPair( actor1, 1 );
    vv.AddPair( actor2, 2 );

    const VersionVector vvRef( vv );
    ASSERT_EQ( 2, vvRef.Size() );

    // merging with self should result in, well, self
    ASSERT_TRUE( vv.Merge( vv ) );
    ASSERT_TRUE( vv == vvRef );
}

TEST(VersionVector, Merge_Identity)
{
    Actor actor1, actor2;
    SetActorId( actor1, '1' );
    SetActorId( actor2, '2' );

    VersionVector vv;
    vv.AddPair( actor1, 1 );
    vv.AddPair( actor2, 2 );

    const VersionVector vvRef( vv );
    ASSERT_EQ( 2, vvRef.Size() );

    // merging with an empty VV should result in self
    VersionVector vv2;
    ASSERT_TRUE( vv2.IsEmpty() );
    ASSERT_TRUE( vv.Merge( vv2 ) );
    ASSERT_TRUE( vv == vvRef );

    // starting empty and merging with a VV should result in the VV
    ASSERT_TRUE( vv2.IsEmpty() );
    ASSERT_TRUE( vv2.Merge( vv ) );
    ASSERT_TRUE( vv2 == vvRef );
    ASSERT_FALSE( vv2.IsEmpty() );
}

TEST(VersionVector, Merge_Concurrent)
{
    Actor actor1, actor2;
    SetActorId( actor1, '1' );
    SetActorId( actor2, '2' );

    VersionVector vv1, vv2;
    vv1.AddPair( actor1, 1 );
    vv2.AddPair( actor2, 2 );
    ASSERT_EQ( 1, vv1.Size() );
    ASSERT_EQ( 1, vv2.Size() );

    ASSERT_TRUE( vv1.Merge( vv2 ) );
    ASSERT_EQ( 2, vv1.Size() );
    ASSERT_EQ( 1, vv2.Size() );
    ASSERT_TRUE( vv2 < vv1 );

    ASSERT_TRUE( vv2.Merge( vv1 ) );
    ASSERT_EQ( 2, vv1.Size() );
    ASSERT_EQ( 2, vv2.Size() );
    ASSERT_TRUE( vv2 == vv1 );
}

TEST(VersionVector, Merge_Descends)
{
    Actor actor1, actor2;
    SetActorId( actor1, '1' );
    SetActorId( actor2, '2' );

    VersionVector vv1, vv2;
    vv1.AddPair( actor1, 1 );
    vv1.AddPair( actor2, 2 );

    // the event values in vv2 descend those in vv1
    vv2.AddPair( actor1, 100 );
    vv2.AddPair( actor2, 200 );

    // so merging vv2 into vv1 should update the pairs in vv1 to match those in vv2
    ASSERT_TRUE( vv1.Merge( vv2 ) );
    ASSERT_TRUE( vv1 == vv2 );

    Counter event;
    ASSERT_TRUE( vv1.ContainsActor( actor1, &event ) );
    ASSERT_EQ( 100, event );
    ASSERT_TRUE( vv1.ContainsActor( actor2, &event ) );
    ASSERT_EQ( 200, event );

    // now update the event values in vv1 so that it descends those in vv2
    vv1.AddPair( actor1, 101 );
    vv1.AddPair( actor2, 202 );

    ASSERT_TRUE( vv1.Merge( vv2 ) );
    ASSERT_TRUE( vv2 < vv1 );

    ASSERT_TRUE( vv1.ContainsActor( actor1, &event ) );
    ASSERT_EQ( 101, event );
    ASSERT_TRUE( vv1.ContainsActor( actor2, &event ) );
    ASSERT_EQ( 202, event );
}

TEST(VersionVector, Compare)
{
    Actor actor1, actor2;
    SetActorId( actor1, '1' );
    SetActorId( actor2, '2' );

    VersionVector vv1, vv2;
    vv1.AddPair( actor1, 1 );
    vv2.AddPair( actor1, 1 );

    // the two VVs should match since they contain the same actor/event
    ASSERT_EQ( 0, vv1.Compare( vv1 ) );
    ASSERT_EQ( 0, vv1.Compare( vv2 ) );
    ASSERT_TRUE( vv1 == vv1 );
    ASSERT_TRUE( vv1 == vv2 );
    ASSERT_TRUE( vv2 == vv1 );

    // now add another actor to vv2, so that it should compare as "greater than" vv1
    vv2.AddPair( actor2, 1 );
    ASSERT_TRUE( vv1 < vv2 );
    ASSERT_FALSE( vv1 == vv2 );

    // add the same actor/event to vv1, so they should be equal again
    vv1.AddPair( actor2, 1 );
    ASSERT_TRUE( vv1 == vv2 );
    ASSERT_FALSE( vv1 < vv2 );

    // now increment the event associated with actor1 in vv2, so that it is again greater than vv1
    vv2.AddPair( actor1, 2 );
    ASSERT_TRUE( vv1 < vv2 );

    // increment actor1 in vv1, so that the VVs are equal again
    vv1.AddPair( actor1, 2 );
    ASSERT_TRUE( vv1 == vv2 );

    // now increment the event associated with actor2 in vv2, so that it is again greater than vv1
    vv2.AddPair( actor2, 2 );
    ASSERT_TRUE( vv1 < vv2 );
}

TEST(VersionVector, ToBinaryValue_Empty)
{
    // expected binary format of "[]" is <<131,106>>
    const char* expectedBinaryValue = "\x83\x6a";
    const size_t expectedBinarySizeInBytes = 2;

    VersionVector vvEmpty;
    basho::bigset::Buffer vvBuff;
    ASSERT_TRUE( vvEmpty.ToBinaryValue( vvBuff ) );
    ASSERT_EQ( expectedBinarySizeInBytes, vvBuff.GetBytesUsed() );
    ASSERT_EQ( 0, ::memcmp( expectedBinaryValue, vvBuff.GetBuffer(), expectedBinarySizeInBytes ) );
}

TEST(VersionVector, ToBinaryValue_SinglePair)
{
    // test with a small (<255) event value for a single actor

    // expected binary format of "[{<<31,124,79,82,56,248,49,176>>,3}]"
    // is <<131,108,0,0,0,1,104,2,109,0,0,0,8,31,124,79,82,56,248,49,176,97,3,106>>
    const char* expectedBinaryValue = "\x83\x6c\x00\x00\x00\x01"
                                      "\x68\x02\x6d\x00\x00\x00\x08\x1f\x7c\x4f\x52\x38\xf8\x31\xb0\x61\x03"
                                      "\x6a";
    const size_t expectedBinarySizeInBytes = 24;

    char actorId[8];
    actorId[0] = (char)31, actorId[1] = (char)124, actorId[2] = (char)79, actorId[3] = (char)82;
    actorId[4] = (char)56, actorId[5] = (char)248, actorId[6] = (char)49, actorId[7] = (char)176;
    Slice actorIdSlice( actorId, sizeof actorId );
    Actor actor( actorIdSlice );

    VersionVector vv;
    vv.AddPair( actor, 3 );

    basho::bigset::Buffer vvBuff;
    ASSERT_TRUE( vv.ToBinaryValue( vvBuff ) );
    ASSERT_EQ( expectedBinarySizeInBytes, vvBuff.GetBytesUsed() );
    ASSERT_EQ( 0, ::memcmp( expectedBinaryValue, vvBuff.GetBuffer(), expectedBinarySizeInBytes ) );
}

TEST(VersionVector, ToBinaryValue_TwoPairs)
{
    // test with multiple actors and small (<255) event values

    // expected binary format of "[{<<33,240,159,213,154,205,244,148>>,2},{<<215,98,38,119,186,244,52,94>>,5}]"
    // is <<131,108,0,0,0,2,104,2,109,0,0,0,8,33,240,159,213,154,205,244,148,97,2,104,2,109,0,0,0,8,215,98,38,119,186,244,52,94,97,5,106>>
    const char* expectedBinaryValue = "\x83\x6c\x00\x00\x00\x02"
                                      "\x68\x02\x6d\x00\x00\x00\x08\x21\xf0\x9f\xd5\x9a\xcd\xf4\x94\x61\x02"
                                      "\x68\x02\x6d\x00\x00\x00\x08\xd7\x62\x26\x77\xba\xf4\x34\x5e\x61\x05"
                                      "\x6a";
    const size_t expectedBinarySizeInBytes = 41;

    char actorId[8];
    actorId[0] = (char)33, actorId[1] = (char)240, actorId[2] = (char)159, actorId[3] = (char)213;
    actorId[4] = (char)154, actorId[5] = (char)205, actorId[6] = (char)244, actorId[7] = (char)148;
    Slice actorIdSlice( actorId, sizeof actorId );
    Actor actor( actorIdSlice );

    VersionVector vv;
    vv.AddPair( actor, 2 );

    actorId[0] = (char)215, actorId[1] = (char)98, actorId[2] = (char)38, actorId[3] = (char)119;
    actorId[4] = (char)186, actorId[5] = (char)244, actorId[6] = (char)52, actorId[7] = (char)94;
    actor.SetId( actorIdSlice );
    vv.AddPair( actor, 5 );

    basho::bigset::Buffer vvBuff;
    ASSERT_TRUE( vv.ToBinaryValue( vvBuff ) );
    ASSERT_EQ( expectedBinarySizeInBytes, vvBuff.GetBytesUsed() );
    ASSERT_EQ( 0, ::memcmp( expectedBinaryValue, vvBuff.GetBuffer(), expectedBinarySizeInBytes ) );
}

TEST(VersionVector, ToBinaryValue_LargeEvent)
{
    // test with a large (>255 but fits in 32-bits) event value for a single actor

    // expected binary format of "[{<<31,124,79,82,56,248,49,176>>,3001}]"
    // is <<131,108,0,0,0,1,104,2,109,0,0,0,8,31,124,79,82,56,248,49,176,98,0,0,11,185,106>>
    const char* expectedBinaryValue = "\x83\x6c\x00\x00\x00\x01"
                                      "\x68\x02\x6d\x00\x00\x00\x08\x1f\x7c\x4f\x52\x38\xf8\x31\xb0\x62\x00\x00\x0b\xb9"
                                      "\x6a";
    const size_t expectedBinarySizeInBytes = 27;

    char actorId[8];
    actorId[0] = (char)31, actorId[1] = (char)124, actorId[2] = (char)79, actorId[3] = (char)82;
    actorId[4] = (char)56, actorId[5] = (char)248, actorId[6] = (char)49, actorId[7] = (char)176;
    Slice actorIdSlice( actorId, sizeof actorId );
    Actor actor( actorIdSlice );

    VersionVector vv;
    vv.AddPair( actor, 3001 );

    basho::bigset::Buffer vvBuff;
    ASSERT_TRUE( vv.ToBinaryValue( vvBuff ) );
    ASSERT_EQ( expectedBinarySizeInBytes, vvBuff.GetBytesUsed() );
    ASSERT_EQ( 0, ::memcmp( expectedBinaryValue, vvBuff.GetBuffer(), expectedBinarySizeInBytes ) );
}

TEST(VersionVector, ToBinaryValue_BignumEvent)
{
    // test with a bignum (too large to fit in 32-bits) event value for a single actor

    // expected binary format of "[{<<31,124,79,82,56,248,49,176>>,8078839158},{<<215,98,38,119,186,244,52,94>>,18446744073709551615}]"
    // is <<131,108,0,0,0,2,104,2,109,0,0,0,8,31,124,79,82,56,248,49,176,110,5,0,118,77,137,225,1,104,2,109,0,0,0,8,215,98,38,119,186,244,52,94,110,8,0,255,255,255,255,255,255,255,255,106>>
    const char* expectedBinaryValue = "\x83\x6c\x00\x00\x00\x02"
                                      "\x68\x02\x6d\x00\x00\x00\x08\x1f\x7c\x4f\x52\x38\xf8\x31\xb0\x6e\x05\x00\x76\x4d\x89\xe1\x01"
                                      "\x68\x02\x6d\x00\x00\x00\x08\xd7\x62\x26\x77\xba\xf4\x34\x5e\x6e\x08\x00\xff\xff\xff\xff\xff\xff\xff\xff"
                                      "\x6a";
    const size_t expectedBinarySizeInBytes = 56;

    char actorId[8];
    actorId[0] = (char)31, actorId[1] = (char)124, actorId[2] = (char)79, actorId[3] = (char)82;
    actorId[4] = (char)56, actorId[5] = (char)248, actorId[6] = (char)49, actorId[7] = (char)176;
    Slice actorIdSlice( actorId, sizeof actorId );
    Actor actor( actorIdSlice );

    VersionVector vv;
    vv.AddPair( actor, 8078839158 );

    actorId[0] = (char)215, actorId[1] = (char)98, actorId[2] = (char)38, actorId[3] = (char)119;
    actorId[4] = (char)186, actorId[5] = (char)244, actorId[6] = (char)52, actorId[7] = (char)94;
    actor.SetId( actorIdSlice );
    vv.AddPair( actor, 18446744073709551615ull );

    basho::bigset::Buffer vvBuff;
    ASSERT_TRUE( vv.ToBinaryValue( vvBuff ) );
    ASSERT_EQ( expectedBinarySizeInBytes, vvBuff.GetBytesUsed() );
    ASSERT_EQ( 0, ::memcmp( expectedBinaryValue, vvBuff.GetBuffer(), expectedBinarySizeInBytes ) );
}

///////////////////////////////////////////////////////////////////////////////
// DotCloud class unit tests

TEST(DotCloud, AddDot)
{
    Actor actor1, actor2;
    SetActorId( actor1, '1' );
    SetActorId( actor2, '2' );

    // add a couple entries to the dot cloud
    DotCloud dc;
    ASSERT_TRUE( dc.IsEmpty() );
    ASSERT_TRUE( dc.AddDot( actor1, 101 ) );
    ASSERT_EQ( 1, dc.Size() );
    ASSERT_FALSE( dc.IsEmpty() );
    ASSERT_TRUE( dc.AddDot( actor2, 202 ) );
    ASSERT_EQ( 2, dc.Size() );

    // ensure we can query the CounterSet values for the actors
    CounterSet events;
    ASSERT_TRUE( dc.ContainsActor( actor1, &events ) );
    ASSERT_EQ( 1, events.size() );
    ASSERT_EQ( 1, events.count( 101 ) );
    ASSERT_TRUE( dc.ContainsActor( actor2, &events ) );
    ASSERT_EQ( 1, events.size() );
    ASSERT_EQ( 1, events.count( 202 ) );

    // now re-add the same actor with the same event value (nothing should change)
    ASSERT_FALSE( dc.AddDot( actor1, 101 ) );
    ASSERT_EQ( 2, dc.Size() );
    ASSERT_TRUE( dc.ContainsActor( actor1, &events ) );
    ASSERT_EQ( 1, events.size() );
    ASSERT_EQ( 1, events.count( 101 ) );

    // now add a new dot to actor1
    ASSERT_TRUE( dc.AddDot( actor1, 102 ) );
    ASSERT_EQ( 2, dc.Size() );
    ASSERT_TRUE( dc.ContainsActor( actor1, &events ) );
    ASSERT_EQ( 2, events.size() );
    ASSERT_EQ( 1, events.count( 101 ) );
    ASSERT_EQ( 1, events.count( 102 ) );
}

TEST(DotCloud, AddDots)
{
    Actor actor1;
    SetActorId( actor1, '1' );

    CounterSet eventsIn;
    eventsIn.insert( 1 );
    eventsIn.insert( 3 );

    // add a couple dots to the dot cloud
    DotCloud dc;
    ASSERT_TRUE( dc.AddDots( actor1, eventsIn ) );
    ASSERT_EQ( 1, dc.Size() );

    // ensure we can query the CounterSet values for the actor
    CounterSet eventsOut;
    ASSERT_TRUE( dc.ContainsActor( actor1, &eventsOut ) );
    ASSERT_EQ( 2, eventsOut.size() );
    ASSERT_EQ( 1, eventsOut.count( 1 ) );
    ASSERT_EQ( 1, eventsOut.count( 3 ) );

    // try re-adding the same dots; should not change anything
    ASSERT_FALSE( dc.AddDots( actor1, eventsIn ) );
    ASSERT_EQ( 1, dc.Size() );
    ASSERT_TRUE( dc.ContainsActor( actor1, &eventsOut ) );
    ASSERT_EQ( 2, eventsOut.size() );
    ASSERT_EQ( 1, eventsOut.count( 1 ) );
    ASSERT_EQ( 1, eventsOut.count( 3 ) );

    // now add another event to eventsIn and re-add; should get the extra counter in the DC
    eventsIn.insert( 5 );
    ASSERT_TRUE( dc.AddDots( actor1, eventsIn ) );
    ASSERT_EQ( 1, dc.Size() );
    ASSERT_TRUE( dc.ContainsActor( actor1, &eventsOut ) );
    ASSERT_EQ( 3, eventsOut.size() );
    ASSERT_EQ( 1, eventsOut.count( 1 ) );
    ASSERT_EQ( 1, eventsOut.count( 3 ) );
    ASSERT_EQ( 1, eventsOut.count( 5 ) );
}

TEST(DotCloud, Merge_Idempotent)
{
    Actor actor1, actor2;
    SetActorId( actor1, '1' );
    SetActorId( actor2, '2' );

    CounterSet events1;
    events1.insert( 1 );
    events1.insert( 3 );

    CounterSet events2;
    events2.insert( 1 );
    events2.insert( 300 );
    events2.insert( 1001 );

    // add a couple dots to the dot cloud
    DotCloud dc;
    ASSERT_TRUE( dc.AddDots( actor1, events1 ) );
    ASSERT_TRUE( dc.AddDots( actor2, events2 ) );
    ASSERT_EQ( 2, dc.Size() );

    const DotCloud dcRef( dc );
    ASSERT_EQ( 2, dcRef.Size() );

    CounterSet events;
    ASSERT_TRUE( dcRef.ContainsActor( actor1 ) );
    ASSERT_TRUE( dcRef.ContainsActor( actor2, &events ) );
    ASSERT_EQ( 3, events.size() );
    ASSERT_EQ( 1, events.count( 1 ) );
    ASSERT_EQ( 1, events.count( 300 ) );
    ASSERT_EQ( 1, events.count( 1001 ) );

    // merging with self should result in, well, self
    ASSERT_TRUE( dc.Merge( dc ) );
    ASSERT_TRUE( dc == dcRef );
}

TEST(DotCloud, Merge_Identity)
{
    Actor actor1, actor2;
    SetActorId( actor1, '1' );
    SetActorId( actor2, '2' );

    CounterSet events1;
    events1.insert( 1 );
    events1.insert( 3 );

    CounterSet events2;
    events2.insert( 1 );
    events2.insert( 300 );
    events2.insert( 1001 );

    // add a couple dots to the dot cloud
    DotCloud dc;
    ASSERT_TRUE( dc.AddDots( actor1, events1 ) );
    ASSERT_TRUE( dc.AddDots( actor2, events2 ) );
    ASSERT_EQ( 2, dc.Size() );

    const DotCloud dcRef( dc );
    ASSERT_EQ( 2, dcRef.Size() );

    CounterSet events;
    ASSERT_TRUE( dcRef.ContainsActor( actor1 ) );
    ASSERT_TRUE( dcRef.ContainsActor( actor2, &events ) );
    ASSERT_EQ( 3, events.size() );
    ASSERT_EQ( 1, events.count( 1 ) );
    ASSERT_EQ( 1, events.count( 300 ) );
    ASSERT_EQ( 1, events.count( 1001 ) );

    // merging with an empty DC should result in self
    DotCloud dc2;
    ASSERT_TRUE( dc2.IsEmpty() );
    ASSERT_TRUE( dc.Merge( dc2 ) );
    ASSERT_TRUE( dc == dcRef );

    // starting empty and merging with a DC should result in the DC
    ASSERT_TRUE( dc2.IsEmpty() );
    ASSERT_TRUE( dc2.Merge( dc ) );
    ASSERT_TRUE( dc2 == dcRef );
    ASSERT_FALSE( dc2.IsEmpty() );
}

TEST(DotCloud, Merge_Concurrent)
{
    Actor actor1, actor2;
    SetActorId( actor1, '1' );
    SetActorId( actor2, '2' );

    CounterSet events1;
    events1.insert( 1 );
    events1.insert( 3 );

    CounterSet events2;
    events2.insert( 1 );
    events2.insert( 300 );
    events2.insert( 1001 );

    DotCloud dc1, dc2;
    ASSERT_TRUE( dc1.AddDots( actor1, events1 ) );
    ASSERT_TRUE( dc2.AddDots( actor2, events2 ) );
    ASSERT_TRUE( dc1 != dc2 );

    ASSERT_TRUE( dc1.Merge( dc2 ) );
    ASSERT_EQ( 2, dc1.Size() );
    ASSERT_EQ( 1, dc2.Size() );
    ASSERT_TRUE( dc2 < dc1 );

    ASSERT_TRUE( dc2.Merge( dc1 ) );
    ASSERT_EQ( 2, dc1.Size() );
    ASSERT_EQ( 2, dc2.Size() );
    ASSERT_TRUE( dc2 == dc1 );
}

TEST(DotCloud, Compare)
{
    Actor actor1, actor2;
    SetActorId( actor1, '1' );
    SetActorId( actor2, '2' );

    DotCloud dc1, dc2;
    dc1.AddDot( actor1, 1 );
    dc2.AddDot( actor1, 1 );

    // the two DCs should match since they contain the same actor/event
    ASSERT_EQ( 0, dc1.Compare( dc1 ) );
    ASSERT_EQ( 0, dc1.Compare( dc2 ) );
    ASSERT_TRUE( dc1 == dc1 );
    ASSERT_TRUE( dc1 == dc2 );
    ASSERT_TRUE( dc2 == dc1 );

    // now add another actor to dc2, so that it should compare as "greater than" dc1
    dc2.AddDot( actor2, 1 );
    ASSERT_TRUE( dc1 < dc2 );
    ASSERT_FALSE( dc1 == dc2 );

    // add the same dot to dc1, so they should be equal again
    dc1.AddDot( actor2, 1 );
    ASSERT_TRUE( dc1 == dc2 );
    ASSERT_FALSE( dc1 < dc2 );

    // now add another dot for actor1 in dc2, so that it is again greater than dc1
    dc2.AddDot( actor1, 2 );
    ASSERT_TRUE( dc1 < dc2 );

    // add the same dot to dc1, so that the DCs are equal again
    dc1.AddDot( actor1, 2 );
    ASSERT_TRUE( dc1 == dc2 );

    // now add a dot associated with actor2 in dc2, so that it is again greater than dc1
    dc2.AddDot( actor2, 2 );
    ASSERT_TRUE( dc1 < dc2 );

    // add the same dot to dc1, to make them equal again
    dc1.AddDot( actor2, 2 );
    ASSERT_TRUE( dc1 == dc2 );

    // now add a dot to both DCs, but this time the event value for dc1 < dc2
    dc1.AddDot( actor1, 3 );
    dc2.AddDot( actor1, 4 );
    ASSERT_TRUE( dc1 < dc2 );

    // add the same dot to dc1 that we added to dc2; now we will have dc2 < dc1 since dc1 will have more dots for actor1
    dc1.AddDot( actor1, 4 );
    ASSERT_TRUE( dc2 < dc1 );

    // finally, add the missing dot to dc2, so they are equal again
    dc2.AddDot( actor1, 3 );
    ASSERT_TRUE( dc1 == dc2 );
}

///////////////////////////////////////////////////////////////////////////////
// BigsetClock class unit tests

// Sample BigsetClock objects in Erlang's term_to_binary() format, along with
// the expected BigsetClock::ToString() format.
//
// Note that the BigsetClock class stores the version vectors in a map, which
// sorts the Actor IDs, so ToString() may not output the version vector in the
// same format as the original Erlang term string that was passed to
// term_to_binary() to generate the binary data. In this case, the original
// Erlang term string is commented out and an equivalent value for the expected
// term string is provided.
struct BigsetClockTermToBinarySample
{
    const char* m_ErlangBinary;
    const char* m_ErlangTerm;
};

static BigsetClockTermToBinarySample g_BigsetClockSamples[] =
{
   { "<<131,104,2,106,106>>",
     "{[],[]}" },

   { "<<131,104,2,108,0,0,0,1,104,2,109,0,0,0,8,31,124,79,82,56,248,49,176,97,3,106,106>>",
     "{[{<<31,124,79,82,56,248,49,176>>,3}],[]}" },

   { "<<131,104,2,108,0,0,0,2,104,2,109,0,0,0,8,215,98,38,119,186,244,52,94,97,5,104,2,109,0,0,0,8,33,240,159,213,154,205,244,148,97,2,106,108,0,0,0,1,104,2,109,0,0,0,8,98,73,199,224,34,168,136,100,107,0,1,2,106>>",
   //"{[{<<215,98,38,119,186,244,52,94>>,5},{<<33,240,159,213,154,205,244,148>>,2}],[{<<98,73,199,224,34,168,136,100>>,[2]}]}" },
     "{[{<<33,240,159,213,154,205,244,148>>,2},{<<215,98,38,119,186,244,52,94>>,5}],[{<<98,73,199,224,34,168,136,100>>,[2]}]}" },

   { "<<131,104,2,108,0,0,0,6,104,2,109,0,0,0,8,235,19,151,105,138,106,58,128,97,11,104,2,109,0,0,0,8,246,213,7,245,42,1,178,31,110,5,0,118,77,137,225,1,104,2,109,0,0,0,8,79,29,206,190,152,110,182,146,110,5,0,183,5,69,6,1,104,2,109,0,0,0,8,101,138,152,250,255,0,144,37,110,5,0,161,218,141,60,1,104,2,109,0,0,0,8,188,76,158,6,20,134,204,182,110,4,0,179,177,47,237,104,2,109,0,0,0,8,9,98,153,155,116,227,198,253,97,17,106,106>>",
   //"{[{<<235,19,151,105,138,106,58,128>>,11},{<<246,213,7,245,42,1,178,31>>,8078839158},{<<79,29,206,190,152,110,182,146>>,4400154039},{<<101,138,152,250,255,0,144,37>>,5310896801},{<<188,76,158,6,20,134,204,182>>,3979325875},{<<9,98,153,155,116,227,198,253>>,17}],[]}"},
     "{[{<<9,98,153,155,116,227,198,253>>,17},{<<79,29,206,190,152,110,182,146>>,4400154039},{<<101,138,152,250,255,0,144,37>>,5310896801},{<<188,76,158,6,20,134,204,182>>,3979325875},{<<235,19,151,105,138,106,58,128>>,11},{<<246,213,7,245,42,1,178,31>>,8078839158}],[]}"},

   { "<<131,104,2,108,0,0,0,1,104,2,109,0,0,0,8,64,11,204,186,190,121,172,124,97,8,106,108,0,0,0,1,104,2,109,0,0,0,8,144,59,206,183,122,144,158,142,108,0,0,0,86,97,4,97,5,97,6,97,7,97,8,97,10,97,13,97,14,97,15,97,16,97,17,97,18,97,19,97,20,97,21,97,22,97,24,97,25,97,27,97,28,97,29,97,30,97,31,97,33,97,34,97,35,97,36,97,37,97,38,97,39,97,40,97,41,98,7,61,44,119,98,49,114,145,213,98,58,83,213,40,98,80,85,242,151,98,104,236,249,191,98,108,46,157,91,98,108,248,159,179,98,114,50,136,205,98,116,68,165,79,98,117,60,155,23,98,123,107,47,236,98,124,74,229,255,98,127,143,166,185,110,4,0,85,169,205,136,110,4,0,218,145,102,189,110,4,0,194,80,112,196,110,4,0,179,172,125,217,110,4,0,56,30,129,236,110,4,0,137,135,59,242,110,4,0,211,26,83,246,110,5,0,58,91,214,3,1,110,5,0,152,202,173,29,1,110,5,0,199,194,161,31,1,110,5,0,100,205,115,49,1,110,5,0,148,77,6,61,1,110,5,0,139,250,65,78,1,110,5,0,65,240,51,93,1,110,5,0,120,62,165,101,1,110,5,0,112,38,19,108,1,110,5,0,127,115,23,113,1,110,5,0,43,84,225,118,1,110,5,0,129,29,102,128,1,110,5,0,15,8,42,130,1,110,5,0,31,115,248,130,1,110,5,0,201,173,234,132,1,110,5,0,168,109,12,138,1,110,5,0,40,234,14,154,1,110,5,0,232,151,75,166,1,110,5,0,236,76,178,179,1,110,5,0,244,124,38,191,1,110,5,0,243,93,69,197,1,110,5,0,252,63,195,197,1,110,5,0,32,156,218,197,1,110,5,0,225,42,74,207,1,110,5,0,43,211,169,213,1,110,5,0,248,137,220,226,1,110,5,0,219,49,48,1,2,110,5,0,67,105,200,11,2,110,5,0,141,146,189,51,2,110,5,0,199,206,132,63,2,110,5,0,170,78,122,74,2,110,5,0,188,165,133,77,2,110,5,0,178,31,231,80,2,110,5,0,219,81,205,81,2,106,106>>",
     "{[{<<64,11,204,186,190,121,172,124>>,8}],[{<<144,59,206,183,122,144,158,142>>,[4,5,6,7,8,10,13,14,15,16,17,18,19,20,21,22,24,25,27,28,29,30,31,33,34,35,36,37,38,39,40,41,121449591,829592021,978572584,1347809943,1760360895,1814994267,1828233139,1915914445,1950655823,1966906135,2070622188,2085283327,2140120761,2295179605,3177615834,3295695042,3648892083,3967884856,4063987593,4132641491,4359347002,4792896152,4825662151,5124640100,5318790548,5607914123,5858652225,6000295544,6108161648,6192329599,6289445931,6449143169,6478759951,6492287775,6524939721,6611037608,6879636008,7084939240,7309774060,7501937908,7604624883,7612874748,7614405664,7772711649,7879643947,8101071352,8609870299,8787618115,9457996429,9655602887,9839464106,9890538940,9947258802,9962344923]}]}"}
};

TEST(BigsetClock, ValueToBigsetClock)
{
    for ( int j = 0; j < COUNTOF( g_BigsetClockSamples ); ++j )
    {
        BigsetClockTermToBinarySample& sample( g_BigsetClockSamples[j] );

        ErlBuffer binaryBuff;
        int binaryByteCount = ParseErlangBinaryString( sample.m_ErlangBinary, binaryBuff );
        ASSERT_LT( 0, binaryByteCount );

        // now create a BigsetClock object from the byte stream
        Slice binaryValue( binaryBuff.GetCharBuffer(), (size_t)binaryByteCount );
        BigsetClock bigsetClock;
        std::string errStr;
        ASSERT_TRUE( BigsetClock::ValueToBigsetClock( binaryValue, bigsetClock, errStr ) );

        // get the string-version of the BigsetClock object and compare to the
        // Erlang tuple string from the sample
        std::string bigsetClockStr = bigsetClock.ToString();
        ASSERT_EQ( 0, strcmp( bigsetClockStr.c_str(), sample.m_ErlangTerm ) );
    }
}

static void CreateActor( Actor& Act, char ActorId )
{
    // create an Actor object from the specified ActorId byte
    char actorId[8];
    ::memset( actorId, ActorId, sizeof actorId );
    Slice actorIdSlice( actorId, sizeof actorId );
    Act.SetId( actorIdSlice );
}

static void BigsetClockAddToVersionVector( BigsetClock& Clock, char ActorId, Counter Event )
{
    Actor actor;
    CreateActor( actor, ActorId );
    Clock.AddToVersionVector( actor, Event );
}

static void BigsetClockAddToDotCloud( BigsetClock& Clock, char ActorId, CounterSet Events )
{
    Actor actor;
    CreateActor( actor, ActorId );
    Clock.AddToDotCloud( actor, Events );
}

// constructs one or more BigsetClock objects with well-known contents
static void CreateBigsetClocks( BigsetClock& Clock, BigsetClock* pClock2 = NULL, BigsetClock* pClock3 = NULL )
{
    // construct a BigsetClock with some stuff in its version vector and dot cloud
    // {[{a, 2}, {b, 9}, {z, 4}], [{a, [7]}, {c, [99]}]}
    Clock.Clear();
    BigsetClockAddToVersionVector( Clock, 'a', 2 );
    BigsetClockAddToVersionVector( Clock, 'b', 9 );
    BigsetClockAddToVersionVector( Clock, 'z', 4 );

    CounterSet events;
    events.insert( 7 );
    BigsetClockAddToDotCloud( Clock, 'a', events );

    events.clear();
    events.insert( 99 );
    BigsetClockAddToDotCloud( Clock, 'c', events );

    if ( pClock2 != NULL )
    {
        // construct a second BigsetClock with stuff in its dot cloud but not its VV
        // {[], [{a, [3, 4, 5, 6]}, {d, [2]}, {z, [6]}]}
        pClock2->Clear();
        events.clear();
        events.insert( 3 );
        events.insert( 4 );
        events.insert( 5 );
        events.insert( 6 );
        BigsetClockAddToDotCloud( *pClock2, 'a', events );

        events.clear();
        events.insert( 2 );
        BigsetClockAddToDotCloud( *pClock2, 'd', events );

        events.clear();
        events.insert( 6 );
        BigsetClockAddToDotCloud( *pClock2, 'z', events );
    }

    if ( pClock3 != NULL )
    {
        // construct a third BigsetClock, which is interesting to merge with 1 and 2
        // {[{a, 5}, {c, 100}, {d, 1}], [{d, [3]}, {z, [5]}]}
        pClock3->Clear();
        BigsetClockAddToVersionVector( *pClock3, 'a', 5 );
        BigsetClockAddToVersionVector( *pClock3, 'c', 100 );
        BigsetClockAddToVersionVector( *pClock3, 'd', 1 );

        events.clear();
        events.insert( 3 );
        BigsetClockAddToDotCloud( *pClock3, 'd', events );

        events.clear();
        events.insert( 5 );
        BigsetClockAddToDotCloud( *pClock3, 'z', events );
    }
}

TEST(BigsetClock, Merge_Idempotent)
{
    // construct a BigsetClock with some stuff in its version vector and dot cloud
    BigsetClock clock1;
    CreateBigsetClocks( clock1 );

    // save a reference copy of clock1
    const BigsetClock clock1Ref( clock1 );

    // merging a clock with itself should produce, well, the clock itself
    ASSERT_TRUE( clock1.Merge( clock1 ) );
    ASSERT_TRUE( clock1 == clock1 );
    ASSERT_TRUE( clock1 == clock1Ref );
}

TEST(BigsetClock, Merge_Identity)
{
    // construct a BigsetClock with some stuff in its version vector and dot cloud
    BigsetClock clock1;
    CreateBigsetClocks( clock1 );

    // save a reference copy of clock1
    const BigsetClock clock1Ref( clock1 );

    // merging a clock with an empty clock should produce the original clock
    BigsetClock clock2;
    ASSERT_TRUE( clock2.IsEmpty() );

    ASSERT_TRUE( clock1.Merge( clock2 ) );
    ASSERT_TRUE( clock1 == clock1Ref );

    // starting empty and merging with a clock should result in the clock
    ASSERT_TRUE( clock2.IsEmpty() );
    ASSERT_TRUE( clock2.Merge( clock1 ) );
    ASSERT_TRUE( clock2 == clock1Ref );
    ASSERT_FALSE( clock2.IsEmpty() );
}

TEST(BigsetClock, Merge_Commutative)
{
    BigsetClock clock1, clock2;
    CreateBigsetClocks( clock1, &clock2 );

    // save reference copies of the clocks
    const BigsetClock clock1Ref( clock1 ), clock2Ref( clock2 );

    // merging clock2 into clock1 should produce the same result as merging clock1 into clock2
    ASSERT_TRUE( clock1.Merge( clock2 ) );
    BigsetClock clock3( clock1 );
    clock1 = clock1Ref;

    ASSERT_TRUE( clock2.Merge( clock1 ) );
    ASSERT_TRUE( clock2 == clock3 );
}

TEST(BigsetClock, Merge_Associative)
{
    BigsetClock clock1, clock2, clock3;
    CreateBigsetClocks( clock1, &clock2, &clock3 );

    // save reference copies of the clocks
    const BigsetClock clock1Ref( clock1 ), clock2Ref( clock2 ), clock3Ref( clock3 );

    // create ((1 Merge 2) Merge 3) and compare to (1 Merge (2 Merge 3))

    // merging clock2 into clock1 should produce the same result as merging clock1 into clock2
    ASSERT_TRUE( clock1.Merge( clock2 ) );
    ASSERT_TRUE( clock1.Merge( clock3 ) );
    BigsetClock clock123Merged( clock1 );
    clock1 = clock1Ref;

    ASSERT_TRUE( clock2.Merge( clock3 ) );
    ASSERT_TRUE( clock2.Merge( clock1 ) );
    BigsetClock clock231Merged( clock2 );

    ASSERT_TRUE( clock123Merged == clock231Merged );
}

TEST(BigsetClock, Merge_Concurrent)
{
    // construct the clocks that we merge together
    BigsetClock clock1; // {[{a, 2}, {b, 9}, {z, 4}], [{a, [7]}, {c, [99]}]}
    BigsetClock clock2; // {[], [{a, [3, 4, 5, 6]}, {d, [2]}, {z, [6]}]}
    BigsetClock clock3; // {[{a, 5}, {c, 100}, {d, 1}], [{d, [3]}, {z, [5]}]}
    CreateBigsetClocks( clock1, &clock2, &clock3 );

    // save reference versions of these clocks
    const BigsetClock clock1Ref( clock1 ), clock2Ref( clock2 ), clock3Ref( clock3 );

    // construct the expected result of merging clock1 with clock2
    BigsetClock clock12Merged; // {[{a, 7}, {b, 9}, {z, 4}], [{c, [99]}, {d, [2]}, {z, [6]}]}
    BigsetClockAddToVersionVector( clock12Merged, 'a', 7 );
    BigsetClockAddToVersionVector( clock12Merged, 'b', 9 );
    BigsetClockAddToVersionVector( clock12Merged, 'z', 4 );

    CounterSet events;
    events.insert( 99 );
    BigsetClockAddToDotCloud( clock12Merged, 'c', events );

    events.clear();
    events.insert( 2 );
    BigsetClockAddToDotCloud( clock12Merged, 'd', events );

    events.clear();
    events.insert( 6 );
    BigsetClockAddToDotCloud( clock12Merged, 'z', events );

    // now try merging clock2 into clock1
    ASSERT_TRUE( clock1.Merge( clock2 ) );
    ASSERT_TRUE( clock1 == clock12Merged );

    // reset clock1 and try merging the other direction
    clock1 = clock1Ref;
    ASSERT_TRUE( clock1 == clock1Ref );
    ASSERT_TRUE( clock2 == clock2Ref );
    ASSERT_TRUE( clock1 != clock12Merged ); // just for good measure

    ASSERT_TRUE( clock2.Merge( clock1 ) );
    ASSERT_TRUE( clock2 == clock12Merged );

    // construct the expected result of merging clock12Merged with clock3
    BigsetClock clock123Merged; // {[{a, 7}, {b, 9}, {c, 100}, {d, 3}, {z, 6}], []}
    BigsetClockAddToVersionVector( clock123Merged, 'a', 7 );
    BigsetClockAddToVersionVector( clock123Merged, 'b', 9 );
    BigsetClockAddToVersionVector( clock123Merged, 'c', 100 );
    BigsetClockAddToVersionVector( clock123Merged, 'd', 3 );
    BigsetClockAddToVersionVector( clock123Merged, 'z', 6 );

    // now merge clock12Merged into clock3
    ASSERT_TRUE( clock3.Merge( clock12Merged ) );
    ASSERT_TRUE( clock3 == clock123Merged );
}

static void AddToDotList( DotList& Dots, char ActorId, Counter Event )
{
    // create an Actor object from the specified ActorId byte
    char actorId[8];
    memset( actorId, ActorId, sizeof actorId );
    Slice actorIdSlice( actorId, sizeof actorId );
    Actor actor( actorIdSlice );
    Dots.AddPair( actor, Event );
}

TEST(BigsetClock, IsSeen)
{
    // create a BigsetClock with a known state and query whether or not
    // it has seen various actor/event pairs
    BigsetClock clock; // {[{a, 2}, {b, 9}, {z, 4}], [{a, [7]}, {c, [99]}]}
    CreateBigsetClocks( clock );

    // create the various actors we want to query
    Actor actorA, actorC, actorX, actorZ;
    CreateActor( actorA, 'a' );
    CreateActor( actorC, 'c' );
    CreateActor( actorX, 'x' );
    CreateActor( actorZ, 'z' );

    ASSERT_TRUE( clock.IsSeen( actorA, 1 ) );
    ASSERT_TRUE( clock.IsSeen( actorZ, 4 ) );
    ASSERT_TRUE( clock.IsSeen( actorC, 99 ) );

    ASSERT_FALSE( clock.IsSeen( actorA, 5 ) );
    ASSERT_FALSE( clock.IsSeen( actorX, 1 ) );
    ASSERT_FALSE( clock.IsSeen( actorC, 1 ) );
}

TEST(BigsetClock, SubtractSeen)
{
    // create a simple DotList with a couple entries
    DotList dotList1;
    AddToDotList( dotList1, 'a', 2 );
    AddToDotList( dotList1, 'b', 7 );

    // save a reference copy of dotList1
    const DotList dotList1Ref( dotList1 );

    // update the DotList by subtracting the events seen by an empty
    // BigsetClock, which shouldn't change the DotList (since none were seen)
    BigsetClock emptyClock;
    ASSERT_EQ( 2, dotList1.Size() );
    emptyClock.SubtractSeen( dotList1 );
    ASSERT_TRUE( dotList1 == dotList1Ref );

    // try again with a BigsetClock that has seen all the events in the DotList
    // (expect the DotList to be emptied in this case)
    BigsetClock clock; // {[{a, 2}, {b, 9}, {z, 4}], [{a, [7]}, {c, [99]}]}
    CreateBigsetClocks( clock );
    clock.SubtractSeen( dotList1 );
    ASSERT_TRUE( dotList1.IsEmpty() );

    // try again where the DotList has an actor present in the BigsetClock, but
    // the associated event has not been seen by the clock
    DotList dotList2( dotList1Ref );
    AddToDotList( dotList2, 'z', 5 );
    clock.SubtractSeen( dotList2 );
    ASSERT_FALSE( dotList2.IsEmpty() );

    // construct the DotList we expect to be remaining for dotList2
    DotList dotList2Remaining;
    AddToDotList( dotList2Remaining, 'z', 5 );
    ASSERT_TRUE( dotList2 == dotList2Remaining );

    // try again where the dot is seen in the clock's cloud rather than its VV
    DotList dotList3;
    AddToDotList( dotList3, 'q', 1 ); // we expect only this left after calling SubtractSeen()
    DotList dotList3Remaining( dotList3 );

    AddToDotList( dotList3, 'c', 99 ); // this should be removed by SubtractSeen()
    clock.SubtractSeen( dotList3 );
    ASSERT_TRUE( dotList3 == dotList3Remaining );
}
