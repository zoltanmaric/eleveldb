//
// Created by Paul A. Place on 1/22/16.
//

#include <BigsetClock.h>
#include <gtest/gtest.h>

using namespace basho::bigset;

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