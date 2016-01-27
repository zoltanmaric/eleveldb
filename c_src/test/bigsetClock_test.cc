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

    Slice buffSlice( buff, sizeof buff );
    actor.SetId( buffSlice );
    actorStr = actor.ToString();
    ASSERT_EQ( 0, ::strcmp( actorStr.c_str(), expectedActorStr.c_str() ) );
}
