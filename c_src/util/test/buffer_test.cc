//
// Created by Paul A. Place on 1/11/16.
//

#include <buffer.h>
#include <gtest/gtest.h>

using namespace basho::utils;

TEST(Buffer, Ctor)
{
    const size_t BUILTIN_BUFF_SIZE = 100;

    // default ctor
    Buffer<BUILTIN_BUFF_SIZE> buff1;
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff1.GetBuffSize() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff1.GetBuiltinBuffSize() );

    // ctor parameter specifies size smaller than builtin buffer (will use builtin buffer)
    const size_t SMALLER_BUFF_SIZE = BUILTIN_BUFF_SIZE - 1;
    Buffer<BUILTIN_BUFF_SIZE> buff2( SMALLER_BUFF_SIZE );
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff2.GetBuffSize() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff2.GetBuiltinBuffSize() );

    // ctor parameter specifies size larger than builtin buffer (will use allocated buffer)
    const size_t LARGER_BUFF_SIZE = BUILTIN_BUFF_SIZE + 1;
    Buffer<BUILTIN_BUFF_SIZE> buff3( LARGER_BUFF_SIZE );
    ASSERT_EQ( LARGER_BUFF_SIZE,  buff3.GetBuffSize() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff3.GetBuiltinBuffSize() );
}

TEST(Buffer, EnsureSize)
{
    const size_t BUILTIN_BUFF_SIZE = 100;

    // default ctor
    Buffer<BUILTIN_BUFF_SIZE> buff;
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff.GetBuffSize() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff.GetBuiltinBuffSize() );

    // try Buffer::EnsureSize() with a value smaller than BUILTIN_BUFF_SIZE (buffer size should not change)
    ASSERT_TRUE( buff.EnsureSize( BUILTIN_BUFF_SIZE - 1 ) );
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff.GetBuffSize() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff.GetBuiltinBuffSize() );

    // try Buffer::EnsureSize() with a value larger than BUILTIN_BUFF_SIZE (buffer size should change)
    ASSERT_TRUE( buff.EnsureSize( BUILTIN_BUFF_SIZE + 1 ) );
    ASSERT_EQ( BUILTIN_BUFF_SIZE + 1, buff.GetBuffSize() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE,     buff.GetBuiltinBuffSize() );

    // ensure we get the expected value returned in the pRealloc output parameter
    bool realloc = true;
    ASSERT_TRUE( buff.EnsureSize( BUILTIN_BUFF_SIZE + 1, &realloc ) ); // BuffSize is already BUILTIN_BUFF_SIZE + 1, so no realloc
    ASSERT_FALSE( realloc );

    ASSERT_TRUE( buff.EnsureSize( BUILTIN_BUFF_SIZE + 2, &realloc ) );
    ASSERT_TRUE( realloc );
}

TEST(Buffer, ContentTest)
{
    // create a buffer and fill it with some stuff, then realloc and add some more stuff
    const size_t BUILTIN_BUFF_SIZE = 100;
    Buffer<BUILTIN_BUFF_SIZE> buff;

    char* pBuff = buff.GetCharBuffer();
    const size_t originalBuffSize = BUILTIN_BUFF_SIZE;
    size_t j;
    for ( j = 0; j < originalBuffSize; ++j )
    {
        pBuff[j] = (char)j;
    }

    // ensure all is good
    for ( j = 0; j < originalBuffSize; ++j )
    {
        if ( pBuff[j] != (char)j ) break;
    }
    ASSERT_EQ( originalBuffSize, j );

    // call Buffer::EnsureSize() with a smaller value; the contents should be unchanged
    bool realloc = true;
    ASSERT_TRUE( buff.EnsureSize( BUILTIN_BUFF_SIZE - 1, &realloc ) );
    ASSERT_FALSE( realloc );

    for ( j = 0; j < originalBuffSize; ++j )
    {
        if ( pBuff[j] != (char)j ) break;
    }
    ASSERT_EQ( originalBuffSize, j );

    // now enlarge the buffer and ensure the original contents are intact
    const size_t newBuffSize = 2 * BUILTIN_BUFF_SIZE;
    ASSERT_TRUE( buff.EnsureSize( newBuffSize, &realloc ) );
    ASSERT_TRUE( realloc );
    ASSERT_EQ( newBuffSize, buff.GetBuffSize() );
    ASSERT_NE( pBuff, buff.GetCharBuffer() );

    pBuff = buff.GetCharBuffer();
    for ( j = 0; j < originalBuffSize; ++j )
    {
        if ( pBuff[j] != (char)j ) break;
    }
    ASSERT_EQ( originalBuffSize, j );

    // fill in the additional buffer space
    for ( j = originalBuffSize; j < newBuffSize; ++j )
    {
        pBuff[j] = (char)j;
    }

    // now enlarge the buffer again and ensure the previous contents are intact
    const size_t newerBuffSize = 2 * newBuffSize;
    ASSERT_TRUE( buff.EnsureSize( newerBuffSize, &realloc ) );
    ASSERT_TRUE( realloc );
    ASSERT_EQ( newerBuffSize, buff.GetBuffSize() );
    ASSERT_NE( pBuff, buff.GetCharBuffer() );

    pBuff = buff.GetCharBuffer();
    for ( j = 0; j < newBuffSize; ++j )
    {
        if ( pBuff[j] != (char)j ) break;
    }
    ASSERT_EQ( newBuffSize, j );
}

TEST(Buffer, BytesUsedTest)
{
    const size_t BUILTIN_BUFF_SIZE = 100;

    // the buffer starts out empty
    Buffer<BUILTIN_BUFF_SIZE> buff;
    ASSERT_TRUE( buff.IsEmpty() );
    ASSERT_EQ( 0, buff.GetBytesUsed() );

    // set the "bytes used" value and ensure we get it back
    ASSERT_TRUE( buff.SetBytesUsed( 1 ) );
    ASSERT_FALSE( buff.IsEmpty() );
    ASSERT_EQ( 1, buff.GetBytesUsed() );

    // set the "bytes used" value to the maximum possible
    ASSERT_TRUE( buff.SetBytesUsed( BUILTIN_BUFF_SIZE ) );
    ASSERT_FALSE( buff.IsEmpty() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff.GetBytesUsed() );

    // ensure we can lower the "bytes used" value
    ASSERT_TRUE( buff.SetBytesUsed( BUILTIN_BUFF_SIZE - 1 ) );
    ASSERT_FALSE( buff.IsEmpty() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE - 1, buff.GetBytesUsed() );

    // try setting the "bytes used" value to something larger than the
    // current buffer size; bytes used should remain unchanged
    ASSERT_FALSE( buff.SetBytesUsed( BUILTIN_BUFF_SIZE + 1 ) );
    ASSERT_FALSE( buff.IsEmpty() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE - 1, buff.GetBytesUsed() );

    // enlarge the buffer and ensure we can set "bytes used" to the larger value
    ASSERT_TRUE( buff.EnsureSize( BUILTIN_BUFF_SIZE + 1 ) );
    ASSERT_TRUE( buff.SetBytesUsed( BUILTIN_BUFF_SIZE + 1 ) );
    ASSERT_FALSE( buff.IsEmpty() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE + 1, buff.GetBytesUsed() );
}

TEST(Buffer, AssignTest)
{
    // set up some data we can assign to the buffer
    std::string dataStr( "This is some data in a string. Let's put a bit more stuff in here." );

    char dataBuff[ 1000 ];
    int j;
    for ( j = 0; j < sizeof dataBuff; ++j )
    {
        dataBuff[j] = (char)j;
    }

    // now create a Buffer, assign some data to it, and check the contents
    Buffer<100> buff;

    ASSERT_TRUE( buff.Assign( dataStr.c_str(), dataStr.size() + 1 ) ); // +1 to get NULL-term char
    ASSERT_EQ( dataStr.size() + 1, buff.GetBytesUsed() );
    ASSERT_EQ( 0, ::strcmp( buff.GetCharBuffer(), dataStr.c_str() ) );

    // first try a slice the same size as the string
    basho::utils::Slice dataSlice1( dataBuff, dataStr.size() + 1 );
    ASSERT_TRUE( buff.Assign( dataSlice1 ) );
    ASSERT_EQ( dataStr.size() + 1, buff.GetBytesUsed() );
    const char* pBuff = buff.GetCharBuffer();
    for ( j = 0; j < buff.GetBytesUsed(); ++j )
    {
        ASSERT_EQ( (char)j, pBuff[j] );
    }

    // now try the full buffer
    basho::utils::Slice dataSlice2( dataBuff, sizeof dataBuff );
    ASSERT_TRUE( buff.Assign( dataSlice2 ) );
    ASSERT_EQ( sizeof dataBuff, buff.GetBytesUsed() );
    pBuff = buff.GetCharBuffer();
    for ( j = 0; j < buff.GetBytesUsed(); ++j )
    {
        ASSERT_EQ( (char)j, pBuff[j] );
    }
}
