// -------------------------------------------------------------------
//
// buffer_test.cc: unit tests for the Buffer template class in the Basho C/C++ utility library
//
// Copyright (c) 2016 Basho Technologies, Inc. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------

#include <utils.h>
#include <gtest/gtest.h>

using namespace basho::utils;

TEST(Buffer, Ctor)
{
    const size_t BUILTIN_BUFF_SIZE = 100;

    // default ctor
    Buffer<BUILTIN_BUFF_SIZE> buff1;
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff1.GetBuffSize() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff1.GetBuiltinBuffSize() );

    // ctor parameter specifies size smaller than built-in buffer (will use built-in buffer)
    const size_t SMALLER_BUFF_SIZE = BUILTIN_BUFF_SIZE - 1;
    Buffer<BUILTIN_BUFF_SIZE> buff2( SMALLER_BUFF_SIZE );
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff2.GetBuffSize() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff2.GetBuiltinBuffSize() );

    // ctor parameter specifies size larger than built-in buffer (will use allocated buffer)
    const size_t LARGER_BUFF_SIZE = BUILTIN_BUFF_SIZE + 1;
    Buffer<BUILTIN_BUFF_SIZE> buff3( LARGER_BUFF_SIZE );
    ASSERT_EQ( LARGER_BUFF_SIZE,  buff3.GetBuffSize() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff3.GetBuiltinBuffSize() );
}

TEST(Buffer, CopyCtor)
{
    std::string smallStr( "abc" );
    std::string largeStr( "This is a much longer string that smallStr, but it isn't really that long, now is it?" );

    Buffer<10> buffUsesBuiltIn;
    buffUsesBuiltIn.Assign( smallStr.c_str(), smallStr.size() + 1 );

    Buffer<10> buffUsesHeap;
    buffUsesHeap.Assign( largeStr.c_str(), largeStr.size() + 1 );

    // now use the copy ctor, one which will use the built-in buffer, one that uses the heap
    Buffer<10> copyBuiltIn( buffUsesBuiltIn );
    ASSERT_EQ( 10, copyBuiltIn.GetBuffSize() );
    ASSERT_EQ( smallStr.size() + 1, copyBuiltIn.GetBytesUsed() );
    ASSERT_EQ( 0, ::strcmp( smallStr.c_str(), copyBuiltIn.GetCharBuffer() ) );

    Buffer<10> copyHeap( buffUsesHeap );
    ASSERT_LT( 10, copyHeap.GetBuffSize() );
    ASSERT_EQ( largeStr.size() + 1, copyHeap.GetBytesUsed() );
    ASSERT_EQ( 0, ::strcmp( largeStr.c_str(), copyHeap.GetCharBuffer() ) );
}


TEST(Buffer, AssignmentOperator)
{
    std::string smallStr( "abc" );
    std::string largeStr( "This is a much longer string that smallStr, but it isn't really that long, now is it?" );

    Buffer<10> buffUsesBuiltIn;
    buffUsesBuiltIn.Assign( smallStr.c_str(), smallStr.size() + 1 );

    Buffer<10> buffUsesHeap;
    buffUsesHeap.Assign( largeStr.c_str(), largeStr.size() + 1 );

    // now use the assignment operator a few times
    Buffer<10> target;
    target = buffUsesBuiltIn;
    ASSERT_EQ( 10, target.GetBuffSize() );
    ASSERT_EQ( smallStr.size() + 1, target.GetBytesUsed() );
    ASSERT_EQ( 0, ::strcmp( smallStr.c_str(), target.GetCharBuffer() ) );

    target = buffUsesHeap;
    ASSERT_LT( 10, target.GetBuffSize() );
    ASSERT_EQ( largeStr.size() + 1, target.GetBytesUsed() );
    ASSERT_EQ( 0, ::strcmp( largeStr.c_str(), target.GetCharBuffer() ) );

    target = buffUsesBuiltIn; // this will not shrink the buffer for target
    ASSERT_EQ( smallStr.size() + 1, target.GetBytesUsed() );
    ASSERT_EQ( 0, ::strcmp( smallStr.c_str(), target.GetCharBuffer() ) );

    // ensure that assigning target to itself doesn't do anything bad
    Buffer<10>& buffRef( target );
    buffRef = target;
    ASSERT_EQ( smallStr.size() + 1, target.GetBytesUsed() );
    ASSERT_EQ( 0, ::strcmp( smallStr.c_str(), target.GetCharBuffer() ) );
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

    // try Buffer::EnsureSize() with a value larger than BUILTIN_BUFF_SIZE (buffer size should increase)
    ASSERT_TRUE( buff.EnsureSize( BUILTIN_BUFF_SIZE + 1 ) );
    ASSERT_LT( BUILTIN_BUFF_SIZE, buff.GetBuffSize() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff.GetBuiltinBuffSize() );

    // ensure we get the expected value returned in the pRealloc output parameter
    bool realloc = true;
    ASSERT_TRUE( buff.EnsureSize( BUILTIN_BUFF_SIZE + 1, &realloc ) ); // BuffSize is already at least BUILTIN_BUFF_SIZE + 1, so no realloc
    ASSERT_FALSE( realloc );

    ASSERT_TRUE( buff.EnsureSize( buff.GetBuffSize() + 1, &realloc ) );
    ASSERT_TRUE( realloc );
}

TEST(Buffer, ReallocGranularity)
{
    // with a small (less than 1024) built-in buffer, the realloc granularity matches the size of the built-in buffer
    const size_t BUILTIN_BUFF_SIZE_SMALL = 10;

    Buffer<BUILTIN_BUFF_SIZE_SMALL> buffSmall;
    ASSERT_EQ( BUILTIN_BUFF_SIZE_SMALL, buffSmall.GetBuffSize() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE_SMALL, buffSmall.GetBuiltinBuffSize() );
    ASSERT_TRUE( buffSmall.EnsureSize( buffSmall.GetBuffSize() + 1 ) );
    ASSERT_EQ( BUILTIN_BUFF_SIZE_SMALL * 2, buffSmall.GetBuffSize() );
    ASSERT_TRUE( buffSmall.EnsureSize( buffSmall.GetBuffSize() + 1 ) );
    ASSERT_EQ( BUILTIN_BUFF_SIZE_SMALL * 3, buffSmall.GetBuffSize() );

    // ensure that a larger realloc works as expected
    size_t largeRealloc = buffSmall.GetBuffSize() + 100;
    ASSERT_TRUE( buffSmall.EnsureSize( largeRealloc ) );
    ASSERT_EQ( largeRealloc, buffSmall.GetBuffSize() );

    // with a large (greater than 1024) built-in buffer, the realloc granularity maxes out at 1024
    const size_t BUILTIN_BUFF_SIZE_LARGE = 2048;

    Buffer<BUILTIN_BUFF_SIZE_LARGE> buffLarge;
    ASSERT_EQ( BUILTIN_BUFF_SIZE_LARGE, buffLarge.GetBuffSize() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE_LARGE, buffLarge.GetBuiltinBuffSize() );
    ASSERT_TRUE( buffLarge.EnsureSize( buffLarge.GetBuffSize() + 1 ) );
    ASSERT_EQ( BUILTIN_BUFF_SIZE_LARGE + 1024, buffLarge.GetBuffSize() );
    ASSERT_TRUE( buffLarge.EnsureSize( buffLarge.GetBuffSize() + 1 ) );
    ASSERT_EQ( BUILTIN_BUFF_SIZE_LARGE + (2*1024), buffLarge.GetBuffSize() );

    // ensure that a larger realloc works as expected
    largeRealloc = buffLarge.GetBuffSize() + 4096;
    ASSERT_TRUE( buffLarge.EnsureSize( largeRealloc ) );
    ASSERT_EQ( largeRealloc, buffLarge.GetBuffSize() );
}

TEST(Buffer, Contents)
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

TEST(Buffer, BytesUsed)
{
    const size_t BUILTIN_BUFF_SIZE = 100;

    // the buffer starts out empty
    Buffer<BUILTIN_BUFF_SIZE> buff;
    ASSERT_TRUE( buff.IsEmpty() );
    ASSERT_EQ( 0, buff.GetBytesUsed() );

    // set the "bytes used" value and ensure we get it back
    buff.SetBytesUsed( 1 );
    ASSERT_FALSE( buff.IsEmpty() );
    ASSERT_EQ( 1, buff.GetBytesUsed() );

    // set the "bytes used" value to the maximum possible
    buff.SetBytesUsed( BUILTIN_BUFF_SIZE );
    ASSERT_FALSE( buff.IsEmpty() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE, buff.GetBytesUsed() );

    // ensure we can lower the "bytes used" value
    buff.SetBytesUsed( BUILTIN_BUFF_SIZE - 1 );
    ASSERT_FALSE( buff.IsEmpty() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE - 1, buff.GetBytesUsed() );

    // enlarge the buffer and ensure we can set "bytes used" to the larger value
    ASSERT_TRUE( buff.EnsureSize( BUILTIN_BUFF_SIZE + 1 ) );
    buff.SetBytesUsed( BUILTIN_BUFF_SIZE + 1 );
    ASSERT_FALSE( buff.IsEmpty() );
    ASSERT_EQ( BUILTIN_BUFF_SIZE + 1, buff.GetBytesUsed() );
}

TEST(Buffer, BytesUsedError)
{
    const size_t BUILTIN_BUFF_SIZE = 100;
    Buffer<BUILTIN_BUFF_SIZE> buff;

    // try setting the "bytes used" value to something larger than the
    // current buffer size
    ASSERT_THROW( buff.SetBytesUsed( BUILTIN_BUFF_SIZE + 1 ), std::logic_error );
}

TEST(Buffer, Assign)
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
    Slice dataSlice1( dataBuff, dataStr.size() + 1 );
    ASSERT_TRUE( buff.Assign( dataSlice1 ) );
    ASSERT_EQ( dataStr.size() + 1, buff.GetBytesUsed() );
    const char* pBuff = buff.GetCharBuffer();
    for ( j = 0; j < buff.GetBytesUsed(); ++j )
    {
        ASSERT_EQ( (char)j, pBuff[j] );
    }

    // now try the full buffer
    Slice dataSlice2( dataBuff, sizeof dataBuff );
    ASSERT_TRUE( buff.Assign( dataSlice2 ) );
    ASSERT_EQ( sizeof dataBuff, buff.GetBytesUsed() );
    pBuff = buff.GetCharBuffer();
    for ( j = 0; j < buff.GetBytesUsed(); ++j )
    {
        ASSERT_EQ( (char)j, pBuff[j] );
    }
}

TEST(Buffer, AssignAndTransfer)
{
    // test the overload of Buffer::Assign() that (optionally) transfers
    // ownership of the allocated buffer

    // set up some data we can assign to the buffers
    std::string dataStr( "This is some data in a string. Let's put a bit more stuff in here." );

    ///////////////////////////////////
    // first we call Buffer::Assign() where we're using the built-in buffer
    Buffer<10> buff1, buff2;

    size_t bytesToAssign = buff1.GetBuiltinBuffSize();
    ASSERT_TRUE( bytesToAssign < dataStr.size() );
    ASSERT_TRUE( buff1.Assign( dataStr.c_str(), bytesToAssign ) );
    ASSERT_EQ( 0, ::memcmp( buff1.GetCharBuffer(), dataStr.c_str(), bytesToAssign ) );
    ASSERT_EQ( bytesToAssign, buff1.GetBytesUsed() );

    void* pBuff1Builtin = buff1.GetBuffer(); // this should be the built-in buffer for buff1

    // first we assign buff1 to buff2 without transferring ownership
    ASSERT_TRUE( buff2.Assign( buff1, false ) );
    ASSERT_EQ( 0, ::memcmp( buff2.GetCharBuffer(), dataStr.c_str(), bytesToAssign ) );
    ASSERT_EQ( bytesToAssign, buff2.GetBytesUsed() );
    ASSERT_TRUE( pBuff1Builtin != buff2.GetBuffer() ); // ensure we didn't transfer the built-in buffer

    // buff1 should still contain the original data
    ASSERT_EQ( 0, ::memcmp( buff1.GetCharBuffer(), dataStr.c_str(), bytesToAssign ) );
    ASSERT_EQ( bytesToAssign, buff1.GetBytesUsed() );
    ASSERT_TRUE( pBuff1Builtin == buff1.GetBuffer() ); // ensure our built-in buffer is, well, still built-in

    // now assign buff1 to buff2 and transfer ownership
    buff2.ResetBuffer();
    ASSERT_TRUE( buff2.Assign( buff1, true ) );
    ASSERT_EQ( 0, ::memcmp( buff2.GetCharBuffer(), dataStr.c_str(), bytesToAssign ) );
    ASSERT_EQ( bytesToAssign, buff2.GetBytesUsed() );

    // buff1 should be empty (although it may still contain the bytes, since we were using the built-in buffer)
    ASSERT_EQ( 0, buff1.GetBytesUsed() );
    ASSERT_TRUE( pBuff1Builtin == buff1.GetBuffer() );

    ///////////////////////////////////
    // now we call Buffer::Assign() where we're using an allocated buffer
    bytesToAssign = dataStr.size();
    ASSERT_TRUE( bytesToAssign > buff1.GetBuiltinBuffSize() );
    ASSERT_TRUE( buff1.Assign( dataStr.c_str(), bytesToAssign ) );
    ASSERT_EQ( 0, ::memcmp( buff1.GetCharBuffer(), dataStr.c_str(), bytesToAssign ) );
    ASSERT_EQ( bytesToAssign, buff1.GetBytesUsed() );

    void* pBuff1Allocated = buff1.GetBuffer(); // this should not be the built-in buffer for buff1
    ASSERT_TRUE( pBuff1Allocated != pBuff1Builtin );

    // first we assign buff1 to buff2 without transferring ownership
    ASSERT_TRUE( buff2.Assign( buff1, false ) );
    ASSERT_EQ( 0, ::memcmp( buff2.GetCharBuffer(), dataStr.c_str(), bytesToAssign ) );
    ASSERT_EQ( bytesToAssign, buff2.GetBytesUsed() );

    // buff1 should still contain the original data
    ASSERT_EQ( 0, ::memcmp( buff1.GetCharBuffer(), dataStr.c_str(), bytesToAssign ) );
    ASSERT_EQ( bytesToAssign, buff1.GetBytesUsed() );
    ASSERT_TRUE( pBuff1Allocated == buff1.GetBuffer() );

    // now assign buff1 to buff2 and transfer ownership
    buff2.ResetBuffer();
    ASSERT_TRUE( buff2.Assign( buff1, true ) );
    ASSERT_EQ( 0, ::memcmp( buff2.GetCharBuffer(), dataStr.c_str(), bytesToAssign ) );
    ASSERT_EQ( bytesToAssign, buff2.GetBytesUsed() );

    // buff1 should be empty and it should now be using its built-in buffer
    ASSERT_EQ( 0, buff1.GetBytesUsed() );
    ASSERT_TRUE( pBuff1Builtin == buff1.GetBuffer() );
}

TEST(Buffer, Append)
{
    // set up some data we can append to the buffer
    char dataBuff[ 1000 ];
    int j;
    for ( j = 0; j < sizeof dataBuff; ++j )
    {
        dataBuff[j] = (char)j;
    }

    // now create a Buffer, append the data to it in chunks, and check the contents
    Buffer<100> buff;

    int chunkSize = 50; // pick something smaller than the size of the built-in buffer and that evenly divides sizeof( dataBuff ), just to make life easy
    int chunkCount = sizeof dataBuff / chunkSize;
    for ( j = 0; j < chunkCount; ++j )
    {
        ASSERT_TRUE( buff.Append( dataBuff + (j * chunkSize), chunkSize ) );
    }

    ASSERT_EQ( sizeof dataBuff, buff.GetBytesUsed() );
    const char* pBuff = buff.GetCharBuffer();
    for ( j = 0; j < sizeof dataBuff; ++j )
    {
        ASSERT_EQ( (char)j, pBuff[j] );
    }
}

TEST(Buffer, Compare)
{
    char dataBuff[ 1000 ];
    int j;
    for ( j = 0; j < sizeof dataBuff; ++j )
    {
        dataBuff[j] = (char)j;
    }

    // now create a Buffer, assign some data to it, and check the contents
    const size_t BUILTIN_BUFF_SIZE = 100;
    Buffer<BUILTIN_BUFF_SIZE> buff;
    buff.Assign( dataBuff, BUILTIN_BUFF_SIZE );
    ASSERT_EQ( 0, buff.Compare( dataBuff, BUILTIN_BUFF_SIZE ) );

    // now check specifying different buffer sizes for dataBuff (if dataBuff
    // has more data than buff, then buff < dataBuff)
    ASSERT_EQ( -1, buff.Compare( dataBuff, BUILTIN_BUFF_SIZE + 1 ) );
    ASSERT_EQ(  1, buff.Compare( dataBuff, BUILTIN_BUFF_SIZE - 1 ) );

    // now alter the data in dataBuff and ensure we get the expected comparison results
    ASSERT_EQ(  0, buff.Compare( dataBuff, BUILTIN_BUFF_SIZE ) );
    dataBuff[42] += 1;
    ASSERT_EQ( -1, buff.Compare( dataBuff, BUILTIN_BUFF_SIZE ) );
    dataBuff[41] -= 1;
    ASSERT_EQ(  1, buff.Compare( dataBuff, BUILTIN_BUFF_SIZE ) );

    // put dataBuff back to its original state
    dataBuff[41] += 1;
    dataBuff[42] -= 1;

    // create some Slice objects from dataBuff and compare
    Slice sliceEQ( dataBuff, BUILTIN_BUFF_SIZE );
    Slice sliceLT( dataBuff, BUILTIN_BUFF_SIZE - 1 );
    Slice sliceGT( dataBuff, BUILTIN_BUFF_SIZE + 1 );

    ASSERT_EQ(  0, buff.Compare( sliceEQ ) );
    ASSERT_EQ( -1, buff.Compare( sliceGT ) );
    ASSERT_EQ(  1, buff.Compare( sliceLT ) );

    // try with operator==
    ASSERT_TRUE( buff == sliceEQ );
    ASSERT_TRUE( buff != sliceLT );
    ASSERT_TRUE( buff != sliceGT );
}

struct ToStringTestCase
{
    const char* m_Input;
    size_t      m_InputChars; // can't use strlen(m_Input) since embedded nulls and escape chars throw off the character count
    const char* m_ExpectedTextOutput;
    const char* m_ExpectedBinaryDecimalOutput;
    const char* m_ExpectedBinaryHexOutput;
};

ToStringTestCase g_ToStringTestCases[] =
{
    { "", 0, "", "", "" },
    { "a", 1, "a", "97", "61" },
    { "A", 1, "A", "65", "41" },
    { "abc ABC", 7, "abc ABC", "97,98,99,32,65,66,67", "61 62 63 20 41 42 43" },
    { "\t", 1, " ", "9", "09" },
    { "abc\x19 ABC\x7f", 9, "abc  ABC ", "97,98,99,25,32,65,66,67,127", "61 62 63 19 20 41 42 43 7F" },
    { "\x00\xff\x01\xfe This is some more text.", 28, "     This is some more text.",
        "0,255,1,254,32,84,104,105,115,32,105,115,32,115,111,109,101,32,109,111,114,101,32,116,101,120,116,46",
        "00 FF 01 FE 20 54 68 69 73 20 69 73 20 73 6F 6D 65 20 6D 6F 72 65 20 74 65 78 74 2E" }
};
TEST(Buffer, ToString)
{
    const size_t BUILTIN_BUFF_SIZE = 8;
    Buffer<BUILTIN_BUFF_SIZE> buff;

    std::string formattedStr;
    for ( int j = 0; j < COUNTOF(g_ToStringTestCases); ++j )
    {
        buff.Assign( g_ToStringTestCases[j].m_Input, g_ToStringTestCases[j].m_InputChars );

        formattedStr = buff.ToString();
        ASSERT_STREQ( g_ToStringTestCases[j].m_ExpectedTextOutput, formattedStr.c_str() );

        formattedStr = buff.ToString( Buffer<BUILTIN_BUFF_SIZE>::FormatAsText );
        ASSERT_STREQ( g_ToStringTestCases[j].m_ExpectedTextOutput, formattedStr.c_str() );

        formattedStr = buff.ToString( Buffer<BUILTIN_BUFF_SIZE>::FormatAsBinaryDecimal );
        ASSERT_STREQ( g_ToStringTestCases[j].m_ExpectedBinaryDecimalOutput, formattedStr.c_str() );

        formattedStr = buff.ToString( Buffer<BUILTIN_BUFF_SIZE>::FormatAsBinaryHex );
        ASSERT_STREQ( g_ToStringTestCases[j].m_ExpectedBinaryHexOutput, formattedStr.c_str() );
    }
}
