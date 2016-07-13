// -------------------------------------------------------------------
//
// stringUtils_test.cc: unit tests for the string utility functions in the Basho C/C++ utility library
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

struct TestCaseUint64
{
    uint64_t    m_Value;
    const char* m_String;
};

TestCaseUint64 g_Uint64TestCases[] =
{
    {                    0u,                          "0" },
    {                    1u,                          "1" },
    {                   23u,                         "23" },
    {                  987u,                        "987" },
    {                 1000u,                      "1,000" },
    {                12345u,                     "12,345" },
    {               123456u,                    "123,456" },
    {              1234567u,                  "1,234,567" },
    {             12345678u,                 "12,345,678" },
    {            123456789u,                "123,456,789" },
    {           9123456789u,              "9,123,456,789" },
    {          98123456789u,             "98,123,456,789" },
    {         987123456789u,            "987,123,456,789" },
    {        6987123456789u,          "6,987,123,456,789" },
    {       56987123456789u,         "56,987,123,456,789" },
    {      456987123456789u,        "456,987,123,456,789" },
    {     3456987123456789u,      "3,456,987,123,456,789" },
    {    23456987123456789u,     "23,456,987,123,456,789" },
    {   123456987123456789u,    "123,456,987,123,456,789" },
    {  7123456987123456789u,  "7,123,456,987,123,456,789" },
    { 18446744073709551615u, "18,446,744,073,709,551,615" } // max uint64_t
};

TEST(Format, FormatUInt64AsString)
{
    for ( int j = 0; j < COUNTOF( g_Uint64TestCases ); ++j )
    {
        std::string valueStr = FormatIntAsString( g_Uint64TestCases[j].m_Value );
        ASSERT_STREQ( g_Uint64TestCases[j].m_String, valueStr.c_str() );
    }
}

TestCaseUint64 g_SizeTestCases[] =
{
    {                    0u,       "0 bytes" },
    {                    1u,       "1 byte"  },
    {                   23u,      "23 bytes" },
    {                  987u,     "987 bytes" },
    {                 1000u,   "1,000 bytes" },
    {                 1024u,    "1.00 KB" },
    {                12345u,   "12.06 KB" },
    {               303841u,  "296.72 KB" },
    {              1024000u, "1000.00 KB" }, // ideally this should be "1,000.00 KB", but that's a problem for another day
    {              1048576u,    "1.00 MB" },
    {           1073741824u,    "1.00 GB" },
    {        1099511627776u,    "1.00 TB" },
    {     1125899906842624u,    "1.00 PB" },
    {  1152921504606846976u,    "1.00 EB" },
    { 18446744073709551615u,   "16.00 EB" } // max uint64_t
};

TEST(Format, FormatSizeAsString)
{
    for ( int j = 0; j < COUNTOF( g_SizeTestCases ); ++j )
    {
        std::string valueStr = FormatSizeAsString( g_SizeTestCases[j].m_Value );
        ASSERT_STREQ( g_SizeTestCases[j].m_String, valueStr.c_str() );
    }
}

TestCaseUint64 g_SizeIncludingExactSizeTestCases[] =
{
    {                    0u,       "0 bytes" },
    {                    1u,       "1 byte"  },
    {                   23u,      "23 bytes" },
    {                  987u,     "987 bytes" },
    {                 1000u,   "1,000 bytes" },
    {                 1024u,    "1.00 KB (1,024 bytes)" },
    {                12345u,   "12.06 KB (12,345 bytes)" },
    {               303841u,  "296.72 KB (303,841 bytes)" },
    {              1024000u, "1000.00 KB (1,024,000 bytes)" },
    {              1048576u,    "1.00 MB (1,048,576 bytes)" },
    {           1073741824u,    "1.00 GB (1,073,741,824 bytes)" },
    {        1099511627776u,    "1.00 TB (1,099,511,627,776 bytes)" },
    {     1125899906842624u,    "1.00 PB (1,125,899,906,842,624 bytes)" },
    {  1152921504606846976u,    "1.00 EB (1,152,921,504,606,846,976 bytes)" },
    { 18446744073709551615u,   "16.00 EB (18,446,744,073,709,551,615 bytes)" } // max uint64_t
};

TEST(Format, FormatSizeAsStringIncludingExactSize)
{
    for ( int j = 0; j < COUNTOF( g_SizeIncludingExactSizeTestCases ); ++j )
    {
        std::string valueStr = FormatSizeAsString( g_SizeIncludingExactSizeTestCases[j].m_Value, true );
        ASSERT_STREQ( g_SizeIncludingExactSizeTestCases[j].m_String, valueStr.c_str() );
    }
}
