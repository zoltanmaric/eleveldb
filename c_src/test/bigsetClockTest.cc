#include <stdio.h>
#include <iostream>
#include <fstream>

#include "../buffer.h"
#include "../BigsetClock.h"

typedef basho::utils::Buffer<1024> Buffer;

std::string TrimCharacters( const std::string& Str, const std::string& CharsToTrim )
{
    // find the index of the first non-CharsToTrim char in Str
    auto firstNonCharToTrim = Str.find_first_not_of( CharsToTrim );
    if ( firstNonCharToTrim == std::string::npos )
    {
        // Str is nothing but CharsToTrim (or possibly it's empty)
        return std::string();
    }

    // find the index of the last non-CharsToTrim char in Str
    auto lastNonCharToTrim = Str.find_last_not_of( CharsToTrim );
    if ( lastNonCharToTrim == std::string::npos )
    {
        // hmm...this shouldn't happen since we already verified above that we
        // have at least 1 non-CharsToTrim char; at any rate, we have nothing
        // to return
        return std::string();
    }

    return Str.substr( firstNonCharToTrim, lastNonCharToTrim - firstNonCharToTrim + 1 ); // second param is length of substring; be sure to include last non-CharsToTrim character
}

std::string TrimWhitespace( const std::string& Str )
{
    static const std::string c_WhiteSpaceChars = " \n\r\t";

    return TrimCharacters( Str, c_WhiteSpaceChars );
}

size_t // number of bytes put in BinaryBuff; 0 on error
ParseBinaryString( const std::string& BinaryStr, Buffer& BinaryBuff )
{
    // start by trimming "<<" and ">>" from BinaryStr
    static const std::string c_BinaryBracketChars = "<>";

    std::string rawBinaryStr = TrimCharacters( BinaryStr, c_BinaryBracketChars );
    //printf( "       :   %s\n", rawBinaryStr.c_str() );

    // rawBinaryStr is a series of integer string tokens, separated by commas
    size_t rawBytes = 0; // number of bytes we've parsed from rawBinaryStr
    const char* pCur = rawBinaryStr.c_str();
    while ( 0 != *pCur )
    {
        // convert the next string token to an integer
        char* pStrEnd = NULL;
        long value = strtol( pCur, &pStrEnd, 10 );

        // ensure we found an integer at pCur
        if ( pStrEnd == pCur || NULL == pStrEnd )
        {
            // we failed to find any integer characters at pCur
            rawBytes = 0;
            break;
        }

        // the only valid token-terminator characters are a comma or the terminating NULL
        char endChar = *pStrEnd;
        if ( ',' != endChar && 0 != endChar )
        {
            // found something in the string we didn't expect
            rawBytes = 0;
            break;
        }

        // ensure we have a byte value
        if ( value < 0 || value > 255 )
        {
            rawBytes = 0;
            break;
        }

        // store the parsed byte value in the caller's buffer
        ++rawBytes;
        if ( !BinaryBuff.EnsureSize( rawBytes ) )
        {
            rawBytes = 0;
            break;
        }
        BinaryBuff.GetCharBuffer()[ rawBytes - 1 ] = static_cast<char>( value );

        // move to the next integer
        pCur = pStrEnd;
        if ( 0 != endChar )
        {
            ++pCur; // we're not at the end of the string, so move past the comma separator char
        }
    }
    return rawBytes;
}

int main( int argc, char** argv )
{
    // validate command line input
    if ( argc < 2 )
    {
        printf( "USAGE: bigsetClockTest <BigsetClockFile>\n" );
        return 1;
    }

    const char* fileName = argv[1];
    printf( "Parsing bigset clocks in '%s'\n", fileName );

    // open the file
    std::ifstream inputFile( fileName );
    if ( !inputFile.is_open() )
    {
        printf( "Unable to open file '%s'\n", fileName );
        return 2;
    }

    int retVal = 0;

    // each line of text in the file is expected to be of the form "Erlang tuple : <<bytes>>"
    int lineCount = 0;
    std::string line;
    while ( std::getline( inputFile, line ) )
    {
        ++lineCount;

        // remove any trailing CR/LF chars from the line
        line = TrimWhitespace( line );

        // parse the line into the the two pieces we expect
        auto separator = line.find( " : " );
        if ( separator == std::string::npos )
        {
            printf( "Line %d (%s) not in the expected format\n", lineCount, line.c_str() );
            retVal = 10;
            break;
        }

        std::string termStr = line.substr( 0, separator );
        std::string binaryStr = line.substr( separator + 3 );

        printf( "LINE %02d: %s\n", lineCount, termStr.c_str() );
        printf( "       : %s\n", binaryStr.c_str() );

        // parse the binaryStr into a buffer of bytes
        Buffer binaryBuff;
        size_t binaryByteCount = ParseBinaryString( binaryStr, binaryBuff );
        if ( 0 == binaryByteCount )
        {
            printf( "Unable to parse the bigset binary string '%s'\n", binaryStr.c_str() );
            retVal = 11;
            break;
        }
        printf( "       : %zd bytes\n", binaryByteCount );

        // now create a BigsetClock object from the byte stream
        leveldb::Slice binaryValue( binaryBuff.GetCharBuffer(), binaryByteCount );
        basho::bigset::BigsetClock bigsetClock;
        std::string errStr;
        if ( !basho::bigset::BigsetClock::ValueToBigsetClock( binaryValue, bigsetClock, errStr ) )
        {
            printf( "Unable to construct a BigsetClock from the binary string '%s'\nERROR: %s\n", binaryStr.c_str(), errStr.c_str() );
            retVal = 12;
            break;
        }

        // get the string-version of the BigsetClock object and compare to the
        // Erlang tuple string from the file
        std::string bigsetClockStr = bigsetClock.ToString();
        if ( bigsetClockStr != termStr )
        {
            printf( "BigsetClock.ToString() '%s' does not match Erlang term from file '%s'\n", bigsetClockStr.c_str(), termStr.c_str() );
            retVal = 13;
            break;
        }

        //if ( lineCount  > 10 ) break;
    }
    printf( "Processed %d lines\n", lineCount );
    return retVal;
}
