#include "utils.h"
#include <stdlib.h>

namespace basho {
namespace utils {

// parses the string representation of an erlang binary term
// (e.g., "<<24,1,240>>"), putting the binary data in a Buffer
int // number of bytes put in BinaryBuff; -1 on error
ParseErlangBinaryString(
    const std::string& BinaryStr,   // IN:  erlang binary term in string form
    ErlBuffer&         BinaryBuff ) // OUT: receives the binary bytes represented by BinaryStr; the BytesUsed property is set
{
    // start by trimming "<<" and ">>" from BinaryStr
    static const std::string c_BinaryBracketChars = "<>";

    std::string rawBinaryStr = TrimCharacters( BinaryStr, c_BinaryBracketChars );

    // rawBinaryStr is a series of integer string tokens, separated by commas
    int rawBytes = 0; // number of bytes we've parsed from rawBinaryStr
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
            rawBytes = -1;
            break;
        }

        // the only valid token-terminator characters are a comma or the terminating NULL
        char endChar = *pStrEnd;
        if ( ',' != endChar && 0 != endChar )
        {
            // found something in the string we didn't expect
            rawBytes = -1;
            break;
        }

        // ensure we have a byte value
        if ( value < 0 || value > 255 )
        {
            rawBytes = -1;
            break;
        }

        // store the parsed byte value in the caller's buffer
        ++rawBytes;
        if ( !BinaryBuff.EnsureSize( static_cast<size_t>( rawBytes ) ) )
        {
            rawBytes = -1;
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

    if ( rawBytes >= 0 )
    {
        BinaryBuff.SetBytesUsed( static_cast<size_t>( rawBytes ) );
    }
    return rawBytes;
}

} // namespace utils
} // namespace basho
