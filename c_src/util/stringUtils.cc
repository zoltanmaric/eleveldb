//
// Created by Paul A. Place on 2/4/16.
//

#include "utils.h"
#include <stdio.h> // need this to ensure snprintf() is declared on all platforms where we build

namespace basho {
namespace utils {

// removes any instances of characters from the ends of a string
std::string
TrimCharacters( const std::string& StrIn, const std::string& CharsToTrim )
{
    // find the index of the first non-CharsToTrim char in Str
    std::string::size_type firstNonCharToTrim = StrIn.find_first_not_of( CharsToTrim );
    if ( firstNonCharToTrim == std::string::npos )
    {
        // Str is nothing but CharsToTrim (or possibly it's empty)
        return std::string();
    }

    // find the index of the last non-CharsToTrim char in Str
    std::string::size_type lastNonCharToTrim = StrIn.find_last_not_of( CharsToTrim );
    if ( lastNonCharToTrim == std::string::npos )
    {
        // hmm...this shouldn't happen since we already verified above that we
        // have at least 1 non-CharsToTrim char; at any rate, we have nothing
        // to return
        return std::string();
    }

    return StrIn.substr( firstNonCharToTrim, lastNonCharToTrim - firstNonCharToTrim + 1 ); // second param is length of substring; be sure to include last non-CharsToTrim character
}

// removes any whitespace (space, tab, CR, LF chars) from the ends of a string
std::string
TrimWhitespace( const std::string& StrIn )
{
    static const std::string c_WhiteSpaceChars = " \n\r\t";

    return TrimCharacters( StrIn, c_WhiteSpaceChars );
}

// formats an integer as a human-readable string with thousands-separators (NOTE: this is not locale aware)
std::string FormatIntAsString( uint64_t Value )
{
    std::string retVal;

    // first we convert Value to a string
    //
    // NOTE: we don't use itoa() or its friends since they are non-standard
    // (i.e., not ANSI-C) and may not be implemented on all platforms we support
    char valueBuff[24]; // large enough to hold max uint64_t (18446744073709551615) + null terminator
    const int charsToWrite = ::snprintf( valueBuff, sizeof valueBuff, "%llu", Value );
    if ( charsToWrite < 0 || static_cast<size_t>( charsToWrite ) >= sizeof valueBuff )
    {
        // we either had a formatting error or we overflowed valueBuff; neither should happen
        retVal = "<ERROR FORMATTING NUMBER>";
    }
    else
    {
        // determine how many commas we're adding and reserve space in the retVal string
        int commaCount = (charsToWrite - 1) / 3;
        retVal.reserve( charsToWrite + commaCount );

        // now copy the formatted number to retVal, adding commas as needed
        int j, charsLeft = charsToWrite;
        while ( commaCount > 0 )
        {
            // determine how many digits we write on this pass
            int thisCharCount = charsLeft % 3;
            if ( 0 == thisCharCount )
            {
                thisCharCount = 3;
            }
            for ( j = 0; j < thisCharCount; ++j )
            {
                retVal.push_back( valueBuff[ charsToWrite - charsLeft + j ] );
            }
            retVal.push_back( ',' );

            --commaCount;
            charsLeft -= thisCharCount;
        }

        // write any remaining chars
        for ( j = 0; j < charsLeft; ++j )
        {
            retVal.push_back( valueBuff[ charsToWrite - charsLeft + j ] );
        }
    }
    return retVal;
}

// formats a size in bytes as a human-readable string (e.g., formats 987 as
// "987 bytes" and formats 303841 as "296.72 KB"); if the IncludeExactSize
// parameter is true, appends the exact byte count if the value of SizeInBytes
// is greater than 1 KB (e.g., formats formats 987 as "987 bytes" and formats
// 303841 as "296.72 KB (303,841 bytes)")
std::string FormatSizeAsString( uint64_t SizeInBytes, bool IncludeExactSize )
{
    std::string retVal;
    if ( SizeInBytes >= 1024 )
    {
        double adjustedSize = (double)SizeInBytes / 1024.0;
        int adjustmentCount = 0;
        while ( adjustedSize >= 1024.0 )
        {
            adjustedSize /= 1024.0;
            ++adjustmentCount;
        }

        static const char* s_Suffixes[] =
        {
            "KB", // kilobyte
            "MB", // megabyte
            "GB", // gigabyte
            "TB", // terabyte
            "PB", // petabyte
            "EB", // exabyte
            "ZB", // zettabyte
            "YB"  // yottabyte
        };

        if ( adjustmentCount < COUNTOF( s_Suffixes ) )
        {
            char valueBuff[32];
            int charsToWrite = ::snprintf( valueBuff, sizeof valueBuff,
                                           "%.2f %s", adjustedSize, s_Suffixes[ adjustmentCount ] );
            if ( charsToWrite > 0 && static_cast<size_t>( charsToWrite ) < sizeof valueBuff )
            {
                retVal = valueBuff;
                if ( IncludeExactSize )
                {
                    std::string exactSizeBytes = FormatIntAsString( SizeInBytes );
                    retVal.reserve( retVal.size() + exactSizeBytes.size() + 9 );
                    retVal.append( " (" );
                    retVal.append( exactSizeBytes );
                    retVal.append( " bytes)" );
                }
            }
        }
    }

    if ( retVal.empty() )
    {
        // either SizeInBytes is less than 1024 or its so large we don't know
        // the suffix; either way, just write the raw value
        retVal = FormatIntAsString( SizeInBytes );
        retVal.append( 1 == SizeInBytes ? " byte" : " bytes" );
    }
    return retVal;
}

} // namespace utils
} // namespace basho
