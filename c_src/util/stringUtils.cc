#include "utils.h"

namespace basho {
namespace utils {

// removes any instances of characters from the ends of a string
std::string
TrimCharacters( const std::string& StrIn, const std::string& CharsToTrim )
{
    // find the index of the first non-CharsToTrim char in Str
    auto firstNonCharToTrim = StrIn.find_first_not_of( CharsToTrim );
    if ( firstNonCharToTrim == std::string::npos )
    {
        // Str is nothing but CharsToTrim (or possibly it's empty)
        return std::string();
    }

    // find the index of the last non-CharsToTrim char in Str
    auto lastNonCharToTrim = StrIn.find_last_not_of( CharsToTrim );
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

} // namespace utils
} // namespace basho
