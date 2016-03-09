//
// Created by Paul A. Place on 2/4/16.
//

#ifndef BASHOUTILS_STRINGUTILS_H
#define BASHOUTILS_STRINGUTILS_H

#include <stdint.h>
#include <string>

namespace basho {
namespace utils {

// removes any instances of characters from the ends of a string
std::string TrimCharacters( const std::string& StrIn, const std::string& CharsToTrim );

// removes any whitespace (space, tab, CR, LF chars) from the ends of a string
std::string TrimWhitespace( const std::string& StrIn );

// formats an integer as a human-readable string with thousands-separators (NOTE: this is not locale aware)
std::string FormatIntAsString( uint64_t Value );

// formats a size in bytes as a human-readable string (e.g., formats 987 as
// "987 bytes" and formats 303841 as "296.72 KB"); if the IncludeExactSize
// parameter is true, appends the exact byte count if the value of SizeInBytes
// is greater than 1 KB (e.g., formats formats 987 as "987 bytes" and formats
// 303841 as "296.72 KB (303,841 bytes)")
std::string FormatSizeAsString( uint64_t SizeInBytes, bool IncludeExactSize = false );

} // namespace utils
} // namespace basho

#endif // BASHOUTILS_STRINGUTILS_H
