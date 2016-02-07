//
// Created by Paul A. Place on 2/4/16.
//

#ifndef BASHOUTILS_STRINGUTILS_H
#define BASHOUTILS_STRINGUTILS_H

namespace basho {
namespace utils {

// removes any instances of characters from the ends of a string
std::string TrimCharacters( const std::string& StrIn, const std::string& CharsToTrim );

// removes any whitespace (space, tab, CR, LF chars) from the ends of a string
std::string TrimWhitespace( const std::string& StrIn );

} // namespace utils
} // namespace basho

#endif // BASHOUTILS_STRINGUTILS_H
