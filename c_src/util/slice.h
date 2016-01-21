// Basho Utils version of the Slice class from leveldb
//
// Created by Paul A. Place on 1/21/16.
//

#ifndef BASHOUTILS_SLICE_H
#define BASHOUTILS_SLICE_H

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <string>

namespace basho {
namespace utils {

class Slice
{
    const char* m_Data;
    size_t      m_Size;

public:
    // Create an empty slice.
    Slice() : m_Data( "" ), m_Size( 0 ) { }

    // Create a slice that refers to a buffer of data
    Slice( const char* Data, size_t Size ) : m_Data( Data ), m_Size( Size ) { }

    // Create a slice that refers to the contents of a std::string
    Slice( const std::string& Str ) : m_Data( Str.data() ), m_Size( Str.size() ) { }

    // Create a slice that refers to C-string
    Slice( const char* Str ) : m_Data( Str ), m_Size( ::strlen( Str ) ) { }

    // Intentionally copyable: do not need to explicitly declare the copy ctor,
    // assignment operator (or dtor, for that matter)

    // Return a pointer to the beginning of the referenced data
    const char* data() const { return m_Data; }

    // Return the length (in bytes) of the referenced data
    size_t size() const { return m_Size; }

    // Return true iff the length of the referenced data is zero
    bool empty() const { return m_Size == 0; }

    // Return the Nth byte in the referenced data
    //
    // REQUIRES: n < size()
    char operator[]( size_t N ) const
    {
        assert( N < size() );
        return m_Data[N];
    }

    // Change this slice to refer to an empty array
    void clear()
    {
        m_Data = "";
        m_Size = 0;
    }

    // Drop the first "n" bytes from this slice
    void remove_prefix( size_t N )
    {
        assert( N <= size() );
        m_Data += N;
        m_Size -= N;
    }

    // Return a string that contains the copy of the referenced data.
    std::string ToString() const { return std::string( m_Data, m_Size ); }

    // Three-way comparison.  Returns value:
    //   <  0 iff "*this" <  "b",
    //   == 0 iff "*this" == "b",
    //   >  0 iff "*this" >  "b"
    int compare( const Slice& b ) const;

    // Return true iff the specified Slice is a prefix of this Slice
    bool StartsWith( const Slice& That ) const
    {
        return ((m_Size >= That.m_Size) &&
                (::memcmp( m_Data, That.m_Data, That.m_Size ) == 0));
    }
};

inline bool operator==( const Slice& x, const Slice& y )
{
    return ((x.size() == y.size()) &&
            (::memcmp( x.data(), y.data(), x.size() ) == 0));
}

inline bool operator!=( const Slice& x, const Slice& y )
{
    return !(x == y);
}

inline int Slice::compare( const Slice& b ) const
{
    const int min_len = (m_Size < b.m_Size) ? m_Size : b.m_Size;
    int       r       = ::memcmp( m_Data, b.m_Data, min_len );
    if ( r == 0 )
    {
        if ( m_Size < b.m_Size )
        {
            r = -1;
        }
        else if ( m_Size > b.m_Size )
        {
            r = 1;
        }
    }
    return r;
}

} // namespace utils
} // namespace basho

#endif  // BASHOUTILS_SLICE_H
