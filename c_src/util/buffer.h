// -------------------------------------------------------------------
//
// buffer.h: C++ buffer template class for the Basho C/C++ utility library
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

#ifndef LIBBASHO_BUFFER_H
#define LIBBASHO_BUFFER_H

#include <cstddef>
#include <cstring>
#include <cstdio>
#include <stdexcept>
#include <string>

#include <leveldb/slice.h>

using leveldb::Slice;

namespace basho {
namespace utils {

// Buffer template class: This class provides basic buffer management, using a
// built-in buffer whose size is specified by the template parameter. This
// class is useful when you need a buffer whose size does not vary too often.
// Using a built-in buffer that can grow if needed allows allocating a buffer
// that is large enough most of the time but allows for growth when needed.
//
// NOTE: Because a call to the EnsureSize() method (or one of the Assign()
// methods, which call the EnsureSize() method) may reallocate the internal
// buffer, you must take care to refresh your buffer pointer after a call
// to EnsureSize().
//
// NOTE: The Buffer class has an optional property called BytesUsed. This
// property is maintained by the Assign() methods; if those methods are not
// used to populate the Buffer object, then the user must maintain the
// BytesUsed property manually (or not use the property).
//
// NOTE: If you try to do something stupid (such as setting the BytesUsed value
// to a number larger than the buffer size), then the method that detects the
// stupidity throws a std::logic_error exception.
template<size_t SIZE>
class Buffer
{
    char   m_Buffer[ SIZE ]; // built-in buffer, intended to avoid a heap alloc, if possible; NOTE: this is declared first to ensure optimal alignment of the user's buffer
    char*  m_pBuff;          // pointer to the actual buffer; either m_Buffer or a buffer from the heap
    size_t m_BuffSize;       // size of the buffer currently pointed to by m_pBuff
    size_t m_BytesUsed;      // number of bytes in the buffer currently used

public:
    // ctors and assignment operators
    //
    // NOTE: for now, if you want to copy/assign a Buffer with a different
    // built-in buffer size, you'll have to use one of the Assign() methods
    Buffer( size_t BuffSize = SIZE ) : m_pBuff( m_Buffer ), m_BuffSize( SIZE ), m_BytesUsed( 0 )
    {
        if ( BuffSize > m_BuffSize )
        {
            m_pBuff = new char[ BuffSize ];
            m_BuffSize = (NULL != m_pBuff) ? BuffSize : 0;
        }
    }

    Buffer( const Buffer& That ) : m_pBuff( m_Buffer ), m_BuffSize( SIZE ), m_BytesUsed( 0 )
    {
        // ensure we have a large enough buffer
        size_t buffSize = That.m_BuffSize;
        if ( buffSize > m_BuffSize )
        {
            m_pBuff = new char[ buffSize ];
            m_BuffSize = (NULL != m_pBuff) ? buffSize : 0;
        }

        // if buffer allocation was successful, copy the data
        if ( m_BuffSize >= buffSize )
        {
            ::memcpy( m_pBuff, That.m_pBuff, buffSize );
            m_BytesUsed = That.m_BytesUsed;
        }
    }

    Buffer& operator=( const Buffer& That )
    {
        if ( this != &That )
        {
            if ( EnsureSize( That.m_BuffSize ) )
            {
                ::memcpy( m_pBuff, That.m_pBuff, That.m_BuffSize );
                m_BytesUsed = That.m_BytesUsed;
            }
            else
            {
                // throw an exception?
            }
        }
        return *this;
    }

    ~Buffer()
    {
        ResetBuffer();
    }

    // frees any allocated resources, resetting back to the built-in buffer
    //
    // NOTE: any data in the buffer is lost
    void
    ResetBuffer()
    {
        if ( m_pBuff != m_Buffer )
        {
            delete [] m_pBuff;
        }
        m_pBuff     = m_Buffer;
        m_BuffSize  = sizeof m_Buffer;
        m_BytesUsed = 0;
    }

    // ensures the buffer is at least the specified size
    //
    // NOTE: any previous data in the buffer is preserved
    bool // true => buffer is now (at least) the specified size
    EnsureSize(
        size_t NewSize,          // IN:  new size of the buffer; if the buffer is already at least this large, nothing happens
        bool*  pRealloc = NULL ) // OUT: receives true if the buffer was reallocated, else false; only used if true returned by this method
    {
        bool success = true;
        if ( NewSize > m_BuffSize )
        {
            // ensure that we have a reasonable allocation granularity
            // (so that we don't have too many reallocs)
            size_t diff = NewSize - m_BuffSize;
            size_t granularity = (SIZE > 1024) ? 1024 : SIZE;
            if ( diff < granularity )
            {
                // the caller is requesting a relatively small increase in
                // buffer size, so bump up the allocation size a bit
                NewSize = m_BuffSize + granularity;
            }

            // we need to allocate a larger buffer
            char* pNewBuff = new char[ NewSize ];
            if ( NULL != pNewBuff )
            {
                // preserve the old buffer's contents
                ::memcpy( pNewBuff, m_pBuff, m_BuffSize );

                // clean up any previously-allocated resources (but we need to preserve m_BytesUsed)
                size_t bytesUsedSave = m_BytesUsed;
                ResetBuffer();

                m_pBuff     = pNewBuff;
                m_BuffSize  = NewSize;
                m_BytesUsed = bytesUsedSave;

                if ( pRealloc != NULL )
                {
                    *pRealloc = true;
                }
            }
            else
            {
                success = false;
            }
        }
        else if ( pRealloc != NULL )
        {
            *pRealloc = false;
        }
        return success;
    }

    ///////////////////////////////////
    // Accessor Methods
    void* GetBuffer() const { return static_cast<void*>( m_pBuff ); }

    char* GetCharBuffer() const { return m_pBuff; }

    size_t GetBuffSize() const { return m_BuffSize; }

    size_t GetBuiltinBuffSize() const { return SIZE; }

    ///////////////////////////////////
    // BytesUsed Methods
    //
    // These methods are used to manage the amount of data in the buffer;
    // this must be maintained by the user
    size_t GetBytesUsed() const { return m_BytesUsed; }
    void SetBytesUsed( size_t BytesUsed )
    {
        if ( BytesUsed <= m_BuffSize )
        {
            m_BytesUsed = BytesUsed;
        }
        else
        {
            throw std::logic_error( "BytesUsed value is too large" );
        }
    }

    bool IsEmpty() const { return m_BytesUsed == 0; }

    ///////////////////////////////////
    // Assignment/Append Methods
    //
    // NOTE: These return true on success and false on failure (typically due
    // to an allocation failure).
    bool Assign( const char* pData, size_t SizeInBytes )
    {
        if ( !EnsureSize( SizeInBytes ) )
        {
            return false;
        }
        ::memcpy( m_pBuff, pData, SizeInBytes );
        m_BytesUsed = SizeInBytes;
        return true;
    }

    bool Assign( const Slice& Data )
    {
        return Assign( Data.data(), Data.size() );
    }

    // override of Assign() that transfers the buffer from another Buffer, avoiding a realloc
    //
    // NOTE: if the TransferOwnership parameter is true, the contents of the
    // source Buffer object are reset
    bool Assign( Buffer& That, bool TransferOwnership )
    {
        bool retVal = true;
        if ( this != &That )
        {
            if ( TransferOwnership && (That.m_pBuff != That.m_Buffer) )
            {
                // That is not using its built-in buffer, so transfer the allocated buffer to this object
                ResetBuffer();
                m_pBuff     = That.m_pBuff;
                m_BuffSize  = That.m_BuffSize;
                m_BytesUsed = That.m_BytesUsed;

                // set That.m_pBuff to NULL, so that the buffer isn't freed when we call That.Reset() below
                That.m_pBuff = NULL;
            }
            else
            {
                retVal = Assign( That.GetCharBuffer(), That.GetBytesUsed() );
            }

            // if we are transferring ownership of the buffer, reset That
            if ( TransferOwnership )
            {
                That.ResetBuffer();
            }
        }
        return retVal;
    }

    // appends bytes to the end of this buffer; the end is determined by the BytesUsed value
    bool Append( const char* pBytesToAppend, size_t Size )
    {
        // ensure we have something to do
        if ( NULL == pBytesToAppend || 0 == Size )
        {
            return true;
        }

        // ensure our buffer is large enough for the additional bytes
        size_t sizeNeeded = m_BytesUsed + Size;
        if ( !EnsureSize( sizeNeeded ) )
        {
            return false;
        }

        // now copy the caller's data to the buffer after the current data
        ::memcpy( m_pBuff + m_BytesUsed, pBytesToAppend, Size );
        m_BytesUsed += Size;
        return true;
    }

    ///////////////////////////////////
    // Comparison Methods
    //
    // NOTE: The methods that return an integer return 0 when the two buffers
    // contain the same data, else -1 (first is less than second) or +1 (first
    // is greater than second).
    int Compare( const char* pData, const size_t SizeInBytes, bool CompareBytesUsed ) const
    {
        // first determine how many bytes we compare from this object
        // (total buffer size or only BytesUsed)
        const size_t thisBytesToCompare = CompareBytesUsed ? m_BytesUsed : m_BuffSize;

        // use memcmp() to compare as many bytes as possible
        size_t bytesToCompare = thisBytesToCompare;
        if ( SizeInBytes < bytesToCompare )
        {
            bytesToCompare = SizeInBytes;
        }

        int retVal = 0;
        if ( bytesToCompare > 0 )
        {
            retVal = ::memcmp( m_pBuff, pData, bytesToCompare );
        }

        if ( 0 == retVal )
        {
            // so far the buffers contain the same bytes; does either buffer
            // contain more stuff?
            if ( thisBytesToCompare > SizeInBytes )
            {
                retVal = 1;
            }
            else if ( thisBytesToCompare < SizeInBytes )
            {
                retVal = -1;
            }
        }
        else if ( retVal < 0 )
        {
            // memcmp() only promises < 0, but we want -1
            retVal = -1;
        }
        else
        {
            retVal = 1;
        }
        return retVal;
    }

    int Compare( const char* pData, const size_t SizeInBytes ) const
    {
        bool compareBytesUsed = (m_BytesUsed > 0);
        return Compare( pData, SizeInBytes, compareBytesUsed );
    }

    int Compare( const Slice& Data ) const
    {
        return Compare( Data.data(), Data.size() );
    }

    bool operator==( const Slice& Data ) const { return 0 == Compare( Data ); }
    bool operator!=( const Slice& Data ) const { return 0 != Compare( Data ); }

    ///////////////////////////////////
    // Utility Methods

    enum ToStringOption
    {
        FormatAsText,
        FormatAsBinaryDecimal, // print a list of comma-separated decimals, similar to an Erlang binary dump
        FormatAsBinaryHex      // print a list of space-separated hexadecimal values
    };

    // returns a human-readable string; length is based on the bytes used
    std::string ToString( ToStringOption Option = FormatAsText, char ReplacementForUnprintable = 0 ) const
    {
        // if the caller did not specify a replacement for unprintable
        // characters, use the space character
        if ( 0 == ReplacementForUnprintable )
        {
            ReplacementForUnprintable = ' ';
        }

        std::string retVal;
        retVal.reserve( (FormatAsText == Option) ? m_BytesUsed : 3 * m_BytesUsed ); // avoid a bunch of re-allocs
        for ( size_t j = 0; j < m_BytesUsed; ++j )
        {
            // get the next character from our buffer, format it, and write it to the output string
            char c = m_pBuff[j];
            if ( FormatAsText == Option )
            {
                // if this is a printable character (between 0x20 and 0x7e, inclusive),
                // use it as-is, else use ReplacementForUnprintable
                if ( c < 0x20 || c > 0x7e )
                {
                    c = ReplacementForUnprintable;
                }
                retVal.push_back( c );
            }
            else
            {
                char buff[8]; // need at most 3 chars (plus null terminator) to represent a char as an integer; only need 2 for hex; also allow for separator char
                const char* formatStr = NULL;
                if ( FormatAsBinaryDecimal == Option )
                {
                    formatStr = retVal.empty() ? "%u" : ",%u";
                }
                else if ( FormatAsBinaryHex == Option )
                {
                    formatStr = retVal.empty() ? "%02X" : " %02X";
                }
                else
                {
                    throw std::invalid_argument( "unknown format option" );
                }
                ::snprintf( buff, sizeof buff, formatStr, (unsigned int)((unsigned char)c) ); // the casts ensure we don't get a sign extension
                retVal.append( buff );
            }
        }
        return retVal;
    }
};

} // namespace utils
} // namespace basho

#endif // LIBBASHO_BUFFER_H
