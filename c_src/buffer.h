//
// Created by Paul A. Place on 1/6/16.
//

#ifndef BASHOUTILS_BUFFER_H
#define BASHOUTILS_BUFFER_H

#include <cstddef>
#include <cstring>

namespace basho {
namespace utils {

template<size_t SIZE>
class Buffer
{
    char   m_Buffer[ SIZE ]; // built-in buffer, intended to avoid a heap alloc, if possible; NOTE: this is declared first to ensure optimal alignment of the user's buffer
    char*  m_pBuff;          // pointer to the actual buffer; either m_Buffer or a buffer from the heap
    size_t m_BuffSize;       // size of the buffer currently pointed to by m_pBuff

public:
    Buffer( size_t BuffSize = SIZE ) : m_pBuff( NULL ), m_BuffSize( 0 )
    {
        if ( BuffSize > SIZE )
        {
            m_pBuff = new char[ BuffSize ];
            if ( NULL != m_pBuff )
            {
                m_BuffSize = BuffSize;
            }
        }
        else
        {
            m_pBuff    = m_Buffer;
            m_BuffSize = sizeof m_Buffer;
        }
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
        m_pBuff    = m_Buffer;
        m_BuffSize = sizeof m_Buffer;
    }

    // accessor methods
    void* GetBuffer() { return static_cast<void*>( m_pBuff ); }

    char* GetCharBuffer() { return m_pBuff; }

    size_t GetBuffSize() const { return m_BuffSize; }

    size_t GetBuiltinBuffSize() const { return SIZE; }

    // ensures the buffer is at least the specified size
    //
    // NOTE: any previous data in the buffer is preserved
    bool // true => buffer is now (at least) the specified size
    EnsureSize(
        size_t NewSize,          // IN:  new size of the buffer; if the buffer is already at least this large, nothing happens
        bool*  pRealloc = NULL ) // OUT: receives true if the buffer was reallocated, else false; only set if true returned by this method
    {
        bool success = true;
        if ( NewSize > m_BuffSize )
        {
            // we need to allocate a larger buffer
            char* pNewBuff = new char[ NewSize ];
            if ( NULL != pNewBuff )
            {
                // preserve the old buffer's contents
                ::memcpy( pNewBuff, m_pBuff, m_BuffSize );

                // clean up any previously-allocated resources
                ResetBuffer();

                m_pBuff = pNewBuff;
                m_BuffSize = NewSize;
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
};

} // namespace utils
} // namespace basho

#endif //BASHOUTILS_BUFFER_H
