//
// Created by Paul A. Place on 12/16/15.
//

#ifndef BASHO_BIGSET_ACC_H
#define BASHO_BIGSET_ACC_H

#include <list>

#include "BigsetKey.h"
#include "BigsetClock.h"

namespace basho {
namespace bigset {

class BigsetAccumulator
{
private:
    ErlTerm     m_ThisActor;
    ErlTerm     m_CurrentSetName;
    ErlTerm     m_CurrentElement;
    BigsetClock m_CurrentContext;
    Dots        m_CurrentDots;

    ErlTerm m_ReadyKey;
    ErlTerm m_ReadyValue;
    bool    m_RecordReady;

    void FinalizeElement();

public:
    BigsetAccumulator() : m_RecordReady( false ) { }

    ~BigsetAccumulator() { }

    void AddRecord( ErlTerm key, ErlTerm value );

    bool
    recordReady()
    {
        return m_RecordReady;
    }

    void
    getCurrentElement( leveldb::Slice& key, leveldb::Slice& value )
    {
        if ( m_RecordReady )
        {
            key           = m_ReadyKey;
            value         = m_ReadyValue;
            m_RecordReady = false; // prepare for the next record
        }
    }
};

} // namespace bigset
} // namespace basho

#endif // BASHO_BIGSET_ACC_H
