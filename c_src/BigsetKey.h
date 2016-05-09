//
// Created by Paul A. Place on 1/14/16.
//

#ifndef BASHO_BIGSET_KEY_H
#define BASHO_BIGSET_KEY_H

#include "BigsetClock.h"

namespace basho {
namespace bigset {

class BigsetKey
{
    enum KeyType
    {
        KeyTypeInvalid      = 0,
        KeyTypeClock        = 'c',
        KeyTypeSetTombstone = 'd',
        KeyTypeElement      = 'e',
        KeyTypeEnd          = 'z'
    };

    KeyType  m_KeyType;
    Slice    m_SetName;
    Slice    m_Element;
    Slice    m_Actor;
    uint64_t m_Counter;

public:
    BigsetKey( Slice key );

    KeyType GetKeyType() const { return m_KeyType; }

    bool IsClock() const { return KeyTypeClock == m_KeyType; }

    bool IsSetTombstone() const { return KeyTypeSetTombstone == m_KeyType; }

    bool IsElement() const { return KeyTypeElement == m_KeyType; }

    bool IsEnd() const { return KeyTypeEnd == m_KeyType; }

    bool IsValid() const { return KeyTypeInvalid != m_KeyType; }

    const Slice& GetSetName() const { return m_SetName; }

    const Slice& GetActor() const { return m_Actor; }

    const Slice& GetElement() const { return m_Element; }

    uint64_t GetCounter() const { return m_Counter; }
};

} // namespace bigset
} // namespace basho

#endif // BASHO_BIGSET_KEY_H
