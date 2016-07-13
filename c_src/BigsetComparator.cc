// -------------------------------------------------------------------
//
// BigsetComparator.cc: Implementation of the BigsetComparator class, which
// implements a custom leveldb comparator for bigsets.
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

#include "BigsetComparator.h"
#include "BigsetKey.h"

namespace basho {
namespace bigset {

const char* BigsetComparator::Name() const
{
    static const char s_Name[] = "BigsetComparator";
    return s_Name;
}

bool BigsetComparator::IsClock( const leveldb::Slice& s )
{
    return 'c' == s[0];
}

bool BigsetComparator::IsHoff( const leveldb::Slice& s )
{
    return 'd' == s[0];
}

bool BigsetComparator::IsElement( const leveldb::Slice& s )
{
    return 'e' == s[0];
}

bool BigsetComparator::IsEnd( const leveldb::Slice& s )
{
    return 'z' == s[0];
}

// overload of the Compare() method that optionally compares only up to the Element name
int BigsetComparator::Compare( const leveldb::Slice& a, const leveldb::Slice& b, bool IgnoreActorCountForElement ) const
{
    // first check the easy case where the keys match exactly
    if ( a == b )
    {
        return 0;
    }

    // now compare the set names
    leveldb::Slice acopy = leveldb::Slice( a.data(), a.size() ), bcopy = leveldb::Slice( b.data(), b.size() );
    leveldb::Slice aset = internal::Get32PrefData( acopy ), bset = internal::Get32PrefData( bcopy );

    int set_cmp = aset.compare( bset );
    if ( set_cmp != 0 )
    {
        return set_cmp;
    }

    // the set names match, so look at the key types (c=clock, d=hoff, e=element, z=end_key)
    Slice a_key_type = internal::GetKeyType( acopy ), b_key_type = internal::GetKeyType( bcopy );

    int key_type_cmp = a_key_type.compare( b_key_type );
    if ( key_type_cmp != 0 || IsEnd( a_key_type ) ) // if we have end keys, there is nothing left after the type
    {
        return key_type_cmp;
    }

    // we have the same set and key type; if we have clock or handoff keys, then compare the actors (which is all that's left)
    if ( IsClock( a_key_type ) || IsHoff( a_key_type ) )
    {
        return acopy.compare( bcopy );
    }
    else if ( !IsElement( a_key_type ) )
    {
        // we have a key type we don't understand, which is an internal error

        // TODO: what is the correct way to handle this? for now, thrown an exception
        throw std::runtime_error( "invalid bigset key type" );
    }

    // at this point, we have element keys, so compare the actual element data
    Slice aelem = internal::Get32PrefData( acopy ), belem = internal::Get32PrefData( bcopy );

    int elem_cmp = aelem.compare( belem );
    if ( elem_cmp != 0 )
    {
        return elem_cmp;
    }

    // if we get here, we have matching elements, so compare actors (unless the caller told us to stop comparing at this point)
    if ( IgnoreActorCountForElement )
    {
        return 0;
    }

    Slice a_actor = internal::Get32PrefData( acopy ), b_actor = internal::Get32PrefData( bcopy );

    int actor_cmp = a_actor.compare( b_actor );
    if ( actor_cmp != 0 )
    {
        return actor_cmp;
    }

    // the only thing left to check is the counters
    uint64_t a_cntr = internal::GetCounter( acopy ), b_cntr = internal::GetCounter( bcopy );

    return static_cast<int>( a_cntr - b_cntr );
}

static leveldb::Comparator* AllocateBigsetComparator()
{
    return new BigsetComparator;
}

// returns the singleton BigsetComparator object
const BigsetComparator* BigsetComparator::GetComparator()
{
    return reinterpret_cast<const BigsetComparator*>( leveldb::GetAltComparator( AllocateBigsetComparator ) );
}

} // namespace bigset
} // namespace basho
