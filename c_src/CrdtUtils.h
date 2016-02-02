//-------------------------------------------------------------------
//
// Based on the file: delta-crdts.cc
//
// @author    Carlos Baquero <cbm@di.uminho.pt>
//
// @copyright 2014 Carlos Baquero
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
// @doc  
//   Reference implementation in C++ of delta enabled CRDTs
// @end  
//
//
//-------------------------------------------------------------------

#ifndef BASHO_CRDTUTILS_H
#define BASHO_CRDTUTILS_H

#include <list>
#include <map>
#include <set>
#include <string>

namespace basho {
namespace crdtUtils {

// template class for a Dot, which is a pair consisting of an actor of
// type A together with a counter of type C
template<typename A, typename C>
class Dot
{
    A m_Actor;
    C m_Counter;

public:
    Dot( const A& Actor, C Counter ) { m_Actor = Actor, m_Counter = Counter; }

    // accessors
    void SetActor( const A& Actor ) { m_Actor = Actor; }
    const A& GetActor() const { return m_Actor; }

    void SetCounter( C Counter ) { m_Counter = Counter; }
    C GetCounter() const { return m_Counter; }
};

// container class for Dot<> objects
template<typename A, typename C>
class DotContainer
{
    std::list< Dot<A, C> > m_Dots;

public:
    DotContainer() { }

    ~DotContainer() { }

    const std::list< Dot<A, C> >& GetDots() const { return m_Dots; }

    void
    AddDot( const A& Actor, C Counter, bool IsTombstone )
    {
        if ( !IsTombstone )
        {
            m_Dots.push_back( Dot<A, C>( Actor, Counter ) );
        }
    }

    void
    AddDot( const A& Actor, C Counter )
    {
        AddDot( Actor, Counter, false );
    }

    void Clear() { m_Dots.clear(); }

    bool IsEmpty() const { return m_Dots.empty(); }

    bool
    ContainsActor( const A& Actor ) const
    {
        for ( auto it = m_Dots.begin(); it != m_Dots.end(); ++it )
        {
            if ( (*it).GetActor() == Actor )
            {
                return true;
            }
        }
        return false;
    }
};


// Autonomous causal context, for context sharing in maps
template<typename T, typename C>
class DotContext
{
    std::map<T, C>       m_CausalContext; // Compact causal context
    std::set<Dot<T, C> > m_DotCloud;      // Dot cloud

public:
    DotContext() { }

    DotContext( const std::map<T, C>& CausalContext,
                const std::set<Dot<T, C> >& DotCloud )
    {
        m_CausalContext = CausalContext;
        m_DotCloud      = DotCloud;
    }

    virtual ~DotContext() { }

    DotContext( const DotContext<T, C>& that )
    {
        m_CausalContext = that.m_CausalContext;
        m_DotCloud      = that.m_DotCloud;
    }

    DotContext<T, C>& operator=( const DotContext<T, C>& that )
    {
        if ( &that != this )
        {
            m_CausalContext = that.m_CausalContext;
            m_DotCloud      = that.m_DotCloud;
        }
        return *this;
    }

    void Clear() { m_CausalContext.clear(), m_DotCloud.clear(); }

    bool IsDotIncluded( const Dot<T, C>& dot ) const
    {
        const auto itm = m_CausalContext.find( dot.GetActor() );
        if ( itm != m_CausalContext.end() && dot.GetCounter() <= itm->second )
        {
            return true;
        }
        return (m_DotCloud.count( dot ) != 0);
    }

    void Compact()
    {
        // Compact DC to CC if possible
        //typename map<K,int>::iterator mit;
        //typename set<pair<K,int> >::iterator sit;
        bool flag; // may need to compact several times if ordering not best
        do
        {
            flag           = false;
            for ( auto sit = m_DotCloud.begin(); sit != m_DotCloud.end(); )
            {
                auto mit = m_CausalContext.find( sit->GetActor() );
                if ( mit == m_CausalContext.end() )
                { // No CC entry
                    if ( sit->GetCounter() == 1 ) // Can compact
                    {
                        m_CausalContext.insert( *sit );
                        m_DotCloud.erase( sit++ );
                        flag = true;
                    }
                    else
                    {
                        ++sit;
                    }
                }
                else // there is a CC entry already
                if ( sit->GetCounter() == m_CausalContext.at( sit->GetActor() ) +
                                    1 ) // Contiguous, can compact
                {
                    m_CausalContext.at( sit->GetActor() )++;
                    m_DotCloud.erase( sit++ );
                    flag = true;
                }
                else if ( sit->GetCounter() <=
                          m_CausalContext.at( sit->GetActor() ) ) // dominated, so prune
                {
                    m_DotCloud.erase( sit++ );
                    // no extra compaction opportunities so flag untouched
                }
                else
                {
                    ++sit;
                }
            }
        }
        while ( flag );
    }

    /*Dot<T, C> MakeDot( const T& id )
    {
        // On a valid dot generator, all dots should be compact on the used id
        // Making the new dot, updates the dot generator and returns the dot
        // pair<typename map<K,int>::iterator,bool> ret;
        auto kib = m_CausalContext.insert( Dot<T, C>( id, 1 ) );
        if ( !kib.second )
        { // already there, so update it
            (kib.first->second) += 1;
        }
        //return dot;
        return Dot<T, C>( *kib.first );
    }*/

    void InsertDot( const Dot<T, C>& d, bool compactnow = true )
    {
        // Set
        m_DotCloud.insert( d );
        if ( compactnow )
        {
            Compact();
        }
    }

    void Join( const DotContext<T, C>& o )
    {
        if ( this == &o )
        {
            // Join is idempotent, but just don't do it.
            return;
        }

        // CC
        //typename  map<K,int>::iterator mit;
        //typename  map<K,int>::const_iterator mito;
        auto mit  = m_CausalContext.begin();
        auto mito = o.m_CausalContext.begin();
        do
        {
            if ( mit != m_CausalContext.end() &&
                 (mito == o.m_CausalContext.end() ||
                  mit->first < mito->first) )
            {
                // cout << "cc one\n";
                // entry only at here
                ++mit;
            }
            else if ( mito != o.m_CausalContext.end() &&
                      (mit == m_CausalContext.end() ||
                       mito->first < mit->first) )
            {
                // cout << "cc two\n";
                // entry only at other
                m_CausalContext.insert( *mito );
                ++mito;
            }
            else if ( mit != m_CausalContext.end() &&
                      mito != o.m_CausalContext.end() )
            {
                // cout << "cc three\n";
                // in both
                m_CausalContext.at( mit->first ) =
                        std::max( mit->second, mito->second );
                ++mit;
                ++mito;
            }
        }
        while ( mit != m_CausalContext.end() ||
                mito != o.m_CausalContext.end() );
        // DC
        // Set
        for ( const auto& e : o.m_DotCloud )
        {
            InsertDot( e, false );
        }

        Compact();
    }
};

} // namespace crdtUtils
} // namespace basho

#endif // BASHO_CRDTUTILS_H
