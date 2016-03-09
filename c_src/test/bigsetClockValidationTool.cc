#include <stdio.h>
#include <iostream>
#include <fstream>

#include "../util/utils.h"
#include "../BigsetClock.h"

// local helper functions
static int ProcessTermToBinaryLine( const std::string& Line, int LineNumber );
static int ProcessMergedClocksLine( const std::string& Line, int LineNumber );

// print the command line usage for this application and call exit()
static void PrintUsageAndExit()
{
    fprintf( stderr, "USAGE: bigsetClockValidationTool <Action>\n" );
    fprintf( stderr, "\n" );
    fprintf( stderr, "Actions: -b<File>  Parse <File> as 'clock : term-to-binary(clock)'\n" );
    fprintf( stderr, "         -m<File>  Parse <File> as 'clock1 : clock2 : merged-clock\n" );
    fprintf( stderr, "\n" );
    fprintf( stderr, "Where each clock is an erlang 2-tuple of the form {version_vector, dot_cloud}\n" );
    fprintf( stderr, "where version_vector is a list of 2-tuples of the form\n" );
    fprintf( stderr, "[{Actor :: binary(8), Count :: pos_integer()}] and dot_cloud is a list of\n" );
    fprintf( stderr, "2-tuples of the form [{Actor :: binary(8), [pos_integer()]}]\n" );
    exit( 1000 );
}

int main( int argc, char** argv )
{
    const char* fileName = NULL;
    const char* description = NULL;

    enum Action
    {
        None,
        ParseBinaryClock,
        CheckMergedClocks
    } action = None;

    // parse command line input
    for ( int j = 1; j < argc; ++j )
    {
        if ( '-' == argv[j][0] )
        {
            switch ( argv[j][1] )
            {
                case 'b':
                    fileName = argv[j] + 2;
                    description = "bigset clocks in term_to_binary format";
                    action = ParseBinaryClock;
                    break;
                case 'm':
                    fileName = argv[j] + 2;
                    description = "merged bigset clocks";
                    action = CheckMergedClocks;
                    break;
                default:
                    PrintUsageAndExit();
            }
        }
    }

    // ensure we have something to do
    if ( None == action )
    {
        PrintUsageAndExit();
    }

    printf( "Parsing %s in '%s'\n", description, fileName );

    // open the file
    std::ifstream inputFile( fileName );
    if ( !inputFile.is_open() )
    {
        printf( "Unable to open file '%s'\n", fileName );
        return 2;
    }

    int retVal = 0;

    int lineCount = 0;
    std::string line;
    while ( 0 == retVal && std::getline( inputFile, line ) )
    {
        ++lineCount;

        if ( ParseBinaryClock == action )
        {
            retVal = ProcessTermToBinaryLine( line, lineCount );
        }
        else if ( CheckMergedClocks == action )
        {
            retVal = ProcessMergedClocksLine( line, lineCount );
        }

        //if ( lineCount > 10 ) break;
    }
    printf( "Processed %d lines\n", lineCount );
    return retVal;
}

static bool
ParseCounterString( const std::string& CounterStr, basho::bigset::Counter& Value )
{
    char* pStrEnd = NULL;
    Value = strtoull( CounterStr.c_str(), &pStrEnd, 10 );

    // ensure we found an integer and that the conversion ended at the NULL terminator
    if ( pStrEnd == CounterStr.c_str() || NULL == pStrEnd || 0 != *pStrEnd )
    {
        return false;
    }
    return true;
}

// parses a version vector or dot cloud string
//
// NOTE: this is not a standard erlang list since the enclosing brackets "[]" have already been removed
static int
ParseActorTupleString( std::string& ActorTupleStr, bool IsVersionVector, basho::bigset::BigsetClock& Clock )
{
    // ActorTupleStr is either a version vector string or a dot cloud string
    //
    // A version vector string is a list of 0 or more 2-tuples of the form
    // {Actor :: binary(8), Count :: pos_integer()}
    //
    // A dot cloud string is a list of 0 or more 2-tuples of the form
    // [{Actor :: binary(8), [pos_integer()]}]
    if ( ActorTupleStr.empty() )
    {
        return 0;
    }

    // find the opening and closing curly braces and extract the actor ID string
    std::string::size_type openingBrace = ActorTupleStr.find( '{' );
    if ( std::string::npos == openingBrace )
    {
        return 40;
    }
    std::string::size_type closingBrace = ActorTupleStr.find( '}', openingBrace + 1 );
    if ( std::string::npos == closingBrace )
    {
        return 41;
    }

    do
    {
        // get the string containing the binary actor ID, followed by the counter integer(s)
        std::string tupleBinaryStr( ActorTupleStr.substr( openingBrace + 1, closingBrace - openingBrace - 1 ) ); // -1 => don't include closing brace

        // isolate the actor ID, which is an erlang binary term of the form <<1,2,3,4,5,6,7,8>>
        if ( '<' != tupleBinaryStr[0] || '<' != tupleBinaryStr[1] )
        {
            return 42;
        }

        const std::string actorIdEndStr( ">>," );
        std::string::size_type binaryEnd = tupleBinaryStr.find( actorIdEndStr );
        if ( std::string::npos == binaryEnd )
        {
            return 43;
        }

        std::string actorIdStr( tupleBinaryStr.substr( 0, binaryEnd ) );
        std::string counterStr( tupleBinaryStr.substr( binaryEnd + actorIdEndStr.size() ) );

        // now convert the actor ID string to binary and create an Actor object
        basho::utils::ErlBuffer actorIdBuff;
        size_t actorIdBytes = basho::utils::ParseErlangBinaryString( actorIdStr, actorIdBuff );
        if ( basho::bigset::Actor::GetActorIdSizeInBytes() != actorIdBytes )
        {
            return 44;
        }

        basho::bigset::Actor actor;
        if ( !actor.SetId( actorIdBuff.GetCharBuffer(), actorIdBuff.GetBuffSize() ) )
        {
            return 45;
        }

        // now convert the counter string to a list of 1 or more integers
        if ( IsVersionVector )
        {
            basho::bigset::Counter counter;
            if ( !ParseCounterString( counterStr, counter ) )
            {
                return 46;
            }

            // now add the actor/counter to the version vector in the caller's BigsetClock object
            if ( !Clock.AddToVersionVector( actor, counter ) )
            {
                return 47;
            }
        }
        else
        {
            basho::bigset::CounterSet counterSet;
            // TODO: get the list of integers
        }

        // see if we have another 2-tuple
        openingBrace = ActorTupleStr.find( '{', closingBrace );
        if ( std::string::npos != openingBrace )
        {
            closingBrace = ActorTupleStr.find( '}', openingBrace + 1 );
            if ( std::string::npos == closingBrace )
            {
                return 49;
            }
        }
    } while ( std::string::npos != openingBrace );
    return 0;
}

// parses a bigset clock string in erlang term format, returning a BigsetClock object
static int
ParseBigsetClockTermString( const std::string& ClockStr, basho::bigset::BigsetClock& Clock )
{
    // initialize the caller's output object
    Clock.Clear();

    // A bigset clock is a 2-tuple {version_vector, dot_cloud}, where
    // version_vector is a list of 2-tuples of the form
    //
    //      [{Actor :: binary(8), Count :: pos_integer()}]
    //
    // and dot_cloud is a list of 2-tuples of the form
    //
    //      [{Actor :: binary(8), [pos_integer()]}]
    std::string clockStr( basho::utils::TrimWhitespace( ClockStr ) );
    std::string::size_type len = clockStr.size();
    if ( len < 7 ) // minimum size for an empty clock
    {
        return 31;
    }

    // ensure clockStr begins with "{[" and ends with "]}"
    if ( '{' != clockStr[0] || '[' != clockStr[1] )
    {
        return 32;
    }
    if ( ']' != clockStr[ len - 2 ] || '}' != clockStr[ len - 1 ] )
    {
        return 33;
    }

    // ensure clockStr is a 2-tuple of lists
    std::string listSepStr( "],[" );
    std::string::size_type listSep = clockStr.find( listSepStr );
    if ( std::string::npos == listSep )
    {
        // didn't find the separator for the two lists in the 2-tuple
        return 34;
    }
    if ( std::string::npos != clockStr.find( listSepStr, listSep + listSepStr.size() ) )
    {
        // we have another list tuple item, which shouldn't happen
        return 35;
    }

    std::string versionVectorStr( clockStr.substr( 2, listSep - 2 ) );
    std::string dotCloudStr( clockStr.substr( listSep + listSepStr.size(), len - listSep - listSepStr.size() - 2 ) );

    int retVal = ParseActorTupleString( versionVectorStr, true, Clock ); // true => this is a version vector string
    if ( 0 != retVal )
    {
        return retVal;
    }
    retVal = ParseActorTupleString( dotCloudStr, false, Clock ); // false => this is a dot cloud string
    if ( 0 != retVal )
    {
        return retVal;
    }
    return 0;
}

// each line of text in the file is expected to be of the form "Erlang tuple : <<bytes>>"
int // 0 => all good, else had an error
ProcessTermToBinaryLine( const std::string& Line, int LineNumber )
{
    // parse the line into the the two pieces we expect
    std::string::size_type separator = Line.find( " : " );
    if ( separator == std::string::npos )
    {
        printf( "Line %d (%s) not in the expected format\n", LineNumber, Line.c_str() );
        return 10;
    }

    std::string termStr   = basho::utils::TrimWhitespace( Line.substr( 0, separator ) );
    std::string binaryStr = basho::utils::TrimWhitespace( Line.substr( separator + 3 ) );

    printf( "LINE %02d: %s\n", LineNumber, termStr.c_str() );
    printf( "       : %s\n", binaryStr.c_str() );

    // parse the binaryStr into a buffer of bytes
    basho::utils::ErlBuffer binaryBuff;
    size_t binaryByteCount = basho::utils::ParseErlangBinaryString( binaryStr, binaryBuff );
    if ( 0 == binaryByteCount )
    {
        printf( "Unable to parse the bigset binary string '%s'\n", binaryStr.c_str() );
        return 11;
    }
    printf( "       : %zd bytes\n", binaryByteCount );

    // now create a BigsetClock object from the byte stream
    Slice binaryValue( binaryBuff.GetCharBuffer(), binaryByteCount );
    basho::bigset::BigsetClock bigsetClock;
    std::string errStr;
    if ( !basho::bigset::BigsetClock::ValueToBigsetClock( binaryValue, bigsetClock, errStr ) )
    {
        printf( "Unable to construct a BigsetClock from the binary string '%s'\nERROR: %s\n", binaryStr.c_str(), errStr.c_str() );
        return 12;
    }

    // get the string-version of the BigsetClock object and compare to the
    // Erlang tuple string from the file
    std::string bigsetClockStr = bigsetClock.ToString();
    if ( bigsetClockStr != termStr )
    {
        printf( "BigsetClock.ToString() '%s' does not match Erlang term from file '%s'\n", bigsetClockStr.c_str(), termStr.c_str() );
        return 13;
    }
    return 0;
}

// each line of text in the file is expected to be of the form "clock1 : clock2 : mergedClock"
int // 0 => all good, else had an error
ProcessMergedClocksLine( const std::string& Line, int LineNumber )
{
    // parse the line into the the three pieces we expect
    const std::string separatorStr( " : " );
    std::string::size_type separator1 = Line.find( separatorStr );
    if ( separator1 == std::string::npos )
    {
        printf( "Line %d (%s) not in the expected format\n", LineNumber, Line.c_str() );
        return 20;
    }
    std::string::size_type separator2 = Line.find( separatorStr, separator1 + separatorStr.size() );
    if ( separator2 == std::string::npos )
    {
        printf( "Line %d (%s) not in the expected format\n", LineNumber, Line.c_str() );
        return 21;
    }

    std::string clock1Str       = basho::utils::TrimWhitespace( Line.substr( 0, separator1 ) );
    std::string clock2Str       = basho::utils::TrimWhitespace( Line.substr( separator1 + 3, separator2 - separator1 - separatorStr.size() ) );
    std::string mergedClocksStr = basho::utils::TrimWhitespace( Line.substr( separator2 + 3 ) );

    printf( "LINE %02d: %s\n", LineNumber, clock1Str.c_str() );
    printf( "       : %s\n", clock2Str.c_str() );
    printf( "       : %s\n", mergedClocksStr.c_str() );

    basho::bigset::BigsetClock clock1;
    int retVal = ParseBigsetClockTermString( clock1Str, clock1 );
    if ( 0 != retVal )
    {
        printf( "Unable to parse clock string '%s'\n", clock1Str.c_str() );
        return retVal;
    }
    return 0;
}
