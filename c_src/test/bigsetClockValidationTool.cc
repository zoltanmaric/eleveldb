#include <stdio.h>
#include <iostream>
#include <fstream>

#include "../util/buffer.h"
#include "../BigsetClock.h"

typedef basho::utils::Buffer<1024> Buffer;

// local helper functions
static std::string TrimCharacters( const std::string& Str, const std::string& CharsToTrim );
static std::string TrimWhitespace( const std::string& Str );
static size_t ParseBinaryString( const std::string& BinaryStr, Buffer& BinaryBuff );
static int ProcessTermToBinaryLine( const std::string& Line, int LineNumber, bool AllowDuplicateClocks );

// print the command line usage for this application and call exit()
static void PrintUsageAndExit()
{
    fprintf( stderr, "USAGE: bigsetClockValidationTool [Options] [Action]\n" );
    fprintf( stderr, "\n" );
    fprintf( stderr, "Options: -a  Allow duplicate actors in a bigset clock\n" );
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
    bool allowDuplicateClocks = false;

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
                case 'a':
                    allowDuplicateClocks = true;
                    break;
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
    if ( allowDuplicateClocks )
    {
        printf( "Allowing duplicate actors in the clocks\n" );
    }

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

        // remove any trailing CR/LF chars from the line
        retVal = ProcessTermToBinaryLine( TrimWhitespace( line ), lineCount, allowDuplicateClocks );
    }
    printf( "Processed %d lines\n", lineCount );
    return retVal;
}

std::string TrimCharacters( const std::string& Str, const std::string& CharsToTrim )
{
    // find the index of the first non-CharsToTrim char in Str
    auto firstNonCharToTrim = Str.find_first_not_of( CharsToTrim );
    if ( firstNonCharToTrim == std::string::npos )
    {
        // Str is nothing but CharsToTrim (or possibly it's empty)
        return std::string();
    }

    // find the index of the last non-CharsToTrim char in Str
    auto lastNonCharToTrim = Str.find_last_not_of( CharsToTrim );
    if ( lastNonCharToTrim == std::string::npos )
    {
        // hmm...this shouldn't happen since we already verified above that we
        // have at least 1 non-CharsToTrim char; at any rate, we have nothing
        // to return
        return std::string();
    }

    return Str.substr( firstNonCharToTrim, lastNonCharToTrim - firstNonCharToTrim + 1 ); // second param is length of substring; be sure to include last non-CharsToTrim character
}

std::string TrimWhitespace( const std::string& Str )
{
    static const std::string c_WhiteSpaceChars = " \n\r\t";

    return TrimCharacters( Str, c_WhiteSpaceChars );
}

size_t // number of bytes put in BinaryBuff; 0 on error
ParseBinaryString( const std::string& BinaryStr, Buffer& BinaryBuff )
{
    // start by trimming "<<" and ">>" from BinaryStr
    static const std::string c_BinaryBracketChars = "<>";

    std::string rawBinaryStr = TrimCharacters( BinaryStr, c_BinaryBracketChars );
    //printf( "       :   %s\n", rawBinaryStr.c_str() );

    // rawBinaryStr is a series of integer string tokens, separated by commas
    size_t rawBytes = 0; // number of bytes we've parsed from rawBinaryStr
    const char* pCur = rawBinaryStr.c_str();
    while ( 0 != *pCur )
    {
        // convert the next string token to an integer
        char* pStrEnd = NULL;
        long value = strtol( pCur, &pStrEnd, 10 );

        // ensure we found an integer at pCur
        if ( pStrEnd == pCur || NULL == pStrEnd )
        {
            // we failed to find any integer characters at pCur
            rawBytes = 0;
            break;
        }

        // the only valid token-terminator characters are a comma or the terminating NULL
        char endChar = *pStrEnd;
        if ( ',' != endChar && 0 != endChar )
        {
            // found something in the string we didn't expect
            rawBytes = 0;
            break;
        }

        // ensure we have a byte value
        if ( value < 0 || value > 255 )
        {
            rawBytes = 0;
            break;
        }

        // store the parsed byte value in the caller's buffer
        ++rawBytes;
        if ( !BinaryBuff.EnsureSize( rawBytes ) )
        {
            rawBytes = 0;
            break;
        }
        BinaryBuff.GetCharBuffer()[ rawBytes - 1 ] = static_cast<char>( value );

        // move to the next integer
        pCur = pStrEnd;
        if ( 0 != endChar )
        {
            ++pCur; // we're not at the end of the string, so move past the comma separator char
        }
    }
    return rawBytes;
}

// each line of text in the file is expected to be of the form "Erlang tuple : <<bytes>>"
int // 0 => all good, else had an error
ProcessTermToBinaryLine( const std::string& Line, int LineNumber, bool AllowDuplicateClocks )
{
    // parse the line into the the two pieces we expect
    auto separator = Line.find( " : " );
    if ( separator == std::string::npos )
    {
        printf( "Line %d (%s) not in the expected format\n", LineNumber, Line.c_str() );
        return 10;
    }

    std::string termStr = Line.substr( 0, separator );
    std::string binaryStr = Line.substr( separator + 3 );

    printf( "LINE %02d: %s\n", LineNumber, termStr.c_str() );
    printf( "       : %s\n", binaryStr.c_str() );

    // parse the binaryStr into a buffer of bytes
    Buffer binaryBuff;
    size_t binaryByteCount = ParseBinaryString( binaryStr, binaryBuff );
    if ( 0 == binaryByteCount )
    {
        printf( "Unable to parse the bigset binary string '%s'\n", binaryStr.c_str() );
        return 11;
    }
    printf( "       : %zd bytes\n", binaryByteCount );

    // now create a BigsetClock object from the byte stream
    Slice binaryValue( binaryBuff.GetCharBuffer(), binaryByteCount );
    basho::bigset::BigsetClock bigsetClock( AllowDuplicateClocks );
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