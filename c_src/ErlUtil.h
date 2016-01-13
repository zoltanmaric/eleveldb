// $Id: $

#ifndef ELEVELDB_ERLUTIL_H
#define ELEVELDB_ERLUTIL_H

/**
 * @file ErlUtil.h
 * 
 * Tagged: Wed Sep  2 14:46:45 PDT 2015
 * 
 * @version: $Revision: $, $Date: $
 * 
 * @author /bin/bash: username: command not found
 */
#include "erl_nif.h"
#include "workitems.h"

#include <string>
#include <cctype>
#include <climits>

namespace eleveldb {

    class ErlUtil {
    public:
      
        // Constructor.

        ErlUtil(ErlNifEnv* env=0);
        ErlUtil(ErlNifEnv* env, ERL_NIF_TERM term);
      
        // Destructor.

        virtual ~ErlUtil();

        //=======================================================================
        // ENIF interface
        //=======================================================================

        void setEnv(ErlNifEnv* env);
        void setTerm(ERL_NIF_TERM term);

        bool isAtom();
        bool isAtom(ERL_NIF_TERM term); 
        static bool isAtom(ErlNifEnv* env, ERL_NIF_TERM term);

        bool isBinary();
        bool isBinary(ERL_NIF_TERM term); 
        static bool isBinary(ErlNifEnv* env, ERL_NIF_TERM term);

        bool isInspectableAsBinary();
        bool isInspectableAsBinary(ERL_NIF_TERM term); 
        static bool isInspectableAsBinary(ErlNifEnv* env, ERL_NIF_TERM term);

        bool isList();
        bool isList(ERL_NIF_TERM term); 
        static bool isList(ErlNifEnv* env, ERL_NIF_TERM term);

        bool isTuple();
        bool isTuple(ERL_NIF_TERM term); 
        static bool isTuple(ErlNifEnv* env, ERL_NIF_TERM term);

        bool isString();
        bool isString(ERL_NIF_TERM term); 
        static bool isString(ErlNifEnv* env, ERL_NIF_TERM term);

        bool isNumber();
        bool isNumber(ERL_NIF_TERM term); 
        static bool isNumber(ErlNifEnv* env, ERL_NIF_TERM term);

        bool isDouble();
        bool isDouble(ERL_NIF_TERM term); 
        static bool isDouble(ErlNifEnv* env, ERL_NIF_TERM term);

        bool isInt64();
        bool isInt64(ERL_NIF_TERM term); 
        static bool isInt64(ErlNifEnv* env, ERL_NIF_TERM term);

        bool isUint64();
        bool isUint64(ERL_NIF_TERM term); 
        static bool isUint64(ErlNifEnv* env, ERL_NIF_TERM term);

        bool isBool();
        bool isBool(ERL_NIF_TERM term); 
        static bool isBool(ErlNifEnv* env, ERL_NIF_TERM term);

        unsigned listLength();
        unsigned listLength(ERL_NIF_TERM term); 
        static unsigned listLength(ErlNifEnv* env, ERL_NIF_TERM term); 

        double getDouble();
        double getDouble(ERL_NIF_TERM term);
        static double getDouble(ErlNifEnv* env, ERL_NIF_TERM term);

        int64_t getInt64();
        int64_t getInt64(ERL_NIF_TERM term);
        static int64_t getInt64(ErlNifEnv* env, ERL_NIF_TERM term);

        uint64_t getUint64();
        uint64_t getUint64(ERL_NIF_TERM term);
        static uint64_t getUint64(ErlNifEnv* env, ERL_NIF_TERM term);

        bool getBool();
        bool getBool(ERL_NIF_TERM term);
        static bool getBool(ErlNifEnv* env, ERL_NIF_TERM term);

        std::string getAtom();
        std::string getAtom(ERL_NIF_TERM term);
        static std::string getAtom(ErlNifEnv* env, ERL_NIF_TERM term, bool toLower=false);

        std::vector<unsigned char> getBinary();
        std::vector<unsigned char> getBinary(ERL_NIF_TERM term);
        static std::vector<unsigned char> getBinary(ErlNifEnv* env, ERL_NIF_TERM term);

        std::vector<unsigned char> getAsBinary();
        std::vector<unsigned char> getAsBinary(ERL_NIF_TERM term);
        static std::vector<unsigned char> getAsBinary(ErlNifEnv* env, ERL_NIF_TERM term);

        std::string getString();
        std::string getString(ERL_NIF_TERM term);
        static std::string getString(ErlNifEnv* env, ERL_NIF_TERM term);

        std::vector<ERL_NIF_TERM> getListCells();
        std::vector<ERL_NIF_TERM> getListCells(ERL_NIF_TERM term);
        static std::vector<ERL_NIF_TERM> getListCells(ErlNifEnv* env, 
                                                      ERL_NIF_TERM term);

        std::vector<ERL_NIF_TERM> getTupleCells();
        std::vector<ERL_NIF_TERM> getTupleCells(ERL_NIF_TERM term);
        static std::vector<ERL_NIF_TERM> getTupleCells(ErlNifEnv* env, ERL_NIF_TERM term);

        // Given an iolist consisting of {name, value} tuples, return a
        // vector of name/value pairs

        std::vector<std::pair<std::string, ERL_NIF_TERM> > getListTuples();
        std::vector<std::pair<std::string, ERL_NIF_TERM> > getListTuples(ERL_NIF_TERM term);

        void decodeRiakObject(ERL_NIF_TERM obj, ERL_NIF_TERM encoding);
        void parseSiblingData(unsigned char* ptr, unsigned len);
        void parseSiblingDataMsgpack(unsigned char* ptr, unsigned len);

        static int32_t  getValAsInt32(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);
        static int64_t  getValAsInt64(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);
        static uint32_t getValAsUint32(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);
        static uint8_t  getValAsUint8(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);
        static uint64_t getValAsUint64(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);
        static double   getValAsDouble(ErlNifEnv* env, ERL_NIF_TERM term, bool exact=true);

        std::string formatTerm();
        std::string formatTerm(ERL_NIF_TERM term);
        static std::string formatTerm(ErlNifEnv* env, ERL_NIF_TERM term);

        static std::string formatAtom(  ErlNifEnv* env, ERL_NIF_TERM term);
        static std::string formatBinary(ErlNifEnv* env, ERL_NIF_TERM term);
        static std::string formatList(  ErlNifEnv* env, ERL_NIF_TERM term);
        static std::string formatNumber(ErlNifEnv* env, ERL_NIF_TERM term);
        static std::string formatString(ErlNifEnv* env, ERL_NIF_TERM term);
        static std::string formatTuple( ErlNifEnv* env, ERL_NIF_TERM term);
        static std::string formatTupleVec(ErlNifEnv* env, std::vector<ERL_NIF_TERM>& tuple);

        static std::string formatBinary(char* data, size_t size);
        static std::string formatBinary(const char* data, size_t size);
        static std::string formatBinary(const unsigned char* data, size_t size);
        static std::string formatBinary(unsigned char* data, size_t size);
        static std::string formatBinary(std::string& str);
        static std::string formatBinary(const std::string& str);
        
    private:

        void checkEnv();
        void checkTerm();

        ErlNifEnv* env_;

        bool hasTerm_;
        ERL_NIF_TERM term_;

    }; // End class ErlUtil

} // End namespace eleveldb



#endif // End #ifndef ELEVELDB_ERLUTIL_H
