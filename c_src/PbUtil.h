#ifndef ELEVELDB_PBUTIL_H
#define ELEVELDB_PBUTIL_H

/**
 * @file PbUtil.h
 * 
 * Tagged: Tue Oct  6 09:53:43 PDT 2015
 * 
 * @version: $Revision: $, $Date: $
 * 
 * @author /bin/bash: username: command not found
 */
#include <vector>

#include "google/protobuf/arena.h"

#include "erl_nif.h"

#include "myTest.pb.h"

#define PB_CONV_UINT8_FN(fn)  uint8_t  (fn)(const MapEntry& entry)
#define PB_CONV_INT64_FN(fn)  int64_t  (fn)(const MapEntry& entry)
#define PB_CONV_UINT64_FN(fn) uint64_t (fn)(const MapEntry& entry)
#define PB_CONV_DOUBLE_FN(fn) double   (fn)(const MapEntry& entry)

namespace eleveldb {

    class PbUtil {
    public:

        //============================================================
        // Class for managing a single entry
        //============================================================
        
        class MyMapEntry {
        public:
            
            MyMapEntry(MapEntry* entry);
            
            friend std::ostream& operator<<(std::ostream& os, MapEntry& entry);
            friend std::ostream& operator<<(std::ostream& os, const MapEntry& entry);

            friend std::ostream& operator<<(std::ostream& os, const MyMapEntry& entry);
            friend std::ostream& operator<<(std::ostream& os, const MapEntry_Type& type);
            
            void setVal(uint64_t val);
            void setVal(int64_t val);
            void setVal(double val);
            void setVal(bool val);
            void setVal(unsigned char* buf, size_t size);
            void setVal(std::vector<unsigned char>& vec);

        private:
            
            MapEntry* entry_;
        };
        
        //============================================================
        // Class for managing a field map
        //============================================================
        
        class MyFieldMap {
        public:
            
            friend class PbUtil;

            MyFieldMap();
            MyFieldMap(google::protobuf::Arena& arena);
            
            ~MyFieldMap();
            
            MyMapEntry addEntry(std::string name);
            
            friend std::ostream& operator<<(std::ostream& os, MyFieldMap& fm);
            bool SerializeToOstream(std::ostream* output) const;
            bool SerializeToString(std::string* output) const;
            bool ParseFromString(const std::string& input);

            unsigned size();
            const MapEntry& getEntry(unsigned index);

        private:
            
            google::protobuf::Arena* arena_;
            FieldMap* fieldMap_;
            std::map<std::string, MapEntry*> entries_;
        };
        
        
        /**
         * Constructor.
         */
        PbUtil();
        
        /**
         * Destructor.
         */
        virtual ~PbUtil();
        
        static ERL_NIF_TERM encodeMap(ErlNifEnv* env, ERL_NIF_TERM term);
        static void decodeMap(ErlNifEnv* env, ERL_NIF_TERM term);
        static void decodeMap(MyFieldMap& fieldMap, unsigned char* data, unsigned size);

        static std::map<std::string, DataType::Type> 
            parseMap(MyFieldMap& fieldMap, unsigned char* data, unsigned size);

        static DataType::Type typeOf(const MapEntry& entry);
        
        // Return a map of conversion functions

        static std::map<MapEntry_Type, PB_CONV_UINT8_FN(*)>  constructUint8Map();
        static std::map<MapEntry_Type, PB_CONV_INT64_FN(*)>  constructInt64Map();
        static std::map<MapEntry_Type, PB_CONV_UINT64_FN(*)> constructUint64Map();
        static std::map<MapEntry_Type, PB_CONV_DOUBLE_FN(*)> constructDoubleMap();
            
        // Static maps of conversion functions
            
        static std::map<MapEntry_Type, PB_CONV_UINT8_FN(*)>  uint8ConvMap_;
        static std::map<MapEntry_Type, PB_CONV_INT64_FN(*)>  int64ConvMap_;
        static std::map<MapEntry_Type, PB_CONV_UINT64_FN(*)> uint64ConvMap_;
        static std::map<MapEntry_Type, PB_CONV_DOUBLE_FN(*)> doubleConvMap_;
            
        // Object conversion functions
            
        static uint8_t  objectToUint8(const MapEntry& entry);
        static int64_t  objectToInt64(const MapEntry& entry);
        static uint64_t objectToUint64(const MapEntry& entry);
        static double   objectToDouble(const MapEntry& entry);
            
        // Templatized type-conversion functions
            
        template<typename typeTo, typename typeFrom> 
            static typeTo convert(const MapEntry& entry);

        static ERL_NIF_TERM decodeToErl(ErlNifEnv* env, ERL_NIF_TERM term);
        static ERL_NIF_TERM formatTuple(ErlNifEnv* env, const MapEntry& entry);
        static ERL_NIF_TERM formatStringAsBinary(ErlNifEnv* env, const std::string& str);
        static ERL_NIF_TERM formatVal(ErlNifEnv* env, const MapEntry& entry);
        
    private:
        
    }; // End class PbUtil

    std::ostream& operator<<(std::ostream& os, PbUtil::MyFieldMap& fm);
    std::ostream& operator<<(std::ostream& os, const PbUtil::MyMapEntry& entry);

    std::ostream& operator<<(std::ostream& os, const MapEntry& entry);
    std::ostream& operator<<(std::ostream& os, MapEntry& entry);
    std::ostream& operator<<(std::ostream& os, const MapEntry_Type& type);
    
} // End namespace eleveldb



#endif // End #ifndef ELEVELDB_PBUTIL_H
