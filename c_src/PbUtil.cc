#include "ErlUtil.h"
#include "PbUtil.h"

#include <cmath>
#include <stddef.h>
#include <stdint.h>


using namespace std;
using namespace eleveldb;
using namespace google::protobuf;

//=======================================================================
// Initialize static maps of conversion functions here
//=======================================================================

std::map<MapEntry_Type, PB_CONV_UINT8_FN(*)>  
PbUtil::uint8ConvMap_ = PbUtil::constructUint8Map();

std::map<MapEntry_Type, PB_CONV_INT64_FN(*)>  
PbUtil::int64ConvMap_ = PbUtil::constructInt64Map();

std::map<MapEntry_Type, PB_CONV_UINT64_FN(*)> 
PbUtil::uint64ConvMap_ = PbUtil::constructUint64Map();

std::map<MapEntry_Type, PB_CONV_DOUBLE_FN(*)> 
PbUtil::doubleConvMap_ = PbUtil::constructDoubleMap();

//=======================================================================
// A macro for declaring a template convert specialization
//=======================================================================

#define CONVERT_DECL(typeTo, typeFrom, accessor, validation)             \
    namespace eleveldb {                                                \
        template<>                                                      \
        typeTo PbUtil::convert<typeTo, typeFrom>(const MapEntry& entry) \
        {                                                               \
            typeFrom val;                                               \
            try {                                                       \
                val = entry.accessor();                                 \
                validation;                                             \
            } catch(...) {                                              \
                ThrowRuntimeError("Object of type " << typeOf(entry) << " can't be represented as a " << #typeTo); \
            }                                                           \
            return (typeTo) val;                                        \
        }                                                               \
    }                                                                   \

//=======================================================================
// A macro for constructing a map of conversion functions to the
// specified type
//=======================================================================

#define CONSTRUCT_PB_CONV_MAP(typeTo)                             \
    convMap[MapEntry_Type_BOOL]  = PbUtil::convert<typeTo, bool>;    \
    convMap[MapEntry_Type_INT]   = PbUtil::convert<typeTo, int64_t>; \
    convMap[MapEntry_Type_FLOAT] = PbUtil::convert<typeTo, double>;     \
    convMap[MapEntry_Type_TIMESTAMP] = PbUtil::convert<typeTo, uint64_t>; \
    
/**.......................................................................
 * Constructor.
 */
PbUtil::PbUtil() {}

/**.......................................................................
 * Destructor.
 */
PbUtil::~PbUtil() {}

/**.......................................................................
 * Encode a map
 */
ERL_NIF_TERM PbUtil::encodeMap(ErlNifEnv* env, ERL_NIF_TERM term)
{
    MyFieldMap fieldMap;

    std::vector<ERL_NIF_TERM> cells = ErlUtil::getListCells(env, term);

    for(unsigned i=0; i < cells.size(); i++) {
        std::vector<ERL_NIF_TERM> tuple = ErlUtil::getTupleCells(env, cells[i]);

        if(tuple.size() != 2) {
            ThrowRuntimeError("Tuple " << ErlUtil::formatTerm(env, cells[i]) << " is not a {string, val} formatted tuple");
        }

        if(!ErlUtil::isString(env, tuple[0])) {
            ThrowRuntimeError("First element " << ErlUtil::formatTerm(env, tuple[0]) << " is not a valid string");
        }

        std::string name = ErlUtil::getString(env, tuple[0]);

        MyMapEntry entry = fieldMap.addEntry(name);

        if(ErlUtil::isBinary(env, tuple[1])) {
            std::vector<unsigned char> bin = ErlUtil::getBinary(env, tuple[1]);
            entry.setVal(bin);

        } else if(ErlUtil::isDouble(env, tuple[1])) {
            entry.setVal(ErlUtil::getDouble(env, tuple[1]));

        } else if(ErlUtil::isUint64(env, tuple[1])) {
            entry.setVal(ErlUtil::getUint64(env, tuple[1]));

        } else if(ErlUtil::isInt64(env, tuple[1])) {
            entry.setVal(ErlUtil::getInt64(env, tuple[1]));

        } else if(ErlUtil::isBool(env, tuple[1])) {
            entry.setVal(ErlUtil::getBool(env, tuple[1]));

            // Anything else (list, etc), we try to encode as a binary if possible

        } else {

            try {
                std::vector<unsigned char> bin = ErlUtil::getAsBinary(env, tuple[1]);
                entry.setVal(bin);
            } catch(...) {
                ThrowRuntimeError("Value can't be encoded as a PB FieldMap");
            }
        }
    }

    std::string str;
    fieldMap.SerializeToString(&str);

    ErlNifBinary bin;
    enif_alloc_binary(str.size(), &bin);
    memcpy(bin.data, &str[0], str.size());

    return enif_make_binary(env, &bin);
}

/**.......................................................................
 * Decode a PB-encoded map
 */
void PbUtil::decodeMap(ErlNifEnv* env, ERL_NIF_TERM term)
{
    MyFieldMap fieldMap;

    ErlNifBinary bin;
    if(!enif_inspect_binary(env, term, &bin))
        ThrowRuntimeError("Not a binary: " << ErlUtil::formatTerm(env, term));

    decodeMap(fieldMap, bin.data, bin.size);
}

ERL_NIF_TERM PbUtil::formatVal(ErlNifEnv* env, const MapEntry& entry)
{
    ERL_NIF_TERM retVal;

    switch (entry.fieldtype_()) {
    case MapEntry_Type_BOOL:
    {
        bool val = entry.boolval_();
        retVal = enif_make_atom(env, (val ? "true" : "false"));
    } 
    break;

    case MapEntry_Type_FLOAT:
    {
        double val = entry.doubleval_();
        retVal = enif_make_double(env, val);
    } 
    break;

    case MapEntry_Type_INT:
    {
        int64_t val = entry.intval_();
        retVal = enif_make_int64(env, val);
    } 
    break;

    case MapEntry_Type_TIMESTAMP:
    {
        uint64_t val = entry.timestampval_();
        retVal = enif_make_uint64(env, val);
    } 
    break;

    case MapEntry_Type_BINARY:
    {
        const std::string& str = entry.byteval_();
        unsigned char* data = enif_make_new_binary(env, str.size(), &retVal);
        memcpy(data, &str[0], str.size());
    } 
    break;

    default:
        ThrowRuntimeError("Unsupported value type encountered: " << entry.fieldtype_());
        break;
    }

    return retVal;
}

ERL_NIF_TERM PbUtil::formatStringAsBinary(ErlNifEnv* env, const std::string& str)
{
    ERL_NIF_TERM retVal;
    unsigned char* data = enif_make_new_binary(env, str.size(), &retVal);
    memcpy(data, &str[0], str.size());
    return retVal;
}

ERL_NIF_TERM PbUtil::formatTuple(ErlNifEnv* env, const MapEntry& entry)
{
    ERL_NIF_TERM fieldName, fieldVal;
    fieldName = formatStringAsBinary(env, entry.fieldname_());
    fieldVal  = formatVal(env, entry);

    return enif_make_tuple2(env, fieldName, fieldVal);
}

/**.......................................................................
 * Decode a PB-encoded map and return as an erlang term
 */
ERL_NIF_TERM PbUtil::decodeToErl(ErlNifEnv* env, ERL_NIF_TERM term)
{
    MyFieldMap fieldMap;

    ErlNifBinary bin;
    if(!enif_inspect_binary(env, term, &bin))
        ThrowRuntimeError("Not a binary: " << ErlUtil::formatTerm(env, term));

    decodeMap(fieldMap, bin.data, bin.size);

    unsigned size = fieldMap.size();
    ERL_NIF_TERM arr[size];

    for(unsigned i=0; i < size; i++) {
        const MapEntry& entry = fieldMap.getEntry(i);
        arr[i] = formatTuple(env, entry);
    }

    return enif_make_list_from_array(env, arr, size);
}


/**.......................................................................
 * Decode a PB-encoded map
 */
void PbUtil::decodeMap(MyFieldMap& fieldMap, unsigned char* data, unsigned size)
{
    std::string str;
    str.resize(size);
    memcpy(&str[0], data, size);
    fieldMap.ParseFromString(str);
}

/**.......................................................................
 * Parse a map encoded as a msgpack object into component keys and
 * datatypes
 */
std::map<std::string, DataType::Type> 
PbUtil::parseMap(MyFieldMap& fieldMap, unsigned char* data, unsigned size)
{
    decodeMap(fieldMap, data, size);

    std::map<std::string, eleveldb::DataType::Type> fieldTypes;

    for(unsigned i=0; i < fieldMap.fieldMap_->entries__size(); i++) {
        const MapEntry& entry = fieldMap.fieldMap_->entries_(i);
        fieldTypes[entry.fieldname_()] = typeOf(entry);
    }

    return fieldTypes;
}

/**.......................................................................
 * Constructor for MapEntry container
 */
PbUtil::MyMapEntry::MyMapEntry(MapEntry* entry)
{
    entry_ = entry;
}

/**.......................................................................
 * Entry setVal methods for supported types
 */
void PbUtil::MyMapEntry::setVal(std::vector<unsigned char>& vec)
{
    setVal(&vec[0], vec.size());
}

void PbUtil::MyMapEntry::setVal(unsigned char* buf, size_t size)
{
    entry_->set_byteval_(buf, size);
    entry_->set_fieldtype_(MapEntry_Type_BINARY);
}

void PbUtil::MyMapEntry::setVal(uint64_t val)
{
    entry_->set_timestampval_(val);
    entry_->set_fieldtype_(MapEntry_Type_TIMESTAMP);
}

void PbUtil::MyMapEntry::setVal(int64_t val)
{
    entry_->set_intval_(val);
    entry_->set_fieldtype_(MapEntry_Type_INT);
}

void PbUtil::MyMapEntry::setVal(double val)
{
    entry_->set_doubleval_(val);
    entry_->set_fieldtype_(MapEntry_Type_FLOAT);
}

void PbUtil::MyMapEntry::setVal(bool val)
{
    entry_->set_boolval_(val);
    entry_->set_fieldtype_(MapEntry_Type_BOOL);
}

/**.......................................................................
 * Initialize a FieldMap from the passed arena on construction only
 */
PbUtil::MyFieldMap::MyFieldMap(Arena& arena)
{
    arena_    = 0;
    fieldMap_ = Arena::Create<FieldMap>(&arena);
}

PbUtil::MyFieldMap::MyFieldMap()
{
    arena_    = new google::protobuf::Arena();
    fieldMap_ = Arena::Create<FieldMap>(arena_);
}

PbUtil::MyFieldMap::~MyFieldMap()
{
    if(arena_) {
        delete arena_;
        arena_ = 0;
    }
}

bool PbUtil::MyFieldMap::SerializeToOstream(std::ostream* output) const
{
    return fieldMap_->SerializeToOstream(output);
}

bool PbUtil::MyFieldMap::SerializeToString(std::string* output) const
{
    return fieldMap_->SerializeToString(output);
}

bool PbUtil::MyFieldMap::ParseFromString(const std::string& input)
{
    return fieldMap_->ParseFromString(input);
}

unsigned PbUtil::MyFieldMap::size()
{
    return fieldMap_->entries__size();
}

const MapEntry& PbUtil::MyFieldMap::getEntry(unsigned index)
{
    return fieldMap_->entries_(index);
}

/**.......................................................................
 * Add an entry to this map.  If the entry already exists, we don't
 * create a new one
 */
PbUtil::MyMapEntry PbUtil::MyFieldMap::addEntry(std::string name)
{
    MapEntry* entry = 0;

    //------------------------------------------------------------
    // See if it's already in our map -- else allocate a new one
    //------------------------------------------------------------

    if(entries_.find(name) != entries_.end()) {
        entry = entries_[name];
    } else {
        entry = fieldMap_->add_entries_();
        entry->set_fieldname_(name);
    }

    entries_[name] = entry;

    MyMapEntry mme(entry);

    return mme;
}

/**.......................................................................
 * Utility print operators
 */
std::ostream& eleveldb::operator<<(std::ostream& os, const MapEntry_Type& type)
{
    switch (type) {
    case MapEntry_Type_BOOL:
        os << "BOOL";
        break;
    case MapEntry_Type_INT:
        os << "INT";
        break;
    case MapEntry_Type_FLOAT:
        os << "FLOAT";
        break;
    case MapEntry_Type_TIMESTAMP:
        os << "TIMESTAMP";
        break;
    case MapEntry_Type_BINARY:
        os << "BINARY";
        break;
    default:
        os << "UNKNOWN";
        break;
    }

    return os;
}

std::ostream& eleveldb::operator<<(std::ostream& os, const PbUtil::MyMapEntry& mme)
{
    os << mme.entry_;
    return os;
}

std::ostream& eleveldb::operator<<(std::ostream& os, const MapEntry& entry)
{
    return operator<<(os, (MapEntry&)entry);
}

std::ostream& eleveldb::operator<<(std::ostream& os, MapEntry& entry)
{
    os << entry.fieldname_() << ":" << std::endl;
    os << "\r  type = " << entry.fieldtype_() << std::endl;
    os << "\r  val  = ";

    switch (entry.fieldtype_()) {
    case MapEntry_Type_BOOL:
        os << entry.boolval_();
        break;
    case MapEntry_Type_INT:
        os << entry.intval_();
        break;
    case MapEntry_Type_FLOAT:
        os << entry.doubleval_();
        break;
    case MapEntry_Type_TIMESTAMP:
        os << entry.timestampval_();
        break;
    case MapEntry_Type_BINARY:
    {
        const std::string& val = entry.byteval_();
        os << ErlUtil::formatBinary(val);
    }
    break;
    default:
        os << "UNKNOWN";
        break;
    }

    os << std::endl;

    return os;
}

std::ostream& eleveldb::operator<<(std::ostream& os, PbUtil::MyFieldMap& fm)
{
    os << "Map size = " << fm.fieldMap_->entries__size() << std::endl;

    for(unsigned i=0; i < fm.fieldMap_->entries__size(); i++) {
        const MapEntry& entry = fm.fieldMap_->entries_(i);
        os << "\rEntry " << i << " = " << entry << std::endl;
    }

    return os;
}

DataType::Type PbUtil::typeOf(const MapEntry& entry)
{
    MapEntry_Type type = entry.fieldtype_();

    switch (type) {
    case MapEntry_Type_BOOL:
        return DataType::BOOL;
        break;
    case MapEntry_Type_INT:
        return DataType::INT64;
        break;
    case MapEntry_Type_FLOAT:
        return DataType::DOUBLE;
        break;
    case MapEntry_Type_TIMESTAMP:
        return DataType::UINT64;
        break;
    case MapEntry_Type_BINARY:
        return DataType::UCHAR_PTR;
        break;
    default:
        return DataType::UNKNOWN;
        break;
    }
}

//=======================================================================
// Templatized convert specializations
//=======================================================================

//------------------------------------------------------------
// Conversions to uint8_t
//------------------------------------------------------------

CONVERT_DECL(uint8_t, bool, boolval_,
             return val;
    );

CONVERT_DECL(uint8_t, int64_t, intval_,
             if(val >= 0 && val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, uint64_t, timestampval_,
             if(val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, double, doubleval_,
             if(val >= 0.0 && val <= (double)UCHAR_MAX && !(fabs(val - (uint8_t)val) > 0.0))
                 return val;
    );

//------------------------------------------------------------
// Conversions to int64_t
//------------------------------------------------------------

CONVERT_DECL(int64_t, bool, boolval_,
             return val;
    );

CONVERT_DECL(int64_t, int64_t, intval_,
             return val;
    );

CONVERT_DECL(int64_t, uint64_t, timestampval_,
             if(val <= LLONG_MAX)
                 return val;
    );

CONVERT_DECL(int64_t, double, doubleval_,
             if(val <= (double)LLONG_MAX && val >= (double)LLONG_MIN && !(fabs(val - (int64_t)val) > 0.0))
                 return val;
    );


//------------------------------------------------------------
// Conversions to uint64_t
//------------------------------------------------------------

CONVERT_DECL(uint64_t, bool, boolval_,
             return val;
    );

CONVERT_DECL(uint64_t, int64_t, intval_,
             if(val >= 0)
                 return val;
    );

CONVERT_DECL(uint64_t, uint64_t, timestampval_,
             return val;
    );

CONVERT_DECL(uint64_t, double, doubleval_,
             if(val >= 0.0 && val <= (double)ULONG_MAX && !(fabs(val - (uint64_t)val) > 0.0))
                 return val;
    );

//------------------------------------------------------------
// Conversions to double
//------------------------------------------------------------

CONVERT_DECL(double, bool, boolval_,
             return val;
    );

CONVERT_DECL(double, int64_t, intval_,
             return val;
    );

CONVERT_DECL(double, uint64_t, timestampval_,
             return val;
    );

CONVERT_DECL(double, double, doubleval_,
             return val;
    );


uint8_t PbUtil::objectToUint8(const MapEntry& entry)
{
    MapEntry_Type type = entry.fieldtype_();
    if(PbUtil::uint8ConvMap_.find(type) != PbUtil::uint8ConvMap_.end())
        return PbUtil::uint8ConvMap_[type](entry);
    else 
        ThrowRuntimeError("Object of type " << type << " can't be converted to a uint8_t type");
    return 0;
}

int64_t PbUtil::objectToInt64(const MapEntry& entry)
{
    MapEntry_Type type = entry.fieldtype_();
    if(PbUtil::int64ConvMap_.find(type) != PbUtil::int64ConvMap_.end())
        return PbUtil::int64ConvMap_[type](entry);
    else 
        ThrowRuntimeError("Object of type " << type << " can't be converted to a int64_t type");
    return 0;
}

uint64_t PbUtil::objectToUint64(const MapEntry& entry)
{
    MapEntry_Type type = entry.fieldtype_();
    if(PbUtil::uint64ConvMap_.find(type) != PbUtil::uint64ConvMap_.end())
        return PbUtil::uint64ConvMap_[type](entry);
    else 
        ThrowRuntimeError("Object of type " << type << " can't be converted to a uint64_t type");
    return 0;
}

double PbUtil::objectToDouble(const MapEntry& entry)
{
    MapEntry_Type type = entry.fieldtype_();
    if(PbUtil::doubleConvMap_.find(type) != PbUtil::doubleConvMap_.end())
        return PbUtil::doubleConvMap_[type](entry);
    else 
        ThrowRuntimeError("Object of type " << type << " can't be converted to a double type");
    return 0;
}

std::map<MapEntry_Type, PB_CONV_UINT8_FN(*)>  PbUtil::constructUint8Map()
{
    std::map<MapEntry_Type, PB_CONV_UINT8_FN(*)> convMap;
    CONSTRUCT_PB_CONV_MAP(uint8_t);
    return convMap;
}

std::map<MapEntry_Type, PB_CONV_INT64_FN(*)>  PbUtil::constructInt64Map()
{
    std::map<MapEntry_Type, PB_CONV_INT64_FN(*)> convMap;
    CONSTRUCT_PB_CONV_MAP(int64_t);
    return convMap;
}

std::map<MapEntry_Type, PB_CONV_UINT64_FN(*)>  PbUtil::constructUint64Map()
{
    std::map<MapEntry_Type, PB_CONV_UINT64_FN(*)> convMap;
    CONSTRUCT_PB_CONV_MAP(uint64_t);
    return convMap;
}

std::map<MapEntry_Type, PB_CONV_DOUBLE_FN(*)>  PbUtil::constructDoubleMap()
{
    std::map<MapEntry_Type, PB_CONV_DOUBLE_FN(*)> convMap;
    CONSTRUCT_PB_CONV_MAP(double);
    return convMap;
}
