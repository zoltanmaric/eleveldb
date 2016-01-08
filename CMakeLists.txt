project(eleveldb)
cmake_minimum_required(VERSION 3.3)

add_subdirectory("c_src/leveldb")

# OS detection
add_definitions(-DLEVELDB_PLATFORM_POSIX)
if(APPLE)
    add_definitions(-DOS_MACOSX)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_definitions(-DOS_LINUX)
elseif(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    add_definitions(-DOS_FREEBSD)
endif()

# eleveldb source files
#
# NOTE: selected header files are included to make CLion happy
include_directories(${CMAKE_CURRENT_SOURCE_DIR} c_src/leveldb/include /Users/pplace/erlang/R16B02/usr/include)
set(ELEVELDB_SRCS
    c_src/atoms.h
    c_src/BigsetAccumulator.cc
    c_src/cmp.cc
    c_src/CmpUtil.cc
    c_src/CrdtUtils.h
    c_src/DataType.cc
    c_src/detail.hpp
    c_src/eleveldb.cc
    c_src/Encoding.cc
    c_src/ErlUtil.cc
    c_src/extractor.cc
    c_src/filter.h
    c_src/filter_parser.cc
    c_src/mutex.h
    c_src/refobjects.cc
    c_src/StringBuf.cc
    c_src/threading.cc
    c_src/work_result.hpp
    c_src/workitems.cc
    )

add_library(eleveldb ${ELEVELDB_SRCS})
