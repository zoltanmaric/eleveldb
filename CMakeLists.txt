project(eleveldb)
cmake_minimum_required(VERSION 3.3)

# set GTEST_DIR and ERLANG_INCLUDE_DIR in the shell env?
set(GTEST_DIR "/Users/pplace/github/googletest/googletest")
set(ERLANG_INCLUDE_DIR "/Users/pplace/erlang/R16B02/usr/include")

set(CMAKE_CXX_FLAGS "-Wall -O0 -g3 -std=c++11 -stdlib=libc++ -Wno-deprecated -pthread")

add_subdirectory("c_src/leveldb")

# OS detection
add_definitions(-DLEVELDB_PLATFORM_POSIX -DSNAPPY -mmacosx-version-min=10.8)
if (APPLE)
    add_definitions(-DOS_MACOSX)
elseif (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_definitions(-DOS_LINUX)
elseif (CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    add_definitions(-DOS_FREEBSD)
endif ()

add_library(gtest_all STATIC ${GTEST_DIR}/src/gtest-all.cc)
add_library(gtest_main STATIC ${GTEST_DIR}/src/gtest_main.cc)

# eleveldb source files
#
# NOTE: selected header files are included to make CLion happy
include_directories(${CMAKE_CURRENT_SOURCE_DIR} c_src c_src/util c_src/leveldb/include ${ERLANG_INCLUDE_DIR} ${GTEST_DIR}/include ${GTEST_DIR})
set(ELEVELDB_SRCS
        c_src/atoms.h
        c_src/BigsetAccumulator.cc
        c_src/BigsetClock.cc
        c_src/BigsetKey.cc
        c_src/cmp.cc
        c_src/CmpUtil.cc
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
        c_src/util/buffer.h
        c_src/util/erlangUtils.cc
        c_src/util/stringUtils.cc
        c_src/util/utils.h)

add_library(eleveldb ${ELEVELDB_SRCS})

add_executable(bigsetClockValidationTool c_src/test/bigsetClockValidationTool.cc c_src/BigsetClock.cc)
target_link_libraries(bigsetClockValidationTool eleveldb)

add_executable(bigsetClock_test c_src/test/bigsetClock_test.cc c_src/BigsetClock.cc)
target_link_libraries(bigsetClock_test gtest_main gtest_all eleveldb)

add_executable(buffer_test c_src/util/test/buffer_test.cc)
target_link_libraries(buffer_test gtest_main gtest_all)
