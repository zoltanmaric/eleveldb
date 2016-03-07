cmake_minimum_required(VERSION 3.3)
project(BashoUtils CXX)

# set GTEST_DIR in the shell env?
set(GTEST_DIR "/Users/pplace/github/googletest/googletest")

set(CMAKE_CXX_FLAGS "-Wall -O0 -g3 -std=c++11 -stdlib=libc++ -Wno-deprecated -pthread")

include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../leveldb/include ${GTEST_DIR}/include ${GTEST_DIR})

add_library(gtest_all STATIC ${GTEST_DIR}/src/gtest-all.cc)
add_library(gtest_main STATIC ${GTEST_DIR}/src/gtest_main.cc)

set(SOURCE_FILES
    buffer.h
    stringUtils.cc)

#add_library(BashoUtils STATIC ${SOURCE_FILES})

add_executable(buffer_test test/buffer_test.cc)
target_link_libraries(buffer_test gtest_main gtest_all)

add_executable(stringUtils_test test/stringUtils_test.cc stringUtils.cc)
target_link_libraries(stringUtils_test gtest_main gtest_all)
