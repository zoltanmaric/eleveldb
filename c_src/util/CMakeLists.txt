cmake_minimum_required(VERSION 3.3)
project(BashoUtils CXX)

# set GTEST_DIR in the shell env?
set(GTEST_DIR "/Users/pplace/github/googletest/googletest")

add_definitions(-Wall -std=c++11 -ansi -Wno-deprecated -pthread)

include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${GTEST_DIR}/include ${GTEST_DIR})

add_library(gtest_all STATIC ${GTEST_DIR}/src/gtest-all.cc)
add_library(gtest_main STATIC ${GTEST_DIR}/src/gtest_main.cc)

set(SOURCE_FILES
    buffer.h
    slice.h)

#add_library(BashoUtils STATIC ${SOURCE_FILES})

add_executable(buffer_test test/buffer_test.cc)
target_link_libraries(buffer_test gtest_main gtest_all)
