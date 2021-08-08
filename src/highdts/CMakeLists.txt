cmake_minimum_required(VERSION 3.20)
project(highdts)

set(CMAKE_CXX_STANDARD 14)

set(BOOST_ROOT "./lib/boost")

add_executable(highdts main.cpp file_split.cpp file_split.h file_reader.cpp file_reader.h metadata.cpp metadata.h loaddata_worker.cpp loaddata_worker.h common.h file_util.h)
