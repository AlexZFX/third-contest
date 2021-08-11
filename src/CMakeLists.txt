
cmake_minimum_required(VERSION 3.15)

project(third-contest)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

set(CMAKE_CXX_STANDARD 11)
set(VERSION 0.0.1)

include_directories(.)


file(GLOB_RECURSE CURR_SRC "*.cpp" "*.cc" "*.hpp")

add_executable(dts demo.cpp ${CURR_SRC})

set(CMAKE_BUILD_TYPE "Debug")

target_link_libraries(dts pthread mysqlclient)


