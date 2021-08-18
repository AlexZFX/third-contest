
cmake_minimum_required(VERSION 3.15)

project(third-contest)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -g -O2")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -fsanitize=address -g3")

set(CMAKE_CXX_STANDARD 11)
set(VERSION 0.0.1)

set(MYSQL_SOURCE_DIR /Users/alexfxzhang/repos/cpp/mysql-8.0.23)
set(MYSQL_INCLUDE_DIR
        ${MYSQL_SOURCE_DIR}
        ${MYSQL_SOURCE_DIR}/include
        ${MYSQL_SOURCE_DIR}/bld/include
        /usr/include/mysql
        )
set(MYSQL_LIB_DIR
#        ${MYSQL_SOURCE_DIR}/bld
#        ${MYSQL_SOURCE_DIR}/bld/lib
#        ${MYSQL_SOURCE_DIR}/bld/archive_output_directory
        /usr/local/Cellar/mysql/8.0.18_1/lib
        /usr/lib64/mysql/
        )

#add_executable(dts demo.cpp workers/line_filter.cpp workers/line_filter.h workers/file_reader.cpp workers/file_reader.h utils/Util.h workers/file_split.cpp workers/file_split.h )
include_directories(./ ./lib ${MYSQL_INCLUDE_DIR})
link_directories(${MYSQL_LIB_DIR})


file(GLOB_RECURSE CURR_SRC common/*.cpp entity/*.cpp utils/*.cpp workers/*.cpp demo.cpp)

add_executable(dts ${CURR_SRC})

set(CMAKE_BUILD_TYPE "Debug")

target_link_libraries(dts pthread mysqlclient)


