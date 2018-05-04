cmake_minimum_required(VERSION 3.8)
project(mysql_compare)

set(CMAKE_CXX_STANDARD 14)

set(SOURCE_FILES main.cpp index.cpp index.h string_format.cpp string_format.h)
add_executable(mysql_compare ${SOURCE_FILES})
include_directories("include/jdbc")
target_link_libraries(mysql_compare ${CMAKE_CURRENT_LIST_DIR}/lib64/libmysqlcppconn.so)