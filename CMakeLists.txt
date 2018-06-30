cmake_minimum_required(VERSION 3.8)
project(mysql_compare VERSION 0.0.0$ENV{TRAVIS_BUILD_NUMBER})

set(SOURCE_FILES main.cpp index.cpp index.h)
add_executable(mysql_compare ${SOURCE_FILES})

set_target_properties(mysql_compare PROPERTIES
        CXX_STANDARD 14
        CXX_STANDARD_REQUIRED ON
        COMPILE_OPTIONS -Wpedantic -Wall -Wextra
        )

##Find Packages

##  Try to find Mysql-Connector-C++
set(MYSQLCONNECTORCPP_ROOT_DIR
        "${MYSQLCONNECTORCPP_ROOT_DIR}"
        CACHE
        PATH
        "Where to start looking for this component.")

if(WIN32)
    find_path(MYSQLCONNECTORCPP_INCLUDE_DIR
            NAMES
            mysql_connection.h
            PATHS
            "C:\\Program Files"
            HINTS
            ${MYSQLCONNECTORCPP_ROOT_DIR}
            PATH_SUFFIXES
            include)

    find_library(MYSQLCONNECTORCPP_LIBRARY
            NAMES
            mysqlcppconn
            mysqlcppconn-static
            HINTS
            ${MYSQLCONNECTORCPP_ROOT_DIR}
            PATH_SUFFIXES
            lib)

else()
    find_path(MYSQLCONNECTORCPP_INCLUDE_DIR
            mysql_connection.h
            HINTS
            ${MYSQLCONNECTORCPP_ROOT_DIR}
            PATH_SUFFIXES
            include)

    find_library(MYSQLCONNECTORCPP_LIBRARY
            NAMES
            mysqlcppconn
            mysqlcppconn-static
            HINTS
            ${MYSQLCONNECTORCPP_ROOT_DIR}
            PATH_SUFFIXES
            lib64
            x86_64-linux-gnu
            lib)
endif()

mark_as_advanced(MYSQLCONNECTORCPP_INCLUDE_DIR MYSQLCONNECTORCPP_LIBRARY)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(MysqlConnectorCpp
        DEFAULT_MSG
        MYSQLCONNECTORCPP_INCLUDE_DIR
        MYSQLCONNECTORCPP_LIBRARY)

#if(MYSQLCONNECTORCPP_FOUND)
set(MYSQLCONNECTORCPP_INCLUDE_DIRS "${MYSQLCONNECTORCPP_INCLUDE_DIR}") # Add any dependencies here
set(MYSQLCONNECTORCPP_LIBRARIES "${MYSQLCONNECTORCPP_LIBRARY}") # Add any dependencies here
mark_as_advanced(MYSQLCONNECTORCPP_ROOT_DIR)
#endif()
set_target_properties(mysql_compare PROPERTIES
        INCLUDE_DIRECTORIES ${MYSQLCONNECTORCPP_INCLUDE_DIRS})
target_link_libraries(mysql_compare ${MYSQLCONNECTORCPP_LIBRARIES})

#find_package(GTest REQUIRED)
#
#set_target_properties(test_mysql_compare PROPERTIES
#        INCLUDE_DIRECTORIES ${GTEST_INCLUDE_DIRS} ${MYSQLCONNECTORCPP_INCLUDE_DIRS}
#        )
#
#target_link_libraries(test_mysql_compare GTest::GTest GTest::Main ${MYSQLCONNECTORCPP_LIBRARIES})

install(TARGETS mysql_compare RUNTIME DESTINATION bin)

set(CPACK_GENERATOR DEB)

set(CPACK_PACKAGE_VERSION_MAJOR "${PROJECT_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${PROJECT_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${PROJECT_VERSION_PATCH}")

set(CPACK_PACKAGE_CONTACT deepdivenow@gmail.com)
set(CPACK_PACKAGE_NAME mysql_compare)

include (CPack)

#enable_testing()
#
#add_test(test_addr_valid test_ip_filter)








