CMAKE_MINIMUM_REQUIRED(VERSION 3.2)

project(db)
add_subdirectory(parser)
set(CMAKE_BUILD_TYPE Release)
set(CMAKE_BINARY_DIR build)
set(EXECUTABLE_OUTPUT_PATH build)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O2")

set(SOURCE
	${SOURCE}
	src/record.cpp
	src/recordmanager.cpp
	src/varcharmanager.cpp
	src/tablemanager.cpp
	src/indexmanager.cpp
	src/databasemanager.cpp
	filesystem/pf.cpp
	src/expression.cpp
)

set(HEADERS
	${HEADERS}
)

add_library(${CMAKE_PROJECT_NAME}_lib ${SOURCE} ${HEADERS})
add_executable(${CMAKE_PROJECT_NAME} src/main.cpp)
target_link_libraries(${CMAKE_PROJECT_NAME} sql_parser ${CMAKE_PROJECT_NAME}_lib)
