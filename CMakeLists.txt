cmake_minimum_required(VERSION 2.8)

project (httest)

include(CheckIncludeFiles)
#include(CheckIncludeFileCXX)
include(CheckCXXCompilerFlag)

set(CMAKE_CONFIGURATION_TYPES "Debug;Release;Profile" CACHE STRING "" FORCE)
set(CMAKE_C_FLAGS_DEBUG "-O0 -UNDEBUG -DYYDEBUG=1 ${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_C_FLAGS_RELEASE "-O4 -DNDEBUG -g")
set(CMAKE_C_FLAGS_PROFILE "${CMAKE_C_FLAGS_RELEASE} -g")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -UNDEBUG -DYYDEBUG=1 ${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_CXX_FLAGS_RELEASE "-O4 -DNDEBUG -g")
set(CMAKE_CXX_FLAGS_PROFILE "${CMAKE_CXX_FLAGS_RELEASE} -g")
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY HELPSTRING "Choose the type of build")
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug;Release;Profile")

CHECK_CXX_COMPILER_FLAG("-mcx16" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcx16")
endif()

#CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
#if(COMPILER_SUPPORTS_MARCH_NATIVE)
#    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
#endif()

add_executable(httest
    httest.cpp
    hashtable.c
    SYLVAN_TABLE.cpp
    dbs-ll.c
    dbs-ll.cpp
    murmurhash.cpp
)
set_property(TARGET httest PROPERTY CXX_STANDARD 14)
set_property(TARGET httest PROPERTY CXX_STANDARD_REQUIRED ON)

add_definitions(-Wall -Wextra -Wno-vla -Wno-implicit-fallthrough -Wno-unused-parameter)

find_package(Threads REQUIRED)
if(THREADS_HAVE_PTHREAD_ARG)
  target_compile_options(httest PUBLIC "-pthread")
endif()
if(CMAKE_THREAD_LIBS_INIT)
  target_link_libraries(httest "${CMAKE_THREAD_LIBS_INIT}")
endif()

set(WITH_LTSMIN "" CACHE FILEPATH "Path to LTSmin install directory")
if(WITH_LTSMIN)
    if(EXISTS "${WITH_LTSMIN}/src/mc-lib/hashtable.h")
		include_directories(
			"${WITH_LTSMIN}/src"
			"${WITH_LTSMIN}/build/src"
		)
		set(HAVE_LTSMIN 1)
    else()
        message(SEND_ERROR "${WITH_LTSMIN} does not contain src/mc-lib/hashtable.h")
    endif()
else()
#    message(SEND_ERROR "Please specify root source directory of LTSmin using -DWITH_LTSMIN=...")
	set(HAVE_LTSMIN 0)
endif()

set(WITH_DIVINE "" CACHE FILEPATH "Path to DIVINE install directory")
if(WITH_DIVINE)
    if(EXISTS "${WITH_DIVINE}/bricks/brick-hashset")
		include_directories(
			"${WITH_DIVINE}/"
		)
		set(HAVE_DIVINE 1)
    else()
        message(SEND_ERROR "${WITH_DIVINE} does not contain bricks/brick-hashset")
    endif()
else()
#    message(SEND_ERROR "Please specify root source directory of DIVINE using -DWITH_DIVINE=...")
	set(HAVE_DIVINE 0)
endif()

include_directories(
    "CLHT/include"
    "${WITH_DIVINE}/bricks"
    "junction"
    "${CMAKE_CURRENT_BINARY_DIR}/junction/include"
    "turf"
    "${CMAKE_CURRENT_BINARY_DIR}/junction/turf/include"
    "${CMAKE_CURRENT_BINARY_DIR}"
    "../hashtable"
	"Honeycomb/src/common"
	"Honeycomb/src/linux"
    "libfrugi/include"
    "libcuckoo/libcuckoo"
    "growt"
)

add_subdirectory("junction")
add_subdirectory("libcuckoo")
add_subdirectory("growt")

add_dependencies(httest junction)
add_dependencies(httest libcuckoo)

set(LIBRARIES numa tbb tbbmalloc junction turf dl atomic libcuckoo)
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/CLHT/libclht.a)
	target_link_libraries(httest -Wl,-E -fPIC
		${CMAKE_CURRENT_SOURCE_DIR}/CLHT/libclht.a
		${CMAKE_CURRENT_SOURCE_DIR}/CLHT/external/ssmem/libssmem.a
	)
	set(HAVE_CLHT 1)
else()
	set(HAVE_CLHT 0)
endif()

target_link_libraries(httest -Wl,-E -fPIC ${LIBRARIES})

install (TARGETS httest DESTINATION bin)

message(STATUS "Available: ${CMAKE_CONFIGURATION_TYPES}")
message(STATUS "Building configuration: ${CMAKE_BUILD_TYPE}")
message(STATUS "    C flags: ${CMAKE_C_FLAGS}")
message(STATUS "  CXX flags: ${CMAKE_CXX_FLAGS}")

configure_file(config.h.in config.h)
