cmake_minimum_required(VERSION 2.8)
cmake_host_system_information(RESULT HOSTNAME QUERY HOSTNAME)

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

#    libcds/src/thread_data.cpp
#    libcds/src/init.cpp
#    libcds/src/topology_linux.cpp

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

set(WITH_VTUNE "" CACHE FILEPATH "Path to VTUNE install directory")
set(VTUNE_LIB "" CACHE FILEPATH "Path to VTUNE ittnotify library")
if(WITH_VTUNE)
    if(EXISTS "${WITH_VTUNE}/amplxe-vars.sh")
        include_directories(
            "${WITH_VTUNE}/include"
        )
        #find_library(VTUNE_LIB ittnotify PATHS "${WITH_VTUNE}/lib32" "${WITH_VTUNE}/lib64")
        target_link_libraries(httest "${WITH_VTUNE}/lib64/libittnotify.a")
        set(HAVE_VTUNE 1)
    else()
        message(SEND_ERROR "${WITH_VTUNE} does not contain VTune")
    endif()
else()
#    message(SEND_ERROR "Please specify root source directory of DIVINE using -DWITH_DIVINE=...")
	set(HAVE_VTUNE 0)
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
    "libcds"
    "libcds/test/include"
    "folly"
    "sparsehash-c11"
)

add_subdirectory("junction")
add_subdirectory("libcuckoo")
add_subdirectory("growt")
add_subdirectory("libcds")
add_subdirectory("folly")
add_subdirectory("sparsehash-c11")

#add_custom_command(
#    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/tbb/release/libtbb.so
#    COMMAND make && mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/tbb/release && cp ${CMAKE_CURRENT_SOURCE_DIR}/tbb/build/*_release/lib*.so ${CMAKE_CURRENT_BINARY_DIR}/tbb/release
#    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tbb
#)
#add_custom_target(tbbproject DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/tbb/release/libtbb.so)

add_dependencies(httest tbbproject)

set(TBB_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tbb/cmake)
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH};${CMAKE_CURRENT_SOURCE_DIR}/tbb/cmake)
set(TBB_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/tbb)

# TBB does not integrate properly with CMake, we need to make some adjustments
include(${TBB_DIR}/TBBBuild.cmake)
if("${HOSTNAME}" STREQUAL "caserta")
    tbb_build(TBB_ROOT ${TBB_ROOT} CONFIG_DIR TBB_DIR MAKE_ARGS compiler=${CMAKE_C_COMPILER})
else()
    tbb_build(TBB_ROOT ${TBB_ROOT} CONFIG_DIR TBB_DIR)
endif()
find_package(TBB)

add_dependencies(httest junction)
add_dependencies(httest libcuckoo)
add_dependencies(httest cds)
add_dependencies(httest folly)
add_dependencies(httest sparsehash-c11)

if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/CLHT/libclht.a)
	target_link_libraries(httest -Wl,-E -fPIC
		${CMAKE_CURRENT_SOURCE_DIR}/CLHT/libclht.a
		${CMAKE_CURRENT_SOURCE_DIR}/CLHT/external/ssmem/libssmem.a
	)
	set(HAVE_CLHT 1)
else()
	set(HAVE_CLHT 0)
endif()

target_link_libraries(httest -Wl,-E -fPIC numa junction turf dl atomic libcuckoo
 folly
 cds
 ${TBB_IMPORTED_TARGETS}
)

install (TARGETS httest DESTINATION bin)

message(STATUS "Host: ${HOSTNAME}")
message(STATUS "Available: ${CMAKE_CONFIGURATION_TYPES}")
message(STATUS "Building configuration: ${CMAKE_BUILD_TYPE}")
message(STATUS "    C flags: ${CMAKE_C_FLAGS}")
message(STATUS "  CXX flags: ${CMAKE_CXX_FLAGS}")
message(STATUS "        TBB: ${TBB_IMPORTED_TARGETS}")
if(HAVE_VTUNE)
    message(STATUS "      VTune: ${WITH_VTUNE}, libs: ${VTUNE_LIB}")
endif()
configure_file(config.h.in httestconfig.h)
