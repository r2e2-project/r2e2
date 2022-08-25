cmake_minimum_required ( VERSION 3.0.0 )
cmake_policy ( SET CMP0054 NEW )
project ( R2E2 )

add_subdirectory ( lib/pbrt )

include ( etc/build_defs.cmake )
include ( etc/build_type.cmake )
include ( etc/cflags.cmake )

include ( etc/clang_format.cmake )
include ( etc/clang_tidy.cmake )

set( Protobuf_USE_STATIC_LIBS ON )
include ( FindProtobuf )

find_package ( Protobuf REQUIRED )
find_library ( PTHREAD pthread )

include ( FindOpenSSL )

include ( etc/find_lz4.cmake )

include_directories ( SYSTEM
  "${CMAKE_CURRENT_LIST_DIR}/lib/pbrt/src/include"
  "${CMAKE_CURRENT_LIST_DIR}/lib/pbrt/src/core"
  "${CMAKE_CURRENT_LIST_DIR}/lib/pbrt/src/ext/glog/src"
  "${CMAKE_CURRENT_LIST_DIR}/lib"
  "${CMAKE_CURRENT_BINARY_DIR}/lib/pbrt/src/ext/glog"
)

add_subdirectory ( src/messages )
include_directories ( "${CMAKE_CURRENT_BINARY_DIR}/src/messages" )

add_definitions ( -D PBRT_CONSTEXPR=constexpr )
add_definitions ( -D PBRT_THREAD_LOCAL=thread_local )

file ( GLOB SOURCE
    src/common/*
    src/master/*
    src/messages/*
    src/net/*
    src/transfer/*
    src/remote/*
    src/schedulers/*
    src/storage/*
    src/util/*
    src/worker/*
)

include_directories ( src )

add_library( r2e2 STATIC
    ${SOURCE}
)

# Force generating protobuf headers first
add_dependencies( r2e2 r2e2_messages )

set ( ALL_LIBS
    r2e2
    r2e2_messages
    pbrt
    pbrt_messages
    atomic
    ${CMAKE_THREAD_LIBS_INIT}
    IlmImf
    Imath
    Half
    glog
    Ptex_static
    ${LZ4_LIBRARY}
    ${ZLIB_LIBRARY}
    ${Protobuf_LIBRARIES}
    ${OPENSSL_LIBRARIES}
)

set( STATIC_LINK_FLAGS "" )

# In Release mode, always build the main binaries statically
if ( CMAKE_BUILD_TYPE MATCHES Release )
  set ( STATIC_LINK_FLAGS dl z unwind lzma -static -Wl,-allow-multiple-definition
                          -Wl,--whole-archive -lpthread -Wl,--no-whole-archive
                          -s )
endif()

add_executable ( r2e2-lambda-master src/frontend/lambda-master.cc )
target_link_libraries( r2e2-lambda-master ${ALL_LIBS} ${STATIC_LINK_FLAGS} )

add_executable ( r2e2-lambda-worker src/frontend/lambda-worker.cc )
target_link_libraries( r2e2-lambda-worker ${ALL_LIBS} ${STATIC_LINK_FLAGS} )

add_executable ( r2e2-aggregate src/frontend/aggregate.cc )
target_link_libraries( r2e2-aggregate ${ALL_LIBS} )

add_executable ( aggregate-timelapse src/frontend/aggregate-timelapse.cc )
target_link_libraries( aggregate-timelapse ${ALL_LIBS} )

add_executable ( load-treelet src/frontend/load-treelet.cc )
target_link_libraries( load-treelet ${ALL_LIBS} )

add_executable ( camera-generator src/frontend/camera-generator.cc )
target_link_libraries( camera-generator ${ALL_LIBS} )

add_executable( memcached_test src/tests/memcached_test.cc )
target_link_libraries( memcached_test ${ALL_LIBS} )

install( TARGETS
  r2e2-lambda-master
  r2e2-lambda-worker
  DESTINATION
  bin
)
